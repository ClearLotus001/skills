# excel-config-validator 使用文档

## 1. 使用目标

通过自然语言描述校验需求，自动执行 Excel/CSV 配置检查并输出报告。支持多文件、多 Sheet、跨文件关联的确定性检查。

## 2. 推荐调用方式

可直接使用以下模板：

```text
请使用 excel-config-validator。

输入目录：
- <你的输入目录或文件>

规则描述：
1. <规则1>
2. <规则2>
3. <规则3>

输出目录：
- <你的输出目录>
```

如果已有 `rules.json`，可直接指定：

```text
请使用 excel-config-validator。

输入目录：C:\data\configs
规则文件：C:\data\rules.json
输出目录：C:\data\output
```

## 3. 支持的文件格式

| 格式 | 后缀 | 解析库 |
|---|---|---|
| Excel 2007+ | `.xlsx` / `.xlsm` | openpyxl |
| Excel 97-2003 | `.xls` | xlrd |
| Excel Binary | `.xlsb` | pyxlsb |
| CSV | `.csv` | 内置 csv（自动检测 UTF-8/GB18030/UTF-16 编码） |

## 4. 命令行参数

执行入口为 `scripts/run_validator.py`，参数说明：

| 参数 | 必需 | 默认值 | 说明 |
|---|---|---|---|
| `--inputs` | 是 | - | 输入文件或目录路径 |
| `--rules` | 是 | - | `rules.json` 规则文件路径 |
| `--out` | 是 | - | 输出目录路径 |
| `--rule-set` | 否 | 全部规则 | 按 `rule_sets` 中定义的分组名筛选执行规则 |
| `--run-id` | 否 | UTC 时间戳 | 运行 ID，用于标识本次执行 |
| `--max-errors` | 否 | 无限制 | 质量门禁：错误数超过阈值时返回退出码 2 |
| `--allow-parser-warning` | 否 | 严格模式 | 允许解析告警继续执行（默认遇到解析告警会失败） |
| `--chunk-size` | 否 | 2000 | 行数据分块大小，影响内存与 I/O 平衡 |
| `--keep-formula` | 否 | 读取计算值 | 读取公式文本而非计算值 |
| `--resume` | 否 | 重新执行 | 从已有 `run_state.json` 断点续跑 |

**退出码**：

| 退出码 | 含义 |
|---|---|
| 0 | 校验成功（可能含警告/信息级 issue） |
| 1 | 执行异常（规则编译失败、解析失败等） |
| 2 | 质量门禁失败（错误数超过 `--max-errors`） |

## 5. rules.json 规范

### 5.1 顶层结构

```json
{
  "datasets": { ... },
  "schema_rules": [ ... ],
  "range_rules": [ ... ],
  "row_rules": [ ... ],
  "relation_rules": [ ... ],
  "aggregate_rules": [ ... ],
  "global_rules": [ ... ],
  "rule_sets": { ... }
}
```

### 5.2 datasets（数据集定义）

将逻辑数据集映射到物理文件和工作表：

```json
{
  "datasets": {
    "orders": {
      "file": "orders.xlsx",
      "sheet": "订单明细"
    },
    "products": {
      "file_pattern": "product_*.csv",
      "sheet": "_csv_"
    }
  }
}
```

- `file`：精确文件名匹配
- `file_pattern`：通配符匹配（支持 `*`、`?`）
- `sheet`：工作表名（CSV 文件固定为 `_csv_`）
- 未指定 `sheet` 时自动使用第一个工作表

### 5.3 schema_rules（结构/字段规则）

```json
{
  "schema_rules": [
    {
      "rule_id": "R001",
      "dataset": "orders",
      "column": "订单号",
      "check": "required",
      "severity": "error"
    },
    {
      "rule_id": "R002",
      "dataset": "orders",
      "column": "订单号",
      "checks": [
        {"type": "required"},
        {"type": "unique"},
        {"type": "min_digits", "min_digits": 6}
      ],
      "severity": "error"
    }
  ]
}
```

**支持的 check 类型**：

| check 类型 | 参数 | 说明 |
|---|---|---|
| `required` | - | 不能为空 |
| `string` | - | 必须为字符串类型 |
| `numeric` | - | 必须为数字 |
| `min_digits` | `min_digits` | 至少 N 位数字 |
| `increasing` | - | 按行严格递增（整数） |
| `unique` | - | 列值唯一 |
| `date` | - | 必须为日期格式 |
| `datetime_format` | `format` | 必须符合时间格式（如 `YYYY-MM-DD HH:mm:ss`） |
| `max_length` | `max_length` | 最大字符长度 |
| `min_length` | `min_length` | 最小字符长度 |
| `regex` | `pattern` | 正则全匹配 |
| `enum` / `whitelist` | `values`, `case_insensitive` | 枚举允许值 |
| `positive` | - | 必须为正数 |
| `non_negative` | - | 必须为非负数 |
| `conditional_required` | `when` | 条件满足时必填（`when` 为行表达式） |

### 5.4 range_rules（范围规则）

```json
{
  "range_rules": [
    {
      "rule_id": "R010",
      "dataset": "orders",
      "column": "金额",
      "min": 0,
      "max": 999999,
      "include_min": true,
      "include_max": false,
      "allow_empty": true,
      "severity": "error"
    }
  ]
}
```

- 支持数值和日期/时间范围
- `include_min` / `include_max`：开闭区间控制（默认闭区间）
- `allow_empty`：空值是否跳过（默认 `true`）
- `value_type`：可选，显式指定 `number` 或 `date`（默认自动推断）

### 5.5 row_rules（行表达式规则）

```json
{
  "row_rules": [
    {
      "rule_id": "R020",
      "dataset": "orders",
      "when": "text('类型') == '退款'",
      "assert": "num('金额') < 0",
      "message": "退款订单的金额必须为负数",
      "severity": "error"
    }
  ]
}
```

**内置函数**：

| 分类 | 函数 | 说明 |
|---|---|---|
| 基础 | `value(col, default)` | 获取列原始值 |
| 基础 | `text(col, default)` | 获取列字符串值（自动 strip） |
| 基础 | `num(col)` | 安全数值解析，返回 float 或 None |
| 基础 | `intv(col)` | 安全整数解析，返回 int 或 None |
| 基础 | `empty(col)` | 判断列值是否为空 |
| 基础 | `exists(col)` | 判断列值是否非空 |
| 基础 | `match(pattern, data)` | 正则全匹配 |
| 日期 | `date_val(col)` | 解析列值为 datetime 对象 |
| 日期 | `today()` | 返回当天零点的 datetime |
| 日期 | `days_between(col1, col2)` | col2 - col1 的天数差 |
| 日期 | `days_since(col)` | 从列日期到今天的天数差 |
| 日期 | `year(col)` / `month(col)` / `day(col)` | 提取日期的年/月/日 |
| 字符串 | `strip(col)` | 去除首尾空白 |
| 字符串 | `lower(col)` / `upper(col)` | 转小写/大写 |
| 字符串 | `contains(col, substr)` | 包含子串检查 |
| 字符串 | `starts_with(col, prefix)` | 前缀检查 |
| 字符串 | `ends_with(col, suffix)` | 后缀检查 |
| 跨行 | `prev_value(col, default)` | 上一行的列原始值（首行返回 default） |
| 跨行 | `prev_text(col, default)` | 上一行的列字符串值 |
| 跨行 | `prev_num(col)` | 上一行的列数值 |
| 多列 | `sum_cols(col1, col2, ...)` | 多列数值求和 |
| 多列 | `coalesce(col1, col2, ...)` | 第一个非空列值 |
| 多列 | `in_list(val, items)` | 值是否在列表中 |

标量函数：`len`、`min`、`max`、`abs`、`round`、`str`、`int`、`float`、`bool`。

**跨行引用示例**：

```json
{
  "rule_id": "R021",
  "dataset": "orders",
  "assert": "prev_num('日期序号') is None or num('日期序号') >= prev_num('日期序号')",
  "message": "日期序号不能回退",
  "severity": "error"
}
```

**日期运算示例**：

```json
{
  "rule_id": "R022",
  "dataset": "orders",
  "when": "exists('交货日期')",
  "assert": "days_between('下单日期', '交货日期') >= 0",
  "message": "交货日期不能早于下单日期",
  "severity": "error"
}
```

**条件分支（branches）模式**：

当同一数据集需要按条件执行不同断言时，可使用 `branches` 替代单一 `when`/`expression`。每行数据按分支顺序匹配，首个 `when` 命中的分支执行其 `assert`；若所有分支均未命中，则执行 `else_assert`。

```json
{
  "rule_id": "R025",
  "dataset": "orders",
  "branches": [
    {
      "when": "text('类型') == '退款'",
      "assert": "num('金额') < 0",
      "message": "退款订单的金额必须为负数"
    },
    {
      "when": "text('类型') == '充值'",
      "assert": "num('金额') > 0",
      "message": "充值订单的金额必须为正数"
    }
  ],
  "else_assert": "num('金额') >= 0",
  "else_message": "其他类型订单的金额不能为负数",
  "severity": "error"
}
```

| 字段 | 必需 | 说明 |
|---|---|---|
| `branches` | 是 | 分支数组，每个分支包含 `when`、`assert`、`message` |
| `branches[].when` | 是 | 分支条件表达式（使用行表达式语法） |
| `branches[].assert` | 是 | 条件命中时执行的断言表达式 |
| `branches[].message` | 否 | 断言失败时的错误消息 |
| `else_assert` | 否 | 所有分支均未命中时执行的断言表达式 |
| `else_message` | 否 | else 断言失败时的错误消息 |

> `branches` 模式与传统 `when`/`assert` 模式互斥，同一条规则只能使用其中一种。

### 5.6 relation_rules（关联规则）

```json
{
  "relation_rules": [
    {
      "rule_id": "R030",
      "source_dataset": "orders",
      "target_dataset": "products",
      "source_key": "商品ID",
      "target_key": "ID",
      "mode": "fk_exists",
      "allow_source_empty": false,
      "severity": "error"
    }
  ]
}
```

**支持的模式**：

| 模式 | 别名 | 说明 |
|---|---|---|
| `fk_exists` | - | 源键值必须在目标键集合中存在（外键检查） |
| `set_equal` | `equal_set`、`same_set` | 源键集合与目标键集合完全一致 |
| `one_to_one` | `1:1` | 双方键均唯一且集合相等（一对一） |
| `one_to_many` | `1:N`、`1:n` | 目标键唯一，源键均在目标中存在（一对多） |
| `many_to_many` | `N:N`、`n:n`、`M:N`、`m:n` | 双向存在性检查（多对多） |

### 5.7 aggregate_rules（聚合校验规则）

对数据集的指定列执行聚合运算后断言，可选按列分组。

```json
{
  "aggregate_rules": [
    {
      "rule_id": "AGG_001",
      "dataset": "orders",
      "column": "金额",
      "function": "sum",
      "assert": "result > 0",
      "message": "订单总金额必须大于0",
      "severity": "error"
    },
    {
      "rule_id": "AGG_002",
      "dataset": "orders",
      "column": "金额",
      "function": "sum",
      "group_by": "部门",
      "assert": "result <= 1000000",
      "message": "每个部门的总金额不能超过100万",
      "severity": "error"
    }
  ]
}
```

**支持的聚合函数**：

| 函数 | 说明 |
|---|---|
| `sum` | 数值列求和 |
| `count` | 非空值计数 |
| `avg` | 数值列平均值 |
| `min` | 数值列最小值 |
| `max` | 数值列最大值 |
| `distinct_count` | 去重后的值计数 |

- `group_by`：可选，指定分组列。分组后对每组分别执行聚合+断言。
- `assert`：断言表达式，可使用 `result`（聚合值）和 `group`（分组键）。
- 空值不参与 sum/avg/min/max 计算。

### 5.8 通用规则字段

所有规则类型（schema/range/row/relation/aggregate）均支持以下通用字段：

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `rule_id` | string | 自动生成 | 规则唯一标识 |
| `severity` | string | `"error"` | 严重级别：`error` / `warn` / `info` |
| `enabled` | boolean | `true` | 设为 `false` 可临时禁用规则，无需从配置中删除 |
| `message` | string | - | 自定义错误消息 |

### 5.9 rule_sets（规则分组）

```json
{
  "rule_sets": {
    "quick_check": ["R001", "R002", "R010"],
    "full_check": ["R001", "R002", "R010", "R020", "R030"]
  }
}
```

通过 `--rule-set quick_check` 选择性执行部分规则。

## 6. 执行流程

```
compile_rules → parse_excel → validate_local → validate_relations → validate_global → render_report
```

| 阶段 | 脚本 | 产物 |
|---|---|---|
| 规则编译 | `compile_rules.py` | `compiled_rules.json` |
| 文件解析 | `parse_excel.py` | `ingest_manifest.json` + `_row_store/*.jsonl` |
| 局部校验 | `validate_local.py` | `_stages/local_issues.json` |
| 关联校验 | `validate_relations.py` | `_stages/relation_issues.json` |
| 全局校验 | `validate_global.py` | `_stages/global_issues.json` |
| 报告生成 | `render_report.py` | `result.json` / `issues.csv` / `report.md` / `report.html` |

## 7. 结果输出目录

```
<out>/
├── result.json          # 完整结构化结果（含 issues + 统计）
├── issues.csv           # 表格化问题清单（19 列中文表头）
├── report.md            # Markdown 摘要报告
├── report.html          # 交互式 HTML 报告（筛选/排序/分页/分组视图）
├── run_state.json       # 运行状态（支持断点恢复）
├── compiled_rules.json  # 编译后规则（含摘要统计）
├── ingest_manifest.json # 解析清单（含行数据索引和解析说明）
├── _stages/             # 各阶段中间结果
│   ├── local_issues.json
│   ├── relation_issues.json
│   └── global_issues.json
└── _row_store/          # 行数据 JSONL 分块存储
```

## 8. 元数据探查工具

在编写规则之前，可使用独立的元数据探查脚本快速了解文件结构：

```powershell
# 输出到 stdout
python excel-config-validator/scripts/inspect_metadata.py C:\data\orders.xlsx C:\data\products.csv

# 输出到文件（推荐，避免在某些环境中 stdout 被截断）
python excel-config-validator/scripts/inspect_metadata.py C:\data\orders.xlsx C:\data\products.csv --out C:\data\metadata.json
```

输出各文件的工作表名和列头（JSON 格式），不读取行数据，适合快速摸清表结构。
指定 `--out` 参数时结果写入文件，否则输出到 stdout。

## 9. 可执行命令示例

### 基础执行

```powershell
python excel-config-validator/scripts/run_validator.py `
  --inputs C:\data\configs `
  --rules C:\data\rules.json `
  --out C:\data\output
```

### 允许解析告警 + 质量门禁

```powershell
python excel-config-validator/scripts/run_validator.py `
  --inputs C:\data\configs `
  --rules C:\data\rules.json `
  --out C:\data\output `
  --allow-parser-warning `
  --max-errors 10
```

### 断点续跑

```powershell
python excel-config-validator/scripts/run_validator.py `
  --inputs C:\data\configs `
  --rules C:\data\rules.json `
  --out C:\data\output `
  --resume
```

### 指定规则分组

```powershell
python excel-config-validator/scripts/run_validator.py `
  --inputs C:\data\configs `
  --rules C:\data\rules.json `
  --out C:\data\output `
  --rule-set quick_check
```

## 10. 解析行为说明

- **列投影**：解析阶段根据编译后规则自动只读取必要列，减少内存和校验开销。
- **分块写入**：行数据按 `--chunk-size`（默认 2000）写入 `_row_store/*.jsonl`，`ingest_manifest.json` 只保存索引。
- **编码检测**：CSV 文件自动尝试 UTF-8-sig → GB18030 → UTF-16，非 UTF-8 编码会在 `parse_notes` 中说明。
- **openpyxl 扩展告警**：检测到不支持的工作表扩展（extLst）时，会扫描 XML 给出根因说明而非静默忽略。
- **临时文件跳过**：自动跳过 `~$` 开头的 Excel 临时文件和 `issues.csv` 等产物文件。
- **公式处理**：默认 `data_only=True` 读取计算值；`--keep-formula` 读取公式文本。
- **单条规则异常不中断**：校验阶段中某条规则执行异常时，该异常会被记录为 issue 并继续执行后续规则。
- **阶段异常不中断**：某个校验阶段（local/relation/global）整体异常时，会写入空 issues 文件并继续后续阶段。

## 11. 已知限制

- **关联校验内存**：`fk_exists`、`set_equal` 等关联模式已改为流式校验，但仍需在内存中持有目标键集合。对于目标键集合本身极大（千万级唯一键）的场景需注意内存。
- **branches 分支数量**：分支按顺序匹配，每行的 eval 环境已缓存复用（不随分支数重复构建），通常 50 个以内分支无明显性能问题。

## 12. 故障排查

| 现象 | 可能原因 | 解决方法 |
|---|---|---|
| `rules.json` 编译失败 | 缺少 `datasets` 字段 / 规则引用未知数据集 | 检查 datasets 定义与规则中的 dataset 名称是否一致 |
| 解析阶段失败 | 缺少 openpyxl/xlrd/pyxlsb 依赖 | `pip install openpyxl xlrd pyxlsb` |
| 解析告警导致中止 | 默认严格模式 | 添加 `--allow-parser-warning` 参数 |
| CSV 编码错误 | 非 UTF-8/GB18030/UTF-16 编码 | 手动转换为 UTF-8 编码 |
| 断点续跑异常 | `run_state.json` 与输入不匹配 | 删除输出目录重新执行 |
| 报告中 issues 为 0 但有异常 | 校验阶段异常已被容错处理 | 查看 `run_state.json` 的 `stage_exceptions` 字段 |
| 内存不足 | 关联校验目标键集合过大 | 减小 `--chunk-size`，或拆分数据集 |
