# excel-config-validator 使用文档

## 1. 使用目标
通过自然语言描述校验需求，自动执行 Excel/CSV 配置检查并输出报告。

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

## 3. 关键参数（脚本层）
技能内部使用：
- `--inputs`：输入文件/目录
- `--rules`：规则文件路径
- `--out`：输出目录（可自定义）
- `--rule-set`：可选规则分组
- `--max-errors`：质量门禁
- `--allow-parser-warning`：允许解析告警（默认严格模式，解析告警会失败）
- `--chunk-size`：行数据分块写入大小（默认 `2000`）
- `--keep-formula`：读取公式文本而非计算值（默认读取计算值，`data_only=True`）
- `--resume`：断点续跑

解析说明：
- 输入解析阶段会把行数据落盘到 `_row_store/*.jsonl`，`ingest_manifest.json` 只保存索引信息与摘要元数据。
- 会按规则自动做列投影，减少无关列读取与后续校验开销。
- 对 openpyxl 的未知扩展会写入 `parse_notes` 给出根因说明（不是静默忽略）。

## 4. 当前已实现校验规则
- 局部校验：
  - 数据集配置的文件/工作表存在性
  - `schema_rules` 列存在性检查
  - 行级规则：`required`、`string`、`numeric`、`min_digits`、`increasing`、`unique`
  - 时间规则：`date`、`datetime_format`
  - 文本规则：`max_length`、`min_length`、`regex`
  - 枚举规则：`enum` / `whitelist`（支持 `case_insensitive`）
  - 数值规则：`positive`、`non_negative`
  - 条件规则：`conditional_required`（when 表达式触发）
  - `range_rules`：数值/日期范围（`min`、`max`、`include_min`、`include_max`、`allow_empty`）
  - `row_rules`：行表达式断言（`expression`/`assert` + `when`）
- 关联校验：
  - `relation_rules` 源/目标数据集是否定义
  - 源/目标数据集映射的文件/工作表是否存在
  - 键列存在性检查
  - 外键存在性（`fk_exists`）
  - 集合一致性（`set_equal`）
- 全局校验：
  - `rule_id` 是否跨规则组重复
- 报告：
  - `report.html` 支持筛选、排序、分页、分组视图
  - `result.json` 含分组摘要（类别/文件+页签/规则）

## 5. 结果输出目录
所有产物输出到你指定的 `--out` 目录，包含：
```
<out>/
├── result.json          # 完整结构化结果（含 issues + 统计）
├── issues.csv           # 表格化问题清单
├── report.md            # Markdown 摘要报告
├── report.html          # 交互式 HTML 报告
├── run_state.json       # 运行状态（断点恢复）
├── compiled_rules.json  # 编译后规则（含摘要统计）
├── ingest_manifest.json # 解析清单
├── _stages/             # 各阶段中间结果
│   ├── local_issues.json
│   ├── relation_issues.json
│   └── global_issues.json
└── _row_store/          # 行数据 JSONL 分块存储
```

## 6. 一条可执行命令（可选）
```powershell
python excel-config-validator/scripts/run_validator.py `
  --inputs C:\path\to\inputs `
  --rules C:\path\to\rules.json `
  --out C:\path\to\output
```
