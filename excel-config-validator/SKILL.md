---
name: excel-config-validator
description: 用自然语言描述 Excel/CSV 配置校验需求，自动转为规则并执行校验，输出 JSON/CSV/HTML 报告。适用于多文件、多 Sheet、跨文件关联的确定性检查。
---

# excel-config-validator

## 何时使用
- 用户提到"校验"、"检查"、"验证"、"validate"、"check" 配合 Excel/CSV/配置表等关键词。
- 用户提供 Excel/CSV 文件并要求检查数据质量、格式合规、一致性。
- 用户需要生成配置校验报告（JSON/CSV/HTML）。
- 用户描述了跨文件/跨表的数据关联校验需求。
- 用户已有 `rules.json` 并要求执行校验。
- 用户提到"规则"、"rules" 配合数据/配置表等关键词。
- 用户以自然语言描述规则，希望自动转换并执行。

## 不适用
- 仅浏览、查看表格内容（非校验目的）。
- 开放式数据分析、统计、可视化需求。
- 非结构化文本校验。
- Excel 公式编写或 VBA 开发。

## 必要输入
- `inputs`：Excel/CSV 文件或目录（`.xlsx/.xls/.xlsm/.xlsb/.csv`）。
- 规则信息：自然语言规则描述，或现成 `rules.json`。
- `out`：输出目录（可自定义）。

## 执行约束（必须）
1. 将自然语言规则明确化为 `rules.json`（有歧义先澄清）。
2. 使用脚本执行，不用模型臆断结果。
3. 执行入口固定为 `scripts/run_validator.py`。
4. **尽量减少命令执行次数**：优先使用 `--scan` 模式探查文件，再一次性执行校验。
5. **不要依赖终端输出**：所有输出自动写入日志文件 `<out>/_run.log`，执行后通过读取日志文件和 `result.json` 获取结果。

## 规则生成工作流（自然语言 → rules.json）
当用户以自然语言描述校验需求时，按以下步骤生成 `rules.json`：

**第 1 步：探查文件结构**
```bash
python scripts/run_validator.py --inputs <输入文件或目录> --out <输出目录> --scan
```
读取 `<out>/_scan.json`，了解文件名、sheet 列表、每个 sheet 的列头和行数。

**第 2 步：构建 datasets 映射**
根据 `_scan.json` 中的文件和 sheet 信息，为每个需要校验的数据源定义逻辑名称。

**第 3 步：逐层构建规则**
按需求依次添加（参考 `references/rule_schema.md` 获取完整字段定义）：
1. `schema_rules` — 列存在性、类型、格式、唯一性等基础检查
2. `range_rules` — 数值/日期范围约束
3. `row_rules` — 行内跨列逻辑断言（参考引擎支持的内置函数）
4. `aggregate_rules` — 聚合校验（sum/count/avg 等）
5. `relation_rules` — 跨表外键、集合一致性、基数约束（参考 `references/relation_patterns.md`）

**第 4 步：编写时注意**
- 每条规则必须有唯一的 `rule_id`
- `severity` 取值：`error`（必须修复）、`warn`（建议修复）、`info`（仅提示）
- 有歧义的需求先向用户澄清，不要猜测

**最小 rules.json 示例**
```json
{
  "datasets": {
    "users": { "file": "用户表.xlsx", "sheet": "Sheet1" }
  },
  "schema_rules": [
    {
      "rule_id": "USR_ID_REQUIRED",
      "dataset": "users",
      "column": "user_id",
      "checks": [{ "type": "required" }, { "type": "unique" }],
      "severity": "error"
    }
  ]
}
```

## 标准执行
```bash
python scripts/run_validator.py \
  --inputs <输入文件或目录> \
  --rules <rules.json> \
  --out <输出目录> \
  [--rule-set <规则分组>] \
  [--max-errors <阈值>] \
  [--allow-parser-warning] \
  [--chunk-size <分块大小>] \
  [--keep-formula] \
  [--resume] \
  [--log <自定义日志路径>]
```

说明：
- 日志自动写入 `<out>/_run.log`（也可通过 `--log` 自定义路径），执行完成后读取日志文件获取执行输出。
- 默认开启严格解析（`fail_on_parser_warning=true`），如需放宽可传 `--allow-parser-warning`。
- 默认 `data_only=True`（读取公式计算值）；如需读取公式文本可传 `--keep-formula`。
- 校验引擎采用 chunk 流式处理，大文件不会导致内存溢出。
- 单条规则异常不会中断整体流程，异常会被记录在 issues 中并继续执行。

## 执行后获取结果
执行完成后（**不要依赖终端输出**）：
1. 读取 `<out>/_run.log` 获取执行日志（包含进度、警告、错误信息）。
2. 读取 `<out>/result.json` 获取完整校验结果。
3. 提示用户打开 `<out>/report.html` 查看交互式报告。

### 结果解读与呈现
读取 `result.json` 后，按以下顺序向用户呈现：
1. **总览**：读取 `summary.total_issues` 和 `summary.severity_counts_zh`，给出问题总数和严重级别分布。
2. **按严重级别分层**：先呈现 error 级别问题（必须修复），再呈现 warn（建议修复），info 可省略。
3. **高频问题聚焦**：读取 `summary.top_rules`，对出现次数最多的规则违规给出修复建议。
4. **按文件/Sheet 分组**：读取 `summary.by_file_sheet`，帮助用户定位问题集中的文件。
5. 最后提示用户可打开 `report.html` 查看完整交互式报告（支持筛选、排序、分页、搜索）。

## 异常处理
当脚本执行失败或返回非零退出码时：
1. **读取日志**：读取 `<out>/_run.log` 定位错误信息。
2. **常见错误修复**：
   - `rules.json 编译失败` → 检查 JSON 语法和字段引用，参考 `references/troubleshooting.md`
   - `文件未找到` → 检查 `--inputs` 路径和 dataset 中的 file/file_pattern 配置
   - `缺少 openpyxl/xlrd/pyxlsb` → 执行 `pip install -r requirements.txt`
   - `解析阶段告警` → 添加 `--allow-parser-warning` 或修复源文件，参考 `references/edge_cases.md`
3. **断点恢复**：如果部分阶段已完成，可用 `--resume` 从断点继续，无需重跑全流程。
4. **详细排查**：参考 `references/troubleshooting.md` 获取更多诊断指引。

## 脚本职责（按执行顺序）
- `scripts/compile_rules.py`：校验并编译 `rules.json`，输出 `compiled_rules.json`（含规则摘要）。
- `scripts/parse_excel.py`：解析输入 Excel/CSV，按规则做列投影只读取必要列，行数据按分块写入 `_row_store/*.jsonl`，输出 `ingest_manifest.json`。
- `scripts/validate_local.py`：执行单表/单列规则（schema/range/row），输出 `_stages/local_issues.json`。
- `scripts/validate_relations.py`：执行跨表关联规则，输出 `_stages/relation_issues.json`。
- `scripts/validate_global.py`：执行全局规则完整性检查，输出 `_stages/global_issues.json`。
- `scripts/render_report.py`：合并问题并渲染 `result.json / issues.csv / report.html`。
- `scripts/run_validator.py`：唯一端到端入口，负责串联全流程与失败恢复。

## 固定输出
```
<out>/
├── result.json          # 完整结构化结果（含 issues + 统计）
├── issues.csv           # 表格化问题清单
├── report.html          # 交互式 HTML 报告（筛选/排序/分页）
├── run_state.json       # 运行状态（支持断点恢复）
├── compiled_rules.json  # 编译后规则（含摘要统计）
├── ingest_manifest.json # 解析清单（含行数据索引）
├── _run.log             # 执行日志（所有输出自动写入，解决终端捕获问题）
├── _scan.json           # 输入文件元数据（--scan 模式输出）
├── _stages/             # 中间校验结果
│   ├── local_issues.json
│   ├── relation_issues.json
│   └── global_issues.json
└── _row_store/          # 行数据 JSONL 分块存储
```

## 当前已实现校验
完整字段定义参考 `references/rule_schema.md`。

- `datasets`：文件/工作表存在性检查，支持 `file`/`file_pattern`/`sha256`/`file_path` 消歧
- `schema_rules`：列存在性、`required`/`string`/`numeric`/`min_digits`/`increasing`/`unique`/`date`/`datetime_format`/`max_length`/`min_length`/`regex`/`enum`/`positive`/`non_negative`/`conditional_required`
- `range_rules`：数值/日期范围（`min`/`max`，开闭区间，可空控制）
- `row_rules`：行表达式断言（`when`/`assert`/`branches`），30+ 内置函数
- `aggregate_rules`：聚合校验（`sum`/`count`/`avg`/`min`/`max`，支持 `group_by`）
- `relation_rules`：外键（`fk_exists`）、集合一致性（`set_equal`）、基数约束（`one_to_one`/`one_to_many`/`many_to_many`）
- `global`：`rule_id` 跨规则组重复检查
- 报告：`result.json`（结构化）+ `issues.csv`（表格）+ `report.html`（交互式，筛选/排序/分页/搜索）

## 当前未完整实现
- `row_rules` 的结构化 then/else 动作（当前以表达式断言为主）。

## 使用文档
- 项目级使用文档在：`/docs/excel-config-validator-usage.md`
