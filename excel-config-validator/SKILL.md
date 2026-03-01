---
name: excel-config-validator
description: 用自然语言描述 Excel/CSV 配置校验需求，自动转为规则并执行校验，输出 JSON/CSV/Markdown/HTML 报告。适用于多文件、多 Sheet、跨文件关联的确定性检查。
---

# excel-config-validator

## 何时使用
- 用户提到"校验"、"检查"、"验证"、"validate"、"check" 配合 Excel/CSV/配置表等关键词。
- 用户提供 Excel/CSV 文件并要求检查数据质量、格式合规、一致性。
- 用户需要生成配置校验报告（JSON/CSV/Markdown/HTML）。
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
  [--resume]
```

说明：
- 默认开启严格解析（`fail_on_parser_warning=true`），如需放宽可传 `--allow-parser-warning`。
- 默认 `data_only=True`（读取公式计算值）；如需读取公式文本可传 `--keep-formula`。
- 校验引擎采用 chunk 流式处理，大文件不会导致内存溢出。
- 单条规则异常不会中断整体流程，异常会被记录在 issues 中并继续执行。

## 脚本职责（按执行顺序）
- `scripts/compile_rules.py`：校验并编译 `rules.json`，输出 `compiled_rules.json`（含规则摘要）。
- `scripts/parse_excel.py`：解析输入 Excel/CSV，按规则做列投影只读取必要列，行数据按分块写入 `_row_store/*.jsonl`，输出 `ingest_manifest.json`。
- `scripts/validate_local.py`：执行单表/单列规则（schema/range/row），输出 `_stages/local_issues.json`。
- `scripts/validate_relations.py`：执行跨表关联规则，输出 `_stages/relation_issues.json`。
- `scripts/validate_global.py`：执行全局规则完整性检查，输出 `_stages/global_issues.json`。
- `scripts/render_report.py`：合并问题并渲染 `result.json / issues.csv / report.md / report.html`。
- `scripts/run_validator.py`：唯一端到端入口，负责串联全流程与失败恢复。

## 固定输出
```
<out>/
├── result.json          # 完整结构化结果（含 issues + 统计）
├── issues.csv           # 表格化问题清单
├── report.md            # Markdown 摘要报告
├── report.html          # 交互式 HTML 报告（筛选/排序/分页）
├── run_state.json       # 运行状态（支持断点恢复）
├── compiled_rules.json  # 编译后规则（含摘要统计）
├── ingest_manifest.json # 解析清单（含行数据索引）
├── _stages/             # 中间校验结果
│   ├── local_issues.json
│   ├── relation_issues.json
│   └── global_issues.json
└── _row_store/          # 行数据 JSONL 分块存储
```

## 当前已实现校验
- `datasets`：文件/工作表存在性检查（支持 `file` 与 `file_pattern`）。
- `schema_rules`：
  - 列存在性检查（缺列）
  - 行级检查：`required`、`string`、`numeric`、`min_digits`、`increasing`、`unique`
  - 时间类检查：`date`、`datetime_format`
  - 文本检查：`max_length`、`min_length`、`regex`
  - 枚举检查：`enum` / `whitelist`（支持 `case_insensitive`）
  - 数值检查：`positive`、`non_negative`
  - 条件检查：`conditional_required`（when 表达式触发）
- `range_rules`：
  - 数值范围与日期/时间范围检查（`min`/`max`）
  - 开闭区间控制（`include_min`/`include_max`）
  - 可空控制（`allow_empty`）
- `row_rules`：
  - 行表达式断言（`expression`/`assert`）
  - 条件触发（`when`）
  - 内置函数：`value/text/num/intv/empty/exists/match`
- `relation_rules`：
  - 数据集/文件/工作表/键列存在性
  - 外键存在性（`fk_exists`）
  - 集合一致性（`set_equal`）
- `global`：`rule_id` 跨规则组重复检查。
- 报告：
  - `result.json` 含严重级别/类别/规则统计与分组摘要
  - `report.html` 支持明细表/按类别分组双视图、筛选、排序、分页、搜索

## 当前未完整实现
- 高阶关系基数约束（1:1、1:N、N:N）与聚合一致性规则。
- `row_rules` 的结构化 then/else 动作（当前以表达式断言为主）。

## 使用文档
- 项目级使用文档在：`/docs/excel-config-validator-usage.md`
