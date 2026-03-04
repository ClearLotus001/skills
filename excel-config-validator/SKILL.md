---
name: excel-config-validator
description: 当用户要求对 Excel/CSV 配置做规则校验时必须触发本技能，包括 validate/check/verify、数据质量审计、跨文件/跨工作表一致性检查、将自然语言需求生成 rules.json 并执行校验，以及输出 result.json/issues.csv/report.html。即使用户只说“帮我检查配置表是否合规”也应触发；仅在任务是浏览/编辑样式、公式建模或开放式分析且不涉及规则校验时不触发。
---

# excel-config-validator

面向 Excel/CSV 配置文件的自动化规则校验技能。支持将自然语言需求转为 `rules.json`，并通过统一入口脚本完成扫描、校验与报告输出。

## 适用场景

当用户提出以下需求时使用本技能：
- 校验 Excel/CSV 配置是否合规（`validate` / `check` / `verify`）
- 按规则检查字段完整性、格式、唯一性、范围
- 需要聚合校验（sum/count/avg 等）或全局一致性校验（如 rule_id 唯一性）
- 校验跨文件/跨工作表关联关系（外键、集合一致、一对多等）
- 输出结构化报告（`result.json` / `issues.csv` / `report.html`）

以下场景不适用：
- 仅浏览表格内容（不涉及规则校验）
- 开放式探索分析或可视化
- VBA 开发、公式建模等非规则校验任务

## 快速流程

1. 让用户提供输入文件路径与校验需求。
2. 先扫描结构：
   `python scripts/run_validator.py --inputs <path> --out <output> --scan`
3. 根据扫描结果生成 `rules.json`。
4. 执行校验：
   `python scripts/run_validator.py --inputs <path> --rules <rules.json> --out <output>`
5. 读取 `<output>/result.json` 与 `<output>/_run.log`，按严重级别汇总结果。

## 执行约束（必须遵守）

1. 统一入口：必须使用 `scripts/run_validator.py` 执行，不要手工拼接阶段结果。
2. 结果不可猜测：必须基于输出文件（`result.json`、`_run.log`）给出结论。
3. 先扫后校：未知输入结构时，先执行 `--scan` 再生成规则。
4. 优先少命令：尽量一次扫描 + 一次校验完成。
5. 日志优先：终端输出可能不完整，以 `<out>/_run.log` 为准。
6. 前置信息不足时不执行：缺少输入路径或规则来源时，先向用户索取信息，再执行。
7. 未提供输出目录时不阻塞：允许省略 `--out`，脚本会自动创建临时目录，必须在回复中返回该绝对路径。

## 标准命令

```bash
python scripts/run_validator.py \
  --inputs <path> \
  --rules <rules.json> \
  [--out <output>] \
  [--rule-set <subset>] \
  [--max-errors <limit>] \
  [--allow-parser-warning] \
  [--skip-xlsx-package-check] \
  [--chunk-size <size>] \
  [--keep-intermediate] \
  [--resume] \
  [--log <custom_log_path>]
```

## 当前解析主链路

- xlsx/xlsm 默认执行包结构预检（ZIP 完整性、必需部件、关键 XML、关系目标）。
- Excel 统一按 `data_only=True` 读取。
- 发现公式单元格时，直接使用本技能包内置的 LibreOffice 公式重算流程。
- 公式重算失败或重算后仍存在 Excel 错误码时，会产生解析告警。
- 如需跳过包结构预检，可显式使用 `--skip-xlsx-package-check`。

## 公式文件引导策略（最佳实践）

1. 默认自动处理
   - 只要执行 `run_validator.py`，公式重算会在解析阶段自动触发。
   - 不要要求用户额外安装或调用其他技能脚本。
2. 失败分流
   - 出现“未找到 soffice”：引导用户安装 LibreOffice 或配置 `soffice` 到 PATH。
   - 出现“公式重算后发现 X 个 Excel 错误”：引导用户先修复公式错误再重试。
3. 严格与容错策略
   - 默认严格模式：解析告警会终止流程。
   - 用户接受告警继续执行时，显式增加 `--allow-parser-warning`。

## 前置检查与回复模板（最佳实践）

1. 缺少输入路径（不执行）
   - 触发条件：用户未提供 Excel/CSV 文件或目录路径。
   - 回复模板：`请先提供 Excel/CSV 输入路径（文件或目录）。当前未提供前置信息，暂不执行校验。`
2. 未提供输出目录（可执行）
   - 触发条件：用户提供了可执行输入，但未提供 `--out`。
   - 动作：直接执行，使用脚本自动创建的临时输出目录。
   - 回复模板：`未指定输出目录，已自动使用临时输出目录：<absolute_path>`
3. 校验完成
   - 回复模板：`校验完成，输出目录：<absolute_path>。可先查看 <absolute_path>/_run.log 与 <absolute_path>/result.json。`
4. 缺少 `soffice`
   - 触发条件：解析告警包含“未找到 soffice”。
   - 回复模板：`检测到公式单元格，但当前环境未找到 soffice。请安装 LibreOffice 或将 soffice 加入 PATH 后重试。`
5. 重算后仍有公式错误
   - 触发条件：解析告警包含“公式重算后发现 X 个 Excel 错误”。
   - 回复模板：`公式重算后仍发现 Excel 错误（如 #REF!/#DIV/0!）。请先修复公式并保存文件，再重新执行校验。`
6. 用户接受告警继续
   - 触发条件：用户明确允许带告警继续。
   - 回复模板：`将按允许解析告警模式继续执行（已启用 --allow-parser-warning）。`
7. 包结构预检告警
   - 触发条件：解析告警包含“包结构预检”。
   - 回复模板：`检测到 Excel 文件包结构异常（如关键部件缺失或关系目标断链）。请先修复源文件，或在确认风险后使用 --skip-xlsx-package-check 跳过预检。`

## 结果读取顺序

1. 读取 `<out>/_run.log` 确认执行状态。
2. 读取 `<out>/result.json` 获取总览和 issue 明细。
3. 按 `error -> warn -> info` 顺序汇报问题。
4. 使用 `summary.top_rules` 和 `summary.by_file_sheet` 定位重点问题。
5. 引导用户打开 `<out>/report.html` 查看交互式报告（含解析告警摘要）。

## 输出结构

最终输出（默认保留）：

```text
<out>/
├── result.json
├── issues.csv
├── report.html
└── _run.log
```

中间产物（默认自动清理，`--keep-intermediate` 可保留）：

```text
<out>/
├── _scan.json
├── run_state.json
├── compiled_rules.json
├── ingest_manifest.json
├── _stages/
└── _row_store/
```

## 规则编写建议

- 每条规则必须有唯一 `rule_id`。
- `severity` 推荐：`error` / `warn` / `info`。
- 关联模式仅使用标准值：`fk_exists` / `set_equal` / `one_to_one` / `one_to_many` / `many_to_many`。
- 有歧义的自然语言需求，先向用户确认再落规则。

## 参考资料

- 使用文档：`/docs/excel-config-validator-usage.md`
- 规则结构：`references/rule_schema.md`
- 关联规则示例：`references/relation_patterns.md`
- 故障排查：`references/troubleshooting.md`
- 边界场景：`references/edge_cases.md`
