---
name: excel-config-validator
description: 对 Excel/CSV 配置文件执行自动化规则校验。支持 validate/check/verify、数据质量审计、跨文件/跨工作表一致性检查，可将自然语言需求转为 rules.json 并生成 result.json、issues.csv、report.html 结构化报告。不适用于仅浏览样式、公式建模或不涉及规则校验的开放式分析。
---

# excel-config-validator

面向 Excel/CSV 配置文件的自动化规则校验技能。通过统一入口脚本完成扫描、校验与报告输出。

## 适用场景

触发本技能：
- 校验 Excel/CSV 配置是否合规（`validate` / `check` / `verify`）
- 按规则检查字段完整性、格式、唯一性、范围
- 聚合校验（sum/count/avg 等）或全局一致性校验
- 跨文件/跨工作表关联校验（外键、集合一致、一对多等）
- 输出结构化报告（`result.json` / `issues.csv` / `report.html`）

不触发：
- 仅浏览表格内容（不涉及规则校验）
- 开放式探索分析或可视化
- VBA 开发、公式建模等非规则校验任务

## 依赖

```bash
pip install -r requirements.txt   # 核心依赖：openpyxl>=3.1.0
```

公式重算需要 LibreOffice（`soffice` 在 PATH 中可用）。

## 工作流

```
校验进度：
- [ ] 收集输入路径与校验需求
- [ ] 扫描输入结构（--scan）
- [ ] 根据扫描结果生成 rules.json
- [ ] 执行校验
- [ ] 读取结果并汇报
```

### 步骤详解

1. 让用户提供输入文件路径与校验需求。
2. 扫描结构：
   ```bash
   python scripts/run_validator.py --inputs <path> --out <output> --scan
   ```
3. 根据 `_scan.json` 生成 `rules.json`（参考 `references/rule_schema.md`）。
4. 执行校验：
   ```bash
   python scripts/run_validator.py --inputs <path> --rules <rules.json> --out <output>
   ```
5. 读取 `<output>/result.json` 与 `<output>/_run.log`，按 `error → warn → info` 汇报。

### 反馈循环

发现问题后按此循环迭代：

```
执行校验 → 检查 _run.log 和 result.json → 定位问题 → 修正规则或数据 → 重新执行
```

- 规则有误：修改 `rules.json` 后重新执行步骤 4。
- 数据有误：引导用户修复源文件后重新执行完整流程。

## 执行约束

1. **统一入口**：必须使用 `scripts/run_validator.py`，不手工拼接阶段结果。
2. **结果不可猜测**：必须基于 `result.json` 和 `_run.log` 给出结论。
3. **先扫后校**：未知输入结构时，先 `--scan` 再生成规则。
4. **日志优先**：终端输出可能不完整，以 `_run.log` 为准。
5. **前置信息不足不执行**：缺少输入路径时先向用户索取。

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

## 解析行为

- xlsx/xlsm 默认执行包结构预检（可用 `--skip-xlsx-package-check` 跳过）。
- Excel 统一按 `data_only=True` 读取。
- 检测到公式单元格时自动触发内置 LibreOffice 重算流程。
- 重算失败或仍有 Excel 错误码时产生解析告警（默认严格模式会终止流程）。

## 回复模板

| 场景 | 回复 |
|------|------|
| 缺少输入路径 | `请先提供 Excel/CSV 输入路径（文件或目录）。当前未提供前置信息，暂不执行校验。` |
| 自动临时目录 | `未指定输出目录，已自动使用临时输出目录：<absolute_path>` |
| 缺少 soffice | `检测到公式单元格，但当前环境未找到 soffice。请安装 LibreOffice 或将 soffice 加入 PATH 后重试。` |
| 公式错误 | `公式重算后仍发现 Excel 错误。请先修复公式并保存文件，再重新执行校验。` |
| 包结构异常 | `检测到 Excel 文件包结构异常。请先修复源文件，或使用 --skip-xlsx-package-check 跳过预检。` |
| 接受告警继续 | `将按允许解析告警模式继续执行（已启用 --allow-parser-warning）。` |
| 校验完成 | `校验完成，输出目录：<absolute_path>。可先查看 _run.log 与 result.json。` |

## 结果读取顺序

1. 读取 `_run.log` 确认执行状态。
2. 读取 `result.json` 获取总览和 issue 明细。
3. 按 `error → warn → info` 顺序汇报。
4. 用 `summary.top_rules` 和 `summary.by_file_sheet` 定位重点问题。
5. 引导用户打开 `report.html` 查看交互式报告。

## 输出结构

最终输出：

```
<out>/
├── result.json
├── issues.csv
├── report.html
└── _run.log
```

中间产物（默认自动清理，`--keep-intermediate` 可保留）：

```
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
- 关联模式仅支持：`fk_exists` / `set_equal` / `one_to_one` / `one_to_many` / `many_to_many`。
- 有歧义的自然语言需求，先向用户确认再落规则。

## 参考资料

- 规则结构：`references/rule_schema.md`
- 关联规则示例：`references/relation_patterns.md`
- 故障排查：`references/troubleshooting.md`
- 边界场景：`references/edge_cases.md`
