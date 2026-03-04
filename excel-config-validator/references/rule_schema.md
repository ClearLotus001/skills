# 规则结构说明（rules.json）

## 目标

用确定性、可机器解析的规则描述 Excel/CSV 配置校验逻辑，确保结果可复现。

## 顶层结构

```json
{
  "metadata": {},
  "datasets": {},
  "schema_rules": [],
  "range_rules": [],
  "row_rules": [],
  "relation_rules": [],
  "aggregate_rules": [],
  "global_rules": [],
  "rule_sets": {},
  "execution": {},
  "output": {}
}
```

## 1. `datasets`

把逻辑数据集映射到物理文件与工作表。

```json
{
  "datasets": {
    "users": { "file": "users.xlsx", "sheet": "Sheet1" },
    "orders": { "file_pattern": "orders_*.csv", "sheet": "_csv_" }
  }
}
```

可选消歧字段：
- `sha256`：文件指纹
- `file_path`：相对或绝对路径

## 2. `schema_rules`

字段存在性、类型、格式、唯一性等规则。

### 支持的 `check` 类型

- `required`
- `string`
- `numeric`
- `min_digits`
- `increasing`
- `unique`
- `date`
- `datetime_format`
- `max_length`
- `min_length`
- `regex`
- `enum` / `whitelist`
- `positive`
- `non_negative`
- `conditional_required`

### 示例

```json
{
  "rule_id": "USR_ID_VALID",
  "dataset": "users",
  "column": "user_id",
  "checks": [
    { "type": "required" },
    { "type": "numeric" },
    { "type": "unique" }
  ],
  "severity": "error"
}
```

## 3. `range_rules`

用于数字或日期范围校验。

```json
{
  "rule_id": "AGE_RANGE",
  "dataset": "users",
  "column": "age",
  "min": 18,
  "max": 120,
  "include_min": true,
  "include_max": true,
  "allow_empty": true,
  "severity": "error"
}
```

可选字段：
- `value_type`：`number` 或 `date`（不填时自动推断）
- `include_min` / `include_max`
- `allow_empty`

## 4. `row_rules`

按行执行表达式断言，支持条件与分支。

```json
{
  "rule_id": "DISCOUNT_VALID",
  "dataset": "orders",
  "when": "exists('discount')",
  "assert": "num('discount') >= 0 and num('discount') <= 100",
  "message": "折扣必须在 0 到 100 之间",
  "severity": "error"
}
```

支持 `branches` + `else_assert` 分支模式（与 `when/assert` 互斥）。

### 常用内置函数

- 值访问：`value` / `text` / `num` / `intv` / `empty` / `exists` / `match`
- 日期：`date_val` / `days_between` / `days_since` / `today` / `year` / `month` / `day`
- 字符串：`strip` / `lower` / `upper` / `contains` / `starts_with` / `ends_with`
- 跨行：`prev_value` / `prev_text` / `prev_num`
- 多列：`sum_cols` / `coalesce` / `in_list`

## 5. `relation_rules`

跨数据集关联校验。

```json
{
  "rule_id": "FK_ORDER_USER",
  "source_dataset": "orders",
  "target_dataset": "users",
  "source_key": "user_id",
  "target_key": "user_id",
  "mode": "fk_exists",
  "severity": "error"
}
```

### `mode`（仅支持标准值）

- `fk_exists`
- `set_equal`
- `one_to_one`
- `one_to_many`
- `many_to_many`

## 6. `aggregate_rules`

先聚合再断言。

```json
{
  "rule_id": "AGG_TOTAL",
  "dataset": "orders",
  "column": "amount",
  "function": "sum",
  "assert": "result > 0",
  "message": "总金额必须大于 0",
  "severity": "error"
}
```

支持函数：
- `sum`
- `count`
- `avg`
- `min`
- `max`
- `distinct_count`

## 7. `rule_sets`

按场景分组执行规则。

```json
{
  "rule_sets": {
    "critical": ["USR_ID_VALID", "FK_ORDER_USER"],
    "full": ["USR_ID_VALID", "FK_ORDER_USER", "AGG_TOTAL"]
  }
}
```

执行方式：

```bash
python scripts/run_validator.py --rule-set critical
```

## 8. 通用字段

所有规则都建议包含：
- `rule_id`：唯一标识
- `severity`：`error` / `warn` / `info`
- `enabled`：默认 `true`
- `message`：可选自定义提示
