# 规则结构（rules.json）

## 1. 目的
用于定义 Excel/CSV 配置校验的确定性规则。
规则必须显式、可机器解析、可复现。

## 2. 顶层结构
```json
{
  "metadata": {},
  "datasets": {},
  "normalization": {},
  "schema_rules": [],
  "range_rules": [],
  "row_rules": [],
  "relation_rules": [],
  "global_rules": [],
  "rule_sets": {},
  "execution": {},
  "output": {}
}
```

## 3. datasets
将逻辑数据集映射到物理文件与 Sheet。

示例：
```json
{
  "users": {
    "file_pattern": "user*.xlsx",
    "sheet": "Users"
  },
  "orders": {
    "file_pattern": "order*.xlsx",
    "sheet": "Orders"
  }
}
```

## 4. schema_rules
用于列存在性与行级内容校验。

当前已支持的 `check`：
- `required` — 字段不能为空
- `string` — 必须为字符串类型
- `numeric` — 必须为数字
- `min_digits` — 至少 N 位数字（参数：`min_digits`）
- `increasing` — 按行严格递增（跨 chunk 保持状态）
- `unique` — 列值唯一（跨 chunk 保持状态）
- `date` — 必须为日期格式
- `datetime_format` — 必须符合指定时间格式（参数：`format`）
- `max_length` — 文本长度不超过 N（参数：`max_length`）
- `min_length` — 文本长度不低于 N（参数：`min_length`）
- `regex` — 必须匹配正则表达式（参数：`pattern`）
- `enum` / `whitelist` — 值必须在允许列表中（参数：`values`，可选 `case_insensitive`）
- `positive` — 数值必须大于 0
- `non_negative` — 数值必须大于等于 0
- `conditional_required` — 满足条件时不能为空（参数：`when` 表达式）

示例：
```json
{
  "rule_id": "USR_REQUIRED_ID",
  "dataset": "users",
  "column": "user_id",
  "checks": [
    { "type": "required" },
    { "type": "numeric" },
    { "type": "min_digits", "min_digits": 4 },
    { "type": "increasing" }
  ],
  "severity": "error"
}
```

时间格式示例：
```json
{
  "rule_id": "TIME_FMT",
  "dataset": "users",
  "column": "create_time",
  "check": "datetime_format",
  "format": "YYYY-MM-DD HH:MM:SS",
  "severity": "error"
}
```

枚举校验示例：
```json
{
  "rule_id": "STATUS_ENUM",
  "dataset": "orders",
  "column": "status",
  "check": "enum",
  "values": ["pending", "processing", "completed", "cancelled"],
  "severity": "error"
}
```

唯一性校验示例：
```json
{
  "rule_id": "EMAIL_UNIQUE",
  "dataset": "users",
  "column": "email",
  "check": "unique",
  "severity": "error"
}
```

条件必填示例：
```json
{
  "rule_id": "ADDR_REQUIRED_IF_SHIPPED",
  "dataset": "orders",
  "column": "shipping_address",
  "check": "conditional_required",
  "when": "text('status') == 'completed'",
  "severity": "error"
}
```

## 5. range_rules
用于数值/日期范围与阈值校验。

示例：
```json
{
  "rule_id": "ORDER_DATE_RANGE",
  "dataset": "orders",
  "column": "order_date",
  "min": "2024-01-01",
  "max": "2026-12-31",
  "severity": "error"
}
```

可选字段：
- `value_type`：`number` / `date`（可省略，系统按样本自动判断）
- `include_min`：默认 `true`
- `include_max`：默认 `true`
- `allow_empty`：默认 `true`

## 6. row_rules
用于行内条件校验与跨列一致性校验。

当前支持表达式断言：
```json
{
  "rule_id": "ROW_ASSERT_EXAMPLE",
  "dataset": "orders",
  "when": "num('amount') is not None and num('amount') > 0",
  "expression": "exists('currency') and match('^[A-Z]{3}$', 'currency')",
  "message": "当 amount>0 时，currency 必须是三位大写货币码",
  "severity": "error"
}
```

表达式可用内置函数：
- `value(column)`
- `text(column)`
- `num(column)`
- `intv(column)`
- `empty(column)`
- `exists(column)`
- `match(pattern, data_or_column)`

## 7. relation_rules
用于跨 Sheet/跨文件主外键与基数约束校验。

当前已支持：
- `mode = "fk_exists"`（默认）：源键值必须存在于目标键集合。
- `mode = "set_equal"`：源键集合与目标键集合需一致。

示例：
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

## 8. global_rules
用于跨数据集唯一性与聚合一致性校验。

## 9. rule_sets
用于按场景组合规则集。

示例：
```json
{
  "strict": ["USR_REQUIRED_ID", "FK_ORDER_USER"],
  "pre-release": ["USR_REQUIRED_ID"],
  "regression": ["FK_ORDER_USER"]
}
```

