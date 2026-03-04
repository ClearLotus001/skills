# 关联规则模式示例

## 1. 外键存在（`fk_exists`）

适用场景：源表记录必须能在目标表中找到对应键。

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

检查点：
- `orders.user_id` 是否都存在于 `users.user_id`
- 是否存在空外键（取决于 `allow_source_empty`）

## 2. 集合一致（`set_equal`）

适用场景：两侧键集合必须完全相同。

```json
{
  "rule_id": "PRODUCT_SET_EQUAL",
  "source_dataset": "inventory",
  "target_dataset": "catalog",
  "source_key": "product_id",
  "target_key": "product_id",
  "mode": "set_equal",
  "severity": "error"
}
```

## 3. 一对一（`one_to_one`）

适用场景：双方键都唯一，且集合一致。

```json
{
  "rule_id": "EMP_BADGE_1TO1",
  "source_dataset": "employees",
  "target_dataset": "badges",
  "source_key": "badge_id",
  "target_key": "badge_id",
  "mode": "one_to_one",
  "severity": "error"
}
```

## 4. 一对多（`one_to_many`）

适用场景：目标键唯一，源键可重复，但每个源键都必须存在于目标侧。

```json
{
  "rule_id": "CUSTOMER_ORDER_1TOM",
  "source_dataset": "orders",
  "target_dataset": "customers",
  "source_key": "customer_id",
  "target_key": "customer_id",
  "mode": "one_to_many",
  "severity": "error"
}
```

## 5. 多对多（`many_to_many`）

适用场景：两侧都可重复，要求双向存在性。

```json
{
  "rule_id": "COURSE_ENROLLMENT_MTOM",
  "source_dataset": "enrollments",
  "target_dataset": "courses",
  "source_key": "course_id",
  "target_key": "course_id",
  "mode": "many_to_many",
  "severity": "error"
}
```

## 常见失败信号

- 数据集未定义：`source_dataset` 或 `target_dataset` 不在 `datasets` 中
- 键列缺失：`source_key` 或 `target_key` 在对应表头中不存在
- 键值为空：外键列出现空值且 `allow_source_empty=false`
- 关系约束不满足：如 `one_to_one` 出现重复键
