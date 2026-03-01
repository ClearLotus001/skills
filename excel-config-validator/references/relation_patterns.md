# 关联规则模式

## 1. 外键存在性（Foreign Key Exists）
用于“源记录必须引用有效目标记录”的场景。

示例：
```json
{
  "rule_id": "FK_ORDER_USER",
  "source_dataset": "orders",
  "target_dataset": "users",
  "source_key": "user_id",
  "target_key": "user_id",
  "severity": "error"
}
```

## 2. 一对一（One-to-One）
用于双方都必须唯一映射的场景。

## 3. 一对多（One-to-Many）
用于一个父记录可映射多个子记录的场景。

## 4. 版本一致性（Version Consistency）
用于多个数据集必须共享同一版本号或批次号的场景。

## 5. 常见失败信号
- 源数据集映射缺失
- 目标数据集映射缺失
- 键列缺失
- 键值为空
- 基数约束超限

