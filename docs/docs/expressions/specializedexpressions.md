---
sidebar_position: 3
---

# Specialized Expressions

## If Expression

[Substrait definition](https://substrait.io/expressions/specialized_record_expressions/#if-expression)

An if statement, or in SQL language a case statement.

### SQL Usage

```sql
SELECT
    CASE
        WHEN c1 = 'hello' THEN 1
        WHEN c1 = 'world' THEN 2
        ELSE 3
    END
FROM ...
```

## Or List Expression

[Substrait definition](https://substrait.io/expressions/specialized_record_expressions/#or-list-equality-expression)

Checks if a value is equal to any value in a list. This uses Kleene logic for equality.

### SQL Usage

```sql
... WHERE column1 IN (1, 5, 17)
```