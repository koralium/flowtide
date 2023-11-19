---
sidebar_position: 3
---

# Specialized Expressions

## Nested Type Constructor Expressions

### Map

Allows the creation of a *map* object type. A map is a typical 'json' object with property names and values.
The map function consists of a list of key value pairs.

#### SQL Usage

The SQL function expects an even number of arguments, the first argument is the key and the second the value for the first key value pair.
The third argument is the second pairs key, etc.

```sql
SELECT map('keyvalue', col1) FROM ...
SELECT map(col2, col1) FROM ...
```

The keys will be converted into string. A null value will result in 'null' as the key.

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