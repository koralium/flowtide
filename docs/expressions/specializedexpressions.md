---
sidebar_position: 5
---

# Specialized Expressions

## Cast Expression

The cast expression allows conversion between data types.
The supported data types at this point are:

* String
* Boolean
* Int
* Double
* Decimal

If it is not possible to cast a value, null will be returned.

### Cast to string

| Data Type     | Value         | Output    |
| ------------- | ------------- | --------- |
| Integer       | 3             | '3'       |
| Float         | 3.1           | '3.1'     |
| Decimal       | 3.1           | '3.1'     |
| Boolean       | True          | 'true'    |
| Boolean       | False         | 'false'   |

#### SQL Usage

```sql
CAST(column1 AS string)
```

### Cast to boolean

For numeric types, any value except 0 becomes true, and 0 becomes false.

| Data Type     | Value         | Output    |
| ------------- | ------------- | --------- |
| Integer       | 3             | true      |
| Integer       | 0             | false     |
| Float         | 3.1           | true      |
| Float         | 0.0           | false     |
| Decimal       | 3.1           | true      |
| Decimal       | 0.0           | false     |
| String        | 'true'        | true      |
| String        | 'false'       | false     |

#### SQL Usage

```sql
CAST(column1 AS boolean)
```

### Cast to integer

For any numeric type with decimals, the value will be floored.

| Data Type     | Value         | Output    |
| ------------- | ------------- | --------- |
| Float         | 3.1           | 3         |
| Decimal       | 3.1           | 3         |
| Boolean       | True          | 1         |
| Boolean       | False         | 0         |
| String        | '1'           | 1         |

#### SQL Usage

```sql
CAST(column1 AS int)
```

### Cast to double

| Data Type     | Value         | Output    |
| ------------- | ------------- | --------- |
| Int           | 3             | 3         |
| Decimal       | 3.1           | 3.1       |
| Boolean       | True          | 1.0       |
| Boolean       | False         | 0.0       |
| String        | '1.3'         | 1.3       |

#### SQL Usage

```sql
CAST(column1 AS double)
```

### Cast to decimal

| Data Type     | Value         | Output    |
| ------------- | ------------- | --------- |
| Int           | 3             | 3         |
| Float         | 3.1           | 3.1       |
| Boolean       | True          | 1.0       |
| Boolean       | False         | 0.0       |
| String        | '1.3'         | 1.3       |

#### SQL Usage

```sql
CAST(column1 AS decimal)
```

## Nested Type Constructor Expressions

### List

Allows the creation of a list object.

#### SQL Usage

```sql
SELECT list(col1, col2) FROM ...
```

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

### Named Struct

Allows the creation of a named struct object type. Struct is a fixed schema data type with a predefined set of named fields. Each field has a defined order, making it ideal for structured and predictable data.

There is a limit of a maximum of 127 different types in a union column, which means you cannot have endlessly different struct types in the same column. If dynamic properties are required, a map is a better choice.

The keys must be string literals when creating the struct.

#### SQL Usage

```sql
SELECT named_struct('key1', value1, 'key2', value2) FROM ...
```

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