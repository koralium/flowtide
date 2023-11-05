---
sidebar_position: 4
---

# String Functions

## Concat

[Substrait definition](https://substrait.io/extensions/functions_string/#concat)

Concatinates two or more string values together.

This function tries and convert non string values into the string type, example:

| Input     | Type      | Output    |
| --------- | --------- | --------- |
| 'hello'   | String    | 'hello'   |
| 13        | Int       | '13'      |
| 13.4      | Float     | '13.4'    |
| true      | Bool      | 'true'    |

If any argument is null, the return value will always be null.

### SQL Usage

```sql
SELECT c1 || ' hello ' || c2 FROM ... 
```

## Lower

[Substrait definition](https://substrait.io/extensions/functions_string/#lower)

Returns the input string in all lowercase characters. If any other type than string is entered, the function will return 'null'.

### SQL Usage

```sql
SELECT lower(c1) FROM ... 
```

## Upper

[Substrait definition](https://substrait.io/extensions/functions_string/#upper)

Returns the input string in all uppercase characters. If any other type than string is entered, the function will return 'null'.

### SQL Usage

```sql
SELECT upper(c1) FROM ... 
```