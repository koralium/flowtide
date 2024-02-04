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

## Trim

[Substrait definition](https://substrait.io/extensions/functions_string/#trim)

Remove whitespaces from both sides of a string

### SQL Usage

```sql
SELECT trim(c1) FROM ... 
```

## LTrim

[Substrait definition](https://substrait.io/extensions/functions_string/#ltrim)

Remove whitespaces from the start of a string

### SQL Usage

```sql
SELECT ltrim(c1) FROM ... 
```

## RTrim

[Substrait definition](https://substrait.io/extensions/functions_string/#rtrim)

Remove whitespaces from the end of a string

### SQL Usage

```sql
SELECT rtrim(c1) FROM ... 
```

## To String

*No substrait definition exists for this function*

Converts different types to a string type.

Example output:

| Input     | Type      | Output    |
| --------- | --------- | --------- |
| 'hello'   | String    | 'hello'   |
| 13        | Int       | '13'      |
| 13.4      | Float     | '13.4'    |
| true      | Bool      | 'true'    |

### SQL Usage

```sql
SELECT to_string(c1) FROM ... 
```

## Starts with

[Substrait definition](https://substrait.io/extensions/functions_string/#starts_with) 

Returns true or false if a string starts with another string.
If the data type of either argument is not string, false will be returned.

### SQL Usage

```sql
SELECT starts_with(c1, 'text') FROM ... 
```

## Substring

[Substrait definition](https://substrait.io/extensions/functions_string/#substring) 

Returns a substring where the first argument is the input.
The second argument is the start index, and an optional third argument is the length of the substring.

### SQL Usage

```sql
SELECT substring(c1, 1) FROM ...
SELECT substring(c1, 1, 5) FROM ...
```