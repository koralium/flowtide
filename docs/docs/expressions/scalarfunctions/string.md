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

## Like

[Substrait definition](https://substrait.io/extensions/functions_string/#like) 

Implements sql like expression. This follows SQL Servers implementation of like:
[SQL Server Like](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/like-transact-sql?view=sql-server-ver16).

### SQL Usage

```sql
... WHERE c1 LIKE '%te' -- starts with
... WHERE c1 LIKE 'te%' -- ends with
... WHERE c1 LIKE '%te%' -- contains
... WHERE c1 LIKE '_te' -- any character
... WHERE c1 LIKE '!_te' ESCAPE '!' -- set escape character, _ is escaped.
... WHERE c1 LIKE '[ab]te' -- match character a or b.
```

## Replace

[Substrait definition](https://substrait.io/extensions/functions_string/#replace) 

Replaces all occurences of the substring defined in the second variable with the value defined in the third argument for the string in the first argument.

### SQL Usage

```sql
--- Replaces the word 'hello' with the word 'world' in the string c1. 
SELECT replace(c1, 'hello', 'world') FROM ...
```

## String Base64 Encode

Accepts a string as a parameter and will return the base64 encoding as a string.

### SQL Usage

```sql
SELECT string_base64_encode(c1) FROM ...
```

## String Base64 Decode

Accepts a string in base64 encoding and will return a decoded string.

### SQL Usage

```sql
SELECT string_base64_decode(c1) FROM ...
```

## Char Length

[Substrait definition](https://substrait.io/extensions/functions_string/#char_length)

Returns the number of characters in a string, if the input is not a string or null, a null value is returned.

### SQL Usage

```sql
SELECT LEN(c1) FROM ...
```

## Strpos

[Substrait definition](https://substrait.io/extensions/functions_string/#strpos)

Finds the index of a substring in another string. This function follows the substrait implementation and returns
index 1 for the first character.

### SQL Usage

```sql
SELECT strpos(c1, 'abc') FROM ...
```

## String split

[Substrait definition](https://substrait.io/extensions/functions_string/#string_split)

Splits a string into a collection of substrings based on the specified delimiter character. 

_If the provided delimiter character is null, the original string will be returned as the only element in the resulting collection._

_If the provided delimiter character is not a string, null will be returned._

### SQL Usage

```sql
SELECT string_split('a b', ' ') ...
```

## Regexp string split

[Substrait definition](https://substrait.io/extensions/functions_string/#regexp_string_split)

Splits a string into a collection of substrings based on the specified pattern.

_If any of the arguments is not string, null will be returned._

### SQL Usage

```sql
SELECT regexp_string_split('a b', '\s') ...
```

## To Json

This function does not have a substrait definition.

Converts an object into json stored as a string.

### SQL Usage

```sql
SELECT to_json(column1) ...
```

## From Json

This function does not have a substrait definition.

Converts a JSON string to flowtide data objects.

### SQL Usage

```sql
SELECT from_json(myjsoncolumn) ...
```