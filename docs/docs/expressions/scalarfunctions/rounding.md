---
sidebar_position: 5
---

# Rounding Functions

## Ceiling

[Substrait definition](https://substrait.io/extensions/functions_rounding/#ceil)

Rounds a number up to its closest integer.

Its output type will always be an integer, if a non numeric type is passed in, the function will return null.

### SQL Usage

```sql
SELECT ceiling(column1) FROM ...
```
or
```sql
SELECT ceil(column1) FROM ...
```

## Floor

[Substrait definition](https://substrait.io/extensions/functions_rounding/#floor)

Rounds a number down to its closest integer.

Its output type will always be an integer, if a non numeric type is passed in, the function will return null.

### SQL Usage

```sql
SELECT floor(column1) FROM ...
```

## Round

[Substrait definition](https://substrait.io/extensions/functions_rounding/#round)

Rounds a number to its closest integer.

Its output type will always be an integer, if a non numeric type is passed in, the function will return null.

### SQL Usage

```sql
SELECT round(column1) FROM ...
```
