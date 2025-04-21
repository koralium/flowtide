---
sidebar_position: 2
---

# Arithmetic Functions

## Sum

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sum)

Calculates the sum of numeric values, if there are no rows a NULL value is returned.
if a value is non numeric such as a string or null, those values are ignored.

### SQL Usage

```sql
SELECT sum(column1) FROM ...
```

## Sum0

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sum0)

Calculates the sum of numeric values, if there are no rows a 0 value is returned.
if a value is non numeric such as a string or null, those values are ignored.

### SQL Usage

```sql
SELECT sum0(column1) FROM ...
```

## Min

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#min)

Returns the minimum value in the result. If there are no rows a NULL value is returned.
MIN ignores any null input values.

### SQL Usage

```sql
SELECT min(column1) FROM ...
```

## Max

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#max)

Returns the maximum value in the result. If there are no rows a NULL value is returned.
MAX ignores any null input values.

### SQL Usage

```sql
SELECT max(column1) FROM ...
```

## Min By

*This function does not have a substrait definition.*

Returns the value of `x` associated with the minimum value of `y`. If there are no rows, a `NULL` value is returned. `MIN_BY` ignores any rows where `y` is `NULL`.

### SQL Usage

```sql
SELECT min_by(x, y) FROM ...
```

## Max By

*This function does not have a substrait definition.*

Returns the value of `x` associated with the maximum value of `y`. If there are no rows, a `NULL` value is returned. `MAX_BY` ignores any rows where `y` is `NULL`.

### SQL Usage

```sql
SELECT max_by(x, y) FROM ...
```




