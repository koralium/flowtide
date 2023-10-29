---
sidebar_position: 3
---

# Comparison Functions

## Equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#equal)

Compares equality of two values.
If the two values have different types they are not considered equal, so a float with value 1 will not equal an integer with value 1.

### SQL Usage

```sql
... WHERE c1 = 'hello'
```

## Not equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#not_equal)

Checks two values for non equality.
Different types will immedietly return true, that the values are not equal.

### SQL Usage

```sql
... WHERE c1 != 'hello'
```

## Greater than

[Substrait definition](https://substrait.io/extensions/functions_comparison/#gt)

Checks if the left value is greater than the right value.

### SQL Usage

```sql
... WHERE c1 > 1
```

## Greater than or equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#gte)

Checks if the left value is greater than or equal to the right value.

### SQL Usage

```sql
... WHERE c1 >= 1
```

## Less than

[Substrait definition](https://substrait.io/extensions/functions_comparison/#lt)

Checks if the left value is less than the right value.

### SQL Usage

```sql
... WHERE c1 < 1
```

## Less than or equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#lte)

Checks if the left value is less than or equal to the right value.

### SQL Usage

```sql
... WHERE c1 <= 1
```

## Is not null

[Substrait definition](https://substrait.io/extensions/functions_comparison/#is_not_null)

Checks if a single argument is not equal to null.

### SQL Usage

```sql
... WHERE c1 is not null
```

## Coalesce

[Substrait definition](https://substrait.io/extensions/functions_comparison/#coalesce)

Returns the first value, left to right that is not equal to null. If all values are null, a null value is returned.

### SQL Usage

```sql
SELECT coalesce(column1, column2) FROM ...
```

## Is Infinite

Checks if a numeric value is positive or negative infinite. If the value is NaN (0 / 0), or another type, it returns false.

### SQL USage

```sql
SELECT is_infinite(column1) FROM ...
```