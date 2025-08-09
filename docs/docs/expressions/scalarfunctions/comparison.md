---
sidebar_position: 3
---

# Comparison Functions

## Equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#equal)

Compares equality of two values.
If the two values have different types they are not considered equal, so a float with value 1 will not equal an integer with value 1.
If any argument is null, the result is null.

### SQL Usage

```sql
... WHERE c1 = 'hello'
```

## Not equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#not_equal)

Checks two values for non equality.
Different types will immedietly return true, that the values are not equal.
If any argument is null, the result is null.

### SQL Usage

```sql
... WHERE c1 != 'hello'
```

## Greater than

[Substrait definition](https://substrait.io/extensions/functions_comparison/#gt)

Checks if the left value is greater than the right value.
If any argument is null, the result is null.

### SQL Usage

```sql
... WHERE c1 > 1
```

## Greater than or equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#gte)

Checks if the left value is greater than or equal to the right value.
If any argument is null, the result is null.

### SQL Usage

```sql
... WHERE c1 >= 1
```

## Less than

[Substrait definition](https://substrait.io/extensions/functions_comparison/#lt)

Checks if the left value is less than the right value.
If any argument is null, the result is null.

### SQL Usage

```sql
... WHERE c1 < 1
```

## Less than or equal

[Substrait definition](https://substrait.io/extensions/functions_comparison/#lte)

Checks if the left value is less than or equal to the right value.
If any argument is null, the result is null.

### SQL Usage

```sql
... WHERE c1 <= 1
```

## Between

[Substrait definition](https://substrait.io/extensions/functions_comparison/#between)

Checks if an expression is between two values.

### SQL Usage

```sql
... WHERE c1 BETWEEN 100 AND 200
```

## Is not null

[Substrait definition](https://substrait.io/extensions/functions_comparison/#is_not_null)

Checks if a single argument is not equal to null.

### SQL Usage

```sql
... WHERE c1 is not null
```

## Is Null

[Substrait definition](https://substrait.io/extensions/functions_comparison/#is_null)

Checks if a signle argument is null.

### SQL Usage

```sql
... WHERE c1 is null
```

## Coalesce

[Substrait definition](https://substrait.io/extensions/functions_comparison/#coalesce)

Returns the first value, left to right that is not equal to null. If all values are null, a null value is returned.

### SQL Usage

```sql
SELECT coalesce(column1, column2) FROM ...
```

## Is Infinite

[Substrait definition](https://substrait.io/extensions/functions_comparison/#is_infinite)

Checks if a numeric value is positive or negative infinite. If the value is NaN (0 / 0), or another type, it returns false.

### SQL Usage

```sql
SELECT is_infinite(column1) FROM ...
```

## Is Finite

[Substrait definition](https://substrait.io/extensions/functions_comparison/#is_finite)

Checks if a numeric value is not positive or negative infinite or NaN. If the value is not numeric it returns false.

### SQL Usage

```sql
SELECT is_finite(column1) FROM ...
```

## Is NaN

[Substrait definition](https://substrait.io/extensions/functions_comparison/#is_nan)

Checks if an exprssion is not a numeric value. A null value returns null as in the substrait definition.

### SQL Usage

```sql
SELECT is_nan(column1) FROM ...
```

## Greatest

[Substrait definition](https://substrait.io/extensions/functions_comparison/#greatest)

Returns the largest non-null value from a list of input expressions. If all values are null, the result is null. Follows the behavior defined in the Substrait specification.

### SQL Usage

```sql
SELECT greatest(column1, column2, column3) FROM ...
```