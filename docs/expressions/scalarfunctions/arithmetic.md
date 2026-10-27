---
sidebar_position: 1
---

# Arithmetic Functions

## Add

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#add)

Add takes two parameters and does an addition of the two values.

Add depends on the input types on what result it will give:

| Left type     | Right type    | Output    |
| ------------- | ------------- | --------- |
| Integer       | Integer       | Integer   |
| Integer       | Float         | Float     |
| Float         | Float         | Float     |
| Decimal       | Integer       | Decimal   |
| Decimal       | Float         | Decimal   |
| Decimal       | Decimal       | Decmial   |
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
| num numeric   | Decimal       | Null      |
| Non numeric   | Non numeric   | Null      |

Only numeric inputs will return a result, otherwise it will return null.

### SQL usage

In SQL the add function is called using the plus operator:

```sql
SELECT column1 + 13 FROM ...
```

## Subtract

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#subtract)

Subtract takes two parameters and does a subtraction of the two values.

Subtract depends on the input types on what result it will give:

| Left type     | Right type    | Output    |
| ------------- | ------------- | --------- |
| Integer       | Integer       | Integer   |
| Integer       | Float         | Float     |
| Float         | Float         | Float     |
| Decimal       | Integer       | Decimal   |
| Decimal       | Float         | Decimal   |
| Decimal       | Decimal       | Decmial   |
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
| num numeric   | Decimal       | Null      |
| Non numeric   | Non numeric   | Null      |

Only numeric inputs will return a result, otherwise it will return null.

### SQL usage

In SQL the subtract function is called using the minus operator:

```sql
SELECT column1 - 13 FROM ...
```

## Multiply

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#multiply)

Multipies two numbers.

Multiply depends on the input types on what result it will give:

| Left type     | Right type    | Output    |
| ------------- | ------------- | --------- |
| Integer       | Integer       | Integer   |
| Integer       | Float         | Float     |
| Float         | Float         | Float     |
| Decimal       | Integer       | Decimal   |
| Decimal       | Float         | Decimal   |
| Decimal       | Decimal       | Decmial   |
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
| num numeric   | Decimal       | Null      |
| Non numeric   | Non numeric   | Null      |

### SQL Usage

```sql
SELECT column1 * 3 FROM ...
```

## Divide

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#divide)

Divide two numbers.

Divide depends on the input types on what result it will give:

| Left type     | Right type    | Output    |
| ------------- | ------------- | --------- |
| Integer       | Integer       | Float     |
| Integer       | Float         | Float     |
| Float         | Float         | Float     |
| Decimal       | Integer       | Decimal   |
| Decimal       | Float         | Decimal   |
| Decimal       | Decimal       | Decmial   |
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
| num numeric   | Decimal       | Null      |
| Non numeric   | Non numeric   | Null      |

There are some special cases when dividing with zero:

* 0 / 0 -> this results in NaN.
*  PositiveNumber / 0 -> +Infinity
* NegativeNumber / 0 -> -Infinity

### SQL Usage

```sql
SELECT column1 / 3 FROM ...
```

## Negate

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#negate)

Negates a numeric value, example:

* 1 becomes -1
* -1 becomes 1
* 1.3 becomes -1.3

Non numeric values becomes 'null'.

### SQL Usage

```sql
SELECT -column1 FROM ...
```

## Modulo

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#modulus)

Calculate the remainder when dividing two numbers.

Modulo depends on the input types on what result it will give:

| Left type     | Right type    | Output    |
| ------------- | ------------- | --------- |
| Integer       | Integer       | Integer   |
| Integer       | Float         | Float     |
| Float         | Float         | Float     |
| Decimal       | Integer       | Decimal   |
| Decimal       | Float         | Decimal   |
| Decimal       | Decimal       | Decmial   |
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
| num numeric   | Decimal       | Null      |
| Non numeric   | Non numeric   | Null      |

Taking modulo between two integers where the divider is 0, the return will be type double and the value NaN.

### SQL Usage

```sql
SELECT column1 % 3 FROM ...
```

## Power

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#power)

Calculate the power with the first argument being the base and the other the exponent.

Power depends on the input types on what result it will give:

| Left type     | Right type    | Output    |
| ------------- | ------------- | --------- |
| Integer       | Integer       | Integer   |
| Integer       | Float         | Float     |
| Float         | Float         | Float     |
| Decimal       | Integer       | Decimal   |
| Decimal       | Float         | Decimal   |
| Decimal       | Decimal       | Decmial   |
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
| num numeric   | Decimal       | Null      |
| Non numeric   | Non numeric   | Null      |

### SQL Usage

```sql
SELECT power(column1, 2) FROM ...
```

## Sqrt

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sqrt)

Calculate the square root of a number.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Decimal   |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT sqrt(column1) FROM ...
```

## Exp

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#exp)

Calculates the constant e raised to the power of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT exp(column1) FROM ...
```

## Cos

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#cos)

Calculate the cosine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT cos(column1) FROM ...
```

## Sin

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sin)

Calculate the sine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT sin(column1) FROM ...
```

## Tan

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#tan)

Calculate the tangent of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT tan(column1) FROM ...
```

## Cosh

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#cosh)

Calculate the hyperbolic cosine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT cosh(column1) FROM ...
```

## Sinh

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sinh)

Calculate the hyperbolic sine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT sinh(column1) FROM ...
```

## Tanh

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#tanh)

Calculate the hyperbolic tangent of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT tanh(column1) FROM ...
```

## Acos

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#acos)

Calculate the arccosine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT acos(column1) FROM ...
```

## Asin

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#asin)

Calculate the arcsine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT asin(column1) FROM ...
```

## Atan

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#atan)

Calculate the arctangent of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT atan(column1) FROM ...
```

## Acosh

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#acosh)

Calculate the hyperbolic arccosine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT acosh(column1) FROM ...
```

## Asinh

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#asinh)

Calculate the hyperbolic arcsine of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT asinh(column1) FROM ...
```

## Atanh

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#atanh)

Calculate the hyperbolic arctangent of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT atanh(column1) FROM ...
```

## Atan2

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#atan2)

Calculate the arctangent of two input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT atan2(column1) FROM ...
```

## Radians

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#radians)

Convert the input value from degrees to radians.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT radians(column1) FROM ...
```

## Degrees

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#degrees)

Convert the input value from radians to degrees.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Float     |
| Float         | Float     |
| Decimal       | Float     |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT degrees(column1) FROM ...
```

## Abs

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#abs)

Calculate the absolute value of the input value.

Output types:

| Type          | Output    |
| ------------- | --------- |
| Integer       | Integer   |
| Float         | Float     |
| Decimal       | Decimal   |
| Non numeric   | Null      |

### SQL Usage

```sql
SELECT abs(column1) FROM ...
```

## Sign

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sign)

Get the sign of the input value.

Output:

| Type          | Output Type    |  Output range            |
| ------------- | -------------- | ------------------------ |
| Integer       | Integer        | [-1, 0, 1]               |
| Float         | Float          | [-1.0, 0.0, 1.0, NaN]    |
| Decimal       | Decimal        | [-1.0, 0.0, 1.0]         |
| Non numeric   | Null           | Null                     |

### SQL Usage

```sql
SELECT sign(column1) FROM ...
```

