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
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
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
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
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
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
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
| Non numeric   | Integer       | Null      |
| num numeric   | Float         | Null      |
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
