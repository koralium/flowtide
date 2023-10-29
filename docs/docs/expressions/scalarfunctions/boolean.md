---
sidebar_position: 2
---

# Boolean Functions

## Or

[Substrait definition](https://substrait.io/extensions/functions_boolean/#or)

*Or* implements the boolean logic *Or* operator. Its return value will always be a boolean.
An argument into the *Or* function that is not a boolean will be the same as the boolean value false.

### SQL Usage

```sql
... WHERE c1 = 'hello' OR c2 = 'world'
```

## And

[Substrait definition](https://substrait.io/extensions/functions_boolean/#and)

*And* implements the boolean logic *And* operator. Its return value will always be a boolean.
An argument into the *And* function that is not a boolean will be the same as the boolean value false.

### SQL Usage

```sql
... WHERE c1 = 'hello' AND c2 = 'world'
```
