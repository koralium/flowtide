---
sidebar_position: 3
---

# List Functions

## List Agg

This function does not have a substrait definition.

List Agg creates a list of values per group. This is useful when denormalizing data.
It takes in one expression which will be the value added to the list for that row.

### SQL Usage

```sql
SELECT key1, list_agg(value1) 
FROM table1
GROUP BY key1
```

Given the following two rows:

1, 'hello'
1, 'world'

The output would be:

1, ['hello', 'world']

## List Union Distinct Agg

This function does not have a substrait definition.

List Union Distinct Agg combines multiple lists and returns the distinct set of all lists.
This can be useful when having multiple rows with lists that need to be combined.

### SQL Usage

```sql
SELECT key1, list_union_distinct_agg(value1) 
FROM table1
GROUP BY key1
```

Given the following two rows:

* 1, ['hello', 'world']
* 1, ['world']

The output would be:

* 1, ['hello', 'world']
