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

