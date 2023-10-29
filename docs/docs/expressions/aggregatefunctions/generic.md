---
sidebar_position: 1
---

# Generic Functions

## Count

[Substrait definition](https://substrait.io/extensions/functions_aggregate_generic/#count_1)

Counts all rows in a query. This function takes no parameters, the implementation of count with a column name is not yet implemented.

### SQL Usage

```sql
SELECT count(*) FROM ... 
```
