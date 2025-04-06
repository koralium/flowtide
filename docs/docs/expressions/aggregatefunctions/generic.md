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

## Surrogate Key Int64

*This function does not have a substrait definition.*

Generates a unique int64 value for each combination of the "group by" columns in an aggregate query.
This can be used for instance when creating a SCD table where the group by can be on both the primary key and date.

### SQL Usage

```sql
SELECT
    surrogate_key_int64() as SK_MyKey,
    id,
    insertDate
FROM testtable
GROUP BY id, insertDate
```