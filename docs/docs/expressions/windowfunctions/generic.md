---
sidebar_position: 2
---

# Generic Functions

## Surrogate Key Int64

*This function does not have a substrait definition.*

Generates a unique int64 value for each combination of the "partition by" columns in an window function query.
This can be used for instance when creating a SCD table where the partition by can be on both the primary key and date.

*This is non-deterministic across replays of a full rerun of a stream.*

### SQL Usage

```sql
SELECT
    surrogate_key_int64() OVER (PARTITION BY id, insertDate) as SK_MyKey,
    id,
    insertDate
FROM testtable
```