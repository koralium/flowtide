---
sidebar_position: 8
---

# List Functions

## List Sort Ascending Null Last

*This function has no substrait equivalent*

Sorts a list of values in ascending order, placing any null values at the end of the result.

Values are ordered according to their natural ascending order (e.g., numerically or lexicographically).
Nulls are not compared to other values directly; they are always considered greater for the purpose of ordering.
Any value that is not a list will return the result as null.

### SQL Usage

```sql
SELECT list_sort_asc_null_last(list(orderkey, userkey)) FROM ...
```
