---
sidebar_position: 4
---

# String functions

## String Agg

[Substrait definition](https://substrait.io/extensions/functions_string/#string_agg)

Concatenates a column of string values with a separator.

### SQL Usage

```sql
SELECT key1, string_agg(value1, ',') 
FROM table1
GROUP BY key1
```