---
sidebar_position: 6
---

# Datetime Functions

## Strftime

[Substrait definition](https://substrait.io/extensions/functions_datetime/#strftime)

Converts a timestamp in microseconds since 1970-01-01 00:00:00.000000 to a string.
See [strftime](https://man7.org/linux/man-pages/man3/strftime.3.html) on how to to write the format string.

Arguments:

1. Timestamp in microseconds
2. Format as a string

### SQL Usage

```sql
INSERT INTO output
SELECT
    strftime(Orderdate, '%Y-%m-%d %H:%M:%S') as Orderdate
FROM Orders
```