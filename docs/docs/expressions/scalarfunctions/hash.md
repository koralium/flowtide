---
sidebar_position: 9
---

# Hash Functions

## XxHash128 Guid String

*This function has no substrait equivalent*

Computes a XxHash128 for one or multiple values and returns it as a guid in string representation.
This function can be useful to create partition keys for instance.

### SQL Usage

```sql
SELECT xxhash128_guid_string(val) FROM ...
SELECT xxhash128_guid_string(val1, val2) FROM ...
SELECT xxhash128_guid_string(val1, val2, ...) FROM ...
```