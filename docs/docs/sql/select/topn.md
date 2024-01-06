---
sidebar_position: 2
---

# Top N

The Top N operator returns only the top N results from a query.
An ordering should be provided as well.

Example:

```
SELECT TOP (10) 
    userkey
FROM users
ORDER BY userkey
```