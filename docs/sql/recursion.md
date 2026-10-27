---
sidebar_position: 6
---

# Recursion

Flowtide also supports recusive queries, such as iterating over a tree structure. This is done with the *WITH* statement, and works similar as in other databases.

Example:

```sql
with user_manager_cte AS (
    SELECT 
        userKey, 
        firstName,
        lastName,
        managerKey,
        null as ManagerFirstName,
        1 as level
    FROM users
    WHERE managerKey is null
    UNION ALL
    SELECT 
        u.userKey, 
        u.firstName,
        u.lastName,
        u.managerKey,
        umc.firstName as ManagerFirstName,
        level + 1 as level 
    FROM users u
    INNER JOIN user_manager_cte umc ON umc.userKey = u.managerKey
)
INSERT INTO output
SELECT userKey, firstName, lastName, managerKey, ManagerFirstName, level
FROM user_manager_cte
```