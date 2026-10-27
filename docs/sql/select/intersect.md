---
sidebar_position: 4
---

# Intersect

## Intersect All

The *INTERSECT ALL* operator returns rows that exist in all statements, this includes duplicates. 

```sql
select_stament1
INTERSECT ALL
select_stament2
```

## Intersect Distinct

The *INTERSECT DISTINCT* operator returns rows that exist in all statements, this outputs distinct rows.

```sql
select_stament
INTERSECT DISTINCT
select_stament
```
