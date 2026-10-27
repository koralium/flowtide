---
sidebar_position: 3
---

# Except

## Except All

The *EXCEPT ALL* operator returns rows that exist in the first statement, which does not exist in any other rows from the other statements. 

```sql
select_stament1
EXCEPT ALL
select_stament2
```

## Except Distinct

The *EXCEPT DISTINCT* operator returns distinct rows that exist in the first statement, which does not exist in any other rows from the other statements.

```sql
select_stament
EXCEPT DISTINCT
select_stament
```
