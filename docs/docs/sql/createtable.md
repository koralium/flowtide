---
sidebar_position: 2
---

# Create Table

The *CREATE TABLE* data definition is used to add metadata about a table to the *SqlPlanBuilder*.
Since in SQL, multiple tables might have the same column name, the builder must know what columns exist in which table.

Since *Flowtide* is typeless when defining inputs, it is not required to say a datatype when using create table.

Example usage:

```sql
CREATE TABLE table_name (
    column1,
    column2,
    column3,
   ....
);
```

Creating table definitions is not required if you are using [table providers](tableprovider).