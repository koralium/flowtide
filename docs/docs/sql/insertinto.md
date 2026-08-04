---
sidebar_position: 5
---

# Insert Into

The *INSERT INTO* statement is used to send data from the stream into a sink. Each stream requires at least one insert into statement to mark which data should be output from the stream.

```sql
INSERT INTO table_name
select_statement
```

## Example

```sql
INSERT INTO outputtable
SELECT column1, column2 FROM inputtable
```

## Primary keys

A sink groups the rows it writes by a primary key. Most connectors find the primary key on their own, for instance by
looking at the destination table, but the *PRIMARY KEY* clause lets the statement decide which columns to use instead.

```sql
INSERT INTO table_name PRIMARY KEY (column1, column2)
select_statement
```

This is useful when the destination has no primary key, when its primary key is not written by the stream, or when a
stream writes to several tables that each need different keys.

Example:

```sql
INSERT INTO outputtable PRIMARY KEY (column1)
SELECT column1, column2 FROM inputtable
```

Every declared column must be written by the statement, a column that is not part of the select is an error. The clause
is also placed after the column list when one is given:

```sql
INSERT INTO outputtable (column1, column2) PRIMARY KEY (column1)
SELECT a, b FROM inputtable
```

> [!NOTE]
> Not all connectors can use declared primary keys. Building the stream fails if the statement declares primary keys for
> a sink that does not support them, rather than writing the data with other keys than the declared ones.