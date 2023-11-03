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