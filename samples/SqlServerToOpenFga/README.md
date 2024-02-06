# SQL Server to OpenFGA Demo

This demo shows how to send data from sql server to openfga using a data stream.

To run the demo, start the docker compose:

```
docker compose up
```

The demo selects data from three different tables using the following SQL statement:

```
INSERT INTO openfga
SELECT 
  'user' AS user_type,
  u.userId as user_id,
  'member' as relation,
  'group' as object_type,
  g.groupId as object_id
FROM demo.dbo.usergroups ug
INNER JOIN demo.dbo.users u ON ug.userkey = u.userkey
INNER JOIN demo.dbo.groups g ON ug.groupkey = g.groupkey
```

and insert tuples for the following authorization model:

```
model
  schema 1.1

type user

type group
  relations
    define member: [user]
```