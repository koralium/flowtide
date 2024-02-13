# OpenFGA and SQL Server to MongoDB Demo

This demo shows the usage of OpenFGA authorization model to query and combines it with data from SQL Server and stores it denormalized in MongoDB.

To run the demo, start the docker compose:

```
docker compose up
```

Visit http://localhost:8000/stream to see status of the stream, and http://localhost:3000/playground to see OpenFGA data.
Connect to mongodb using: mongodb://mongo:27017 as the connection string to see the output data.

The demo runs the following query where it creates a list aggregate of all the users per document that can view it.
It then combines the data with document information from SQL Server and stores it in MongoDB.

```
CREATE VIEW docpermissions AS
SELECT
  object_id,
  list_agg(user_type || ':' || user_id) AS permissions
FROM permissionview
GROUP BY object_id;

INSERT INTO outputdata
SELECT docid, [name], permissions
FROM demo.dbo.docs
LEFT JOIN docpermissions ON docid = object_id;
```

The authorization model used looks like this:

```
model
  schema 1.1

type user

type role
  relations
    define can_view: [user:*]

type role_binding
  relations
    define can_view: user and can_view from role
    define role: [role]
    define user: [user]

type organization_group
  relations
    define can_view: can_view from role_bindings or can_view from parent
    define parent: [organization_group]
    define role_bindings: [role_binding]

type doc
  relations
    define can_view: can_view from organization
    define organization: [organization_group]
```