---
sidebar_position: 9
---

# AuthZed/SpiceDB Connector

[AuthZed](https://authzed.com/)/[SpiceDB](https://github.com/authzed/spicedb) is an authorization service built on Zanzibar.
This connector allows writing, reading and materializing/denormalizing permissions from SpiceDB.

## Sink

The sink allows inserting data from other sources into SpiceDB.

These columns are required to insert data:

* **subject_type** - subject type
* **subject_id** - identifier of the subject
* **relation** - relation name
* **resource_type** - resource type
* **resource_id** - identifier of the resource. 

Optional:

* **subject_relation** - optional subject relation.


To use the *SpiceDB Sink* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddSpiceDbSink("spicedb", new SpiceDbSinkOptions
{
    Channel = grpcChannel, // Grpc channel used to connect to SpiceDB
    GetMetadata = () =>
    {
        var metadata = new Metadata();
        // Add any headers etc here.
        metadata.Add("Authorization", "Bearer {token}");
        return metadata;
    }
});
```

Sql example:

```sql
INSERT INTO spicedb
SELECT 
    'user' as subject_type,
    o.userkey as subject_id,
    'reader' as relation,
    'document' as resource_type,
    o.orderkey as resource_id
FROM orders o
```

### Events

The following event listeners exist that can be used to modify or get the current watermark of the stream that has been sent to SpiceDB:

* **BeforeWriteRequestFunc** - Called before each write, its possible to modify the data before it gets sent here.
* **OnWatermarkFunc** - Called after a watermark is recieved and the data has been added to SpiceDB, also contains the last recieved zedtoken from SpiceDB.
* **OnInitialDataSentFunc** - Called the first time data has been written to SpiceDB.

### Delete existing data if not updated

It is possible to delete existing data in SpiceDB if it is not in the result set of the stream.
This is done by passing in the property *DeleteExistingDataFilter* which is the filter of what data to fetch.
If your stream say updates resource type *document* and relation *reader* you should pass that in as the filter if you wish
to delete existing data that is not from the current stream.

This will cause all data to be downloaded into the stream which will cause a slower performance to read the initial data.

## Source

The source allows reading data from SpiceDB. The following columns are returned:

* **subject_type** - subject type
* **subject_id** - identifier of the subject
* **relation** - relation name
* **resource_type** - resource type
* **resource_id** - identifier of the resource. 
* **subject_relation** - optional subject relation.

Filter conditions on resource type, relation and subject type will tried to be pushed down in the query to SpiceDB if possible.

Example on using the spicedb source:

```csharp
connectorManager.AddSpiceDbSource("spicedb", new SpiceDbSourceOptions
{
    Channel = grpcChannel,
    GetMetadata = () =>
    {
        var metadata = new Metadata();
        // Add any headers etc here.
        metadata.Add("Authorization", "Bearer {token}");
        return metadata;
    }
});
```

The first argument is a plain table name string (case-insensitive). This is the name you use to reference the source table in SQL, for example `FROM spicedb`.

## Materialize/Denormalize Permissions using `materialize_permission`

The easiest way to materialize permissions is to use the `materialize_permission` SQL table function directly in your query.
It is exposed as `{tablename}.materialize_permission(...)` where `{tablename}` matches the table name used when registering the source.
The schema is automatically fetched from SpiceDB at query-plan build time, so no manual schema loading is required.

The function signature is:

```
{tablename}.materialize_permission(
    resource_type   STRING,   -- required: the resource type to expand (e.g. 'document')
    relation        STRING,   -- required: the permission or relation to expand (e.g. 'view')
    recurseAtStopType BOOL,   -- optional (default false): recurse into stop types before halting
    stopType1       STRING,   -- optional: first stop type
    stopType2       STRING,   -- optional: additional stop types ...
)
```

The function returns rows with the following columns:

| Column                | Description                               |
|-----------------------|-------------------------------------------|
| `subject_type`        | The subject type                          |
| `subject_id`          | The subject identifier                    |
| `subject_relation`    | The subject relation (if any)             |
| `relation`            | The expanded relation/permission name     |
| `resource_type`       | The resource type                         |
| `resource_id`         | The resource identifier                   |

:::note
The function can only be used as a standalone `FROM` source; it cannot be placed inside a `JOIN`.
:::

Example — expand the `view` permission on `document`:

```sql
INSERT INTO outputtable
SELECT
    subject_type,
    subject_id,
    relation,
    resource_type,
    resource_id
FROM spicedb.materialize_permission('document', 'view')
```

Example — expand `can_view` on `file`, stopping at `folder` but recursing into it:

```sql
INSERT INTO outputtable
SELECT
    subject_type,
    subject_id,
    relation,
    resource_type,
    resource_id
FROM spicedb.materialize_permission('file', 'can_view', true, 'folder')
```

## Materialize/Denormalize Permissions using the plan API

It is possible to denormalize the relations in a SpiceDB schema based on a permission in a type.
This can be useful to add permissions into a search engine or similar where searching should be done based on the users permissions.

First a plan must be created:

```csharp
var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "document", "view", "spicedb");
```

It requires the schema, which can be fetched from the schema service, or loaded for a file.
The second argument is the type, and the third is the permission/relation to denormalize.
The last argument is which table name should be used, and should be matched in the *ReadWriteFactory*.

```csharp
// Add the plan as a view for sql 
sqlPlanBuilder.AddPlanAsView("authdata", viewPermissionPlan);

// use the view in a query
sqlPlanBuilder.Sql(@"
INSERT INTO outputtable
SELECT 
  subject_type,
  subject_id,
  relation,
  resource_type,
  resource_id
FROM authdata
");
```

### Stop at types

It is possible to send in an array of type names where the search should end.
This can be useful in scenarios where say an entire company has access to a resource, it can be better to add the company identifier instead of every single user in the company.

Example:

```csharp
var modelPlan = SpiceDbToFlowtide.Convert(schemaText, "{type name}", "{relation name}", "{input table name}", false, "company");
```

The relation name will still be the relation name you are filtering on but instead with the object type company and its identifier.

It is also possible to allow recursions at stop types to create a flat list. This is useful in scenarios such as a folder
structure:

```
definition folder {
  relation user: user
  relation parent: folder

  permission can_view = user + parent->can_view 
}

definition file {
    relation user: user
    relation folder: folder

    permission can_view = user + folder->can_view
}
```

If one would use `folder` as a stop type at a folder structure like this: `/folder1/folder2/folder3/file.txt`.

With the following command:

```csharp
var modelPlan = SpiceDbToFlowtide.Convert(schemaText, "file", "can_view", "{input table name}", true, "folder");
```

The result for `file.txt` would become:

```
/folder1
/folder1/folder2
/folder1/folder2/folder3
+ any users that have file access
```