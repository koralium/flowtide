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
connectorManager.AddSpiceDbSink("regex pattern for tablename", new SpiceDbSinkOptions
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
connectorManager.AddSpiceDbSource("regex pattern for tablename", new SpiceDbSourceOptions
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

## Materialize/Denormalize Permissions

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