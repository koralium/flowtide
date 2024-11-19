---
sidebar_position: 2
---

# MongoDB Connector

The MongoDB connector allows you to insert data into a MongoDB collection.
At this time only a sink is implemented, there is no support yet to have MongoDB as a source.

The nuget package for the connector is:

* FlowtideDotNet.Connector.MongoDB

## Source

The MongoDB source allows a user to create a stream that reads data from MongoDB and any changes on the collection.
The connector requires that change stream is enabled on the MongoDB instance to function.

To add the *MongoDB source* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddMongoDbSource("connectionString");
```

To select data from MongoDB use `{database}.{collectionName}`.

You can also add MongoDB under a catalog:

```csharp
connectorManager.AddCatalog("mymongodb", c => {
    c.AddMongoDbSource("connectionString");
});
```

This will then be referenced with `mymongodb.{database}.{collectionName}`.

### Accessing properties

By default, the mongoDB source contains two properties defined:

* *_id* - the _id field in MongoDB.
* *_doc* - Map with key values of all the properties in the document.

Example usage:

```sql
SELECT _doc.firstName, _doc.lastName
FROM {database}.{collection}
```

It is possible to define a schema for easier access by using `CREATE TABLE`:

```sql
CREATE TABLE {database}.{collection} (
  firstName,
  lastName
);

SELECT firstName, lastName FROM {database}.{collection}
```

Using create table limits the data that is sent through the stream and only sends out the properties defined in the create table.

:::warning

When using _doc field the entire document is sent through the stream which can result in a performance decrease.
A better alternative if one does not want to use `CREATE TABLE` is to use `CREATE VIEW` to project the map values earlier in the stream.

```sql
CREATE VIEW mymongodb AS
SELECT _doc.firstName, _doc.lastName
FROM {database}.{collection};

SELECT firstName, lastName FROM mymongodb;
```

:::


## Sink

The MongoDB sink allows the insertion of data into a MongoDB collection.

Its implementation waits fully until the stream has reached a steady state at a time T until it writes data to the collection.
This means that its table output can always be traced back to a state from the source systems.

To use the *MongoDB Sink* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddMongoDbSink("regex pattern for tablename", new FlowtideMongoDBSinkOptions()
    {
        Collection = collection, //MongoDB collection
        Database = databaseName, // MongoDB database
        ConnectionString = connectionString, //Connection string to MongoDB
        PrimaryKeys = primaryKeys //List of columns that will be treated as primary keys in the collection
    });
```

### Overwriting data in a collection and cleaning up old data

It is possible with the MongoDB sink to append metadata to documents and remove data from previous runs.
This can be helpful when the stream is changed and you want to write to the same collection, but remove data from a previous run.

To do this we add the following code:

```csharp
connectorManager.AddMongoDbSink("regex pattern for tablename", new FlowtideMongoDBSinkOptions()
    {
        ...
        TransformDocument = (doc) => {
            // version should come from configuration
            doc.Add("_metadata", run_version);
        },
        OnInitialDataSent = async (collection) => {
            await collection.DeleteManyAsync(Builders<BsonDocument>.Filter.Not(Builders<BsonDocument>.Filter.Eq("_metadata", run_version)));
        }
    });
```

This will append a metadata field to all documents with the current run version.
When the initial data from the stream has been saved, it will delete all documents that does not have the metadata information.

### Watermark updates

It is possible to listen to watermark updates, this is done by setting the *OnWatermarkUpdate* property in the options.

Example:

```csharp
connectorManager.AddMongoDbSink("regex pattern for tablename", new FlowtideMongoDBSinkOptions()
    {
        ...
        OnWatermarkUpdate = async (watermark) => {
            // Inform other systems for instance about the watermark change.
        }
    });
```