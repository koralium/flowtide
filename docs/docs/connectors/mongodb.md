---
sidebar_position: 2
---

# MongoDB Connector

The MongoDB connector allows you to insert data into a MongoDB collection.
At this time only a sink is implemented, there is no support yet to have MongoDB as a source.

## Sink

The MongoDB sink allows the insertion of data into a MongoDB collection.

The nuget package for the connector is:

* FlowtideDotNet.Connector.MongoDB

Its implementation waits fully until the stream has reached a steady state at a time T until it writes data to the collection.
This means that its table output can always be traced back to a state from the source systems.

To use the *MongoDB Sink* add the following line to the *ReadWriteFactory*:

```csharp
factory.AddMongoDbSink("regex pattern for tablename", new FlowtideMongoDBSinkOptions()
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
factory.AddMongoDbSink("regex pattern for tablename", new FlowtideMongoDBSinkOptions()
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
factory.AddMongoDbSink("regex pattern for tablename", new FlowtideMongoDBSinkOptions()
    {
        ...
        OnWatermarkUpdate = async (watermark) => {
            // Inform other systems for instance about the watermark change.
        }
    });
```