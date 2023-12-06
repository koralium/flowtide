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