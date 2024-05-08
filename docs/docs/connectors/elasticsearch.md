---
sidebar_position: 3
---

# Elasticsearch Connector

The ElasticsSarch connector allows you to insert data into ElasticSearch.
There is only a sink operator implemented, and there is no plans yet to support a source.

## Sink

The ElasticSearch sink allows insertion into an index.

:::info

All ElasticSearch insertions must contain a column called '_id' this column is the unique identifier in the elasticsearch index.
This field will not be added to the source fields.

:::

To use the *ElasticSearch Sink* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddElasticsearchSink("*", elasticSearchConnectionSettings);
```

The table name in the write relation becomes the index the sink writes to.

### Example

Having a column named '_id' is required for the sink to function.

```csharp
sqlBuilder.Sql(@"
    INSERT into elastic_index_name
    SELECT userKey as _id, userKey, companyId, firstName, lastName 
    FROM users
");

connectorManager.AddElasticsearchSink("*", elasticSearchConnectionSettings);

...
```

### Set alias on initial data completion

One way to integrate with elasticsearch is to create a new index for each new stream version and change an alias to point to the new index.
This is possible by using the *GetIndexNameFunc* and *OnInitialDataSent* functions in the options.

Example:

```csharp
connectorManager.AddElasticsearchSink("*", new FlowtideDotNet.Connector.ElasticSearch.FlowtideElasticsearchOptions()
{
    ConnectionSettings = connectionSettings,
    CustomMappings = (props) =>
    {
        // Add cusotm mappings
    },
    GetIndexNameFunc = (writeRelation) =>
    {
        // Set an index name that will be unique for this run
        // The index name must be possible to be recovered between crashes to write to the same index
        return $"{writeRelation.NamedObject.DotSeperated}-{tagVersion}";
    },
    OnInitialDataSent = async (client, writeRelation, indexName) =>
    {
        var aliasName = writeRelation.NamedObject.DotSeperated;
        // Get indices that the alias already points to.
        var oldIndices = await client.GetIndicesPointingToAliasAsync(aliasName);
        // Add the index to the alias
        var putAliasResponse = await client.Indices.PutAliasAsync(indexName, aliasName);
        
        if (putAliasResponse.IsValid)
        {
            // Remove all old indices that existed on the alias
            foreach (var oldIndex in oldIndices)
            {
                await client.Indices.DeleteAsync(oldIndex);
            }
        }
        else
        {
            throw new InvalidOperationException(putAliasResponse.ServerError.Error.StackTrace);
        }
    },
});
```
