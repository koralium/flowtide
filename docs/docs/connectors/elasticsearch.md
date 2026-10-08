---
sidebar_position: 3
---

# Elasticsearch Connector

The Elasticsearch connector allows you to insert data into ElasticSearch.
There is only a sink operator implemented, and there is no plans yet to support a source.

## Sink

The ElasticSearch sink allows insertion into an index.

> [!NOTE]
> All ElasticSearch insertions must contain a column called '_id' this column is the unique identifier in the elasticsearch index.
> This field will not be added to the source fields.


To use the *ElasticSearch Sink* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddElasticsearchSink("*", new FlowtideElasticsearchOptions()
{
    ConnectionSettings = () => new ElasticsearchClientSettings(new Uri(...))
});
```

The table name in the write relation becomes the index the sink writes to. The connection settings are a function to allow the usage of rolling passwords when connecting to elasticsearch.

### Options

| Option             | required | default    | Description                                                                                          |
| :----------------- | :------: | :--------: | :--------------------------------------------------------------------------------------------------- |
| ConnectionSettings |   True   |            | Function returning the client settings, called for each new client to allow rolling credentials.     |
| GetIndexNameFunc   |   False  | table name | Returns the index name for a write relation. Must be a concrete index, not an alias or data stream.  |
| CustomMappings     |   False  |            | Modifies the mapping properties on startup. Receives the existing properties, or empty for a new index. |
| OnIndexCreation    |   False  |            | Called before the sink creates an index that does not exist, to set settings, aliases and mappings.  |
| OnInitialDataSent  |   False  |            | Called after the initial data has been sent to elasticsearch.                                        |
| OnDataSent         |   False  |            | Called each time after data has been sent to elasticsearch.                                          |
| ExecutionMode      |   False  | Hybrid     | Execution mode of the sink.                                                                          |

### Example

Having a column named '_id' is required for the sink to function.

```csharp
sqlBuilder.Sql(@"
    INSERT into elastic_index_name
    SELECT userKey as _id, userKey, companyId, firstName, lastName 
    FROM users
");

connectorManager.AddElasticsearchSink("*", new FlowtideElasticsearchOptions()
{
    ConnectionSettings = () => new ElasticsearchClientSettings(new Uri(...))
});

...
```

### Customizing index creation

When the index does not exist, the sink creates it with a single create index request.
*OnIndexCreation* is called before that request is sent, with a context containing the client, the write relation, the index name and the *CreateIndexRequest*.
The request mappings already contain the properties from *CustomMappings*, so the mappings can use analyzers and normalizers defined in the same request.

Example:

```csharp
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Analysis;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;

connectorManager.AddElasticsearchSink("*", new FlowtideElasticsearchOptions()
{
    ConnectionSettings = () => connectionSettings,
    CustomMappings = (properties) =>
    {
        properties["name"] = new TextProperty { Analyzer = "folding" };
        properties["sku"] = new KeywordProperty { Normalizer = "lc" };
    },
    OnIndexCreation = (context) =>
    {
        context.Request.Settings = new IndexSettings
        {
            NumberOfShards = 3,
            NumberOfReplicas = 1,
            RefreshInterval = "30s",
            Analysis = new IndexSettingsAnalysis
            {
                Analyzers = new Analyzers
                {
                    { "folding", new CustomAnalyzer("standard") { Filter = new List<string> { "lowercase", "asciifolding" } } }
                },
                Normalizers = new Normalizers
                {
                    { "lc", new CustomNormalizer { Filter = new List<string> { "lowercase" } } }
                }
            }
        };
        context.Request.Aliases = new Dictionary<Name, Alias>
        {
            [$"{context.WriteRelation.NamedObject.DotSeperated}_search"] = new Alias()
        };
        return Task.CompletedTask;
    }
});
```

### Set alias on initial data completion

One way to integrate with elasticsearch is to create a new index for each new stream version and change an alias to point to the new index.
This is possible by using the *GetIndexNameFunc* and *OnInitialDataSent* functions in the options.
Combine it with *OnIndexCreation* to give each new index its own settings.

Example:

```csharp
connectorManager.AddElasticsearchSink("*", new FlowtideElasticsearchOptions()
{
    ConnectionSettings = () => connectionSettings,
    CustomMappings = (props) =>
    {
        // Add custom mappings
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
        var getAliasResponse = await client.Indices.GetAliasAsync(new Elastic.Clients.Elasticsearch.IndexManagement.GetAliasRequest(name: aliasName));

        var putAliasResponse = await client.Indices.PutAliasAsync(indexName, aliasName);

        var oldIndices = getAliasResponse.Aliases?.Keys.ToList() ?? new List<string>();
        if (putAliasResponse.IsSuccess())
        {
            foreach (var oldIndex in oldIndices)
            {
                if (oldIndex != indexName)
                {
                    await client.Indices.DeleteAsync(oldIndex);
                }
            }
        }
        else
        {
            throw new InvalidOperationException($"Failed to put alias '{aliasName}' on '{indexName}': {putAliasResponse.ElasticsearchServerError?.Error?.Reason ?? putAliasResponse.DebugInformation}");
        }
    },
});
```
