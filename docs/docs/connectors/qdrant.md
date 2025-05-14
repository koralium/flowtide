---
sidebar_position: 12
---

# Qdrant connector

The *Qdrant connector* only supports a sink, where data is stored into a Qdrant collection.

To use it, add the following nuget:
* FlowtideDotNet.Connector.Qdrant

## Qdrant sink

The qdrant sink is used to continously update, insert and delete points in a qdrant collection. 

Flowtide will add identifying metadata under the `flowtide` key, this contains metadata that is used to track and update rows.

It will also add some general metadata under the `data` key (unless modified by the `QdrantPayloadDataPropertyName`-option), this contains information like `last_update` and which embedding generator was used. AS well as the vectorized string (if not disabled by the `QdrantIncludeVectorTextInPayload`-option). 

:::info

It's important that the Qdrant collection contains a payload indexes for the keys `flowtide` and `id`.

* The `flowtide` index should be a text index.
* The `id` index should (most likely) be a keyword index.

Without these indexes resources used will be drastically higher and performance will be diminished.

:::

To use it add it to the *ConnectorManager*:

```csharp
connectorManager.AddQdrantSink("{regexPattern}", new QdrantSinkOptions
{
    QdrantChannelFunc = () => QdrantChannel.ForAddress("{qdrantAddress}"),
    CollectionName = collection,
}, 
EmbeddingGeneratorImplementation,
chunker);

// there's also a built in open ai embedding generator, register by setting the OpenAiEmbeddingOptions
connectorManager.AddQdrantSink("{regexPattern}", new QdrantSinkOptions
{
    QdrantChannelFunc = () => QdrantChannel.ForAddress("{qdrantAddress}"),
    CollectionName = collection,
}, new OpenAiEmbeddingOptions()
{
    ApiKeyFunc = () => "{OpenAi api key}",
    UrlFunc = () => "{OpenAi api url}"
}, 
chunker);

// optional string chunker
var chunker = new TikTokenChunker(new TikTokenChunkerOptions
{
    Tokenizer = TiktokenTokenizer.CreateForModel("gpt-4o"),
    TokenChunkSize = 170,
    TokenChunkOverlap = 20,
});
```

### Special columns

There are two required columns that must be selected.
* **id**: The identifier/PK for the row. This can be modified with the `IdColumnName` option.
* **vector_string**: The string content that should be vectorized. This can be modified with the `VectorStringColumnName` option.

### Options 

Options for the sink:
| Option                          | required |  default      | Description                                                                     |
| :------------------------------ | :------: | :-----------: |:------------------------------------------------------------------------------- |
| QdrantChannelFunc               |   True   |               | Channel for communicating with the Qdrant instance.                             |
| CollectionName                  |   True   |               | The collection name that should be used to store data.                          |
| IdColumnName                    |   True   |   id          | The selected column that should be used as identity.                            |
| VectorStringColumnName          |   True   | vector_string | The selected column that should be vectorized.                                  |
| OnInitialize                    |   false  |               | Hook that is run when the sink is intializing.                                  |
| OnInitialDataSent               |   false  |               | Hook that is run after initial data has been sent.                              |
| OnChangesDone                   |   false  |               | Hook that is run after changes have been handled.                               |
| Wait                            |   false  |     true      | Wait for calls against Qdrant to complete before continuing.                    |
| QdrantVectorTextPropertyName    |   true   |     text      | The property name that should be used for the vectorized string in the payload  |
| QdrantPayloadDataPropertyName   |   true   |     data      | Under which key metadata should be added to in the Qdrant collection. This will contain information like `last_update` and which embedding generator was used.  |
| QdrantIncludeVectorTextInPayload|   true   |     true      | If the vectorized string should be stored in the payload or not.                 |
| QdrantStoreMapsUnderOwnKey      |   true   |     false     | If set to true any selected `map` or `named_struct` will be added under its own key in the payload | 
| QdrantStoreListsUnderOwnKey     |   true   |     false     | If true any selected `list` will be added under its own key in the payload.      |
| QdrantPayloadUpdateMode         |   true   | OverWritePayload | `Set` or `overwrite` the payload when updating a point. `Set` will keep any property not handled by the `flowtide` stream, `overwrite` will remove any such property. |
| ResiliencePipeline              |   true   |  Incremental retries | Resilience pipeline for requests against Qdrant. By default an incremental retry strategy that retries 10 times. |
| MaxNumberOfBatchOperations      |   true   |  1000         | The max number of operations before the batch should be sent to Qdrant. |

### Example updating alias on initialize

```csharp
OnInitialize = async (state, client) =>
{
    var collection = state.CollectionName;
    // set the collection name to a unique name based on the current version of flowtide
    // note that flowtide versioning needs to be added with, for instance, flowtideBuilder.AddVersioningFromString(...)
    if (!string.IsNullOrWhiteSpace(state.StreamVersion?.Version))
    {
        collection = $"collection-{state.StreamVersion.Version}";
    }

    // if the collection does not already exist, add it to qdrant and add payload indexes
    state.CollectionName = collection;
    if (!await client.CollectionExistsAsync(state.CollectionName))
    {
        await client.CreateCollectionAsync(state.CollectionName, new VectorParams
        {
            // configure the collection
            Size = 1536,
            Distance = Distance.Cosine,
        });

        // These indexes are important as they are used by flowtide to filter and udate points
        await client.CreatePayloadIndexAsync(state.CollectionName, "id", PayloadSchemaType.Keyword);
        await client.CreatePayloadIndexAsync(state.CollectionName, "flowtide", PayloadSchemaType.Text);
    }
},
OnInitialDataSent = async (state, client) =>
{
    // delete the alias from the original collection and add it to the new collection
    await client.UpdateAliasesAsync(
    [
        new AliasOperations
        {
          DeleteAlias = new DeleteAlias
          {
            AliasName = alias,
          },
          CreateAlias = new CreateAlias
          {
            AliasName = alias,
            CollectionName = state.CollectionName,
          },
        },
    ]);

    // delete the old collection
    await client.DeleteCollectionAsync(state.OriginalCollectionName);
},
```

## Embeddings generator

An embeddings generator is required for this sink. It should implement `IEmbeddingGenerator`.

### OpenAi embeddings generator

Embedding generator for the open ai api (`OpenAiEmbeddingsGenerator`), but can be used for any api that matches the return type of open ai, like azure open ai for instance.

#### Options

| Option                          | required |  default      | Description     |
| :------------------------------ | :------: | :-----------: |:--------------- |
| UrlFunc                         |    true  |               | Url to the api. |
| ApiKeyFunc                      |    true  |               | Api key for the api. |
| MaxRequestsPerMinute            |    true  |    900        | Max number of requests per minute. This is used by the default `ResiliencePipeline`.  |
| ResiliencePipeline              |  true   |  Inner rate limiter with outer retry strategy. | Adds a rate limiter that permits `MaxRequestsPerMinute` requets per minute. If the rate limiter hits its limit the retry strategy will wait upto 1 minute until the next request occurs. For general errors the retry strategy will retry 5 times incrementally before failing. |


## String chunker

Optionally the sink can take a string chunker (`IStringChunker`) that splits the `vector_string` into multiple smaller chunks. 

### TikTokenStringChunker

A simple chunker that removes newlines and splits the content into multiple parts with overlap based on the options `TokenChunkSize` and `TokenChunkOverlap`. 

#### Options

| Option            | required |  default | Description     |
| :---------------- | :------: | :------: |:--------------- |
| TokenChunkSize    |  true    |          | The chunk size. |
| TokenChunkOverlap |  true    |          |  The chunk overlap. This many tokens from the previous chunk should be added to the start of the current. |
| TiktokenTokenizer |  true    |          | The tiktoken tokenizer that should be used to tokenize the content. |
| MinTokenChunkSize |  false   |          | If a chunk is below this limit it will be merged into the previous chunk.  |