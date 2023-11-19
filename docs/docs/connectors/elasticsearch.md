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

To use the *ElasticSearch Sink* add the following line to the *ReadWriteFactory*:

```csharp
factory.AddElasticsearchSink("*", elasticSearchConnectionSettings);
```

The table name in the write relation becomes the index the sink writes to.

### Example

Having a column named 'id' and also a column matching the configured primary key is required for the sink to function.

```csharp
sqlBuilder.Sql(@"
    INSERT into elastic_index_name
    SELECT userKey as _id, userKey, companyId, firstName, lastName 
    FROM users
");

factory.AddElasticsearchSink("*", elasticSearchConnectionSettings);

...
```