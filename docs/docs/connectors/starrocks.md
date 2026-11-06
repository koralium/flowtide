---
sidebar_position: 13
---

# Starrocks Connector

The **StarRocks connector** allows you to insert data into a [StarRocks](https://www.starrocks.io/) database.  
Currently, only a **sink operator** is implemented.

The **StarRocks sink** enables data insertion into a StarRocks table using the built-in Stream Load or HTTP-based ingestion mechanism.

> [!NOTE]
> All StarRocks sinks require that the target table exists **and has a primary key defined**.  
> If the target table does not have a primary key, the connector will throw an error.  
> This is required for supporting both upsert and delete operations in StarRocks.  


To use the *StarRocks Sink*, add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddStarRocksSink("*", new StarRocksSinkOptions()
{
    HttpUrl = "http://starrocks-fe-host:8030",
    Credentials = () => new StarRocksUsernamePassword("username", "password"),
    ExecutionMode = ExecutionMode.OnCheckpoint
});
```

The table name in the write relation becomes the StarRocks table the sink writes to.

Sql example:

```sql
INSERT INTO my_db.my_table
SELECT 
    o.orderkey,
    o.ordername
FROM orders o
```

### Execution Modes

The StarRocks connector supports two execution modes that control when and how data is committed to the target table.

#### **OnWatermark**

- Data is uploaded to StarRocks whenever a **watermark** event is received.  
- This mode offers **low latency** and **high throughput**, as data is flushed frequently.  
- However, in the event of a **stream failure or restart**, already inserted data is **not rolled back**, which may result in duplicates or partial inserts.  
- Recommended for **at-least-once** delivery semantics when latency is more important than strict consistency.
- Loads run in the background, so the sink keeps receiving data while a load is in flight.
  Watermarks that arrive meanwhile are merged into the next load.
- A checkpoint waits until all pending data has been loaded into StarRocks.

#### **OnCheckpoint**

- Uses **transactions** and commits data only after a **checkpoint** has been successfully completed.  
- This mode provides **exactly-once** delivery semantics — ensuring that no duplicate or partial inserts occur, even in case of failures.  
- Slightly higher latency compared to `OnWatermark`, due to the transactional guarantees.  
- Recommended for **production** or **critical pipelines** where correctness and consistency are prioritized.

### Batching and parallel loads

| Option | Default | Description |
| ------ | ------- | ----------- |
| `BatchSize` | 1000 | Rows per Stream Load request. |
| `MaxParallelLoads` | 1 | Stream Load requests in flight at once within one upload. Only used in `OnWatermark` and `Hybrid` mode. |

Each Stream Load request is its own StarRocks transaction and creates a new version of the table.
Raising `BatchSize` reduces the number of transactions. Raising `MaxParallelLoads` helps when uploads span several batches,
but more concurrent loads increase the risk of StarRocks *too many versions* errors.

### Retries

Every request to StarRocks runs through `ResiliencePipeline`, a [Polly](https://www.pollydocs.org/) pipeline.
By default a failed request is retried 5 times with exponential backoff starting at 1 second, with jitter.
Any error is retried, including HTTP timeouts, except when the stream itself is stopping.
Each failed attempt is logged as a warning. If the last retry also fails, the stream fails and restarts from its latest checkpoint.

```csharp
connectorManager.AddStarRocksSink("*", new StarRocksSinkOptions()
{
    HttpUrl = "http://starrocks-fe-host:8030",
    Credentials = () => new StarRocksUsernamePassword("username", "password"),
    // Custom retry policy, or ResiliencePipeline.Empty to disable retries
    ResiliencePipeline = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions { MaxRetryAttempts = 10, Delay = TimeSpan.FromSeconds(2) })
        .Build()
});
```

A retried Stream Load can be applied twice if the first attempt reached StarRocks before it failed.
This is safe for primary key tables, upserts and deletes give the same result when repeated.






