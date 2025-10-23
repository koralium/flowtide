---
sidebar_position: 13
---

# Starrocks Connector

The **StarRocks connector** allows you to insert data into a [StarRocks](https://www.starrocks.io/) database.  
Currently, only a **sink operator** is implemented.

The **StarRocks sink** enables data insertion into a StarRocks table using the built-in Stream Load or HTTP-based ingestion mechanism.

:::info  
All StarRocks sinks require that the target table exists **and has a primary key defined**.  
If the target table does not have a primary key, the connector will throw an error.  
This is required for supporting both upsert and delete operations in StarRocks.  
:::

To use the *StarRocks Sink*, add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddStarRocksSink("*", new FlowtideStarRocksOptions()
{
    HttpUrl = "http://starrocks-fe-host:8030",
    Username = "user",
    Password = "password",
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

#### **OnCheckpoint**

- Uses **transactions** and commits data only after a **checkpoint** has been successfully completed.  
- This mode provides **exactly-once** delivery semantics — ensuring that no duplicate or partial inserts occur, even in case of failures.  
- Slightly higher latency compared to `OnWatermark`, due to the transactional guarantees.  
- Recommended for **production** or **critical pipelines** where correctness and consistency are prioritized.






