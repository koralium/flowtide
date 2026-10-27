---
sidebar_position: 10
---


# Permify Connector

The Permify Connector allows you to read and write data to/from [Permify](https://permify.co/) which is an authorization service built on Zanzibar.

In Permify you can write both relationships and attributes to an entity. At this point there is only support to read and write relationships.

## Relationship Sink

The relationship sink allows inserting relationship data from other sources into Permify.

These columns are required to insert data:

* **subject_type** - subject type
* **subject_id** - identifier of the subject
* **relation** - relation name
* **entity_type** - Entity type
* **entity_id** - identifier of the entity. 

Optional:

* **subject_relation** - optional subject relation.

To use the *Permify Relationship Sink* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddPermifyRelationshipSink("regex pattern for tablename", new PermifySinkOptions()
{
    Channel = grpcChannel,
    TenantId = tenantId
});
```

The regex pattern should match what you write in *SQL* as the table name in the insert statement.

Sql example:

```sql
INSERT INTO permify
SELECT 
    'user' as subject_type,
    o.userkey as subject_id,
    'reader' as relation,
    'document' as entity_type,
    o.orderkey as entity_id
FROM orders o
```

### Events

The following event listener exist so far in the Permify relationship sink:

* **OnWatermarkFunc** - Called after a watermark is recieved and the data has been added to Permify, also contains the last recieved snaptoken from Permify.

## Relationship Source

The relationship source allows reading relationship data from Permify and use it in your data streams.

The following columns are returned:

* **subject_type** - subject type
* **subject_id** - identifier of the subject
* **relation** - relation name
* **entity_type** - resource type
* **entity_id** - identifier of the resource. 
* **subject_relation** - optional subject relation.


Example on using the relationship source:

```csharp
connectorManager.AddPermifyRelationshipSource("regex pattern for tablename", new PermifySourceOptions()
{
    Channel = grpcChannel,
    TenantId = tenantId
});
```

## Materialize/Denormalize Permissions

It is not yet possible to materialize/denormalize permissions from Permify.