---
sidebar_position: 5
---

# Kafka Connector

## Source

The Kafka Source allows a stream to fetch data from a kafka stream as a table in a stream.

The source is added to the read write factory with the following line:

```csharp
factory.AddKafkaSource("your regexp on table names",  new FlowtideKafkaSourceOptions()
    {
        Topic = topic,
        ConsumerConfig = kafkaConsumerConfig,
        KeyDeserializer = keyDeserializer,
        ValueDeserializer = valueDeserializer
    });
```

The following key deserializers exist:

* **FlowtideKafkaJsonKeyDeserializer** - deserializes a json key
* **FlowtidekafkaStringKeyDeserializer** - deserializes a string key

The following value deserializers exist:

* **FlowtideKafkaUpsertJsonDeserializer** - Deserializes the value as json, if the value is not null, its an upsert, if it is null, its a delete.

The source has a special column name for the key, it is called *_key* which can be used to select the key value from a kafka message.

### Usage in SQL

```sql
CREATE TABLE kafkasource (
    _key,
    firstName,
    lastName
);

INSERT INTO outputtable
SELECT _key, firstName, lastName
FROM kafkasource;
```