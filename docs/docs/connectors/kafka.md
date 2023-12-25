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
        ConsumerConfig = kafkaConsumerConfig,
        KeyDeserializer = keyDeserializer,
        ValueDeserializer = valueDeserializer
    });
```

The table name in the read relation becomes the topic name the source will read from. 

The following key deserializers exist:

* **FlowtideKafkaJsonKeyDeserializer** - deserializes a json key
* **FlowtidekafkaStringKeyDeserializer** - deserializes a string key

The following value deserializers exist:

* **FlowtideKafkaUpsertJsonDeserializer** - Deserializes the value as json, if the value is not null, its an upsert, if it is null, its a delete.

The source has a special column name for the key, it is called *_key* which can be used to select the key value from a kafka message.

### Usage in SQL

```sql
CREATE TABLE my_kafka_topic (
    _key,
    firstName,
    lastName
);

INSERT INTO outputtable
SELECT _key, firstName, lastName
FROM my_kafka_topic;
```

## Sink

The kafka sink allows a stream to write events to kafka.

The sink is added to the read write factory with the following line:

```csharp
factory.AddKafkaSink("your regexp on table names",  new FlowtideKafkaSinkOptions()
    {
        KeySerializer = new FlowtideKafkaStringKeySerializer(),
        ProducerConfig = config,
        ValueSerializer = new FlowtideKafkaUpsertJsonSerializer()
    });
```

Same as the source, it writes to the topic name that is entered in the write relation.

Available key serializers:

* **FlowtideKafkaJsonKeySerializer** - JSON serializes the key value
* **FlowtideKafkaStringKeySerializer** - Outputs a string value, only works if the key is in a string type.

Available value serializers:

* **FlowtideKafkaUpsertJsonSerializer** - Outputs the value as json, if it is a delete, it outputs null as the value.