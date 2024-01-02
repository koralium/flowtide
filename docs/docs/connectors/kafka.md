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

### Extend event processing logic

There are two properties in the options that can help add extra logic to the kafka sink.

* **EventProcessor** - Called on all events that will be sent to kafka.
* **OnInitialDataSent** - Called once on a new stream when the first data has been sent (usually after the first checkpoint).

These functions can help implement logic such as having an external state store for a kafka topic that keeps track of all the data
that has been sent to the topic. If a stream is modified and republished and has to start from the beginning, this state store
can then make sure only delta valeus are sent to the kafka topic, and deletions of events that no longer exists.

Example:

```csharp
factory.AddKafkaSink("your regexp on table names",  new FlowtideKafkaSinkOptions()
    {
        ...
        EventProcessor = async (events) => {
            for (int i = 0; i < events.Count; i++) {
                // Check if the event exists with the exact key and value in the store
                var exists = await dbLookup.ExistAsync(events[i].Key, events[i].Value);
                if (exists) {
                    events.RemoveAt(i);
                    i--;
                }
                // Add the data with a run version that should be unique of this stream version.
                await dbLookup.Add(events[i].Key, events[i].Value, runVersion);
            }
        },
        OnInitialDataSent = (producer, writeRelation, topicName) => {
            // Get all events that does not have the latest run version
            await foreach (var e in dbLookup.GetEventsNotMatchingVersion(runVersion)) {
                // Send delete on all events that no longer exist
                await producer.SendAsync(new Message<byte[], byte[]?>() {
                    Key = e.Key,
                    Value = null
                });
            }
            // Delete the events from the state store. 
            await dbLookup.DeleteNotMatchingVersion(runVersion);
        }
    });
```