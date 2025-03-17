// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Confluent.Kafka;
using FlowtideDotNet.Core;

namespace FlowtideDotNet.Connector.Kafka.Internal
{
    public class KafkaReadClient
    {
        private readonly IFlowtideKafkaDeserializer valueDeserializer;
        private readonly IFlowtideKafkaKeyDeserializer keyDeserializer;
        private readonly IConsumer<byte[], byte[]> _consumer;
        private readonly List<TopicPartition> _topicPartitions = new List<TopicPartition>();

        public KafkaReadClient(
            ConsumerConfig consumerConfig, 
            string topicName,
            IFlowtideKafkaDeserializer valueDeserializer,
            IFlowtideKafkaKeyDeserializer keyDeserializer)
        {
            var adminConf = new AdminClientConfig(consumerConfig);

            var adminClient = new AdminClientBuilder(adminConf).Build();
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics[0];
            var partitions = metadata.Topics[0].Partitions;
            List<TopicPartitionOffset> topicPartitionOffsets = new List<TopicPartitionOffset>();
            foreach (var partition in partitions)
            {
                var topicPartition = new TopicPartition(topic.Topic, new Partition(partition.PartitionId));
                _topicPartitions.Add(topicPartition);
                topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartition, new Offset(0)));
            }

            _consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();

            _consumer.Assign(topicPartitionOffsets);
            this.valueDeserializer = valueDeserializer;
            this.keyDeserializer = keyDeserializer;
        }

        internal static Dictionary<int, long> GetCurrentWatermarks(IConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions)
        {
            Dictionary<int, long> beforeStartOffsets = new Dictionary<int, long>();
            foreach (var topicPartition in topicPartitions)
            {
                var offsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));
                var offset = offsets.High.Value - 1;
                if (beforeStartOffsets.TryGetValue(topicPartition.Partition.Value, out var currentOffset))
                {
                    if (currentOffset < offset)
                    {
                        beforeStartOffsets[topicPartition.Partition.Value] = offset;
                    }
                }
                else
                {
                    beforeStartOffsets.Add(topicPartition.Partition.Value, offset);
                }
            }
            return beforeStartOffsets;
        }

        public IEnumerable<RowEvent> ReadInitial()
        {
            Dictionary<int, long> currentOffsets = new Dictionary<int, long>();
            Dictionary<int, long> beforeStartOffsets = GetCurrentWatermarks(_consumer, _topicPartitions);

            // Set all the partition offsets to start at -1 incase there is no data.
            foreach(var kv in beforeStartOffsets)
            {
                currentOffsets[kv.Key] = -1;
            }

            while (true)
            {
                var result = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result != null)
                {
                    // Parse the result
                    currentOffsets[result.Partition.Value] = result.Offset.Value;
                    var ev = this.valueDeserializer.Deserialize(keyDeserializer, result.Message.Value, result.Message.Key);
                    yield return ev;
                }
                if (result == null)
                {
                    bool offsetsReached = true;
                    foreach (var kv in beforeStartOffsets)
                    {
                        if (currentOffsets.TryGetValue(kv.Key, out var currentOffset))
                        {
                            if (currentOffset < kv.Value)
                            {
                                offsetsReached = false;
                                break;
                            }
                        }
                        else
                        {
                            offsetsReached = false;
                        }
                    }
                    if (offsetsReached)
                    {
                        break;
                    }
                }
            }
        }
    }
}
