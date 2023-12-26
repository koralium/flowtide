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
using FlexBuffers;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Kafka.Internal
{
    internal class KafkaDataSourceState
    {
        public Dictionary<int, long>? PartitionOffsets { get; set; }
    }

    internal class KafkaDataSource : ReadBaseOperator<KafkaDataSourceState>
    {
        private readonly ReadRelation readRelation;
        private readonly FlowtideKafkaSourceOptions flowtideKafkaOptions;
        private IReadOnlySet<string>? _watermarkNames;
        private KafkaDataSourceState? _state;
        private IConsumer<byte[], byte[]>? _consumer;
        private List<TopicPartition> _topicPartitions;
        private IFlowtideKafkaDeserializer _valueDeserializer;
        private readonly FlexBuffer _flexBuffer;
        private IFlowtideKafkaKeyDeserializer _keyDeserializer;
        private readonly string topicName;

        public KafkaDataSource(ReadRelation readRelation, FlowtideKafkaSourceOptions flowtideKafkaOptions, DataflowBlockOptions options) : base(options)
        {
            topicName = readRelation.NamedTable.DotSeperated;
            this.readRelation = readRelation;
            this.flowtideKafkaOptions = flowtideKafkaOptions;
            _valueDeserializer = flowtideKafkaOptions.ValueDeserializer;
            _keyDeserializer = flowtideKafkaOptions.KeyDeserializer;

            _flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);

            _topicPartitions = new List<TopicPartition>();
        }

        public override string DisplayName => "Kafka Source";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            Debug.Assert(_watermarkNames != null);
            return Task.FromResult(_watermarkNames);
        }

        protected override async Task InitializeOrRestore(long restoreTime, KafkaDataSourceState? state, IStateManagerClient stateManagerClient)
        {
            await _valueDeserializer.Initialize(readRelation);
            if (state == null)
            {
                state = new KafkaDataSourceState()
                {
                    PartitionOffsets = new Dictionary<int, long>()
                };
            }
            _state = state;

            var adminConf = new AdminClientConfig(flowtideKafkaOptions.ConsumerConfig);

            var adminClient = new AdminClientBuilder(adminConf).Build();
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            HashSet<string> watermarkNames = new HashSet<string>();
            var topic = metadata.Topics.First();
            var partitions = metadata.Topics.First().Partitions;
            List<TopicPartitionOffset> topicPartitionOffsets = new List<TopicPartitionOffset>();
            foreach(var partition in partitions)
            {
                watermarkNames.Add(topicName + "_" + partition.PartitionId);

                var topicPartition = new TopicPartition(topic.Topic, new Partition(partition.PartitionId));
                _topicPartitions.Add(topicPartition);
                // Add the partition offset to the list of partitions to consume from
                if (_state.PartitionOffsets.TryGetValue(partition.PartitionId, out var offset))
                {
                    topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartition, new Offset(offset)));
                }
                else
                {
                    topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartition, new Offset(0)));
                }
            }
            _watermarkNames = watermarkNames;

            var conf = flowtideKafkaOptions.ConsumerConfig;
            _consumer = new ConsumerBuilder<byte[], byte[]>(conf).Build();

            _consumer.Assign(topicPartitionOffsets);
        }

        protected override Task<KafkaDataSourceState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            return Task.FromResult(_state);
        }

        private async Task LoadChangesTask(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.PartitionOffsets != null);
            Debug.Assert(_consumer != null);

            List<RowEvent> rows = new List<RowEvent>();
            int waitTimeMs = 100;

            bool inLock = false;
            while (!output.CancellationToken.IsCancellationRequested)
            {
                output.CancellationToken.ThrowIfCancellationRequested();

                var result = _consumer.Consume(TimeSpan.FromMilliseconds(waitTimeMs));

                if (result != null)
                {
                    await output.EnterCheckpointLock();
                    inLock = true;
                    _state.PartitionOffsets[result.Partition.Value] = result.Offset.Value;

                    // Parse the result
                    var ev = this._valueDeserializer.Deserialize(_keyDeserializer, result.Message.Value, result.Message.Key);
                    rows.Add(ev);
                    // Wait at most 1ms between fetches, to make sure latency is as low as possible
                    waitTimeMs = 1;
                    if (rows.Count > 100)
                    {
                        await output.SendAsync(new StreamEventBatch(null, rows));
                        rows = new List<RowEvent>();
                        await SendWatermark(output);
                    }
                }
                else
                {
                    if (rows.Count > 0)
                    {
                        await output.SendAsync(new StreamEventBatch(null, rows));
                        rows = new List<RowEvent>();
                        await SendWatermark(output);
                    }
                    if (inLock)
                    {
                        ScheduleCheckpoint(TimeSpan.FromMilliseconds(0));
                        output.ExitCheckpointLock();
                        inLock = false;
                    }
                    // Reset wait time
                    waitTimeMs = 100;
                }
            }
            if (inLock)
            {
                output.ExitCheckpointLock();
                inLock = false;
            }
        }

        private async Task SendWatermark(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.PartitionOffsets != null);
            var watermark = new Dictionary<string, long>();
            foreach (var kv in _state.PartitionOffsets)
            {
                watermark.Add(topicName + "_" + kv.Key, kv.Value);
            }
            await output.SendWatermark(new Base.Watermark(watermark.ToImmutableDictionary()));
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.PartitionOffsets != null);
            Debug.Assert(_consumer != null);

            await output.EnterCheckpointLock();
            Dictionary<int, long> beforeStartOffsets = new Dictionary<int, long>();
            //Dictionary<int, long> currentOffsets = new Dictionary<int, long>();
            foreach(var topicPartition in _topicPartitions)
            {
                var offsets = _consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));
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

            List<RowEvent> rows = new List<RowEvent>();
            while (true)
            {
                output.CancellationToken.ThrowIfCancellationRequested();

                var result = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result != null)
                {
                    _state.PartitionOffsets[result.Partition.Value] = result.Offset.Value;

                    // Parse the result
                    var ev = this._valueDeserializer.Deserialize(_keyDeserializer, result.Message.Value, result.Message.Key);
                    rows.Add(ev);
                }
                
                if (result == null || rows.Count >= 100)
                {
                    await output.SendAsync(new StreamEventBatch(null, rows));
                    rows = new List<RowEvent>();
                    // Check offsets
                    bool offsetsReached = true;
                    foreach(var kv in beforeStartOffsets)
                    {
                        if (_state.PartitionOffsets.TryGetValue(kv.Key, out var currentOffset))
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

            if (rows.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(null, rows));
            }

            // Send watermark
            await SendWatermark(output);

            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(10));
            _ = RunTask(LoadChangesTask, taskCreationOptions: TaskCreationOptions.LongRunning);
        }
    }
}
