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
using FlowtideDotNet.Base;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Kafka.Internal
{
    internal class KafkaSink : SimpleGroupedWriteOperator
    {
        private readonly WriteRelation _writeRelation;
        private readonly FlowtideKafkaSinkOptions flowtideKafkaSinkOptions;
        private IProducer<byte[], byte[]?>? _producer;
        private readonly IReadOnlyList<int> _primaryKeys;
        private readonly int _primaryKeyIndex;
        private readonly string topicName;

        public KafkaSink(WriteRelation writeRelation, FlowtideKafkaSinkOptions flowtideKafkaSinkOptions, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, executionDataflowBlockOptions)
        {
            if ((flowtideKafkaSinkOptions.FetchExistingKeyDeserializer != null || flowtideKafkaSinkOptions.FetchExistingValueDeserializer != null) &&
                (flowtideKafkaSinkOptions.FetchExistingValueDeserializer == null || flowtideKafkaSinkOptions.FetchExistingKeyDeserializer == null))
            {
                throw new InvalidOperationException("Both FetchExistingKeyDeserializer and FetchExistingValueDeserializer must be set or both must be null");
            }
            int keyIndex = -1;
            topicName = writeRelation.NamedObject.DotSeperated;
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                if (writeRelation.TableSchema.Names[i].ToLower() == "_key")
                {
                    keyIndex = i;
                    break;
                }
            }
            if (keyIndex == -1)
            {
                throw new InvalidOperationException("_key must be present in the table schema for the kafka sink");
            }
            _primaryKeys = new List<int>() { keyIndex };
            _primaryKeyIndex = keyIndex;
            _writeRelation = writeRelation;
            this.flowtideKafkaSinkOptions = flowtideKafkaSinkOptions;
        }

        public override string DisplayName => "Kafka Sink";

        protected override async Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            await flowtideKafkaSinkOptions.ValueSerializer.Initialize(_writeRelation);
            _producer = new ProducerBuilder<byte[], byte[]?>(flowtideKafkaSinkOptions.ProducerConfig).Build();

            if (flowtideKafkaSinkOptions.FetchExistingConfig != null)
            {
                var readRel = new ReadRelation()
                {
                    BaseSchema = _writeRelation.TableSchema,
                    NamedTable = _writeRelation.NamedObject
                };
                await flowtideKafkaSinkOptions.FetchExistingValueDeserializer.Initialize(readRel);
            }

            return new MetadataResult(_primaryKeys);
        }

        protected override async Task OnInitialDataSent()
        {
            Debug.Assert(_producer != null);
            if (flowtideKafkaSinkOptions.OnInitialDataSent != null)
            {
                await flowtideKafkaSinkOptions.OnInitialDataSent(_producer, _writeRelation, topicName);
            }
        }

        protected override bool FetchExistingData => flowtideKafkaSinkOptions.FetchExistingConfig != null;

        protected override IAsyncEnumerable<RowEvent> GetExistingData()
        {
            if (flowtideKafkaSinkOptions.FetchExistingConfig == null)
            {
                throw new InvalidOperationException("FetchExistingConfig must be set for the kafka sink");
            }
            if (flowtideKafkaSinkOptions.FetchExistingValueDeserializer == null)
            {
                throw new InvalidOperationException("FetchExistingValueDeserializer must be set for the kafka sink");
            }
            if (flowtideKafkaSinkOptions.FetchExistingKeyDeserializer == null)
            {
                throw new InvalidOperationException("FetchExistingKeyDeserializer must be set for the kafka sink");
            }

            var client = new KafkaReadClient(
                flowtideKafkaSinkOptions.FetchExistingConfig,
                topicName,
                flowtideKafkaSinkOptions.FetchExistingValueDeserializer,
                flowtideKafkaSinkOptions.FetchExistingKeyDeserializer);

            return client.ReadInitial();
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(_producer != null);
            var errorsReceived = 0;
            Error? lastSeentError = default;

            void DeliveryHandler(DeliveryReport<byte[], byte[]?> report)
            {
                if (report.Error.IsError)
                {
                    lastSeentError = report.Error;
                    Interlocked.Increment(ref errorsReceived);
                }
            }

            List<KeyValuePair<byte[], byte[]?>> output = new List<KeyValuePair<byte[], byte[]?>>();
            await foreach(var row in rows)
            {
                if (FetchExistingData && !row.IsDeleted)
                {
                    // Compare against existing
                    var (exist, existingVal) = await GetExistingData(row.Row);
                    // Check if the rows completely match, then do nothing since the data is already in the stream
                    if (RowEvent.Compare(existingVal, row.Row) == 0)
                    {
                        continue;
                    }
                }
                var key = flowtideKafkaSinkOptions.KeySerializer.Serialize(row.Row.GetColumn(_primaryKeyIndex));
                var val = flowtideKafkaSinkOptions.ValueSerializer.Serialize(row.Row, row.IsDeleted);

                if (Volatile.Read(ref errorsReceived) > 0)
                {
                    throw new InvalidOperationException($"Error when inserting to kafka with error: {lastSeentError!.Reason}");
                }

                output.Add(new KeyValuePair<byte[], byte[]?>(key, val));

                if (output.Count > 100)
                {
                    if (flowtideKafkaSinkOptions.EventProcessor != null)
                    {
                        await flowtideKafkaSinkOptions.EventProcessor(output);
                    }
                    foreach (var kvp in output)
                    {
                        _producer.Produce(topicName, new Message<byte[], byte[]?>()
                        {
                            Key = kvp.Key,
                            Value = kvp.Value
                        }, DeliveryHandler);
                    }
                    output.Clear();
                }
            }

            if (output.Count > 0)
            {
                if (flowtideKafkaSinkOptions.EventProcessor != null)
                {
                    await flowtideKafkaSinkOptions.EventProcessor(output);
                }
                foreach (var kvp in output)
                {
                    _producer.Produce(topicName, new Message<byte[], byte[]?>()
                    {
                        Key = kvp.Key,
                        Value = kvp.Value
                    }, DeliveryHandler);
                }
                output.Clear();
            }

            int queue = 0;
            do
            {
                if (Volatile.Read(ref errorsReceived) > 0)
                {
                    throw new InvalidOperationException($"Error when inserting to kafka with error: {lastSeentError!.Reason}");
                }

                queue = _producer.Flush(TimeSpan.FromSeconds(10));
            } while (queue > 0);
        }
    }
}
