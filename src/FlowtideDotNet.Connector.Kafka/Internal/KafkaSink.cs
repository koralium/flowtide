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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Kafka.Internal
{
    internal class KafkaSink : ColumnGroupedWriteOperator
    {
        private readonly FlowtideKafkaSinkOptions _flowtideKafkaSinkOptions;
        private readonly WriteRelation _writeRelation;
        private IProducer<byte[], byte[]?>? _producer;
        private readonly IReadOnlyList<int> _primaryKeys;
        private readonly int _primaryKeyIndex;
        private readonly string topicName;

        public KafkaSink(FlowtideKafkaSinkOptions flowtideKafkaSinkOptions, ExecutionMode executionMode, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            this._flowtideKafkaSinkOptions = flowtideKafkaSinkOptions;
            this._writeRelation = writeRelation;
            if ((flowtideKafkaSinkOptions.FetchExistingKeyDeserializer != null ||
               flowtideKafkaSinkOptions.FetchExistingValueDeserializer != null ||
               flowtideKafkaSinkOptions.FetchExistingConfig != null) &&
               (flowtideKafkaSinkOptions.FetchExistingValueDeserializer == null ||
               flowtideKafkaSinkOptions.FetchExistingKeyDeserializer == null ||
               flowtideKafkaSinkOptions.FetchExistingConfig == null))
            {
                throw new InvalidOperationException("FetchExistingConfig, FetchExistingKeyDeserializer and FetchExistingValueDeserializer must be set or all must be null");
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
        }

        public override string DisplayName => "Kafka Sink";

        protected override void Checkpoint(long checkpointTime)
        {
            // No checkpointing required for KafkaSink as it does not maintain operator state.
        }

        protected override bool FetchExistingData => _flowtideKafkaSinkOptions.FetchExistingConfig != null;

        protected override IAsyncEnumerable<EventBatchData> GetExistingData()
        {
            if (_flowtideKafkaSinkOptions.FetchExistingConfig == null)
            {
                throw new InvalidOperationException("FetchExistingConfig must be set for the kafka sink");
            }
            if (_flowtideKafkaSinkOptions.FetchExistingValueDeserializer == null)
            {
                throw new InvalidOperationException("FetchExistingValueDeserializer must be set for the kafka sink");
            }
            if (_flowtideKafkaSinkOptions.FetchExistingKeyDeserializer == null)
            {
                throw new InvalidOperationException("FetchExistingKeyDeserializer must be set for the kafka sink");
            }

            var client = new KafkaReadClient(
                _flowtideKafkaSinkOptions.FetchExistingConfig,
                topicName,
                _flowtideKafkaSinkOptions.FetchExistingValueDeserializer,
                _flowtideKafkaSinkOptions.FetchExistingKeyDeserializer);

            return client.ReadInitial(_writeRelation.TableSchema.Names.Count, MemoryAllocator).ToAsyncEnumerable();
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult(_primaryKeys);
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
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
            await foreach (var row in rows)
            {
                var rowReference = new ColumnRowReference() { referenceBatch = row.EventBatchData, RowIndex = row.Index };

                if (row.IsDeleted && isInitialData && FetchExistingData)
                {
                    // Skip deletes on initial load if fetching existing data
                    // Since it can be tombstones for data that is not present in the source
                    continue;
                }

                if (FetchExistingData && !row.IsDeleted)
                {
                    // Compare against existing
                    var (exist, existingVal) = await GetExistingDataRow(rowReference);
                    // Check if the rows completely match, then do nothing since the data is already in the stream
                    
                    if (exist && ExistingRowComparer.CompareTo(existingVal, rowReference) == 0)
                    {
                        continue;
                    }
                }
                var key = _flowtideKafkaSinkOptions.KeySerializer.Serialize(row.EventBatchData.Columns[_primaryKeyIndex].GetValueAt(row.Index, default));
                var val = _flowtideKafkaSinkOptions.ValueSerializer.Serialize(rowReference, row.IsDeleted);

                if (Volatile.Read(ref errorsReceived) > 0)
                {
                    throw new InvalidOperationException($"Error when inserting to kafka with error: {lastSeentError!.Reason}");
                }

                output.Add(new KeyValuePair<byte[], byte[]?>(key, val));

                if (output.Count > 100)
                {
                    if (_flowtideKafkaSinkOptions.EventProcessor != null)
                    {
                        await _flowtideKafkaSinkOptions.EventProcessor(output);
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
                if (_flowtideKafkaSinkOptions.EventProcessor != null)
                {
                    await _flowtideKafkaSinkOptions.EventProcessor(output);
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

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await _flowtideKafkaSinkOptions.ValueSerializer.Initialize(_writeRelation);
            _producer = new ProducerBuilder<byte[], byte[]?>(_flowtideKafkaSinkOptions.ProducerConfig).Build();

            if (_flowtideKafkaSinkOptions.FetchExistingConfig != null)
            {
                if (_flowtideKafkaSinkOptions.FetchExistingValueDeserializer == null)
                {
                    throw new InvalidOperationException("FetchExistingValueDeserializer must be set for the kafka sink");
                }
                var readRel = new ReadRelation()
                {
                    BaseSchema = _writeRelation.TableSchema,
                    NamedTable = _writeRelation.NamedObject
                };
                await _flowtideKafkaSinkOptions.FetchExistingValueDeserializer.Initialize(readRel);
            }

            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

    }
}
