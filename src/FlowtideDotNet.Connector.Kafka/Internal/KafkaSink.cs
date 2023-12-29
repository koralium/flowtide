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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Kafka.Internal
{
    internal class KafkaSink : SimpleGroupedWriteOperator
    {
        private readonly WriteRelation _writeRelation;
        private readonly FlowtideKafkaSinkOptions flowtideKafkaSinkOptions;
        private IProducer<byte[], byte[]> _producer;
        private readonly IReadOnlyList<int> _primaryKeys;
        private readonly int _primaryKeyIndex;
        private readonly string topicName;

        public KafkaSink(WriteRelation writeRelation, FlowtideKafkaSinkOptions flowtideKafkaSinkOptions, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, executionDataflowBlockOptions)
        {
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
            _producer = new ProducerBuilder<byte[], byte[]>(flowtideKafkaSinkOptions.ProducerConfig).Build();
            return new MetadataResult(_primaryKeys);
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            var errorsReceived = 0;
            Error? lastSeentError = default;

            void DeliveryHandler(DeliveryReport<byte[], byte[]> report)
            {
                if (report.Error.IsError)
                {
                    lastSeentError = report.Error;
                    Interlocked.Increment(ref errorsReceived);
                }
            }

            await foreach(var row in rows)
            {
                var key = flowtideKafkaSinkOptions.KeySerializer.Serialize(row.Row.GetColumn(_primaryKeyIndex));
                var val = flowtideKafkaSinkOptions.ValueSerializer.Serialize(row.Row, row.IsDeleted);

                if (Volatile.Read(ref errorsReceived) > 0)
                {
                    throw new InvalidOperationException($"Error when inserting to kafka with error: {lastSeentError!.Reason}");
                }

                _producer.Produce(topicName, new Message<byte[], byte[]>()
                {
                    Key = key,
                    Value = val
                }, DeliveryHandler);
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
