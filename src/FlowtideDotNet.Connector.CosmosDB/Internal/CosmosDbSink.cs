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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Azure.Cosmos;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.CosmosDB.Internal
{
    internal class CosmosDbSink : GroupedWriteBaseOperator
    {
        private readonly FlowtideCosmosOptions cosmosOptions;
        private readonly WriteRelation writeRelation;
        private IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>? m_modified;
        private bool m_hasModified;
        private Container? m_container;
        private Func<RowEvent, PartitionKey>? m_eventToPartitionKey;
        private IReadOnlyList<int>? m_primaryKeys;
        private StreamEventToJsonCosmos? m_serializer;
        private int idIndex;
        private CosmosClient? m_cosmosClient;
        private ICounter<long>? _eventsProcessed;

        public CosmosDbSink(FlowtideCosmosOptions cosmosOptions, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.cosmosOptions = cosmosOptions;
            this.writeRelation = writeRelation;
        }

        public override string DisplayName => "CosmosDB Sink";

        protected override async Task Checkpoint(long checkpointTime)
        {
            await FlushAsync();
        }

        private async Task CompleteTasks(List<Task<ResponseMessage>> tasks, List<MemoryStream> activeStreams)
        {
            try
            {
                var responses = await Task.WhenAll(tasks);
                foreach (var response in responses)
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new InvalidOperationException($"CosmosDB transaction failed with status code {response.StatusCode} and error message: {response.ErrorMessage}");
                    }
                }
                tasks.Clear();
            }
            finally
            {
                // Always dispose the memory streams to not have memory leaks
                foreach (var stream in activeStreams)
                {
                    stream.Dispose();
                }
                activeStreams.Clear();
            }

        }

        private async Task FlushAsync()
        {
            if (!m_hasModified)
            {
                return;
            }
            Debug.Assert(m_eventToPartitionKey != null);
            Debug.Assert(m_modified != null);
            Debug.Assert(m_container != null);
            Debug.Assert(m_serializer != null);

            Logger.StartingCosmosDBUpdate(StreamName, Name);
            var iterator = m_modified.CreateIterator();
            await iterator.SeekFirst();

            List<MemoryStream> activeStreams = new List<MemoryStream>();
            List<Task<ResponseMessage>> tasks = new List<Task<ResponseMessage>>();
            int operationCount = 0;
            await foreach (var page in iterator)
            {

                foreach (var kv in page)
                {
                    operationCount++;
                    var pk = m_eventToPartitionKey(kv.Key);

                    var (rows, isDeleted) = await this.GetGroup(kv.Key);


                    if (rows.Count > 1)
                    {
                        var lastRow = rows.Last();
                        MemoryStream stream = new MemoryStream();
                        m_serializer.Write(stream, lastRow);
                        tasks.Add(m_container.UpsertItemStreamAsync(stream, pk));
                        activeStreams.Add(stream);
                    }
                    else if (rows.Count == 1)
                    {

                        MemoryStream stream = new MemoryStream();
                        m_serializer.Write(stream, rows[0]);
                        tasks.Add(m_container.UpsertItemStreamAsync(stream, pk));
                        activeStreams.Add(stream);
                    }
                    else if (isDeleted)
                    {
                        var idString = GetIdValue(kv.Key);
                        tasks.Add(m_container.DeleteItemStreamAsync(idString, pk));
                    }
                    if (tasks.Count >= 1000)
                    {
                        await CompleteTasks(tasks, activeStreams);
                    }
                }
            }

            if (tasks.Count > 0)
            {
                await CompleteTasks(tasks, activeStreams);
            }

            // Clear the modified table
            await m_modified.Clear();
            m_hasModified = false;
            Logger.CosmosDBUpdateComplete(StreamName, Name, operationCount);
        }

        private string GetIdValue(RowEvent streamEvent)
        {
            var idColumn = streamEvent.GetColumn(idIndex);

            if (idColumn.ValueType == FlexBuffers.Type.Null)
            {
                return "null";
            }
            if (idColumn.ValueType == FlexBuffers.Type.Int)
            {
                return idColumn.AsLong.ToString();
            }
            if (idColumn.ValueType == FlexBuffers.Type.Uint)
            {
                return idColumn.AsULong.ToString();
            }
            if (idColumn.ValueType == FlexBuffers.Type.Bool)
            {
                return idColumn.AsBool.ToString();
            }
            if (idColumn.ValueType == FlexBuffers.Type.Float)
            {
                return idColumn.AsDouble.ToString();
            }
            if (idColumn.ValueType == FlexBuffers.Type.String)
            {
                return idColumn.AsString;
            }
            throw new InvalidOperationException("Could not parse id field");
        }

        private async Task LoadMetadata()
        {
            m_cosmosClient = new CosmosClient(cosmosOptions.ConnectionString, new CosmosClientOptions()
            {
                MaxRetryAttemptsOnRateLimitedRequests = 100,
                AllowBulkExecution = true,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(120)
            });
            m_container = m_cosmosClient.GetContainer(cosmosOptions.DatabaseName, cosmosOptions.ContainerName);
            var containerProperties = await m_container.ReadContainerAsync();
            var partitionKeyPath = containerProperties.Resource.PartitionKeyPath;

            if (partitionKeyPath.StartsWith('/'))
            {
                partitionKeyPath = partitionKeyPath.Substring(1);
            }
            var keySplitted = partitionKeyPath.Split('/');

            if (keySplitted.Length > 1)
            {
                throw new InvalidOperationException("CosmosDB sink only supports partition keys in the root of the document at this time.");
            }
            var initialPath = keySplitted[0];

            var pkIndex = writeRelation.TableSchema.Names.IndexOf(initialPath);
            if (pkIndex < 0)
            {
                throw new Exception($"Could not find partition key '{initialPath}' in the insert query.");
            }
            else
            {
                m_eventToPartitionKey = CreatePartitionKeyExtractFunction(pkIndex);
            }


            // Find the id field
            List<int> primaryKeyIndices = new List<int>();
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                if (writeRelation.TableSchema.Names[i].Equals("id", StringComparison.OrdinalIgnoreCase))
                {
                    idIndex = i;
                    primaryKeyIndices.Add(i);
                }
            }
            m_primaryKeys = primaryKeyIndices;
            m_serializer = new StreamEventToJsonCosmos(writeRelation.TableSchema.Names, idIndex);
        }

        protected override async ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            if (m_primaryKeys == null)
            {
                await LoadMetadata();
            }
            Debug.Assert(m_primaryKeys != null);
            return m_primaryKeys;
        }

        private Func<RowEvent, PartitionKey> CreatePartitionKeyExtractFunction(int index)
        {
            return (e) =>
            {
                var val = e.GetColumn(index);
                if (val.IsNull)
                {
                    return new PartitionKey(default(string));
                }
                else if (val.ValueType == FlexBuffers.Type.String)
                {
                    return new PartitionKey(val.AsString);
                }
                else if (val.ValueType == FlexBuffers.Type.Float)
                {
                    return new PartitionKey(val.AsDouble);
                }
                else if (val.ValueType == FlexBuffers.Type.Bool)
                {
                    return new PartitionKey(val.AsBool);
                }
                else if (val.ValueType == FlexBuffers.Type.Int)
                {
                    return new PartitionKey(val.AsLong);
                }
                else if (val.ValueType == FlexBuffers.Type.Map)
                {
                    return new PartitionKey(val.ToJson);
                }
                else
                {
                    throw new InvalidOperationException("Partition key type is not supported");
                }
            };
        }

        protected override async Task Initialize(long restoreTime, FlowtideDotNet.Storage.StateManager.IStateManagerClient stateManagerClient)
        {
            if (m_cosmosClient == null)
            {
                await LoadMetadata();
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            m_modified = await stateManagerClient.GetOrCreateTree("temporary",
                new Storage.Tree.BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<RowEvent>(PrimaryKeyComparer!),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                    MemoryAllocator = MemoryAllocator
                });

            // Clear the modified tree in case of a crash
            await m_modified.Clear();
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(msg.Events.Count);

            foreach (var e in msg.Events)
            {
                // Add the row to permanent storage
                await this.Insert(e);
                m_hasModified = true;
                // Add the row to the modified storage to keep track on which rows where changed
                await m_modified.Upsert(e, 0);
            }
        }

        public override ValueTask DisposeAsync()
        {
            if (m_cosmosClient != null)
            {
                m_cosmosClient.Dispose();
                m_cosmosClient = null;
            }

            return base.DisposeAsync();
        }
    }
}
