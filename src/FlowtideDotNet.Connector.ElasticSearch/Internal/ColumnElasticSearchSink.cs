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

using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Connector.ElasticSearch.Exceptions;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.ElasticSearch.Internal
{

    internal class ColumnElasticSearchSink : ColumnGroupedWriteOperator
    {
        private static byte NewlineChar = Encoding.UTF8.GetBytes("\n")[0];
        private readonly WriteRelation m_writeRelation;
        private readonly FlowtideElasticsearchOptions m_elasticsearchOptions;
        private ElasticsearchClient? m_client;
        private readonly ColumnToJsonElastic m_serializer;
        private readonly IReadOnlyList<int> m_primaryKeys;
        private readonly string m_displayName;
        private readonly string m_indexName;
        private ICounter<long>? m_eventsCounter;


        public ColumnElasticSearchSink(FlowtideElasticsearchOptions elasticsearchOptions, ExecutionMode executionMode, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            m_elasticsearchOptions = elasticsearchOptions;

            if (elasticsearchOptions.GetIndexNameFunc == null)
            {
                m_indexName = writeRelation.NamedObject.DotSeperated;
            }
            else
            {
                m_indexName = elasticsearchOptions.GetIndexNameFunc(writeRelation);
            }

            m_writeRelation = writeRelation;
            m_displayName = $"ElasticSearchSink-{m_indexName}";
            var idFieldIndex = FindUnderscoreIdField(writeRelation);
            m_primaryKeys = new List<int>() { idFieldIndex };
            m_serializer = new ColumnToJsonElastic(writeRelation.TableSchema.Names, idFieldIndex, m_indexName);
        }

        private static int FindUnderscoreIdField(WriteRelation writeRelation)
        {
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                if (writeRelation.TableSchema.Names[i].Equals("_id"))
                {
                    return i;
                }
            }
            throw new InvalidOperationException("No _id field found in table schema. It is a required field for elasticsearch sink");
        }

        public override string DisplayName => m_displayName;

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult(m_primaryKeys);
        }

        protected override Task OnInitialDataSent()
        {
            if (m_elasticsearchOptions.OnInitialDataSent != null)
            {
                return m_elasticsearchOptions.OnInitialDataSent(m_client!, m_writeRelation, m_indexName);
            }
            return base.OnInitialDataSent();
        }

        internal async Task CreateIndexAndMappings()
        {
            var client = new ElasticsearchClient(m_elasticsearchOptions.ConnectionSettings());

            var existingIndex = await client.Indices.GetAsync(m_indexName);
            IndexState? indexState = default;
            Properties? properties = null;

            if (existingIndex != null && existingIndex.IsValidResponse && existingIndex.Indices.TryGetValue(m_indexName, out indexState))
            {
                properties = indexState.Mappings.Properties ?? new Properties();
            }
            else
            {
                properties = new Properties();
            }

            if (m_elasticsearchOptions.CustomMappings != null)
            {
                m_elasticsearchOptions.CustomMappings(properties);
            }

            if (indexState == null)
            {
                var response = await client.Indices.CreateAsync(m_indexName);
                if (!response.IsValidResponse)
                {
                    throw new FlowtideElasticsearchResponseException(response);
                }
            }

            var mapResponse = await client.Indices.PutMappingAsync(new PutMappingRequest(m_indexName)
            {
                Properties = properties
            });

            if (!mapResponse.IsValidResponse)
            {
                throw new FlowtideElasticsearchResponseException(mapResponse);
            }
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            if (m_eventsCounter == null)
            {
                m_eventsCounter = Metrics.CreateCounter<long>("events");
            }
            m_client = new ElasticsearchClient(m_elasticsearchOptions.ConnectionSettings());
            return base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_serializer != null);
            Debug.Assert(m_eventsCounter != null);

            // Create a new client for each upload changes, this is to allow using new credentials if they exist
            m_client = new ElasticsearchClient(m_elasticsearchOptions.ConnectionSettings());

            int batchCount = 0;
            using MemoryStream memoryStream = new MemoryStream();
            Utf8JsonWriter jsonWriter = new Utf8JsonWriter(memoryStream);
            await foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!row.IsDeleted)
                {
                    m_serializer.WriteIndexUpsertMetadata(in jsonWriter, row.EventBatchData, row.Index);
                    jsonWriter.Reset();
                    memoryStream.WriteByte(NewlineChar);
                    m_serializer.WriteObject(in jsonWriter, row.EventBatchData, row.Index);
                    jsonWriter.Reset();
                    memoryStream.WriteByte(NewlineChar);
                }
                else
                {
                    m_serializer.WriteIndexDeleteMetadata(in jsonWriter, row.EventBatchData, row.Index);
                    jsonWriter.Reset();
                    memoryStream.WriteByte(NewlineChar);
                }
                batchCount++;
                if (batchCount >= 1000)
                {
                    m_eventsCounter.Add(batchCount);
                    BulkResponse? response;
                    try
                    {
                        response = await m_client.Transport.PostAsync<BulkResponse>("_bulk", PostData.ReadOnlyMemory(memoryStream.ToArray()));
                        //response = await m_client.LowLevel.BulkAsync<BulkResponse>(PostData.ReadOnlyMemory(memoryStream.ToArray()), ctx: cancellationToken);
                    }
                    catch(Exception e)
                    {
                        if (e is TaskCanceledException)
                        {
                            throw new Exception("Upload to elasticsearch was cancelled", e);
                        }
                        throw;
                    }
                    

                    if (response.Errors)
                    {
                        foreach (var itemWithError in response.ItemsWithErrors)
                        {
                            if (itemWithError.Error != null)
                            {
                                Logger.ElasticSearchInsertError(itemWithError.Error.ToString()!, StreamName, Name);
                            }
                        }
                        throw new InvalidOperationException("Error in elasticsearch sink");
                    }
                    if (response.TryGetOriginalException(out var originalException))
                    {
                        throw originalException!;
                    }
                    if (!response.IsSuccess())
                    {
                        throw new InvalidOperationException("Error in elasticsearch sink");
                    }

                    memoryStream.Position = 0;
                    memoryStream.SetLength(0);
                    jsonWriter.Reset();
                    batchCount = 0;
                }
            }

            if (batchCount > 0)
            {
                m_eventsCounter.Add(batchCount);
                var response = await m_client.Transport.PostAsync<BulkResponse>("_bulk", PostData.ReadOnlyMemory(memoryStream.ToArray()));
                //var response = await m_client.LowLevel.BulkAsync<BulkResponse>(PostData.ReadOnlyMemory(memoryStream.ToArray()), ctx: cancellationToken);

                if (response.Errors)
                {
                    foreach (var itemWithError in response.ItemsWithErrors)
                    {
                        if (itemWithError.Error != null)
                        {
                            Logger.ElasticSearchInsertError(itemWithError.Error.ToString()!, StreamName, Name);
                        }
                    }
                    throw new InvalidOperationException("Error in elasticsearch sink");
                }
                if (response.TryGetOriginalException(out var originalException))
                {
                    throw originalException!;
                }
                if (!response.IsSuccess())
                {
                    throw new InvalidOperationException("Error in elasticsearch sink");
                }
            }

            if (m_elasticsearchOptions.OnDataSent != null)
            {
                await m_elasticsearchOptions.OnDataSent(m_client, m_writeRelation, m_indexName, watermark);
            }
        }
    }
}
