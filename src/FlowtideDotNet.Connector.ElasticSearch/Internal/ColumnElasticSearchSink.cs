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

using Elasticsearch.Net;
using FlowtideDotNet.Base;
using FlowtideDotNet.Connector.ElasticSearch.Exceptions;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Nest;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.ElasticSearch.Internal
{
    internal class ElasticState : ColumnWriteState
    {

    }

    internal class ColumnElasticSearchSink : ColumnGroupedWriteOperator<ElasticState>
    {
        private static byte NewlineChar = Encoding.UTF8.GetBytes("\n")[0];
        private readonly WriteRelation m_writeRelation;
        private readonly FlowtideElasticsearchOptions m_elasticsearchOptions;
        private ElasticClient? m_client;
        private readonly ColumnToJsonElastic m_serializer;
        private readonly IReadOnlyList<int> m_primaryKeys;
        private readonly string m_displayName;
        private readonly string m_indexName;

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

        protected override ElasticState Checkpoint(long checkpointTime)
        {
            return new ElasticState();
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

        internal void CreateIndexAndMappings()
        {
            var client = new ElasticClient(m_elasticsearchOptions.ConnectionSettings);

            var existingIndex = client.Indices.Get(m_indexName);
            IndexState? indexState = default;
            IProperties? properties = null;
            if (existingIndex != null && existingIndex.IsValid && existingIndex.Indices.TryGetValue(m_indexName, out indexState))
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
                var response = client.Indices.Create(m_indexName);
                if (!response.IsValid)
                {
                    throw new FlowtideElasticsearchResponseException(response);
                }
            }

            var mapResponse = client.Map(new PutMappingRequest(m_indexName)
            {
                Properties = properties
            });

            if (!mapResponse.IsValid)
            {
                throw new FlowtideElasticsearchResponseException(mapResponse);
            }
        }

        protected override Task InitializeOrRestore(long restoreTime, ElasticState? state, IStateManagerClient stateManagerClient)
        {
            m_client = new ElasticClient(m_elasticsearchOptions.ConnectionSettings);
            return base.InitializeOrRestore(restoreTime, state, stateManagerClient);
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_serializer != null);

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

                    var response = await m_client.LowLevel.BulkAsync<BulkResponse>(PostData.ReadOnlyMemory(memoryStream.ToArray()), ctx: cancellationToken);

                    if (response.Errors)
                    {
                        foreach (var itemWithError in response.ItemsWithErrors)
                        {
                            Logger.ElasticSearchInsertError(itemWithError.Error.ToString(), StreamName, Name);
                        }
                        throw new InvalidOperationException("Error in elasticsearch sink");
                    }
                    if (response.OriginalException != null)
                    {
                        throw response.OriginalException;
                    }
                    if (!response.ApiCall.Success)
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
                var response = await m_client.LowLevel.BulkAsync<BulkResponse>(PostData.ReadOnlyMemory(memoryStream.ToArray()), ctx: cancellationToken);

                if (response.Errors)
                {
                    foreach (var itemWithError in response.ItemsWithErrors)
                    {
                        Logger.ElasticSearchInsertError(itemWithError.Error.ToString(), StreamName, Name);
                    }
                    throw new InvalidOperationException("Error in elasticsearch sink");
                }
                if (response.OriginalException != null)
                {
                    throw response.OriginalException;
                }
                if (!response.ApiCall.Success)
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
