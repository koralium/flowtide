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

using Authzed.Api.V1;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Grpc.Core;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class ColumnSpiceDbSink : ColumnGroupedWriteOperator
    {
        private readonly int m_resourceObjectTypeIndex;
        private readonly int m_resourceObjectIdIndex;
        private readonly int m_relationIndex;
        private readonly int m_subjectObjectTypeIndex;
        private readonly int m_subjectObjectIdIndex;
        private readonly int m_subjectRelationIndex;
        private readonly List<int> m_primaryKeys;
        private readonly SpiceDbSinkOptions m_spiceDbSinkOptions;
        private readonly WriteRelation m_writeRelation;
        private PermissionsService.PermissionsServiceClient? m_client;
        private readonly ColumnSpiceDbRowEncoder? m_existingDataEncoder;
        private ICounter<long>? _eventsCounter;
        private readonly string m_displayName;
        private readonly DataValueContainer m_dataValueContainer;

        public ColumnSpiceDbSink(
            SpiceDbSinkOptions spiceDbSinkOptions,
            ExecutionMode executionMode, 
            WriteRelation writeRelation, 
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            m_resourceObjectTypeIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("resource_type", StringComparison.OrdinalIgnoreCase));
            if (m_resourceObjectTypeIndex == -1)
            {
                throw new InvalidOperationException("SpiceDB sink requires resource_type column");
            }
            m_resourceObjectIdIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("resource_id", StringComparison.OrdinalIgnoreCase));
            if (m_resourceObjectIdIndex == -1)
            {
                throw new InvalidOperationException("SpiceDB sink requires resource_id column");
            }
            m_relationIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("relation", StringComparison.OrdinalIgnoreCase));
            if (m_relationIndex == -1)
            {
                throw new InvalidOperationException("SpiceDB sink requires relation column");
            }
            m_subjectObjectTypeIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("subject_type", StringComparison.OrdinalIgnoreCase));
            if (m_subjectObjectTypeIndex == -1)
            {
                throw new InvalidOperationException("SpiceDB sink requires subject_type column");
            }
            m_subjectObjectIdIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("subject_id", StringComparison.OrdinalIgnoreCase));
            if (m_subjectObjectIdIndex == -1)
            {
                throw new InvalidOperationException("SpiceDB sink requires subject_id column");
            }
            m_subjectRelationIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("subject_relation", StringComparison.OrdinalIgnoreCase));

            m_primaryKeys = new List<int>() { m_resourceObjectTypeIndex, m_resourceObjectIdIndex, m_relationIndex, m_subjectObjectTypeIndex, m_subjectObjectIdIndex };
            this.m_spiceDbSinkOptions = spiceDbSinkOptions;
            this.m_writeRelation = writeRelation;
            if (m_spiceDbSinkOptions.DeleteExistingDataFilter != null)
            {
                m_existingDataEncoder = ColumnSpiceDbRowEncoder.Create(writeRelation.TableSchema.Names);
            }
            m_displayName = $"SpiceDB Sink(Name={writeRelation.NamedObject.DotSeperated})";
            m_dataValueContainer = new DataValueContainer();
        }

        public override string DisplayName => m_displayName;

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            m_client = new PermissionsService.PermissionsServiceClient(m_spiceDbSinkOptions.Channel);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            return base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return new ValueTask<IReadOnlyList<int>>(m_primaryKeys);
        }

        protected override bool FetchExistingData => m_spiceDbSinkOptions.DeleteExistingDataFilter != null;

        protected override async IAsyncEnumerable<EventBatchData> GetExistingData()
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_existingDataEncoder != null);
            Debug.Assert(m_spiceDbSinkOptions.DeleteExistingDataFilter != null);
            Metadata? metadata = default;
            if (m_spiceDbSinkOptions.GetMetadata != null)
            {
                metadata = m_spiceDbSinkOptions.GetMetadata();
            }
            Cursor? cursor = default;
            bool run = true;

            Column[] columns = new Column[m_writeRelation.TableSchema.Names.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
            int count = 0;

            while (run)
            {
                if (cursor != null)
                {
                    m_spiceDbSinkOptions.DeleteExistingDataFilter.OptionalCursor = cursor;
                }

                var relationshipsStream = m_client.ReadRelationships(m_spiceDbSinkOptions.DeleteExistingDataFilter, metadata);
                var readAllEnumerable = relationshipsStream.ResponseStream;

                while (run)
                {
                    try
                    {
                        if (!await readAllEnumerable.MoveNext())
                        {
                            run = false;
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        if (e is RpcException rpcException &&
                            rpcException.InnerException is HttpProtocolException protocolException &&
                            protocolException.ErrorCode == 0)
                        {
                            Logger.RecievedGrpcNoErrorRetry(StreamName, Name);
                            break;
                        }
                        else
                        {
                            throw;
                        }
                    }
                    m_existingDataEncoder.Encode(readAllEnumerable.Current.Relationship, columns);
                    count++;
                    if (count >= 100)
                    {
                        yield return new EventBatchData(columns);
                        count = 0;
                        columns = new Column[m_writeRelation.TableSchema.Names.Count];
                        for (int i = 0; i < columns.Length; i++)
                        {
                            columns[i] = Column.Create(MemoryAllocator);
                        }
                    }
                    cursor = readAllEnumerable.Current.AfterResultCursor;
                }
            }

            if (count > 0)
            {
                yield return new EventBatchData(columns);
            }
            else
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Dispose();
                }
            }
        }

        private static string ColumnToString<T>(T dataValue)
            where T : IDataValue
        {
            switch (dataValue.Type)
            {
                case ArrowTypeId.Boolean:
                    return dataValue.AsBool.ToString();
                case ArrowTypeId.Null:
                    return "null";
                case ArrowTypeId.Decimal128:
                    return dataValue.AsDecimal.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.Double:
                    return dataValue.AsDouble.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.Map:
                    return dataValue.AsMap.ToString()!;
                case ArrowTypeId.Int64:
                    return dataValue.AsLong.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.String:
                    return dataValue.AsString.ToString();
                default:
                    throw new InvalidOperationException($"Unsupported type {dataValue.Type}");
            }
        }

        private Relationship GetRelationship(ColumnWriteOperation row)
        {
            row.EventBatchData.Columns[m_resourceObjectTypeIndex].GetValueAt(row.Index, m_dataValueContainer, default);
            var resourceObjectType = ColumnToString(m_dataValueContainer);
            row.EventBatchData.Columns[m_resourceObjectIdIndex].GetValueAt(row.Index, m_dataValueContainer, default);
            var resourceObjectId = ColumnToString(m_dataValueContainer);
            row.EventBatchData.Columns[m_relationIndex].GetValueAt(row.Index, m_dataValueContainer, default);
            var relation = ColumnToString(m_dataValueContainer);
            row.EventBatchData.Columns[m_subjectObjectTypeIndex].GetValueAt(row.Index, m_dataValueContainer, default);
            var subjectObjectType = ColumnToString(m_dataValueContainer);
            row.EventBatchData.Columns[m_subjectObjectIdIndex].GetValueAt(row.Index, m_dataValueContainer, default);
            var subjectObjectId = ColumnToString(m_dataValueContainer);

            string? subjectOptionalRelation = null;
            if (m_subjectRelationIndex >= 0)
            {
                row.EventBatchData.Columns[m_subjectRelationIndex].GetValueAt(row.Index, m_dataValueContainer, default);
                subjectOptionalRelation = ColumnToString(m_dataValueContainer);
            }

            var resource = new ObjectReference()
            {
                ObjectType = resourceObjectType,
                ObjectId = resourceObjectId
            };
            var subjectObject = new ObjectReference()
            {
                ObjectType = subjectObjectType,
                ObjectId = subjectObjectId
            };
            var subject = new SubjectReference()
            {
                Object = subjectObject
            };

            if (subjectOptionalRelation != null)
            {
                subject.OptionalRelation = subjectOptionalRelation;
            }

            return new Relationship()
            {
                Resource = resource,
                Subject = subject,
                Relation = relation
            };
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(_eventsCounter != null);

            var request = new WriteRelationshipsRequest();
            var watch = Stopwatch.StartNew();
            string? lastToken = default;
            List<Task<string>> uploadTasks = new List<Task<string>>();
            await foreach (var row in rows)
            {
                
                var relationship = GetRelationship(row);

                if (row.IsDeleted)
                {
                    request.Updates.Add(new RelationshipUpdate()
                    {
                        Operation = RelationshipUpdate.Types.Operation.Delete,
                        Relationship = relationship
                    });
                }
                else
                {
                    request.Updates.Add(new RelationshipUpdate()
                    {
                        Operation = RelationshipUpdate.Types.Operation.Touch,
                        Relationship = relationship
                    });
                }
                if (request.Updates.Count >= m_spiceDbSinkOptions.BatchSize)
                {
                    Metadata? metadata = default;
                    if (m_spiceDbSinkOptions.GetMetadata != null)
                    {
                        metadata = m_spiceDbSinkOptions.GetMetadata();
                    }
                    if (m_spiceDbSinkOptions.BeforeWriteRequestFunc != null)
                    {
                        await m_spiceDbSinkOptions.BeforeWriteRequestFunc(request);
                    }
                    _eventsCounter.Add(request.Updates.Count);
                    uploadTasks.Add(Task.Factory.StartNew(async (obj) =>
                    {
                        var req = (WriteRelationshipsRequest)obj!;
                        int retries = 0;
                        WriteRelationshipsResponse? response = default;
                        while (true)
                        {
                            try
                            {
                                response = await m_client.WriteRelationshipsAsync(req, metadata, cancellationToken: cancellationToken);
                                break;
                            }
                            catch (Exception e)
                            {
                                retries++;
                                if (retries < 5)
                                {
                                    var delay = TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, retries) * 3));
                                    Logger.FailedToWriteRelationships(e, delay, StreamName, Name);
                                    await Task.Delay(delay, cancellationToken);
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }
                        return response.WrittenAt.Token;
                    }, request, cancellationToken)
                        .Unwrap()
                        );
                    request = new WriteRelationshipsRequest();

                    while (uploadTasks.Count > m_spiceDbSinkOptions.MaxParallellCalls)
                    {
                        for (int i = 0; i < uploadTasks.Count; i++)
                        {
                            if (uploadTasks[i].IsCompleted)
                            {
                                lastToken = uploadTasks[i].Result;
                                uploadTasks.RemoveAt(i);
                            }
                        }
                        if (uploadTasks.Count > m_spiceDbSinkOptions.MaxParallellCalls)
                        {
                            await Task.WhenAny(uploadTasks);
                        }
                    }
                }
            }

            await Task.WhenAll(uploadTasks);
            watch.Stop();
            if (request.Updates.Count > 0)
            {
                Metadata? metadata = default;
                if (m_spiceDbSinkOptions.GetMetadata != null)
                {
                    metadata = m_spiceDbSinkOptions.GetMetadata();
                }
                if (m_spiceDbSinkOptions.BeforeWriteRequestFunc != null)
                {
                    await m_spiceDbSinkOptions.BeforeWriteRequestFunc(request);
                }
                var response = await m_client.WriteRelationshipsAsync(request, metadata, cancellationToken: cancellationToken);
                lastToken = response.WrittenAt.Token;
            }
            if (m_spiceDbSinkOptions.OnWatermarkFunc != null && lastToken != null)
            {
                await m_spiceDbSinkOptions.OnWatermarkFunc(watermark, lastToken);
            }
        }
    }
}
