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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Grpc.Core;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class SpiceDbSink : SimpleGroupedWriteOperator
    {
        private readonly int m_resourceObjectTypeIndex; 
        private readonly int m_resourceObjectIdIndex;
        private readonly int m_relationIndex;
        private readonly int m_subjectObjectTypeIndex;
        private readonly int m_subjectObjectIdIndex;
        private readonly int m_subjectRelationIndex;
        private readonly List<int> m_primaryKeys;
        private readonly SpiceDbSinkOptions m_spiceDbSinkOptions;
        private PermissionsService.PermissionsServiceClient? m_client;
        private readonly SpiceDbRowEncoder? m_existingDataEncoder;
        private ICounter<long>? _eventsCounter;
        private readonly string m_displayName;

        public SpiceDbSink(SpiceDbSinkOptions spiceDbSinkOptions, WriteRelation writeRelation, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, executionDataflowBlockOptions)
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

            if (m_spiceDbSinkOptions.DeleteExistingDataFilter != null)
            {
                m_existingDataEncoder = SpiceDbRowEncoder.Create(writeRelation.TableSchema.Names);
            }
            m_displayName = $"SpiceDB Sink(Name={writeRelation.NamedObject.DotSeperated})";
        }

        public override string DisplayName => m_displayName;

        protected override bool FetchExistingData => m_spiceDbSinkOptions.DeleteExistingDataFilter != null;

        protected override Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            m_client = new PermissionsService.PermissionsServiceClient(m_spiceDbSinkOptions.Channel);
            return Task.FromResult(new MetadataResult(m_primaryKeys));
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            return base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async IAsyncEnumerable<RowEvent> GetExistingData()
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
                        if(!await readAllEnumerable.MoveNext())
                        {
                            run = false;
                            break;
                        }
                    }
                    catch(Exception e)
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
                    yield return m_existingDataEncoder.Encode(readAllEnumerable.Current.Relationship, 1);
                    cursor = readAllEnumerable.Current.AfterResultCursor;
                }
            }   
        }

        private static string ColumnToString(scoped in FlxValueRef flxValue)
        {
            switch (flxValue.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    return flxValue.AsBool.ToString();
                case FlexBuffers.Type.Null:
                    return "null";
                case FlexBuffers.Type.Decimal:
                    return flxValue.AsDecimal.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.Float:
                    return flxValue.AsDouble.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.Map:
                    return flxValue.AsMap.ToJson;
                case FlexBuffers.Type.Int:
                    return flxValue.AsLong.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.String:
                    return flxValue.AsString;
                default:
                    throw new InvalidOperationException($"Unsupported type {flxValue.ValueType}");
            }
        }

        private Relationship GetRelationship(SimpleChangeEvent row)
        {
            var resourceObjectType = row.Row.GetColumnRef(m_resourceObjectTypeIndex);
            var resourceObjectId = row.Row.GetColumnRef(m_resourceObjectIdIndex);
            var relation = row.Row.GetColumnRef(m_relationIndex);
            var subjectObjectType = row.Row.GetColumnRef(m_subjectObjectTypeIndex);
            var subjectObjectId = row.Row.GetColumnRef(m_subjectObjectIdIndex);

            string? subjectOptionalRelation = null;
            if (m_subjectRelationIndex >= 0)
            {
                subjectOptionalRelation = ColumnToString(row.Row.GetColumnRef(m_subjectRelationIndex));
            }

            var resource = new ObjectReference()
            {
                ObjectType = ColumnToString(resourceObjectType),
                ObjectId = ColumnToString(resourceObjectId)
            };
            var subjectObject = new ObjectReference()
            {
                ObjectType = ColumnToString(subjectObjectType),
                ObjectId = ColumnToString(subjectObjectId)
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
                Relation = ColumnToString(relation)
            };
        }

        protected override async Task OnInitialDataSent()
        {
            if (m_spiceDbSinkOptions.OnInitialDataSentFunc != null)
            {
                await m_spiceDbSinkOptions.OnInitialDataSentFunc();
            }
            await base.OnInitialDataSent();
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(_eventsCounter != null);

            var request = new WriteRelationshipsRequest();
            var watch = Stopwatch.StartNew();
            string? lastToken = default;
            List<Task<string>> uploadTasks = new List<Task<string>>();
            await foreach(var row in rows)
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
                            catch(Exception e)
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
