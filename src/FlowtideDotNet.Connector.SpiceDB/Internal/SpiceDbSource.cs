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
using Authzed.Internal;
using FlexBuffers;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Connector.SpiceDB.Internal.SchemaParser;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using SqlParser;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class FlowtideSpiceDbSourceState
    {
        public string? ContinuationToken { get; set; }
        public Dictionary<string, long>? TypeTimestamps { get; set; }
    }
    internal class SpiceDbSource : ReadBaseOperator<FlowtideSpiceDbSourceState>
    {
        private PermissionsService.PermissionsServiceClient? m_client;
        private readonly SpiceDbSourceOptions m_spiceDbSourceOptions;
        private FlowtideSpiceDbSourceState? m_state;
        private readonly SpiceDbRowEncoder m_rowEncoder;
        private WatchService.WatchServiceClient? m_watchClient;
        private List<string> readTypes = new List<string>();
        private readonly bool readAllTypes = true;
        private AsyncServerStreamingCall<WatchResponse>? watchStream;
        private HashSet<string>? watermarkNames;
        private readonly string? _relationFilter;
        private readonly string? _subjectTypeFilter;
        private readonly string _displayName;

        public SpiceDbSource(SpiceDbSourceOptions spiceDbSourceOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this.m_spiceDbSourceOptions = spiceDbSourceOptions;
            m_rowEncoder = SpiceDbRowEncoder.Create(readRelation.BaseSchema.Names);

            if (readRelation.Filter != null)
            {
                var filterVisitor = new SpiceDbFilterVisitor(readRelation);
                if (filterVisitor.Visit(readRelation.Filter, default))
                {
                    if (filterVisitor.ResourceType != null)
                    {
                        readTypes = new List<string> { filterVisitor.ResourceType };
                        readAllTypes = false;
                        watermarkNames = new HashSet<string> { $"spicedb_{filterVisitor.ResourceType}" };
                    }
                    _relationFilter = filterVisitor.Relation;
                    _subjectTypeFilter = filterVisitor.SubjectType;

                    StringBuilder displayBuilder = new StringBuilder("SpiceDB(");
                    if (filterVisitor.ResourceType != null)
                    {
                        displayBuilder.Append(filterVisitor.ResourceType);
                    }
                    else
                    {
                        displayBuilder.Append("all");
                    }
                    if (filterVisitor.Relation != null)
                    {
                        displayBuilder.Append($", {filterVisitor.Relation}");
                    }
                    if (filterVisitor.SubjectType != null)
                    {
                        displayBuilder.Append($", {filterVisitor.SubjectType}");
                    }
                    displayBuilder.Append(')');
                    _displayName = displayBuilder.ToString();
                }
                else
                {
                    _displayName = "SpiceDB(all)";
                }
            }
            else
            {
                _displayName = "SpiceDB(all)";
            }
        }

        public override string DisplayName => _displayName;

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
            Debug.Assert(watermarkNames != null);
            return Task.FromResult<IReadOnlySet<string>>(watermarkNames);
        }

        protected override async Task InitializeOrRestore(long restoreTime, FlowtideSpiceDbSourceState? state, IStateManagerClient stateManagerClient)
        {
            if (state != null)
            {
                m_state = state;
            }
            else
            {
                m_state = new FlowtideSpiceDbSourceState()
                {
                    TypeTimestamps = new Dictionary<string, long>()
                };
            }
            Metadata? metadata = default;
            if (m_spiceDbSourceOptions.GetMetadata != null)
            {
                metadata = m_spiceDbSourceOptions.GetMetadata();
            }

            var schemaService = new SchemaService.SchemaServiceClient(m_spiceDbSourceOptions.Channel);

            // Fetch the schema and parse it to get out the available types
            // This is required since spicedb does not have a way to list all available types
            if (readTypes == null)
            {
                var schemaResponse = await schemaService.ReadSchemaAsync(new ReadSchemaRequest(), metadata);
                var schema = SpiceDbParser.ParseSchema(schemaResponse.SchemaText);
                readTypes = new List<string>();
                foreach (var type in schema.Types)
                {
                    readTypes.Add(type.Key);
                }           
                watermarkNames = new HashSet<string>();
                foreach (var type in readTypes)
                {
                    watermarkNames.Add($"spicedb_{type}");
                }
            }
            

            m_client = new PermissionsService.PermissionsServiceClient(m_spiceDbSourceOptions.Channel);
            m_watchClient = new WatchService.WatchServiceClient(m_spiceDbSourceOptions.Channel);
        }

        protected override Task<FlowtideSpiceDbSourceState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(m_state != null);
            return Task.FromResult(m_state);
        }

        private static long GetRevision(string token)
        {
            var bytes = Convert.FromBase64String(token);
            var decodedToken = DecodedZedToken.Parser.ParseFrom(bytes);
            if (!long.TryParse(decodedToken.V1.Revision, out var result))
            {
                try
                {
                    var postgresRevisionBytes = Convert.FromBase64String(decodedToken.V1.Revision);
                    var postgresRevision = PostgresRevision.Parser.ParseFrom(postgresRevisionBytes);
                    return (long)postgresRevision.Xmin;
                }
                catch
                {
                    throw new InvalidOperationException("Could not parse revision from token");
                }
            }
            return result;
        }

        private Watermark GetCurrentWatermark()
        {
            Debug.Assert(m_state != null);
            Debug.Assert(m_state.TypeTimestamps != null);

            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach(var kv in m_state.TypeTimestamps)
            {
                builder.Add($"spicedb_{kv.Key}", kv.Value);
            }
            return new Watermark(builder.ToImmutable());
        }

        private async Task LoadChangesTask(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(watchStream != null);
            Debug.Assert(m_watchClient != null);
            Debug.Assert(m_state != null);
            Debug.Assert(m_state.TypeTimestamps != null);

            bool initWatch = false;

            while (!output.CancellationToken.IsCancellationRequested)
            {
                output.CancellationToken.ThrowIfCancellationRequested();
                try
                {
                    if (initWatch)
                    {
                        Metadata? metadata = default;
                        if (m_spiceDbSourceOptions.GetMetadata != null)
                        {
                            metadata = m_spiceDbSourceOptions.GetMetadata();
                        }
                        var watchRequest = new WatchRequest();
                        if (m_state.ContinuationToken != null)
                        {
                            watchRequest.OptionalStartCursor = new ZedToken
                            {
                                Token = m_state.ContinuationToken
                            };
                        }
                        if (!readAllTypes)
                        {
                            foreach (var readType in readTypes)
                            {
                                watchRequest.OptionalObjectTypes.Add(readType);
                            }
                        }
                        watchStream = m_watchClient.Watch(watchRequest, metadata);
                        initWatch = false;
                    }
                    // If we managed to start watching again, set health to true
                    SetHealth(true);
                    if (await watchStream.ResponseStream.MoveNext(output.CancellationToken))
                    {
                        await output.EnterCheckpointLock();
                        var current = watchStream.ResponseStream.Current;
                        m_state.ContinuationToken = current.ChangesThrough.Token;
                        var revision = GetRevision(current.ChangesThrough.Token);

                        List<RowEvent> outputData = new List<RowEvent>();
                        foreach (var update in current.Updates)
                        {
                            if (update.Relationship == null)
                            {
                                continue;
                            }
                            if (!readAllTypes && !readTypes.Contains(update.Relationship.Resource.ObjectType))
                            {
                                continue;
                            }
                            var weight = update.Operation == RelationshipUpdate.Types.Operation.Delete ? -1 : 1;
                            outputData.Add(m_rowEncoder.Encode(update.Relationship, weight));
                            m_state.TypeTimestamps[update.Relationship.Resource.ObjectType] = revision;
                        }
                        if (outputData.Count > 0)
                        {
                            await output.SendAsync(new StreamEventBatch(outputData));
                            await output.SendWatermark(GetCurrentWatermark());
                            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                        }
                        output.ExitCheckpointLock();
                    }
                }
                catch(Exception e)
                {
                    if (e is RpcException rpcException &&
                        rpcException.Status.StatusCode == StatusCode.Unavailable)
                    {
                        Logger.RecievedGrpcNoErrorRetry(StreamName, Name);
                    }
                    else
                    {
                        SetHealth(false);
                        Logger.ErrorInSpiceDb(e, StreamName, Name);
                    }
                    initWatch = true;
                }
               
                
            }
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_watchClient != null);
            Debug.Assert(m_state != null);
            Debug.Assert(m_state.TypeTimestamps != null);

            Metadata? metadata = default;
            if (m_spiceDbSourceOptions.GetMetadata != null)
            {
                metadata = m_spiceDbSourceOptions.GetMetadata();
            }
            var watchRequest = new WatchRequest();
            if (m_state.ContinuationToken != null)
            {
                watchRequest.OptionalStartCursor = new ZedToken
                {
                    Token = m_state.ContinuationToken
                };
            }
            if (!readAllTypes)
            {
                foreach (var readType in readTypes)
                {
                    watchRequest.OptionalObjectTypes.Add(readType);
                }
            }

            // Start the watch stream, this will be used later to check for changes
            watchStream = m_watchClient.Watch(watchRequest, metadata);

            if (m_state.ContinuationToken == null)
            {
                await output.EnterCheckpointLock();

                long minRevision = long.MaxValue;
                string? firstToken = default;
                Cursor? cursor = default;
                int retries = 0;
                foreach (var readType in readTypes)
                {
                    while (true)
                    {
                        try
                        {
                            var readRequest = new ReadRelationshipsRequest();
                            readRequest.RelationshipFilter = new RelationshipFilter()
                            {
                                ResourceType = readType
                            };
                            if (cursor != null)
                            {
                                readRequest.OptionalCursor = cursor;
                            }
                            if (_relationFilter != null)
                            {
                                readRequest.RelationshipFilter.OptionalRelation = _relationFilter;
                            }
                            if (_subjectTypeFilter != null)
                            {
                                readRequest.RelationshipFilter.OptionalSubjectFilter = new SubjectFilter()
                                {
                                    SubjectType = _subjectTypeFilter
                                };
                            }
                            var stream = m_client.ReadRelationships(readRequest, metadata);

                            List<RowEvent> outputData = new List<RowEvent>();
                            await foreach (var r in stream.ResponseStream.ReadAllAsync())
                            {
                                outputData.Add(m_rowEncoder.Encode(r.Relationship, 1));

                                if (outputData.Count >= 100)
                                {
                                    retries = 0;
                                    cursor = r.AfterResultCursor;
                                    await output.SendAsync(new StreamEventBatch(outputData));
                                    outputData = new List<RowEvent>();
                                }
                                if (firstToken == null)
                                {
                                    firstToken = r.ReadAt.Token;
                                    minRevision = GetRevision(r.ReadAt.Token);
                                }
                            }
                            if (outputData.Count > 0)
                            {
                                await output.SendAsync(new StreamEventBatch(outputData));
                            }
                            m_state.TypeTimestamps[readType] = minRevision;
                            break;
                        }
                        catch(Exception e)
                        {
                            if (e is RpcException rpcException && 
                                rpcException.InnerException != null &&
                                rpcException.InnerException is HttpProtocolException httpProtocolException &&
                                httpProtocolException.ErrorCode == 0)
                            {
                                Logger.RecievedGrpcNoErrorRetry(StreamName, Name);
                            }
                            else
                            {
                                Logger.ErrorInSpiceDbWithRetry(e, retries, StreamName, Name);
                                retries++;
                                if (retries > 3)
                                {
                                    throw;
                                }
                            }
                        }
                    }
                    
                }
                m_state.ContinuationToken = firstToken;
                await output.SendWatermark(GetCurrentWatermark());
                output.ExitCheckpointLock();
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            _ = RunTask(LoadChangesTask, taskCreationOptions: TaskCreationOptions.LongRunning);
        }
    }
}
