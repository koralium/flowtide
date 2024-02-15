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
        private readonly List<Action<Relationship, int, FlxValue[]>> m_encoders;
        private readonly FlexBuffer flexBuffer;
        private readonly Dictionary<string, FlxValue> _typesAndRelationValues = new Dictionary<string, FlxValue>();
        private WatchService.WatchServiceClient? m_watchClient;
        private List<string> readTypes = new List<string>();
        private readonly bool readAllTypes = true;
        private AsyncServerStreamingCall<WatchResponse>? watchStream;
        private HashSet<string>? watermarkNames;
        private readonly string? _relationFilter;
        private readonly string? _subjectTypeFilter;

        public SpiceDbSource(SpiceDbSourceOptions spiceDbSourceOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            m_encoders = new List<Action<Relationship, int, FlxValue[]>>();
            this.m_spiceDbSourceOptions = spiceDbSourceOptions;

            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                var name = readRelation.BaseSchema.Names[i];
                switch (name.ToLower())
                {
                    case "subject_type":
                        m_encoders.Add((r, i, v) =>
                        {
                            if (_typesAndRelationValues.TryGetValue(r.Subject.Object.ObjectType, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Subject.Object.ObjectType);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(r.Subject.Object.ObjectType, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "subject_id":
                        m_encoders.Add((r, i, v) =>
                        {
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Subject.Object.ObjectId);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            v[i] = flxValue;
                        });
                        break;
                    case "subject_relation":
                        m_encoders.Add((r, i, v) =>
                        {
                            if (_typesAndRelationValues.TryGetValue(r.Subject.OptionalRelation, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Subject.OptionalRelation);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(r.Subject.OptionalRelation, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "relation":
                        m_encoders.Add((r, i, v) =>
                        {
                            if (_typesAndRelationValues.TryGetValue(r.Relation, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Relation);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(r.Relation, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "resource_type":
                        m_encoders.Add((r, i, v) =>
                        {
                            if (_typesAndRelationValues.TryGetValue(r.Resource.ObjectType, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Resource.ObjectType);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(r.Resource.ObjectType, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "resource_id":
                        m_encoders.Add((r, i, v) =>
                        {
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Resource.ObjectId);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            v[i] = flxValue;
                        });
                        break;
                }
            }

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
                }
            }
        }

        public override string DisplayName => "SpiceDB";

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
                throw new InvalidOperationException("Could not parse revision from token");
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
            Debug.Assert(m_state != null);
            Debug.Assert(m_state.TypeTimestamps != null);

            while (!output.CancellationToken.IsCancellationRequested)
            {
                output.CancellationToken.ThrowIfCancellationRequested();
                if (await watchStream.ResponseStream.MoveNext(output.CancellationToken))
                {
                    await output.EnterCheckpointLock();
                    var current = watchStream.ResponseStream.Current;
                    m_state.ContinuationToken = current.ChangesThrough.Token;
                    var revision = GetRevision(current.ChangesThrough.Token);

                    List<RowEvent> outputData = new List<RowEvent>();
                    foreach(var update in current.Updates)
                    {
                        if (update.Relationship == null)
                        {
                            continue;
                        }
                        if (!readAllTypes && !readTypes.Contains(update.Relationship.Resource.ObjectType))
                        {
                            continue;
                        }
                        FlxValue[] arr = new FlxValue[m_encoders.Count];
                        for (int i = 0; i < m_encoders.Count; i++)
                        {
                            m_encoders[i](update.Relationship, i, arr);
                        }
                        var weight = update.Operation == RelationshipUpdate.Types.Operation.Delete ? -1 : 1;
                        outputData.Add(new RowEvent(weight, 0, new ArrayRowData(arr)));
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
                foreach (var readType in readTypes)
                {
                    var readRequest = new ReadRelationshipsRequest();
                    readRequest.RelationshipFilter = new RelationshipFilter()
                    {
                        ResourceType = readType
                    };
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
                        FlxValue[] arr = new FlxValue[m_encoders.Count];
                        for (int i = 0; i < m_encoders.Count; i++)
                        {
                            m_encoders[i](r.Relationship, i, arr);
                        }

                        outputData.Add(new RowEvent(1, 0, new ArrayRowData(arr)));

                        if (outputData.Count >= 100)
                        {
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
