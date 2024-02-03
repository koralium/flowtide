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

using FlexBuffers;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Model;
using System.Buffers;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class FlowtideOpenFGASourceState 
    {
        public string? ContinuationToken { get; set; }
        public long LastTimestamp { get; set; }
    }
    internal class FlowtideOpenFGASource : ReadBaseOperator<FlowtideOpenFGASourceState>
    {
        private readonly OpenFGASourceOptions m_options;
        private OpenFgaClient? m_client;
        private FlowtideOpenFGASourceState? m_state;

        private List<Action<TupleKey, int, FlxValue[]>> m_encoders;
        private FlexBuffer flexBuffer;
        private Task? _changesTask;
        private readonly string? m_objectTypeFilter;

        private const string WatermarkName = "openfga";

        /// <summary>
        /// Cache for types and relation values
        /// These will be low cardinality and can be cached to reduce memory consumption in the stream.
        /// </summary>
        private Dictionary<string, FlxValue> _typesAndRelationValues = new Dictionary<string, FlxValue>();

        public FlowtideOpenFGASource(OpenFGASourceOptions openFgaOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            m_encoders = new List<Action<TupleKey, int, FlxValue[]>>();
            flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                var name = readRelation.BaseSchema.Names[i];
                switch (name.ToLower())
                {
                    case "user_type":
                        m_encoders.Add((tupleKey, i, arr) => {
                            var typeName = tupleKey.User.Substring(0, tupleKey.User.IndexOf(':'));
                            if (_typesAndRelationValues.TryGetValue(typeName, out var value))
                            {
                                arr[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(typeName);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(typeName, flxValue);
                            arr[i] = flxValue;
                        });
                        break;
                    case "user_id":
                        m_encoders.Add((tupleKey, i, arr) => {
                            flexBuffer.NewObject();
                            flexBuffer.Add(tupleKey.User.Substring(tupleKey.User.IndexOf(':') + 1));
                            arr[i] = FlxValue.FromBytes(flexBuffer.Finish());
                            });
                        break;
                    case "relation":
                        m_encoders.Add((tupleKey, i, arr) => {
                            if (_typesAndRelationValues.TryGetValue(tupleKey.Relation, out var value))
                            {
                                arr[i] = value;
                                return;
                            } flexBuffer.NewObject(); 
                            flexBuffer.Add(tupleKey.Relation);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(tupleKey.Relation, flxValue);
                            arr[i] = flxValue;
                            });
                        break;
                    case "object_id":
                        m_encoders.Add((tupleKey, i, arr) => {
                            flexBuffer.NewObject();
                            flexBuffer.Add(tupleKey.Object.Substring(tupleKey.Object.IndexOf(':') + 1));
                            arr[i] = FlxValue.FromBytes(flexBuffer.Finish());
                            });
                        break;
                    case "object_type":
                        m_encoders.Add((tupleKey, i, arr) => {
                            var objectTypeValue = tupleKey.Object.Substring(0, tupleKey.Object.IndexOf(':'));
                            if (_typesAndRelationValues.TryGetValue(objectTypeValue, out var value))
                            {
                                arr[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(objectTypeValue);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            _typesAndRelationValues.Add(objectTypeValue, flxValue);
                            arr[i] = flxValue;
                            });
                        break;
                }
            }

            if (readRelation.Filter != null)
            {
                var filterVisitor = new OpenFgaFilterVisitor(readRelation);
                m_objectTypeFilter = filterVisitor.Visit(readRelation.Filter, default);
            }

            this.m_options = openFgaOptions;
        }

        public override string DisplayName => "OpenFGA Source";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "load_changes" && (_changesTask == null || _changesTask.IsCompleted))
            {
                // Fetch data using change tracking
                _changesTask = RunTask(FetchChanges);
            }
            return Task.CompletedTask;
        }

        private async Task FetchChanges(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_state != null);
            ClientReadChangesRequest? request = default;

            if (m_objectTypeFilter != null)
            {
                request = new ClientReadChangesRequest()
                {
                    Type = m_objectTypeFilter
                };
            }

            var changes = await m_client.ReadChanges(request, options: new OpenFga.Sdk.Client.Model.ClientReadChangesOptions()
            {
                ContinuationToken = m_state.ContinuationToken,
            });

            if (changes.Changes.Count > 0)
            {
                await output.EnterCheckpointLock();
                await SendChanges(request, changes, output);
                await output.SendWatermark(new Watermark(WatermarkName, m_state.LastTimestamp));
                output.ExitCheckpointLock();
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { WatermarkName });
        }

        protected override Task InitializeOrRestore(long restoreTime, FlowtideOpenFGASourceState? state, IStateManagerClient stateManagerClient)
        {
            if (state != null)
            {
                m_state = state;
            }
            else
            {
                m_state = new FlowtideOpenFGASourceState();
            }
            m_client = new OpenFgaClient(m_options.ClientConfiguration);
            return Task.CompletedTask;
        }

        protected override Task<FlowtideOpenFGASourceState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(m_state != null);
            return Task.FromResult(m_state);
        }

        private async Task SendChanges(ClientReadChangesRequest request, ReadChangesResponse changes, IngressOutput<StreamEventBatch> output)
        {
            do
            {
                List<RowEvent> outputData = new List<RowEvent>();
                foreach (var change in changes.Changes)
                {
                    FlxValue[] arr = new FlxValue[m_encoders.Count];
                    for (int i = 0; i < m_encoders.Count; i++)
                    {
                        m_encoders[i](change.TupleKey, i, arr);
                    }
                    var row = new ArrayRowData(arr);

                    if (change.Operation == TupleOperation.WRITE)
                    {
                        outputData.Add(new RowEvent(1, 0, row));
                    }
                    else
                    {
                        outputData.Add(new RowEvent(-1, 0, row));
                    }
                    var timestamp = new DateTimeOffset(change.Timestamp).ToUnixTimeMilliseconds();
                    if (timestamp > m_state.LastTimestamp)
                    {
                        m_state.LastTimestamp = timestamp;
                    }
                }
                await output.SendAsync(new StreamEventBatch(outputData));
                m_state.ContinuationToken = changes.ContinuationToken;

                changes = await m_client.ReadChanges(request, options: new OpenFga.Sdk.Client.Model.ClientReadChangesOptions()
                {
                    ContinuationToken = m_state.ContinuationToken,
                });
            } while (changes.Changes.Count > 0);
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_state != null);

            ClientReadChangesRequest? request = default;

            if (m_objectTypeFilter != null)
            {
                request = new ClientReadChangesRequest()
                {
                    Type = m_objectTypeFilter
                };
            }

            await output.EnterCheckpointLock();
            var changes = await m_client.ReadChanges(request, options: new OpenFga.Sdk.Client.Model.ClientReadChangesOptions()
            {
                ContinuationToken = m_state.ContinuationToken,
            });

            await SendChanges(request, changes, output);

            await output.SendWatermark(new Watermark(WatermarkName, m_state.LastTimestamp));
            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            await RegisterTrigger("load_changes", TimeSpan.FromSeconds(1));
        }
    }
}
