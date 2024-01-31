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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        private List<Action<TupleKey, FlexBuffer>> m_encoders;
        private FlexBuffer flexBuffer;
        private Task? _changesTask;
        private readonly string? m_objectTypeFilter;

        private const string WatermarkName = "openfga";
        
        public FlowtideOpenFGASource(OpenFGASourceOptions openFgaOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            m_encoders = new List<Action<TupleKey, FlexBuffer>>();
            flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                var name = readRelation.BaseSchema.Names[i];
                switch (name.ToLower())
                {
                    case "user":
                        m_encoders.Add((tupleKey, builder) => builder.Add(tupleKey.User));
                        break;
                    case "relation":
                        m_encoders.Add((tupleKey, builder) => builder.Add(tupleKey.Relation));
                        break;
                    case "object":
                        m_encoders.Add((tupleKey, builder) => builder.Add(tupleKey.Object));
                        break;
                    case "object_type":
                        m_encoders.Add((tupleKey, builder) => builder.Add(tupleKey.Object.Substring(0, tupleKey.Object.IndexOf(':'))));
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
                do
                {
                    List<RowEvent> outputData = new List<RowEvent>();
                    foreach (var change in changes.Changes)
                    {
                        flexBuffer.NewObject();
                        var vectorStart = flexBuffer.StartVector();
                        for (int i = 0; i < m_encoders.Count; i++)
                        {
                            m_encoders[i](change.TupleKey, flexBuffer);
                        }
                        flexBuffer.EndVector(vectorStart, false, false);
                        var vector = flexBuffer.Finish();
                        var row = new CompactRowData(vector);
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
            
            do
            {
                List<RowEvent> outputData = new List<RowEvent>();
                foreach(var change in changes.Changes)
                {
                    flexBuffer.NewObject();
                    var vectorStart = flexBuffer.StartVector();
                    for(int i = 0; i < m_encoders.Count; i++)
                    {
                        m_encoders[i](change.TupleKey, flexBuffer);
                    }
                    flexBuffer.EndVector(vectorStart, false, false);
                    var vector = flexBuffer.Finish();
                    var row = new CompactRowData(vector);
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

            await output.SendWatermark(new Watermark(WatermarkName, m_state.LastTimestamp));
            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            await RegisterTrigger("load_changes", TimeSpan.FromSeconds(1));
        }
    }
}
