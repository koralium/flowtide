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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Model;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class FlowtideOpenFgaSourceState
    {
        public string? ContinuationToken { get; set; }
        public long LastTimestamp { get; set; }
    }
    internal class FlowtideOpenFgaSource : ReadBaseOperator
    {
        private readonly OpenFgaSourceOptions m_options;
        private readonly ReadRelation m_readRelation;
        private OpenFgaClient? m_client;
        private IObjectState<FlowtideOpenFgaSourceState>? m_state;

        private readonly OpenFgaRowEncoder m_encoder;
        private Task? _changesTask;
        private readonly string? m_objectTypeFilter;

        private readonly string m_watermarkName;
        private readonly string m_displayName;

        public FlowtideOpenFgaSource(OpenFgaSourceOptions openFgaOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            m_encoder = OpenFgaRowEncoder.Create(readRelation.BaseSchema.Names);

            if (readRelation.Filter != null)
            {
                var filterVisitor = new OpenFgaFilterVisitor(readRelation);
                m_objectTypeFilter = filterVisitor.Visit(readRelation.Filter, default);
                m_displayName = $"OpenFGA(type={m_objectTypeFilter})";
                m_watermarkName = $"openfga_{m_objectTypeFilter}";
            }
            else
            {
                m_displayName = "OpenFGA(type=all)";
                m_watermarkName = "openfga";
            }

            this.m_options = openFgaOptions;
            this.m_readRelation = readRelation;
        }

        public override string DisplayName => m_displayName;

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
            Debug.Assert(m_state?.Value != null);
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
                ContinuationToken = m_state.Value.ContinuationToken,
            });

            if (changes.Changes.Count > 0)
            {
                await output.EnterCheckpointLock();
                await SendChanges(request, changes, output);
                await output.SendWatermark(new Watermark(m_watermarkName, m_state.Value.LastTimestamp));
                output.ExitCheckpointLock();
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { m_watermarkName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            m_state = await stateManagerClient.GetOrCreateObjectStateAsync<FlowtideOpenFgaSourceState>("openfga_state");
            if (m_state.Value == null)
            {
                m_state.Value = new FlowtideOpenFgaSourceState();
            }
            m_client = new OpenFgaClient(m_options.ClientConfiguration);
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(m_state != null);
            await m_state.Commit();
        }

        private async Task SendChanges(ClientReadChangesRequest? request, ReadChangesResponse changes, IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_state?.Value != null);

            do
            {
                List<RowEvent> outputData = new List<RowEvent>();
                foreach (var change in changes.Changes)
                {
                    if (change.Operation == TupleOperation.WRITE)
                    {
                        outputData.Add(m_encoder.Encode(change.TupleKey, 1));
                    }
                    else
                    {
                        outputData.Add(m_encoder.Encode(change.TupleKey, -1));
                    }
                    var timestamp = new DateTimeOffset(change.Timestamp).ToUnixTimeMilliseconds();
                    if (timestamp > m_state.Value.LastTimestamp)
                    {
                        m_state.Value.LastTimestamp = timestamp;
                    }
                }
                await output.SendAsync(new StreamEventBatch(outputData, m_readRelation.OutputLength));
                m_state.Value.ContinuationToken = changes.ContinuationToken;

                changes = await m_client.ReadChanges(request, options: new OpenFga.Sdk.Client.Model.ClientReadChangesOptions()
                {
                    ContinuationToken = m_state.Value.ContinuationToken,
                });
            } while (changes.Changes.Count > 0);
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(m_client != null);
            Debug.Assert(m_state?.Value != null);

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
                ContinuationToken = m_state.Value.ContinuationToken,
            });

            await SendChanges(request, changes, output);

            await output.SendWatermark(new Watermark(m_watermarkName, m_state.Value.LastTimestamp));
            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            await RegisterTrigger("load_changes", TimeSpan.FromSeconds(1));
        }
    }
}
