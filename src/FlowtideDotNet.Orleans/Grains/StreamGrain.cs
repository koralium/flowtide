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

using FlowtideDotNet.Core;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using FlowtideDotNet.Substrait.Relations;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Grains
{
    [GenerateSerializer]
    public class StreamGrainStorage
    {
        [Id(0)]
        public List<string> StartedSubstreams { get; set; } = new List<string>();
    }

    internal class StreamGrain : Grain, IStreamGrain
    {
        private readonly IConnectorManager connectorManager;
        private readonly IPersistentState<StreamGrainStorage> _state;

        public StreamGrain(
            IConnectorManager connectorManager,
            [PersistentState("stream", "stream_metadata")] IPersistentState<StreamGrainStorage> state)
        {
            this.connectorManager = connectorManager;
            _state = state;
        }

        public async Task StartStreamAsync(StartStreamRequest request)
        {
            var substreams = GetSubstreamNames(request.SqlText, request.SubstreamCount);

            // The started set is persisted before the substream grains start, a stop uses
            // only this set. Missing a started substream grain here would leave it running
            // with no way to stop it, its keep alive reminder restarts it forever.
            foreach (var substream in substreams)
            {
                if (!_state.State.StartedSubstreams.Contains(substream))
                {
                    _state.State.StartedSubstreams.Add(substream);
                }
            }
            await _state.WriteStateAsync();

            List<Task> startTasks = new List<Task>();
            foreach (var substream in substreams)
            {
                var substreamKey = $"{this.GetPrimaryKeyString()}_{substream}";
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                startTasks.Add(substreamGrain.StartStreamAsync(new StartStreamMessage(this.GetPrimaryKeyString(), request.SqlText, substream, request.SubstreamCount)));
            }

            await Task.WhenAll(startTasks);
        }

        public async Task StopStreamAsync()
        {
            // All substreams are stopped together so the coordinated stop can drain the data
            // exchanged between them, a substream only finishes stopping when the other
            // substreams have received everything it sent before its stop barrier.
            List<Task> stopTasks = new List<Task>();
            foreach (var substream in _state.State.StartedSubstreams)
            {
                var substreamKey = $"{this.GetPrimaryKeyString()}_{substream}";
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                stopTasks.Add(substreamGrain.StopStreamAsync());
            }

            await Task.WhenAll(stopTasks);

            _state.State.StartedSubstreams.Clear();
            await _state.ClearStateAsync();
        }

        private HashSet<string> GetSubstreamNames(string sqlText, int? substreamCount)
        {
            // The plan is only built here to find the substream names, each substream grain
            // builds its own plan from the SQL text.
            var plan = OrleansStreamPlanBuilder.BuildPlan(connectorManager, sqlText, substreamCount);

            HashSet<string> substreams = new HashSet<string>();

            // Find out the different substreams
            foreach(var relation in plan.Relations)
            {
                if (relation is SubStreamRootRelation substreamRoot)
                {
                    substreams.Add(substreamRoot.Name);
                }
            }
            if (substreams.Count == 0)
            {
                substreams.Add("default");
            }
            return substreams;
        }
    }
}
