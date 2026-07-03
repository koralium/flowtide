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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Grains
{
    internal class StreamGrain : Grain, IStreamGrain
    {
        private readonly IConnectorManager connectorManager;

        public StreamGrain(IConnectorManager connectorManager)
        {
            this.connectorManager = connectorManager;
        }

        public async Task StartStreamAsync(StartStreamRequest request)
        {
            var substreams = GetSubstreamNames(request.SqlText, request.SubstreamCount);

            List<Task> startTasks = new List<Task>();
            foreach (var substream in substreams)
            {
                var substreamKey = $"{this.GetPrimaryKeyString()}_{substream}";
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                startTasks.Add(substreamGrain.StartStreamAsync(new StartStreamMessage(this.GetPrimaryKeyString(), request.SqlText, substream, request.SubstreamCount)));
            }

            await Task.WhenAll(startTasks);
        }

        public async Task StopStreamAsync(StopStreamRequest request)
        {
            var substreams = GetSubstreamNames(request.SqlText, request.SubstreamCount);

            // All substreams are stopped together so the coordinated stop can drain the data
            // exchanged between them, a substream only finishes stopping when the other
            // substreams have received everything it sent before its stop barrier.
            List<Task> stopTasks = new List<Task>();
            foreach (var substream in substreams)
            {
                var substreamKey = $"{this.GetPrimaryKeyString()}_{substream}";
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                stopTasks.Add(substreamGrain.StopStreamAsync());
            }

            await Task.WhenAll(stopTasks);
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
