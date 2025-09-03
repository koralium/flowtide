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
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Messages;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
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
            var sqlBuilder = new SqlPlanBuilder();
            foreach(var tableProvider in connectorManager.GetTableProviders())
            {
                sqlBuilder.AddTableProvider(tableProvider);
            }
            sqlBuilder.Sql(request.SqlText);
            var plan = sqlBuilder.GetPlan();

            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
            {
                Parallelization = 1,
                SimplifyProjection = true
            });

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

            List<Task> startTasks = new List<Task>();
            foreach (var substream in substreams)
            {
                var substreamKey = $"{this.GetPrimaryKeyString()}_{substream}";
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                startTasks.Add(substreamGrain.StartStreamAsync(new StartStreamMessage(this.GetPrimaryKeyString(), plan, substream)));
            }

            await Task.WhenAll(startTasks);
        }
    }
}
