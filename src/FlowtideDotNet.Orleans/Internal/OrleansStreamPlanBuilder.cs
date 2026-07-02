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
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Orleans.Internal
{
    /// <summary>
    /// Builds the plan for a stream from its SQL text.
    ///
    /// The plan is built independently by the stream grain and by each substream grain
    /// instead of being sent between grains. Since the SQL compiler, the distributed plan
    /// modifier and the optimizer are deterministic, every grain computes an identical plan
    /// as long as the same connectors are registered in every silo.
    /// This avoids serializing plans between silos and avoids sharing one mutable plan
    /// instance between substreams in the same silo.
    /// </summary>
    internal static class OrleansStreamPlanBuilder
    {
        public static Plan BuildPlan(IConnectorManager connectorManager, string sqlText, int? substreamCount)
        {
            var sqlBuilder = new SqlPlanBuilder();
            foreach (var tableProvider in connectorManager.GetTableProviders())
            {
                sqlBuilder.AddTableProvider(tableProvider);
            }
            sqlBuilder.Sql(sqlText);
            var plan = sqlBuilder.GetPlan();

            var settings = new PlanOptimizerSettings()
            {
                Parallelization = 1,
                SimplifyProjection = true
            };
            if (substreamCount.HasValue)
            {
                // Distribution runs as the last step inside the optimizer, after joins have
                // been converted to merge joins so they can be partitioned across substreams.
                settings.DistributedPlanOptions = new DistributedPlanOptions()
                {
                    SubstreamCount = substreamCount.Value
                };
            }

            return PlanOptimizer.Optimize(plan, settings);
        }
    }
}
