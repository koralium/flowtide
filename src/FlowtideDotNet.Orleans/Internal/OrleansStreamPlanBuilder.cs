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
    /// Builds and optimizes the plan for a stream. The plan is prepared once, centrally, by
    /// the stream grain - compiled from SQL text using the silo's connectors, or deserialized
    /// from a user provided plan - and the final distributed plan is then sent serialized to
    /// every substream grain. Sending the plan instead of SQL lets user created plans run on
    /// Orleans, and deserialization gives every substream grain its own fresh plan instance,
    /// which matters because building a stream mutates the plan in place.
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

            return OptimizePlan(plan, substreamCount);
        }

        public static Plan OptimizePlan(Plan plan, int? substreamCount)
        {
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
