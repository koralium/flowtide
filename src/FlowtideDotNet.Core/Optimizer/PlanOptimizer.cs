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

using FlowtideDotNet.Core.Optimizer.FilterPushdown;
using FlowtideDotNet.Core.Optimizer.GetTimestamp;
using FlowtideDotNet.Substrait;

namespace FlowtideDotNet.Core.Optimizer
{
    public static class PlanOptimizer
    {
        public static Plan Optimize(Plan plan, PlanOptimizerSettings? settings = null)
        {
            if (settings == null)
            {
                settings = new PlanOptimizerSettings();
            }

            // Start with timestamp to join since its actual logic and not only optimization
            if (settings.GetTimestampToJoin)
            {
                plan = TimestampToJoin.Optimize(plan, settings);
            }

            for (int i = 0; i < plan.Relations.Count; i++)
            {
                var relation = plan.Relations[i];

                var filterBeforeJoinOptimize = new JoinFilterPushdownVisitor();
                relation = filterBeforeJoinOptimize.Visit(relation, null!);

                var filterIntoRead = new FilterIntoReadOptimizer();
                relation = filterIntoRead.Visit(relation, null!);

                plan.Relations[i] = relation;
            }

            plan = JoinProjectionPushdown.JoinProjectionPushdown.Optimize(plan);

            for (int i = 0; i < plan.Relations.Count; i++)
            {
                var relation = plan.Relations[i];

                if (!settings.NoMergeJoin)
                {
                    var mergeJoinOptimize = new MergeJoinFindVisitor();
                    relation = mergeJoinOptimize.Visit(relation, null!);
                }
                plan.Relations[i] = relation;
            }

            // Try and remove any direct field references if possible
            if (settings.SimplifyProjection)
            {
                plan = DirectFieldSimplification.DirectFieldSimplification.Optimize(plan);
            }

            EmitPushdown.EmitPushdown.Optimize(plan);

            if (settings.Parallelization > 1)
            {
                plan = MergeJoinParallelize.ParallelizeOptimizer.Optimize(plan, settings.Parallelization);
            }
            return plan;
        }
    }
}
