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
            for(int i = 0; i < plan.Relations.Count; i++)
            {
                var relation = plan.Relations[i];

                var filterBeforeJoinOptimize = new JoinFilterPushdownVisitor();
                relation = filterBeforeJoinOptimize.Visit(relation, null);

                var filterIntoRead = new FilterIntoReadOptimizer();
                relation = filterIntoRead.Visit(relation, null);

                if (!settings.NoMergeJoin)
                {
                    var mergeJoinOptimize = new MergeJoinFindVisitor();
                    relation = mergeJoinOptimize.Visit(relation, null);
                }
                plan.Relations[i] = relation;
            }

            EmitPushdown.EmitPushdown.Optimize(plan);
            return plan;
        }
    }
}
