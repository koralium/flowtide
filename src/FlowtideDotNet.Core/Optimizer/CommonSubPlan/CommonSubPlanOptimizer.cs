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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.CommonSubPlan
{
    /// <summary>
    /// Finds identical subtrees in the plan and computes them only once.
    /// The subtree is moved to its own plan relation and each usage is replaced
    /// with a reference relation, so for example the same aggregation used in
    /// two places becomes a single operator with two outputs.
    /// </summary>
    internal static class CommonSubPlanOptimizer
    {
        public static Plan Optimize(Plan plan)
        {
            // Substream plans place relations in specific streams, sharing
            // between substreams requires exchanges, skip those plans.
            foreach (var relation in plan.Relations)
            {
                if (relation is SubStreamRootRelation)
                {
                    return plan;
                }
            }

            var countVisitor = new CommonSubPlanCountVisitor();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                countVisitor.Visit(plan.Relations[i], null!);
            }

            bool anyDuplicate = false;
            foreach (var count in countVisitor.Counts.Values)
            {
                if (count >= 2)
                {
                    anyDuplicate = true;
                    break;
                }
            }
            if (!anyDuplicate)
            {
                return plan;
            }

            var replaceVisitor = new CommonSubPlanReplaceVisitor(plan, countVisitor.Counts, countVisitor.SafeNodes);
            replaceVisitor.Run();
            return plan;
        }
    }
}
