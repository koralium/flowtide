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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.CommonSubPlan
{
    /// <summary>
    /// Counts how many times each subtree value occurs in the plan.
    /// Only subtrees built entirely from safe value equality types are counted.
    /// </summary>
    internal class CommonSubPlanCountVisitor : OptimizerBaseVisitor
    {
        /// <summary>
        /// Occurrence count per subtree value.
        /// </summary>
        public Dictionary<Relation, int> Counts { get; } = new Dictionary<Relation, int>();

        /// <summary>
        /// Node instances whose whole subtree has value based equality.
        /// </summary>
        public HashSet<Relation> SafeNodes { get; } = new HashSet<Relation>(ReferenceEqualityComparer.Instance);

        public override Relation Visit(Relation relation, object state)
        {
            // Children are visited first through the base visitor
            var result = base.Visit(relation, state);

            if (CommonSubPlanUtils.HasValueEquality(relation))
            {
                bool safe = true;
                foreach (var child in CommonSubPlanUtils.GetChildren(relation))
                {
                    if (!SafeNodes.Contains(child))
                    {
                        safe = false;
                        break;
                    }
                }
                if (safe)
                {
                    SafeNodes.Add(relation);
                    if (CommonSubPlanUtils.IsExtractable(relation))
                    {
                        Counts[relation] = Counts.TryGetValue(relation, out var count) ? count + 1 : 1;
                    }
                }
            }
            return result;
        }

        public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
        {
            // Loop internals cannot use references, only visit the input
            if (iterationRelation.Input != null)
            {
                iterationRelation.Input = Visit(iterationRelation.Input, state);
            }
            return iterationRelation;
        }
    }
}
