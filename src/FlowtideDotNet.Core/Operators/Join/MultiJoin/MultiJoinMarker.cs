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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// Marks the merge joins of a plan that form multi join groups. Joins inside a loop body are not marked.
    /// </summary>
    internal static class MultiJoinMarker
    {
        /// <summary>
        /// Returns the number of groups marked, marks the plan carried before are replaced. Run it on a plan that is not parallelized or distributed.
        /// </summary>
        public static int Mark(Plan plan, int maxInputs)
        {
            Clear(plan);
            var visitor = new Visitor(maxInputs);
            var loopBodies = MultiJoinLoopBodies.Find(plan);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                if (!loopBodies.Contains(i))
                {
                    visitor.Visit(plan.Relations[i], null!);
                }
            }
            return visitor.Groups;
        }

        /// <summary>
        /// Removes every mark, a plan read from its serialized form can carry marks from an earlier run of the optimizer.
        /// </summary>
        public static void Clear(Plan plan)
        {
            var clear = new ClearVisitor();
            foreach (var relation in plan.Relations)
            {
                clear.Visit(relation, null!);
            }
        }

        /// <summary>
        /// Returns the number of groups the plan carries marks for.
        /// </summary>
        public static int Count(Plan plan)
        {
            var count = new CountVisitor();
            foreach (var relation in plan.Relations)
            {
                count.Visit(relation, null!);
            }
            return count.Groups.Count;
        }

        private sealed class CountVisitor : OptimizerBaseVisitor
        {
            public HashSet<int> Groups { get; } = new HashSet<int>();

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                if (mergeJoinRelation.MultiJoinGroup != null)
                {
                    Groups.Add(mergeJoinRelation.MultiJoinGroup.Value);
                }
                return base.VisitMergeJoinRelation(mergeJoinRelation, state);
            }
        }

        private sealed class ClearVisitor : OptimizerBaseVisitor
        {
            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                mergeJoinRelation.MultiJoinGroup = null;
                return base.VisitMergeJoinRelation(mergeJoinRelation, state);
            }
        }

        private sealed class Visitor : OptimizerBaseVisitor
        {
            private readonly int _maxInputs;
            private int _loopDepth;

            public int Groups { get; private set; }

            public Visitor(int maxInputs)
            {
                _maxInputs = maxInputs;
            }

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                if (_loopDepth == 0)
                {
                    var plan = MultiJoinPlanBuilder.Discover(mergeJoinRelation, _maxInputs);
                    if (plan != null)
                    {
                        Groups++;
                        foreach (var join in plan.Joins)
                        {
                            join.MultiJoinGroup = Groups;
                        }
                        // The other joins of the group are not visited, only the inputs are
                        foreach (var input in plan.Inputs)
                        {
                            Visit(input.Relation, state);
                        }
                        return mergeJoinRelation;
                    }
                }
                Visit(mergeJoinRelation.Left, state);
                Visit(mergeJoinRelation.Right, state);
                return mergeJoinRelation;
            }

            public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
            {
                if (iterationRelation.Input != null)
                {
                    Visit(iterationRelation.Input, state);
                }
                _loopDepth++;
                Visit(iterationRelation.LoopPlan, state);
                _loopDepth--;
                return iterationRelation;
            }
        }
    }
}
