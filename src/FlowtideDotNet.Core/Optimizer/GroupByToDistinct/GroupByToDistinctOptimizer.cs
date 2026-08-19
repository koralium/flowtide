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

namespace FlowtideDotNet.Core.Optimizer.GroupByToDistinct
{
    /// <summary>
    /// Rewrites a group by without measures into a distinct.
    /// </summary>
    internal static class GroupByToDistinctOptimizer
    {
        public static Plan Optimize(Plan plan)
        {
            var visitor = new GroupByToDistinctVisitor();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                plan.Relations[i] = visitor.Visit(plan.Relations[i], null!);
            }
            return plan;
        }
    }

    internal class GroupByToDistinctVisitor : OptimizerBaseVisitor
    {
        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, state);

            if (!CanRewrite(aggregateRelation))
            {
                return aggregateRelation;
            }

            var groupingExpressions = aggregateRelation.Groupings![0].GroupingExpressions;
            var inputLength = aggregateRelation.Input.OutputLength;

            // Project the grouping values, emit only those columns
            var projectEmit = new List<int>(groupingExpressions.Count);
            for (int i = 0; i < groupingExpressions.Count; i++)
            {
                projectEmit.Add(inputLength + i);
            }

            var projectRelation = new ProjectRelation()
            {
                Input = aggregateRelation.Input,
                Expressions = groupingExpressions,
                Emit = projectEmit
            };

            return new SetRelation()
            {
                Operation = SetOperation.UnionDistinct,
                Inputs = new List<Relation>() { projectRelation },
                // Emit points at the grouping values, keep it
                Emit = aggregateRelation.Emit,
                Hint = aggregateRelation.Hint
            };
        }

        private static bool CanRewrite(AggregateRelation aggregateRelation)
        {
            if (aggregateRelation.Measures != null && aggregateRelation.Measures.Count > 0)
            {
                return false;
            }
            if (aggregateRelation.Groupings == null)
            {
                return false;
            }
            // Grouping sets add a grouping id we cant produce
            if (aggregateRelation.Groupings.Count != 1)
            {
                return false;
            }
            // Group by with no expressions returns a single row
            if (aggregateRelation.Groupings[0].GroupingExpressions.Count == 0)
            {
                return false;
            }
            return true;
        }
    }
}
