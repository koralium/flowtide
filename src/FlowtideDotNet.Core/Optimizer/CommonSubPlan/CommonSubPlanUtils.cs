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
    internal static class CommonSubPlanUtils
    {
        /// <summary>
        /// Relation types with verified deep value based equality.
        /// Only subtrees built entirely from these types can be shared,
        /// types missing from this list fall back to the base class equality
        /// which would treat different subtrees as equal.
        /// Types added here must also return their inputs in <see cref="GetChildren"/>,
        /// otherwise an unsafe input is never checked.
        /// </summary>
        public static bool HasValueEquality(Relation relation)
        {
            switch (relation)
            {
                case ReadRelation:
                case VirtualTableReadRelation:
                case ReferenceRelation:
                case FilterRelation:
                case ProjectRelation:
                case AggregateRelation:
                case JoinRelation:
                case MergeJoinRelation:
                case SetRelation:
                case SortRelation:
                case TopNRelation:
                case FetchRelation:
                case BufferRelation:
                case NormalizationRelation:
                case TableFunctionRelation:
                case ConsistentPartitionWindowRelation:
                case CheckRelation:
                // Loop plan is checked by IsClosedLoop
                case IterationRelation:
                case IterationReferenceReadRelation:
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// A loop can only be shared as a whole when its loop plan has value based
        /// equality and every iteration read binds to a loop inside it.
        /// A read of an outer loop would lose its loop when moved to its own relation.
        /// </summary>
        public static bool IsClosedLoop(IterationRelation iterationRelation)
        {
            return IsClosedLoop(iterationRelation, new List<string>());
        }

        private static bool IsClosedLoop(IterationRelation iterationRelation, List<string> scope)
        {
            // Input runs outside the loop, its name is not in scope
            if (iterationRelation.Input != null && !IsClosed(iterationRelation.Input, scope))
            {
                return false;
            }
            scope.Add(iterationRelation.IterationName);
            var closed = IsClosed(iterationRelation.LoopPlan, scope);
            scope.RemoveAt(scope.Count - 1);
            return closed;
        }

        private static bool IsClosed(Relation relation, List<string> scope)
        {
            switch (relation)
            {
                case IterationRelation iterationRelation:
                    return IsClosedLoop(iterationRelation, scope);
                case IterationReferenceReadRelation read:
                    return scope.Contains(read.IterationName);
            }
            if (!HasValueEquality(relation))
            {
                return false;
            }
            foreach (var child in GetChildren(relation))
            {
                if (!IsClosed(child, scope))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Only non leaf subtrees are extracted, sharing plain reads would
        /// stop later filter pushdown into each read.
        /// </summary>
        public static bool IsExtractable(Relation relation)
        {
            if (!HasValueEquality(relation))
            {
                return false;
            }
            if (relation is IterationRelation)
            {
                // Always a computation, even without an input
                return true;
            }
            foreach (var _ in GetChildren(relation))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Child relations for the value equality types, other types are not needed
        /// since their subtrees are never shared.
        /// </summary>
        public static IEnumerable<Relation> GetChildren(Relation relation)
        {
            switch (relation)
            {
                case FilterRelation filter:
                    yield return filter.Input;
                    break;
                case ProjectRelation project:
                    yield return project.Input;
                    break;
                case AggregateRelation aggregate:
                    yield return aggregate.Input;
                    break;
                case JoinRelation join:
                    yield return join.Left;
                    yield return join.Right;
                    break;
                case MergeJoinRelation mergeJoin:
                    yield return mergeJoin.Left;
                    yield return mergeJoin.Right;
                    break;
                case SetRelation set:
                    foreach (var input in set.Inputs)
                    {
                        yield return input;
                    }
                    break;
                case SortRelation sort:
                    yield return sort.Input;
                    break;
                case TopNRelation topN:
                    yield return topN.Input;
                    break;
                case FetchRelation fetch:
                    yield return fetch.Input;
                    break;
                case BufferRelation buffer:
                    yield return buffer.Input;
                    break;
                case NormalizationRelation normalization:
                    yield return normalization.Input;
                    break;
                case TableFunctionRelation tableFunction:
                    if (tableFunction.Input != null)
                    {
                        yield return tableFunction.Input;
                    }
                    break;
                case ConsistentPartitionWindowRelation window:
                    yield return window.Input;
                    break;
                case CheckRelation check:
                    yield return check.Input;
                    break;
                case IterationRelation iteration:
                    // Loop plan is not a child, it is never shared on its own
                    if (iteration.Input != null)
                    {
                        yield return iteration.Input;
                    }
                    break;
            }
        }
    }
}
