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
    /// Replaces duplicated subtrees with reference relations.
    /// The first usage moves the subtree to its own plan relation, all other
    /// usages reference it so the result is only computed once.
    /// </summary>
    internal class CommonSubPlanReplaceVisitor : OptimizerBaseVisitor
    {
        private readonly Plan _plan;
        private readonly Dictionary<Relation, int> _counts;
        private readonly HashSet<Relation> _safeNodes;
        private readonly Dictionary<Relation, int> _definitions = new Dictionary<Relation, int>();
        private bool _atRoot;

        public CommonSubPlanReplaceVisitor(Plan plan, Dictionary<Relation, int> counts, HashSet<Relation> safeNodes)
        {
            _plan = plan;
            _counts = counts;
            _safeNodes = safeNodes;
        }

        public void Run()
        {
            // Existing shareable top level relations, such as views, become definitions directly
            for (int i = 0; i < _plan.Relations.Count; i++)
            {
                var relation = _plan.Relations[i];
                if (IsShared(relation) && !_definitions.ContainsKey(relation))
                {
                    _definitions.Add(relation, i);
                }
            }

            // Count is re-evaluated each iteration so extracted definitions are visited as well
            for (int i = 0; i < _plan.Relations.Count; i++)
            {
                _atRoot = true;
                _plan.Relations[i] = Visit(_plan.Relations[i], null!);
            }
        }

        private bool IsShared(Relation relation)
        {
            return _safeNodes.Contains(relation) &&
                CommonSubPlanUtils.IsExtractable(relation) &&
                _counts.TryGetValue(relation, out var count) && count >= 2;
        }

        public override Relation Visit(Relation relation, object state)
        {
            var isRoot = _atRoot;
            _atRoot = false;

            if (isRoot)
            {
                // Top level relations are never replaced, they can only act as definitions
                return base.Visit(relation, state);
            }

            if (_definitions.TryGetValue(relation, out var relationId))
            {
                // This copy of the subtree is discarded, remove its occurrences
                DecrementCounts(relation);
                return new ReferenceRelation()
                {
                    RelationId = relationId,
                    ReferenceOutputLength = relation.OutputLength
                };
            }
            if (IsShared(relation))
            {
                // First usage, move the subtree to its own relation
                relationId = _plan.Relations.Count;
                _plan.Relations.Add(relation);
                _definitions.Add(relation, relationId);
                return new ReferenceRelation()
                {
                    RelationId = relationId,
                    ReferenceOutputLength = relation.OutputLength
                };
            }
            return base.Visit(relation, state);
        }

        /// <summary>
        /// Removes the occurrences of a discarded subtree so its inner nodes
        /// are not extracted with only a single remaining usage.
        /// </summary>
        private void DecrementCounts(Relation relation)
        {
            if (_counts.TryGetValue(relation, out var count))
            {
                _counts[relation] = count - 1;
            }
            foreach (var child in CommonSubPlanUtils.GetChildren(relation))
            {
                DecrementCounts(child);
            }
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
