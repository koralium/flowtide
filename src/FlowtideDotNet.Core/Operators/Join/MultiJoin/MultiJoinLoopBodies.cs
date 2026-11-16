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
    /// Finds the relations of a plan that run inside a loop although the loop names them by reference.
    /// </summary>
    internal static class MultiJoinLoopBodies
    {
        /// <summary>
        /// The indexes of the relations that read a loop they are not the owner of.
        /// </summary>
        public static HashSet<int> Find(Plan plan)
        {
            // Per relation the loops it reads without owning them, a reference passes those of its target on
            var reads = new HashSet<string>[plan.Relations.Count];
            for (int i = 0; i < reads.Length; i++)
            {
                reads[i] = new HashSet<string>();
            }
            var visitor = new Visitor(reads);
            bool added = true;
            while (added)
            {
                added = false;
                for (int i = 0; i < reads.Length; i++)
                {
                    added |= visitor.Collect(plan.Relations[i], reads[i]);
                }
            }
            var result = new HashSet<int>();
            for (int i = 0; i < reads.Length; i++)
            {
                if (reads[i].Count > 0)
                {
                    result.Add(i);
                }
            }
            return result;
        }

        private sealed class Visitor : OptimizerBaseVisitor
        {
            private readonly HashSet<string>[] _reads;
            private readonly List<string> _owned = new List<string>();
            private HashSet<string> _target = null!;
            private bool _added;

            public Visitor(HashSet<string>[] reads)
            {
                _reads = reads;
            }

            public bool Collect(Relation relation, HashSet<string> target)
            {
                _target = target;
                _added = false;
                _owned.Clear();
                Visit(relation, null!);
                return _added;
            }

            private void Read(string loop)
            {
                if (!_owned.Contains(loop))
                {
                    _added |= _target.Add(loop);
                }
            }

            public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
            {
                if (iterationRelation.Input != null)
                {
                    Visit(iterationRelation.Input, state);
                }
                _owned.Add(iterationRelation.IterationName);
                Visit(iterationRelation.LoopPlan, state);
                _owned.RemoveAt(_owned.Count - 1);
                return iterationRelation;
            }

            public override Relation VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, object state)
            {
                Read(iterationReferenceReadRelation.IterationName);
                return iterationReferenceReadRelation;
            }

            public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
            {
                ReadThrough(referenceRelation.RelationId);
                return referenceRelation;
            }

            public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
            {
                ReadThrough(standardOutputExchangeReferenceRelation.RelationId);
                return standardOutputExchangeReferenceRelation;
            }

            private void ReadThrough(int relationId)
            {
                if (relationId >= 0 && relationId < _reads.Length)
                {
                    // Copied: the set may be the one being added to
                    foreach (var loop in _reads[relationId].ToArray())
                    {
                        Read(loop);
                    }
                }
            }
        }
    }
}
