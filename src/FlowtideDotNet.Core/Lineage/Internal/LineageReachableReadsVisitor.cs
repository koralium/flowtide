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
using FlowtideDotNet.Core.Optimizer.GetTimestamp;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    // Overrides return their input, the plan stays untouched.
    internal sealed class LineageReachableReadsVisitor : OptimizerBaseVisitor
    {
        private readonly LineagePlanIndex _index;

        // Dedup and cycle guard in one.
        private readonly HashSet<Relation> _followed = new HashSet<Relation>(ReferenceEqualityComparer.Instance);

        public LineageReachableReadsVisitor(LineagePlanIndex index)
        {
            _index = index;
        }

        public List<ReadRelation> Reads { get; } = new List<ReadRelation>();

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (readRelation.NamedTable.DotSeperated != GetTimestampVisitor.GetTimestampTableName)
            {
                Reads.Add(readRelation);
            }
            return readRelation;
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
        {
            if (_index.TryGetRelation(referenceRelation.RelationId, out var target))
            {
                Follow(target, state);
            }
            return referenceRelation;
        }

        public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
        {
            if (_index.TryResolve(standardOutputExchangeReferenceRelation, out var exchange))
            {
                Follow(exchange, state);
            }
            return standardOutputExchangeReferenceRelation;
        }

        public override Relation VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, object state)
        {
            if (_index.TryResolve(substreamExchangeReferenceRelation, out var exchange))
            {
                Follow(exchange, state);
            }
            return substreamExchangeReferenceRelation;
        }

        public override Relation VisitPullExchangeReferenceRelation(PullExchangeReferenceRelation pullExchangeReferenceRelation, object state)
        {
            if (_index.TryResolve(pullExchangeReferenceRelation, out var exchange))
            {
                Follow(exchange, state);
            }
            return pullExchangeReferenceRelation;
        }

        public override Relation VisitPlanRelation(PlanRelation planRelation, object state)
        {
            Visit(planRelation.Root, state);
            return planRelation;
        }

        private void Follow(Relation relation, object state)
        {
            if (_followed.Add(relation))
            {
                Visit(relation, state);
            }
        }
    }
}
