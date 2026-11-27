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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    // Finds the relation that hosts each row store of a plan
    internal sealed class RowStoreHostCollector : BaseRelationExpressionVisitor<object?>
    {
        // A subquery's relation sits inside an expression
        private sealed class SubqueryVisitor : ExpressionVisitor<object?, object>
        {
            private readonly RowStoreHostCollector _collector;

            public SubqueryVisitor(RowStoreHostCollector collector)
            {
                _collector = collector;
            }

            public override object? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object state)
            {
                _collector.Visit(setPredicateExpression.Relation, state);
                return base.VisitSetPredicateExpression(setPredicateExpression, state);
            }
        }

        private readonly SubqueryVisitor _subqueryVisitor;

        public RowStoreHostCollector()
        {
            _subqueryVisitor = new SubqueryVisitor(this);
        }

        public override ExpressionVisitor<object?, object> Visitor => _subqueryVisitor;

        // Overrides return their input, the plan stays untouched
        public Dictionary<string, Relation> Hosts { get; } = new Dictionary<string, Relation>();

        public int Fetches { get; private set; }

        // One per store a fetch reads from
        public int Readers { get; private set; }

        // The stores of a plan and the readers its fetches open
        public static void Count(IReadOnlyList<Relation> relations, out int stores, out int readers)
        {
            var collector = new RowStoreHostCollector();
            for (int i = 0; i < relations.Count; i++)
            {
                collector.Visit(relations[i], null!);
            }
            stores = collector.Hosts.Count;
            readers = collector.Readers;
        }

        // True when a plan stores rows behind references or fetches them
        public static bool UsesRowStores(IReadOnlyList<Relation> relations)
        {
            var collector = new RowStoreHostCollector();
            for (int i = 0; i < relations.Count; i++)
            {
                collector.Visit(relations[i], null!);
            }
            return collector.Hosts.Count > 0 || collector.Fetches > 0;
        }

        public static Dictionary<string, Relation> Collect(IReadOnlyList<Relation> relations)
        {
            var collector = new RowStoreHostCollector();
            for (int i = 0; i < relations.Count; i++)
            {
                collector.Visit(relations[i], null!);
            }
            return collector.Hosts;
        }

        public static RowStoreDefinition? RowStoreOf(Relation relation)
        {
            switch (relation)
            {
                case ReadRelation read:
                    return read.RowStore;
                case NormalizationRelation normalization:
                    return normalization.RowStore;
                default:
                    return null;
            }
        }

        private void Add(Relation relation, RowStoreDefinition? rowStore)
        {
            if (rowStore == null)
            {
                return;
            }
            if (Hosts.TryGetValue(rowStore.Name, out var existing) && !ReferenceEquals(existing, relation))
            {
                throw new InvalidOperationException($"Row store '{rowStore.Name}' is hosted by more than one relation.");
            }
            Hosts[rowStore.Name] = relation;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            Add(readRelation, readRelation.RowStore);
            return base.VisitReadRelation(readRelation, state);
        }

        public override Relation VisitFetchRequiredColumnsRelation(FetchRequiredColumnsRelation fetchRequiredColumnsRelation, object state)
        {
            Fetches++;
            Readers += fetchRequiredColumnsRelation.Fetches.Count;
            return base.VisitFetchRequiredColumnsRelation(fetchRequiredColumnsRelation, state);
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            Add(normalizationRelation, normalizationRelation.RowStore);
            return base.VisitNormalizationRelation(normalizationRelation, state);
        }
    }
}
