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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal sealed class LineagePlanIndex
    {
        private readonly IReadOnlyList<Relation> _relations;
        private readonly string?[] _rootSubstreams;
        private readonly Dictionary<(string Substream, int TargetId), ExchangeRelation> _substreamTargets = new();
        private readonly Dictionary<(string Substream, int TargetId), ExchangeRelation> _pullTargets = new();

        public LineagePlanIndex(IReadOnlyList<Relation> relations)
        {
            _relations = relations;
            _rootSubstreams = new string?[relations.Count];

            for (int i = 0; i < relations.Count; i++)
            {
                var inner = Unwrap(relations[i], out var substreamName);
                _rootSubstreams[i] = substreamName;

                if (inner is not ExchangeRelation exchangeRelation)
                {
                    continue;
                }

                // Keyed by the producer substream, as references name it.
                var producer = substreamName ?? string.Empty;
                foreach (var target in exchangeRelation.Targets)
                {
                    if (target is SubstreamExchangeTarget substreamTarget)
                    {
                        _substreamTargets.TryAdd((producer, substreamTarget.ExchangeTargetId), exchangeRelation);
                    }
                    else if (target is PullBucketExchangeTarget pullTarget)
                    {
                        _pullTargets.TryAdd((producer, pullTarget.ExchangeTargetId), exchangeRelation);
                    }
                }
            }
        }

        public IReadOnlyList<Relation> Relations => _relations;

        public string? GetRootSubstream(int relationId)
        {
            return _rootSubstreams[relationId];
        }

        public static Relation Unwrap(Relation relation)
        {
            return Unwrap(relation, out _);
        }

        public static Relation Unwrap(Relation relation, out string? substreamName)
        {
            substreamName = null;
            while (true)
            {
                switch (relation)
                {
                    case SubStreamRootRelation subStreamRoot:
                        substreamName ??= subStreamRoot.Name;
                        relation = subStreamRoot.Input;
                        break;
                    case RootRelation root:
                        relation = root.Input;
                        break;
                    case PlanRelation planRelation:
                        relation = planRelation.Root.Input;
                        break;
                    default:
                        return relation;
                }
            }
        }

        public bool TryGetRelation(int relationId, [NotNullWhen(true)] out Relation? relation)
        {
            if (relationId < 0 || relationId >= _relations.Count)
            {
                relation = null;
                return false;
            }
            relation = _relations[relationId];
            return true;
        }

        public bool TryResolve(StandardOutputExchangeReferenceRelation reference, [NotNullWhen(true)] out ExchangeRelation? exchange)
        {
            if (TryGetRelation(reference.RelationId, out var relation) &&
                Unwrap(relation) is ExchangeRelation exchangeRelation)
            {
                exchange = exchangeRelation;
                return true;
            }
            exchange = null;
            return false;
        }

        public bool TryResolve(SubstreamExchangeReferenceRelation reference, [NotNullWhen(true)] out ExchangeRelation? exchange)
        {
            return _substreamTargets.TryGetValue((reference.SubStreamName, reference.ExchangeTargetId), out exchange);
        }

        public bool TryResolve(PullExchangeReferenceRelation reference, [NotNullWhen(true)] out ExchangeRelation? exchange)
        {
            return _pullTargets.TryGetValue((reference.SubStreamName, reference.ExchangeTargetId), out exchange);
        }
    }
}
