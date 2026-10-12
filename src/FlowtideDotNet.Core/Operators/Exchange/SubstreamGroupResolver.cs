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

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// The substreams one substream exchanges data with, how many its connected group holds and the most hops between two of them.
    /// </summary>
    internal sealed record SubstreamGroup(IReadOnlyCollection<string> Peers, int GroupSize, int Distance);

    /// <summary>
    /// Derives the substream graph from the full plan, every substream sees the same one.
    /// </summary>
    internal static class SubstreamGroupResolver
    {
        public static SubstreamGroup Resolve(Plan plan, string selfSubstreamName)
        {
            var edges = new Dictionary<string, HashSet<string>>();
            foreach (var relation in plan.Relations)
            {
                if (relation is SubStreamRootRelation root)
                {
                    var visitor = new EdgeVisitor(plan, root.Name);
                    visitor.Visit(root.Input, null!);
                    foreach (var neighbour in visitor.Neighbours)
                    {
                        // Undirected, claims travel both ways whoever produces the data.
                        Connect(edges, root.Name, neighbour);
                        Connect(edges, neighbour, root.Name);
                    }
                }
            }

            if (!edges.TryGetValue(selfSubstreamName, out var peers))
            {
                return new SubstreamGroup(Array.Empty<string>(), 1, 0);
            }

            // Substreams that never exchange data, directly or through others, do not count.
            var group = new HashSet<string>() { selfSubstreamName };
            var queue = new Queue<string>();
            queue.Enqueue(selfSubstreamName);
            while (queue.Count > 0)
            {
                foreach (var neighbour in edges[queue.Dequeue()])
                {
                    if (group.Add(neighbour))
                    {
                        queue.Enqueue(neighbour);
                    }
                }
            }
            // Every substream derives the same number, the claims travel exactly this far.
            var distance = 0;
            foreach (var start in group)
            {
                distance = Math.Max(distance, HopsToTheFarthest(edges, start));
            }
            return new SubstreamGroup(peers.ToList(), group.Count, distance);
        }

        private static int HopsToTheFarthest(Dictionary<string, HashSet<string>> edges, string start)
        {
            var hops = new Dictionary<string, int>() { { start, 0 } };
            var queue = new Queue<string>();
            queue.Enqueue(start);
            var farthest = 0;
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                foreach (var neighbour in edges[current])
                {
                    if (hops.TryAdd(neighbour, hops[current] + 1))
                    {
                        farthest = Math.Max(farthest, hops[neighbour]);
                        queue.Enqueue(neighbour);
                    }
                }
            }
            return farthest;
        }

        private static void Connect(Dictionary<string, HashSet<string>> edges, string from, string to)
        {
            if (from == to)
            {
                return;
            }
            if (!edges.TryGetValue(from, out var neighbours))
            {
                neighbours = new HashSet<string>();
                edges.Add(from, neighbours);
            }
            neighbours.Add(to);
        }

        private sealed class EdgeVisitor : OptimizerBaseVisitor
        {
            private readonly Plan _plan;
            private readonly string _substreamName;
            private readonly HashSet<int> _followedReferences = new HashSet<int>();

            public EdgeVisitor(Plan plan, string substreamName)
            {
                _plan = plan;
                _substreamName = substreamName;
            }

            public HashSet<string> Neighbours { get; } = new HashSet<string>();

            public override Relation VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, object state)
            {
                Neighbours.Add(substreamExchangeReferenceRelation.SubStreamName);
                return substreamExchangeReferenceRelation;
            }

            public override Relation VisitExchangeRelation(ExchangeRelation exchangeRelation, object state)
            {
                foreach (var target in exchangeRelation.Targets)
                {
                    if (target is SubstreamExchangeTarget substreamTarget)
                    {
                        Neighbours.Add(substreamTarget.SubstreamName);
                    }
                }
                return base.VisitExchangeRelation(exchangeRelation, state);
            }

            public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
            {
                // A shared sub plan belongs to whoever references it.
                if (_followedReferences.Add(referenceRelation.RelationId) &&
                    referenceRelation.RelationId >= 0 &&
                    referenceRelation.RelationId < _plan.Relations.Count)
                {
                    Visit(_plan.Relations[referenceRelation.RelationId], state);
                }
                return referenceRelation;
            }

            public override Relation VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, object state)
            {
                if (subStreamRootRelation.Name != _substreamName)
                {
                    // Another substream's part of the plan, its edges are collected from its own root.
                    return subStreamRootRelation;
                }
                return base.VisitSubStreamRootRelation(subStreamRootRelation, state);
            }
        }
    }
}
