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

namespace FlowtideDotNet.Core.Optimizer.DistributedMode
{
    /// <summary>
    /// Splits a not-yet-distributed plan into substreams. Sink roots are assigned round robin, and
    /// partitionable operators (merge joins, aggregates, window functions) are split into one copy
    /// per substream by <see cref="DistributeOperatorsVisitor"/>. Existing exchange relations (e.g.
    /// from distributed views) are placed in one consumer's substream; consumers in other substreams
    /// get cross-substream targets and references.
    ///
    /// Requires an optimized plan (only merge joins can be partitioned) and runs as the last optimizer
    /// step when <see cref="PlanOptimizerSettings.DistributedPlanOptions"/> is set. Deterministic, so
    /// each substream host can compute the plan independently.
    /// </summary>
    public static class DistributedPlanModifier
    {
        public static Plan ToDistributedPlan(Plan plan, DistributedPlanOptions options)
        {
            if (options.SubstreamCount < 1)
            {
                throw new ArgumentException("SubstreamCount must be at least 1", nameof(options));
            }
            foreach (var relation in plan.Relations)
            {
                if (relation is SubStreamRootRelation)
                {
                    throw new InvalidOperationException("The plan is already distributed, it contains substream root relations.");
                }
            }

            var nameGenerator = options.SubstreamNameGenerator ?? (i => $"substream_{i}");

            var (exchangeConsumers, plainReferencedIds, referencedIds) = CollectReferences(plan);

            // Sink roots are all top level relations that are not referenced by any other relation
            var sinkRoots = new List<int>();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                if (!referencedIds.Contains(i))
                {
                    sinkRoots.Add(i);
                }
            }
            if (sinkRoots.Count == 0)
            {
                throw new InvalidOperationException("The plan does not contain any sink roots, cannot distribute it.");
            }

            // Substream name per top level relation, null means the relation is global
            // and is built in every substream.
            var substreamNames = new string?[plan.Relations.Count];
            for (int i = 0; i < sinkRoots.Count; i++)
            {
                substreamNames[sinkRoots[i]] = nameGenerator(i % options.SubstreamCount);
            }

            // Distribute partitionable operators in the sink trees across the substreams.
            // The visitor appends new top level relations that are already wrapped in
            // substream roots.
            if (options.SubstreamCount > 1)
            {
                var operatorsVisitor = new DistributeOperatorsVisitor(plan, options.SubstreamCount, nameGenerator, GetNextExchangeTargetId(plan));
                for (int i = 0; i < sinkRoots.Count; i++)
                {
                    if (ContainsIteration(plan.Relations[sinkRoots[i]], plan))
                    {
                        // A loop must stay in one stream; its checkpoint prepares don't cross
                        // substreams. Keep the sink tree whole instead of partitioning it.
                        continue;
                    }
                    plan.Relations[sinkRoots[i]] = operatorsVisitor.VisitSinkRoot(plan.Relations[sinkRoots[i]], i);
                }

                // Push operators above the lane unions (filters, projects) down into the lanes so
                // they run per substream before the gather, reducing shuffled data.
                if (operatorsVisitor.LaneUnions.Count > 0)
                {
                    var pushdownVisitor = new LanePushdownVisitor(operatorsVisitor.LaneUnions);
                    for (int i = 0; i < sinkRoots.Count; i++)
                    {
                        plan.Relations[sinkRoots[i]] = pushdownVisitor.Visit(plan.Relations[sinkRoots[i]], null!);
                    }
                }

                // The plan may have grown with new already wrapped relations, take their
                // substream names from the wrappers and collect the references again since
                // parts of the sink trees have moved into the new relations.
                var grownSubstreamNames = new string?[plan.Relations.Count];
                Array.Copy(substreamNames, grownSubstreamNames, substreamNames.Length);
                for (int i = 0; i < plan.Relations.Count; i++)
                {
                    if (plan.Relations[i] is SubStreamRootRelation subStreamRoot)
                    {
                        grownSubstreamNames[i] = subStreamRoot.Name;
                    }
                }
                substreamNames = grownSubstreamNames;

                (exchangeConsumers, plainReferencedIds, _) = CollectReferences(plan);
            }

            // Resolve substreams for pre existing exchange relations, they are placed in the
            // substream of one of their consumers.
            foreach (var exchangeRelationId in exchangeConsumers.Keys)
            {
                if (plan.Relations[exchangeRelationId] is SubStreamRootRelation)
                {
                    // Created by the operator distribution, already assigned and has correct targets
                    continue;
                }
                ResolveSubstream(exchangeRelationId, substreamNames, exchangeConsumers, plainReferencedIds, new HashSet<int>());
            }

            // A loop and everything that reaches it through plain references must live in ONE
            // substream: a global (plain referenced) relation is built in EVERY substream, and a
            // substream that builds a loop nothing there consumes runs it orphaned - the loop has
            // no egress in that substream, commits state outside the checkpoint gating and
            // crashes at the first checkpoint. Every top level relation that contains or reaches
            // an iteration is therefore pinned to a single substream, together with the sinks and
            // exchanges that reach it (their plain references then stay inside that substream).
            // Grouping all of them into one substream is conservative when a plan has several
            // independent loops, but loop-reaching sinks are never partitioned anyway, so only
            // their placement is affected.
            var iterationGroup = new List<int>();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                if (plan.Relations[i] is SubStreamRootRelation)
                {
                    // Created by the operator distribution; loop-reaching sinks are never
                    // partitioned, so the grown lane relations cannot contain iterations.
                    continue;
                }
                if (ContainsIteration(plan.Relations[i], plan))
                {
                    iterationGroup.Add(i);
                }
            }
            if (iterationGroup.Count > 0)
            {
                var groupName = iterationGroup
                    .Select(i => substreamNames[i])
                    .FirstOrDefault(name => name != null) ?? nameGenerator(0);
                foreach (var i in iterationGroup)
                {
                    substreamNames[i] = groupName;
                }
            }

            // Find the highest existing exchange target id so new ids do not collide
            int nextExchangeTargetId = GetNextExchangeTargetId(plan);

            // Convert standard output targets and references that cross substream boundaries
            var replacements = new Dictionary<Relation, Relation>(ReferenceEqualityComparer.Instance);
            foreach (var kvp in exchangeConsumers)
            {
                var exchangeRelationId = kvp.Key;
                if (plan.Relations[exchangeRelationId] is SubStreamRootRelation)
                {
                    // Created by the operator distribution, the targets already have the correct types
                    continue;
                }
                if (plan.Relations[exchangeRelationId] is not ExchangeRelation exchangeRelation)
                {
                    throw new InvalidOperationException($"Relation {exchangeRelationId} is referenced as an exchange but is not an exchange relation.");
                }
                var exchangeSubstream = substreamNames[exchangeRelationId]!;

                foreach (var (reference, consumerIndex) in kvp.Value)
                {
                    var consumerSubstream = substreamNames[consumerIndex];
                    if (consumerSubstream == null)
                    {
                        throw new NotSupportedException(
                            "An exchange relation is referenced from a relation that is shared between multiple substreams, " +
                            "this is not supported by automatic distribution.");
                    }
                    if (consumerSubstream == exchangeSubstream)
                    {
                        // Same substream, keep the standard output target and reference
                        continue;
                    }
                    if (exchangeRelation.ExchangeKind.Type != ExchangeKindType.Scatter)
                    {
                        throw new NotSupportedException($"Exchange kind '{exchangeRelation.ExchangeKind.Type}' does not yet support substream targets.");
                    }
                    if (reference.TargetId < 0 || reference.TargetId >= exchangeRelation.Targets.Count)
                    {
                        throw new InvalidOperationException($"Exchange reference target id {reference.TargetId} is out of range.");
                    }
                    if (exchangeRelation.Targets[reference.TargetId] is not StandardOutputExchangeTarget standardOutputTarget)
                    {
                        throw new NotSupportedException("Multiple exchange references use the same target id, this is not supported by automatic distribution.");
                    }

                    var exchangeTargetId = nextExchangeTargetId++;
                    exchangeRelation.Targets[reference.TargetId] = new SubstreamExchangeTarget()
                    {
                        ExchangeTargetId = exchangeTargetId,
                        PartitionIds = standardOutputTarget.PartitionIds,
                        SubstreamName = consumerSubstream
                    };
                    replacements.Add(reference, new SubstreamExchangeReferenceRelation()
                    {
                        SubStreamName = exchangeSubstream,
                        ExchangeTargetId = exchangeTargetId,
                        ReferenceOutputLength = reference.ReferenceOutputLength,
                        Emit = reference.Emit,
                        Hint = reference.Hint
                    });
                }
            }

            if (replacements.Count > 0)
            {
                var rewriter = new ExchangeReferenceRewriter(replacements);
                for (int i = 0; i < plan.Relations.Count; i++)
                {
                    plan.Relations[i] = rewriter.Visit(plan.Relations[i], null!);
                }
            }

            // Wrap all relations that belong to a substream in substream root relations,
            // relations created by the operator distribution are already wrapped.
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                if (substreamNames[i] != null && plan.Relations[i] is not SubStreamRootRelation)
                {
                    plan.Relations[i] = new SubStreamRootRelation()
                    {
                        Name = substreamNames[i]!,
                        Input = plan.Relations[i]
                    };
                }
            }

            return plan;
        }

        private static (
            Dictionary<int, List<(StandardOutputExchangeReferenceRelation Reference, int ConsumerIndex)>> ExchangeConsumers,
            HashSet<int> PlainReferencedIds,
            HashSet<int> ReferencedIds) CollectReferences(Plan plan)
        {
            var referencedIds = new HashSet<int>();
            // Relation id -> consumer top level relation indices through exchange references
            var exchangeConsumers = new Dictionary<int, List<(StandardOutputExchangeReferenceRelation Reference, int ConsumerIndex)>>();
            // Relation ids referenced through plain reference relations
            var plainReferencedIds = new HashSet<int>();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                var collector = new ReferenceCollector();
                collector.Visit(plan.Relations[i], null!);

                foreach (var exchangeReference in collector.ExchangeReferences)
                {
                    referencedIds.Add(exchangeReference.RelationId);
                    if (!exchangeConsumers.TryGetValue(exchangeReference.RelationId, out var consumers))
                    {
                        consumers = new List<(StandardOutputExchangeReferenceRelation, int)>();
                        exchangeConsumers.Add(exchangeReference.RelationId, consumers);
                    }
                    consumers.Add((exchangeReference, i));
                }
                foreach (var plainReference in collector.PlainReferences)
                {
                    referencedIds.Add(plainReference.RelationId);
                    plainReferencedIds.Add(plainReference.RelationId);
                }
            }
            return (exchangeConsumers, plainReferencedIds, referencedIds);
        }

        private static string? ResolveSubstream(
            int relationId,
            string?[] substreamNames,
            Dictionary<int, List<(StandardOutputExchangeReferenceRelation Reference, int ConsumerIndex)>> exchangeConsumers,
            HashSet<int> plainReferencedIds,
            HashSet<int> visiting)
        {
            if (substreamNames[relationId] != null)
            {
                return substreamNames[relationId];
            }
            if (plainReferencedIds.Contains(relationId))
            {
                // Referenced through plain references, the relation is built in every substream
                return null;
            }
            if (!visiting.Add(relationId))
            {
                throw new InvalidOperationException("The plan contains a reference cycle, cannot distribute it.");
            }
            if (exchangeConsumers.TryGetValue(relationId, out var consumers))
            {
                // Place the exchange in the substream of one of its consumers.
                // The starting consumer is rotated on the relation id so multiple exchanges
                // spread across the substreams instead of all being placed in the first one.
                var orderedConsumers = consumers.Select(x => x.ConsumerIndex).Distinct().OrderBy(x => x).ToList();
                for (int i = 0; i < orderedConsumers.Count; i++)
                {
                    var consumerIndex = orderedConsumers[(relationId + i) % orderedConsumers.Count];
                    var substream = ResolveSubstream(consumerIndex, substreamNames, exchangeConsumers, plainReferencedIds, visiting);
                    if (substream != null)
                    {
                        substreamNames[relationId] = substream;
                        visiting.Remove(relationId);
                        return substream;
                    }
                }
                throw new NotSupportedException(
                    "An exchange relation is only referenced from relations that are shared between multiple substreams, " +
                    "this is not supported by automatic distribution.");
            }
            visiting.Remove(relationId);
            return null;
        }

        private static bool ContainsIteration(Relation relation, Plan plan)
        {
            var visitor = new IterationDetector(plan);
            visitor.Visit(relation, null!);
            return visitor.FoundIteration;
        }

        /// <summary>
        /// Detects iteration relations inside a relation tree, they run loops that cannot be
        /// split across substreams. Follows plain reference relations into the referenced top
        /// level relation, a recursive query is often hoisted into its own relation and reached
        /// only through a reference, partitioning the referencing tree would leave the loop
        /// built globally in every substream while only one of them consumes it.
        /// </summary>
        private sealed class IterationDetector : OptimizerBaseVisitor
        {
            private readonly Plan _plan;
            private readonly HashSet<int> _visited = new HashSet<int>();

            public IterationDetector(Plan plan)
            {
                _plan = plan;
            }

            public bool FoundIteration { get; private set; }

            public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
            {
                FoundIteration = true;
                return iterationRelation;
            }

            public override Relation VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, object state)
            {
                FoundIteration = true;
                return iterationReferenceReadRelation;
            }

            public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
            {
                if (referenceRelation.RelationId >= 0 &&
                    referenceRelation.RelationId < _plan.Relations.Count &&
                    _visited.Add(referenceRelation.RelationId))
                {
                    Visit(_plan.Relations[referenceRelation.RelationId], state);
                }
                return referenceRelation;
            }
        }

        private static int GetNextExchangeTargetId(Plan plan)
        {
            int maxId = -1;
            var collector = new ReferenceCollector();
            foreach (var relation in plan.Relations)
            {
                var unwrapped = relation;
                if (unwrapped is SubStreamRootRelation subStreamRoot)
                {
                    unwrapped = subStreamRoot.Input;
                }
                if (unwrapped is ExchangeRelation exchangeRelation)
                {
                    foreach (var target in exchangeRelation.Targets)
                    {
                        if (target is SubstreamExchangeTarget substreamTarget)
                        {
                            maxId = Math.Max(maxId, substreamTarget.ExchangeTargetId);
                        }
                        else if (target is PullBucketExchangeTarget pullBucketTarget)
                        {
                            maxId = Math.Max(maxId, pullBucketTarget.ExchangeTargetId);
                        }
                    }
                }
                collector.Visit(relation, null!);
            }
            foreach (var substreamReference in collector.SubstreamReferences)
            {
                maxId = Math.Max(maxId, substreamReference.ExchangeTargetId);
            }
            return maxId + 1;
        }

        /// <summary>
        /// Collects all reference relations inside a relation tree.
        /// </summary>
        private sealed class ReferenceCollector : OptimizerBaseVisitor
        {
            public List<StandardOutputExchangeReferenceRelation> ExchangeReferences { get; } = new List<StandardOutputExchangeReferenceRelation>();

            public List<ReferenceRelation> PlainReferences { get; } = new List<ReferenceRelation>();

            public List<SubstreamExchangeReferenceRelation> SubstreamReferences { get; } = new List<SubstreamExchangeReferenceRelation>();

            public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
            {
                ExchangeReferences.Add(standardOutputExchangeReferenceRelation);
                return standardOutputExchangeReferenceRelation;
            }

            public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
            {
                PlainReferences.Add(referenceRelation);
                return referenceRelation;
            }

            public override Relation VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, object state)
            {
                SubstreamReferences.Add(substreamExchangeReferenceRelation);
                return substreamExchangeReferenceRelation;
            }
        }

        /// <summary>
        /// Replaces specific standard output exchange reference instances with substream exchange references.
        /// </summary>
        private sealed class ExchangeReferenceRewriter : OptimizerBaseVisitor
        {
            private readonly Dictionary<Relation, Relation> _replacements;

            public ExchangeReferenceRewriter(Dictionary<Relation, Relation> replacements)
            {
                _replacements = replacements;
            }

            public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
            {
                if (_replacements.TryGetValue(standardOutputExchangeReferenceRelation, out var replacement))
                {
                    return replacement;
                }
                return standardOutputExchangeReferenceRelation;
            }
        }
    }
}
