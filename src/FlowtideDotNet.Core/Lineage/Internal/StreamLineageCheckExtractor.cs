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

using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    // Runs on the plan the operators were built from, so the check relations are the same objects.
    internal static class StreamLineageCheckExtractor
    {
        public static IReadOnlyList<StreamLineageCheck> Extract(
            Plan plan,
            IConnectorManager connectorManager,
            StreamLineage lineage,
            IReadOnlyList<(CheckRelation Relation, IReadOnlyList<string> CheckIds)> builtChecks,
            bool distributed)
        {
            if (builtChecks.Count == 0)
            {
                return [];
            }

            var index = new LineagePlanIndex(plan.Relations);
            var outputs = new Dictionary<string, StreamLineageOutput>(StringComparer.Ordinal);
            foreach (var output in lineage.Outputs)
            {
                outputs.TryAdd(output.Key, output);
            }

            // Every write counts, a check can feed a write of another substream.
            var targets = new Dictionary<CheckRelation, List<StreamLineageCheckTarget>>(ReferenceEqualityComparer.Instance);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                if (LineagePlanIndex.Unwrap(plan.Relations[i]) is not WriteRelation write)
                {
                    continue;
                }
                var reachable = new LineageReachableReadsVisitor(index);
                reachable.Visit(write.Input, null!);
                if (reachable.Checks.Count == 0)
                {
                    continue;
                }
                var target = GetTarget(write, outputs, connectorManager);
                if (target == null)
                {
                    continue;
                }
                foreach (var check in reachable.Checks)
                {
                    if (!targets.TryGetValue(check, out var checkTargets))
                    {
                        checkTargets = new List<StreamLineageCheckTarget>();
                        targets.Add(check, checkTargets);
                    }
                    if (!checkTargets.Any(x => x.Key == target.Key))
                    {
                        checkTargets.Add(target);
                    }
                }
            }

            var replicated = distributed ? GetReplicatedChecks(index) : new HashSet<CheckRelation>(ReferenceEqualityComparer.Instance);
            var result = new List<StreamLineageCheck>();
            foreach (var (relation, checkIds) in builtChecks)
            {
                var checkTargets = targets.TryGetValue(relation, out var found) ? found : [];
                for (int i = 0; i < checkIds.Count; i++)
                {
                    result.Add(new StreamLineageCheck()
                    {
                        CheckId = checkIds[i],
                        Message = relation.Checks[i].Message,
                        Targets = checkTargets,
                        Replicated = replicated.Contains(relation)
                    });
                }
            }
            return result;
        }

        // Writes of other substreams are resolved here, a failing connector only loses that target.
        private static StreamLineageCheckTarget? GetTarget(WriteRelation write, Dictionary<string, StreamLineageOutput> outputs, IConnectorManager connectorManager)
        {
            var key = write.NamedObject.DotSeperated;
            if (outputs.TryGetValue(key, out var output))
            {
                return new StreamLineageCheckTarget(key, output.Namespace, output.TableName, output.NameParts);
            }
            try
            {
                var metadata = connectorManager.GetSinkFactory(write).GetLineageMetadata(write, false);
                return new StreamLineageCheckTarget(key, metadata.Namespace, metadata.TableName, StreamLineageExtractor.GetNameParts(write.NamedObject.Names, metadata.TableName));
            }
            catch (Exception)
            {
                return null;
            }
        }

        // A global relation is built by every substream that references it.
        private static HashSet<CheckRelation> GetReplicatedChecks(LineagePlanIndex index)
        {
            var collector = new CheckCollector();
            for (int i = 0; i < index.Relations.Count; i++)
            {
                if (index.GetRootSubstream(i) == null)
                {
                    collector.Visit(index.Relations[i], null!);
                }
            }
            return collector.Checks;
        }

        // References are not followed, so only the checks inside the visited relation are found.
        private sealed class CheckCollector : OptimizerBaseVisitor
        {
            public HashSet<CheckRelation> Checks { get; } = new HashSet<CheckRelation>(ReferenceEqualityComparer.Instance);

            public override Relation VisitCheckRelation(CheckRelation checkRelation, object state)
            {
                Checks.Add(checkRelation);
                return base.VisitCheckRelation(checkRelation, state);
            }

            public override Relation VisitPlanRelation(PlanRelation planRelation, object state)
            {
                Visit(planRelation.Root, state);
                return planRelation;
            }
        }
    }
}
