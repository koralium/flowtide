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
using System.Text;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// One group of joins a multi join would replace.
    /// </summary>
    public sealed class MultiJoinReportGroup
    {
        public required int InputCount { get; init; }
        public required int InnerJoinCount { get; init; }
        public required int LeftJoinCount { get; init; }
        public required IReadOnlyList<string> InputNames { get; init; }
    }

    /// <summary>
    /// Which merge joins of a plan would become multi joins and why the others would not. Reading it changes nothing.
    /// </summary>
    public sealed class MultiJoinReport
    {
        public const string InsideLoopReason = "inside a loop body";

        public int MergeJoinCount { get; private set; }

        public IReadOnlyList<MultiJoinReportGroup> Groups => _groups;

        /// <summary>
        /// Merge joins outside every group, counted by reason.
        /// </summary>
        public IReadOnlyDictionary<string, int> LeftOut => _leftOut;

        private readonly List<MultiJoinReportGroup> _groups = new List<MultiJoinReportGroup>();
        private readonly Dictionary<string, int> _leftOut = new Dictionary<string, int>();

        // The last reason seen per join, turned into counts once every group is known
        private readonly Dictionary<MergeJoinRelation, string> _reasons = new Dictionary<MergeJoinRelation, string>(ReferenceEqualityComparer.Instance);

        private MultiJoinReport()
        {
        }

        /// <summary>
        /// Run it on a plan that is optimized but not yet parallelized or distributed, those steps rewrite every join.
        /// </summary>
        public static MultiJoinReport Create(Plan plan, int maxInputs)
        {
            return Create(plan, maxInputs, true);
        }

        /// <summary>
        /// The same with the joins the setting for conditions on both sides lets in.
        /// </summary>
        public static MultiJoinReport Create(Plan plan, int maxInputs, bool pairConditions)
        {
            if (maxInputs < MultiJoinPlanBuilder.MinInputs)
            {
                throw new ArgumentOutOfRangeException(nameof(maxInputs), $"A multi join needs room for at least {MultiJoinPlanBuilder.MinInputs} inputs.");
            }
            var report = new MultiJoinReport();
            var visitor = new Visitor(report, maxInputs, pairConditions);
            var loopBodies = MultiJoinLoopBodies.Find(plan);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                visitor.VisitRoot(plan.Relations[i], loopBodies.Contains(i));
            }
            foreach (var reason in report._reasons.Values)
            {
                report._leftOut.TryGetValue(reason, out var count);
                report._leftOut[reason] = count + 1;
            }
            return report;
        }

        public override string ToString()
        {
            int grouped = _groups.Sum(x => x.InnerJoinCount + x.LeftJoinCount);
            var sb = new StringBuilder();
            sb.AppendLine($"Merge joins: {MergeJoinCount}, in multi join groups: {grouped}");
            for (int i = 0; i < _groups.Count; i++)
            {
                var group = _groups[i];
                sb.AppendLine($"Group {i + 1}: {group.InputCount} inputs, {group.InnerJoinCount} inner and {group.LeftJoinCount} outer or mark joins");
                sb.AppendLine($"  inputs: {string.Join(", ", group.InputNames)}");
            }
            if (_leftOut.Count > 0)
            {
                sb.AppendLine("Left out, by reason:");
                foreach (var kv in _leftOut.OrderByDescending(x => x.Value).ThenBy(x => x.Key))
                {
                    sb.AppendLine($"  {kv.Value} x {kv.Key}");
                }
            }
            return sb.ToString();
        }

        private sealed class Visitor : OptimizerBaseVisitor
        {
            private readonly MultiJoinReport _report;
            private readonly int _maxInputs;
            private int _loopDepth;

            private readonly bool _pairConditions;

            public Visitor(MultiJoinReport report, int maxInputs, bool pairConditions)
            {
                _report = report;
                _pairConditions = pairConditions;
                _maxInputs = maxInputs;
            }

            public void VisitRoot(Relation relation, bool loopBody)
            {
                _loopDepth = loopBody ? 1 : 0;
                Visit(relation, null!);
            }

            private void AddLeftOut(MergeJoinRelation join, string reason)
            {
                // A join left out of a group is looked at again as a root of its own, the later look decides
                _report._reasons[join] = reason;
            }

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                _report.MergeJoinCount++;
                if (_loopDepth > 0)
                {
                    AddLeftOut(mergeJoinRelation, InsideLoopReason);
                    Visit(mergeJoinRelation.Left, state);
                    Visit(mergeJoinRelation.Right, state);
                    return mergeJoinRelation;
                }

                var leftOut = new List<MultiJoinLeftOut>();
                var plan = MultiJoinPlanBuilder.Discover(mergeJoinRelation, _maxInputs, leftOut, _pairConditions);
                foreach (var entry in leftOut)
                {
                    AddLeftOut(entry.Join, entry.Reason);
                }
                if (plan == null)
                {
                    Visit(mergeJoinRelation.Left, state);
                    Visit(mergeJoinRelation.Right, state);
                    return mergeJoinRelation;
                }

                // A join can be left out of a group above and still form a group of its own with a fresh cap
                foreach (var join in plan.Joins)
                {
                    _report._reasons.Remove(join);
                }
                // The other joins of the group are not visited, only the inputs are
                _report.MergeJoinCount += plan.Joins.Count - 1;
                _report._groups.Add(new MultiJoinReportGroup()
                {
                    InputCount = plan.Inputs.Count,
                    InnerJoinCount = plan.InnerJoinCount,
                    LeftJoinCount = plan.LeftJoinCount,
                    InputNames = plan.Inputs.Select(x => Describe(x.Relation)).ToList()
                });
                foreach (var input in plan.Inputs)
                {
                    Visit(input.Relation, state);
                }
                return mergeJoinRelation;
            }

            public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
            {
                if (iterationRelation.Input != null)
                {
                    Visit(iterationRelation.Input, state);
                }
                // Only the loop plan is the loop body, the input runs once outside it
                _loopDepth++;
                Visit(iterationRelation.LoopPlan, state);
                _loopDepth--;
                return iterationRelation;
            }
        }

        private static string Describe(Relation relation)
        {
            switch (relation)
            {
                case ReadRelation read:
                    return read.NamedTable.DotSeperated;
                case ReferenceRelation reference:
                    return $"reference {reference.RelationId}";
                case FilterRelation filter:
                    return $"filter({Describe(filter.Input)})";
                case ProjectRelation project:
                    return $"project({Describe(project.Input)})";
                case MergeJoinRelation:
                    return "join";
                default:
                    var name = relation.GetType().Name;
                    return name.EndsWith("Relation") ? name.Substring(0, name.Length - "Relation".Length) : name;
            }
        }
    }
}
