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

using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// Flattens a tree of merge joins into a multi join plan.
    /// </summary>
    internal sealed class MultiJoinPlanBuilder
    {
        public const int MinInputs = 3;

        private readonly MultiJoinPlan _plan = new MultiJoinPlan();
        private readonly List<MultiJoinLeftOut> _leftOut = new List<MultiJoinLeftOut>();
        private readonly int? _groupId;
        private int _joinBudget;

        private MultiJoinPlanBuilder(int? groupId, int joinBudget)
        {
            _groupId = groupId;
            _joinBudget = joinBudget;
        }

        /// <summary>
        /// The group a root would form under the input cap, existing marks are ignored.
        /// </summary>
        public static MultiJoinPlan? Discover(MergeJoinRelation root, int maxInputs, List<MultiJoinLeftOut>? leftOut = null)
        {
            if (maxInputs < MinInputs)
            {
                throw new ArgumentOutOfRangeException(nameof(maxInputs), $"A multi join needs room for at least {MinInputs} inputs.");
            }
            var builder = new MultiJoinPlanBuilder(null, maxInputs - 1);
            var result = builder.Flatten(root);
            MultiJoinPlan? plan = null;
            if (result.Node.IsLeaf)
            {
                // The root itself was left out, its reason is already recorded
            }
            else if (builder._plan.Inputs.Count < MinInputs)
            {
                builder._leftOut.Clear();
                builder._leftOut.Add(new MultiJoinLeftOut(root, "fewer than three inputs"));
            }
            else
            {
                plan = builder.Finish(result);
            }
            leftOut?.AddRange(builder._leftOut);
            return plan;
        }

        /// <summary>
        /// The group described by the marks under a marked root, fails when they do not describe a valid group.
        /// </summary>
        public static MultiJoinPlan Build(MergeJoinRelation markedRoot)
        {
            if (markedRoot.MultiJoinGroup == null)
            {
                throw new InvalidOperationException("The merge join is not marked as part of a multi join.");
            }
            var builder = new MultiJoinPlanBuilder(markedRoot.MultiJoinGroup, int.MaxValue);
            var result = builder.Flatten(markedRoot);
            if (builder._plan.Inputs.Count < MinInputs)
            {
                throw new InvalidOperationException($"Multi join group {markedRoot.MultiJoinGroup} has fewer than three inputs.");
            }
            return builder.Finish(result);
        }

        private readonly record struct Flattened(MultiJoinNode Node, List<MultiJoinColumn> Output);

        // Marks of a group must be connected, one below an input would silently run as an ordinary join
        private sealed class MarkFinder : FlowtideDotNet.Core.Optimizer.OptimizerBaseVisitor
        {
            private readonly int _groupId;
            private bool _found;

            private MarkFinder(int groupId)
            {
                _groupId = groupId;
            }

            public static bool HasMark(Relation relation, int groupId)
            {
                var finder = new MarkFinder(groupId);
                finder.Visit(relation, null!);
                return finder._found;
            }

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                if (mergeJoinRelation.MultiJoinGroup == _groupId)
                {
                    _found = true;
                }
                return base.VisitMergeJoinRelation(mergeJoinRelation, state);
            }
        }

        // What a join's own conditions say before its inputs are flattened
        private sealed class Conditions
        {
            public List<Expression> RightFilters = new List<Expression>();
            public List<Expression> LeftFilters = new List<Expression>();
            public List<Expression> KeptOnly = new List<Expression>();
            public List<Expression> Checks = new List<Expression>();
        }

        private Flattened AddLeaf(Relation relation)
        {
            if (_groupId != null && MarkFinder.HasMark(relation, _groupId.Value))
            {
                throw new InvalidOperationException($"Multi join group {_groupId} cannot be built: a join of the group lies below one of its inputs.");
            }
            var index = _plan.Inputs.Count;
            var leaf = new MultiJoinNode() { Leaf = index };
            leaf.Leaves.Add(index);
            _plan.Inputs.Add(new MultiJoinInput() { Relation = relation, Node = leaf });
            var output = new List<MultiJoinColumn>(relation.OutputLength);
            for (int i = 0; i < relation.OutputLength; i++)
            {
                output.Add(new MultiJoinColumn(index, i));
            }
            return new Flattened(leaf, output);
        }

        private Flattened LeaveOut(MergeJoinRelation join, string reason, List<Expression>? filters)
        {
            if (_groupId != null)
            {
                throw new InvalidOperationException($"Multi join group {_groupId} cannot be built: {reason}.");
            }
            _leftOut.Add(new MultiJoinLeftOut(join, reason));
            return AddLeaf(Filtered(join, filters));
        }

        private static Relation Filtered(Relation relation, List<Expression>? filters)
        {
            if (filters == null)
            {
                return relation;
            }
            return new FilterRelation() { Input = relation, Condition = And(filters.Select(x => x.Clone()).ToList()) };
        }

        // The filters read the output of the relation, they come from a join above that only this side of it can fail
        private Flattened Flatten(Relation relation, List<Expression>? filters = null)
        {
            if (relation is not MergeJoinRelation join)
            {
                return AddLeaf(Filtered(relation, filters));
            }
            if (_groupId != null)
            {
                if (join.MultiJoinGroup != _groupId)
                {
                    return AddLeaf(Filtered(relation, filters));
                }
            }
            else if (_joinBudget <= 0)
            {
                // Over the cap, the rest of the chain is an input and may form a group of its own
                return AddLeaf(Filtered(relation, filters));
            }
            List<Expression>? extra = null;
            if (filters != null)
            {
                if (join.Type != JoinType.Inner)
                {
                    // Only on an inner join is a filter on the result the same as one more condition
                    return AddLeaf(Filtered(relation, filters));
                }
                extra = new List<Expression>(filters.Count);
                foreach (var copy in filters.Select(x => x.Clone()))
                {
                    if (join.EmitSet)
                    {
                        var usage = new ExpressionFieldUsageVisitor(int.MaxValue);
                        usage.Visit(copy, null);
                        var oldToNew = new Dictionary<int, int>();
                        foreach (var field in usage.UsedFieldsLeft.Distinct())
                        {
                            oldToNew.Add(field, join.Emit[field]);
                        }
                        new ExpressionFieldReplaceVisitor(oldToNew).Visit(copy, null);
                    }
                    extra.Add(copy);
                }
            }

            var reason = ReadConditions(join, extra, out var conditions);
            if (reason != null)
            {
                return LeaveOut(join, reason, filters);
            }

            // Everything below is undone when the keys turn out not to fit
            int inputsBefore = _plan.Inputs.Count;
            int joinsBefore = _plan.Joins.Count;
            int leftOutBefore = _leftOut.Count;
            int budgetBefore = _joinBudget;
            int innerBefore = _plan.InnerJoinCount;
            int leftBefore = _plan.LeftJoinCount;
            int marksBefore = _plan.Marks.Count;

            _joinBudget--;
            // Reserve the position so the root comes first
            _plan.Joins.Add(join);

            var left = conditions.LeftFilters.Count > 0
                ? Flatten(join.Left, SideFilters(join, conditions.LeftFilters, false))
                : FlattenSide(join, false);
            var right = conditions.RightFilters.Count > 0
                ? Flatten(join.Right, SideFilters(join, conditions.RightFilters, true))
                : FlattenSide(join, true);

            var concat = new List<MultiJoinColumn>(left.Output.Count + right.Output.Count);
            concat.AddRange(left.Output);
            concat.AddRange(right.Output);

            var node = new MultiJoinNode()
            {
                Left = left.Node,
                Right = right.Node,
                PadRight = join.Type == JoinType.Left || join.Type == JoinType.Outer,
                PadLeft = join.Type == JoinType.Right || join.Type == JoinType.Outer,
                IsMark = join.Type == JoinType.LeftMark,
                MarkId = join.Type == JoinType.LeftMark ? _plan.Marks.Count : -1
            };
            if (node.IsMark)
            {
                _plan.Marks.Add(node);
            }
            reason = ResolveKeys(join, node, left.Output.Count, concat);
            if (reason == null && (ReadsMark(conditions.Checks, concat) || ReadsMark(conditions.KeptOnly, concat)))
            {
                reason = "condition on a mark";
            }
            if (reason != null)
            {
                _plan.Inputs.RemoveRange(inputsBefore, _plan.Inputs.Count - inputsBefore);
                _plan.Joins.RemoveRange(joinsBefore, _plan.Joins.Count - joinsBefore);
                _leftOut.RemoveRange(leftOutBefore, _leftOut.Count - leftOutBefore);
                _joinBudget = budgetBefore;
                _plan.InnerJoinCount = innerBefore;
                _plan.LeftJoinCount = leftBefore;
                _plan.Marks.RemoveRange(marksBefore, _plan.Marks.Count - marksBefore);
                return LeaveOut(join, reason, filters);
            }

            foreach (var check in conditions.Checks)
            {
                node.Checks.Add(new MultiJoinCheck() { Condition = check, Columns = concat });
            }
            if (conditions.KeptOnly.Count > 0)
            {
                node.KeptOnlyCheck = new MultiJoinCheck() { Condition = And(conditions.KeptOnly), Columns = concat };
            }

            left.Node.Parent = node;
            right.Node.Parent = node;
            node.Leaves.AddRange(left.Node.Leaves);
            node.Leaves.AddRange(right.Node.Leaves);
            if (node.KeepsUnmatched)
            {
                _plan.LeftJoinCount++;
            }
            else
            {
                _plan.InnerJoinCount++;
            }

            var output = concat;
            if (node.IsMark)
            {
                // The right side is not part of the output, the mark stands where it would begin
                output = new List<MultiJoinColumn>(left.Output.Count + 1);
                output.AddRange(left.Output);
                output.Add(new MultiJoinColumn(MultiJoinColumn.MarkInput, node.MarkId));
            }
            if (join.EmitSet)
            {
                var emitted = new List<MultiJoinColumn>(join.Emit.Count);
                foreach (var index in join.Emit)
                {
                    emitted.Add(output[index]);
                }
                return new Flattened(node, emitted);
            }
            return new Flattened(node, output);
        }

        // The optimizer leaves a not null filter on a key above an outer join, the key never matches those rows anyway
        private Flattened FlattenSide(MergeJoinRelation join, bool right)
        {
            var side = right ? join.Right : join.Left;
            if (side is not FilterRelation filter || filter.Input is not MergeJoinRelation below ||
                !OnlyRemovesNullKeys(join, filter, right))
            {
                return Flatten(side);
            }
            var result = Flatten(below);
            if (result.Node.IsLeaf && ReferenceEquals(_plan.Inputs[result.Node.Leaf].Relation, below))
            {
                // The join below is not part of the group, it keeps its filter
                int index = result.Node.Leaf;
                _plan.Inputs[index] = new MultiJoinInput() { Relation = filter, Node = result.Node };
                var columns = new List<MultiJoinColumn>(filter.OutputLength);
                for (int i = 0; i < filter.OutputLength; i++)
                {
                    columns.Add(new MultiJoinColumn(index, i));
                }
                return new Flattened(result.Node, columns);
            }
            if (!filter.EmitSet)
            {
                return result;
            }
            var emitted = new List<MultiJoinColumn>(filter.Emit!.Count);
            foreach (var column in filter.Emit)
            {
                emitted.Add(result.Output[column]);
            }
            return new Flattened(result.Node, emitted);
        }

        private static bool OnlyRemovesNullKeys(MergeJoinRelation join, FilterRelation filter, bool right)
        {
            // Unmatched rows of a kept side are part of the result, a filter on them is not implied
            bool sideIsKept = right
                ? join.Type != JoinType.Inner && join.Type != JoinType.Left
                : join.Type != JoinType.Inner && join.Type != JoinType.Right;
            if (sideIsKept)
            {
                return false;
            }
            int leftSize = join.Left.OutputLength;
            var conjuncts = new List<Expression>();
            SplitAnd(filter.Condition, conjuncts);
            foreach (var conjunct in conjuncts)
            {
                if (conjunct is not ScalarFunction function ||
                    function.ExtensionUrn != FunctionsComparison.Urn ||
                    function.ExtensionName != FunctionsComparison.IsNotNull ||
                    function.Arguments.Count != 1 ||
                    function.Arguments[0] is not FieldReference reference ||
                    !TryGetField(reference, out var field))
                {
                    return false;
                }
                bool isKey = false;
                for (int i = 0; i < join.LeftKeys.Count && !isKey; i++)
                {
                    if (right)
                    {
                        isKey = TryGetField(join.RightKeys[i], out var rightField) && BeforeEmit(filter, rightField - leftSize) == field;
                    }
                    else
                    {
                        isKey = TryGetField(join.LeftKeys[i], out var leftField) && BeforeEmit(filter, leftField) == field;
                    }
                }
                if (!isKey)
                {
                    return false;
                }
            }
            return true;
        }

        // The condition of a filter reads its input, a join above reads what the filter emits
        private static int BeforeEmit(FilterRelation filter, int field)
        {
            if (field < 0 || field >= filter.OutputLength)
            {
                return -1;
            }
            return filter.EmitSet ? filter.Emit![field] : field;
        }

        // A mark is made per batch at the end, a condition cannot read it before that
        private static bool ReadsMark(List<Expression> conditions, List<MultiJoinColumn> concat)
        {
            foreach (var condition in conditions)
            {
                var usage = new ExpressionFieldUsageVisitor(int.MaxValue);
                usage.Visit(condition, null);
                if (usage.UsedFieldsLeft.Any(x => concat[x].IsMark))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Sorts the conditions of a join by the side they read, returns a reason when the join cannot be part of a group.
        /// </summary>
        private static string? ReadConditions(MergeJoinRelation join, List<Expression>? extra, out Conditions conditions)
        {
            conditions = new Conditions();
            if (join.Type != JoinType.Inner && join.Type != JoinType.Left && join.Type != JoinType.Right &&
                join.Type != JoinType.Outer && join.Type != JoinType.LeftMark)
            {
                return $"join type {join.Type}";
            }
            if (join.Type == JoinType.LeftMark && join.EmitSet && join.Emit.Any(x => x > join.Left.OutputLength))
            {
                return "mark join output beyond the mark";
            }
            if (join.LeftKeys.Count == 0 || join.LeftKeys.Count != join.RightKeys.Count)
            {
                return "no usable keys";
            }
            for (int i = 0; i < join.LeftKeys.Count; i++)
            {
                if (join.GetComparisonType(i) != JoinComparisonType.Equal)
                {
                    return "range key";
                }
                if (!TryGetField(join.LeftKeys[i], out _) || !TryGetField(join.RightKeys[i], out _))
                {
                    return "nested or computed key";
                }
            }
            var conjuncts = new List<Expression>();
            if (join.PostJoinFilter != null)
            {
                SplitAnd(join.PostJoinFilter, conjuncts);
            }
            if (extra != null)
            {
                conjuncts.AddRange(extra);
            }

            bool isLeftJoin = join.Type == JoinType.Left;
            bool keepsOneSide = isLeftJoin || join.Type == JoinType.Right;
            // A filter on an input is only the same as the condition when the input's unmatched rows are not kept
            bool rightKept = join.Type == JoinType.Right || join.Type == JoinType.Outer;
            bool leftKept = join.Type != JoinType.Inner && join.Type != JoinType.Right;
            foreach (var conjunct in conjuncts)
            {
                var usage = new ExpressionFieldUsageVisitor(join.Left.OutputLength);
                usage.Visit(conjunct, null);
                if (!usage.CanOptimize)
                {
                    return "condition with a nested reference";
                }
                bool readsLeft = usage.UsedFieldsLeft.Count > 0;
                bool readsRight = usage.UsedFieldsRight.Count > 0;
                if (readsRight && !readsLeft && !rightKept)
                {
                    // Removes right rows before they can match, the same as filtering the right input
                    conditions.RightFilters.Add(conjunct);
                }
                else if (readsLeft && !readsRight && !leftKept)
                {
                    conditions.LeftFilters.Add(conjunct);
                }
                else if (join.Type != JoinType.Inner && !keepsOneSide)
                {
                    return $"condition on a {join.Type} join";
                }
                else if (!keepsOneSide)
                {
                    conditions.Checks.Add(conjunct);
                }
                else if (isLeftJoin ? readsLeft && !readsRight : readsRight && !readsLeft)
                {
                    conditions.KeptOnly.Add(conjunct);
                }
                else if (readsLeft || readsRight)
                {
                    return isLeftJoin ? "condition on both sides of a left join" : "condition on both sides of a right join";
                }
                else
                {
                    return isLeftJoin ? "constant condition on a left join" : "constant condition on a right join";
                }
            }
            return null;
        }

        /// <summary>
        /// Picks the key of a join, one input on each side.
        /// </summary>
        private static string? ResolveKeys(MergeJoinRelation join, MultiJoinNode node, int leftSize, List<MultiJoinColumn> concat)
        {
            var pairs = new List<(int leftField, int rightField)>();
            for (int i = 0; i < join.LeftKeys.Count; i++)
            {
                TryGetField(join.LeftKeys[i], out var leftField);
                TryGetField(join.RightKeys[i], out var rightField);
                if (leftField >= leftSize || rightField < leftSize || rightField >= concat.Count)
                {
                    return "key outside its side";
                }
                if (concat[leftField].IsMark || concat[rightField].IsMark)
                {
                    return "key on a mark";
                }
                // The same condition written twice, compared by column since an emit can repeat a column
                var resolved = (concat[leftField], concat[rightField]);
                if (!pairs.Any(p => (concat[p.leftField], concat[p.rightField]) == resolved))
                {
                    pairs.Add((leftField, rightField));
                }
            }

            // Every pair has to be between the same two inputs, the first one names them
            (int left, int right) best = (concat[pairs[0].leftField].Input, concat[pairs[0].rightField].Input);

            var leftColumns = new List<int>();
            var rightColumns = new List<int>();
            foreach (var pair in pairs)
            {
                var l = concat[pair.leftField];
                var r = concat[pair.rightField];
                bool fits = (l.Input, r.Input) == best && !leftColumns.Contains(l.Column) && !rightColumns.Contains(r.Column);
                if (fits)
                {
                    leftColumns.Add(l.Column);
                    rightColumns.Add(r.Column);
                    continue;
                }
                if ((l.Input, r.Input) != best)
                {
                    // One lookup would find the rows of one input only, the join stays an ordinary one
                    return "key from several inputs";
                }
                if (node.KeepsUnmatched)
                {
                    return "column compared with two columns on a join that keeps unmatched rows";
                }
                node.Checks.Add(new MultiJoinCheck()
                {
                    Condition = FieldsEqual(pair.leftField, pair.rightField),
                    Columns = concat
                });
            }
            node.LeftKey = new MultiJoinKeySide() { Input = best.left, Columns = leftColumns.ToArray() };
            node.RightKey = new MultiJoinKeySide() { Input = best.right, Columns = rightColumns.ToArray() };
            return null;
        }

        private MultiJoinPlan Finish(Flattened root)
        {
            _plan.Root = root.Node;
            _plan.Output = root.Output;

            CollectPadTables(root.Node);
            // The programs tell which column sets each input is looked up by
            var collected = MultiJoinProgram.Collect(_plan);
            var lookedUpBy = collected.Keys;
            var sets = new List<int[]>[_plan.Inputs.Count];
            for (int i = 0; i < sets.Length; i++)
            {
                sets[i] = new List<int[]>();
            }
            for (int i = 0; i < sets.Length; i++)
            {
                sets[i].AddRange(lookedUpBy[i]);
                foreach (var columns in InIndexOrder(sets[i]))
                {
                    GetOrAddKeyList(_plan.Inputs[i], columns);
                }
            }

            for (int i = 0; i < _plan.Inputs.Count; i++)
            {
                // Only what some program reads from the rows it finds is copied out of a tree
                var read = collected.Read[i].ToList();
                read.Sort();
                _plan.Inputs[i].NeededColumns.AddRange(read);
            }
            return _plan;
        }

        // One table per join and side whose unmatched rows are kept
        private void CollectPadTables(MultiJoinNode node)
        {
            if (node.IsLeaf)
            {
                return;
            }
            CollectPadTables(node.Left!);
            CollectPadTables(node.Right!);
            if (node.PadRight || node.IsMark)
            {
                _plan.PadTableIds.Add((node, true), _plan.PadTables.Count);
                _plan.PadTables.Add((node, true));
            }
            if (node.PadLeft)
            {
                _plan.PadTableIds.Add((node, false), _plan.PadTables.Count);
                _plan.PadTables.Add((node, false));
            }
        }

        // The fewest runs of nested sets that hold every set: each run is one tree, the longest the primary one
        private static List<int[]> InIndexOrder(List<int[]> sets)
        {
            var ordered = new List<int[]>();
            foreach (var columns in sets.Select(x => x.OrderBy(c => c).ToArray()))
            {
                if (!ordered.Exists(x => x.SequenceEqual(columns)))
                {
                    ordered.Add(columns);
                }
            }
            ordered.Sort((a, b) =>
            {
                if (a.Length != b.Length)
                {
                    return a.Length.CompareTo(b.Length);
                }
                for (int i = 0; i < a.Length; i++)
                {
                    if (a[i] != b[i])
                    {
                        return a[i].CompareTo(b[i]);
                    }
                }
                return 0;
            });
            // A set is followed in its run by one set that contains it: the most such pairs give the fewest runs
            var next = new int[ordered.Count];
            var previous = new int[ordered.Count];
            Array.Fill(next, -1);
            Array.Fill(previous, -1);
            for (int i = 0; i < ordered.Count; i++)
            {
                FindNext(ordered, i, next, previous, new bool[ordered.Count]);
            }
            var runs = new List<List<int[]>>();
            for (int i = 0; i < ordered.Count; i++)
            {
                if (previous[i] >= 0)
                {
                    continue;
                }
                var run = new List<int[]>();
                for (int at = i; at >= 0; at = next[at])
                {
                    run.Add(ordered[at]);
                }
                runs.Add(run);
            }
            // The longest run first, among equals the one met first in the order above
            var result = new List<int[]>();
            foreach (var run in runs.OrderByDescending(x => x.Count))
            {
                result.AddRange(run);
            }
            return result;
        }

        private static bool FindNext(List<int[]> sets, int from, int[] next, int[] previous, bool[] seen)
        {
            for (int to = 0; to < sets.Count; to++)
            {
                if (seen[to] || sets[to].Length <= sets[from].Length || !sets[from].All(sets[to].Contains))
                {
                    continue;
                }
                seen[to] = true;
                if (previous[to] < 0 || FindNext(sets, previous[to], next, previous, seen))
                {
                    next[from] = to;
                    previous[to] = from;
                    return true;
                }
            }
            return false;
        }

        private static int GetOrAddKeyList(MultiJoinInput input, int[] columns)
        {
            int existing = input.FindKeyList(columns);
            if (existing >= 0)
            {
                return existing;
            }
            var primary = input.PrimaryOrder;
            int shared = Math.Min(primary.Count, columns.Length);
            bool leads = true;
            for (int i = 0; i < shared; i++)
            {
                leads &= columns.Contains(primary[i]);
            }
            if (leads)
            {
                // The columns lead the primary tree, or do once it is ordered by the rest of them too
                foreach (var column in columns)
                {
                    if (!primary.Contains(column))
                    {
                        primary.Add(column);
                    }
                }
                input.KeyLists.Add(primary.GetRange(0, columns.Length).ToArray());
                input.SecondaryOf.Add(-1);
                input.PrefixLengths.Add(columns.Length);
                return input.KeyLists.Count - 1;
            }
            // A secondary index whose key columns these extend serves them too
            int secondary = -1;
            for (int s = 0; s < input.SecondaryLayouts.Count && secondary < 0; s++)
            {
                if (input.SecondaryLayouts[s].Take(input.SecondaryKeyLengths[s]).All(columns.Contains))
                {
                    secondary = s;
                }
            }
            var layout = new List<int>();
            if (secondary >= 0)
            {
                layout.AddRange(input.SecondaryLayouts[secondary].Take(input.SecondaryKeyLengths[secondary]));
            }
            foreach (var column in columns)
            {
                if (!layout.Contains(column))
                {
                    layout.Add(column);
                }
            }
            int keyLength = layout.Count;
            foreach (var column in columns)
            {
                if (!primary.Contains(column))
                {
                    primary.Add(column);
                }
            }
            // Then the primary columns its key lists lead to, the last key list added has the widest prefix
            foreach (var column in primary)
            {
                if (!layout.Contains(column))
                {
                    layout.Add(column);
                }
            }
            if (secondary < 0)
            {
                secondary = input.SecondaryLayouts.Count;
                input.SecondaryLayouts.Add(layout.ToArray());
                input.SecondaryKeyLengths.Add(keyLength);
            }
            else
            {
                input.SecondaryLayouts[secondary] = layout.ToArray();
                input.SecondaryKeyLengths[secondary] = keyLength;
                for (int j = 0; j < input.SecondaryOf.Count; j++)
                {
                    if (input.SecondaryOf[j] == secondary)
                    {
                        // An entry now stands for one value of the wider prefix, a shorter one would find its rows once per entry
                        input.PrefixLengths[j] = primary.Count;
                    }
                }
            }
            input.SecondaryOf.Add(secondary);
            input.KeyLists.Add(layout.GetRange(0, columns.Length).ToArray());
            input.PrefixLengths.Add(primary.Count);
            return input.KeyLists.Count - 1;
        }

        /// <summary>
        /// The conditions that read only one side of a join, as filters on the output of that side. They are copied, the plan is not changed.
        /// </summary>
        private static List<Expression> SideFilters(MergeJoinRelation join, List<Expression> conditions, bool right)
        {
            if (!right)
            {
                return conditions.Select(x => x.Clone()).ToList();
            }
            int leftSize = join.Left.OutputLength;
            var copies = new List<Expression>(conditions.Count);
            foreach (var copy in conditions.Select(x => x.Clone()))
            {
                var usage = new ExpressionFieldUsageVisitor(leftSize);
                usage.Visit(copy, null);
                var oldToNew = new Dictionary<int, int>();
                foreach (var field in usage.UsedFieldsRight.Distinct())
                {
                    oldToNew.Add(field, field - leftSize);
                }
                new ExpressionFieldReplaceVisitor(oldToNew).Visit(copy, null);
                copies.Add(copy);
            }
            return copies;
        }

        private static void SplitAnd(Expression expression, List<Expression> conjuncts)
        {
            if (expression is ScalarFunction function &&
                function.ExtensionUrn == FunctionsBoolean.Urn &&
                function.ExtensionName == FunctionsBoolean.And)
            {
                foreach (var argument in function.Arguments)
                {
                    SplitAnd(argument, conjuncts);
                }
                return;
            }
            conjuncts.Add(expression);
        }

        private static Expression And(List<Expression> conditions)
        {
            if (conditions.Count == 1)
            {
                return conditions[0];
            }
            return new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.And,
                Arguments = new List<Expression>(conditions)
            };
        }

        private static Expression FieldsEqual(int leftField, int rightField)
        {
            return new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = FunctionsComparison.Equal,
                Arguments = new List<Expression>()
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = leftField } },
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = rightField } }
                }
            };
        }

        private static bool TryGetField(FieldReference reference, out int field)
        {
            if (reference is DirectFieldReference direct &&
                direct.ReferenceSegment is StructReferenceSegment segment &&
                segment.Child == null)
            {
                field = segment.Field;
                return true;
            }
            field = -1;
            return false;
        }
    }
}
