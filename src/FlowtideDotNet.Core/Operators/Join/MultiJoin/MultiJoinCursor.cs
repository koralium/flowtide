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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// One lookup of a program: an input found by one of its key lists, the key taken from rows joined before it.
    /// </summary>
    internal readonly record struct MultiJoinStep
    {
        public required int Input { get; init; }
        public required int KeyList { get; init; }

        // The frame the key is read from, -1 when the program has no rows for the key
        public required int KeyFrame { get; init; }

        // Per key column its position in the columns of that frame
        public required int[] KeyPositions { get; init; }

        // With TableJoin: per key column of the join, where it stands among the key columns of the step
        public int[]? TableOrder { get; init; }

        // Set on the step entering the right side of a left join from its left side.
        public int PadLast { get; init; }

        // With PadLast, entering the right side of a mark join: every row goes on once, marked when it has a match
        public bool Mark { get; init; }

        // With PadLast: the rows found are only counted, with what the change adds. A row goes on padded when that turns it
        public bool Turn { get; init; }

        // Set on the step entering a side whose unmatched rows are kept, coming from the other side with a change.
        public int TableJoin { get; init; }

        // With TableJoin: the last step of the side entered, a key is remembered once a row of the whole side is found for it
        public int TableEnd { get; init; }

        // Comparisons of the join between a column the program already has and a column of the input, checked before a row is copied
        public MultiJoinStepCompare[]? Compares { get; init; }

        // With TableJoin, when the pad table looks only at the kept rows a change joined: where the columns it keeps of them stand in the frame of the step
        public int[]? AffectedPositions { get; init; }

        // The last key column is a bound: the rows whose column stands so to the key value are found, the columns before it are equal
        public JoinComparisonType? Range { get; init; }

        // A null in the key finds the rows that hold null there
        public bool MatchNulls { get; init; }
    }

    /// <summary>
    /// The value at a position of a frame, compared with a column of the tree a step reads.
    /// </summary>
    internal readonly record struct MultiJoinStepCompare(int Frame, int Position, MultiJoinCompareOp Op, int Column);

    /// <summary>
    /// A condition of a join as one program reads it. The scratch is its own, a program serves one cursor.
    /// </summary>
    internal sealed class MultiJoinStepCheck
    {
        public required Func<EventBatchData, int, bool> Condition { get; set; }

        // The condition of the plan this was made from
        public MultiJoinCheck? Source { get; init; }

        // Per column the condition reads: its frame, -1 when the program has no rows for it, and its position there
        public required int[] Frames { get; init; }
        public required int[] Positions { get; init; }

        // The frames read through row indexes, highest first
        public required int[] EarlierFrames { get; init; }

        public IColumn[] Columns = Array.Empty<IColumn>();
        public EventBatchData Batch = null!;

        // Per column read through a view in the start rows, the row indexes composed with the view's
        public PrimitiveList<int>?[] OwnOffsets = Array.Empty<PrimitiveList<int>?>();
    }

    /// <summary>
    /// What happens to the rows of a frame when the side of a join ends there.
    /// </summary>
    internal readonly record struct MultiJoinSettle(int Entry, MultiJoinStepCheck? Check);

    /// <summary>
    /// The lookups a set of rows goes through, in order. Step t fills frame t + 1, frame 0 is what it starts from.
    /// </summary>
    internal sealed class MultiJoinProgram
    {
        public required MultiJoinStep[] Steps { get; init; }

        // Per frame the output columns it gives
        public required int[][] FrameOutputs { get; init; }

        // Per output column its position in the columns of its frame
        public required int[] OutputPositions { get; init; }

        // Output columns of inputs the program has no rows for
        public required int[] NullOutputs { get; init; }

        // Per step what the joins whose side ends there do with its rows, innermost join first.
        public required MultiJoinSettle[][] SettlesEndingAt { get; init; }

        // The pad tables this program feeds, lowest join first
        public required int[] TableJoins { get; init; }

        // Per step the steps with a pad table whose side ends there
        public required int[][] TableEntriesEndingAt { get; init; }

        // Only the match totals of the rows it starts from are wanted
        public bool TotalsOnly { get; init; }

        // No rows come out: only totals are wanted, or the program ends on the right side of a mark join
        public bool NoOutput { get; init; }

        // Output columns that are a mark, with the frame whose rows carry it
        public required (int Output, int Frame)[] MarkOutputs { get; init; }

        // Per step the first mark step whose right side ends there, int.MaxValue without one
        public required int[] FirstMarkEndingAt { get; init; }

        // Per step whether anything reads the columns of the rows it finds
        public required bool[] CopyColumns { get; init; }

        private sealed class Builder
        {
            public required MultiJoinPlan Plan { get; init; }

            // Set while the plan is made: per input the column sets it is looked up by, no program is finished
            public List<int[]>[]? Collected { get; init; }

            // Set while a plan is made: per input the columns a program reads from its copied rows
            public HashSet<int>[]? Read { get; init; }

            // Set while a plan is made: per input the equal key columns with a column a lookup would take a range on
            public List<(int[] Equal, int Column)>[]? Ranges { get; init; }

            private readonly record struct KeySource(int Column, int Frame, int Position);

            // The input frame 0 holds, -1 when frame 0 is a set of keys
            public required int StartInput { get; init; }

            public List<MultiJoinStep> Steps { get; } = new List<MultiJoinStep>();
            public List<int> TableJoins { get; } = new List<int>();
            public int[] FrameOfInput = Array.Empty<int>();
            public int[] FrameOfMark = Array.Empty<int>();

            // The climb ended on the right side of a mark join, whose rows are not part of any output
            public bool Stopped;

            // The rows carried up cancel out per key, no pad table above is told about them
            public bool TotalsUnchanged;

            // The join whose padded rows the program gives one by one
            public MultiJoinNode? TurnJoin;

            // Frames read by comparisons that are checked before a row is copied
            public List<int> CompareFrames { get; } = new List<int>();

            // The comparisons of a join that have the input on one side and a column the program already has on the other
            private MultiJoinStepCompare[]? ComparesFor(MultiJoinNode join, int input)
            {
                List<MultiJoinStepCompare>? result = null;
                foreach (var check in join.Checks)
                {
                    var compare = check.Compare;
                    if (compare == null)
                    {
                        continue;
                    }
                    var (left, op, right) = compare.Value;
                    if (op == MultiJoinCompareOp.NotEqual)
                    {
                        // Nearly every row passes it, checking it twice only costs
                        continue;
                    }
                    if (left.Input == input && right.Input != input)
                    {
                        // The column of the input goes on the right
                        (left, right) = (right, left);
                        op = op switch
                        {
                            MultiJoinCompareOp.LessThan => MultiJoinCompareOp.GreaterThan,
                            MultiJoinCompareOp.LessThanOrEqual => MultiJoinCompareOp.GreaterThanOrEqual,
                            MultiJoinCompareOp.GreaterThan => MultiJoinCompareOp.LessThan,
                            MultiJoinCompareOp.GreaterThanOrEqual => MultiJoinCompareOp.LessThanOrEqual,
                            _ => op
                        };
                    }
                    else if (right.Input != input || left.Input == input)
                    {
                        continue;
                    }
                    int frame = FrameOfInput[left.Input];
                    if (frame < 0)
                    {
                        continue;
                    }
                    CompareFrames.Add(frame);
                    (result ??= new List<MultiJoinStepCompare>()).Add(new MultiJoinStepCompare(frame, Position(left.Input, left.Column), op, right.Column));
                }
                return result?.ToArray();
            }

            // Conditions with the step their join is entered at and the step its side ends at
            public List<(int End, int Entry, MultiJoinCheck Check)> Checks { get; } = new();

            public void AddChecks(MultiJoinNode join, int entry)
            {
                int end = Steps.Count - 1;
                foreach (var check in join.Checks)
                {
                    Checks.Add((end, entry, check));
                }
                if (join.KeptOnlyCheck != null)
                {
                    Checks.Add((end, entry, join.KeptOnlyCheck));
                }
            }

            private int AddStep(int input, List<KeySource> sources, int tableJoin, int[]? joinColumns, (int Column, int Position, JoinComparisonType Type)? range = null)
            {
                int keyList = 0;
                int equalKeyList = 0;
                JoinComparisonType? rangeType = null;
                if (sources.Count > 0)
                {
                    var columns = sources.Select(x => x.Column).ToArray();
                    if (Collected != null)
                    {
                        if (!Collected[input].Exists(x => x.Length == columns.Length && columns.All(x.Contains)))
                        {
                            Collected[input].Add(columns);
                        }
                        if (range != null && !Ranges![input].Exists(x => x.Column == range.Value.Column && x.Equal.Length == columns.Length && columns.All(x.Equal.Contains)))
                        {
                            // Wanted, not needed: the planner takes it only where it costs no index
                            Ranges[input].Add((columns, range.Value.Column));
                        }
                    }
                    else
                    {
                        keyList = -1;
                        if (range != null)
                        {
                            var planInput = Plan.Inputs[input];
                            int candidate = planInput.FindKeyList(columns.Append(range.Value.Column).ToArray());
                            // Only straight in the tree of the input, with the bound as its last key column
                            equalKeyList = planInput.FindKeyList(columns);
                            if (candidate >= 0 && equalKeyList >= 0 && planInput.SecondaryOf[candidate] < 0 && planInput.SecondaryOf[equalKeyList] < 0 &&
                                planInput.KeyLists[candidate][^1] == range.Value.Column)
                            {
                                keyList = candidate;
                                rangeType = range.Value.Type;
                                sources.Add(new KeySource(range.Value.Column, sources[0].Frame, range.Value.Position));
                            }
                        }
                        if (keyList < 0)
                        {
                            keyList = Plan.Inputs[input].FindKeyList(columns);
                        }
                        if (keyList < 0)
                        {
                            throw new InvalidOperationException("A lookup of the multi join has no index for its key.");
                        }
                        // The key columns stand in the order of the index
                        var order = Plan.Inputs[input].KeyLists[keyList];
                        sources.Sort((a, b) => Array.IndexOf(order, a.Column).CompareTo(Array.IndexOf(order, b.Column)));
                    }
                }
                int[]? tableOrder = null;
                if (tableJoin >= 0 && sources.Count > 0)
                {
                    tableOrder = new int[joinColumns!.Length];
                    for (int i = 0; i < tableOrder.Length; i++)
                    {
                        tableOrder[i] = sources.FindIndex(x => x.Column == joinColumns[i]);
                    }
                }
                Steps.Add(new MultiJoinStep()
                {
                    Input = input,
                    KeyList = keyList,
                    KeyFrame = sources.Count > 0 ? sources[0].Frame : -1,
                    KeyPositions = sources.Select(x => x.Position).ToArray(),
                    TableOrder = tableOrder,
                    PadLast = -1,
                    TableEnd = Steps.Count,
                    TableJoin = tableJoin,
                    Range = rangeType
                });
                return Steps.Count - 1;
            }

            // The start input is read as it arrived, the others from the columns copied out of their tree
            private int Position(int input, int column)
            {
                if (input == StartInput)
                {
                    return column;
                }
                if (Read != null)
                {
                    Read[input].Add(column);
                    return 0;
                }
                return Plan.Inputs[input].NeededColumns.IndexOf(column);
            }

            public void AddEntry(MultiJoinKeySide entry, bool matchNulls = false)
            {
                var sources = new List<KeySource>();
                for (int i = 0; i < entry.Columns.Length; i++)
                {
                    sources.Add(new KeySource(entry.Columns[i], 0, i));
                }
                int index = AddStep(entry.Input, sources, -1, null);
                if (matchNulls)
                {
                    Steps[index] = Steps[index] with { MatchNulls = true };
                }
                FrameOfInput[entry.Input] = Steps.Count;
            }

            public void Climb(MultiJoinNode from, MultiJoinNode top, bool carriesChange)
            {
                var node = from;
                while (node != top)
                {
                    var parent = node.Parent!;
                    bool fromLeft = node == parent.Left;
                    var ownKey = fromLeft ? parent.LeftKey! : parent.RightKey!;
                    var otherKey = fromLeft ? parent.RightKey! : parent.LeftKey!;
                    var otherSide = fromLeft ? parent.Right! : parent.Left!;

                    bool otherKept = fromLeft ? parent.PadLeft : parent.PadRight || parent.IsMark;
                    bool ownKept = fromLeft ? parent.PadRight || parent.IsMark : parent.PadLeft;
                    int keyFrame = FrameOfInput[ownKey.Input];
                    var sources = new List<KeySource>();
                    for (int i = 0; keyFrame >= 0 && i < ownKey.Columns.Length; i++)
                    {
                        sources.Add(new KeySource(otherKey.Columns[i], keyFrame, Position(ownKey.Input, ownKey.Columns[i])));
                    }
                    int tableJoin = -1;
                    if (otherKept && carriesChange && !TotalsUnchanged)
                    {
                        tableJoin = Plan.PadTableIds[(parent, !fromLeft)];
                        TableJoins.Add(tableJoin);
                    }
                    // Only a group with a condition on both sides of a kept side, or a range key, compares before the copy
                    var compares = Plan.PairConditions ? ComparesFor(parent, otherKey.Input) : null;
                    (int Column, int Position, JoinComparisonType Type)? range = null;
                    int rangeAt = -1;
                    for (int c = 0; Plan.RangeKeys && compares != null && keyFrame >= 0 && c < compares.Length && range == null; c++)
                    {
                        // A bound on a column of the input, read from the frame the key is read from
                        var compare = compares[c];
                        if (compare.Frame != keyFrame || otherKey.Columns.Contains(compare.Column) ||
                            compare.Op == MultiJoinCompareOp.Equal || compare.Op == MultiJoinCompareOp.NotEqual)
                        {
                            continue;
                        }
                        // The comparison is value OP column, the type is column OP value
                        var type = compare.Op switch
                        {
                            MultiJoinCompareOp.LessThan => JoinComparisonType.GreaterThan,
                            MultiJoinCompareOp.LessThanOrEqual => JoinComparisonType.GreaterThanOrEqual,
                            MultiJoinCompareOp.GreaterThan => JoinComparisonType.LessThan,
                            _ => JoinComparisonType.LessThanOrEqual
                        };
                        range = (compare.Column, compare.Position, type);
                        rangeAt = c;
                    }
                    int index = AddStep(otherKey.Input, sources, tableJoin, otherKey.Columns, range);
                    if (Steps[index].Range != null)
                    {
                        // The bound does what the comparison before the copy did
                        compares = compares!.Where((_, i) => i != rangeAt).ToArray();
                        if (compares.Length == 0)
                        {
                            compares = null;
                        }
                    }
                    if (compares != null)
                    {
                        Steps[index] = Steps[index] with { Compares = compares };
                    }
                    if (tableJoin >= 0 && parent.PadPerRow)
                    {
                        var start = MultiJoinPadTable.StartColumnsOf(parent, !fromLeft);
                        if (start.Fast)
                        {
                            // The frame of the step is copied for them
                            CompareFrames.Add(Steps.Count);
                            Steps[index] = Steps[index] with { AffectedPositions = start.Columns.Select(x => Position(otherKey.Input, x)).ToArray() };
                        }
                    }
                    FrameOfInput[otherKey.Input] = Steps.Count;
                    if (!fromLeft && parent.IsMark)
                    {
                        if (!carriesChange)
                        {
                            throw new InvalidOperationException("The right side of a mark join cannot hold a key of a join above it.");
                        }
                        Stopped = true;
                        return;
                    }
                    // The other side is read as it is stored, from the input its key is on
                    Climb(Plan.Inputs[otherKey.Input].Node, otherSide, false);
                    AddChecks(parent, index);
                    if (tableJoin >= 0)
                    {
                        Steps[index] = Steps[index] with { TableEnd = Steps.Count - 1 };
                    }
                    if (ownKept)
                    {
                        Steps[index] = Steps[index] with { PadLast = Steps.Count - 1, Mark = parent.IsMark, Turn = parent == TurnJoin };
                        if (parent.IsMark)
                        {
                            FrameOfMark[parent.MarkId] = index + 1;
                        }
                    }
                    node = parent;
                }
            }

            private MultiJoinStepCheck CreateCheck(MultiJoinCheck check, int ownFrame, bool[] readFrames)
            {
                var reads = check.Reads;
                var frames = new int[reads.Length];
                var positions = new int[reads.Length];
                var earlier = new List<int>();
                for (int i = 0; i < reads.Length; i++)
                {
                    frames[i] = FrameOfInput[reads[i].Input];
                    if (frames[i] < 0)
                    {
                        continue;
                    }
                    positions[i] = Position(reads[i].Input, reads[i].Column);
                    readFrames[frames[i]] = true;
                    if (frames[i] != ownFrame && !earlier.Contains(frames[i]))
                    {
                        earlier.Add(frames[i]);
                    }
                }
                earlier.Sort((a, b) => b.CompareTo(a));
                var columns = new IColumn[reads.Length];
                return new MultiJoinStepCheck()
                {
                    // Conditions are compiled after the plan is made
                    Condition = check.Compiled ?? (Read != null ? null! : throw new InvalidOperationException("A condition of the multi join was not compiled.")),
                    Source = check,
                    Frames = frames,
                    Positions = positions,
                    EarlierFrames = earlier.ToArray(),
                    Columns = columns,
                    // The rows are addressed through one batch object, its columns are swapped per part
                    Batch = new EventBatchData(columns),
                    OwnOffsets = new PrimitiveList<int>?[reads.Length]
                };
            }

            public MultiJoinProgram Finish(bool totalsOnly)
            {
                bool noOutput = totalsOnly || Stopped;
                var markOutputs = new List<(int, int)>();
                var frameOutputs = new List<int>[Steps.Count + 1];
                for (int f = 0; f < frameOutputs.Length; f++)
                {
                    frameOutputs[f] = new List<int>();
                }
                var nullOutputs = new List<int>();
                var outputPositions = new int[Plan.Output.Count];
                for (int i = 0; i < outputPositions.Length; i++)
                {
                    var column = Plan.Output[i];
                    if (column.IsMark && FrameOfMark[column.Column] >= 0)
                    {
                        markOutputs.Add((i, FrameOfMark[column.Column]));
                        continue;
                    }
                    int frame = column.IsMark ? -1 : FrameOfInput[column.Input];
                    if (frame < 0)
                    {
                        nullOutputs.Add(i);
                        continue;
                    }
                    frameOutputs[frame].Add(i);
                    outputPositions[i] = Position(column.Input, column.Column);
                }
                var settles = new List<MultiJoinSettle>[Steps.Count];
                for (int s = 0; s < settles.Length; s++)
                {
                    settles[s] = new List<MultiJoinSettle>();
                }
                var readFrames = new bool[Steps.Count + 1];
                foreach (int frame in CompareFrames)
                {
                    readFrames[frame] = true;
                }
                // A join's own condition comes before its match count: a row failing it is no match
                for (int s = Steps.Count - 1; s >= 0; s--)
                {
                    foreach (var (end, entry, check) in Checks)
                    {
                        if (entry == s)
                        {
                            settles[end].Add(new MultiJoinSettle(s, CreateCheck(check, end + 1, readFrames)));
                        }
                    }
                    if (Steps[s].PadLast >= 0)
                    {
                        settles[Steps[s].PadLast].Add(new MultiJoinSettle(s, null));
                    }
                }
                var copyColumns = new bool[Steps.Count];
                var firstMark = new int[Steps.Count];
                Array.Fill(firstMark, int.MaxValue);
                for (int s = 0; s < copyColumns.Length; s++)
                {
                    // Only rows that are sent on, tested by a condition or the key of a later step are copied
                    copyColumns[s] = (!noOutput && frameOutputs[s + 1].Count > 0) || readFrames[s + 1] || Steps.Exists(x => x.KeyFrame == s + 1);
                    if (Steps[s].Mark || Steps[s].Turn)
                    {
                        firstMark[Steps[s].PadLast] = Math.Min(firstMark[Steps[s].PadLast], s);
                    }
                }
                return new MultiJoinProgram()
                {
                    NoOutput = noOutput,
                    MarkOutputs = markOutputs.ToArray(),
                    FirstMarkEndingAt = firstMark,
                    CopyColumns = copyColumns,
                    Steps = Steps.ToArray(),
                    FrameOutputs = frameOutputs.Select(x => x.ToArray()).ToArray(),
                    OutputPositions = outputPositions,
                    NullOutputs = nullOutputs.ToArray(),
                    SettlesEndingAt = settles.Select(x => x.ToArray()).ToArray(),
                    TableJoins = TableJoins.ToArray(),
                    TableEntriesEndingAt = Enumerable.Range(0, Steps.Count)
                        .Select(end => Enumerable.Range(0, Steps.Count).Where(s => Steps[s].TableJoin >= 0 && Steps[s].TableEnd == end).ToArray())
                        .ToArray(),
                    TotalsOnly = totalsOnly
                };
            }
        }

        private static Builder NewBuilder(MultiJoinPlan plan, int startInput, MultiJoinCollected? collected)
        {
            var builder = new Builder() { Plan = plan, StartInput = startInput, Collected = collected?.Keys, Read = collected?.Read, Ranges = collected?.Ranges };
            builder.FrameOfInput = new int[plan.Inputs.Count];
            Array.Fill(builder.FrameOfInput, -1);
            builder.FrameOfMark = new int[plan.Marks.Count];
            Array.Fill(builder.FrameOfMark, -1);
            return builder;
        }

        /// <summary>
        /// A change on one input carried to the output of the group.
        /// </summary>
        public static MultiJoinProgram ForChange(MultiJoinPlan plan, int input)
        {
            return BuildChange(plan, input, null).Finish(false);
        }

        private static Builder BuildChange(MultiJoinPlan plan, int input, MultiJoinCollected? collected)
        {
            var builder = NewBuilder(plan, input, collected);
            builder.FrameOfInput[input] = 0;
            builder.Climb(plan.Inputs[input].Node, plan.Root, true);
            return builder;
        }

        /// <summary>
        /// Builds every program of a plan to see what it asks of each input.
        /// </summary>
        public static MultiJoinCollected Collect(MultiJoinPlan plan)
        {
            var collected = new MultiJoinCollected()
            {
                Keys = new List<int[]>[plan.Inputs.Count],
                Read = new HashSet<int>[plan.Inputs.Count],
                Ranges = new List<(int[] Equal, int Column)>[plan.Inputs.Count]
            };
            for (int i = 0; i < plan.Inputs.Count; i++)
            {
                collected.Keys[i] = new List<int[]>();
                collected.Read[i] = new HashSet<int>();
                collected.Ranges[i] = new List<(int[] Equal, int Column)>();
            }
            for (int i = 0; i < plan.Inputs.Count; i++)
            {
                BuildChange(plan, i, collected).Finish(false);
            }
            foreach (var (join, keptLeft) in plan.PadTables)
            {
                BuildMatchTotals(plan, join, keptLeft, collected).Finish(true);
                BuildPaddedRows(plan, join, keptLeft, collected).Finish(false);
            }
            return collected;
        }

        /// <summary>
        /// Keys of a join looked up in the side opposite the kept one, as stored, for the weight each key matches there.
        /// </summary>
        public static MultiJoinProgram ForMatchTotals(MultiJoinPlan plan, MultiJoinNode join, bool keptLeft)
        {
            return BuildMatchTotals(plan, join, keptLeft, null).Finish(true);
        }

        private static Builder BuildMatchTotals(MultiJoinPlan plan, MultiJoinNode join, bool keptLeft, MultiJoinCollected? collected)
        {
            var key = keptLeft ? join.RightKey! : join.LeftKey!;
            var builder = NewBuilder(plan, -1, collected);
            builder.AddEntry(key);
            builder.Climb(plan.Inputs[key.Input].Node, keptLeft ? join.Right! : join.Left!, false);
            builder.Steps[0] = builder.Steps[0] with { PadLast = builder.Steps.Count - 1 };
            return builder;
        }

        /// <summary>
        /// Keys of a join looked up in its kept side, the rows found go on as padded rows of that join, or with the mark their start row carries.
        /// </summary>
        public static MultiJoinProgram ForPaddedRows(MultiJoinPlan plan, MultiJoinNode join, bool keptLeft)
        {
            return BuildPaddedRows(plan, join, keptLeft, null).Finish(false);
        }

        private static Builder BuildPaddedRows(MultiJoinPlan plan, MultiJoinNode join, bool keptLeft, MultiJoinCollected? collected)
        {
            var key = keptLeft ? join.LeftKey! : join.RightKey!;
            var builder = NewBuilder(plan, -1, collected);
            // With a condition on both sides the start rows may carry the columns of the key input the conditions read
            // The columns next to the key name rows, a null among them is a value like any other
            var start = MultiJoinPadTable.StartColumnsOf(join, keptLeft);
            builder.AddEntry(join.PadPerRow ? new MultiJoinKeySide() { Input = key.Input, Columns = start.Columns } : key, start.Fast);
            builder.Climb(plan.Inputs[key.Input].Node, keptLeft ? join.Left! : join.Right!, false);
            if (join.PadPerRow)
            {
                // The rows of the kept side enter the join as a change of that side would, its conditions decide per row
                builder.TurnJoin = join;
                builder.Climb(keptLeft ? join.Left! : join.Right!, plan.Root, true);
                return builder;
            }
            if (join.KeptOnlyCheck != null)
            {
                // A row failing it is padded whatever the other side holds, it is no row that turns
                builder.Checks.Add((builder.Steps.Count - 1, 0, join.KeptOnlyCheck));
            }
            if (join.IsMark)
            {
                builder.FrameOfMark[join.MarkId] = 0;
            }
            // A mark that turns takes a row away and gives it back, the totals of the joins above stay as they are
            builder.TotalsUnchanged = join.IsMark;
            builder.Climb(join, plan.Root, true);
            return builder;
        }
    }

    /// <summary>
    /// What the programs of a plan ask of each input: the column sets it is looked up by and the columns read from the rows found.
    /// </summary>
    internal sealed class MultiJoinCollected
    {
        public required List<int[]>[] Keys { get; init; }
        public required HashSet<int>[] Read { get; init; }
        public required List<(int[] Equal, int Column)>[] Ranges { get; init; }
    }

    /// <summary>
    /// Joins a set of rows with the stored inputs by one program, depth first, one output batch at a time.
    /// </summary>
    internal sealed class MultiJoinCursor
    {
        private readonly MultiJoinPlan _plan;
        private readonly MultiJoinInputState[] _inputs;
        private readonly MultiJoinPadTable[] _tables;
        private readonly IMemoryAllocator _allocator;
        private readonly int _rowBudget;
        private readonly MultiJoinProgram _program;
        private readonly MultiJoinFrame[] _frames;
        private readonly bool[] _active;
        private readonly bool[] _skip;
        private readonly bool[] _padded;
        private readonly bool[] _passThrough;
        private readonly bool[] _held;

        // Per padding step the weight its rows have matched so far
        private readonly int[]?[] _totals;

        // A step that turns rows: what the entries of the pad table add per row, and where its parts of them are
        private int[] _deltas = Array.Empty<int>();
        private int[] _deltaRows = Array.Empty<int>();
        private int _deltaPhase;
        private int _deltaRow;
        private int _deltaEntry;

        // The pad table whose entries a step that turns rows reads
        public MultiJoinPadTable? TurnTable { get; set; }

        // Reading the entries from their tree: the key columns of the step in the order of the table, and where the columns read go
        private IColumn[] _turnKeyColumns = Array.Empty<IColumn>();

        // Per step with comparisons before the copy: whether this run makes them, the share of rows that passed lately, and the runs so far
        private readonly bool[] _filtered;
        private readonly float[] _passRate;
        private readonly int[] _filterRuns;

        // Per step: the rows of this run came from a lookup that ran beside another, the lookup of the step met none
        private readonly bool[] _fromKept;

        // Tests: lookups begun with a range
        internal static long RangeLookupsForTests;


        // Step runs of this cursor that compared before the copy
        internal long FilteredRuns;

        // Above this share of rows passing, comparing before the copy is left out
        private const float MaxPassRate = 0.9f;

        // The lookup of the entries in their tree while it runs, let go of when the run is cut short
        private MultiJoinTreeLookup? _entryLookup;

        // Kept rows told to a pad table, and the table that holds more of them than it keeps in memory
        private int[] _affectedRows = Array.Empty<int>();
        private MultiJoinPadTable? _spill;
        private readonly MultiJoinFrame _deltaFrame = new MultiJoinFrame();
        private List<int>? _deltaCopy;
        private int[] _deltaTarget = Array.Empty<int>();

        // Per step feeding a pad table: the key rows that met a row, and the key columns
        private readonly bool[]?[] _found;
        private readonly bool[] _anyFound;
        private readonly IColumn[]?[] _tableKeys;

        // Per step the row indexes its key views read through
        private readonly PrimitiveList<int>?[] _keyOffsets;
        private readonly PrimitiveList<int>?[][] _keyOwnOffsets;

        // Per step with comparisons: one column per comparison with a value per key row, and the row indexes they are read through
        private readonly IColumn[]?[] _compareColumns;
        private readonly PrimitiveList<int>?[][] _compareOffsets;

        // Offsets composed with the offsets of a view in the start rows, per output batch
        private readonly List<(PrimitiveList<int> Source, PrimitiveList<int> Composed)> _composed = new();

        // Per frame the row indexes conditions read it through
        private readonly PrimitiveList<int>?[] _checkOffsets;
        private int[] _checkRows = Array.Empty<int>();
        private int[] _failed = Array.Empty<int>();

        // Stands in for the columns of a padded side when a condition reads them
        private Column? _checkNulls;

        // Lookups run beside one that waits: per step the rows found for the rows of its key frame, grouped by key row
        private readonly TaskScheduler? _scheduler;
        private readonly MultiJoinFrame?[] _kept;
        private readonly MultiJoinFrame?[] _keptProbe;
        private readonly int[]?[] _keptStart;
        private readonly int[]?[] _keptRows;
        private readonly bool[] _keptReady;
        private readonly bool[] _keptHeld;
        private readonly bool[] _keptTried;
        private readonly int[] _keptAt;
        private readonly int[] _keptWithin;
        private int[] _keptTake = Array.Empty<int>();
        private readonly Task?[] _keptTask;
        private readonly bool[] _keptTooMany;
        private readonly int[]?[] _keptKeyRow;
        private int _running;
        private bool _anyKept;
        private long _lookupsRunBeside;

        // A cursor that reads beside the cursors of other inputs has lookups of its own, made when a step first needs one
        private readonly MultiJoinInputLookup?[]? _ownLookups;
        private readonly MultiJoinInputLookup?[]? _ownBesideLookups;

        // In a round: the queue of each input's trees, the place of this batch in them, and per step whether its lookup has given its last part
        private MultiJoinTreeTurn[]? _turns;
        private int _turnPlace;
        private readonly bool[] _turnFinal;

        private int[] _scratch = Array.Empty<int>();
        private IReadOnlyList<IColumn>? _startColumns;
        private PrimitiveList<uint>? _startIterations;
        private bool _identity;
        private int _level = -1;

        public MultiJoinCursor(MultiJoinPlan plan, MultiJoinProgram program, MultiJoinInputState[] inputs, MultiJoinPadTable[] tables, IMemoryAllocator allocator, int rowBudget, TaskScheduler? scheduler = null, bool ownLookups = false)
        {
            _scheduler = scheduler;
            _turnFinal = new bool[program.Steps.Length];
            if (ownLookups)
            {
                _ownLookups = new MultiJoinInputLookup?[program.Steps.Length];
                _ownBesideLookups = new MultiJoinInputLookup?[program.Steps.Length];
            }
            _kept = new MultiJoinFrame?[program.Steps.Length];
            _keptProbe = new MultiJoinFrame?[program.Steps.Length];
            _keptStart = new int[]?[program.Steps.Length];
            _keptRows = new int[]?[program.Steps.Length];
            _keptReady = new bool[program.Steps.Length];
            _keptHeld = new bool[program.Steps.Length];
            _keptTried = new bool[program.Steps.Length];
            _keptAt = new int[program.Steps.Length];
            _keptWithin = new int[program.Steps.Length];
            _keptTask = new Task?[program.Steps.Length];
            _keptTooMany = new bool[program.Steps.Length];
            _keptKeyRow = new int[]?[program.Steps.Length];
            _plan = plan;
            _program = program;
            _inputs = inputs;
            _tables = tables;
            _allocator = allocator;
            _rowBudget = rowBudget;
            int steps = program.Steps.Length;
            _frames = new MultiJoinFrame[steps + 1];
            for (int f = 0; f < _frames.Length; f++)
            {
                _frames[f] = new MultiJoinFrame();
            }
            _active = new bool[steps];
            _skip = new bool[steps];
            _padded = new bool[steps];
            _passThrough = new bool[steps];
            _held = new bool[steps + 1];
            _totals = new int[]?[steps];
            _found = new bool[]?[steps];
            _anyFound = new bool[steps];
            _tableKeys = new IColumn[]?[steps];
            _keyOffsets = new PrimitiveList<int>?[steps];
            _keyOwnOffsets = new PrimitiveList<int>?[steps][];
            _filtered = new bool[steps];
            _passRate = new float[steps];
            _filterRuns = new int[steps];
            _fromKept = new bool[steps];
            _compareColumns = new IColumn[]?[steps];
            _compareOffsets = new PrimitiveList<int>?[steps][];
            _checkOffsets = new PrimitiveList<int>?[steps + 1];
        }

        public MultiJoinProgram Program => _program;

        /// <summary>
        /// Gives the cursor compiled conditions of its own, for one that runs beside other cursors.
        /// </summary>
        public void CompileOwnConditions(IFunctionsRegister functionsRegister)
        {
            foreach (var settles in _program.SettlesEndingAt)
            {
                foreach (var settle in settles)
                {
                    if (settle.Check?.Source != null)
                    {
                        settle.Check.Condition = settle.Check.Source.CompileNew(functionsRegister);
                    }
                }
            }
        }

        /// <summary>
        /// The most rows one lookup part has given, for tests.
        /// </summary>
        internal int MaxPartRows { get; private set; }

        public StreamEventBatch Current { get; private set; } = null!;

        /// <summary>
        /// Rows of the output batch last handed out.
        /// </summary>
        public int CurrentCount { get; private set; }

        /// <summary>
        /// The rows a run starts from. The caller fills row and weight, then begins.
        /// </summary>
        public MultiJoinFrame StartFrame => _frames[0];

        /// <summary>
        /// After a totals only run: per start row the weight it matched.
        /// </summary>
        public int[] StartTotals => _totals[0]!;

        /// <summary>
        /// Begins on a batch of the program's input. False when no row of it has a weight.
        /// </summary>
        public bool Start(EventBatchWeighted data)
        {
            Reset();
            int count = data.Weights.Count;
            var frame = _frames[0];
            frame.EnsureCapacity(count);
            var weights = data.Weights.Span;
            int kept = 0;
            for (int i = 0; i < count; i++)
            {
                int weight = weights[i];
                if (weight == 0)
                {
                    // A row without weight changes nothing
                    continue;
                }
                frame.Parent[kept] = i;
                frame.Weight[kept] = weight;
                kept++;
            }
            frame.Count = kept;
            if (kept == 0)
            {
                return false;
            }
            Begin(data.EventBatchData.Columns, kept == count, data.Iterations);
            return true;
        }

        /// <summary>
        /// Begins on the rows put in the start frame, each naming its row in the columns.
        /// </summary>
        public void Begin(IReadOnlyList<IColumn> columns, bool allRowsInOrder, PrimitiveList<uint>? iterations)
        {
            _identity = allRowsInOrder;
            _startColumns = columns;
            _startIterations = iterations;
            _level = 0;
        }

        /// <summary>
        /// Gives back what an unfinished run still holds.
        /// </summary>
        public void Reset()
        {
            for (int f = 1; f < _frames.Length; f++)
            {
                Release(f);
                _frames[f].IsNull = false;
                _frames[f].HasMark = false;
            }
            for (int level = 0; level < _active.Length; level++)
            {
                if (_active[level] && !_skip[level])
                {
                    // A lookup cut short still holds pages and the rows it copied
                    LookupOf(level).Abandon();
                }
            }
            _spill = null;
            if (_entryLookup != null)
            {
                // The lookup of the entries was cut short too
                _entryLookup.Abandon();
                _entryLookup = null;
            }
            _deltaPhase = 0;
            var deltaColumns = _deltaFrame.Columns;
            for (int i = 0; i < deltaColumns.Length; i++)
            {
                deltaColumns[i]?.Dispose();
                deltaColumns[i] = null!;
            }
            Array.Clear(_keptTooMany);
            if (_anyKept)
            {
                _anyKept = false;
                for (int level = 0; level < _kept.Length; level++)
                {
                    ReleaseKept(level);
                }
            }
            Array.Clear(_active);
            Array.Clear(_passThrough);
            for (int s = 0; s < _tableKeys.Length; s++)
            {
                if (_tableKeys[s] != null)
                {
                    Array.Clear(_tableKeys[s]!);
                }
            }
            for (int s = 0; s < _compareColumns.Length; s++)
            {
                if (_compareColumns[s] != null)
                {
                    Array.Clear(_compareColumns[s]!);
                }
            }
            _startColumns = null;
            _startIterations = null;
            _level = -1;
            Current = null!;
        }

        public void Dispose()
        {
            Reset();
            for (int s = 0; s < _compareOffsets.Length; s++)
            {
                var lists = _compareOffsets[s];
                for (int i = 0; lists != null && i < lists.Length; i++)
                {
                    lists[i]?.Dispose();
                    lists[i] = null;
                }
            }
            for (int s = 0; s < _keyOffsets.Length; s++)
            {
                _keyOffsets[s]?.Dispose();
                _keyOffsets[s] = null;
                var own = _keyOwnOffsets[s];
                if (own != null)
                {
                    for (int i = 0; i < own.Length; i++)
                    {
                        own[i]?.Dispose();
                        own[i] = null;
                    }
                }
            }
            for (int f = 0; f < _checkOffsets.Length; f++)
            {
                _checkOffsets[f]?.Dispose();
                _checkOffsets[f] = null;
            }
            foreach (var settles in _program.SettlesEndingAt)
            {
                foreach (var own in settles.Select(x => x.Check?.OwnOffsets))
                {
                    for (int i = 0; own != null && i < own.Length; i++)
                    {
                        own[i]?.Dispose();
                        own[i] = null;
                    }
                }
            }
            _checkNulls?.Dispose();
            _checkNulls = null;
        }

        private void Release(int frameIndex)
        {
            if (!_held[frameIndex])
            {
                return;
            }
            _held[frameIndex] = false;
            var columns = _frames[frameIndex].Columns;
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i].Return();
                columns[i] = null!;
            }
        }

        private Span<int> Scratch(int count)
        {
            if (_scratch.Length < count)
            {
                _scratch = new int[Math.Max(count, _scratch.Length * 2)];
            }
            return _scratch.AsSpan(0, count);
        }

        private void Fill(ref PrimitiveList<int>? list, ReadOnlySpan<int> values)
        {
            list ??= new PrimitiveList<int>(_allocator);
            list.Clear();
            list.EnsureCapacity(values.Length);
            list.SetLength(values.Length);
            values.CopyTo(list.Span);
        }

        private static void Through(Span<int> rows, ReadOnlySpan<int> map)
        {
            for (int i = 0; i < rows.Length; i++)
            {
                rows[i] = map[rows[i]];
            }
        }

        /// <summary>
        /// Puts the key columns of a step in place, one key row per row of the frame before it.
        /// </summary>
        private bool SetKeys(int level, MultiJoinInputLookup lookup)
        {
            ref readonly var step = ref _program.Steps[level];
            int f = step.KeyFrame;
            // A padded row has no value to seek by
            if (f < 0 || _frames[f].IsNull)
            {
                return false;
            }
            var keyColumns = lookup.KeyColumns;
            // The equal columns come first, a bound is the last
            var positions = step.KeyPositions.AsSpan(0, keyColumns.Length);
            int count = _frames[level].Count;

            // The key rows of the frame the step starts from are its rows as they are
            bool asIs = f == level && (level > 0 || _identity);
            var rows = Scratch(count);
            if (!asIs)
            {
                for (int i = 0; i < count; i++)
                {
                    rows[i] = i;
                }
                for (int at = level; at > f; at--)
                {
                    Through(rows, _frames[at].Parent);
                }
                if (f == 0 && !_identity)
                {
                    Through(rows, _frames[0].Parent);
                }
            }
            bool shared = false;
            for (int i = 0; i < positions.Length; i++)
            {
                var inner = f == 0 ? _startColumns![positions[i]] : _frames[f].Columns[positions[i]];
                if (asIs)
                {
                    if (i >= MultiJoinTreeLookup.MaxComparedInPlace && _scheduler != null && inner is ColumnWithOffset late && late.InnerColumn is Column)
                    {
                        // The sorter reads the state of the first columns only, a lookup beside this one may not be the first to read the rest
                        late.GetColumnState();
                    }
                    keyColumns[i] = inner;
                    continue;
                }
                if (inner is ColumnWithOffset view)
                {
                    // Views are not nested, this key reads the column the view is over
                    var own = _keyOwnOffsets[level] ??= new PrimitiveList<int>?[step.KeyPositions.Length];
                    var list = own[i] ??= new PrimitiveList<int>(_allocator);
                    list.Clear();
                    list.EnsureCapacity(count);
                    list.SetLength(count);
                    var target = list.Span;
                    var viewOffsets = view.Offsets.Span;
                    for (int r = 0; r < count; r++)
                    {
                        target[r] = viewOffsets[rows[r]];
                    }
                    keyColumns[i] = new ColumnWithOffset(view.InnerColumn, list);
                    continue;
                }
                if (!shared)
                {
                    Fill(ref _keyOffsets[level], rows);
                    shared = true;
                }
                keyColumns[i] = new ColumnWithOffset(inner, _keyOffsets[level]!);
            }

            var compares = step.Compares;
            // Comparing before the copy pays when rows fail. When nearly all passed lately it is left out, and tried again now and then
            _filtered[level] = compares != null && lookup.TakesFilter && !(_passRate[level] > MaxPassRate && (_filterRuns[level]++ & 63) != 63);
            if (_filtered[level])
            {
                FilteredRuns++;
                lookup.SetFilter(CompareColumns(level, compares!, count), compares);
            }
            else
            {
                lookup.SetFilter(null, null);
            }
            if (step.Turn && TurnTable != null)
            {
                // Kept for the lookup of the entries, which comes after the one in the rows stored
                var paddedKey = TurnTable.PaddedKey;
                var order = _plan.Inputs[step.Input].KeyLists[step.KeyList];
                if (_turnKeyColumns.Length != paddedKey.Length)
                {
                    _turnKeyColumns = new IColumn[paddedKey.Length];
                }
                for (int i = 0; i < paddedKey.Length; i++)
                {
                    _turnKeyColumns[i] = keyColumns[Array.IndexOf(order, paddedKey[i])];
                }
            }
            if (step.TableJoin >= 0)
            {
                // The lookup lets go of its key columns when it ends, the pad table needs them after that
                var order = step.TableOrder!;
                var kept = _tableKeys[level] ??= new IColumn[order.Length];
                for (int i = 0; i < kept.Length; i++)
                {
                    kept[i] = keyColumns[order[i]];
                }
            }
            return true;
        }

        // Per comparison the column it reads from a frame, with one value per row of the frame the step starts from
        private IColumn[] CompareColumns(int level, MultiJoinStepCompare[] compares, int count)
        {
            int most = compares.Length;
            var columns = _compareColumns[level] ??= new IColumn[most];
            var offsets = _compareOffsets[level] ??= new PrimitiveList<int>?[most];
            for (int c = 0; c < compares.Length; c++)
            {
                int f = compares[c].Frame;
                if (f > 0 && _frames[f].IsNull)
                {
                    // The rows of that frame are padded
                    columns[c] = Nulls(count);
                    continue;
                }
                var inner = f == 0 ? _startColumns![compares[c].Position] : _frames[f].Columns[compares[c].Position];
                if (f == level && (level > 0 || _identity))
                {
                    columns[c] = inner;
                    continue;
                }
                var rows = Scratch(count);
                for (int i = 0; i < count; i++)
                {
                    rows[i] = i;
                }
                for (int at = level; at > f; at--)
                {
                    Through(rows, _frames[at].Parent);
                }
                if (f == 0 && !_identity)
                {
                    Through(rows, _frames[0].Parent);
                }
                if (inner is ColumnWithOffset view)
                {
                    // Views are not nested, the comparison reads the column the view is over
                    var viewOffsets = view.Offsets.Span;
                    for (int r = 0; r < count; r++)
                    {
                        rows[r] = viewOffsets[rows[r]];
                    }
                    inner = view.InnerColumn;
                }
                Fill(ref offsets[c], rows);
                columns[c] = new ColumnWithOffset(inner, offsets[c]!);
            }
            return columns;
        }

        private void BeginStep(int level, int sourceCount)
        {
            ref readonly var step = ref _program.Steps[level];
            _padded[level] = false;
            if (step.PadLast >= 0)
            {
                var totals = _totals[level];
                if (totals == null || totals.Length < sourceCount)
                {
                    totals = _totals[level] = new int[Math.Max(sourceCount, (totals?.Length ?? 0) * 2)];
                }
                Array.Clear(totals, 0, sourceCount);
            }
            if (step.Turn)
            {
                if (_deltas.Length < sourceCount)
                {
                    _deltas = new int[Math.Max(sourceCount, _deltas.Length * 2)];
                }
                Array.Clear(_deltas, 0, sourceCount);
                _deltaPhase = 0;
                _deltaRow = 0;
                _deltaEntry = -1;
            }
            if (step.TableJoin >= 0)
            {
                var found = _found[level];
                if (found == null || found.Length < sourceCount)
                {
                    found = _found[level] = new bool[Math.Max(sourceCount, (found?.Length ?? 0) * 2)];
                }
                Array.Clear(found, 0, sourceCount);
                _anyFound[level] = false;
            }
        }

        // A joined row weighs the product of its rows
        private static void Multiply(MultiJoinFrame frame, MultiJoinFrame before)
        {
            var weight = frame.Weight.AsSpan(0, frame.Count);
            var parent = frame.Parent.AsSpan(0, frame.Count);
            var beforeWeight = before.Weight.AsSpan(0, before.Count);
            for (int r = 0; r < weight.Length; r++)
            {
                weight[r] *= beforeWeight[parent[r]];
            }
        }

        // Rows of a whole kept side stand in the frame after its last step: their keys may have padded rows to correct
        private void MarkFound(int entry, int end)
        {
            int count = _frames[end + 1].Count;
            if (count == 0)
            {
                return;
            }
            var found = _found[entry]!;
            if (entry == end)
            {
                var parent = _frames[end + 1].Parent.AsSpan(0, count);
                for (int r = 0; r < parent.Length; r++)
                {
                    found[parent[r]] = true;
                }
            }
            else
            {
                for (int r = 0; r < count; r++)
                {
                    int row = r;
                    for (int f = end + 1; f > entry; f--)
                    {
                        row = _frames[f].Parent[row];
                    }
                    found[row] = true;
                }
            }
            _anyFound[entry] = true;
            var positions = _program.Steps[entry].AffectedPositions;
            if (positions != null)
            {
                AddAffected(entry, end, count, positions);
            }
        }

        // The kept rows a change joined, for a pad table that looks only at them
        private void AddAffected(int entry, int end, int count, int[] positions)
        {
            if (_affectedRows.Length < count)
            {
                _affectedRows = new int[Math.Max(count, _affectedRows.Length * 2)];
            }
            var rows = _affectedRows.AsSpan(0, count);
            for (int r = 0; r < count; r++)
            {
                int row = r;
                for (int f = end + 1; f > entry + 1; f--)
                {
                    row = _frames[f].Parent[row];
                }
                rows[r] = row;
            }
            var table = _tables[_program.Steps[entry].TableJoin];
            table.AddAffected(_frames[entry + 1].Columns, positions, rows);
            if (table.NeedsSpill)
            {
                _spill = table;
            }
        }

        private Column Nulls(int count)
        {
            var nulls = _checkNulls ??= Column.Create(_allocator);
            if (nulls.Count < count)
            {
                nulls.InsertNullRange(nulls.Count, count - nulls.Count);
            }
            return nulls;
        }

        /// <summary>
        /// Removes the rows of the frame after a step that fail a condition.
        /// </summary>
        private void ApplyCheck(int endLevel, MultiJoinStepCheck check)
        {
            int own = endLevel + 1;
            var frame = _frames[own];
            int count = frame.Count;
            if (_checkRows.Length < count)
            {
                _checkRows = new int[Math.Max(count, _checkRows.Length * 2)];
                _failed = new int[_checkRows.Length];
            }

            var earlier = check.EarlierFrames;
            if (earlier.Length > 0)
            {
                var rows = _checkRows.AsSpan(0, count);
                for (int i = 0; i < count; i++)
                {
                    rows[i] = i;
                }
                int at = own;
                for (int e = 0; e < earlier.Length; e++)
                {
                    int f = earlier[e];
                    while (at > f)
                    {
                        Through(rows, _frames[at].Parent);
                        at--;
                    }
                    if (f == 0 && !_identity)
                    {
                        // Frame 0 is the lowest, nothing reads the rows after this
                        Through(rows, _frames[0].Parent);
                    }
                    Fill(ref _checkOffsets[f], rows);
                }
            }

            var columns = check.Columns;
            for (int c = 0; c < columns.Length; c++)
            {
                int f = check.Frames[c];
                if (f < 0 || _frames[f].IsNull)
                {
                    columns[c] = Nulls(count);
                    continue;
                }
                int position = check.Positions[c];
                if (f == own)
                {
                    columns[c] = frame.Columns[position];
                    continue;
                }
                var inner = f == 0 ? _startColumns![position] : _frames[f].Columns[position];
                if (inner is ColumnWithOffset view)
                {
                    // Views are not nested, the condition reads the view's own column
                    var list = check.OwnOffsets[c] ??= new PrimitiveList<int>(_allocator);
                    list.Clear();
                    list.EnsureCapacity(count);
                    list.SetLength(count);
                    var target = list.Span;
                    var viewOffsets = view.Offsets.Span;
                    var through = _checkOffsets[f]!.Span;
                    for (int r = 0; r < count; r++)
                    {
                        target[r] = viewOffsets[through[r]];
                    }
                    columns[c] = new ColumnWithOffset(view.InnerColumn, list);
                    continue;
                }
                columns[c] = new ColumnWithOffset(inner, _checkOffsets[f]!);
            }

            int failed = 0;
            var condition = check.Condition;
            var batch = check.Batch;
            for (int r = 0; r < count; r++)
            {
                if (!condition(batch, r))
                {
                    _failed[failed++] = r;
                }
            }
            Array.Clear(columns);
            if (failed == 0)
            {
                return;
            }

            int kept = 0;
            int next = 0;
            bool hasMark = frame.HasMark;
            for (int r = 0; r < count; r++)
            {
                if (next < failed && _failed[next] == r)
                {
                    next++;
                    continue;
                }
                frame.Parent[kept] = frame.Parent[r];
                frame.Weight[kept] = frame.Weight[r];
                if (hasMark)
                {
                    frame.Mark[kept] = frame.Mark[r];
                }
                kept++;
            }
            frame.Count = kept;
            if (!frame.IsNull)
            {
                var ownColumns = frame.Columns;
                for (int i = 0; i < ownColumns.Length; i++)
                {
                    ownColumns[i].DeleteBatch(_failed.AsSpan(0, failed));
                }
            }
        }

        /// <summary>
        /// What the joins whose side ends at a step do with the rows that arrived in the frame after it, innermost join first.
        /// </summary>
        private bool Settle(int endLevel, int startLimit)
        {
            var settles = _program.SettlesEndingAt[endLevel];
            // The steps with a pad table whose side ends here, a row padded by the step itself is no row it found
            var entries = _program.TableEntriesEndingAt[endLevel];
            int entry = entries.Length - 1;
            while (entry >= 0 && entries[entry] >= startLimit)
            {
                entry--;
            }
            var frame = _frames[endLevel + 1];
            Span<int> rows = default;
            int at = -1;
            for (int s = 0; s < settles.Length; s++)
            {
                int start = settles[s].Entry;
                if (start >= startLimit)
                {
                    continue;
                }
                // The conditions inside a kept side and the one on the kept side itself come first, a key is remembered for the rows that pass
                while (entry >= 0 && entries[entry] > start)
                {
                    MarkFound(entries[entry--], endLevel);
                }
                var check = settles[s].Check;
                if (check != null)
                {
                    ApplyCheck(endLevel, check);
                    if (frame.Count == 0)
                    {
                        return false;
                    }
                    // Rows were removed, the row indexes are read again
                    at = -1;
                    continue;
                }
                int count = frame.Count;
                if (at < 0)
                {
                    rows = Scratch(count);
                    for (int i = 0; i < count; i++)
                    {
                        rows[i] = i;
                    }
                    at = endLevel + 1;
                }
                while (at > start)
                {
                    Through(rows, _frames[at].Parent);
                    at--;
                }
                bool turn = _program.Steps[start].Turn;
                var totals = turn && _deltaPhase == 1 ? _deltas : _totals[start]!;
                var weight = frame.Weight.AsSpan(0, count);
                for (int i = 0; i < count; i++)
                {
                    totals[rows[i]] += weight[i];
                }
                if (_program.Steps[start].Mark || turn)
                {
                    // Rows of a mark join's right side are no rows of the joins around it: the marked row comes later and is settled then
                    return true;
                }
            }
            while (entry >= 0)
            {
                MarkFound(entries[entry--], endLevel);
            }
            return true;
        }

        /// <summary>
        /// Fills the frames of a padded side with the rows that matched nothing there. False when every row matched.
        /// </summary>
        private bool CreatePadPart(int level, int padLast, bool mark)
        {
            var source = _frames[level];
            var totals = _totals[level]!;
            var first = _frames[level + 1];
            Release(level + 1);
            first.Count = 0;
            first.EnsureCapacity(source.Count);
            int count = 0;
            if (mark)
            {
                // Every row goes on, the mark tells whether it matched
                if (first.Mark.Length < source.Count)
                {
                    first.Mark = new bool[Math.Max(source.Count, first.Mark.Length * 2)];
                }
                for (int r = 0; r < source.Count; r++)
                {
                    first.Parent[r] = r;
                    first.Weight[r] = source.Weight[r];
                    first.Mark[r] = totals[r] != 0;
                }
                count = source.Count;
            }
            else if (_program.Steps[level].Turn)
            {
                for (int r = 0; r < source.Count; r++)
                {
                    int before = totals[r];
                    if ((before != 0) == (before + _deltas[r] != 0))
                    {
                        continue;
                    }
                    // A row that lost its last match gets its padded row, one that got its first match loses it
                    first.Parent[count] = r;
                    first.Weight[count] = before != 0 ? source.Weight[r] : -source.Weight[r];
                    count++;
                }
            }
            else
            {
                for (int r = 0; r < source.Count; r++)
                {
                    if (totals[r] == 0)
                    {
                        first.Parent[count] = r;
                        first.Weight[count] = source.Weight[r];
                        count++;
                    }
                }
            }
            if (count == 0)
            {
                return false;
            }
            first.Count = count;
            first.IsNull = true;
            first.HasMark = mark;
            for (int f = level + 2; f <= padLast + 1; f++)
            {
                var frame = _frames[f];
                Release(f);
                frame.EnsureCapacity(count);
                frame.Count = count;
                frame.IsNull = true;
                for (int i = 0; i < count; i++)
                {
                    frame.Parent[i] = i;
                }
                Array.Copy(first.Weight, frame.Weight, count);
            }
            return true;
        }

        /// <summary>
        /// Lookups that ran beside another since the last call.
        /// </summary>
        public long TakeLookupsRunBeside()
        {
            long count = _lookupsRunBeside;
            _lookupsRunBeside = 0;
            return count;
        }

        private void ReleaseKept(int level)
        {
            _keptReady[level] = false;
            _keptTried[level] = false;
            if (_keptHeld[level])
            {
                _keptHeld[level] = false;
                ReturnColumns(_kept[level]!);
            }
        }

        private static void ReturnColumns(MultiJoinFrame frame)
        {
            var columns = frame.Columns;
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i]?.Return();
                columns[i] = null!;
            }
        }

        // The rows of a frame are new, what was found for its old rows is of no use
        private void ReleaseKeptOf(int frameIndex)
        {
            var steps = _program.Steps;
            for (int l = frameIndex; l < steps.Length; l++)
            {
                if (steps[l].KeyFrame == frameIndex)
                {
                    ReleaseKept(l);
                }
            }
        }

        // The lookup of this step waits for a page: starts the lookups of the later steps whose key rows exist already
        private void StartBeside(int level)
        {
            var steps = _program.Steps;
            int cap = MultiJoinOperator.ParallelLookupMaxRows;
            for (int l = level + 1; l < steps.Length; l++)
            {
                var step = steps[l];
                int f = step.KeyFrame;
                if (f < 0 || f > level || _keptTried[l] || _keptTooMany[l] || _frames[f].IsNull)
                {
                    continue;
                }
                var lookup = BesideLookupOf(l);
                var positions = step.KeyPositions;
                var keyColumns = lookup.KeyColumns;
                int keyRows = 0;
                for (int i = 0; i < positions.Length; i++)
                {
                    IColumn column = f == 0 ? _startColumns![positions[i]] : _frames[f].Columns[positions[i]];
                    if (column is ColumnWithOffset view && view.InnerColumn is Column)
                    {
                        // A view works out on first use whether it holds nulls, two threads may not do that at once
                        view.GetColumnState();
                    }
                    keyColumns[i] = column;
                    // Every row the start columns have, also the ones without weight: their rows are found and never asked for
                    keyRows = f == 0 ? column.Count : _frames[f].Count;
                }
                _keptTried[l] = true;
                _anyKept = true;
                _keptTask[l] = StartKeep(lookup, l, keyRows, _program.CopyColumns[l], cap);
                _running++;
            }
        }

        // On its own, so the values a task takes with it are only set aside when one is started
        private Task StartKeep(MultiJoinInputLookup lookup, int level, int keyRows, bool copy, int cap)
        {
            return Task.Factory.StartNew(
                () => Keep(lookup, level, keyRows, copy, cap),
                CancellationToken.None,
                TaskCreationOptions.None,
                _scheduler!).Unwrap();
        }

        // A lookup that was started is not left alone when the lookups beside it cannot be started
        private static async ValueTask EndThenThrow(ValueTask started, Exception failure)
        {
            try
            {
                await started;
            }
            catch
            {
                // The failure to start is the one told
            }
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

        private static async ValueTask<bool> EndThenThrow(ValueTask<bool> started, MultiJoinFrame part, Exception failure)
        {
            try
            {
                if (await started)
                {
                    // Nobody has the columns of this part yet, and nobody will
                    var columns = part.Columns;
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i]?.Dispose();
                        columns[i] = null!;
                    }
                }
            }
            catch
            {
                // The failure to start is the one told
            }
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
            return false;
        }

        private bool StartsOthers(bool completed)
        {
            return (!completed || MultiJoinOperator.ParallelLookupsAlways) && _scheduler != null && MultiJoinOperator.ParallelLookups;
        }

        // The rows a lookup beside the others found are needed now
        private Task TakeTask(int level)
        {
            var task = _keptTask[level]!;
            _keptTask[level] = null;
            _running--;
            _lookupsRunBeside++;
            return task;
        }

        // The lookups still running read the rows of this frame, they end before the rows go
        private async ValueTask EndTasksOf(int frameIndex)
        {
            var steps = _program.Steps;
            for (int l = frameIndex; l < steps.Length; l++)
            {
                if (_keptTask[l] != null && steps[l].KeyFrame == frameIndex)
                {
                    await TakeTask(l);
                }
            }
        }

        // No lookup runs on while the rows are handed out or let go of
        private async ValueTask EndTasks(bool failed)
        {
            Exception? first = null;
            for (int l = 0; l < _keptTask.Length; l++)
            {
                if (_keptTask[l] == null)
                {
                    continue;
                }
                try
                {
                    await TakeTask(l);
                }
                catch (Exception e)
                {
                    first ??= e;
                }
            }
            // After a failure of the run that failure is the one reported
            if (first != null && !failed)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(first).Throw();
            }
        }

        // Looks a step up for every row of its key frame and keeps the rows found
        private async Task Keep(MultiJoinInputLookup lookup, int level, int keyRows, bool copy, int cap)
        {
            var kept = _kept[level] ??= new MultiJoinFrame();
            kept.Count = 0;
            bool held = false;
            try
            {
                bool complete = true;
                if (_turns != null)
                {
                    // The walk lets go of the trees when it has taken the rows kept here
                    await _turns[_program.Steps[level].Input].Enter(_turnPlace);
                }
                if (keyRows > 0)
                {
                    await lookup.Start(keyRows);
                    if (await lookup.NextPart(kept, cap, _allocator, copy))
                    {
                        var columns = kept.Columns;
                        for (int i = 0; i < columns.Length; i++)
                        {
                            columns[i].Rent(1);
                        }
                        held = true;
                        var probe = _keptProbe[level] ??= new MultiJoinFrame();
                        complete = !await lookup.NextPart(probe, 1, _allocator, false);
                    }
                }
                lookup.Abandon();
                if (!complete)
                {
                    // More rows than are kept: the step is looked up in its turn and not tried again for this batch
                    _keptTooMany[level] = true;
                    ReturnColumns(kept);
                    return;
                }
                GroupByKeyRow(level, kept, keyRows);
                _keptHeld[level] = held;
                _keptReady[level] = true;
            }
            catch
            {
                lookup.Abandon();
                if (held)
                {
                    ReturnColumns(kept);
                }
                throw;
            }
        }

        // Per key row where its rows are among the ones kept
        private void GroupByKeyRow(int level, MultiJoinFrame kept, int keyRows)
        {
            int count = kept.Count;
            var start = _keptStart[level];
            if (start == null || start.Length < keyRows + 1)
            {
                start = _keptStart[level] = new int[Math.Max(keyRows + 1, (start?.Length ?? 0) * 2)];
            }
            Array.Clear(start, 0, keyRows + 1);
            for (int i = 0; i < count; i++)
            {
                start[kept.Parent[i] + 1]++;
            }
            for (int k = 0; k < keyRows; k++)
            {
                start[k + 1] += start[k];
            }
            var rows = _keptRows[level];
            if (rows == null || rows.Length < count + keyRows + 1)
            {
                rows = _keptRows[level] = new int[Math.Max(count + keyRows + 1, (rows?.Length ?? 0) * 2)];
            }
            // Where the next row of each key row goes, behind the rows themselves
            var at = rows.AsSpan(count, keyRows + 1);
            start.AsSpan(0, keyRows + 1).CopyTo(at);
            for (int i = 0; i < count; i++)
            {
                rows[at[kept.Parent[i]]++] = i;
            }
        }

        // The next rows of a step from the ones kept for it, as a lookup would give them
        private bool NextKept(int level, MultiJoinFrame target)
        {
            var kept = _kept[level]!;
            int count = _frames[level].Count;
            target.Count = 0;
            int at = _keptAt[level];
            if (kept.Count == 0 || at >= count)
            {
                return false;
            }
            // Worked out with the first part and kept: a frame of padded rows gives many parts
            var keyRows = _keptKeyRow[level];
            if (keyRows == null || keyRows.Length < count)
            {
                keyRows = _keptKeyRow[level] = new int[Math.Max(count, (keyRows?.Length ?? 0) * 2)];
            }
            var keyRow = keyRows.AsSpan(0, count);
            if (at == 0 && _keptWithin[level] == 0)
            {
                int f = _program.Steps[level].KeyFrame;
                for (int i = 0; i < count; i++)
                {
                    keyRow[i] = i;
                }
                for (int frame = level; frame > f; frame--)
                {
                    Through(keyRow, _frames[frame].Parent);
                }
                if (f == 0 && !_identity)
                {
                    Through(keyRow, _frames[0].Parent);
                }
            }
            var start = _keptStart[level]!;
            var byKey = _keptRows[level]!;
            int within = _keptWithin[level];
            int n = 0;
            while (at < count && n < _rowBudget)
            {
                int first = start[keyRow[at]] + within;
                int end = start[keyRow[at] + 1];
                int take = Math.Min(end - first, _rowBudget - n);
                if (take > 0)
                {
                    target.EnsureCapacity(n + take);
                    if (_keptTake.Length < n + take)
                    {
                        Array.Resize(ref _keptTake, Math.Max(n + take, _keptTake.Length * 2));
                    }
                    for (int m = first; m < first + take; m++)
                    {
                        int row = byKey[m];
                        target.Parent[n] = at;
                        target.Weight[n] = kept.Weight[row];
                        _keptTake[n] = row;
                        n++;
                    }
                    target.Count = n;
                }
                if (first + take >= end)
                {
                    at++;
                    within = 0;
                }
                else
                {
                    within += take;
                }
            }
            _keptAt[level] = at;
            _keptWithin[level] = within;
            if (n == 0)
            {
                return false;
            }
            if (target.Columns.Length != kept.Columns.Length)
            {
                target.Columns = kept.Columns.Length == 0 ? Array.Empty<Column>() : new Column[kept.Columns.Length];
            }
            int created = 0;
            try
            {
                for (; created < kept.Columns.Length; created++)
                {
                    var column = Column.Create(_allocator);
                    target.Columns[created] = column;
                    MultiJoinTreeLookup.CopyRuns(column, kept.Columns[created], _keptTake.AsSpan(0, n));
                }
            }
            catch
            {
                // Nobody has the columns of this part yet
                for (int i = 0; i <= created && i < target.Columns.Length; i++)
                {
                    target.Columns[i]?.Dispose();
                    target.Columns[i] = null!;
                }
                throw;
            }
            return true;
        }

        private MultiJoinInputLookup LookupOf(int level)
        {
            var step = _program.Steps[level];
            if (step.Range != null && !_active[level])
            {
                Interlocked.Increment(ref RangeLookupsForTests);
            }
            if (_ownLookups == null)
            {
                return _inputs[step.Input].GetLookup(step.KeyList, step.Range, false, step.MatchNulls);
            }
            return _ownLookups[level] ??= _inputs[step.Input].CreateLookup(step.KeyList, step.Range, step.MatchNulls);
        }

        private MultiJoinInputLookup BesideLookupOf(int level)
        {
            var step = _program.Steps[level];
            if (_ownBesideLookups == null)
            {
                return _inputs[step.Input].GetLookup(step.KeyList, step.Range, true, step.MatchNulls);
            }
            return _ownBesideLookups[level] ??= _inputs[step.Input].CreateLookup(step.KeyList, step.Range, step.MatchNulls);
        }

        /// <summary>
        /// The next run is a batch of a round: a step waits for its place in the queue of the trees it reads.
        /// </summary>
        public void EnterRound(MultiJoinTreeTurn[] turns, int place)
        {
            _turns = turns;
            _turnPlace = place;
        }

        public void LeaveRound()
        {
            _turns = null;
        }

        // The batch comes back to the trees of a step for every further part of the steps below it
        private bool FinalBelow(int level)
        {
            for (int l = 0; l < level; l++)
            {
                if (!_turnFinal[l])
                {
                    return false;
                }
            }
            return true;
        }

        // A part of what the change itself gives the kept rows: per kept row the entries of its key, read as rows of the padded input
        private bool FillDeltaPart(int level, MultiJoinFrame frame)
        {
            var table = TurnTable!;
            var source = _frames[level];
            var groupStart = table.TurnGroupStart;
            var entryRows = table.TurnEntryRows;
            var sums = table.TurnEntrySums;
            frame.EnsureCapacity(_rowBudget);
            if (_deltaRows.Length < _rowBudget)
            {
                _deltaRows = new int[_rowBudget];
            }
            int count = 0;
            while (_deltaRow < source.Count && count < _rowBudget)
            {
                // The start rows are the keys in the order of the table
                int group = _deltaRow;
                for (int f = level; f > 0; f--)
                {
                    group = _frames[f].Parent[group];
                }
                group = table.TurnGroupOf[group];
                int end = groupStart[group + 1];
                int entry = _deltaEntry < 0 ? groupStart[group] : _deltaEntry;
                while (entry < end && count < _rowBudget)
                {
                    frame.Parent[count] = _deltaRow;
                    frame.Weight[count] = sums[entry];
                    _deltaRows[count] = entryRows[entry];
                    count++;
                    entry++;
                }
                if (entry < end)
                {
                    _deltaEntry = entry;
                    break;
                }
                _deltaEntry = -1;
                _deltaRow++;
            }
            if (count == 0)
            {
                return false;
            }
            frame.Count = count;
            if (!_program.CopyColumns[level])
            {
                frame.Columns = Array.Empty<Column>();
                return true;
            }
            var needed = _plan.Inputs[table.PaddedInput].NeededColumns;
            if (frame.Columns.Length != needed.Count)
            {
                frame.Columns = new Column[needed.Count];
            }
            var rows = _deltaRows.AsSpan(0, count);
            try
            {
                for (int c = 0; c < needed.Count; c++)
                {
                    var column = Column.Create(_allocator);
                    frame.Columns[c] = column;
                    var kept = table.TurnColumn(needed[c]);
                    if (kept == null)
                    {
                        // Nothing of the join reads it
                        column.InsertNullRange(0, count);
                    }
                    else
                    {
                        table.CopyEntries(column, kept, rows);
                    }
                }
            }
            catch
            {
                DisposeUnheld(frame);
                throw;
            }
            return true;
        }

        // Nobody has rented the columns of a part that could not be made in full
        private static void DisposeUnheld(MultiJoinFrame frame)
        {
            var columns = frame.Columns;
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i]?.Dispose();
                columns[i] = null!;
            }
            frame.Count = 0;
        }

        // The same from the tree of entries, for a change that did not fit in memory
        private async ValueTask<bool> FillDeltaPartFromTree(int level, MultiJoinFrame frame)
        {
            var table = TurnTable!;
            var lookup = table.EntryLookup;
            var needed = _plan.Inputs[table.PaddedInput].NeededColumns;
            if (_deltaCopy == null)
            {
                _deltaCopy = new List<int>();
                _deltaTarget = new int[needed.Count];
                for (int c = 0; c < needed.Count; c++)
                {
                    int treeColumn = table.TurnTreeColumn(needed[c]);
                    _deltaTarget[c] = treeColumn < 0 ? -1 : _deltaCopy.Count;
                    if (treeColumn >= 0)
                    {
                        _deltaCopy.Add(treeColumn);
                    }
                }
            }
            if (_deltaEntry < 0)
            {
                for (int i = 0; i < _turnKeyColumns.Length; i++)
                {
                    lookup.KeyColumns[i] = _turnKeyColumns[i];
                }
                _deltaEntry = 0;
                _entryLookup = lookup;
                await lookup.Start(_frames[level].Count);
            }
            bool copy = _program.CopyColumns[level];
            var scratch = _deltaFrame;
            if (!await lookup.NextPart(scratch, copy ? _deltaCopy : Array.Empty<int>(), _rowBudget, _allocator))
            {
                lookup.Abandon();
                _entryLookup = null;
                return false;
            }
            int count = scratch.Count;
            frame.EnsureCapacity(count);
            frame.Count = count;
            scratch.Parent.AsSpan(0, count).CopyTo(frame.Parent);
            scratch.Weight.AsSpan(0, count).CopyTo(frame.Weight);
            if (!copy)
            {
                frame.Columns = Array.Empty<Column>();
                return true;
            }
            if (frame.Columns.Length != needed.Count)
            {
                frame.Columns = new Column[needed.Count];
            }
            try
            {
                for (int c = 0; c < needed.Count; c++)
                {
                    int at = _deltaTarget[c];
                    if (at >= 0)
                    {
                        frame.Columns[c] = scratch.Columns[at];
                        scratch.Columns[at] = null!;
                        continue;
                    }
                    // Nothing of the join reads it
                    var column = Column.Create(_allocator);
                    frame.Columns[c] = column;
                    column.InsertNullRange(0, count);
                }
            }
            catch
            {
                // What is still in the scratch frame goes when the cursor is reset
                DisposeUnheld(frame);
                throw;
            }
            return true;
        }

        private ValueTask AddToTable(int level)
        {
            var source = _frames[level];
            var table = _tables[_program.Steps[level].TableJoin];
            if (table.Join.PadPerRow)
            {
                if (level != 0)
                {
                    throw new InvalidOperationException("A join with a condition on both sides is told of a change from the rows of the change.");
                }
                return table.Add(_tableKeys[level]!, source.Count, _found[level]!, source.Weight, _startColumns!, _identity ? null : _frames[0].Parent);
            }
            return _tables[_program.Steps[level].TableJoin].Add(_tableKeys[level]!, source.Count, _found[level]!, source.Weight);
        }

        /// <summary>
        /// Moves to the next output batch. False when the rows are joined in full.
        /// </summary>
        public async ValueTask<bool> MoveNext()
        {
            var steps = _program.Steps;
            int last = steps.Length - 1;
            bool totalsOnly = _program.TotalsOnly;
            bool noOutput = _program.NoOutput;
            var firstMark = _program.FirstMarkEndingAt;
            try
            {
                while (_level >= 0)
                {
                    if (_spill != null)
                    {
                        var spill = _spill;
                        _spill = null;
                        await spill.Spill();
                    }
                    int level = _level;
                    if (_passThrough[level])
                    {
                        // The frame of this step was padded by a step below, nothing was looked up here
                        _passThrough[level] = false;
                        _level--;
                        continue;
                    }
                    var step = steps[level];
                    // A step without a key finds nothing, its input may have no index at all
                    var lookup = step.KeyFrame < 0 ? null : LookupOf(level);
                    if (!_active[level])
                    {
                        if (lookup != null && step.Compares != null)
                        {
                            // Other steps share the lookup, what they met is not counted for this one
                            lookup.TakeScanned();
                            lookup.TakePassRate();
                        }
                        _fromKept[level] = false;
                        BeginStep(level, _frames[level].Count);
                        _skip[level] = lookup == null || !SetKeys(level, lookup);
                        // Set first: a lookup that fails while it starts is let go of like any other
                        _active[level] = true;
                        _keptAt[level] = 0;
                        _keptWithin[level] = 0;
                        if (_keptTask[level] != null)
                        {
                            await TakeTask(level);
                        }
                        if (_turns != null && lookup != null)
                        {
                            _turnFinal[level] = _skip[level];
                            if (!_skip[level])
                            {
                                await _turns[step.Input].Enter(_turnPlace);
                            }
                            else if (FinalBelow(level))
                            {
                                _turns[step.Input].Release(_turnPlace);
                            }
                        }
                        if (!_skip[level] && !_keptReady[level])
                        {
                            var started = lookup!.Start(_frames[level].Count);
                            if (StartsOthers(started.IsCompleted))
                            {
                                try
                                {
                                    StartBeside(level);
                                }
                                catch (Exception e)
                                {
                                    await EndThenThrow(started, e);
                                }
                            }
                            await started;
                        }
                    }
                    var frame = _frames[level + 1];
                    if (_anyKept)
                    {
                        if (_running > 0)
                        {
                            await EndTasksOf(level + 1);
                        }
                        ReleaseKeptOf(level + 1);
                    }
                    Release(level + 1);
                    bool got = false;
                    bool turn = step.Turn && !_skip[level] && !_padded[level];
                    if (!_skip[level] && !_padded[level] && !(turn && _deltaPhase > 0))
                    {
                        if (_keptReady[level])
                        {
                            _fromKept[level] = true;
                            got = NextKept(level, frame);
                        }
                        else
                        {
                            var part = lookup!.NextPart(frame, _rowBudget, _allocator, _program.CopyColumns[level]);
                            if (StartsOthers(part.IsCompleted))
                            {
                                try
                                {
                                    StartBeside(level);
                                }
                                catch (Exception e)
                                {
                                    await EndThenThrow(part, frame, e);
                                }
                            }
                            got = await part;
                        }
                        // The trees of the step are done with when its lookup has no more rows for the rows it was asked for
                        if (_turns != null && (!got || (_keptReady[level] ? _keptAt[level] >= _frames[level].Count : lookup!.IsExhausted)))
                        {
                            _turnFinal[level] = true;
                            if (FinalBelow(level))
                            {
                                _turns[step.Input].Release(_turnPlace);
                            }
                        }
                    }
                    if (!got && turn && _deltaPhase < 2)
                    {
                        // After the rows stored: the rows the change adds, counted apart
                        _deltaPhase = 1;
                        got = TurnTable!.TurnInTree ? await FillDeltaPartFromTree(level, frame) : FillDeltaPart(level, frame);
                        if (!got)
                        {
                            _deltaPhase = 2;
                        }
                    }
                    if (!got)
                    {
                        if (step.PadLast >= 0 && !_padded[level])
                        {
                            _padded[level] = true;
                            // A padded row is a row of the join it was padded for: the joins around it check and count it
                            if (!(totalsOnly && level == 0) && CreatePadPart(level, step.PadLast, step.Mark) && Settle(step.PadLast, level))
                            {
                                if (firstMark[step.PadLast] < level)
                                {
                                    // Inside the right side of a mark join: counted, not carried on
                                    continue;
                                }
                                if (step.PadLast == last)
                                {
                                    if (noOutput)
                                    {
                                        continue;
                                    }
                                    if (_running > 0)
                                    {
                                        await EndTasks(false);
                                    }
                                    Current = CreateOutput();
                                    return true;
                                }
                                for (int l = level + 1; l <= step.PadLast; l++)
                                {
                                    _passThrough[l] = true;
                                }
                                _level = step.PadLast + 1;
                                continue;
                            }
                        }
                        if (step.TableJoin >= 0 && _anyFound[level])
                        {
                            await AddToTable(level);
                        }
                        for (int f = level + 1; f <= (step.PadLast >= 0 ? step.PadLast : level) + 1; f++)
                        {
                            _frames[f].IsNull = false;
                            _frames[f].HasMark = false;
                        }
                        if (lookup != null && !_skip[level] && !_fromKept[level] && _filtered[level])
                        {
                            long scanned = lookup.TakeScanned();
                            float passed = lookup.TakePassRate();
                            if (passed >= 0)
                            {
                                _passRate[level] = _passRate[level] * 0.75f + passed * 0.25f;
                            }
                            else if (scanned > 0)
                            {
                                // The values are no plain ones, nothing was compared
                                _passRate[level] = 1;
                            }
                        }
                        _active[level] = false;
                        _level--;
                        continue;
                    }
                    frame.IsNull = false;
                    if (frame.Count > MaxPartRows)
                    {
                        MaxPartRows = frame.Count;
                    }
                    var columns = frame.Columns;
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i].Rent(1);
                    }
                    _held[level + 1] = true;
                    Multiply(frame, _frames[level]);
                    if (!Settle(level, int.MaxValue))
                    {
                        // Every row of the part failed a condition
                        continue;
                    }

                    if (firstMark[level] <= level)
                    {
                        // The right side of a mark join: its rows are counted, not carried on
                        continue;
                    }
                    if (level == last)
                    {
                        if (noOutput)
                        {
                            continue;
                        }
                        if (_running > 0)
                        {
                            await EndTasks(false);
                        }
                        Current = CreateOutput();
                        return true;
                    }
                    _level++;
                }
            }
            catch
            {
                if (_running > 0)
                {
                    await EndTasks(true);
                }
                throw;
            }
            if (_running > 0)
            {
                await EndTasks(false);
            }
            Reset();
            return false;
        }

        private StreamEventBatch CreateOutput()
        {
            var program = _program;
            int last = program.Steps.Length;
            var lastFrame = _frames[last];
            int count = lastFrame.Count;
            CurrentCount = count;

            var weights = new PrimitiveList<int>(_allocator, count);
            weights.EnsureCapacity(count);
            weights.SetLength(count);
            lastFrame.Weight.AsSpan(0, count).CopyTo(weights.Span);
            var iterations = new PrimitiveList<uint>(_allocator, count);
            iterations.EnsureCapacity(count);
            iterations.SetLength(count);

            var output = new IColumn[_plan.Output.Count];
            var rows = Scratch(count);
            for (int i = 0; i < count; i++)
            {
                rows[i] = i;
            }
            _composed.Clear();
            // One column of nulls serves every padded column of the batch
            Column? nulls = null;

            for (int f = last; f >= 0; f--)
            {
                if (f < last)
                {
                    Through(rows, _frames[f + 1].Parent);
                }
                var markOutputs = program.MarkOutputs;
                for (int m = 0; m < markOutputs.Length; m++)
                {
                    if (markOutputs[m].Frame == f)
                    {
                        output[markOutputs[m].Output] = _frames[f].HasMark ? CreateMarks(_frames[f].Mark, rows) : nulls ??= CreateNulls(count);
                    }
                }
                if (f == 0)
                {
                    if (!_identity)
                    {
                        Through(rows, _frames[0].Parent);
                    }
                    var target = iterations.Span;
                    if (_startIterations == null)
                    {
                        target.Clear();
                    }
                    else
                    {
                        var source = _startIterations.Span;
                        for (int r = 0; r < count; r++)
                        {
                            target[r] = source[rows[r]];
                        }
                    }
                }
                var outputs = program.FrameOutputs[f];
                if (outputs.Length == 0)
                {
                    continue;
                }
                bool isNull = _frames[f].IsNull;
                PrimitiveList<int>? offsets = null;
                for (int o = 0; o < outputs.Length; o++)
                {
                    int position = program.OutputPositions[outputs[o]];
                    if (isNull)
                    {
                        output[outputs[o]] = nulls ??= CreateNulls(count);
                        continue;
                    }
                    if (f == last)
                    {
                        // The rows of the batch are the rows of the last lookup
                        output[outputs[o]] = lastFrame.Columns[position];
                        continue;
                    }
                    var inner = f == 0 ? _startColumns![position] : _frames[f].Columns[position];
                    if (inner is ColumnWithOffset view)
                    {
                        output[outputs[o]] = new ColumnWithOffset(view.InnerColumn, Compose(view.Offsets, rows));
                        continue;
                    }
                    if (offsets == null)
                    {
                        Fill(ref offsets, rows);
                    }
                    output[outputs[o]] = new ColumnWithOffset(inner, offsets!);
                }
            }
            var nullOutputs = program.NullOutputs;
            for (int o = 0; o < nullOutputs.Length; o++)
            {
                output[nullOutputs[o]] = nulls ??= CreateNulls(count);
            }
            _composed.Clear();
            return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(output)));
        }

        private Column CreateMarks(bool[] marks, ReadOnlySpan<int> rows)
        {
            var column = Column.Create(_allocator);
            for (int r = 0; r < rows.Length; r++)
            {
                column.Add(marks[rows[r]] ? BoolValue.True : BoolValue.False);
            }
            return column;
        }

        private Column CreateNulls(int count)
        {
            var column = Column.Create(_allocator);
            column.InsertNullRange(0, count);
            return column;
        }

        // Columns that came out of the same join share their offsets, they share the composed ones too
        private PrimitiveList<int> Compose(PrimitiveList<int> viewOffsets, ReadOnlySpan<int> rows)
        {
            for (int i = 0; i < _composed.Count; i++)
            {
                if (ReferenceEquals(_composed[i].Source, viewOffsets))
                {
                    return _composed[i].Composed;
                }
            }
            var list = new PrimitiveList<int>(_allocator, rows.Length);
            list.EnsureCapacity(rows.Length);
            list.SetLength(rows.Length);
            var target = list.Span;
            var source = viewOffsets.Span;
            for (int r = 0; r < rows.Length; r++)
            {
                target[r] = source[rows[r]];
            }
            _composed.Add((viewOffsets, list));
            return list;
        }
    }
}
