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
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// A column of one input of a multi join. With <see cref="MarkInput"/> as input it is the mark of a mark join.
    /// </summary>
    internal readonly record struct MultiJoinColumn(int Input, int Column)
    {
        public const int MarkInput = -1;

        public bool IsMark => Input == MarkInput;
    }

    /// <summary>
    /// One side of a join key, all columns come from the same input.
    /// </summary>
    internal sealed class MultiJoinKeySide
    {
        public required int Input { get; init; }
        public required int[] Columns { get; init; }
    }

    /// <summary>
    /// A condition applied to joined rows after the lookup.
    /// </summary>
    internal sealed class MultiJoinCheck
    {
        // Field indexes address the columns list
        public required Expression Condition { get; init; }
        public required IReadOnlyList<MultiJoinColumn> Columns { get; init; }

        // The columns the condition reads, the compiled condition addresses them by position
        public MultiJoinColumn[] Reads => _reads ??= FindReads();

        private MultiJoinColumn[]? _reads;
        private List<int> _fields = new List<int>();

        public Func<EventBatchData, int, bool>? Compiled { get; private set; }

        public void Compile(IFunctionsRegister functionsRegister)
        {
            if (Compiled != null)
            {
                return;
            }
            Compiled = CompileNew(functionsRegister);
        }

        /// <summary>
        /// A compiled condition of its own for a cursor that runs beside others, a compiled condition keeps its working values.
        /// </summary>
        public Func<EventBatchData, int, bool> CompileNew(IFunctionsRegister functionsRegister)
        {
            _reads ??= FindReads();
            var oldToNew = new Dictionary<int, int>();
            for (int i = 0; i < _fields.Count; i++)
            {
                oldToNew.Add(_fields[i], i);
            }
            // The plan keeps the condition as it was written
            var copy = Condition.Clone();
            new ExpressionFieldReplaceVisitor(oldToNew).Visit(copy, null);
            return ColumnBooleanCompiler.Compile(copy, functionsRegister);
        }

        // The condition reads a value inside a column, a field of a struct or a key of a map
        public bool ReadsInside => _readsInside ??= InsideFinder.Find(Condition);

        private bool? _readsInside;

        private sealed class InsideFinder : ExpressionVisitor<object?, object?>
        {
            private bool _found;

            public static bool Find(Expression expression)
            {
                var finder = new InsideFinder();
                finder.Visit(expression, null);
                return finder._found;
            }

            public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
            {
                _found |= directFieldReference.ReferenceSegment.Child != null;
                return base.VisitDirectFieldReference(directFieldReference, state);
            }
        }

        // Set when the condition is one comparison of two columns
        public MultiJoinCompare? Compare
        {
            get
            {
                if (!_compareLooked)
                {
                    _compareLooked = true;
                    _compare = FindCompare();
                }
                return _compare;
            }
        }

        private bool _compareLooked;
        private MultiJoinCompare? _compare;

        private MultiJoinCompare? FindCompare()
        {
            if (Condition is not ScalarFunction function || function.ExtensionUrn != FunctionsComparison.Urn || function.Arguments.Count != 2 ||
                function.Arguments[0] is not DirectFieldReference { ReferenceSegment: StructReferenceSegment left } ||
                function.Arguments[1] is not DirectFieldReference { ReferenceSegment: StructReferenceSegment right } ||
                left.Child != null || right.Child != null)
            {
                // Also when it reads a value inside a column: only the compiled condition knows how
                return null;
            }
            MultiJoinCompareOp op;
            switch (function.ExtensionName)
            {
                case FunctionsComparison.Equal: op = MultiJoinCompareOp.Equal; break;
                case FunctionsComparison.NotEqual: op = MultiJoinCompareOp.NotEqual; break;
                case FunctionsComparison.LessThan: op = MultiJoinCompareOp.LessThan; break;
                case FunctionsComparison.LessThanOrEqual: op = MultiJoinCompareOp.LessThanOrEqual; break;
                case FunctionsComparison.GreaterThan: op = MultiJoinCompareOp.GreaterThan; break;
                case FunctionsComparison.GreaterThanOrEqual: op = MultiJoinCompareOp.GreaterThanOrEqual; break;
                default: return null;
            }
            if (left.Field >= Columns.Count || right.Field >= Columns.Count || Columns[left.Field].IsMark || Columns[right.Field].IsMark)
            {
                return null;
            }
            return new MultiJoinCompare(Columns[left.Field], op, Columns[right.Field]);
        }

        private MultiJoinColumn[] FindReads()
        {
            var usage = new ExpressionFieldUsageVisitor(int.MaxValue);
            usage.Visit(Condition, null);
            _fields = usage.UsedFieldsLeft.Distinct().ToList();
            var reads = new MultiJoinColumn[_fields.Count];
            for (int i = 0; i < reads.Length; i++)
            {
                reads[i] = Columns[_fields[i]];
            }
            return reads;
        }
    }

    internal enum MultiJoinCompareOp
    {
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual
    }

    /// <summary>
    /// A condition that compares one column with another.
    /// </summary>
    internal readonly record struct MultiJoinCompare(MultiJoinColumn Left, MultiJoinCompareOp Op, MultiJoinColumn Right);

    /// <summary>
    /// A node of the flattened join tree, a leaf is one input.
    /// </summary>
    internal sealed class MultiJoinNode
    {
        public MultiJoinNode? Parent { get; set; }
        public MultiJoinNode? Left { get; init; }
        public MultiJoinNode? Right { get; init; }

        // Input index for a leaf, -1 for a join
        public int Leaf { get; init; } = -1;

        public bool IsLeaf => Leaf >= 0;

        // Rows of the left side without a match come out with nulls for the right side
        public bool PadRight { get; init; }

        // Rows of the right side without a match come out with nulls for the left side
        public bool PadLeft { get; init; }

        // A condition reads both sides: whether a kept row has a match is decided per row, not per key
        public bool PadPerRow { get; set; }

        // A key of the join with a range comparison was read as a condition
        public bool RangeKey { get; set; }

        // Rows of the left side come out once, with a column telling whether they have a match
        public bool IsMark { get; init; }

        // Index into the marks of the plan, -1 without a mark
        public int MarkId { get; init; } = -1;

        // The key decides alone whether a row has a match
        public bool KeepsUnmatched => PadRight || PadLeft || IsMark;

        public MultiJoinKeySide? LeftKey { get; set; }
        public MultiJoinKeySide? RightKey { get; set; }

        // Inner joins only, joined rows failing one are no rows of the join
        public List<MultiJoinCheck> Checks { get; } = new List<MultiJoinCheck>();

        // Joins that keep unmatched rows only: rows of the kept side failing it are always padded
        public MultiJoinCheck? KeptOnlyCheck { get; set; }

        public List<int> Leaves { get; } = new List<int>();
    }

    internal sealed class MultiJoinInput
    {
        public required Relation Relation { get; init; }
        public required MultiJoinNode Node { get; init; }
        public int ColumnCount => Relation.OutputLength;

        // The distinct column sets this input is looked up by
        public List<int[]> KeyLists { get; } = new List<int[]>();

        // Leading columns of the primary tree, the key lists concatenated without repeats
        public List<int> PrimaryOrder { get; } = new List<int>();

        // Per key list its secondary index, -1 when its columns lead the primary tree
        public List<int> SecondaryOf { get; } = new List<int>();

        // Per secondary index: the columns of its key lists, then the primary columns they lead to and lack
        public List<int[]> SecondaryLayouts { get; } = new List<int[]>();

        // Per secondary index how many leading columns of its layout are key columns
        public List<int> SecondaryKeyLengths { get; } = new List<int>();

        // Leading primary columns known once key list j has been looked up
        public List<int> PrefixLengths { get; } = new List<int>();

        /// <summary>
        /// The key list with these columns in any order, -1 without one.
        /// </summary>
        public int FindKeyList(IReadOnlyList<int> columns)
        {
            for (int i = 0; i < KeyLists.Count; i++)
            {
                if (KeyLists[i].Length == columns.Count && columns.All(KeyLists[i].Contains))
                {
                    return i;
                }
            }
            return -1;
        }

        // Columns copied out when this input is looked up
        public List<int> NeededColumns { get; } = new List<int>();
    }

    /// <summary>
    /// A group of equi joins flattened into inputs and key edges.
    /// </summary>
    internal sealed class MultiJoinPlan
    {
        public List<MultiJoinInput> Inputs { get; } = new List<MultiJoinInput>();

        // A lookup may take a range comparison of its join into its key, which orders the tree by that column too
        public bool RangeKeys { get; set; }

        // A join of the group has a range key, or keeps unmatched rows with a condition on both sides
        public bool PairConditions { get; set; }

        public MultiJoinNode Root { get; set; } = null!;

        public List<MultiJoinColumn> Output { get; set; } = new List<MultiJoinColumn>();

        // The merge joins that became part of the group, the root first
        public List<MergeJoinRelation> Joins { get; } = new List<MergeJoinRelation>();

        public int InnerJoinCount { get; set; }

        // Left, right, full outer and mark joins
        public int LeftJoinCount { get; set; }

        // The mark joins, a mark column names its join by position
        public List<MultiJoinNode> Marks { get; } = new List<MultiJoinNode>();

        // Per join and side whose unmatched rows are kept, the position of its pad table
        public Dictionary<(MultiJoinNode Join, bool KeptLeft), int> PadTableIds { get; } = new Dictionary<(MultiJoinNode Join, bool KeptLeft), int>();

        public List<(MultiJoinNode Join, bool KeptLeft)> PadTables { get; } = new List<(MultiJoinNode Join, bool KeptLeft)>();
    }

    /// <summary>
    /// A merge join that was looked at and stays an ordinary join, with the reason.
    /// </summary>
    internal readonly record struct MultiJoinLeftOut(MergeJoinRelation Join, string Reason);
}
