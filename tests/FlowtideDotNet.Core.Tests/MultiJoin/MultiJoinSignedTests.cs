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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using Xunit.Abstractions;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinSignedTests : OperatorTestBase, IDisposable, IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
        private readonly BufferBlock<IStreamEvent> _outputBlock = new BufferBlock<IStreamEvent>();

        public MultiJoinSignedTests(ITestOutputHelper output)
        {
            _output = output;
        }

        // The shared default directory can still be held by the test before this one
        private readonly string _directory = $"./data/multiJoinSignedTests/{Guid.NewGuid():N}";

        private FileCachePersistentStorage? _storage;

        protected override StateManagerOptions CreateStateManagerOptions()
        {
            _storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/persistent" });
            return new StateManagerOptions()
            {
                PersistentStorage = _storage,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/tmp" }
            };
        }

        private readonly List<MultiJoinOperator> _operators = new List<MultiJoinOperator>();

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var op in _operators)
            {
                await op.DisposeAsync();
            }
        }

        void IDisposable.Dispose()
        {
            Dispose();
            _storage?.Dispose();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
                // A file may still be closing, the directory is under the build output and goes with it
            }
        }

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                    Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(x => (SubstraitBaseType)new AnyType()).ToList() }
                }
            };
        }

        // Keys are field indexes into the left output followed by the right output
        private static MergeJoinRelation LeftJoin(Relation left, Relation right, params (int left, int right)[] keys)
        {
            var join = Join(left, right, keys);
            join.Type = JoinType.Left;
            return join;
        }

        private static MergeJoinRelation Typed(JoinType type, Relation left, Relation right, params (int left, int right)[] keys)
        {
            var join = Join(left, right, keys);
            join.Type = type;
            return join;
        }

        private static MergeJoinRelation Join(Relation left, Relation right, params (int left, int right)[] keys)
        {
            return new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = left,
                Right = right,
                LeftKeys = keys.Select(x => (FieldReference)new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.left } }).ToList(),
                RightKeys = keys.Select(x => (FieldReference)new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.right } }).ToList()
            };
        }

        // a(k, v), b(k, j, v), c(j, v): the second join is keyed from b, so b is looked up by two keys
        private static MergeJoinRelation Chain()
        {
            var ab = Join(Read("a", 2), Read("b", 3), (0, 2));
            return Join(ab, Read("c", 2), (3, 5));
        }

        // f(k1, k2, k3, v) with three inputs each joined on another column of f
        private static MergeJoinRelation Star()
        {
            var j1 = Join(Read("f", 4), Read("d1", 2), (0, 4));
            var j2 = Join(j1, Read("d2", 2), (1, 6));
            return Join(j2, Read("d3", 2), (2, 8));
        }

        // x(a, b, v) and y(a, b, v) joined on two columns, then z(k, v) on y.v
        private static MergeJoinRelation Composite()
        {
            var xy = Join(Read("x", 3), Read("y", 3), (0, 3), (1, 4));
            return Join(xy, Read("z", 2), (5, 6));
        }

        // The chain with both joins left: the second join has its key on the padded side of the first
        private static MergeJoinRelation LeftChain()
        {
            var ab = LeftJoin(Read("a", 2), Read("b", 3), (0, 2));
            return LeftJoin(ab, Read("c", 2), (3, 5));
        }

        // The star with every join left
        private static MergeJoinRelation LeftStar()
        {
            var j1 = LeftJoin(Read("f", 4), Read("d1", 2), (0, 4));
            var j2 = LeftJoin(j1, Read("d2", 2), (1, 6));
            return LeftJoin(j2, Read("d3", 2), (2, 8));
        }

        // A left join below an inner join keyed on the preserved side, or on the padded side
        private static MergeJoinRelation LeftThenInner(bool keyOnPaddedSide)
        {
            var ab = LeftJoin(Read("a", 2), Read("b", 3), (0, 2));
            return keyOnPaddedSide ? Join(ab, Read("c", 2), (3, 5)) : Join(ab, Read("c", 2), (1, 5));
        }

        // An inner join below a left join
        private static MergeJoinRelation InnerThenLeft()
        {
            var ab = Join(Read("a", 2), Read("b", 3), (0, 2));
            return LeftJoin(ab, Read("c", 2), (3, 5));
        }

        // l(k, v) left joined with b(k, j, v) joined with c(j, v): the padded side is a join
        private static MergeJoinRelation LeftWithJoinedRight(bool innerRight)
        {
            var bc = innerRight ? Join(Read("b", 3), Read("c", 2), (1, 3)) : LeftJoin(Read("b", 3), Read("c", 2), (1, 3));
            return LeftJoin(Read("l", 2), bc, (0, 2));
        }

        // l left joined with (b left joined with c), then left joined with d on a column of c
        private static MergeJoinRelation LeftNested()
        {
            var bc = LeftJoin(Read("b", 3), Read("c", 2), (1, 3));
            var lbc = LeftJoin(Read("l", 2), bc, (0, 2));
            return LeftJoin(lbc, Read("d", 2), (6, 7));
        }

        // The left join plans with every left join turned into another type that keeps unmatched rows
        private static MergeJoinRelation Retyped(Relation relation, JoinType type)
        {
            var join = (MergeJoinRelation)relation;
            return new MergeJoinRelation()
            {
                Type = join.Type == JoinType.Left ? type : join.Type,
                Left = join.Left is MergeJoinRelation ? Retyped(join.Left, type) : join.Left,
                Right = join.Right is MergeJoinRelation ? Retyped(join.Right, type) : join.Right,
                LeftKeys = join.LeftKeys,
                RightKeys = join.RightKeys
            };
        }

        // a(k, v), b(k, j, v), c(j, v), d(k, v). A mark join gives its left side and the mark, nothing of its right side.
        private static MergeJoinRelation MarkPlan(string name)
        {
            switch (name)
            {
                case "markRoot":
                    // (a join b) marked by c
                    return Typed(JoinType.LeftMark, Join(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5));
                case "markBelowInner":
                    // (a marked by b) join c on a.v
                    return Join(Typed(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 3));
                case "markJoinedRight":
                    return Typed(JoinType.LeftMark, Read("l", 2), Join(Read("b", 3), Read("c", 2), (1, 3)), (0, 2));
                case "markLeftRight":
                    return Typed(JoinType.LeftMark, Read("l", 2), LeftJoin(Read("b", 3), Read("c", 2), (1, 3)), (0, 2));
                case "markOuterRight":
                    return Typed(JoinType.LeftMark, Read("l", 2), Typed(JoinType.Outer, Read("b", 3), Read("c", 2), (1, 3)), (0, 2));
                case "markUnderLeft":
                    // d left join (a marked by b): the mark is null on a padded row
                    return LeftJoin(Read("d", 2), Typed(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2)), (0, 2));
                case "markUnderOuter":
                    return Typed(JoinType.Outer, Typed(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 3));
                case "markOfMark":
                    // (a marked by b) marked by c
                    return Typed(JoinType.LeftMark, Typed(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 3));
                default:
                    // l marked by (b marked by c)
                    return Typed(JoinType.LeftMark, Read("l", 2), Typed(JoinType.LeftMark, Read("b", 3), Read("c", 2), (1, 3)), (0, 2));
            }
        }

        // Left, right and full outer joins in one tree
        private static MergeJoinRelation MixedPlan(string name)
        {
            switch (name)
            {
                case "rightOverLeft":
                    return Typed(JoinType.Right, LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5));
                case "leftOverRight":
                    return LeftJoin(Typed(JoinType.Right, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5));
                case "outerOverInner":
                    return Typed(JoinType.Outer, Join(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5));
                case "innerOverOuter":
                    return Join(Typed(JoinType.Outer, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5));
                case "outerOverRight":
                    return Typed(JoinType.Outer, Typed(JoinType.Right, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5));
                default:
                    // l right join (b outer join c), then outer join d on a column of c
                    var bc = Typed(JoinType.Outer, Read("b", 3), Read("c", 2), (1, 3));
                    var lbc = Typed(JoinType.Right, Read("l", 2), bc, (0, 2));
                    return Typed(JoinType.Outer, lbc, Read("d", 2), (6, 7));
            }
        }

        // Conditions beside the keys, each compares two fields of the left output followed by the right output
        private static MergeJoinRelation On(MergeJoinRelation join, params (int left, string comparison, int right)[] conditions)
        {
            var compared = conditions.Select(x => (Expression)new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = x.comparison,
                Arguments = new List<Expression>()
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.left } },
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.right } }
                }
            }).ToList();
            join.PostJoinFilter = compared.Count == 1 ? compared[0] : new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.And,
                Arguments = compared
            };
            return join;
        }

        // a(k, v), b(k, j, v), c(j, v), d(k, v), l(k, v). Joins with conditions that are checked after the lookup.
        private static MergeJoinRelation CheckPlan(string name)
        {
            switch (name)
            {
                case "checkChain":
                    // The second join compares c with both inputs below it
                    return On(Join(On(Join(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.LessThan, 4)), Read("c", 2), (3, 5)),
                        (4, FunctionsComparison.GreaterThanOrEqual, 6), (1, FunctionsComparison.NotEqual, 6));
                case "checkEqualOtherInput":
                    // c is keyed from a, a condition compares it with b
                    return On(Join(Join(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5)), (3, FunctionsComparison.Equal, 6));
                case "checkComparedTwice":
                    return Join(Join(Read("a", 2), Read("b", 3), (0, 2), (0, 3)), Read("c", 2), (3, 5));
                case "checkStar":
                    var j1 = On(Join(Read("f", 4), Read("d1", 2), (0, 4)), (3, FunctionsComparison.LessThanOrEqual, 5));
                    var j2 = On(Join(j1, Read("d2", 2), (1, 6)), (5, FunctionsComparison.NotEqual, 7));
                    return Join(j2, Read("d3", 2), (2, 8));
                case "checkUnderLeft":
                    return LeftJoin(Read("l", 2), On(Join(Read("b", 3), Read("c", 2), (1, 3)), (2, FunctionsComparison.GreaterThan, 4)), (0, 2));
                case "checkUnderOuter":
                    return Typed(JoinType.Outer, Read("l", 2), On(Join(Read("b", 3), Read("c", 2), (1, 3)), (2, FunctionsComparison.GreaterThan, 4)), (0, 2));
                case "checkUnderMark":
                    return Typed(JoinType.LeftMark, Read("l", 2), On(Join(Read("b", 3), Read("c", 2), (1, 3)), (2, FunctionsComparison.GreaterThan, 4)), (0, 2));
                case "checkOverLeft":
                    // The condition reads the padded side of the join below
                    return On(Join(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5)), (4, FunctionsComparison.LessThan, 6));
                case "checkOverLeftOnPadded":
                    return On(Join(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5)), (1, FunctionsComparison.LessThanOrEqual, 6));
                case "checkOverOuter":
                    return On(Join(Typed(JoinType.Outer, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5)), (4, FunctionsComparison.LessThan, 6));
                case "checkDistinctOverLeft":
                    // The condition holds on a padded row and fails on some matched ones
                    return On(Join(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5)), (4, FunctionsComparison.IsDistinctFrom, 6));
                case "checkDistinctOverOuter":
                    return On(Join(Typed(JoinType.Outer, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5)), (4, FunctionsComparison.IsDistinctFrom, 6));
                case "checkDistinctOverJoinedLeft":
                    // The left join below is the right side of the join with the condition
                    return On(Join(Read("c", 2), LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (0, 3)), (1, FunctionsComparison.IsDistinctFrom, 6));
                case "checkOverMark":
                    // (a marked by b) join c on a.v, with a condition on a and c
                    return On(Join(Typed(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 3)), (0, FunctionsComparison.LessThanOrEqual, 4));
                case "leftOnly":
                    // Rows of a failing the condition are padded whatever b holds
                    return Join(On(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.GreaterThan, 0)), Read("c", 2), (1, 5));
                case "leftOnlyOnPadded":
                    return LeftJoin(On(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.GreaterThan, 0)), Read("c", 2), (3, 5));
                case "leftOnlyJoinedLeft":
                    // The condition compares the two inputs of the kept side
                    return On(LeftJoin(Join(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5)), (1, FunctionsComparison.LessThan, 4));
                case "leftOnlyJoinedRight":
                    return On(LeftJoin(Read("l", 2), Join(Read("b", 3), Read("c", 2), (1, 3)), (0, 2)), (1, FunctionsComparison.GreaterThan, 0));
                case "rightOnly":
                    // Rows of b failing the condition are padded whatever a holds
                    return Join(On(Typed(JoinType.Right, Read("a", 2), Read("b", 3), (0, 2)), (4, FunctionsComparison.GreaterThan, 3)), Read("c", 2), (3, 5));
                case "rightOnlyOnPadded":
                    return LeftJoin(On(Typed(JoinType.Right, Read("a", 2), Read("b", 3), (0, 2)), (4, FunctionsComparison.GreaterThan, 3)), Read("c", 2), (1, 5));
                case "rightOnlyJoinedRight":
                    // The condition compares the two inputs of the kept side
                    return On(Typed(JoinType.Right, Read("l", 2), Join(Read("b", 3), Read("c", 2), (1, 3)), (0, 2)), (4, FunctionsComparison.LessThan, 6));
                case "rightOnlyJoinedLeft":
                    return On(Typed(JoinType.Right, Join(Read("b", 3), Read("c", 2), (1, 3)), Read("l", 2), (0, 5)), (6, FunctionsComparison.GreaterThan, 5));
                default:
                    // d left join (a left join b with a condition on a)
                    return LeftJoin(Read("d", 2), On(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.GreaterThan, 0)), (0, 2));
            }
        }

        // Works a condition out from its expression, apart from the compiled one the operator runs
        private static bool Holds(MultiJoinCheck check, object?[]?[] rows)
        {
            return Holds(check.Condition, check.Columns, rows);
        }

        private static bool Holds(Expression expression, IReadOnlyList<MultiJoinColumn> columns, object?[]?[] rows)
        {
            var function = (ScalarFunction)expression;
            if (function.ExtensionName == FunctionsBoolean.And)
            {
                return function.Arguments.All(x => Holds(x, columns, rows));
            }
            var values = function.Arguments.Select(x =>
            {
                var column = columns[((StructReferenceSegment)((DirectFieldReference)x).ReferenceSegment).Field];
                return (long?)rows[column.Input]?[column.Column];
            }).ToArray();
            if (function.ExtensionName == FunctionsComparison.IsDistinctFrom)
            {
                return values[0] != values[1];
            }
            if (values[0] == null || values[1] == null)
            {
                // A comparison with null is not true
                return false;
            }
            long compared = values[0]!.Value.CompareTo(values[1]!.Value);
            return function.ExtensionName switch
            {
                FunctionsComparison.Equal => compared == 0,
                FunctionsComparison.NotEqual => compared != 0,
                FunctionsComparison.LessThan => compared < 0,
                FunctionsComparison.LessThanOrEqual => compared <= 0,
                FunctionsComparison.GreaterThan => compared > 0,
                FunctionsComparison.GreaterThanOrEqual => compared >= 0,
                _ => throw new NotSupportedException(function.ExtensionName)
            };
        }

        private sealed class ZSet
        {
            public readonly Dictionary<string, (object?[] row, long weight)> Rows = new Dictionary<string, (object?[] row, long weight)>();

            public void Add(object?[] row, long weight)
            {
                var key = Key(row);
                Rows.TryGetValue(key, out var existing);
                long sum = existing.weight + weight;
                if (sum == 0)
                {
                    Rows.Remove(key);
                }
                else
                {
                    Rows[key] = (row, sum);
                }
            }
        }

        private static string Key(IEnumerable<object?> values)
        {
            return string.Join("|", values.Select(x => x == null ? "NULL" : x.ToString()));
        }

        // The join of everything sent so far, by nested loops over the join tree A joined row has a slot per input and, after them, one per mark join holding its mark
        private static List<(object?[]?[] rows, long weight)> Evaluate(MultiJoinNode node, ZSet[] inputs, int slots)
        {
            var result = new List<(object?[]?[] rows, long weight)>();
            if (node.IsLeaf)
            {
                foreach (var entry in inputs[node.Leaf].Rows.Values)
                {
                    var rows = new object?[]?[slots];
                    rows[node.Leaf] = entry.row;
                    result.Add((rows, entry.weight));
                }
                return result;
            }
            var left = Evaluate(node.Left!, inputs, slots);
            var right = Evaluate(node.Right!, inputs, slots);
            var matchedRight = new long[right.Count];
            foreach (var l in left)
            {
                long matched = 0;
                for (int ri = 0; ri < right.Count; ri++)
                {
                    var r = right[ri];
                    bool match = true;
                    for (int i = 0; i < node.LeftKey!.Columns.Length && match; i++)
                    {
                        // A padded side has no row for its inputs, its key is null
                        var leftValue = l.rows[node.LeftKey.Input]?[node.LeftKey.Columns[i]];
                        var rightValue = r.rows[node.RightKey!.Input]?[node.RightKey.Columns[i]];
                        // A null key matches nothing
                        match = leftValue != null && rightValue != null && leftValue.Equals(rightValue);
                    }
                    if (!match)
                    {
                        continue;
                    }
                    var rows = new object?[]?[slots];
                    for (int i = 0; i < rows.Length; i++)
                    {
                        rows[i] = l.rows[i] ?? r.rows[i];
                    }
                    if (!node.Checks.All(x => Holds(x, rows)))
                    {
                        continue;
                    }
                    // A condition on the kept side is one more condition of the join
                    if (node.KeptOnlyCheck != null && !Holds(node.KeptOnlyCheck, rows))
                    {
                        continue;
                    }
                    matched += r.weight;
                    matchedRight[ri] += l.weight;
                    if (node.IsMark)
                    {
                        continue;
                    }
                    result.Add((rows, l.weight * r.weight));
                }
                if (node.IsMark)
                {
                    var rows = (object?[]?[])l.rows.Clone();
                    rows[inputs.Length + node.MarkId] = new object?[] { matched != 0 };
                    result.Add((rows, l.weight));
                }
                else if (node.PadRight && matched == 0)
                {
                    // Padded when the weights of what it matches add up to nothing
                    result.Add((l.rows, l.weight));
                }
            }
            for (int ri = 0; node.PadLeft && ri < right.Count; ri++)
            {
                if (matchedRight[ri] == 0)
                {
                    result.Add((right[ri].rows, right[ri].weight));
                }
            }
            return result;
        }

        private static Dictionary<string, long> Expected(MultiJoinPlan plan, ZSet[] inputs)
        {
            var expected = new Dictionary<string, long>();
            foreach (var (rows, weight) in Evaluate(plan.Root, inputs, inputs.Length + plan.Marks.Count))
            {
                var key = Key(plan.Output.Select(x => x.IsMark ? rows[inputs.Length + x.Column]?[0] : rows[x.Input]?[x.Column]));
                expected.TryGetValue(key, out var sum);
                expected[key] = sum + weight;
            }
            foreach (var key in expected.Where(x => x.Value == 0).Select(x => x.Key).ToList())
            {
                expected.Remove(key);
            }
            return expected;
        }

        private static StreamMessage<StreamEventBatch> CreateMessage(List<(object?[] row, int weight)> rows, int columnCount)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var column = Column.Create(allocator);
                foreach (var (row, _) in rows)
                {
                    switch (row[c])
                    {
                        case null:
                            column.Add(NullValue.Instance);
                            break;
                        case long number:
                            column.Add(new Int64Value(number));
                            break;
                        default:
                            column.Add(new StringValue((string)row[c]!));
                            break;
                    }
                }
                columns[c] = column;
            }
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var (_, weight) in rows)
            {
                weights.Add(weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            // The sender holds one rent per receiver, the operator returns it
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static string Text(IColumn column, int index)
        {
            var value = column.GetValueAt(index, default);
            if (value.IsNull)
            {
                return "NULL";
            }
            if (value.Type == ArrowTypeId.Boolean)
            {
                return value.AsBool.ToString();
            }
            return value.Type == ArrowTypeId.Int64 ? value.AsLong.ToString() : value.AsString.ToString();
        }

        private async Task DrainUntilCheckpoint(MultiJoinOperator op, Dictionary<string, long> actual, long time)
        {
            foreach (var target in op.Targets)
            {
                await target.SendAsync(new Checkpoint(time, time + 1, 1));
            }
            while (true)
            {
                IStreamEvent received;
                try
                {
                    var receive = _outputBlock.ReceiveAsync(TimeSpan.FromSeconds(30));
                    if (await Task.WhenAny(receive, op.Completion) != receive)
                    {
                        // A failed operator fails the case at once
                        await op.Completion;
                        throw new InvalidOperationException("The operator completed before its checkpoint came out");
                    }
                    received = await receive;
                }
                catch (TimeoutException)
                {
                    // An operator that failed says why
                    throw new TimeoutException($"No output for 30 seconds, operator is {op.Completion.Status}: {op.Completion.Exception}");
                }
                if (received is ICheckpointEvent)
                {
                    return;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    for (int i = 0; i < data.Count; i++)
                    {
                        var key = string.Join("|", Enumerable.Range(0, data.EventBatchData.Columns.Count).Select(c => Text(data.EventBatchData.Columns[c], i)));
                        // A row without weight says nothing and must not be sent on
                        Assert.NotEqual(0, data.Weights.Get(i));
                        actual.TryGetValue(key, out var sum);
                        sum += data.Weights.Get(i);
                        if (sum == 0)
                        {
                            actual.Remove(key);
                        }
                        else
                        {
                            actual[key] = sum;
                        }
                    }
                    data.Return();
                }
            }
        }

        private static void AssertSame(Dictionary<string, long> expected, Dictionary<string, long> actual, string step)
        {
            var differences = new List<string>();
            foreach (var key in expected.Keys.Union(actual.Keys))
            {
                expected.TryGetValue(key, out var e);
                actual.TryGetValue(key, out var a);
                if (e != a)
                {
                    differences.Add($"{key}: expected {e}, got {a}");
                }
            }
            Assert.True(differences.Count == 0, $"After {step}: {differences.Count} rows differ, first ones: {string.Join("; ", differences.Take(5))}");
        }

        private async Task Run(MergeJoinRelation root, int seed, int rowBudget, int keyDomain, bool pairConditions = false)
        {
            var plan = MultiJoinPlanBuilder.Discover(root, 9, null, pairConditions);
            Assert.NotNull(plan);
            int budgetBefore = MultiJoinOperator.RowBudget;
            MultiJoinOperator.RowBudget = rowBudget;
            try
            {
                var op = new MultiJoinOperator(plan, FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
                _operators.Add(op);
                op.LinkTo(_outputBlock);
                await InitializeOperator(op);

                var random = new Random(seed);
                var inputs = plan.Inputs.Select(x => new ZSet()).ToArray();
                var actual = new Dictionary<string, long>();
                // What was sent with a positive weight, so retractions can hit rows that exist
                var sent = plan.Inputs.Select(x => new List<object?[]>()).ToArray();

                for (int round = 1; round <= 12; round++)
                {
                    int batches = random.Next(1, 5);
                    for (int b = 0; b < batches; b++)
                    {
                        int input = random.Next(plan.Inputs.Count);
                        int columnCount = plan.Inputs[input].ColumnCount;
                        var rows = new List<(object?[] row, int weight)>();
                        int rowCount = random.Next(1, 25);
                        for (int r = 0; r < rowCount; r++)
                        {
                            object?[] row;
                            int weight;
                            int kind = random.Next(10);
                            if (kind < 3 && sent[input].Count > 0)
                            {
                                // A row that exists, retracted, repeated or sent without weight
                                row = sent[input][random.Next(sent[input].Count)];
                                weight = random.Next(-2, 2);
                            }
                            else
                            {
                                row = new object?[columnCount];
                                for (int c = 0; c < columnCount; c++)
                                {
                                    // Small domains so rows meet, with a null now and then
                                    row[c] = random.Next(12) == 0 ? null : (object)(long)random.Next(keyDomain);
                                }
                                // Mostly inserts, sometimes a retraction of a row that was never sent
                                weight = kind == 9 ? -1 : random.Next(1, 4);
                                sent[input].Add(row);
                            }
                            rows.Add((row, weight));
                            inputs[input].Add(row, weight);
                        }
                        await op.Targets[input].SendAsync(CreateMessage(rows, columnCount));
                    }
                    await DrainUntilCheckpoint(op, actual, round);
                    AssertSame(Expected(plan, inputs), actual, $"round {round} of seed {seed}");
                }
                _output.WriteLine($"seed {seed}: {actual.Count} distinct output rows at the end");
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
            }
        }

        private async Task<MultiJoinOperator> RunScript(MergeJoinRelation root, params (int input, (object?[] row, int weight)[] rows)[] batches)
        {
            var plan = MultiJoinPlanBuilder.Discover(root, 9);
            Assert.NotNull(plan);
            var op = new MultiJoinOperator(plan, FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            _operators.Add(op);
            op.LinkTo(_outputBlock);
            await InitializeOperator(op);
            var inputs = plan.Inputs.Select(x => new ZSet()).ToArray();
            var actual = new Dictionary<string, long>();
            for (int b = 0; b < batches.Length; b++)
            {
                var (input, rows) = batches[b];
                foreach (var (row, weight) in rows)
                {
                    inputs[input].Add(row, weight);
                }
                await op.Targets[input].SendAsync(CreateMessage(rows.ToList(), plan.Inputs[input].ColumnCount));
                await DrainUntilCheckpoint(op, actual, b + 1);
                AssertSame(Expected(plan, inputs), actual, $"batch {b + 1}");
            }
            return op;
        }

        private static async Task WithRowBudget(int rowBudget, Func<Task> run)
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            MultiJoinOperator.RowBudget = rowBudget;
            try
            {
                await run();
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
            }
        }

        /// <summary>
        /// The rows a left row matches have weights that add up to nothing: both go out, and the padded row too.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task LeftRowWhoseMatchesCancelIsPadded(int rowBudget)
        {
            var ab = LeftJoin(Read("a", 2), Read("b", 3), (0, 2));
            var root = Join(ab, Read("c", 2), (0, 5));
            await WithRowBudget(rowBudget, () => RunScript(root,
                (1, new[] { Row(1, 1, 0, 0) }),
                (1, new[] { Row(-1, 1, 0, 1) }),
                (2, new[] { Row(1, 1, 0) }),
                (0, new[] { Row(1, 1, 0) })));
        }

        /// <summary>
        /// One row arriving on the right side completes two joined rows there. The padded row leaves once, not twice.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task PaddedRowLeavesOnceWhenTwoMatchesAppear(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(LeftWithJoinedRight(true),
                (0, new[] { Row(1, 1, 0) }),
                (1, new[] { Row(1, 1, 1, 0), Row(1, 1, 1, 1) }),
                (2, new[] { Row(1, 1, 0) })));
        }

        /// <summary>
        /// A change on the right side reaches the left join under many keys, one of them has a left row.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task OnlyKeysWithALeftRowAreRemembered(int rowBudget)
        {
            var many = Enumerable.Range(1, 300).Select(x => Row(1, x, 1, 0)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(LeftWithJoinedRight(true),
                    (0, new[] { Row(1, 1, 0) }),
                    (1, many),
                    (2, new[] { Row(1, 1, 0) }));
                // The table of the outer join, the one of b with c does not exist: that join is inner
                Assert.Equal(1, op.PadTables.Single().KeysAdded);
            });
        }

        // Correction keys require a complete preserved-side row.
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task KeysWithoutACompletePreservedRowAreNotRemembered(int rowBudget)
        {
            var ab = Join(Read("a", 2), Read("b", 2), (1, 2));
            var root = LeftJoin(ab, Read("c", 2), (0, 4));
            var many = Enumerable.Range(1, 300).Select(x => Row(1, x, 1)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(root,
                    (0, many),
                    (2, many));
                Assert.Equal(0, op.PadTables.Single().KeysAdded);
            });
        }

        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task KeysWhosePreservedRowsFailTheirConditionAreNotRemembered(int rowBudget)
        {
            var ab = On(Join(Read("a", 2), Read("b", 2), (1, 2)),
                (1, FunctionsComparison.GreaterThan, 2));
            var root = LeftJoin(ab, Read("c", 2), (0, 4));
            var many = Enumerable.Range(1, 300).Select(x => Row(1, x, 1)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(root,
                    (0, many),
                    (1, new[] { Row(1, 1, 0) }),
                    (2, many));
                Assert.Equal(0, op.PadTables.Single().KeysAdded);
            });
        }

        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task KeysWhosePreservedRowsFailTheKeptOnlyConditionAreNotRemembered(int rowBudget)
        {
            var ab = Join(Read("a", 3), Read("b", 2), (1, 3));
            var root = On(LeftJoin(ab, Read("c", 2), (0, 5)),
                (2, FunctionsComparison.GreaterThan, 1));
            var left = Enumerable.Range(1, 1025).Select(x => Row(1, x, 1, 0)).ToArray();
            var right = Enumerable.Range(1, 1025).Select(x => Row(1, x, 0)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(root,
                    (0, left),
                    (1, new[] { Row(1, 1, 0) }),
                    (2, right));
                Assert.Equal(0, op.PadTables.Single().KeysAdded);
            });
        }

        /// <summary>
        /// A left row arrives and matches very many joined rows of the right side.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(16)]
        [InlineData(1)]
        public async Task LeftRowMatchingManyJoinedRows(int rowBudget)
        {
            var b = Enumerable.Range(0, 120).Select(x => Row(1, 1, 1, x)).ToArray();
            var c = Enumerable.Range(0, 120).Select(x => Row(1, 1, x)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(LeftWithJoinedRight(true),
                    (1, b),
                    (2, c),
                    (0, new[] { Row(1, 1, 0) }));
                Assert.InRange(op.MaxPartRows, 1, rowBudget);
            });
        }

        /// <summary>
        /// Both rows of one input of the right side go in one batch.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task PaddedRowReturnsOnceWhenAllMatchesLeave(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(LeftWithJoinedRight(true),
                (0, new[] { Row(1, 1, 0) }),
                (1, new[] { Row(2, 1, 1, 0) }),
                (2, new[] { Row(2, 1, 0) }),
                (2, new[] { Row(-2, 1, 0) })));
        }

        /// <summary>
        /// A padded row made by a left join on the right side counts as a match for the left join around it, and the change that pads it travels on through that join.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task PaddingInsideThePaddedSide(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(LeftNested(),
                (0, new[] { Row(1, 1, 0) }),
                (1, new[] { Row(1, 1, 5, 0) }),
                (3, new[] { Row(1, 7, 0) }),
                (2, new[] { Row(1, 5, 7) }),
                (2, new[] { Row(-1, 5, 7) }),
                (1, new[] { Row(-1, 1, 5, 0) }),
                (0, new[] { Row(-1, 1, 0) })));
        }

        private static (object?[] row, int weight) Row(int weight, params long[] values)
        {
            return (values.Select(x => (object?)x).ToArray(), weight);
        }

        /// <summary>
        /// Every row of a batch changes a stored row, some create one and some remove one, and the batch is not in tree order.
        /// </summary>
        [Fact]
        public async Task SecondKeyFollowsRowsThatAllChangeInOneBatch()
        {
            await RunScript(Chain(),
                (1, new[] { Row(1, 1, 1, 0) }),
                (1, new[] { Row(1, 2, 2, 0), Row(-1, 1, 1, 0) }),
                (1, new[] { Row(1, 2, 2, 1) }),
                (0, new[] { Row(1, 1, 0), Row(1, 2, 0) }),
                (2, new[] { Row(1, 1, 0), Row(1, 2, 0) }));
        }

        /// <summary>
        /// The rows found for one key fill several leaves of the tree, through the first key of b and through its second.
        /// </summary>
        [Fact]
        public async Task LookupOverManyLeaves()
        {
            const int rows = 6000;
            int budgetBefore = MultiJoinOperator.RowBudget;
            MultiJoinOperator.RowBudget = 1;
            try
            {
                var many = Enumerable.Range(0, rows).Select(x => Row(1, 1, 1, x)).ToArray();
                await RunScript(Chain(),
                    (1, many),
                    (2, new[] { Row(1, 1, 0) }),
                    (0, new[] { Row(1, 1, 0) }),
                    (2, new[] { Row(1, 1, 1) }),
                    (0, new[] { Row(-1, 1, 0) }));
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
            }
        }

        public static IEnumerable<object[]> LeftPlans()
        {
            int count = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var seeds) ? seeds : 5;
            foreach (var plan in new[] { "chain", "star", "leftThenInner", "leftThenInnerOnPadded", "innerThenLeft", "joinedRight", "leftRight", "nested" })
            {
                foreach (var rowBudget in new[] { MultiJoinOperator.DefaultRowBudget, 1, ThroughTheTree, Deferred })
                {
                    for (int seed = 1; seed <= (rowBudget < 0 ? 2 : count); seed++)
                    {
                        yield return new object[] { plan, rowBudget, seed };
                    }
                }
            }
        }

        private static MergeJoinRelation LeftPlan(string name)
        {
            return name switch
            {
                "chain" => LeftChain(),
                "star" => LeftStar(),
                "leftThenInner" => LeftThenInner(false),
                "leftThenInnerOnPadded" => LeftThenInner(true),
                "innerThenLeft" => InnerThenLeft(),
                "joinedRight" => LeftWithJoinedRight(true),
                "leftRight" => LeftWithJoinedRight(false),
                _ => LeftNested()
            };
        }

        // In place of a row budget: every pad table key through the tree, or every batch held back until the checkpoint
        private const int ThroughTheTree = -1;
        private const int Deferred = -2;

        private async Task RunWithPadKeys(MergeJoinRelation root, int seed, int rowBudget, int rows)
        {
            if (rowBudget > 0)
            {
                await Run(root, seed, rowBudget, rows);
                return;
            }
            int before = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = rowBudget == ThroughTheTree ? 0 : before;
            MultiJoinOperator.DeferEveryBatch = rowBudget == Deferred;
            try
            {
                await Run(root, seed, MultiJoinOperator.DefaultRowBudget, rows);
            }
            finally
            {
                MultiJoinPadTable.MemoryKeys = before;
                MultiJoinOperator.DeferEveryBatch = false;
            }
        }

        [Theory]
        [MemberData(nameof(LeftPlans))]
        public async Task LeftJoins(string plan, int rowBudget, int seed)
        {
            await RunWithPadKeys(LeftPlan(plan), seed, rowBudget, plan == "star" ? 3 : 4);
        }

        /// <summary>
        /// One row arriving on the right side of a mark join completes two joined rows there.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task MarkTurnsOnceWhenTwoMatchesAppear(int rowBudget)
        {
            var many = Enumerable.Range(2, 200).Select(x => Row(1, x, 1, 0)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(MarkPlan("markJoinedRight"),
                    (0, new[] { Row(1, 1, 0) }),
                    (1, new[] { Row(1, 1, 1, 0), Row(1, 1, 1, 1) }),
                    (1, many),
                    (2, new[] { Row(1, 1, 0) }),
                    (2, new[] { Row(-1, 1, 0) }));
                // Each change of c reaches 202 rows of b, the two with the key of the left row are remembered
                Assert.Equal(4, op.PadTables.Single().KeysAdded);
            });
        }

        /// <summary>
        /// The rows a left row matches have weights that add up to nothing: its mark is not set.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task MarkOfRowWhoseMatchesCancel(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(MarkPlan("markRoot"),
                (2, new[] { Row(1, 1, 0) }),
                (2, new[] { Row(-1, 1, 1) }),
                (1, new[] { Row(1, 1, 1, 0) }),
                (0, new[] { Row(1, 1, 0) }),
                (2, new[] { Row(1, 1, 1) }),
                (2, new[] { Row(1, 1, 1) })));
        }

        /// <summary>
        /// A row of b turns the mark of a under a left join: no pad table keeps rows after the message.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task NoPadTableKeepsRowsAfterAMarkTurns(int rowBudget)
        {
            await WithRowBudget(rowBudget, async () =>
            {
                // d left join (a marked by b), inputs d = 0, a = 1, b = 2
                var op = await RunScript(MarkPlan("markUnderLeft"),
                    (0, new[] { Row(1, 1, 10) }),
                    (1, new[] { Row(1, 1, 20) }),
                    (2, new[] { Row(1, 1, 5, 7) }));
                Assert.All(op.PadTables, x => Assert.False(x.HasRows));
            });
        }

        /// <summary>
        /// A full outer join.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task OuterJoinPadsEitherSide(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(MixedPlan("outerOverInner"),
                (2, new[] { Row(1, 1, 0), Row(1, 2, 0) }),
                (0, new[] { Row(1, 1, 0) }),
                (1, new[] { Row(1, 1, 1, 0), Row(1, 1, 1, 1), Row(1, 1, 3, 0) }),
                (2, new[] { Row(1, 3, 0) }),
                (0, new[] { Row(-1, 1, 0) }),
                (2, new[] { Row(-1, 1, 0), Row(-1, 3, 0) })));
        }

        /// <summary>
        /// A right join keeps the rows of its right side: one loses its only match and is padded, then leaves.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task RightRowLosesItsLastMatch(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(Retyped(LeftChain(), JoinType.Right),
                (2, new[] { Row(1, 1, 0) }),
                (1, new[] { Row(1, 1, 1, 0) }),
                (0, new[] { Row(1, 1, 0) }),
                (1, new[] { Row(-1, 1, 1, 0) }),
                (2, new[] { Row(-1, 1, 0) })));
        }

        public static IEnumerable<object[]> OtherPlans()
        {
            int count = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var seeds) ? seeds : 5;
            var leftPlans = new[] { "chain", "star", "leftThenInner", "leftThenInnerOnPadded", "innerThenLeft", "joinedRight", "leftRight", "nested" };
            var plans = leftPlans.Select(x => "right:" + x)
                .Concat(leftPlans.Select(x => "outer:" + x))
                .Concat(new[] { "rightOverLeft", "leftOverRight", "outerOverInner", "innerOverOuter", "outerOverRight", "mixedNested" })
                .Concat(new[] { "markRoot", "markBelowInner", "markJoinedRight", "markLeftRight", "markOuterRight", "markUnderLeft", "markUnderOuter", "markOfMark", "markInMark" });
            foreach (var plan in plans)
            {
                foreach (var rowBudget in new[] { MultiJoinOperator.DefaultRowBudget, 1, ThroughTheTree, Deferred })
                {
                    for (int seed = 1; seed <= (rowBudget < 0 ? 2 : count); seed++)
                    {
                        yield return new object[] { plan, rowBudget, seed };
                    }
                }
            }
        }

        [Theory]
        [MemberData(nameof(OtherPlans))]
        public async Task RightOuterAndMarkJoins(string plan, int rowBudget, int seed)
        {
            MergeJoinRelation root;
            if (plan.StartsWith("right:"))
            {
                root = Retyped(LeftPlan(plan.Substring(6)), JoinType.Right);
            }
            else if (plan.StartsWith("outer:"))
            {
                root = Retyped(LeftPlan(plan.Substring(6)), JoinType.Outer);
            }
            else if (plan.StartsWith("mark"))
            {
                root = MarkPlan(plan);
            }
            else
            {
                root = MixedPlan(plan);
            }
            await RunWithPadKeys(root, seed, rowBudget, plan.EndsWith("star") ? 3 : 4);
        }

        public static IEnumerable<object[]> CheckPlans()
        {
            int count = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var seeds) ? seeds : 5;
            var plans = new[]
            {
                "checkChain", "checkEqualOtherInput", "checkComparedTwice", "checkStar", "checkUnderLeft", "checkUnderOuter", "checkUnderMark",
                "checkOverLeft", "checkOverLeftOnPadded", "checkOverOuter", "checkOverMark",
                "checkDistinctOverLeft", "checkDistinctOverOuter", "checkDistinctOverJoinedLeft",
                "leftOnly", "leftOnlyOnPadded", "leftOnlyJoinedLeft", "leftOnlyJoinedRight", "leftOnlyUnderLeft",
                "rightOnly", "rightOnlyOnPadded", "rightOnlyJoinedRight", "rightOnlyJoinedLeft"
            };
            foreach (var plan in plans)
            {
                foreach (var rowBudget in new[] { MultiJoinOperator.DefaultRowBudget, 1 })
                {
                    for (int seed = 1; seed <= count; seed++)
                    {
                        yield return new object[] { plan, rowBudget, seed };
                    }
                }
            }
        }

        [Theory]
        [MemberData(nameof(CheckPlans))]
        public async Task ConditionsCheckedAfterTheLookup(string plan, int rowBudget, int seed)
        {
            var root = CheckPlan(plan);
            // Every join is part of the group, none became an input with its condition inside
            var leftOut = new List<MultiJoinLeftOut>();
            var discovered = MultiJoinPlanBuilder.Discover(root, 9, leftOut);
            Assert.NotNull(discovered);
            Assert.Empty(leftOut);
            Assert.Equal(plan == "checkStar" ? 4 : 3, discovered.Inputs.Count);
            await Run(root, seed, rowBudget, plan == "checkStar" ? 3 : 4);
        }

        [Fact]
        public void KeyFromSeveralInputsLeavesTheJoinOut()
        {
            // c is keyed from a and from b
            var root = Join(Join(Join(Read("a", 2), Read("d", 2), (0, 2)), Read("b", 3), (0, 4)), Read("c", 2), (1, 7), (5, 8));
            var leftOut = new List<MultiJoinLeftOut>();
            var plan = MultiJoinPlanBuilder.Discover(root, 9, leftOut);
            Assert.Null(plan);
            Assert.Contains(leftOut, x => x.Join == root && x.Reason == "key from several inputs");
            // The joins below it group on their own
            var below = MultiJoinPlanBuilder.Discover((MergeJoinRelation)root.Left, 9);
            Assert.NotNull(below);
            Assert.Equal(3, below.Inputs.Count);
        }

        /// <summary>
        /// The joined right side holds a pair with the key of the left row that fails its condition.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task PairFailingItsConditionIsNoMatch(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(CheckPlan("checkUnderLeft"),
                (1, new[] { Row(1, 1, 1, 1) }),
                (2, new[] { Row(1, 1, 5) }),
                (0, new[] { Row(1, 1, 0) }),
                (2, new[] { Row(1, 1, 0) }),
                (2, new[] { Row(-1, 1, 0) })));
        }

        /// <summary>
        /// Two left rows with one key in one batch, one passes the condition on the left side and one fails it.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task LeftRowFailingItsConditionStaysPadded(int rowBudget)
        {
            await WithRowBudget(rowBudget, () => RunScript(CheckPlan("leftOnly"),
                (2, new[] { Row(1, 2, 0), Row(1, 0, 0) }),
                (1, new[] { Row(1, 1, 1, 0) }),
                (0, new[] { Row(1, 1, 2), Row(1, 1, 0) }),
                (1, new[] { Row(-1, 1, 1, 0) }),
                (1, new[] { Row(1, 1, 1, 0) })));
        }

        /// <summary>
        /// A key shared with one input matches many rows, the rest of the key is a condition that one of them passes.
        /// </summary>
        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(16)]
        [InlineData(1)]
        public async Task KeyPartMatchingManyRows(int rowBudget)
        {
            var many = Enumerable.Range(0, 300).Select(x => Row(1, 7, x)).ToArray();
            await WithRowBudget(rowBudget, async () =>
            {
                var op = await RunScript(CheckPlan("checkEqualOtherInput"),
                    (2, many),
                    (1, new[] { Row(1, 1, 5, 0), Row(1, 1, 250, 0) }),
                    (0, new[] { Row(1, 1, 7) }),
                    (0, new[] { Row(-1, 1, 7) }));
                Assert.InRange(op.MaxPartRows, 1, rowBudget);
            });
        }

        // a(k, v), b(k, j, v), c(j, v). A condition reads both sides of a join that keeps unmatched rows
        private static MergeJoinRelation PairPlan(string name)
        {
            switch (name)
            {
                case "pairLeft":
                    // The join above is keyed on the padded side
                    return LeftJoin(On(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.GreaterThanOrEqual, 4)), Read("c", 2), (3, 5));
                case "pairLeftTwice":
                    return LeftJoin(On(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.GreaterThanOrEqual, 4), (1, FunctionsComparison.NotEqual, 3)), Read("c", 2), (3, 5));
                case "pairLeftUnderInner":
                    // The join above is keyed on the kept side
                    return Join(On(LeftJoin(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.LessThan, 4)), Read("c", 2), (0, 5));
                case "pairRight":
                    // b is filled with nulls, a is kept
                    return Join(On(Typed(JoinType.Right, Read("b", 3), Read("a", 2), (0, 3)), (2, FunctionsComparison.NotEqual, 4)), Read("c", 2), (3, 5));
                case "pairJoinedKept":
                    // The kept side is a join, the key is on a and the condition reads c
                    return On(LeftJoin(Join(Read("a", 2), Read("c", 2), (0, 2)), Read("b", 3), (0, 4)), (3, FunctionsComparison.GreaterThanOrEqual, 6));
                case "pairOverLeft":
                    // The kept side holds padded rows of the join below
                    return On(LeftJoin(LeftJoin(Read("a", 2), Read("c", 2), (0, 2)), Read("b", 3), (0, 4)), (3, FunctionsComparison.LessThan, 6));
                default:
                    throw new ArgumentException(name);
            }
        }

        public static IEnumerable<object[]> PairPlans()
        {
            int count = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var seeds) ? seeds : 5;
            foreach (var plan in new[] { "pairLeft", "pairLeftTwice", "pairLeftUnderInner", "pairRight", "pairJoinedKept", "pairOverLeft" })
            {
                foreach (var rowBudget in new[] { MultiJoinOperator.DefaultRowBudget, 1 })
                {
                    for (int seed = 1; seed <= count; seed++)
                    {
                        // Half the seeds keep no entry of a pad table in memory
                        yield return new object[] { plan, rowBudget, seed, seed % 2 == 0 ? 0 : MultiJoinPadTable.MemoryKeys };
                    }
                }
            }
        }

        [Theory]
        [MemberData(nameof(PairPlans))]
        public async Task ConditionsOnBothSidesOfAJoinThatKeepsUnmatchedRows(string plan, int rowBudget, int seed, int memoryKeys)
        {
            var root = PairPlan(plan);
            var leftOut = new List<MultiJoinLeftOut>();
            Assert.Null(MultiJoinPlanBuilder.Discover(PairPlan(plan), 9));
            var discovered = MultiJoinPlanBuilder.Discover(root, 9, leftOut, pairConditions: true);
            Assert.NotNull(discovered);
            Assert.Empty(leftOut);
            Assert.Equal(3, discovered.Inputs.Count);
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            bool rangeKeysBefore = MultiJoinPlanBuilder.RangeKeys;
            MultiJoinPadTable.MemoryKeys = memoryKeys;
            // Seeds 1, 2 and 5 take the range into the key as a stream does, the others run without it
            MultiJoinPlanBuilder.RangeKeys = !(seed % 3 == 0 || seed == 4);
            long rangeLookupsBefore = Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests);
            try
            {
                await Run(root, seed, rowBudget, 4, pairConditions: true);
                if (MultiJoinPlanBuilder.RangeKeys && plan.StartsWith("pairLeft", StringComparison.Ordinal))
                {
                    // In these plans the bound is on a column of the input that holds the key, so a lookup can take it
                    Assert.True(Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests) > rangeLookupsBefore, "No lookup took the range into its key");
                }
            }
            finally
            {
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
                MultiJoinPlanBuilder.RangeKeys = rangeKeysBefore;
            }
        }

        [Fact]
        public void AComparisonOfValuesInsideColumnsIsNotCheckedBeforeTheCopy()
        {
            Expression Inside(int field) => new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment() { Field = field, Child = new StructReferenceSegment() { Field = 0 } }
            };
            Expression Whole(int field) => new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = field } };
            var columns = new List<MultiJoinColumn>() { new MultiJoinColumn(0, 0), new MultiJoinColumn(0, 1), new MultiJoinColumn(1, 0), new MultiJoinColumn(1, 1) };
            MultiJoinCheck Check(Expression left, Expression right) => new MultiJoinCheck()
            {
                Condition = new ScalarFunction() { ExtensionUrn = FunctionsComparison.Urn, ExtensionName = FunctionsComparison.NotEqual, Arguments = new List<Expression>() { left, right } },
                Columns = columns
            };

            Assert.NotNull(Check(Whole(1), Whole(3)).Compare);
            Assert.Null(Check(Inside(1), Whole(3)).Compare);
            Assert.Null(Check(Whole(1), Inside(3)).Compare);
        }

        [Fact]
        public void AConditionOnBothSidesLeavesTheJoinOutWhenTheSideFilledWithNullsIsAJoin()
        {
            // l left join (b join c), the condition compares l with c
            var root = On(LeftJoin(Read("l", 2), Join(Read("b", 3), Read("c", 2), (1, 3)), (0, 2)), (1, FunctionsComparison.LessThan, 6));
            var leftOut = new List<MultiJoinLeftOut>();
            Assert.Null(MultiJoinPlanBuilder.Discover(root, 9, leftOut, pairConditions: true));
            Assert.Contains(leftOut, x => x.Join == root && x.Reason == "condition on both sides, the side filled with nulls is a join");
        }

        [Fact]
        public void AConditionOnAMarkLeavesTheJoinOut()
        {
            // (a marked by b) join c on a.v, with the mark compared with a column of c
            var root = On(Join(Typed(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 3)), (2, FunctionsComparison.Equal, 4));
            var leftOut = new List<MultiJoinLeftOut>();
            Assert.Null(MultiJoinPlanBuilder.Discover(root, 9, leftOut));
            Assert.Contains(leftOut, x => x.Reason == "condition on a mark");
        }

        public static IEnumerable<object[]> Seeds()
        {
            int count = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var seeds) ? seeds : 5;
            for (int seed = 1; seed <= count; seed++)
            {
                yield return new object[] { seed };
            }
        }

        private async Task<MultiJoinOperator> StartChain()
        {
            var plan = MultiJoinPlanBuilder.Discover(Chain(), 9);
            Assert.NotNull(plan);
            var op = new MultiJoinOperator(plan, FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            _operators.Add(op);
            op.LinkTo(_outputBlock);
            await InitializeOperator(op);
            return op;
        }

        // Hands the operator one row of every input while nothing else runs on it, with every batch held back
        private async Task HoldBackOneRowOfEach(MultiJoinOperator op, int inputs)
        {
            var rows = new[] { (Row(1, 1, 1), 2), (Row(1, 1, 2, 0), 3), (Row(1, 2, 0), 2) };
            for (int i = 0; i < inputs; i++)
            {
                var message = CreateMessage(new List<(object?[] row, int weight)>() { rows[i].Item1 }, rows[i].Item2);
                await foreach (var batch in op.OnRecieve(i, message.Data, 0))
                {
                    Assert.Fail("A batch that is held back gives no rows");
                }
                message.Data.Data.Return();
            }
            Assert.True(op.HasDeferredRows);
        }

        private async Task<long> RowsUntilOneComes(MultiJoinOperator op)
        {
            long rows = 0;
            while (rows == 0)
            {
                var receive = _outputBlock.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (await Task.WhenAny(receive, op.Completion) != receive)
                {
                    // A failed operator fails the test at once
                    await op.Completion;
                    throw new InvalidOperationException("The operator completed before a row came out");
                }
                var received = await receive;
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    rows += message.Data.Data.Count;
                    message.Data.Data.Return();
                }
            }
            return rows;
        }

        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task RowsWithTheSameKeyInOneBatchAreJoinedEachOnItsOwn(int rowBudget)
        {
            // a(k, v) joins b(k, j, v) on k where a.v < b.v, and b joins c(j, v) on j
            var root = Join(On(Join(Read("a", 2), Read("b", 3), (0, 2)), (1, FunctionsComparison.LessThan, 4)), Read("c", 2), (3, 5));
            var b = new List<(object?[] row, int weight)>();
            var c = new List<(object?[] row, int weight)>();
            var first = new List<(object?[] row, int weight)>();
            var replaced = new List<(object?[] row, int weight)>();
            var back = new List<(object?[] row, int weight)>();
            for (long k = 1; k <= 3; k++)
            {
                // Two rows of b per key, so a key row has matches to go on from when a part is full
                b.Add(Row(1, k, k, 5));
                b.Add(Row(1, k, k, 7));
                c.Add(Row(1, k, 0));
                first.Add(Row(1, k, 9));
                // The row that leaves fails the condition, the ones that come pass it for both, one or none of the rows of b
                replaced.Add(Row(-1, k, 9));
                replaced.Add(Row(1, k, 1));
                replaced.Add(Row(1, k, 6));
                back.Add(Row(-1, k, 1));
                back.Add(Row(1, k, 9));
                back.Add(Row(-1, k, 6));
            }
            // A row that leaves one key for another, beside a row that stays on each of them
            var moved = new List<(object?[] row, int weight)>()
            {
                Row(-1, 1, 9), Row(1, 2, 9), Row(-1, 2, 9), Row(1, 2, 2), Row(1, 1, 3), Row(-1, 3, 9), Row(1, 3, 6)
            };
            await WithRowBudget(rowBudget, () => RunScript(root,
                (1, b.ToArray()),
                (2, c.ToArray()),
                (0, first.ToArray()),
                (0, replaced.ToArray()),
                (0, back.ToArray()),
                (0, moved.ToArray())));
        }

        // Holds every task queued on it until released, then runs them and every later one on the thread pool
        private sealed class HeldScheduler : TaskScheduler
        {
            private readonly System.Collections.Concurrent.ConcurrentQueue<Task> _held = new();
            private volatile bool _released;
            public readonly TaskCompletionSource Queued = new(TaskCreationOptions.RunContinuationsAsynchronously);

            protected override void QueueTask(Task task)
            {
                if (_released)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(_ => TryExecuteTask(task), null);
                    return;
                }
                _held.Enqueue(task);
                Queued.TrySetResult();
            }

            public void Release()
            {
                _released = true;
                while (_held.TryDequeue(out var task))
                {
                    ThreadPool.UnsafeQueueUserWorkItem(_ => TryExecuteTask(task), null);
                }
            }

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => _released && TryExecuteTask(task);

            protected override IEnumerable<Task> GetScheduledTasks() => _held.ToArray();
        }

        [Fact]
        public async Task AFailedJoinWaitsForTheInsertBesideItBeforeTheOperatorFails()
        {
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            MultiJoinOperator.ParallelInsertMinRows = 1;
            var held = new HeldScheduler();
            MultiJoinOperator.JoinHookForTests = () => throw new InvalidOperationException("The lookup failed");
            try
            {
                var op = await StartChain();
                // The insert beside the join runs on the operator's scheduler, here one that holds it
                typeof(MultiJoinOperator).GetField("_scheduler", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(op, held);
                await op.Targets[0].SendAsync(CreateMessage(new List<(object?[] row, int weight)>() { Row(1, 1, 1) }, 2));
                await held.Queued.Task.WaitAsync(TimeSpan.FromSeconds(30));
                // The batch is still being stored, so the failure may not end the message yet
                var first = await Task.WhenAny(op.Completion, Task.Delay(TimeSpan.FromMilliseconds(500)));
                Assert.NotSame(op.Completion, first);
                held.Release();
                var error = await Assert.ThrowsAnyAsync<Exception>(() => op.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
                Assert.Contains("The lookup failed", error.ToString());
            }
            finally
            {
                held.Release();
                MultiJoinOperator.JoinHookForTests = null;
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
            }
        }

        [Fact]
        public async Task ALookupThatFailsWhileTheBatchIsStoredFailsTheOperatorWithItsError()
        {
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            MultiJoinOperator.ParallelInsertMinRows = 1;
            MultiJoinOperator.JoinHookForTests = () => throw new InvalidOperationException("The lookup failed");
            try
            {
                var op = await StartChain();
                await op.Targets[0].SendAsync(CreateMessage(new List<(object?[] row, int weight)>() { Row(1, 1, 1) }, 2));
                // The join is the failure that shows
                var error = await Assert.ThrowsAnyAsync<Exception>(() => op.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
                Assert.Contains("The lookup failed", error.ToString());
            }
            finally
            {
                MultiJoinOperator.JoinHookForTests = null;
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
            }
        }

        [Fact]
        public async Task DeferredRowsComeOutAtAWatermark()
        {
            MultiJoinOperator.DeferEveryBatch = true;
            try
            {
                var op = await StartChain();
                for (int i = 0; i < op.Targets.Length; i++)
                {
                    await SendWatermarkInitialize(op.Targets[i], new HashSet<string>() { "t" + i });
                }
                await HoldBackOneRowOfEach(op, 3);
                for (int i = 0; i < op.Targets.Length; i++)
                {
                    await SendWatermark(op.Targets[i], new Dictionary<string, AbstractWatermarkValue>() { ["t" + i] = LongWatermarkValue.Create(1) });
                }
                // No checkpoint is sent: the joined row can only come from the flush a watermark makes
                Assert.Equal(1, await RowsUntilOneComes(op));
            }
            finally
            {
                MultiJoinOperator.DeferEveryBatch = false;
            }
        }

        [Fact]
        public async Task DeferredRowsComeOutWhenALoopPrepares()
        {
            MultiJoinOperator.DeferEveryBatch = true;
            try
            {
                var op = await StartChain();
                await HoldBackOneRowOfEach(op, 3);
                var prepare = new LockingEventPrepare(new Checkpoint(1, 2, 1), false);
                foreach (var target in op.Targets)
                {
                    await target.SendAsync(prepare);
                }
                // A loop below can only settle on rows it has seen
                Assert.Equal(1, await RowsUntilOneComes(op));
            }
            finally
            {
                MultiJoinOperator.DeferEveryBatch = false;
            }
        }

        [Fact]
        public async Task DeferredRowsAreDroppedByARestore()
        {
            MultiJoinOperator.DeferEveryBatch = true;
            try
            {
                var op = await StartChain();
                await HoldBackOneRowOfEach(op, 1);
                // A rollback: the source sends the row again when it is still wanted, the held one may not be applied
                await ReinitializeOperator(op);
                Assert.False(op.HasDeferredRows);
                await op.Targets[1].SendAsync(CreateMessage(new List<(object?[] row, int weight)>() { Row(1, 1, 2, 0) }, 3));
                await op.Targets[2].SendAsync(CreateMessage(new List<(object?[] row, int weight)>() { Row(1, 2, 0) }, 2));
                var actual = new Dictionary<string, long>();
                await DrainUntilCheckpoint(op, actual, 1);
                Assert.Empty(actual);
            }
            finally
            {
                MultiJoinOperator.DeferEveryBatch = false;
            }
        }

        [Fact]
        public async Task ChangeOnManyKeysDrainsThePadTableOverSeveralLeaves()
        {
            // (a join b) left join c, every row of c brings a key of its own: more than stay in memory, wide enough for several leaves
            const int Count = 2500;
            string Key(int x) => "key" + x.ToString().PadLeft(60, '0');
            var root = LeftJoin(Join(Read("a", 2), Read("b", 2), (1, 2)), Read("c", 2), (0, 4));
            var op = await RunScript(root,
                (0, Enumerable.Range(0, Count).Select(x => (new object?[] { Key(x), (long)x }, 1)).ToArray()),
                (1, Enumerable.Range(0, Count).Select(x => Row(1, x, 0)).ToArray()),
                (2, Enumerable.Range(0, Count).Select(x => (new object?[] { Key(x), 1L }, 1)).ToArray()));
            var table = op.PadTables.Single();
            Assert.Equal(Count, table.KeysAdded);
            Assert.True(table.LeavesDrained > 1, $"The keys were drained from {table.LeavesDrained} leaves");
        }

        [Fact]
        public async Task StateWrittenForOtherLookupsIsRefused()
        {
            await RunScript(Chain(),
                (0, new[] { Row(1, 1, 1) }),
                (1, new[] { Row(1, 1, 1, 0) }));
            // The same inputs with c joined to a: a is looked up by another column and its trees are laid out differently
            var plan = MultiJoinPlanBuilder.Discover(Join(Join(Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (1, 5)), 9)!;
            var second = new MultiJoinOperator(plan, FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            _operators.Add(second);
            second.Setup("stream", "1");
            second.CreateBlock();
            second.Link();
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ReinitializeOperator(second));
            Assert.Contains("new version", error.Message);
        }

        // l(k, v), b(k, j, v), c(j, v). A condition that reads only c, the second input of a joined side whose rows are not kept.
        private static MergeJoinRelation PushedPlan(string name)
        {
            switch (name)
            {
                case "pushedIntoJoinedRight":
                    return On(LeftJoin(Read("l", 2), Join(Read("b", 3), Read("c", 2), (1, 3)), (0, 2)), (6, FunctionsComparison.GreaterThan, 5));
                case "pushedIntoEmittedRight":
                    {
                        // The joined side gives the columns of c before those of b
                        var side = Join(Read("b", 3), Read("c", 2), (1, 3));
                        side.Emit = new List<int>() { 3, 4, 0, 1, 2 };
                        return On(LeftJoin(Read("l", 2), side, (0, 4)), (3, FunctionsComparison.GreaterThan, 2));
                    }
                default:
                    // The joined side is the left one of a right join
                    return On(Typed(JoinType.Right, Join(Read("b", 3), Read("c", 2), (1, 3)), Read("l", 2), (0, 5)), (4, FunctionsComparison.GreaterThan, 3));
            }
        }

        // The plans of this class by name, a new tree on every call
        internal static MergeJoinRelation NamedPlan(string name)
        {
            if (name == "chain")
            {
                return Chain();
            }
            if (name == "star")
            {
                return Star();
            }
            if (name == "nestedKeys")
            {
                return StarWithNestedKeys();
            }
            if (name.StartsWith("left:"))
            {
                return LeftPlan(name.Substring(5));
            }
            if (name.StartsWith("right:"))
            {
                return Retyped(LeftPlan(name.Substring(6)), JoinType.Right);
            }
            if (name.StartsWith("outer:"))
            {
                return Retyped(LeftPlan(name.Substring(6)), JoinType.Outer);
            }
            if (name.StartsWith("mark"))
            {
                return MarkPlan(name);
            }
            if (name.StartsWith("check") || name.Contains("Only"))
            {
                return CheckPlan(name);
            }
            if (name.StartsWith("pushed"))
            {
                return PushedPlan(name);
            }
            return MixedPlan(name);
        }

        // f joined to d on k1, p on (k1, k2), c on k3 and q on (k3, k4): one run of nested keys shares the primary tree of f, the other one index
        private static MergeJoinRelation StarWithNestedKeys()
        {
            var j1 = Join(Read("f", 5), Read("d", 2), (0, 5));
            var j2 = Join(j1, Read("p", 3), (0, 7), (1, 8));
            var j3 = Join(j2, Read("c", 2), (2, 10));
            return Join(j3, Read("q", 3), (2, 12), (3, 13));
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task KeyListsSharingAnIndex(int seed)
        {
            var root = StarWithNestedKeys();
            var f = MultiJoinPlanBuilder.Discover(root, 9)!.Inputs[0];
            Assert.Single(f.SecondaryLayouts);
            Assert.Equal(2, f.SecondaryOf.Count(x => x == 0));
            await Run(root, seed, seed % 2 == 0 ? 1 : MultiJoinOperator.DefaultRowBudget, 3);
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task ChainWithKeysOnDifferentInputs(int seed)
        {
            await Run(Chain(), seed, MultiJoinOperator.DefaultRowBudget, 4);
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task StarOfFourInputs(int seed)
        {
            await Run(Star(), seed, MultiJoinOperator.DefaultRowBudget, 3);
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task StarWithItsIndexesUpdatedSideBySide(int seed)
        {
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            // The middle input has three indexes, every batch updates them at once
            MultiJoinOperator.ParallelInsertMinRows = 1;
            try
            {
                await Run(Star(), seed, MultiJoinOperator.DefaultRowBudget, 3);
                Assert.True(_operators[^1].ParallelIndexUpdates > 0);
            }
            finally
            {
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
            }
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task CompositeKey(int seed)
        {
            await Run(Composite(), seed, MultiJoinOperator.DefaultRowBudget, 3);
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task SlicesOfOneRow(int seed)
        {
            await Run(Chain(), seed, 1, 4);
        }
    }
}
