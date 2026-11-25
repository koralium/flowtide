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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    // Random plans, run with the planner on and off
    public class DifferentialRandomPlanTests
    {
        private enum Kind
        {
            Number,
            Text,
            Flag
        }

        private sealed record Node(Relation Relation, List<Kind> Columns);

        private sealed class Generator
        {
            private readonly Random _random;
            private readonly List<string> _free = ["a", "b", "c"];
            private Relation? _shared;

            public Generator(int seed)
            {
                _random = new Random(seed);
            }

            public List<string> Tables { get; } = new List<string>();

            public bool GroupedSink { get; private set; }

            public string Shape { get; private set; } = "";

            public Plan Build()
            {
                var node = Unary(Tree(_random.Next(1, 4)));
                if (_random.Next(4) == 0)
                {
                    int column = _random.Next(node.Columns.Count);
                    Shape += " group";
                    node = new Node(
                        new AggregateRelation() { Input = node.Relation, Groupings = [new AggregateGrouping() { GroupingExpressions = [Field(column)] }] },
                        [node.Columns[column]]);
                }
                var plan = Write(node.Relation, node.Columns.Count);
                if (_shared != null)
                {
                    plan.Relations.Insert(0, _shared);
                }
                GroupedSink = _random.Next(3) == 0;
                int form = _random.Next(4);
                if (form == 0)
                {
                    Shape += " marked:" + Core.Operators.Join.MultiJoin.MultiJoinMarker.Mark(plan, 8);
                }
                else if (form == 1)
                {
                    int partitions = _random.Next(2, 4);
                    Shape += " partitions:" + partitions;
                    plan = Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(plan, partitions);
                }
                return plan;
            }

            private Node Tree(int leaves)
            {
                if (leaves == 1)
                {
                    return Leaf();
                }
                int leftLeaves = _random.Next(1, leaves);
                var left = Unary(Tree(leftLeaves));
                var right = Unary(Tree(leaves - leftLeaves));
                return Join(left, right);
            }

            private string Take()
            {
                var name = _free[0];
                _free.RemoveAt(0);
                Tables.Add(name);
                return name;
            }

            private Node Leaf()
            {
                var columns = new List<Kind>() { Kind.Number, Kind.Number, Kind.Text, Kind.Text };
                if (_shared == null && _random.Next(4) == 0)
                {
                    // One relation read by two branches of a union
                    _shared = Read(Take(), false);
                    Shape += " shared";
                    Relation Branch(string comparison, int bound)
                    {
                        return new FilterRelation()
                        {
                            Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 4 },
                            Condition = Compare(comparison, Field(1), new NumericLiteral() { Value = bound })
                        };
                    }
                    return new Node(
                        new SetRelation() { Operation = SetOperation.UnionAll, Inputs = [Branch(FunctionsComparison.GreaterThan, 1), Branch(FunctionsComparison.LessThan, 3)] },
                        columns);
                }
                return new Node(Read(Take(), false), columns);
            }

            private int Pick(List<Kind> columns, Kind kind)
            {
                var found = Enumerable.Range(0, columns.Count).Where(x => columns[x] == kind).ToList();
                return found.Count == 0 ? -1 : found[_random.Next(found.Count)];
            }

            // Sometimes an emit, always with a number column in it
            private List<Kind> Emit(Relation relation, List<Kind> columns)
            {
                if (_random.Next(3) != 0)
                {
                    return columns;
                }
                var emit = new List<int>();
                int count = _random.Next(1, columns.Count + 1);
                for (int i = 0; i < count; i++)
                {
                    emit.Add(_random.Next(columns.Count));
                }
                if (!emit.Any(x => columns[x] == Kind.Number))
                {
                    emit.Insert(_random.Next(emit.Count + 1), Pick(columns, Kind.Number));
                }
                relation.Emit = emit;
                Shape += " emit[" + string.Join(",", emit) + "]";
                return emit.Select(x => columns[x]).ToList();
            }

            private Expression Condition(List<Kind> columns)
            {
                int text = Pick(columns, Kind.Text);
                if (text >= 0 && _random.Next(2) == 0)
                {
                    return Compare(FunctionsComparison.GreaterThan, Field(text), new StringLiteral() { Value = "t" + _random.Next(2, 7) });
                }
                return Compare(FunctionsComparison.GreaterThan, Field(Pick(columns, Kind.Number)), new NumericLiteral() { Value = _random.Next(0, 3) });
            }

            private Node Unary(Node node)
            {
                int steps = _random.Next(0, 3);
                for (int i = 0; i < steps; i++)
                {
                    if (_random.Next(2) == 0)
                    {
                        var filter = new FilterRelation() { Input = node.Relation, Condition = Condition(node.Columns) };
                        Shape += " filter";
                        node = new Node(filter, Emit(filter, node.Columns));
                        continue;
                    }
                    var columns = new List<Kind>(node.Columns);
                    var expressions = new List<Expression>();
                    int count = _random.Next(1, 3);
                    for (int e = 0; e < count; e++)
                    {
                        if (_random.Next(2) == 0)
                        {
                            int column = _random.Next(node.Columns.Count);
                            expressions.Add(Field(column));
                            columns.Add(node.Columns[column]);
                        }
                        else
                        {
                            expressions.Add(Condition(node.Columns));
                            columns.Add(Kind.Flag);
                        }
                    }
                    var project = new ProjectRelation() { Input = node.Relation, Expressions = expressions };
                    Shape += " project";
                    node = new Node(project, Emit(project, columns));
                }
                return node;
            }

            private Node Join(Node left, Node right)
            {
                var types = new[] { JoinType.Inner, JoinType.Left, JoinType.Right, JoinType.Outer };
                var join = new MergeJoinRelation()
                {
                    Left = left.Relation,
                    Right = right.Relation,
                    Type = types[_random.Next(types.Length)],
                    LeftKeys = [Field(Pick(left.Columns, Kind.Number))],
                    RightKeys = [Field(left.Columns.Count + Pick(right.Columns, Kind.Number))]
                };
                int leftText = Pick(left.Columns, Kind.Text);
                int rightText = Pick(right.Columns, Kind.Text);
                if (leftText >= 0 && rightText >= 0 && _random.Next(4) == 0)
                {
                    join.PostJoinFilter = Compare(FunctionsComparison.GreaterThan, Field(leftText), Field(left.Columns.Count + rightText));
                    Shape += " post";
                }
                Shape += " join:" + join.Type;
                return new Node(join, Emit(join, left.Columns.Concat(right.Columns).ToList()));
            }
        }

        public static IEnumerable<object[]> Seeds()
        {
            // A failing seed is run again alone
            if (int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_TEST_RANDOM_PLAN_SEED"), out var only))
            {
                return [[only]];
            }
            // The long campaign is run by hand with a larger count
            int count = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_TEST_RANDOM_PLANS"), out var fromEnvironment) ? fromEnvironment : 40;
            return Enumerable.Range(0, count).Select(x => new object[] { x });
        }

        [Theory]
        [MemberData(nameof(Seeds))]
        public async Task ARandomPlanGivesTheSameRowsWithThePlannerOnAndOff(int seed)
        {
            var first = new Generator(seed);
            first.Build();
            var random = new Random(seed * 7919 + 1);
            object?[] Random(long key)
            {
                return Row(key, random.Next(5) == 0 ? null : random.Next(5), random.Next(5) == 0 ? null : "t" + random.Next(10), random.Next(3) == 0 ? null : new string('n', random.Next(1, 30)));
            }
            var initial = first.Tables.ToDictionary(x => x, x => Enumerable.Range(0, 15).Select(k => Random(k)).ToArray());
            var batch = first.Tables.ToDictionary(x => x, x => random.Next(3) == 0 ? int.MaxValue : random.Next(1, 7));
            await using var pair = DifferentialPair.Planned(
                () => new Generator(seed).Build(),
                () => first.Tables.Select(x =>
                {
                    var table = Table(x, initial[x]);
                    table.MaxBatch = batch[x];
                    return table;
                }).ToArray(),
                null,
                first.GroupedSink ? () => new DifferentialDestination() { Mode = seed % 2 == 0 ? ExecutionMode.OnCheckpoint : ExecutionMode.OnWatermark } : null);
            try
            {
                async Task Compare()
                {
                    await pair.SettleBoth();
                    Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
                    if (!first.GroupedSink)
                    {
                        Assert.Empty(pair.Plain.Negative());
                        Assert.Empty(pair.Hosted.Negative());
                    }
                }
                int restartAfter = random.Next(3) == 0 ? -1 : random.Next(4);
                await pair.StartAsync();
                await Compare();

                for (int round = 0; round < 4; round++)
                {
                    foreach (var table in first.Tables)
                    {
                        var upserts = Enumerable.Range(0, random.Next(1, 5)).Select(x => Random(random.Next(22))).DistinctBy(x => x[0]).ToArray();
                        var deletes = Enumerable.Range(0, random.Next(0, 3)).Select(x => (long)random.Next(22)).Distinct().ToArray();
                        pair.Change(tables =>
                        {
                            tables[table].Upsert(upserts);
                            tables[table].Delete(deletes);
                        });
                        if (random.Next(3) == 0)
                        {
                            await Compare();
                        }
                    }
                    await Compare();
                    if (round == restartAfter)
                    {
                        await pair.RestartAsync();
                    }
                }
            }
            catch (Exception e) when (e is not Xunit.Sdk.XunitException || !e.Message.Contains("shape:"))
            {
                throw new Xunit.Sdk.XunitException($"seed {seed}, shape:{first.Shape}, grouped sink {first.GroupedSink}: {e}");
            }
        }
    }
}
