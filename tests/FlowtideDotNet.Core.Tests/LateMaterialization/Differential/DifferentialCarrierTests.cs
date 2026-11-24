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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    public class DifferentialCarrierTests
    {
        // k, n, text, note; the last two stored
        private static ReadRelation Read(string table, bool hosted)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = [table] },
                BaseSchema = new NamedStruct()
                {
                    Names = ["k", "n", "text", "note"],
                    Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition() { Name = table, StoredColumns = [2, 3] } : null
            };
        }

        private static DirectFieldReference Field(int field)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = field } };
        }

        private static ScalarFunction Compare(string name, Expression left, Expression right)
        {
            return new ScalarFunction() { ExtensionUrn = FunctionsComparison.Urn, ExtensionName = name, Arguments = [left, right] };
        }

        private static Plan Write(Relation input, int columns)
        {
            return new Plan()
            {
                Relations =
                [
                    new WriteRelation()
                    {
                        Input = input,
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList() }
                    }
                ]
            };
        }

        private static DifferentialTable Table(string name, params object?[][] rows)
        {
            var table = new DifferentialTable(name, 4);
            table.Load(rows);
            return table;
        }

        private static object?[] Row(long key, long? number, string? text, string? note)
        {
            return [key, number, text, note];
        }

        // Rows with n over 10, flagged over 25
        private static Plan FilterAndProject(bool hosted)
        {
            var filter = new FilterRelation()
            {
                Input = Read("t", hosted),
                Condition = Compare(FunctionsComparison.GreaterThan, Field(1), new NumericLiteral() { Value = 10 })
            };
            var project = new ProjectRelation()
            {
                Input = filter,
                Expressions = [Compare(FunctionsComparison.GreaterThan, Field(1), new NumericLiteral() { Value = 25 })]
            };
            if (!hosted)
            {
                return Write(project, 5);
            }
            // The project names the reference again, the fetch reads that one
            project.Expressions.Add(Field(2));
            project.Emit = [1, 4, 0, 3];
            return Write(new FetchRequiredColumnsRelation()
            {
                Input = project,
                Fetches = [new RowStoreFetch() { ReferenceColumn = 1, StoreName = "t", Columns = [0, 1] }],
                Emit = [2, 0, 4, 5, 3]
            }, 5);
        }

        [Fact]
        public async Task AFilterAndAProjectCarryReferencesToTheFetch()
        {
            await using var pair = new DifferentialPair(FilterAndProject, () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))]);
            await pair.StartAsync();
            await pair.Expect("2|20|b|null|false", "3|30|null|z|true");

            pair.Change(tables => tables["t"].Upsert(Row(1, 40, "a2", "x"), Row(2, 20, "b2", "y")));
            await pair.Expect("1|40|a2|x|true", "2|20|b2|y|false", "3|30|null|z|true");

            pair.Change(tables => tables["t"].Upsert(Row(3, 7, "gone", "z")));
            pair.Change(tables => tables["t"].Delete(2));
            await pair.Expect("1|40|a2|x|true");

            await pair.RestartAsync();
            pair.Change(tables => tables["t"].Upsert(Row(4, 11, "d", "w"), Row(1, 41, "a3", "x")));
            await pair.Expect("1|41|a3|x|true", "4|11|d|w|false");
        }

        // l joined with r on l.n = r.k
        private static Plan Join(bool hosted, JoinType type)
        {
            var left = Read("l", hosted);
            var right = Read("r", hosted);
            var join = new MergeJoinRelation()
            {
                Left = left,
                Right = right,
                Type = type,
                LeftKeys = [Field(1)],
                RightKeys = [Field(left.OutputLength)]
            };
            if (!hosted)
            {
                return Write(join, 8);
            }
            return Write(new FetchRequiredColumnsRelation()
            {
                Input = join,
                Fetches =
                [
                    new RowStoreFetch() { ReferenceColumn = 2, StoreName = "l", Columns = [0, 1] },
                    new RowStoreFetch() { ReferenceColumn = 5, StoreName = "r", Columns = [0, 1] }
                ],
                Emit = [0, 1, 6, 7, 3, 4, 8, 9]
            }, 8);
        }

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        [InlineData(JoinType.Right)]
        [InlineData(JoinType.Outer)]
        public async Task AMergeJoinCarriesTheReferencesOfBothStores(JoinType type)
        {
            await using var pair = new DifferentialPair(
                hosted => Join(hosted, type),
                () =>
                [
                    Table("l", Row(1, 10, "l1", "a"), Row(2, 10, "l2", null), Row(3, 30, "l3", "c")),
                    Table("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"))
                ]);
            await pair.StartAsync();
            var rows = new List<string>() { "1|10|l1|a|10|1|r10|x", "2|10|l2|null|10|1|r10|x" };
            bool leftOuter = type == JoinType.Left || type == JoinType.Outer;
            bool rightOuter = type == JoinType.Right || type == JoinType.Outer;
            if (leftOuter)
            {
                rows.Add("3|30|l3|c|null|null|null|null");
            }
            if (rightOuter)
            {
                rows.Add("null|null|null|null|20|2|r20|y");
            }
            await pair.Expect(rows.ToArray());

            pair.Change(tables => tables["r"].Upsert(Row(10, 1, "r10b", "x"), Row(30, 3, "r30", null)));
            pair.Change(tables => tables["l"].Delete(2));
            rows = new List<string>() { "1|10|l1|a|10|1|r10b|x", "3|30|l3|c|30|3|r30|null" };
            if (rightOuter)
            {
                rows.Add("null|null|null|null|20|2|r20|y");
            }
            await pair.Expect(rows.ToArray());

            await pair.RestartAsync();
            pair.Change(tables => tables["r"].Delete(10));
            pair.Change(tables => tables["l"].Upsert(Row(4, 20, "l4", "d")));
            rows = new List<string>() { "3|30|l3|c|30|3|r30|null", "4|20|l4|d|20|2|r20|y" };
            if (leftOuter)
            {
                rows.Add("1|10|l1|a|null|null|null|null");
            }
            await pair.Expect(rows.ToArray());
        }

        private static int Count(DifferentialStream stream, string displayName)
        {
            return stream.Stream.GetDiagnosticsGraph().Nodes.Values.Count(x => x.DisplayName == displayName);
        }

        [Fact]
        public async Task PartitionedJoinsCarryReferencesThroughTheirExchangesAndUnion()
        {
            await using var pair = new DifferentialPair(
                hosted => Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(Join(hosted, JoinType.Left), 3),
                () =>
                [
                    Table("l", Enumerable.Range(1, 40).Select(x => Row(x, x % 7, "l" + x, x % 3 == 0 ? null : "n" + x)).ToArray()),
                    Table("r", Enumerable.Range(0, 5).Select(x => Row(x, x, "r" + x, "m" + x)).ToArray())
                ]);
            await pair.StartAsync();

            string Joined(int x)
            {
                var left = $"{x}|{x % 7}|l{x}|{(x % 3 == 0 ? "null" : "n" + x)}";
                return x % 7 < 5 ? $"{left}|{x % 7}|{x % 7}|r{x % 7}|m{x % 7}" : left + "|null|null|null|null";
            }
            await pair.Expect(Enumerable.Range(1, 40).Select(Joined).ToArray());
            Assert.Equal(3, Count(pair.Hosted, "Merge Join Bulk"));
            Assert.Equal(2, Count(pair.Hosted, "Exchange"));
            Assert.Equal(1, Count(pair.Hosted, "Set(UnionAll)"));
            Assert.Equal(1, Count(pair.Hosted, "Fetch required columns"));
            Assert.Equal(3, Count(pair.Plain, "Merge Join Bulk"));

            pair.Change(tables => tables["l"].Delete(Enumerable.Range(1, 20).Select(x => (long)x).ToArray()));
            pair.Change(tables => tables["r"].Upsert(Row(5, 5, "r5", "m5"), Row(6, 6, "r6", "m6")));
            string Later(int x)
            {
                return $"{x}|{x % 7}|l{x}|{(x % 3 == 0 ? "null" : "n" + x)}|{x % 7}|{x % 7}|r{x % 7}|m{x % 7}";
            }
            await pair.Expect(Enumerable.Range(21, 20).Select(Later).ToArray());

            await pair.RestartAsync();
            pair.Change(tables => tables["r"].Delete(0, 1, 2, 3, 4, 5, 6));
            await pair.Expect(Enumerable.Range(21, 20).Select(x => $"{x}|{x % 7}|l{x}|{(x % 3 == 0 ? "null" : "n" + x)}|null|null|null|null").ToArray());
        }

        // a joins b on n, b joins c on n
        private static Plan ThreeTables(bool hosted, JoinType second)
        {
            var a = Read("a", hosted);
            var b = Read("b", hosted);
            var c = Read("c", hosted);
            int width = a.OutputLength;
            var first = new MergeJoinRelation() { Left = a, Right = b, Type = JoinType.Inner, LeftKeys = [Field(1)], RightKeys = [Field(width)] };
            var join = new MergeJoinRelation() { Left = first, Right = c, Type = second, LeftKeys = [Field(width + 1)], RightKeys = [Field(2 * width)] };
            Plan plan;
            if (!hosted)
            {
                plan = Write(join, 12);
            }
            else
            {
                plan = Write(new FetchRequiredColumnsRelation()
                {
                    Input = join,
                    Fetches =
                    [
                        new RowStoreFetch() { ReferenceColumn = 2, StoreName = "a", Columns = [0, 1] },
                        new RowStoreFetch() { ReferenceColumn = 5, StoreName = "b", Columns = [1, 0] },
                        new RowStoreFetch() { ReferenceColumn = 8, StoreName = "c", Columns = [0, 1] }
                    ],
                    Emit = [0, 1, 9, 10, 3, 4, 12, 11, 6, 7, 13, 14]
                }, 12);
            }
            Assert.Equal(1, Core.Operators.Join.MultiJoin.MultiJoinMarker.Mark(plan, 8));
            return plan;
        }

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        public async Task AMultiJoinCarriesTheReferencesOfThreeStores(JoinType second)
        {
            await using var pair = new DifferentialPair(
                hosted => ThreeTables(hosted, second),
                () =>
                [
                    Table("a", Row(1, 10, "a1", "x"), Row(2, 20, "a2", null), Row(3, 10, null, "z")),
                    Table("b", Row(10, 100, "b10", "p"), Row(20, 200, "b20", "q")),
                    Table("c", Row(100, 0, "c100", null))
                ]);
            await pair.StartAsync();
            var rows = new List<string>() { "1|10|a1|x|10|100|b10|p|100|0|c100|null", "3|10|null|z|10|100|b10|p|100|0|c100|null" };
            if (second == JoinType.Left)
            {
                rows.Add("2|20|a2|null|20|200|b20|q|null|null|null|null");
            }
            await pair.Expect(rows.ToArray());
            Assert.Equal(1, Count(pair.Hosted, "Multi Join"));
            Assert.Equal(1, Count(pair.Plain, "Multi Join"));

            pair.Change(tables => tables["c"].Upsert(Row(200, 1, "c200", "w"), Row(100, 0, "c100b", null)));
            pair.Change(tables => tables["a"].Delete(3));
            pair.Change(tables => tables["b"].Upsert(Row(10, 100, "b10b", null)));
            await pair.Expect("1|10|a1|x|10|100|b10b|null|100|0|c100b|null", "2|20|a2|null|20|200|b20|q|200|1|c200|w");

            await pair.RestartAsync();
            pair.Change(tables => tables["b"].Delete(20));
            pair.Change(tables => tables["a"].Upsert(Row(4, 10, "a4", "v")));
            await pair.Expect("1|10|a1|x|10|100|b10b|null|100|0|c100b|null", "4|10|a4|v|10|100|b10b|null|100|0|c100b|null");
        }

        // Rows over 10 and rows under 30, some twice
        private static Plan UnionOfTwoFilters(bool hosted)
        {
            Relation Branch(string comparison, int bound)
            {
                return new FilterRelation()
                {
                    Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = hosted ? 3 : 4 },
                    Condition = Compare(comparison, Field(1), new NumericLiteral() { Value = bound })
                };
            }
            Relation union = new SetRelation() { Operation = SetOperation.UnionAll, Inputs = [Branch(FunctionsComparison.GreaterThan, 10), Branch(FunctionsComparison.LessThan, 30)] };
            if (hosted)
            {
                union = new FetchRequiredColumnsRelation()
                {
                    Input = union,
                    Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "t", Columns = [1, 0] }],
                    Emit = [0, 1, 4, 3]
                };
            }
            var plan = Write(union, 4);
            plan.Relations.Insert(0, Read("t", hosted));
            return plan;
        }

        [Fact]
        public async Task AUnionOfTwoBranchesOfOneStoreCarriesItsReferences()
        {
            await using var pair = new DifferentialPair(UnionOfTwoFilters, () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))]);
            await pair.StartAsync();
            await pair.Expect("1|5|a|x", "2|20|b|null", "2|20|b|null", "3|30|null|z");

            pair.Change(tables => tables["t"].Upsert(Row(1, 15, "a2", "x"), Row(3, 31, "c", null)));
            pair.Change(tables => tables["t"].Delete(2));
            await pair.Expect("1|15|a2|x", "1|15|a2|x", "3|31|c|null");

            await pair.RestartAsync();
            pair.Change(tables => tables["t"].Upsert(Row(1, 40, "a3", null)));
            await pair.Expect("1|40|a3|null", "3|31|c|null");
        }

        // A function that only works on whole rows, over column 0
        private static ScalarFunction RowsOnly()
        {
            return new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "is_odd", Arguments = [Field(0)] };
        }

        private static void RegisterRowsOnly(Core.Engine.FlowtideBuilder builder)
        {
            ((Core.Compute.FunctionsRegister)builder.FunctionsRegister).RegisterScalarFunctionWithExpression(
                "extension:test:rows", "is_odd", x => FlexBuffers.FlxValue.FromBytes(FlexBuffers.FlexBuffer.SingleValue(x.AsLong % 2 == 1)));
        }

        private static Plan RowFunctionFilter(bool hosted, bool fetchFirst)
        {
            Relation input = Read("t", hosted);
            if (hosted && fetchFirst)
            {
                input = new FetchRequiredColumnsRelation()
                {
                    Input = input,
                    Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "t", Columns = [0, 1] }],
                    Emit = [0, 1, 3, 4]
                };
            }
            Relation filter = new FilterRelation() { Input = input, Condition = RowsOnly() };
            if (hosted && !fetchFirst)
            {
                filter = new FetchRequiredColumnsRelation()
                {
                    Input = filter,
                    Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "t", Columns = [0, 1] }],
                    Emit = [0, 1, 3, 4]
                };
            }
            return Write(filter, 4);
        }

        [Fact]
        public async Task ARowFunctionRunsWhenEveryStoredColumnIsFetchedBeforeIt()
        {
            await using var pair = new DifferentialPair(hosted => RowFunctionFilter(hosted, true), () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))], RegisterRowsOnly);
            await pair.StartAsync();
            await pair.Expect("1|5|a|x", "3|30|null|z");

            pair.Change(tables => tables["t"].Upsert(Row(5, 50, "e", "v"), Row(1, 6, "a2", "x")));
            pair.Change(tables => tables["t"].Delete(3));
            await pair.Expect("1|6|a2|x", "5|50|e|v");
        }

        [Fact]
        public async Task ARowFunctionBeforeTheFetchIsRefusedWhenTheStreamIsBuilt()
        {
            await using var stream = new DifferentialStream(() => RowFunctionFilter(true, false), RegisterRowsOnly, Table("t", Row(1, 5, "a", "x")));

            var error = await Assert.ThrowsAsync<InvalidOperationException>(stream.StartAsync);

            Assert.Equal(
                "A Filter relation uses function 'is_odd' in its condition next to row references and the function has no columnar form, it would work on whole rows and a row reference cannot be turned into one. Fetch every stored column before it.",
                error.Message);
        }

        private static string Text(object? value)
        {
            return value == null ? "null" : value.ToString()!;
        }

        // The left join of l and r, computed here
        private static string[] LeftJoined(Dictionary<string, DifferentialTable> tables)
        {
            var rows = new List<string>();
            foreach (var left in tables["l"].Rows)
            {
                var matches = tables["r"].Rows.Where(x => left[1] != null && Equals(x[0], left[1])).ToList();
                if (matches.Count == 0)
                {
                    rows.Add(string.Join("|", left.Select(Text)) + "|null|null|null|null");
                }
                foreach (var right in matches)
                {
                    rows.Add(string.Join("|", left.Concat(right).Select(Text)));
                }
            }
            return rows.ToArray();
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(2, 1)]
        [InlineData(3, 3)]
        [InlineData(4, 3)]
        public async Task RandomChangesGiveTheSameRowsWithAndWithoutStores(int seed, int partitions)
        {
            var random = new Random(seed);
            object?[] Random(long key)
            {
                return Row(key, random.Next(4) == 0 ? null : random.Next(6), random.Next(5) == 0 ? null : "t" + random.Next(1000), random.Next(3) == 0 ? null : new string('n', random.Next(1, 40)));
            }
            var left = Enumerable.Range(0, 30).Select(x => Random(x)).ToArray();
            var right = Enumerable.Range(0, 4).Select(x => Random(x)).ToArray();
            await using var pair = new DifferentialPair(
                hosted => partitions == 1 ? Join(hosted, JoinType.Left) : Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(Join(hosted, JoinType.Left), partitions),
                () => [Table("l", left), Table("r", right)]);
            await pair.StartAsync();
            await pair.Expect(LeftJoined(pair.Plain.Tables));

            for (int round = 0; round < 12; round++)
            {
                var upserts = Enumerable.Range(0, random.Next(1, 6)).Select(x => Random(random.Next(45))).DistinctBy(x => x[0]).ToArray();
                var deletes = Enumerable.Range(0, random.Next(0, 4)).Select(x => (long)random.Next(45)).Distinct().ToArray();
                var rightUpserts = Enumerable.Range(0, random.Next(0, 3)).Select(x => Random(random.Next(6))).DistinctBy(x => x[0]).ToArray();
                var rightDeletes = Enumerable.Range(0, random.Next(0, 2)).Select(x => (long)random.Next(6)).ToArray();
                pair.Change(tables =>
                {
                    tables["l"].Upsert(upserts);
                    tables["l"].Delete(deletes);
                    tables["r"].Upsert(rightUpserts);
                    tables["r"].Delete(rightDeletes);
                });
                await pair.Expect(LeftJoined(pair.Plain.Tables));
                Assert.Equal(LeftJoined(pair.Plain.Tables).Order(StringComparer.Ordinal), LeftJoined(pair.Hosted.Tables).Order(StringComparer.Ordinal));
                if (round == 5)
                {
                    await pair.RestartAsync();
                }
            }
        }
    }
}
