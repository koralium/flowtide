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

using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Xunit.Abstractions;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinSettingOffLayoutTests
    {
        private readonly ITestOutputHelper _output;

        public MultiJoinSettingOffLayoutTests(ITestOutputHelper output)
        {
            _output = output;
        }

        // An inner join with a condition on both sides is in a multi join on main too
        [Fact]
        public void APlanMadeWithoutTheSettingIsBuiltWithoutRangeKeys()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(MultiJoinPlanBuilderTests.Tables + @"INSERT INTO output SELECT o.orderkey, u.name, c.name FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey AND o.amount <> u.active
                INNER JOIN companies c ON u.companyid = c.companyid;");
            // Multi join on, conditions on both sides off
            var root = MultiJoinPlanBuilderTests.RootJoin(PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings() { MultiJoinPairConditions = false }));
            Assert.NotNull(root.MultiJoinGroup);

            Assert.False(MultiJoinPlanBuilder.Build(root).RangeKeys, "A plan made without the setting may take ranges into its keys");
        }

        private static ReadRelation Read(string name, int columns) => new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = new List<string>() { name } },
            BaseSchema = new NamedStruct()
            {
                Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(x => (SubstraitBaseType)new AnyType()).ToList() }
            }
        };

        private static FieldReference Field(int index) => new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };

        // A Substrait plan from a client: (a join b on a.k = b.k and a.v < b.v) join c on b.j = c.j, marked with the default settings
        [Fact]
        public void AnInnerJoinWithALessThanBesideItsKeyKeepsTheTreeOrderOfMainWithoutTheSetting()
        {
            var ab = new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = Read("a", 2),
                Right = Read("b", 3),
                LeftKeys = new List<FieldReference>() { Field(0) },
                RightKeys = new List<FieldReference>() { Field(2) },
                PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.LessThan,
                    Arguments = new List<Expression>() { Field(1), Field(4) }
                }
            };
            var root = new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = ab,
                Right = Read("c", 2),
                LeftKeys = new List<FieldReference>() { Field(3) },
                RightKeys = new List<FieldReference>() { Field(5) }
            };
            var plan = new Plan() { Relations = new List<Relation>() { root } };
            Assert.Equal(1, MultiJoinMarker.Mark(plan, 5));

            var built = MultiJoinPlanBuilder.Build(root);
            var discovered = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: false)!;
            for (int i = 0; i < built.Inputs.Count; i++)
            {
                _output.WriteLine($"input {i}: built [{string.Join(",", built.Inputs[i].PrimaryOrder)}], without the setting [{string.Join(",", discovered.Inputs[i].PrimaryOrder)}]");
            }
            for (int i = 0; i < built.Inputs.Count; i++)
            {
                Assert.Equal(discovered.Inputs[i].PrimaryOrder, built.Inputs[i].PrimaryOrder);
            }
            Assert.False(built.PairConditions);
        }

        // A left join with a condition on both sides lies below a join that is left out, so it is not part of the group
        [Fact]
        public void AJoinThatIsLeftOutDoesNotGiveTheGroupRangeKeys()
        {
            // b(k, v, x) left join c(k, v, x) on b.k = c.k and b.v < c.v
            var bc = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = Read("b", 3),
                Right = Read("c", 3),
                LeftKeys = new List<FieldReference>() { Field(0) },
                RightKeys = new List<FieldReference>() { Field(3) },
                PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.LessThan,
                    Arguments = new List<Expression>() { Field(1), Field(4) }
                }
            };
            // Joined with d(k, x) on b.k = d.k and c.x = d.x: a key from several inputs, left out
            var bcd = new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = bc,
                Right = Read("d", 2),
                LeftKeys = new List<FieldReference>() { Field(0), Field(5) },
                RightKeys = new List<FieldReference>() { Field(6), Field(7) }
            };
            var withE = new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = bcd,
                Right = Read("e", 2),
                LeftKeys = new List<FieldReference>() { Field(6) },
                RightKeys = new List<FieldReference>() { Field(8) }
            };
            var root = new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = withE,
                Right = Read("f", 2),
                LeftKeys = new List<FieldReference>() { Field(8) },
                RightKeys = new List<FieldReference>() { Field(10) }
            };

            var discovered = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
            Assert.Equal(3, discovered.Inputs.Count);
            var plan = new Plan() { Relations = new List<Relation>() { root } };
            MultiJoinMarker.Mark(plan, 5, pairConditions: true);
            var built = MultiJoinPlanBuilder.Build(root);
            Assert.Equal(built.Inputs.Count, discovered.Inputs.Count);

            Assert.Equal(built.RangeKeys, discovered.RangeKeys);
        }

        // a(k, v) left join b(k, v, x) on a.k = b.k and a.v >= b.v, then left join c on the given column
        private static MergeJoinRelation PeriodsThenC(int column)
        {
            var periods = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = Read("a", 2),
                Right = Read("b", 3),
                LeftKeys = new List<FieldReference>() { Field(0) },
                RightKeys = new List<FieldReference>() { Field(2) },
                PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(1), Field(3) }
                }
            };
            return new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = periods,
                Right = Read("c", 2),
                LeftKeys = new List<FieldReference>() { Field(column) },
                RightKeys = new List<FieldReference>() { Field(5) }
            };
        }

        // b is looked up by k and x with a range on v, by k alone and by x alone: the range would order its tree further, and its second index would store v too
        [Fact]
        public void ARangeThatFitsTheTreeIsNotTakenOnAnInputThatHasASecondIndex()
        {
            // a(k, x, v) left join b(k, v, x) on a.k = b.k and a.x = b.x and a.v >= b.v
            var periods = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = Read("a", 3),
                Right = Read("b", 3),
                LeftKeys = new List<FieldReference>() { Field(0), Field(1) },
                RightKeys = new List<FieldReference>() { Field(3), Field(5) },
                PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(2), Field(4) }
                }
            };
            // left join c on b.x = c.x
            var withC = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = periods,
                Right = Read("c", 2),
                LeftKeys = new List<FieldReference>() { Field(5) },
                RightKeys = new List<FieldReference>() { Field(6) }
            };
            // left join d on b.k = d.k
            var root = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = withC,
                Right = Read("d", 2),
                LeftKeys = new List<FieldReference>() { Field(3) },
                RightKeys = new List<FieldReference>() { Field(8) }
            };
            var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
            var b = plan.Inputs[1];
            _output.WriteLine($"primary [{string.Join(",", b.PrimaryOrder)}], key lists {string.Join(" ", b.KeyLists.Select(x => "[" + string.Join(",", x) + "]"))}, second indexes {string.Join(" ", b.SecondaryLayouts.Select(x => "[" + string.Join(",", x) + "]"))}");

            Assert.NotEmpty(b.SecondaryLayouts);
            Assert.DoesNotContain(b.KeyLists, x => x.Contains(1));
            Assert.DoesNotContain(b.SecondaryLayouts, x => x.Contains(1));
        }

        // The column of the range would be stored in the second index of the input too
        [Fact]
        public void ARangeIsNotTakenOnAnInputThatHasASecondIndex()
        {
            // c joins on a.k: b is looked up by its key only
            var one = MultiJoinPlanBuilder.Discover(PeriodsThenC(0), 5, null, pairConditions: true)!;
            Assert.Empty(one.Inputs[1].SecondaryLayouts);
            Assert.Contains(one.Inputs[1].KeyLists, x => x.Contains(1));

            // c joins on b.x: b is looked up by x too
            var two = MultiJoinPlanBuilder.Discover(PeriodsThenC(4), 5, null, pairConditions: true)!;
            Assert.NotEmpty(two.Inputs[1].SecondaryLayouts);
            Assert.DoesNotContain(two.Inputs[1].KeyLists, x => x.Contains(1));
            Assert.DoesNotContain(two.Inputs[1].SecondaryLayouts, x => x.Contains(1));
        }
    }
}
