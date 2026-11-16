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

using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinSettingTests
    {
        private const string Sql = @"
            CREATE TABLE a (k any, v any);
            CREATE TABLE b (k any, j any, v any);
            CREATE TABLE c (j any, i any, v any);
            CREATE TABLE d (i any, v any);

            INSERT INTO output
            SELECT ta.v, tb.v, tc.v, td.v
            FROM a ta
            INNER JOIN b tb ON ta.k = tb.k
            INNER JOIN c tc ON tb.j = tc.j
            INNER JOIN d td ON tc.i = td.i;";

        private static Plan Optimized(PlanOptimizerSettings? settings)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(Sql);
            return PlanOptimizer.Optimize(builder.GetPlan(), settings);
        }

        private sealed class MarkedJoins : OptimizerBaseVisitor
        {
            public int Count { get; private set; }

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                if (mergeJoinRelation.MultiJoinGroup != null)
                {
                    Count++;
                }
                return base.VisitMergeJoinRelation(mergeJoinRelation, state);
            }
        }

        private static int MarkedJoinCount(Plan plan)
        {
            var visitor = new MarkedJoins();
            foreach (var relation in plan.Relations)
            {
                visitor.Visit(relation, null!);
            }
            return visitor.Count;
        }

        [Fact]
        public void JoinsAreMarkedByDefault()
        {
            Assert.True(MultiJoinMarker.Count(Optimized(null)) > 0);
            Assert.True(MultiJoinMarker.Count(Optimized(new PlanOptimizerSettings())) > 0);
        }

        [Fact]
        public void NoJoinIsMarkedWithTheSettingOff()
        {
            var plan = Optimized(new PlanOptimizerSettings() { MultiJoin = false });
            Assert.Equal(0, MultiJoinMarker.Count(plan));
            var json = SubstraitSerializer.SerializeToJson(plan);
            Assert.DoesNotContain("MultiJoinHint", json);
            Assert.DoesNotContain("advancedExtension", json);
        }

        [Fact]
        public void TheSettingMarksTheChainAsOneGroup()
        {
            var plan = Optimized(new PlanOptimizerSettings() { MultiJoin = true });
            Assert.Equal(1, MultiJoinMarker.Count(plan));
            Assert.Equal(3, MarkedJoinCount(plan));
        }

        [Fact]
        public void TheInputCapDecidesHowManyJoinsAGroupTakes()
        {
            var plan = Optimized(new PlanOptimizerSettings() { MultiJoin = true, MultiJoinMaxInputs = 3 });
            // Three inputs are two joins, the join that is left has two inputs and stays an ordinary one
            Assert.Equal(1, MultiJoinMarker.Count(plan));
            Assert.Equal(2, MarkedJoinCount(plan));
        }

        [Fact]
        public void AnInputCapBelowThreeIsRefused()
        {
            var error = Assert.Throws<ArgumentOutOfRangeException>(() => Optimized(new PlanOptimizerSettings() { MultiJoin = true, MultiJoinMaxInputs = 2 }));
            // The message of the optimizer, the pass below it has one of its own
            Assert.Contains("MultiJoinMaxInputs must be at least 3", error.Message);
        }

        [Fact]
        public void NoJoinIsMarkedInADistributedPlan()
        {
            var plan = Optimized(new PlanOptimizerSettings()
            {
                MultiJoin = true,
                DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = 2 }
            });
            Assert.Equal(0, MultiJoinMarker.Count(plan));
        }

        [Fact]
        public void MarkedJoinsAreNotPartitioned()
        {
            var marked = Optimized(new PlanOptimizerSettings() { MultiJoin = true, Parallelization = 4 });
            Assert.Equal(1, MultiJoinMarker.Count(marked));
            Assert.Equal(3, MarkedJoinCount(marked));
            // Every join of the plan is in the group, so nothing was copied per partition
            var single = Optimized(new PlanOptimizerSettings() { MultiJoin = true });
            Assert.Equal(single.Relations.Count, marked.Relations.Count);
            // The plan builds its group from the marks it has after the pass
            var roots = new List<MergeJoinRelation>();
            new RootFinder(roots).Visit(marked.Relations[^1], null!);
            Assert.Equal(4, MultiJoinPlanBuilder.Build(Assert.Single(roots)).Inputs.Count);
        }

        private sealed class RootFinder : OptimizerBaseVisitor
        {
            private readonly List<MergeJoinRelation> _roots;

            public RootFinder(List<MergeJoinRelation> roots)
            {
                _roots = roots;
            }

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                // The first marked join met from the top is the root of its group
                if (mergeJoinRelation.MultiJoinGroup != null)
                {
                    _roots.Add(mergeJoinRelation);
                    return mergeJoinRelation;
                }
                return base.VisitMergeJoinRelation(mergeJoinRelation, state);
            }
        }

        [Fact]
        public void TheMarksSurviveSerialization()
        {
            var plan = Optimized(new PlanOptimizerSettings() { MultiJoin = true });
            var json = SubstraitSerializer.SerializeToJson(plan);
            var read = SubstraitDeserializer.DeserializeFromJson(json);
            Assert.Equal(1, MultiJoinMarker.Count(read));
            Assert.Equal(3, MarkedJoinCount(read));

            // The binary form
            var bytes = Google.Protobuf.MessageExtensions.ToByteArray(SubstraitSerializer.Serialize(plan));
            var fromBytes = new SubstraitDeserializer().Deserialize(global::Substrait.Protobuf.Plan.Parser.ParseFrom(bytes));
            Assert.Equal(1, MultiJoinMarker.Count(fromBytes));
            Assert.Equal(3, MarkedJoinCount(fromBytes));
        }

        private const string BaselineTables = @"
CREATE TABLE a (k any, v any);
CREATE TABLE b (k any, j any, v any);
CREATE TABLE c (j any, i any, v any);
CREATE TABLE d (i any, v any);
";

        // The hashes are of the serialized plans of commit 428417537, the last one without multi joins. Take them again when main changes the optimizer or the serializer
        [Theory]
        [InlineData("INSERT INTO output SELECT ta.v, tb.v, tc.v, td.v FROM a ta INNER JOIN b tb ON ta.k = tb.k INNER JOIN c tc ON tb.j = tc.j INNER JOIN d td ON tc.i = td.i;", 1, "D0626AE3AD2976DFED7E6D42B64F4C08D4740F025DB9F05225764206E0FDAC66")]
        [InlineData("INSERT INTO output SELECT ta.v, tb.v, tc.v, td.v FROM a ta INNER JOIN b tb ON ta.k = tb.k INNER JOIN c tc ON tb.j = tc.j INNER JOIN d td ON tc.i = td.i;", 4, "A1D329D2C5F2CFADC1EEAABB078DBCE59DCCB13636651A2FB492199884399193")]
        [InlineData("INSERT INTO output SELECT ta.v, tb.v, tc.v, td.v FROM a ta LEFT JOIN b tb ON ta.k = tb.k FULL OUTER JOIN c tc ON tb.j = tc.j RIGHT JOIN d td ON tc.i = td.i AND td.v > 3;", 1, "23A2DF29606D241C57782D8DE5992473B3EB8AD0C4072BCAD4FC5BFCBF7E16FD")]
        [InlineData("INSERT INTO output SELECT ta.v, tb.v, tc.v, td.v FROM a ta LEFT JOIN b tb ON ta.k = tb.k FULL OUTER JOIN c tc ON tb.j = tc.j RIGHT JOIN d td ON tc.i = td.i AND td.v > 3;", 4, "23EEF341BB69E4DB0D1DAF8D13DA2ECF9C37C9FA5D61433A3CBA5B71F404ABE8")]
        [InlineData("INSERT INTO output SELECT ta.k, count(*) FROM a ta INNER JOIN b tb ON ta.k = tb.k INNER JOIN c tc ON tb.j = tc.j WHERE tc.v = 1 GROUP BY ta.k;", 1, "86F664C9FAF1F87360CB17B1CC064B68B9FD2785D0EC97C81325FB4D95643CFC")]
        [InlineData("INSERT INTO output SELECT ta.k, count(*) FROM a ta INNER JOIN b tb ON ta.k = tb.k INNER JOIN c tc ON tb.j = tc.j WHERE tc.v = 1 GROUP BY ta.k;", 4, "F53A8D67F9521AF6284C34D32DB91510C98073E0C3CFFA47BB557493915AA55A")]
        public void APlanWithTheSettingOffIsSerializedAsBeforeTheSettingExisted(string sql, int parallelization, string hashBefore)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(BaselineTables + sql);
            var plan = PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings() { MultiJoin = false, Parallelization = parallelization });
            // The serializer ends its lines the way of the machine it runs on, the hashes are of the text with line feeds
            var json = SubstraitSerializer.SerializeToJson(plan).ReplaceLineEndings("\n");
            var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(json)));
            Assert.Equal(hashBefore, hash);
        }

        [Fact]
        public void TheSettingAndTheCapChangeTheSerializedPlan()
        {
            var off = SubstraitSerializer.SerializeToJson(Optimized(new PlanOptimizerSettings() { MultiJoin = false }));
            var on = SubstraitSerializer.SerializeToJson(Optimized(new PlanOptimizerSettings() { MultiJoin = true }));
            var capped = SubstraitSerializer.SerializeToJson(Optimized(new PlanOptimizerSettings() { MultiJoin = true, MultiJoinMaxInputs = 3 }));
            Assert.NotEqual(off, on);
            Assert.NotEqual(on, capped);
            Assert.NotEqual(off, capped);
        }

        private static ReadRelation Read(string name)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                }
            };
        }

        // A join whose key the serializer has no form for
        private static Plan PlanThatCannotBeSerialized(int? group)
        {
            var join = new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = Read("a"),
                Right = Read("b"),
                LeftKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new MapKeyReferenceSegment() { Key = "x" } } },
                RightKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new MapKeyReferenceSegment() { Key = "x" } } },
                MultiJoinGroup = group
            };
            return new Plan() { Relations = new List<Relation>() { join } };
        }

        [Fact]
        public void AMarkedPlanThatCannotBeSerializedFailsTheBuild()
        {
            var error = Assert.Throws<InvalidOperationException>(() => new FlowtideBuilder("test")
                .AddPlan(PlanThatCannotBeSerialized(1), false)
                .AddConnectorManager(new ConnectorManager())
                .Build());
            Assert.Contains("multi join", error.Message);
            Assert.Contains("could not be serialized", error.Message);
        }

        [Fact]
        public void APlanWithoutMarksThatCannotBeSerializedIsBuiltAsBefore()
        {
            // The build goes on past the hash and stops at the first thing that is missing, here the connector
            var error = Assert.ThrowsAny<Exception>(() => new FlowtideBuilder("test")
                .AddPlan(PlanThatCannotBeSerialized(null), false)
                .AddConnectorManager(new ConnectorManager())
                .Build());
            Assert.DoesNotContain("could not be serialized", error.Message);
        }
    }
}
