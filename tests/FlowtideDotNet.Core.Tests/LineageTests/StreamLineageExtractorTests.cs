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

using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    public class StreamLineageExtractorTests
    {
        private const string SubstreamSql = @"
            CREATE TABLE table1 (val any);

            SUBSTREAM stream1;

            CREATE VIEW read_table_1 WITH (DISTRIBUTED = true, SCATTER_BY = val, PARTITION_COUNT = 2) AS
            SELECT val FROM table1;

            INSERT INTO output1 SELECT val FROM read_table_1 WITH (PARTITION_ID = 0);

            SUBSTREAM stream2;

            INSERT INTO output2 SELECT val FROM read_table_1 WITH (PARTITION_ID = 1);
            ";

        private const string GetTimestampSql = @"
            CREATE TABLE input1 (a int, d timestamp);
            CREATE TABLE output (c1 int);
            INSERT INTO output SELECT a as c1 FROM input1 WHERE d < gettimestamp();
            ";

        private const string ProjectionSql = @"
            CREATE TABLE input (a int);
            CREATE TABLE output (c1 int);
            INSERT INTO output SELECT a as c1 FROM input;
            ";

        [Fact]
        public void TwoInsertsIntoSameTableAreMerged()
        {
            var lineage = LineageTestHelper.ExtractWithSql(@"
                CREATE TABLE input1 (a int, b int);
                CREATE TABLE input2 (a int, c int);
                CREATE TABLE output (c1 int, c2 int, c3 int);
                INSERT INTO output SELECT a as c1, b as c2 FROM input1;
                INSERT INTO output SELECT a as c1, c as c3 FROM input2;
                ");

            var output = Assert.Single(lineage.Outputs);
            Assert.Equal("output", output.Key);
            Assert.Equal(["c1", "c2", "c3"], output.PlanColumns.Select(x => x.Name));
            Assert.Equal(["input1", "input2"], output.UpstreamInputKeys);
            Assert.Equal(["input1", "input2"], lineage.Inputs.Select(x => x.Key));
            LineageTestHelper.AssertLineageEquivalent(new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Identity("input1", "a"), Identity("input2", "a")]),
                ["c2"] = new ColumnLineageField([Identity("input1", "b")]),
                ["c3"] = new ColumnLineageField([Identity("input2", "c")])
            }, []), output.ColumnLineage);

            var ev = LineageEventCreator.CreateFromLineage(Guid.NewGuid(), lineage, false);
            var outputTable = Assert.Single(ev.Outputs);
            Assert.Same(output.ColumnLineage, outputTable.Facets.ColumnLineage);
        }

        [Fact]
        public void SubstreamWritesHaveLineageWithoutScope()
        {
            // Output1 reads standard output, output2 crosses substreams.
            var lineage = LineageTestHelper.ExtractWithSql(SubstreamSql);

            Assert.Null(lineage.SubstreamName);
            Assert.Equal(["output1", "output2"], lineage.Outputs.Select(x => x.Key));
            foreach (var output in lineage.Outputs)
            {
                Assert.Equal(["table1"], output.UpstreamInputKeys);
                LineageTestHelper.AssertLineageEquivalent(new ColumnLineage(new Dictionary<string, ColumnLineageField>()
                {
                    ["val"] = new ColumnLineageField([Identity("table1", "val")])
                }, []), output.ColumnLineage);
            }
        }

        [Fact]
        public void SubstreamScopeReportsOnlyOwnWrites()
        {
            var stream1 = LineageTestHelper.ExtractWithSql(SubstreamSql, scope: "stream1");
            var stream2 = LineageTestHelper.ExtractWithSql(SubstreamSql, scope: "stream2");

            Assert.Equal("stream1", stream1.SubstreamName);
            Assert.Equal(["output1"], stream1.Outputs.Select(x => x.Key));
            Assert.Equal(["table1"], stream1.Inputs.Select(x => x.Key));

            // Table1 is only reached across the exchange.
            Assert.Equal("stream2", stream2.SubstreamName);
            Assert.Equal(["output2"], stream2.Outputs.Select(x => x.Key));
            Assert.Equal(["table1"], stream2.Inputs.Select(x => x.Key));
            LineageTestHelper.AssertLineageEquivalent(new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["val"] = new ColumnLineageField([Identity("table1", "val")])
            }, []), stream2.Outputs[0].ColumnLineage);
        }

        [Fact]
        public void AutomaticallyDistributedPlanMatchesUndistributed()
        {
            const string sql = @"
                CREATE TABLE orders (orderkey int, userkey int, amount int);
                CREATE TABLE users (userkey int, username string);
                CREATE TABLE output (c1 string, c2 int);
                INSERT INTO output
                SELECT u.username as c1, SUM(o.amount) as c2
                FROM orders o
                JOIN users u ON o.userkey = u.userkey
                GROUP BY u.username;
                ";
            var distributed = new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = 2 }
            };

            var baseline = LineageTestHelper.ExtractWithSql(sql);
            var baselineLineage = Assert.Single(baseline.Outputs).ColumnLineage!;
            Assert.Contains(baselineLineage.Dataset, x => x.TableName == "orders" && x.Field == "userkey");
            Assert.Contains(baselineLineage.Dataset, x => x.TableName == "users" && x.Field == "userkey");
            Assert.Contains(baselineLineage.Dataset, x => x.TableName == "users" && x.Field == "username");

            var unscoped = LineageTestHelper.ExtractWithSql(sql, distributed);
            LineageTestHelper.AssertLineageEquivalent(baselineLineage, Assert.Single(unscoped.Outputs).ColumnLineage);

            var scoped = new[] { "substream_0", "substream_1" }
                .Select(x => LineageTestHelper.ExtractWithSql(sql, distributed, x))
                .ToList();
            var owner = Assert.Single(scoped, x => x.Outputs.Count > 0);
            var output = Assert.Single(owner.Outputs);
            Assert.Equal("output", output.Key);
            LineageTestHelper.AssertLineageEquivalent(baselineLineage, output.ColumnLineage);
            Assert.Equal(["orders", "users"], output.UpstreamInputKeys.Order(StringComparer.Ordinal));

            var inputKeys = scoped.SelectMany(x => x.Inputs).Select(x => x.Key).ToHashSet();
            Assert.Contains("orders", inputKeys);
            Assert.Contains("users", inputKeys);
        }

        [Fact]
        public void SubstreamWithoutWriteReportsGlobalViewReads()
        {
            const string sql = @"
                CREATE TABLE orders (orderkey int, userkey int);
                CREATE TABLE users (userkey int, username string);
                CREATE VIEW active_users AS SELECT userkey, username FROM users WHERE username != 'x';
                INSERT INTO output
                SELECT o.orderkey, u.username FROM orders o JOIN active_users u ON o.userkey = u.userkey;
                ";
            var distributed = new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = 2 }
            };

            // Substream_1 scatters the global view, writes nothing.
            var producer = LineageTestHelper.ExtractWithSql(sql, distributed, "substream_1");
            Assert.Empty(producer.Outputs);
            Assert.Equal(["users"], producer.Inputs.Select(x => x.Key));
        }

        [Fact]
        public void GetTimestampReadIsSkipped()
        {
            // The real manager throws for the timestamp read.
            var lineage = LineageTestHelper.ExtractWithSql(GetTimestampSql);

            Assert.Equal(["input1"], lineage.Inputs.Select(x => x.Key));
            var output = Assert.Single(lineage.Outputs);
            Assert.Equal(["input1"], output.UpstreamInputKeys);
            Assert.Equal([Identity("input1", "a")], output.ColumnLineage!.Fields["c1"].InputFields);
        }

        [Fact]
        public void ConnectorFailurePropagates()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new LineageTestSourceFactory());

            Assert.Throws<FlowtideNoConnectorFoundException>(() => LineageTestHelper.Extract(LineageTestHelper.BuildPlan(ProjectionSql), connectorManager));
        }

        [Fact]
        public void NamePartsAreCatalogStripped()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddCatalog("cat", catalog =>
            {
                catalog.AddSource(new LineageTestSourceFactory());
                catalog.AddSink(new LineageTestSinkFactory());
            });
            var plan = LineageTestHelper.BuildPlan(@"
                CREATE TABLE cat.dbo.orders (a int);
                INSERT INTO cat.dbo.result SELECT a FROM cat.dbo.orders;
                ");

            var lineage = LineageTestHelper.Extract(plan, connectorManager);

            var input = Assert.Single(lineage.Inputs);
            Assert.Equal("cat.dbo.orders", input.Key);
            Assert.Equal("dbo.orders", input.TableName);
            Assert.Equal(["dbo", "orders"], input.NameParts);
            var output = Assert.Single(lineage.Outputs);
            Assert.Equal("cat.dbo.result", output.Key);
            Assert.Equal(["dbo", "result"], output.NameParts);
            Assert.Equal(["cat.dbo.orders"], output.UpstreamInputKeys);
        }

        [Fact]
        public void NamePartsFallBackToTableName()
        {
            Assert.Equal(["a", "b"], StreamLineageExtractor.GetNameParts(["a", "b"], "a.b"));
            Assert.Equal(["b", "c"], StreamLineageExtractor.GetNameParts(["a", "b", "c"], "b.c"));
            Assert.Equal(["x", "y"], StreamLineageExtractor.GetNameParts(["a", "b"], "x.y"));
        }

        [Fact]
        public void ConnectorSchemaWinsOverPlanColumns()
        {
            var connectorManager = LineageTestHelper.CreateConnectorManager(read => new NamedStruct()
            {
                Names = ["A", "extra"],
                Struct = new Struct() { Types = [new Int32Type(), new StringType()] }
            });

            var lineage = LineageTestHelper.Extract(LineageTestHelper.BuildPlan(ProjectionSql), connectorManager, includeSchema: true);

            var input = Assert.Single(lineage.Inputs);
            Assert.Equal(["a"], input.PlanColumns.Select(x => x.Name));
            Assert.Equal(["A", "extra"], input.SchemaColumns.Select(x => x.Name));
            var output = Assert.Single(lineage.Outputs);
            Assert.Null(output.ConnectorColumns);
            Assert.Equal(["c1"], output.SchemaColumns.Select(x => x.Name));
        }

        [Fact]
        public void BuildTimeIsUtcMicroseconds()
        {
            var lineage = StreamLineageExtractor.Extract(new StreamLineageExtractionContext()
            {
                Plan = LineageTestHelper.BuildPlan(ProjectionSql),
                ConnectorManager = LineageTestHelper.CreateConnectorManager(),
                BuilderStreamName = "s",
                BuildTime = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)).AddTicks(1234567)
            });

            Assert.Equal(TimeSpan.Zero, lineage.BuildTime.Offset);
            Assert.Equal(new DateTimeOffset(2026, 1, 2, 1, 4, 5, TimeSpan.Zero).AddTicks(1234560), lineage.BuildTime);
            Assert.True(lineage.TryGetInput("input", out var input));
            Assert.Equal("input", input.TableName);
            Assert.False(lineage.TryGetInput("missing", out _));
        }

        [Theory]
        [InlineData("6_orders_substream_0", "substream_0", "orders")]
        [InlineData("6_my_ord_s1", "s1", "my_ord")]
        [InlineData("7_default_default", "default", "default")]
        [InlineData("orders_s1", "s1", "orders_s1")]
        [InlineData("5_orders_s1", "s1", "5_orders_s1")]
        [InlineData("06_orders_s1", "s1", "06_orders_s1")]
        [InlineData("6_orders_s2", "s1", "6_orders_s2")]
        [InlineData("-6_orders_s1", "s1", "-6_orders_s1")]
        [InlineData("99_orders_s1", "s1", "99_orders_s1")]
        [InlineData("6_orders_s1", null, "6_orders_s1")]
        public void LogicalStreamNameParsing(string builderName, string? substreamName, string expected)
        {
            Assert.Equal(expected, LineageStreamNames.GetLogicalStreamName(builderName, substreamName));
        }

        [Fact]
        public void CreateFromLineageMatchesTodaysEvent()
        {
            var lineage = LineageTestHelper.ExtractWithSql(ProjectionSql);
            var runId = Guid.NewGuid();

            var ev = LineageEventCreator.CreateFromLineage(runId, lineage, true);

            Assert.Equal(runId, ev.Run.RunId);
            Assert.Equal(LineageEventType.Start, ev.EventType);
            Assert.Equal("flowtide", ev.Job.Namespace);
            Assert.Equal("s", ev.Job.Name);
            var input = Assert.Single(ev.Inputs);
            Assert.Equal(("test_ns", "input"), (input.Namespace, input.TableName));
            var inputField = Assert.Single(input.Facets.Schema!.Fields);
            Assert.Equal(("a", "bigint"), (inputField.Name, inputField.Type));
            var output = Assert.Single(ev.Outputs);
            Assert.Equal(("test_ns", "output"), (output.Namespace, output.TableName));
            var outputField = Assert.Single(output.Facets.Schema!.Fields);
            Assert.Equal(("c1", "bigint"), (outputField.Name, outputField.Type));
            Assert.Equal(new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Identity("input", "a")])
            }, []), output.Facets.ColumnLineage);

            var withoutSchema = LineageEventCreator.CreateFromLineage(runId, lineage, false);
            Assert.Null(Assert.Single(withoutSchema.Inputs).Facets.Schema);
            Assert.Null(Assert.Single(withoutSchema.Outputs).Facets.Schema);
            Assert.NotNull(withoutSchema.Outputs[0].Facets.ColumnLineage);
        }

        private static LineageInputField Identity(string table, string field)
        {
            return new LineageInputField("test_ns", table, field, [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]);
        }
    }
}
