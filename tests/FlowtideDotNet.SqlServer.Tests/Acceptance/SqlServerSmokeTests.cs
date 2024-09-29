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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Tests.SmokeTests;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using FlowtideDotNet.Substrait.Sql;
using FluentAssertions;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Type;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Core;
using System.Diagnostics.Metrics;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.SqlServer.Tests.Acceptance
{
    public class SqlServerSmokeTests : QuerySmokeTestBase, IClassFixture<SqlServerFixture>
    {
        private readonly SqlServerFixture sqlServerFixture;
        public SqlServerSmokeTests(SqlServerFixture sqlServerFixture)
        {
            this.sqlServerFixture = sqlServerFixture;
        }

        public override async Task AddLineItems(IEnumerable<LineItem> lineItems)
        {
            await sqlServerFixture.DbContext.BulkInsertAsync(lineItems);
        }

        public override async Task AddOrders(IEnumerable<Order> orders)
        {
            await sqlServerFixture.DbContext.BulkInsertAsync(orders);
        }

        public override void AddReadResolvers(IConnectorManager connectorManager)
        {
            connectorManager.AddSqlServerSource(() => sqlServerFixture.ConnectionString, (rel) =>
            {
                var name = rel.NamedTable.Names[0];
                return $"tpch.dbo.{name}";
            });
        }

        public override async Task AddShipmodes(IEnumerable<Shipmode> shipmodes)
        {
            await sqlServerFixture.DbContext.BulkInsertAsync(shipmodes);
        }

        public override async Task ClearAllTables()
        {
            var context = sqlServerFixture.DbContext;
            await context.LineItems.ExecuteDeleteAsync();
            await context.Orders.ExecuteDeleteAsync();
            await context.Shipmodes.ExecuteDeleteAsync();
        }

        public override async Task UpdateShipmodes(IEnumerable<Shipmode> shipmode)
        {
            await sqlServerFixture.DbContext.BulkUpdateAsync(shipmode);
        }

        [Fact]
        public void TestSqlTableProvider()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddSqlServerProvider(() => sqlServerFixture.ConnectionString);
            sqlPlanBuilder.Sql("SELECT orderKey FROM tpch.dbo.orders");
            var plan = sqlPlanBuilder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>() { 9 },
                            Expressions = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 0
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new NamedTable()
                                {
                                    Names = new List<string>() { "tpch", "dbo", "orders" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>()
                                    {
                                        "Orderkey", "Custkey", "Orderstatus", "Totalprice", "Orderdate", "Orderpriority", "Clerk", "Shippriority", "Comment"
                                    },
                                    Struct = new Substrait.Type.Struct()
                                    {
                                        Types = new List<Substrait.Type.SubstraitBaseType>()
                                        {
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType()
                                        }
                                    }
                                }
                            }
                        }
                    },
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes()
                );
        }

        [Fact]
        public void TestChangeTrackingError()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddSqlServerProvider(() => sqlServerFixture.ConnectionString);
            sqlPlanBuilder.Sql("SELECT id FROM tpch.dbo.notracking");
            var plan = sqlPlanBuilder.GetPlan();

            ConnectorManager connectorManager = new ConnectorManager();
            connectorManager.AddSqlServerSource(() => sqlServerFixture.ConnectionString);

            var e = Assert.Throws<InvalidOperationException>(() =>
            {
                var stream = new FlowtideBuilder("stream")
                .AddPlan(plan)
                .AddConnectorManager(connectorManager)
                .WithStateOptions(new Storage.StateManager.StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new Storage.FileCacheOptions())
                })
                .Build();
            });
            Assert.Equal("Change tracking must be enabled on table 'tpch.dbo.notracking'", e.Message);

        }

        public override Task Crash()
        {
            return sqlServerFixture.StopAsync();
        }

        public override Task Restart()
        {
            return sqlServerFixture.StartAsync();
        }

        [Fact]
        public async Task PrimaryKeyOnlyColumnInSink()
        {
            var writeRel = new WriteRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new Substrait.Type.NamedTable()
                    {
                        Names = new List<string>() { "table1" }
                    },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "id" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                    }
                },
                NamedObject = new NamedTable()
                {
                    Names = new List<string>() { "tpch", "dbo", "notracking" }
                },
                TableSchema = new NamedStruct()
                {
                    Names = new List<string>() { "id" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                }
            };

            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000,
                PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions())
            }, new NullLogger<StateManagerSync<object>>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();
            var stateClient = stateManager.GetOrCreateClient("node");

            StreamMetrics streamMetrics = new StreamMetrics("stream");
            var nodeMetrics = streamMetrics.GetOrCreateVertexMeter("node1", () => "node1");

            var streamMemoryManager = new StreamMemoryManager("stream");
            var memoryManager = streamMemoryManager.CreateOperatorMemoryManager("op");

            var vertexHandler = new VertexHandler("mergejoinstream", "op", (time) =>
            {

            }, (v1, v2, time) =>
            {
                return Task.CompletedTask;
            }, nodeMetrics, stateClient, new NullLoggerFactory(), memoryManager);
            var sink = new SqlServerSink(new Connector.SqlServer.SqlServerSinkOptions() { ConnectionStringFunc = () => sqlServerFixture.ConnectionString }, writeRel, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());

            sink.Setup("mergejoinstream", "op");
            sink.CreateBlock();
            sink.Link();

            
            await sink.Initialize("1", 0, 0, null, vertexHandler);

            await sink.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new List<RowEvent>()
            {
                RowEvent.Create(1, 0, (b) =>
                {
                    b.Add(1);
                })
            }, 1), 0));

            await sink.SendAsync(new Checkpoint(0, 1));

            var token = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (true)
            {
                token.Token.ThrowIfCancellationRequested();

                var hasRow = await sqlServerFixture.ExecuteReader("SELECT id FROM tpch.dbo.notracking",
                    (reader) =>
                    {
                        return reader.Read();
                    });

                if(hasRow)
                {
                    break;
                }
            }
        }
    }
}
