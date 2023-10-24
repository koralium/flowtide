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

        public override void AddReadResolvers(ReadWriteFactory readWriteFactory)
        {
            readWriteFactory.AddSqlServerSource(".*", () => sqlServerFixture.ConnectionString, (rel) =>
            {
                var name = rel.NamedTable.Names[0];
                rel.NamedTable.Names = new List<string>() { "tpch", "dbo", name };
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
                                    Names = new List<string>() { "tpch.dbo.orders" }
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

        public override Task Crash()
        {
            return sqlServerFixture.StopAsync();
        }

        public override Task Restart()
        {
            return sqlServerFixture.StartAsync();
        }
    }
}
