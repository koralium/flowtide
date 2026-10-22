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

using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.TestFramework.Tests
{
    /// <summary>
    /// The source must only send the columns the read relation asks for.
    /// </summary>
    public class TestDataSourceEmitTests
    {
        public class Event
        {
            public long Id { get; set; }
            public string? Owner { get; set; }
            public long Action { get; set; }
            public string? ChangedBy { get; set; }
            public string? Data { get; set; }
            public long ChangedDate { get; set; }
        }

        private static Plan GetPlan(string sql)
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("input", new NamedStruct()
            {
                Names = new List<string>() { "Id", "Owner", "Action", "ChangedBy", "Data", "ChangedDate" },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>()
                    {
                        new Int64Type(),
                        new StringType(),
                        new Int64Type(),
                        new StringType(),
                        new StringType(),
                        new Int64Type()
                    }
                }
            });
            builder.Sql(sql);
            return builder.GetPlan();
        }

        private static async Task RunAsync(string sql, TestDataTable source, TestDataSink sink, Func<StreamTestMonitor, Task> test)
        {
            var tempDir = Path.Combine(Path.GetTempPath(), "flowtide_tests", Guid.NewGuid().ToString("N"));
            var monitor = new StreamTestMonitor();
            var connectorManager = new ConnectorManager();
            connectorManager.AddTestDataTable("input", source);
            connectorManager.AddTestDataSink("output", sink);

            var stream = new FlowtideBuilder("test")
                .AddPlan(GetPlan(sql))
                .AddConnectorManager(connectorManager)
                .WithCheckpointListener(monitor)
                .WithFailureListener(monitor)
                .WithStateOptions(new StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions()
                    {
                        DirectoryPath = tempDir
                    })
                })
                .Build();

            var runTask = stream.RunAsync();
            try
            {
                await test(monitor);
            }
            finally
            {
                await stream.StopAsync();
                await stream.DisposeAsync();
                try
                {
                    await runTask;
                }
                catch
                {
                }
                try
                {
                    if (Directory.Exists(tempDir))
                    {
                        Directory.Delete(tempDir, true);
                    }
                }
                catch
                {
                }
            }
        }

        private static Event[] Events()
        {
            return new[]
            {
                new Event() { Id = 1, Owner = "a", Action = 1, ChangedBy = "x", Data = "first", ChangedDate = 100 },
                new Event() { Id = 2, Owner = "a", Action = 2, ChangedBy = "y", Data = "second", ChangedDate = 200 },
                new Event() { Id = 3, Owner = "b", Action = 1, ChangedBy = "z", Data = "third", ChangedDate = 300 }
            };
        }

        [Fact]
        public async Task SubsetOfColumnsSelected()
        {
            var source = TestDataTable.Create(Events());
            var sink = new TestDataSink();

            await RunAsync("INSERT INTO output SELECT Data, Owner FROM input", source, sink, async monitor =>
            {
                await monitor.WaitForCheckpoint();

                Assert.True(sink.IsCurrentDataEqual(new[]
                {
                    new { Data = "first", Owner = "a" },
                    new { Data = "second", Owner = "a" },
                    new { Data = "third", Owner = "b" }
                }));
            });
        }

        [Fact]
        public async Task FilterOnColumnNotSelected()
        {
            var source = TestDataTable.Create(Events());
            var sink = new TestDataSink();

            await RunAsync("INSERT INTO output SELECT Data FROM input WHERE Action = 2", source, sink, async monitor =>
            {
                await monitor.WaitForCheckpoint();

                Assert.True(sink.IsCurrentDataEqual(new[]
                {
                    new { Data = "second" }
                }));
            });
        }

        [Fact]
        public async Task LatestRowPerPartitionWithWindow()
        {
            var source = TestDataTable.Create(Events());
            var sink = new TestDataSink();

            await RunAsync(@"
                INSERT INTO output
                SELECT Owner, Action, Data
                FROM (
                    SELECT Owner, Action, Data, ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY Id DESC) AS rn
                    FROM input
                )
                WHERE rn = 1", source, sink, async monitor =>
            {
                await monitor.WaitForCheckpoint();

                Assert.True(sink.IsCurrentDataEqual(new[]
                {
                    new { Owner = "a", Action = 2L, Data = "second" },
                    new { Owner = "b", Action = 1L, Data = "third" }
                }));

                source.AddRows(new Event() { Id = 4, Owner = "b", Action = 2, ChangedBy = "w", Data = "fourth", ChangedDate = 400 });
                await monitor.WaitForCheckpoint();

                Assert.True(sink.IsCurrentDataEqual(new[]
                {
                    new { Owner = "a", Action = 2L, Data = "second" },
                    new { Owner = "b", Action = 2L, Data = "fourth" }
                }));
            });
        }
    }
}
