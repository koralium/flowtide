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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.TestFramework.Tests
{
    public class TestDataSinkPrimaryKeyTests
    {
        private static Plan GetPlan(string sql, (string name, SubstraitBaseType type)[] columns)
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("input", new NamedStruct()
            {
                Names = columns.Select(c => c.name).ToList(),
                Struct = new Struct()
                {
                    Types = columns.Select(c => c.type).ToList()
                }
            });
            builder.Sql(sql);
            return builder.GetPlan();
        }

        private static (FlowtideDotNet.Base.Engine.DataflowStream stream, StreamTestMonitor monitor, string tempDir) CreateStream(
            Plan plan,
            TestDataTable source,
            TestDataSink sink)
        {
            var tempDir = Path.Combine(Path.GetTempPath(), "flowtide_tests", Guid.NewGuid().ToString("N"));
            var monitor = new StreamTestMonitor();
            var connectorManager = new ConnectorManager();
            connectorManager.AddTestDataTable("input", source);
            connectorManager.AddTestDataSink("output", sink);

            var stream = new FlowtideBuilder("test")
                .AddPlan(plan)
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

            return (stream, monitor, tempDir);
        }

        private static void CleanupTempDir(string tempDir)
        {
            try
            {
                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, true);
                }
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }

        [Fact]
        public async Task Test_PrimaryKeyDeclaration_Success()
        {
            var plan = GetPlan(
                "INSERT INTO output PRIMARY KEY (id) SELECT id, name FROM input",
                new[] { ("id", (SubstraitBaseType)new Int64Type()), ("name", new StringType()) });

            var source = TestDataTable.Create(
                new { id = 1L, name = "Alice" },
                new { id = 2L, name = "Bob" }
            );
            var sink = new TestDataSink();
            var (stream, monitor, tempDir) = CreateStream(plan, source, sink);

            var runTask = stream.RunAsync();
            try
            {
                await monitor.WaitForCheckpoint();

                Assert.NotNull(sink.PrimaryKeyNames);
                Assert.Equal(new[] { "id" }, sink.PrimaryKeyNames);

                Assert.True(sink.IsCurrentDataEqual(new[]
                {
                    new { id = 1L, name = "Alice" },
                    new { id = 2L, name = "Bob" }
                }));
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
                CleanupTempDir(tempDir);
            }
        }

        [Fact]
        public async Task Test_PrimaryKeyDeclaration_Update_Success()
        {
            var plan = GetPlan(
                "INSERT INTO output PRIMARY KEY (id) SELECT id, name FROM input",
                new[] { ("id", (SubstraitBaseType)new Int64Type()), ("name", new StringType()) });

            var source = TestDataTable.Create(
                new { id = 1L, name = "Alice" },
                new { id = 2L, name = "Bob" }
            );
            var sink = new TestDataSink();
            var (stream, monitor, tempDir) = CreateStream(plan, source, sink);

            var runTask = stream.RunAsync();
            try
            {
                await monitor.WaitForCheckpoint();

                source.RemoveRows(new { id = 1L, name = "Alice" });
                source.AddRows(new { id = 1L, name = "Alice Updated" });

                await monitor.WaitForCheckpoint();

                Assert.True(sink.IsCurrentDataEqual(new[]
                {
                    new { id = 1L, name = "Alice Updated" },
                    new { id = 2L, name = "Bob" }
                }));
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
                CleanupTempDir(tempDir);
            }
        }

        [Fact]
        public async Task Test_PrimaryKeyDeclaration_DuplicateKeys_Throws()
        {
            var plan = GetPlan(
                "INSERT INTO output PRIMARY KEY (id) SELECT id, name FROM input",
                new[] { ("id", (SubstraitBaseType)new Int64Type()), ("name", new StringType()) });

            var source = TestDataTable.Create(
                new { id = 1L, name = "Alice" },
                new { id = 1L, name = "Bob" }
            );
            var sink = new TestDataSink();
            var (stream, monitor, tempDir) = CreateStream(plan, source, sink);

            var runTask = stream.RunAsync();
            try
            {
                var ex = await Assert.ThrowsAsync<FlowtideDuplicatePrimaryKeyException>(async () =>
                {
                    await monitor.WaitForCheckpoint();
                });

                Assert.Contains("id='1'", ex.Message);
                Assert.Contains("output", ex.Message);
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
                CleanupTempDir(tempDir);
            }
        }

        [Fact]
        public async Task Test_PrimaryKeyDeclaration_DuplicateKeysOnUpdate_Throws()
        {
            var plan = GetPlan(
                "INSERT INTO output PRIMARY KEY (id) SELECT id, name FROM input",
                new[] { ("id", (SubstraitBaseType)new Int64Type()), ("name", new StringType()) });

            var source = TestDataTable.Create(
                new { id = 1L, name = "Alice" },
                new { id = 2L, name = "Bob" }
            );
            var sink = new TestDataSink();
            var (stream, monitor, tempDir) = CreateStream(plan, source, sink);

            var runTask = stream.RunAsync();
            try
            {
                await monitor.WaitForCheckpoint();

                // Add a second row with id = 1 without removing the existing one -> duplicate key
                source.AddRows(new { id = 1L, name = "Alice Duplicate" });

                var ex = await Assert.ThrowsAsync<FlowtideDuplicatePrimaryKeyException>(async () =>
                {
                    await monitor.WaitForCheckpoint();
                });

                Assert.Contains("id='1'", ex.Message);
                Assert.Contains("output", ex.Message);
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
                CleanupTempDir(tempDir);
            }
        }

        [Fact]
        public async Task Test_CompoundPrimaryKeyDeclaration_DuplicateKeys_Throws()
        {
            var plan = GetPlan(
                "INSERT INTO output PRIMARY KEY (dept, id) SELECT dept, id, name FROM input",
                new[] { ("dept", (SubstraitBaseType)new Int64Type()), ("id", (SubstraitBaseType)new Int64Type()), ("name", new StringType()) });

            var source = TestDataTable.Create(
                new { dept = 1L, id = 10L, name = "Alice" },
                new { dept = 1L, id = 20L, name = "Bob" },
                new { dept = 1L, id = 10L, name = "Charlie" }
            );
            var sink = new TestDataSink();
            var (stream, monitor, tempDir) = CreateStream(plan, source, sink);

            var runTask = stream.RunAsync();
            try
            {
                var ex = await Assert.ThrowsAsync<FlowtideDuplicatePrimaryKeyException>(async () =>
                {
                    await monitor.WaitForCheckpoint();
                });

                Assert.Contains("dept='1'", ex.Message);
                Assert.Contains("id='10'", ex.Message);
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
                CleanupTempDir(tempDir);
            }
        }

        [Fact]
        public async Task Test_WithoutPrimaryKey_AllowsDuplicates()
        {
            var plan = GetPlan(
                "INSERT INTO output SELECT id, name FROM input",
                new[] { ("id", (SubstraitBaseType)new Int64Type()), ("name", new StringType()) });

            var source = TestDataTable.Create(
                new { id = 1L, name = "Alice" },
                new { id = 1L, name = "Alice" }
            );
            var sink = new TestDataSink();
            var (stream, monitor, tempDir) = CreateStream(plan, source, sink);

            var runTask = stream.RunAsync();
            try
            {
                await monitor.WaitForCheckpoint();
                Assert.Null(sink.PrimaryKeyNames);
                Assert.NotNull(sink.CurrentData);
                Assert.Equal(2, sink.CurrentData.Count);
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
                CleanupTempDir(tempDir);
            }
        }
    }
}
