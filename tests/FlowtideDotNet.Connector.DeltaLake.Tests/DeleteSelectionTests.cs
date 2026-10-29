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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Globalization;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeleteSelectionTests
    {
        [Fact]
        public async Task ADecimalRetractionMatchesItsStoredRow()
        {
            var testName = nameof(ADecimalRetractionMatchesItsStoredRow);
            var storage = Files.Of.InternalMemory($"./{testName}");
            // A file per row, so each file's bounds hold one sign only
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.MaxFileSizeBytes = 1);
            stream.Generate(20);
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    d DECIMAL(10, 2)
                );

                INSERT INTO test
                SELECT userkey, CASE WHEN userkey % 2 = 0 THEN CAST(1.005 AS DECIMAL) ELSE CAST(-1.005 AS DECIMAL) END AS d FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            // One of each sign, the deletes carry the unrounded literal
            var even = stream.Users.First(x => x.UserKey % 2 == 0);
            var odd = stream.Users.First(x => x.UserKey % 2 != 0);
            // One fetch, so both deletes reach one checkpoint
            stream.DeleteUsers(new[] { even, odd });
            await WaitForVersion(storage, "test", stream, 1);

            Assert.Equal(stream.Users.Select(x => (long)x.UserKey).Order(), await LiveKeys(storage, "test"));
        }

        [Fact]
        public async Task DecimalsRoundHalfUpInFilesBoundsAndPartitionValues()
        {
            var testName = nameof(DecimalsRoundHalfUpInFilesBoundsAndPartitionValues);
            var storage = Files.Of.InternalMemory($"./{testName}");
            // Logical names, so the statistics are keyed by d
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.EnableColumnMappingOnNewTables = false);
            stream.Generate(20);
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    d DECIMAL(10, 2)
                );

                INSERT INTO test
                SELECT userkey, CASE WHEN userkey % 2 = 0 THEN CAST(1.005 AS DECIMAL) ELSE CAST(-1.005 AS DECIMAL) END AS d FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            // Away from zero, as Spark and SQL round
            var values = (await ForeignRewrite.LiveRows(storage, "test")).Select(x => x.Row.Split(' ')[1]).Distinct().Order(StringComparer.Ordinal);
            Assert.Equal(new[] { "-1.01", "1.01" }, values);
            var add = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles);
            using (var statistics = JsonDocument.Parse(add.Statistics!))
            {
                Assert.Equal(-1.01m, statistics.RootElement.GetProperty("minValues").GetProperty("d").GetDecimal());
                Assert.Equal(1.01m, statistics.RootElement.GetProperty("maxValues").GetProperty("d").GetDecimal());
            }
            var partition = new Internal.Delta.PartitionWriters.DecimalColumnPartitionWriter(new Internal.Delta.Schema.Types.DecimalType(10, 2));
            Assert.Equal(("1.01", "-1.01"), (partition.GetValue(new Core.ColumnStore.DecimalValue(1.005m)), partition.GetValue(new Core.ColumnStore.DecimalValue(-1.005m))));
        }

        [Fact]
        public async Task TheOptOutDeletesTheLiveCopyAndCountsTheRest()
        {
            const string table = "unmatched_copies";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.IgnoreUnmatchedDeletes = true;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            // Without normalization the three retractions of one row reach the sink
            stream.SourceImmutable();
            stream.Generate(10);
            await stream.StartStream(@"
                CREATE TABLE unmatched_copies (
                    userkey INT,
                    name STRING
                );

                INSERT INTO unmatched_copies
                SELECT userkey, firstName as name FROM users
            ");
            await WaitForVersion(storage, table, stream, 0);
            await SettlePublications(stream);

            // A retraction of -3 against one live copy
            var user = stream.Users[4];
            var users = stream.MockDatabaseForTests.GetOrCreateTable<AcceptanceTests.Entities.User>("users");
            users.Delete(new[] { user, user, user });
            await WaitForVersion(storage, table, stream, 1);

            Assert.DoesNotContain((long)user.UserKey, await LiveKeys(storage, table));
            Assert.Equal(9, (await LiveKeys(storage, table)).Count);
            Assert.Equal(1, await ChangeRows(storage, table, 1, "delete"));
            Assert.Equal(new[] { 2L }, ReadGauge(stream, "flowtide_delta_delete_unmatched_rows"));
        }

        [Fact]
        public async Task ChangeRowsHoldEachMatchedEntrysOwnRow()
        {
            var testName = nameof(ChangeRowsHoldEachMatchedEntrysOwnRow);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.WriteChangeDataOnNewTables = true);
            stream.Generate(10);
            // A user's two rows store the same value, so the second entry stands for the third stored row
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    d DECIMAL(10, 2)
                );

                INSERT INTO test
                SELECT userkey, CAST(1.001 AS DECIMAL) AS d FROM users
                UNION ALL
                SELECT userkey, CAST(1.004 AS DECIMAL) AS d FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var first = stream.Users[2];
            var second = stream.Users[7];
            // One fetch, so both deletes reach one checkpoint
            stream.DeleteUsers(new[] { first, second });
            await WaitForVersion(storage, "test", stream, 1);

            // Each matched copy is a change row of its own row
            var expected = new[] { first.UserKey, first.UserKey, second.UserKey, second.UserKey }.Select(x => $"{x} 1.00").Order(StringComparer.Ordinal);
            Assert.Equal(expected, await ChangeRowValues(storage, "test", 1, "delete"));
            Assert.Equal(stream.Users.SelectMany(x => new[] { (long)x.UserKey, x.UserKey }).Order(), await LiveKeys(storage, "test"));
        }

        [Theory]
        // The last pass runs in the walk
        [InlineData(1, 2)]
        // The last pass runs after the walk
        [InlineData(2, 3)]
        public async Task AnEarlierPassKeepsItsChangeRowsWhenTheLastPassMatchesNothing(int rowCap, int deletes)
        {
            var table = $"matched_then_unmatched_{rowCap}";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                // A file per row, change data on
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 1;
                options.IgnoreUnmatchedDeletes = true;
                options.DeleteLimitsOverride = new Internal.Deletes.DeleteLimits() { RowCap = rowCap };
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream($@"
                CREATE TABLE {table} (
                    userkey INT,
                    name STRING
                );

                INSERT INTO {table}
                SELECT userkey, firstName as name FROM users
            ");
            await WaitForVersion(storage, table, stream, 0);
            await SettlePublications(stream);

            // The first pass deletes its rows, another writer removed the last pass's row
            var deleted = stream.Users.OrderBy(x => x.UserKey).Take(deletes).ToList();
            var paths = (await ForeignRewrite.LiveRows(storage, table)).ToDictionary(x => long.Parse(x.Row.Split(' ')[0], CultureInfo.InvariantCulture), x => x.Path);
            await WriteCommit(storage, table, 1, new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = paths[deleted[^1].UserKey], DataChange = true, DeletionTimestamp = 0 } });
            stream.DeleteUsers(deleted);
            await WaitForVersion(storage, table, stream, 2);

            Assert.Equal(stream.Users.Select(x => (long)x.UserKey).Order(), await LiveKeys(storage, table));
            Assert.Equal(deletes - 1, await ChangeRows(storage, table, 2, "delete"));
            Assert.Equal(new[] { 1L }, ReadGauge(stream, "flowtide_delta_delete_unmatched_rows"));
        }

        [Theory]
        [InlineData("byte", 300, 44)]
        [InlineData("short", 70_000, 4_464)]
        public async Task AWrappedSmallIntegerKeepsBoundsOfItsStoredValue(string type, long value, long stored)
        {
            var testName = nameof(AWrappedSmallIntegerKeepsBoundsOfItsStoredValue) + type;
            var storage = Files.Of.InternalMemory($"./{testName}");
            var schema = "{\"type\":\"struct\",\"fields\":[" +
                "{\"name\":\"userkey\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}," +
                $"{{\"name\":\"small\",\"type\":\"{type}\",\"nullable\":true,\"metadata\":{{}}}}]}}";
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream($@"
                INSERT INTO test
                SELECT userKey AS userkey, {value} AS small FROM users
            ");
            await WaitForVersion(storage, "test", stream, 1);

            // The file stores the wrapped value, its statistics must bound it
            var add = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 1))!.AddedFiles);
            using (var statistics = JsonDocument.Parse(add.Statistics!))
            {
                Assert.Equal(stored, statistics.RootElement.GetProperty("minValues").GetProperty("small").GetInt64());
                Assert.Equal(stored, statistics.RootElement.GetProperty("maxValues").GetProperty("small").GetInt64());
            }
            var user = stream.Users[3];
            stream.DeleteUser(user);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Users.Select(x => (long)x.UserKey).Order(), await LiveKeys(storage, "test"));
        }

        [Fact]
        public async Task AnUnmatchedDeleteFailsTheCheckpointAndLeavesNoFiles()
        {
            const string table = "unmatched_delete";
            // Owned, so the in-memory store deletes the attempt's files by name
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{table}"));
            long version;
            List<string> before;
            Exception failure;
            AcceptanceTests.Entities.User user;
            await using (var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Every row rolls a file, so the attempt writes data and change files before the throw
                options.MaxFileSizeBytes = 1;
            }))
            {
                stream.WaitForUpdateDoesNotRequireDataChange();
                stream.Generate(20);
                await stream.StartStream(@"
                    CREATE TABLE unmatched_delete (
                        userkey INT,
                        name STRING
                    );

                    INSERT INTO unmatched_delete
                    SELECT userkey, firstName as name FROM users
                ");
                await WaitForVersion(storage, table, stream, 0);
                await SettlePublications(stream);

                // Another writer removes every file
                var snapshot = (await DeltaTransactionReader.ReadTable(storage, table))!;
                version = snapshot.Version;
                var removes = snapshot.AddFiles.Select(x => new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = x.Path, DataChange = true, DeletionTimestamp = 0 } }).ToArray();
                await WriteCommit(storage, table, version + 1, removes);
                before = await DataFiles(storage, table);

                // New rows beside the delete, so the failed attempt writes data and change files first
                user = stream.Users[0];
                storage.ClearRequests();
                stream.Generate(20);
                stream.DeleteUser(user);
                failure = await Assert.ThrowsAnyAsync<Exception>(async () => await WaitForVersion(storage, table, stream, version + 2, TimeSpan.FromSeconds(30)));
            }

            // The failed attempt did write files, so the equality below proves the cleanup
            Assert.Contains(storage.Requests, x => x.StartsWith("OpenWrite ", StringComparison.Ordinal) && x.Contains("/part-", StringComparison.Ordinal));
            Assert.Contains(storage.Requests, x => x.StartsWith("OpenWrite ", StringComparison.Ordinal) && x.Contains("/cdc-", StringComparison.Ordinal));
            Assert.Contains("1 deleted rows were not found", failure.ToString());

            // The stream is stopped, so no retry writes while the files are listed
            Assert.Contains($"Could not find any data file that contains the row {{{user.UserKey},", failure.ToString());
            Assert.Contains("deleted rows were not found. Set DeltaLakeOptions.IgnoreUnmatchedDeletes to skip them", failure.ToString());
            Assert.False(await storage.Exists(CommitPath(table, version + 2)));
            Assert.Equal(before, await DataFiles(storage, table));
        }

        [Fact]
        public async Task TheOptOutSkipsAndCountsAnUnmatchedDelete()
        {
            const string table = "unmatched_skipped";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.IgnoreUnmatchedDeletes = true);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(@"
                CREATE TABLE unmatched_skipped (
                    userkey INT,
                    name STRING
                );

                INSERT INTO unmatched_skipped
                SELECT userkey, firstName as name FROM users
            ");
            await WaitForVersion(storage, table, stream, 0);
            await SettlePublications(stream);
            var snapshot = (await DeltaTransactionReader.ReadTable(storage, table))!;
            var removes = snapshot.AddFiles.Select(x => new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = x.Path, DataChange = true, DeletionTimestamp = 0 } }).ToArray();
            await WriteCommit(storage, table, snapshot.Version + 1, removes);

            var user = stream.Users[0];
            stream.Generate(5);
            var added = stream.Users.Skip(20).Select(x => (long)x.UserKey).Order().ToList();
            stream.DeleteUser(user);
            await WaitForVersion(storage, table, stream, snapshot.Version + 2);

            // The commit holds the new rows only, the missing one is counted
            Assert.Equal(added, await LiveKeys(storage, table));
            Assert.Equal(new[] { 1L }, ReadGauge(stream, "flowtide_delta_delete_unmatched_rows"));
        }

        // The foreign file takes the lower id, so an admitted file is scanned before the rows resolve
        [Theory]
        [InlineData(16, false)]
        [InlineData(0, true)]
        public async Task AnAnticorrelatedFileScannedFirstIsOpenedOnlyWhenAdmitted(int threshold, bool opened)
        {
            var table = $"anticorrelated_first_{threshold}";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            var schema = "{\"type\":\"struct\",\"fields\":[" +
                "{\"name\":\"orderkey\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}," +
                "{\"name\":\"userkey\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
            await WriteCommit(storage, table, 0, Protocol(), Metadata(schema));
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
                options.DeleteLimitsOverride = new FlowtideDotNet.Connector.DeltaLake.Internal.Deletes.DeleteLimits() { SelectorThreshold = threshold });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            var first = stream.Orders[0];
            var second = stream.Orders.First(x => x.UserKey != first.UserKey);

            // Written before the stream starts, the catalog gives it id 0
            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("orderkey", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Apache.Arrow.Field("userkey", Apache.Arrow.Types.Int64Type.Default, true))
                .Build();
            var foreign = ForeignRewrite.Write(arrowSchema, new Apache.Arrow.IArrowArray[]
            {
                new Apache.Arrow.Int64Array.Builder().Append(first.OrderKey).Build(),
                new Apache.Arrow.Int64Array.Builder().Append(second.UserKey).Build()
            });
            using (var write = await storage.OpenWrite($"/{table}/anticorrelated.parquet"))
            {
                await write.WriteAsync(foreign);
            }
            var bounds = $"{{\"orderkey\":{first.OrderKey},\"userkey\":{second.UserKey}}}";
            await WriteCommit(storage, table, 1, new DeltaAction()
            {
                Add = new DeltaAddAction()
                {
                    Path = "anticorrelated.parquet",
                    Size = foreign.Length,
                    DataChange = true,
                    PartitionValues = new Dictionary<string, string>(),
                    Statistics = $"{{\"numRecords\":1,\"minValues\":{bounds},\"maxValues\":{bounds},\"nullCount\":{{\"orderkey\":0,\"userkey\":0}}}}"
                }
            });

            await stream.StartStream($@"
                INSERT INTO {table}
                SELECT orderkey, userkey FROM orders
            ");
            await WaitForVersion(storage, table, stream, 2);
            await SettlePublications(stream);

            storage.ClearRequests();
            stream.MockDatabaseForTests.GetOrCreateTable<AcceptanceTests.Entities.Order>("orders").Delete(new[] { first, second });
            await WaitForVersion(storage, table, stream, 3);

            Assert.Equal(opened, storage.Requests.Any(x => x.EndsWith("anticorrelated.parquet", StringComparison.Ordinal)));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EveryPassChecksItsUnmatchedDeletes(bool optOut)
        {
            var table = $"unmatched_passes_{(optOut ? "skipped" : "failed")}";
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                // A file per row, and two deletes per pass
                options.MaxFileSizeBytes = 1;
                options.IgnoreUnmatchedDeletes = optOut;
                options.DeleteLimitsOverride = new Internal.Deletes.DeleteLimits() { RowCap = 2 };
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream($@"
                CREATE TABLE {table} (
                    userkey INT,
                    name STRING
                );

                INSERT INTO {table}
                SELECT userkey, firstName as name FROM users
            ");
            await WaitForVersion(storage, table, stream, 0);
            await SettlePublications(stream);

            // Another writer removes the first and third deleted rows, so each pass misses one
            var deleted = stream.Users.OrderBy(x => x.UserKey).Take(3).ToList();
            var paths = (await ForeignRewrite.LiveRows(storage, table)).ToDictionary(x => long.Parse(x.Row.Split(' ')[0], CultureInfo.InvariantCulture), x => x.Path);
            await WriteCommit(storage, table, 1, new[] { deleted[0], deleted[2] }
                .Select(x => new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = paths[x.UserKey], DataChange = true, DeletionTimestamp = 0 } }).ToArray());
            stream.DeleteUsers(deleted);

            if (!optOut)
            {
                // The first pass holds a match and a miss, and names the miss
                var failure = await Assert.ThrowsAnyAsync<Exception>(async () => await WaitForVersion(storage, table, stream, 2, TimeSpan.FromSeconds(30)));
                Assert.Contains($"Could not find any data file that contains the row {{{deleted[0].UserKey},", failure.ToString());
                Assert.Contains("1 deleted rows were not found", failure.ToString());
                return;
            }
            await WaitForVersion(storage, table, stream, 2);
            Assert.Equal(stream.Users.Select(x => (long)x.UserKey).Order(), await LiveKeys(storage, table));
            Assert.Equal(new[] { 2L }, ReadGauge(stream, "flowtide_delta_delete_unmatched_rows"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ADeleteInTheCommitThatCreatesTheTableReachesTheCheck(bool optOut)
        {
            var table = $"unmatched_new_{(optOut ? "skipped" : "failed")}";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.IgnoreUnmatchedDeletes = optOut);
            // Without normalization a retraction no insert preceded reaches the sink
            stream.SourceImmutable();
            stream.Generate(10);
            var ghost = new AcceptanceTests.Entities.User() { UserKey = -1, FirstName = "ghost" };
            stream.MockDatabaseForTests.GetOrCreateTable<AcceptanceTests.Entities.User>("users").Delete(new[] { ghost });
            await stream.StartStream($@"
                CREATE TABLE {table} (
                    userkey INT,
                    name STRING
                );

                INSERT INTO {table}
                SELECT userkey, firstName as name FROM users
            ");

            if (!optOut)
            {
                var failure = await Assert.ThrowsAnyAsync<Exception>(async () => await WaitForVersion(storage, table, stream, 0, TimeSpan.FromSeconds(30)));
                Assert.Contains("Could not find any data file that contains the row {-1,ghost}", failure.ToString());
                return;
            }
            await WaitForVersion(storage, table, stream, 0);
            Assert.Equal(stream.Users.Select(x => (long)x.UserKey).Order(), await LiveKeys(storage, table));
            Assert.Equal(new[] { 1L }, ReadGauge(stream, "flowtide_delta_delete_unmatched_rows"));
        }

        private static async Task<List<long>> LiveKeys(IFileStorage storage, string table)
        {
            return (await ForeignRewrite.LiveRows(storage, table)).Select(x => long.Parse(x.Row.Split(' ')[0], CultureInfo.InvariantCulture)).Order().ToList();
        }

        // The key and decimal of a version's change rows of one type
        private static async Task<List<string>> ChangeRowValues(IFileStorage storage, string table, long version, string changeType)
        {
            var rows = new List<string>();
            foreach (var action in await ReadCommitActions(storage, table, version))
            {
                if (action.Cdc == null)
                {
                    continue;
                }
                using var memory = new MemoryStream(await ReadBytes(storage, $"/{table}/{action.Cdc.Path}"));
                using var reader = new ParquetSharp.Arrow.FileReader(memory);
                using var batches = reader.GetRecordBatchReader();
                Apache.Arrow.RecordBatch batch;
                while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        var keys = (Apache.Arrow.Int64Array)batch.Column(0);
                        var values = (Apache.Arrow.Decimal128Array)batch.Column(1);
                        var changes = (Apache.Arrow.StringArray)batch.Column("_change_type");
                        for (int i = 0; i < changes.Length; i++)
                        {
                            if (changes.GetString(i) == changeType)
                            {
                                rows.Add($"{keys.GetValue(i)} {values.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture)}");
                            }
                        }
                    }
                }
            }
            return rows.Order(StringComparer.Ordinal).ToList();
        }

        // Data and change files under the table
        private static async Task<List<string>> DataFiles(IFileStorage storage, string table)
        {
            var entries = await storage.Ls($"/{table}/", recurse: true);
            return entries.Select(x => x.Name).Where(x => x.StartsWith("part-", StringComparison.Ordinal) || x.StartsWith("cdc-", StringComparison.Ordinal)).Order(StringComparer.Ordinal).ToList();
        }
    }
}
