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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Core.ColumnStore;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSourceHistoryTests
    {
        private const string DeletionLogs = "table_with_deletion_logs";
        private const string Cdc = "simple_table_with_cdc";

        // commitInfo.timestamp of the test tables
        private const long DeletionLogsV0 = 1690885056892;
        private const long DeletionLogsV3 = 1690885062611;
        private const long DeletionLogsV4 = 1690885064445;
        private const long DeletionLogsV20 = 1690885085712;
        private const long CdcV1 = 1671962472835;
        private const long CdcV2 = 1671962475765;

        private static readonly Action<DeltaLakeOptions> History = o => o.IncludeChangeHistory = true;

        private static readonly Action<DeltaLakeOptions> HistoryFromFirstVersion = o =>
        {
            o.IncludeChangeHistory = true;
            o.StartFromFirstVersion = true;
        };

        [Fact]
        public async Task DeletionVectorDeletesBecomeInserts()
        {
            await using var stream = new DeltaLakeTestStream(nameof(DeletionVectorDeletesBecomeInserts), Files.Of.LocalDisk("../../../testdata"), recordPublished: true, configure: History);
            await stream.StartStream($"INSERT INTO output SELECT id, __weight, __modificationDate, __version FROM {DeletionLogs}");

            var rows = ReadRows(await stream.WaitForPublished(1));
            Assert.Equal(100, rows.Count);
            Assert.All(rows, x => Assert.Equal<(long, long?, long)>((1, DeletionLogsV0, 0), (x.Weight, x.Time, x.Version)));

            rows = ReadRows(await stream.WaitForPublished(2));
            Assert.Equal(101, rows.Count);
            var delete = Assert.Single(rows, x => x.Weight == -1);
            Assert.Equal<(long, long?, long)>((12, DeletionLogsV3, 3), (delete.Id, delete.Time, delete.Version));

            rows = ReadRows(await stream.WaitForPublished(3));
            Assert.Equal(102, rows.Count);
            var deletes = rows.Where(x => x.Weight == -1).OrderBy(x => x.Version).Select(x => (x.Id, x.Time, x.Version)).ToList();
            Assert.Equal<(long, long?, long)>([(12, DeletionLogsV3, 3), (89, DeletionLogsV4, 4)], deletes);
        }

        [Fact]
        public async Task CdcUpdateIsDeleteAndInsert()
        {
            await using var stream = new DeltaLakeTestStream(nameof(CdcUpdateIsDeleteAndInsert), Files.Of.LocalDisk("../../../testdata"), recordPublished: true, configure: History);
            await stream.StartStream($"INSERT INTO output SELECT id, name, __weight, __modificationDate, __version FROM {Cdc}");

            Assert.Empty(await stream.WaitForPublished(1));
            await stream.AssertPublishedEqual(2, new[] { Row(0, "Mario", 1, CdcV1, 1) });
            await stream.AssertPublishedEqual(3, new[]
            {
                Row(0, "Mario", 1, CdcV1, 1),
                Row(0, "Mario", -1, CdcV2, 2),
                Row(0, "Mino", 1, CdcV2, 2)
            });
        }

        [Fact]
        public async Task UpdateOfUnselectedColumnSendsNothing()
        {
            await using var stream = new DeltaLakeTestStream(nameof(UpdateOfUnselectedColumnSendsNothing), Files.Of.LocalDisk("../../../testdata"), recordPublished: true, configure: History);
            await stream.StartStream($"INSERT INTO output SELECT id, __weight, __version FROM {Cdc}");

            await stream.AssertPublishedEqual(2, new[] { new { id = 0L, weight = 1L, version = 1L } });

            // Pre and post image only differ in name, only the insert of version 1 was sent
            await stream.AssertPublishedEqual(3, new[] { new { id = 0L, weight = 1L, version = 1L } });
            Assert.Equal(1, stream.ChangeRowsReceived);
        }

        [Fact]
        public async Task LatestSnapshotGetsItsCommitTime()
        {
            await using var stream = new DeltaLakeTestStream(nameof(LatestSnapshotGetsItsCommitTime), Files.Of.LocalDisk("../../../testdata"), oneVersionPerCheckpoint: false, recordPublished: true, configure: History);
            await stream.StartStream($"INSERT INTO output SELECT id, __weight, __modificationDate, __version FROM {DeletionLogs}");

            var rows = ReadRows(await stream.WaitForPublished(1));
            Assert.Equal(98, rows.Count);
            Assert.All(rows, x => Assert.Equal<(long, long?, long)>((1, DeletionLogsV20, 20), (x.Weight, x.Time, x.Version)));
        }

        [Fact]
        public async Task WithoutCommitInfoTheAddTimeIsUsed()
        {
            var storage = await CopyToMemory(nameof(WithoutCommitInfoTheAddTimeIsUsed), Cdc);
            await RewriteCommit(storage, Cdc, 1, x => x.CommitInfo == null);
            await RewriteCommit(storage, Cdc, 2, x => x.CommitInfo == null);

            await using var stream = new DeltaLakeTestStream(nameof(WithoutCommitInfoTheAddTimeIsUsed), storage, oneVersionPerCheckpoint: false, configure: HistoryFromFirstVersion);
            await stream.StartStream($"INSERT INTO output SELECT id, name, __weight, __modificationDate, __version FROM {Cdc}");

            await WaitForRows(stream, new[]
            {
                Row(0, "Mario", 1, 1671962472000, 1),
                Row(0, "Mario", -1, 1671962475000, 2),
                Row(0, "Mino", 1, 1671962475000, 2)
            });
        }

        [Fact]
        public async Task CommitWithOnlyRemovesUsesDeletionTimestamp()
        {
            var storage = await CopyToMemory(nameof(CommitWithOnlyRemovesUsesDeletionTimestamp), Cdc);
            await RewriteCommit(storage, Cdc, 2, x => x.CommitInfo == null && x.Add == null);

            await using var stream = new DeltaLakeTestStream(nameof(CommitWithOnlyRemovesUsesDeletionTimestamp), storage, oneVersionPerCheckpoint: false, configure: HistoryFromFirstVersion);
            await stream.StartStream($"INSERT INTO output SELECT id, name, __weight, __modificationDate, __version FROM {Cdc}");

            await WaitForRows(stream, new[]
            {
                Row(0, "Mario", 1, CdcV1, 1),
                Row(0, "Mario", -1, 1671962475760, 2),
                Row(0, "Mino", 1, 1671962475760, 2)
            });
        }

        [Fact]
        public async Task CommitWithoutAnyTimeHasNullDate()
        {
            var storage = await CopyToMemory(nameof(CommitWithoutAnyTimeHasNullDate), Cdc);
            await RewriteCommit(storage, Cdc, 2, x =>
            {
                if (x.Remove != null)
                {
                    x.Remove.DeletionTimestamp = null;
                }
                return x.CommitInfo == null && x.Add == null;
            });

            await using var stream = new DeltaLakeTestStream(nameof(CommitWithoutAnyTimeHasNullDate), storage, oneVersionPerCheckpoint: false, configure: HistoryFromFirstVersion);
            await stream.StartStream($"INSERT INTO output SELECT id, name, __weight, __modificationDate, __version FROM {Cdc}");

            await WaitForRows(stream, new[]
            {
                Row(0, "Mario", 1, CdcV1, 1),
                Row(0, "Mario", -1, null, 2),
                Row(0, "Mino", 1, null, 2)
            });
        }

        [Fact]
        public async Task IdenticalDeletesKeepTheirCountAsWeight()
        {
            var storage = await CopyToMemory(nameof(IdenticalDeletesKeepTheirCountAsWeight), DeletionLogs);
            // Removes the 98 rows left, only history columns are selected so all of them are identical
            var add = (await ReadCommitActions(storage, DeletionLogs, 4)).Single(x => x.Add != null).Add!;
            await WriteCommit(storage, DeletionLogs, 21, new DeltaAction()
            {
                Remove = new DeltaRemoveFileAction()
                {
                    Path = add.Path,
                    DataChange = true,
                    DeletionVector = add.DeletionVector,
                    PartitionValues = add.PartitionValues,
                    Size = add.Size,
                    DeletionTimestamp = 1700000000000
                }
            });

            await using var stream = new DeltaLakeTestStream(nameof(IdenticalDeletesKeepTheirCountAsWeight), storage, oneVersionPerCheckpoint: false, configure: HistoryFromFirstVersion);
            await stream.StartStream($"INSERT INTO output SELECT __weight, __modificationDate, __version FROM {DeletionLogs}");

            var expected = Enumerable.Repeat(HistoryRow(1, DeletionLogsV0, 0), 100)
                .Append(HistoryRow(-1, DeletionLogsV3, 3))
                .Append(HistoryRow(-1, DeletionLogsV4, 4))
                .Concat(Enumerable.Repeat(HistoryRow(-1, 1700000000000, 21), 98));
            await WaitForRows(stream, expected);
        }

        [Fact]
        public async Task RestoreDoesNotSendAVersionAgain()
        {
            var name = nameof(RestoreDoesNotSendAVersionAgain);
            var storage = await CopyToMemory(name, Cdc);
            var version2 = await ReadCommitActions(storage, Cdc, 2);
            await storage.Rm(CommitPath(Cdc, 2));
            var stateFiles = new KeepAliveMemoryFileProvider();
            var sql = $"INSERT INTO output SELECT id, name, __weight, __modificationDate, __version FROM {Cdc}";

            await using (var first = new DeltaLakeTestStream(name, storage, oneVersionPerCheckpoint: false, stateFiles: stateFiles, configure: HistoryFromFirstVersion))
            {
                await first.StartStream(sql);
                await WaitForRows(first, new[] { Row(0, "Mario", 1, CdcV1, 1) });
                await first.StopStream();
            }

            await WriteCommit(storage, Cdc, 2, version2.ToArray());

            await using var second = new DeltaLakeTestStream(name, storage, oneVersionPerCheckpoint: false, stateFiles: stateFiles, configure: HistoryFromFirstVersion);
            await second.StartStream(sql);
            await WaitForRows(second, new[]
            {
                Row(0, "Mario", 1, CdcV1, 1),
                Row(0, "Mario", -1, CdcV2, 2),
                Row(0, "Mino", 1, CdcV2, 2)
            });
        }

        [Fact]
        public async Task StartFromFirstVersionWithoutHistoryEndsAtTheLatestRows()
        {
            await using var stream = new DeltaLakeTestStream(nameof(StartFromFirstVersionWithoutHistoryEndsAtTheLatestRows), Files.Of.LocalDisk("../../../testdata"), oneVersionPerCheckpoint: false, configure: o => o.StartFromFirstVersion = true);
            await stream.StartStream($"INSERT INTO output SELECT id FROM {DeletionLogs}");

            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            var ids = new List<long>();
            while (ids.Count != 98)
            {
                Assert.True(DateTime.UtcNow < deadline, $"{ids.Count} rows");
                await stream.SchedulerTick();
                await Task.Delay(10);
                // The sink disposes a result when it publishes the next one
                try
                {
                    var data = stream.GetActualRowsAsVectors();
                    ids = Enumerable.Range(0, data.Count).Select(i => data.Columns[0].GetValueAt(i, default).AsLong).ToList();
                }
                catch (Exception) when (DateTime.UtcNow < deadline)
                {
                }
            }

            // The first snapshot and both deletes were sent, the result is the latest version
            Assert.DoesNotContain(12L, ids);
            Assert.DoesNotContain(89L, ids);
            Assert.Equal(102, stream.ChangeRowsReceived);
        }

        [Fact]
        public async Task HistoryColumnsNeedTheOption()
        {
            await using var stream = new DeltaLakeTestStream(nameof(HistoryColumnsNeedTheOption), Files.Of.LocalDisk("../../../testdata"));
            await Assert.ThrowsAnyAsync<Exception>(() => stream.StartStream($"INSERT INTO output SELECT id, __weight FROM {Cdc}"));
        }

        [Fact]
        public async Task TableWithAHistoryColumnNameIsRejected()
        {
            var storage = await CopyToMemory(nameof(TableWithAHistoryColumnNameIsRejected), Cdc);
            await RewriteCommit(storage, Cdc, 0, x =>
            {
                if (x.MetaData != null)
                {
                    x.MetaData.SchemaString = x.MetaData.SchemaString!.Replace("\"fields\":[", "\"fields\":[{\"name\":\"__weight\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},");
                }
                return true;
            });

            await using var stream = new DeltaLakeTestStream(nameof(TableWithAHistoryColumnNameIsRejected), storage, configure: History);
            await stream.StartStream($"INSERT INTO output SELECT id FROM {Cdc}");
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => stream.WaitForUpdate());
            Assert.Contains("__weight", failure.ToString());
        }

        public sealed record NamedRow(long Id, string Name, long Weight, DateTimeOffset? Date, long Version);

        public sealed record HistoryOnlyRow(long Weight, DateTimeOffset Date, long Version);

        private static NamedRow Row(long id, string name, long weight, long? time, long version)
        {
            return new NamedRow(id, name, weight, time.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(time.Value) : null, version);
        }

        private static HistoryOnlyRow HistoryRow(long weight, long time, long version)
        {
            return new HistoryOnlyRow(weight, DateTimeOffset.FromUnixTimeMilliseconds(time), version);
        }

        // Rows of id, weight, date as unix ms, version
        private static List<(long Id, long Weight, long? Time, long Version)> ReadRows(EventBatchData data)
        {
            var rows = new List<(long, long, long?, long)>();
            for (int i = 0; i < data.Count; i++)
            {
                var date = data.Columns[2].GetValueAt(i, default);
                long? time = date.Type == ArrowTypeId.Null ? null : new DateTimeOffset(date.AsTimestamp.ticks, TimeSpan.Zero).ToUnixTimeMilliseconds();
                rows.Add((data.Columns[0].GetValueAt(i, default).AsLong, data.Columns[1].GetValueAt(i, default).AsLong, time, data.Columns[3].GetValueAt(i, default).AsLong));
            }
            return rows;
        }

        // Versions may arrive in any number of checkpoints
        private static async Task WaitForRows<T>(DeltaLakeTestStream stream, IEnumerable<T> expected)
        {
            var rows = expected.ToList();
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (true)
            {
                await stream.SchedulerTick();
                await Task.Delay(10);
                try
                {
                    stream.AssertCurrentDataEqual(rows);
                    return;
                }
                catch (Exception) when (DateTime.UtcNow < deadline)
                {
                }
            }
        }

        // The log can then be changed, checksums are left out
        private static async Task<IFileStorage> CopyToMemory(string name, string table)
        {
            var disk = Files.Of.LocalDisk("../../../testdata");
            var memory = Files.Of.InternalMemory($"./{name}");
            foreach (var entry in await disk.Ls(new IOPath($"/{table}/"), recurse: true))
            {
                if (entry.Path.IsFolder || entry.Name.EndsWith(".crc"))
                {
                    continue;
                }
                var bytes = await ReadBytes(disk, entry.Path.Full);
                using var target = await memory.OpenWrite(entry.Path);
                await target.WriteAsync(bytes);
            }
            return memory;
        }

        // Keeps the actions the filter accepts, it may also change them
        private static async Task RewriteCommit(IFileStorage storage, string table, long version, Func<DeltaAction, bool> keep)
        {
            var actions = await ReadCommitActions(storage, table, version);
            await WriteCommit(storage, table, version, actions.Where(keep).ToArray());
        }
    }
}
