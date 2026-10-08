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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeCatalogTests
    {
        private readonly ITestOutputHelper _output;

        public DeltaLakeCatalogTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private static string Insert(string table) => UserInsert.Replace("test", table);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CatalogEqualsTheLogUnderARandomWorkload(bool changeData)
        {
            var table = $"catalog_equiv_{(changeData ? "cdf" : "plain")}";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            var check = new CatalogCheck(storage, table);
            using var registration = check.Register();
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.WriteChangeDataOnNewTables = changeData;
                options.MaxFileSizeBytes = 2048;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(100);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);

            var random = new Random(changeData ? 21 : 22);
            for (int round = 0; round < 12; round++)
            {
                // Few deletes write deletion vectors, many rewrite the file
                var deletes = random.Next(3) == 0 ? 25 : random.Next(1, 3);
                foreach (var user in stream.Users.OrderBy(_ => random.Next()).Take(deletes).ToList())
                {
                    stream.DeleteUser(user);
                }
                stream.Generate(random.Next(0, 20));
                await RunCheckpoints(stream, 2);
                if (round == 5)
                {
                    await stream.Crash();
                }
                if (round == 8)
                {
                    // Another writer commits between checkpoints, the sink reads the table again
                    var head = (await DeltaTransactionReader.ReadTable(storage, table))!.Version;
                    await WriteCommit(storage, table, head + 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                }
            }
            stream.Generate(1);
            await RunCheckpoints(stream, 2);

            check.AssertClean(minimumChecks: 10);
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task OverwriteReplacesEveryFileOfTheCatalog()
        {
            const string table = "catalog_overwrite";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            // An earlier writer left files the overwrite replaces
            await using (var earlier = new DeltaLakeSinkStream(table + "_earlier", storage, options => options.MaxFileSizeBytes = 512))
            {
                earlier.WaitForUpdateDoesNotRequireDataChange();
                earlier.Generate(60);
                await earlier.StartStream(Insert(table));
                await WaitForVersion(storage, table, earlier, 0);
                await SettlePublications(earlier);
            }

            var check = new CatalogCheck(storage, table);
            using var registration = check.Register();
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.MaxFileSizeBytes = 512);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(30);
            await stream.StartStream($@"
                INSERT OVERWRITE {table}
                SELECT userKey AS userkey, firstName AS name FROM users
            ");
            await WaitForVersion(storage, table, stream, 1);
            var overwrite = await ReadCommitActions(storage, table, 1);
            Assert.NotEmpty(overwrite.Where(x => x.Remove != null));

            // Deletes after the overwrite only find the new files
            foreach (var user in stream.Users.Take(3).ToList())
            {
                stream.DeleteUser(user);
            }
            stream.Generate(5);
            await RunCheckpoints(stream, 3);

            check.AssertClean(minimumChecks: 2);
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task ADeleteReadsOnlyTheFilesItsStatisticsAllow()
        {
            const string table = "catalog_pruning";
            var inner = Files.Of.InternalMemory($"./{table}");
            var storage = new HookFileStorage(inner);
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.MaxFileSizeBytes = 256;
                options.CheckpointInterval = 0;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            // Keys arrive in order, each small file covers its own key range
            stream.Generate(400);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);
            var files = (await DeltaTransactionReader.ReadTable(storage, table))!.AddFiles.Count;
            Assert.True(files > 10, $"{files} files");

            storage.ClearRequests();
            stream.DeleteUser(stream.Users[200]);
            await WaitForVersion(storage, table, stream, 1);

            var dataReads = storage.Requests.Count(x => x.StartsWith("OpenRead ") && x.EndsWith(".parquet") && !x.Contains("_delta_log"));
            Assert.InRange(dataReads, 1, 2);
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task CatalogSpillsUnderASmallCache()
        {
            const string table = "catalog_spill";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            var check = new CatalogCheck(storage, table);
            using var registration = check.Register();
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.MaxFileSizeBytes = 256);
            stream.CachePageCount = 4;
            stream.MinCachePageCount = 1;
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(3000);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);

            var random = new Random(23);
            long peakSpill = 0;
            for (int round = 0; round < 6; round++)
            {
                foreach (var user in stream.Users.OrderBy(_ => random.Next()).Take(30).ToList())
                {
                    stream.DeleteUser(user);
                }
                await RunCheckpoints(stream, 2);
                peakSpill = Math.Max(peakSpill, CatalogSpillBytes(table));
            }

            check.AssertClean(minimumChecks: 5);
            Assert.True(check.MaxLiveFiles > 40, $"{check.MaxLiveFiles} files");
            Assert.True(peakSpill > 0, "the catalog never spilled");
            await AssertTableHolds(table, storage, stream);
        }

        // Catalog trees are never committed, evicted pages go to the client's temporary file cache
        private static long CatalogSpillBytes(string testName)
        {
            try
            {
                var directory = new DirectoryInfo($"./data/tempFiles/{testName}/tmp");
                return directory.Exists ? directory.EnumerateFiles("*catalog_*").Sum(x => x.Length) : 0;
            }
            catch (IOException)
            {
                return 0;
            }
        }

        [Fact]
        public async Task TreeModeDeletesCorrectlyAndRotates()
        {
            // No room for even the first column, the bounds live in the spillable tree
            const string table = "catalog_tree_mode";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            var check = new CatalogCheck(storage, table);
            using var registration = check.Register();
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.ReservationOverride = new PruningReservation();
                options.PruningMemoryBytes = 1;
                options.MaxFileSizeBytes = 512;
                options.CatalogRotationFloor = 30;
                options.CatalogMigrationSlice = 7;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(400);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);

            var random = new Random(24);
            for (int round = 0; round < 15; round++)
            {
                // Single deletes write deletion vectors, a burst rewrites files and frees their ids
                var deletes = round % 4 == 3 ? 40 : 1;
                foreach (var user in stream.Users.OrderBy(_ => random.Next()).Take(deletes).ToList())
                {
                    stream.DeleteUser(user);
                }
                stream.Generate(random.Next(5, 30));
                await RunCheckpoints(stream, 2);
            }

            check.AssertClean(minimumChecks: 10);
            Assert.True(check.SawTreeMode);
            Assert.True(check.MaxRotations > 0, "no rotation");
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task RotationKeepsTheCatalogEqualToTheLog()
        {
            const string table = "catalog_rotation";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            var check = new CatalogCheck(storage, table);
            using var registration = check.Register();
            await using var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.MaxFileSizeBytes = 512;
                options.CatalogRotationFloor = 30;
                options.CatalogMigrationSlice = 5;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(300);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);

            var random = new Random(25);
            for (int round = 0; round < 20; round++)
            {
                foreach (var user in stream.Users.OrderBy(_ => random.Next()).Take(round % 3 == 0 ? 30 : 1).ToList())
                {
                    stream.DeleteUser(user);
                }
                stream.Generate(random.Next(5, 25));
                await RunCheckpoints(stream, 2);
            }

            check.AssertClean(minimumChecks: 15);
            Assert.True(check.MaxRotations >= 2, $"rotations {check.MaxRotations}");
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task AFirstColumnThatDoesNotFitRevokesTheExtraColumnsOfAHaltedTable()
        {
            // Rows of 64 files: the first column is 18 bytes, both columns 53
            var reservation = new PruningReservation();
            const long capacity = 64 * 53 + 64 * 18 - 1;
            const string first = "revoke_first";
            const string second = "revoke_second";
            var firstInner = Files.Of.InternalMemory($"./{first}");
            var firstStorage = new HookFileStorage(firstInner);
            var firstLogs = new TestLogCollector();
            var firstCheck = new CatalogCheck(firstStorage, first);
            using var firstRegistration = firstCheck.Register();
            await using var firstStream = new DeltaLakeSinkStream(first, firstStorage, options =>
            {
                options.ReservationOverride = reservation;
                options.PruningMemoryBytes = capacity;
            });
            firstStream.AddLoggerProvider(firstLogs);
            firstStream.WaitForUpdateDoesNotRequireDataChange();
            firstStream.Generate(20);
            await firstStream.StartStream(Insert(first));
            await WaitForVersion(firstStorage, first, firstStream, 0);
            DeleteOneAndAddOne(firstStream);
            await WaitForVersion(firstStorage, first, firstStream, 1);
            Assert.Equal(64 * 53, reservation.Charged);

            // The first table halts on a foreign commit
            ForeignCommitBeforePublish(firstStorage, firstInner, first, 2);
            firstStream.Generate(5);
            await WaitUntil(firstStream, () => firstLogs.Errors.Count >= 1);

            var secondStorage = new HookFileStorage(Files.Of.InternalMemory($"./{second}"));
            var secondLogs = new TestLogCollector();
            var secondCheck = new CatalogCheck(secondStorage, second);
            using var secondRegistration = secondCheck.Register();
            await using var secondStream = new DeltaLakeSinkStream(second, secondStorage, options =>
            {
                options.ReservationOverride = reservation;
                // Ignored, the first registration fixed the capacity
                options.PruningMemoryBytes = 7;
            });
            secondStream.AddLoggerProvider(secondLogs);
            secondStream.WaitForUpdateDoesNotRequireDataChange();
            secondStream.Generate(20);
            await secondStream.StartStream(Insert(second));
            await WaitForVersion(secondStorage, second, secondStream, 0);
            DeleteOneAndAddOne(secondStream);
            await WaitForVersion(secondStorage, second, secondStream, 1);
            Assert.Single(secondLogs.Warnings, x => x.Contains("differs from the process wide pruning memory"));
            Assert.True(secondCheck.SawTreeMode);

            // The halted table gives up its second column at its next checkpoint
            await RunCheckpoints(firstStream, 1);
            Assert.Equal(64 * 18, reservation.Charged);

            // Room again, the second table stays in tree mode for this run
            DeleteOneAndAddOne(secondStream);
            await WaitForVersion(secondStorage, second, secondStream, 2);
            Assert.True(secondCheck.LastTreeMode);

            // Also after another writer's commit makes the catalog read the table again
            await WriteCommit(secondStorage, second, 3, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
            DeleteOneAndAddOne(secondStream);
            await WaitForVersion(secondStorage, second, secondStream, 4);
            DeleteOneAndAddOne(secondStream);
            await WaitForVersion(secondStorage, second, secondStream, 5);
            Assert.True(secondCheck.LastTreeMode);

            // Rows that wait while halted are written after the resume
            firstStream.Generate(3);
            await RunCheckpoints(firstStream, 2);
            await firstInner.Rm(CommitPath(first, 2));
            await WaitForVersionCheckpointing(firstStorage, first, firstStream, 3);
            Assert.Equal(1, firstCheck.LastScanColumns);
            firstCheck.AssertClean(minimumChecks: 2);
            secondCheck.AssertClean(minimumChecks: 2);
            await AssertTableHolds(first, firstStorage, firstStream);
            await AssertTableHolds(second, secondStorage, secondStream);
        }

        private static void DeleteOneAndAddOne(FlowtideTestStream stream)
        {
            foreach (var user in stream.Users.Take(1).ToList())
            {
                stream.DeleteUser(user);
            }
            stream.Generate(1);
        }

        [Fact]
        public async Task ADataCheckpointListsTheLogOnceAndReadsNoLogFile()
        {
            const string table = "catalog_requests";
            var inner = Files.Of.InternalMemory($"./{table}");
            var storage = new HookFileStorage(inner);
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(50);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);
            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 1);

            // Idle checkpoints cost nothing
            await RunCheckpoints(stream, 3);
            storage.ClearRequests();
            await RunCheckpoints(stream, 3);
            Assert.Empty(storage.Requests);

            // A delete and new rows: one listing, no log or checkpoint reads besides the publication target
            stream.DeleteUser(stream.Users[0]);
            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 2);
            await RunCheckpoints(stream, 1);
            var requests = storage.Requests;
            _output.WriteLine(string.Join(Environment.NewLine, requests));
            Assert.Single(requests, x => x.StartsWith("Ls ") && x.Contains("_delta_log"));
            Assert.All(requests.Where(x => x.StartsWith("OpenRead ") && x.Contains("_delta_log/0")), x => Assert.EndsWith(CommitPath(table, 2), x));

            // A foreign commit is absorbed with one listing
            await WriteCommit(inner, table, 3, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
            storage.ClearRequests();
            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 4);
            Assert.Single(storage.Requests, x => x.StartsWith("Ls ") && x.Contains("_delta_log"));
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task AForeignAdvanceWithCleanupIsNeverWrittenBelow()
        {
            const string table = "catalog_stale_head";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);
            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 1);

            // Another writer goes to 4 with a checkpoint at 3, its cleanup keeps 3.json and later
            for (long version = 2; version <= 4; version++)
            {
                await WriteCommit(storage, table, version, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
            }
            await DeltaCheckpointWriter.WriteCheckpoint(storage, table, (await DeltaTransactionReader.ReadTable(storage, table, 3))!);
            await storage.Rm(CommitPath(table, 1));
            await storage.Rm(CommitPath(table, 2));
            await storage.Rm($"/{table}/_delta_log/00000000000000000000.json");

            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 5);

            Assert.False(await storage.Exists(CommitPath(table, 2)));
            Assert.DoesNotContain(await DeltaTransactionReader.ListHiddenLogFiles(storage, table), x => x.Name.Contains("00000000000000000002"));
            await AssertTableHolds(table, storage, stream);
        }

        [Fact]
        public async Task OnlyUnsupportedLogFilesFailTheCheckpointInsteadOfCreatingTheTable()
        {
            const string table = "catalog_unsupported";
            var inner = Files.Of.InternalMemory($"./{table}");
            var storage = new HookFileStorage(inner);
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);
            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 1);

            // Only a UUID named checkpoint is left
            await inner.Rm(CommitPath(table, 0));
            await inner.Rm(CommitPath(table, 1));
            await WriteText(inner, $"/{table}/_delta_log/00000000000000000001.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet", "x");
            storage.ClearRequests();

            stream.Generate(5);
            var failure = await Assert.ThrowsAnyAsync<Exception>(async () => await WaitForVersion(storage, table, stream, 2, TimeSpan.FromSeconds(30)));

            Assert.Contains("only has log files this reader does not support", failure.ToString());
            Assert.False(await inner.Exists(CommitPath(table, 0)));
            Assert.DoesNotContain(await DeltaTransactionReader.ListHiddenLogFiles(inner, table), x => x.Name.StartsWith(".00000000000000000000"));
        }

        [Fact]
        public async Task ADeleteNoFileCanHoldStillFailsTheCheckpoint()
        {
            const string table = "catalog_unmatched";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);
            await SettlePublications(stream);

            // Another writer removes every file
            var snapshot = (await DeltaTransactionReader.ReadTable(storage, table))!;
            var removes = snapshot.AddFiles.Select(x => new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = x.Path, DataChange = true, DeletionTimestamp = 0 } }).ToArray();
            await WriteCommit(storage, table, snapshot.Version + 1, removes);

            var user = stream.Users[0];
            stream.DeleteUser(user);
            var failure = await Assert.ThrowsAnyAsync<Exception>(async () => await WaitForVersion(storage, table, stream, snapshot.Version + 2, TimeSpan.FromSeconds(30)));

            Assert.Contains($"Could not find any data file that contains the row {{{user.UserKey},", failure.ToString());
        }

        [Fact]
        public async Task CatalogGaugesAreRegisteredOnceAcrossARecovery()
        {
            const string table = "catalog_gauges";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            await using var stream = new DeltaLakeSinkStream(table, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(Insert(table));
            await WaitForVersion(storage, table, stream, 0);
            await stream.Crash();
            stream.Generate(5);
            await WaitForVersion(storage, table, stream, 1);
            await SettlePublications(stream);

            var readings = new List<int>();
            using (var listener = new MeterListener())
            {
                listener.InstrumentPublished = (instrument, meterListener) =>
                {
                    if (instrument.Meter.Name.StartsWith($"flowtide.{stream.StreamName}.operator.", StringComparison.Ordinal) && instrument.Name == "flowtide_delta_catalog_files")
                    {
                        meterListener.EnableMeasurementEvents(instrument);
                    }
                };
                listener.SetMeasurementEventCallback<int>((instrument, value, tags, state) => readings.Add(value));
                listener.Start();
                listener.RecordObservableInstruments();
            }

            var files = (await DeltaTransactionReader.ReadTable(storage, table))!.AddFiles.Count;
            Assert.Equal(new[] { files }, readings);
        }

        [Theory]
        [InlineData(false, 4)]
        [InlineData(true, 4)]
        [InlineData(false, 60)]
        [InlineData(true, 60)]
        public async Task ChurnMeasurements(bool legacySink, int cachePages)
        {
            // Reported, not asserted, the allocation figure is process wide so run it alone
            var table = $"{(legacySink ? "churn_legacy" : "catalog_churn")}_{cachePages}";
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{table}"));
            DeltaSinkCatalog? catalog = null;
            using var registration = new Registration(table, c => { catalog = c; return Task.CompletedTask; });
            var stream = new DeltaLakeSinkStream(table, storage, options =>
            {
                options.MaxFileSizeBytes = 1024;
                options.CheckpointInterval = 0;
                options.CatalogRotationFloor = 200;
                options.CatalogMigrationSlice = 50;
            }, legacySink: legacySink);
            var running = true;
            try
            {
                stream.CachePageCount = cachePages;
                stream.MinCachePageCount = 1;
                stream.WaitForUpdateDoesNotRequireDataChange();
                stream.Generate(2000);
                await stream.StartStream(Insert(table));
                await WaitForVersion(storage, table, stream, 0);

                var random = new Random(26);
                var worstRound = TimeSpan.Zero;
                var total = TimeSpan.Zero;
                long peakSpill = 0;
                var progress = new RoundProgress();
                var allocatedBefore = GC.GetTotalAllocatedBytes(true);
                storage.ClearRequests();
                for (int round = 0; round < 25; round++)
                {
                    foreach (var user in stream.Users.OrderBy(_ => random.Next()).Take(20).ToList())
                    {
                        stream.DeleteUser(user);
                    }
                    stream.Generate(20);
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    await WaitForRound(storage, table, stream, progress, stream.Users.Max(x => x.UserKey));
                    worstRound = watch.Elapsed > worstRound ? watch.Elapsed : worstRound;
                    total += watch.Elapsed;
                    peakSpill = Math.Max(peakSpill, CatalogSpillBytes(table));
                }
                // Everything generated is published before the counters are read
                await SettlePublications(stream);
                var allocated = GC.GetTotalAllocatedBytes(true) - allocatedBefore;
                var requests = storage.Requests;
                var measuredHead = requests.Select(CommitWritten).Max();
                // Stopped before the check, late work is then either measured or a failure below
                await stream.DisposeAsync();
                running = false;
                int files;
                using (storage.Unrecorded())
                {
                    // No data after the measured head, so the figures cover the whole workload
                    for (var version = measuredHead + 1; await storage.Exists(CommitPath(table, version)); version++)
                    {
                        Assert.DoesNotContain(await ReadCommitActions(storage, table, version), x => x.Add != null || x.Remove != null || x.Cdc != null);
                    }
                    files = (await DeltaTransactionReader.ReadTable(storage, table))!.AddFiles.Count;
                }
                _output.WriteLine($"{(legacySink ? "legacy" : "catalog")} cache {cachePages}: files {files}, rotations {catalog?.Rotations}, migrated {catalog?.RecordsMigrated}, last clear {catalog?.LastClearTime}, " +
                    $"worst round {worstRound.TotalMilliseconds:F0} ms, total {total.TotalMilliseconds:F0} ms, allocated {allocated / 1024 / 1024} MiB, peak catalog spill {peakSpill / 1024} KiB, requests {requests.Count}, " +
                    $"listings {requests.Count(x => x.StartsWith("Ls "))}, log reads {requests.Count(x => x.StartsWith("OpenRead ") && x.Contains("_delta_log"))}, " +
                    $"data reads {requests.Count(x => x.StartsWith("OpenRead ") && x.EndsWith(".parquet") && !x.Contains("_delta_log"))}");
                await AssertTableEquals(table, storage, stream.Users);
            }
            finally
            {
                if (running)
                {
                    await stream.DisposeAsync();
                }
            }
        }

        private sealed class RoundProgress
        {
            public long NextVersion { get; set; } = 1;

            public long PublishedMaxKey { get; set; } = long.MinValue;
        }

        // A round ends when a commit holds its newest key, keys only grow and its deletes came before its inserts
        private static async Task WaitForRound(HookFileStorage storage, string table, FlowtideTestStream stream, RoundProgress progress, long roundMaxKey)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (progress.PublishedMaxKey < roundMaxKey)
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Key {roundMaxKey} of {table} was not published in time");
                }
                await RunCheckpoints(stream, 0);
                using (storage.Unrecorded())
                {
                    while (await storage.Exists(CommitPath(table, progress.NextVersion)))
                    {
                        foreach (var add in (await ReadCommitActions(storage, table, progress.NextVersion)).Where(x => x.Add?.Statistics != null))
                        {
                            progress.PublishedMaxKey = Math.Max(progress.PublishedMaxKey, MaxIntegerBound(add.Add!.Statistics!));
                        }
                        progress.NextVersion++;
                    }
                }
            }
        }

        // The largest integer max value in the statistics, the key is the table's only integer column
        private static long MaxIntegerBound(string statistics)
        {
            using var document = JsonDocument.Parse(statistics);
            var max = long.MinValue;
            if (document.RootElement.TryGetProperty("maxValues", out var maxValues))
            {
                foreach (var property in maxValues.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.Number && property.Value.TryGetInt64(out var value))
                    {
                        max = Math.Max(max, value);
                    }
                }
            }
            return max;
        }

        // The version of a commit file the request wrote, -1 for any other request
        private static long CommitWritten(string request)
        {
            var match = System.Text.RegularExpressions.Regex.Match(request, @"^OpenWrite .*/_delta_log/(\d{20})\.json$");
            return match.Success ? long.Parse(match.Groups[1].Value) : -1;
        }

        private static async Task WriteText(IFileStorage storage, string path, string text)
        {
            using var write = await storage.OpenWrite(path);
            await write.WriteAsync(System.Text.Encoding.UTF8.GetBytes(text));
        }

        private static void ForeignCommitBeforePublish(HookFileStorage storage, IFileStorage inner, string table, long version)
        {
            var written = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath(table, version)) && Interlocked.Exchange(ref written, 1) == 0)
                {
                    await WriteCommit(inner, table, version, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                }
            };
        }

        private static async Task WaitUntil(FlowtideTestStream stream, Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (!condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("Condition was not reached in time");
                }
                await stream.SchedulerTick();
                await Task.Delay(50);
            }
        }

        private static async Task AssertTableHolds(string table, IFileStorage storage, FlowtideTestStream stream)
        {
            await SettlePublications(stream);
            await AssertTableEquals(table, storage, stream.Users);
        }

        private static async Task AssertTableEquals(string table, IFileStorage storage, IEnumerable<User> users)
        {
            await using var reader = new DeltaLakeTestStream(table + "_compare", storage, oneVersionPerCheckpoint: false);
            await reader.StartStream($"INSERT INTO result SELECT userkey, name FROM {table}");
            await reader.WaitForUpdate();
            reader.AssertCurrentDataEqual(users.Select(x => new { x.UserKey, x.FirstName }));
        }

        private sealed class Registration : IDisposable
        {
            private readonly string _table;

            public Registration(string table, Func<DeltaSinkCatalog, Task> hook)
            {
                _table = table;
                DeltaLakeSink.CatalogHooksForTests[table] = hook;
            }

            public void Dispose()
            {
                DeltaLakeSink.CatalogHooksForTests.TryRemove(_table, out _);
            }
        }

        /// <summary>
        /// Compares the catalog with the log at the catalog's head, on the sink's thread.
        /// </summary>
        private sealed class CatalogCheck
        {
            private readonly IFileStorage _storage;
            private readonly string _table;
            private readonly ConcurrentQueue<string> _failures = new ConcurrentQueue<string>();
            private int _checks;

            public CatalogCheck(IFileStorage storage, string table)
            {
                _storage = storage;
                _table = table;
            }

            public int MaxLiveFiles { get; private set; }

            public int MaxRotations { get; private set; }

            public bool SawTreeMode { get; private set; }

            public bool LastTreeMode { get; private set; }

            public int LastScanColumns { get; private set; }

            public Registration Register()
            {
                return new Registration(_table, Check);
            }

            private async Task Check(DeltaSinkCatalog catalog)
            {
                try
                {
                    if (catalog.Header == null)
                    {
                        return;
                    }
                    var snapshot = await DeltaTransactionReader.ReadTable(_storage, _table, catalog.Head);
                    var expected = snapshot!.AddFiles.ToDictionary(x => x.Path!, x => Normalize(x));
                    var actual = new Dictionary<string, string>();
                    await foreach (var (_, record) in catalog.ScanFiles())
                    {
                        actual[record.Path] = Normalize(record.ToAdd());
                    }
                    var missing = expected.Keys.Where(x => !actual.ContainsKey(x)).ToList();
                    var extra = actual.Keys.Where(x => !expected.ContainsKey(x)).ToList();
                    var changed = expected.Where(x => actual.TryGetValue(x.Key, out var value) && value != x.Value).Select(x => $"{x.Value} != {actual[x.Key]}").ToList();
                    if (missing.Count > 0 || extra.Count > 0 || changed.Count > 0)
                    {
                        _failures.Enqueue($"version {catalog.Head}: missing [{string.Join(",", missing)}] extra [{string.Join(",", extra)}] changed [{string.Join(" | ", changed)}]");
                    }
                    MaxLiveFiles = Math.Max(MaxLiveFiles, catalog.LiveFiles);
                    MaxRotations = Math.Max(MaxRotations, catalog.Rotations);
                    SawTreeMode |= catalog.TreeMode;
                    LastTreeMode = catalog.TreeMode;
                    LastScanColumns = catalog.ScanLayout.Columns.Count;
                    if (catalog.LiveFiles != expected.Count)
                    {
                        _failures.Enqueue($"version {catalog.Head}: {catalog.LiveFiles} live files counted, {expected.Count} in the log");
                    }
                    Interlocked.Increment(ref _checks);
                }
                catch (Exception e)
                {
                    _failures.Enqueue(e.ToString());
                }
            }

            private static string Normalize(DeltaAddAction add)
            {
                add.DataChange = true;
                // A checkpoint reads an absent map back as an empty one
                add.Tags = add.Tags?.Count > 0 ? add.Tags : null;
                add.PartitionValues = add.PartitionValues?.Count > 0 ? add.PartitionValues : null;
                return JsonSerializer.Serialize(add);
            }

            public void AssertClean(int minimumChecks)
            {
                Assert.True(_failures.IsEmpty, string.Join("\n", _failures));
                Assert.True(Volatile.Read(ref _checks) >= minimumChecks, $"{_checks} checks");
            }
        }
    }
}
