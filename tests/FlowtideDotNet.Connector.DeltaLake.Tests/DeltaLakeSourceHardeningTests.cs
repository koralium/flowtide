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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSourceHardeningTests
    {
        private const string ReadUsers = "INSERT INTO result SELECT userkey, name FROM test";

        [Fact]
        public async Task ARestoreAfterTheLogBelowACheckpointWasRemovedContinues()
        {
            var name = nameof(ARestoreAfterTheLogBelowACheckpointWasRemovedContinues);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name);
            await using var writerScope = writer;

            writer.Generate(5);
            await WaitForVersion(inner, "test", writer, 2);
            await SettlePublications(writer);
            // Cleanup after a checkpoint at the version the source reads next
            await DeltaCheckpointWriter.WriteCheckpoint(inner, "test", (await DeltaTransactionReader.ReadTable(inner, "test", 2))!);
            await inner.Rm(CommitPath("test", 0));
            await inner.Rm(CommitPath("test", 1));

            await using var source = NewSource(name, new HookFileStorage(inner), stateFiles, out var logs);
            await source.StartStream(ReadUsers);
            await WaitForRows(source, writer.Users);

            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
            Assert.Equal(1, SourceHealth(source));
        }

        [Fact]
        public async Task ARemovedNextCommitHaltsTheSourceAtTheFirstMiss()
        {
            var name = nameof(ARemovedNextCommitHaltsTheSourceAtTheFirstMiss);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name);
            await using var writerScope = writer;

            writer.Generate(5);
            await WaitForVersion(inner, "test", writer, 2);
            writer.Generate(5);
            await WaitForVersion(inner, "test", writer, 3);
            await SettlePublications(writer);
            // Retention removed the commit the source reads next, a checkpoint keeps the table readable
            await DeltaCheckpointWriter.WriteCheckpoint(inner, "test", (await DeltaTransactionReader.ReadTable(inner, "test", 3))!);
            for (int version = 0; version <= 2; version++)
            {
                await inner.Rm(CommitPath("test", version));
            }

            var storage = new HookFileStorage(inner);
            await using var source = NewSource(name, storage, stateFiles, out var logs);
            source.WaitForUpdateDoesNotRequireDataChange();
            await source.StartStream(ReadUsers);
            await WaitUntil(source, () => logs.Errors.Count >= 1);

            // Checked on the first miss, not after several polls
            var missed = CommitPath("test", 2);
            Assert.InRange(storage.Requests.Count(x => x == $"Exists {missed}"), 1, 2);
            Assert.Single(logs.Errors, x => x.Contains("commit 2 is missing"));

            storage.ClearRequests();
            await source.HealthyFor(TimeSpan.FromMilliseconds(500));
            Assert.Equal(0, source.FailureNotificationCount);
            Assert.Equal(0, SourceHealth(source));
            Assert.Single(logs.Errors);
            Assert.Empty(storage.Requests);
        }

        [Fact]
        public async Task AnIdleSourceOnATableWithACheckpointDoesNotHalt()
        {
            var storage = Files.Of.LocalDisk("../../../testdata");
            await using var source = new DeltaLakeTestStream(nameof(AnIdleSourceOnATableWithACheckpointDoesNotHalt), storage, oneVersionPerCheckpoint: false);
            var logs = new TestLogCollector();
            source.AddLoggerProvider(logs);
            await source.StartStream("INSERT INTO output SELECT version FROM simple_table_with_checkpoint");
            await source.WaitForUpdate();

            // Polls miss the next commit, the first one lists the log
            await source.HealthyFor(TimeSpan.FromSeconds(1));

            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
            Assert.Equal(1, SourceHealth(source));
        }

        [Fact]
        public async Task ANextCommitPublishedDuringTheCheckIsRead()
        {
            var name = nameof(ANextCommitPublishedDuringTheCheckIsRead);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name);
            await using var writerScope = writer;

            var storage = new HookFileStorage(inner);
            await using var source = NewSource(name, storage, stateFiles, out var logs);
            source.WaitForUpdateDoesNotRequireDataChange();
            await source.StartStream(ReadUsers);
            await WaitForFirstPoll(source, storage);

            // The listing sees version 3 before version 2, version 2 lands before the second look
            var phase = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "Ls" && Interlocked.CompareExchange(ref phase, 1, 0) == 0)
                {
                    await WriteCommit(inner, "test", 3, Foreign());
                }
                else if (verb == "Exists" && path.Full == CommitPath("test", 2) && Interlocked.CompareExchange(ref phase, 2, 1) == 1)
                {
                    await WriteCommit(inner, "test", 2, Foreign());
                }
            };
            await WaitUntil(source, () => Volatile.Read(ref phase) == 2);
            await source.HealthyFor(TimeSpan.FromMilliseconds(500));

            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
            Assert.Equal(1, SourceHealth(source));
            Assert.Contains($"Exists {CommitPath("test", 4)}", storage.Requests);
        }

        [Fact]
        public async Task AMissingDataFileHaltsWithNothingSent()
        {
            var name = nameof(AMissingDataFileHaltsWithNothingSent);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name, options => options.MaxFileSizeBytes = 256);
            await using var writerScope = writer;

            writer.Generate(40);
            await WaitForVersion(inner, "test", writer, 2);
            await SettlePublications(writer);
            // Files before the missing one are read and buffered first
            var added = (await ReadCommitActions(inner, "test", 2)).Where(x => x.Add != null).Select(x => x.Add!.Path!).ToList();
            Assert.True(added.Count >= 2, $"{added.Count} files");
            await inner.Rm($"/test/{added[^1]}");

            var storage = new HookFileStorage(inner);
            await using var source = NewSource(name, storage, stateFiles, out var logs);
            source.WaitForUpdateDoesNotRequireDataChange();
            await source.StartStream(ReadUsers);
            await WaitUntil(source, () => logs.Errors.Count >= 1);

            Assert.Single(logs.Errors, x => x.Contains(added[^1]) && x.Contains("VACUUM"));
            Assert.Contains($"OpenRead /test/{added[0]}", storage.Requests);
            storage.ClearRequests();
            await source.HealthyFor(TimeSpan.FromMilliseconds(500));
            Assert.Equal(0, source.FailureNotificationCount);
            Assert.Equal(0, SourceHealth(source));
            Assert.Empty(storage.Requests);
            Assert.Equal(0, source.ChangeRowsReceived);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        public async Task AMissingChangeDataFileHaltsWithNothingSent(int missing)
        {
            var name = $"{nameof(AMissingChangeDataFileHaltsWithNothingSent)}_{missing}";
            var (inner, stateFiles, writer) = await StartAtVersionOne(name, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 256;
            });
            await using var writerScope = writer;

            // Deletes and inserts write several change data files
            foreach (var user in writer.Users.Take(10).ToList())
            {
                writer.DeleteUser(user);
            }
            writer.Generate(10);
            await WaitForVersion(inner, "test", writer, 2);
            await SettlePublications(writer);
            var cdc = (await ReadCommitActions(inner, "test", 2)).Where(x => x.Cdc != null).Select(x => x.Cdc!.Path!).ToList();
            Assert.True(cdc.Count >= 2, $"{cdc.Count} change data files");
            await inner.Rm($"/test/{cdc[missing]}");

            var storage = new HookFileStorage(inner);
            await using var source = NewSource(name, storage, stateFiles, out var logs);
            source.WaitForUpdateDoesNotRequireDataChange();
            await source.StartStream(ReadUsers);
            await WaitUntil(source, () => logs.Errors.Count >= 1);

            Assert.Single(logs.Errors, x => x.Contains(cdc[missing]));
            await source.HealthyFor(TimeSpan.FromMilliseconds(500));
            Assert.Equal(0, source.FailureNotificationCount);
            Assert.Equal(0, SourceHealth(source));
            Assert.Equal(0, source.ChangeRowsReceived);
        }

        [Fact]
        public async Task AChangeDataFileRemovedAfterRowsWereSentFailsAndTheRetryHalts()
        {
            var name = nameof(AChangeDataFileRemovedAfterRowsWereSentFailsAndTheRetryHalts);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 256;
            });
            await using var writerScope = writer;

            foreach (var user in writer.Users.Take(10).ToList())
            {
                writer.DeleteUser(user);
            }
            writer.Generate(10);
            await WaitForVersion(inner, "test", writer, 2);
            await SettlePublications(writer);
            var cdc = (await ReadCommitActions(inner, "test", 2)).Where(x => x.Cdc != null).Select(x => x.Cdc!.Path!).ToList();
            Assert.True(cdc.Count >= 2, $"{cdc.Count} change data files");

            // Removed after the check found it and after the first file's rows were sent
            var storage = new HookFileStorage(inner);
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full == $"/test/{cdc[1]}" && storage.Requests.Contains($"OpenRead /test/{cdc[0]}"))
                {
                    await inner.Rm(path);
                }
            };
            await using (var first = NewSource(name, storage, stateFiles, out var firstLogs))
            {
                first.WaitForUpdateDoesNotRequireDataChange();
                await first.StartStream(ReadUsers);
                // Rows were sent, so the version fails and is rolled back instead of halting
                var failure = await Assert.ThrowsAnyAsync<Exception>(() => WaitUntil(first, () => false));
                Assert.Contains(nameof(DeltaFileNotFoundException), failure.ToString());
                Assert.Contains($"Exists /test/{cdc[1]}", storage.Requests);
                Assert.DoesNotContain(firstLogs.Errors, x => x.Contains("halted"));
            }

            // The retry from the last checkpoint finds the file missing before sending anything
            var retryStorage = new HookFileStorage(inner);
            await using var retry = NewSource(name, retryStorage, stateFiles, out var logs);
            retry.WaitForUpdateDoesNotRequireDataChange();
            await retry.StartStream(ReadUsers);
            await WaitUntil(retry, () => logs.Errors.Count >= 1);

            Assert.Single(logs.Errors, x => x.Contains("halted") && x.Contains(cdc[1]));
            Assert.Equal(0, retry.ChangeRowsReceived);
            Assert.DoesNotContain($"OpenRead /test/{cdc[0]}", retryStorage.Requests);
        }

        [Fact]
        public async Task ARewriteWithoutDataChangeIsSkipped()
        {
            var name = nameof(ARewriteWithoutDataChangeIsSkipped);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name);
            await using var writerScope = writer;
            var dataFile = (await ReadCommitActions(inner, "test", 0)).First(x => x.Add != null).Add!;

            var storage = new HookFileStorage(inner);
            await using var source = NewSource(name, storage, stateFiles, out var logs);
            source.WaitForUpdateDoesNotRequireDataChange();
            await source.StartStream(ReadUsers);
            await WaitForFirstPoll(source, storage);

            // The same rows in a new file, only dataChange=false actions
            await CopyFile(inner, $"/test/{dataFile.Path}", "/test/rewritten.parquet");
            await WriteCommit(inner, "test", 2,
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = dataFile.Path, DataChange = false, DeletionTimestamp = 0 } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "rewritten.parquet", PartitionValues = new Dictionary<string, string>(), Size = dataFile.Size, ModificationTime = dataFile.ModificationTime, DataChange = false, Statistics = dataFile.Statistics } });
            await WaitUntil(source, () => storage.Requests.Contains($"Exists {CommitPath("test", 3)}"));

            Assert.DoesNotContain("OpenRead /test/rewritten.parquet", storage.Requests);
            Assert.Equal(0, source.ChangeRowsReceived);
            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
        }

        [Fact]
        public void ChangeDataCountsAsADataChangeAlthoughItsActionsSayOtherwise()
        {
            var add = new DeltaAddAction() { Path = "a.parquet", DataChange = true };
            var rewrite = new DeltaAddAction() { Path = "b.parquet", DataChange = false };
            var remove = new DeltaRemoveFileAction() { Path = "c.parquet", DataChange = true };
            var compacted = new DeltaRemoveFileAction() { Path = "d.parquet", DataChange = false };
            // The spec writes cdc actions with dataChange=false
            var cdc = new DeltaCdcAction() { Path = "_change_data/e.parquet", DataChange = false };

            Assert.True(DeltaLakeSource.HasDataChange(new DeltaCommit(new() { add }, new(), new(), null)));
            Assert.True(DeltaLakeSource.HasDataChange(new DeltaCommit(new(), new() { remove }, new(), null)));
            Assert.True(DeltaLakeSource.HasDataChange(new DeltaCommit(new(), new(), new() { cdc }, null)));
            Assert.False(DeltaLakeSource.HasDataChange(new DeltaCommit(new() { rewrite }, new() { compacted }, new(), null)));
            Assert.False(DeltaLakeSource.HasDataChange(new DeltaCommit(new(), new(), new(), null)));
        }

        [Fact]
        public async Task SkippedVersionsAreCheckpointedSoARestoreDoesNotReadThemAgain()
        {
            var name = nameof(SkippedVersionsAreCheckpointedSoARestoreDoesNotReadThemAgain);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name);
            await using var writerScope = writer;

            var firstStorage = new HookFileStorage(inner);
            await using (var first = NewSource(name, firstStorage, stateFiles, out var firstLogs))
            {
                first.WaitForUpdateDoesNotRequireDataChange();
                await first.StartStream(ReadUsers);
                await WaitForFirstPoll(first, firstStorage);
                // Checkpoints of the start are done before the skips
                await HealthyFor(first, firstLogs, TimeSpan.FromMilliseconds(300));
                var checkpointed = first.SinkLastCheckpointDoneVersion;

                for (int version = 2; version <= 4; version++)
                {
                    await WriteCommit(inner, "test", version, Foreign());
                }
                // Only the source's own schedule checkpoints, the test triggers none
                await WaitUntil(first, () => first.SinkLastCheckpointDoneVersion > checkpointed);
                await first.StopStream();
            }

            // Retention removes everything below a checkpoint at the last skipped version
            await DeltaCheckpointWriter.WriteCheckpoint(inner, "test", (await DeltaTransactionReader.ReadTable(inner, "test", 4))!);
            for (int version = 0; version <= 3; version++)
            {
                await inner.Rm(CommitPath("test", version));
            }

            var secondStorage = new HookFileStorage(inner);
            await using var second = NewSource(name, secondStorage, stateFiles, out var logs);
            second.WaitForUpdateDoesNotRequireDataChange();
            await second.StartStream(ReadUsers);
            // Restored after the skipped versions
            await WaitForFirstPoll(second, secondStorage, nextVersion: 5);
            await HealthyFor(second, logs, TimeSpan.FromMilliseconds(500));
            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
            Assert.Equal(1, SourceHealth(second));
        }

        [Fact]
        public async Task ARecoveryAcrossARemovedCommitWithoutACheckpointStartsHalted()
        {
            var name = nameof(ARecoveryAcrossARemovedCommitWithoutACheckpointStartsHalted);
            var (inner, stateFiles, writer) = await StartAtVersionOne(name);
            await using var writerScope = writer;

            // The running source does not see the new versions until it crashes
            var storage = new HookFileStorage(inner);
            storage.Hidden = path => path.Full.StartsWith("/test/_delta_log/") && !path.Full.EndsWith(".json.tmp") && TryCommitVersion(path.Full, out var version) && version >= 2;
            await using var source = NewSource(name, storage, stateFiles, out var logs);
            source.WaitForUpdateDoesNotRequireDataChange();
            await source.StartStream(ReadUsers);
            await WaitForFirstPoll(source, storage);

            writer.Generate(5);
            await WaitForVersion(inner, "test", writer, 2);
            writer.Generate(5);
            await WaitForVersion(inner, "test", writer, 3);
            await SettlePublications(writer);
            // Nothing can rebuild the table past the gap, so a new plan could not even be built
            await inner.Rm(CommitPath("test", 2));

            // The next poll fails, recovery initializes the source again without planning
            var crashed = 0;
            storage.Before = (verb, path) =>
            {
                if (verb == "Exists" && path.Full == CommitPath("test", 2) && Interlocked.Exchange(ref crashed, 1) == 0)
                {
                    storage.Hidden = null;
                    throw new CrashException("crash");
                }
                return Task.CompletedTask;
            };
            await WaitUntil(source, () => logs.Errors.Any(x => x.Contains("halted")));

            Assert.Single(logs.Errors, x => x.Contains("halted") && x.Contains("commit 2 is missing"));
            storage.ClearRequests();
            await source.HealthyFor(TimeSpan.FromMilliseconds(500));
            Assert.Equal(1, source.FailureNotificationCount);
            Assert.Equal(0, SourceHealth(source));
            Assert.Empty(storage.Requests);
            Assert.Equal(0, source.ChangeRowsReceived);
        }

        [Fact]
        public async Task AFileMissingFromTheFirstSnapshotFailsTheStreamInsteadOfHalting()
        {
            var name = nameof(AFileMissingFromTheFirstSnapshotFailsTheStreamInsteadOfHalting);
            var inner = Files.Of.InternalMemory($"./{name}");
            await using var writer = new DeltaLakeSinkStream(name + "_writer", new HookFileStorage(inner), options => options.CheckpointInterval = 0);
            writer.WaitForUpdateDoesNotRequireDataChange();
            writer.Generate(20);
            await writer.StartStream(UserInsert);
            await WaitForVersion(inner, "test", writer, 0);
            await SettlePublications(writer);
            var file = (await ReadCommitActions(inner, "test", 0)).First(x => x.Add != null).Add!.Path!;
            await inner.Rm($"/test/{file}");

            await using var source = new DeltaLakeTestStream(name, new HookFileStorage(inner), oneVersionPerCheckpoint: false);
            await source.StartStream(ReadUsers);
            // The retry reads the newest snapshot again, a halt would stop for good
            var failure = await Assert.ThrowsAnyAsync<Exception>(async () => await source.WaitForUpdate());
            Assert.Contains(nameof(DeltaFileNotFoundException), failure.ToString());
            Assert.Contains(file, failure.ToString());
        }

        // Writer at version 1, a source that read it and checkpointed into the state files
        private static async Task<(IFileStorage Inner, KeepAliveMemoryFileProvider StateFiles, DeltaLakeSinkStream Writer)> StartAtVersionOne(string name, Action<DeltaLakeOptions>? configure = null)
        {
            var inner = Files.Of.InternalMemory($"./{name}");
            var stateFiles = new KeepAliveMemoryFileProvider();
            var writer = new DeltaLakeSinkStream(name + "_writer", new HookFileStorage(inner), options =>
            {
                options.CheckpointInterval = 0;
                configure?.Invoke(options);
            });
            writer.WaitForUpdateDoesNotRequireDataChange();
            writer.Generate(20);
            await writer.StartStream(UserInsert);
            await WaitForVersion(inner, "test", writer, 0);
            writer.Generate(5);
            await WaitForVersion(inner, "test", writer, 1);
            await SettlePublications(writer);

            await using (var source = NewSource(name, new HookFileStorage(inner), stateFiles, out _))
            {
                await source.StartStream(ReadUsers);
                await WaitForRows(source, writer.Users);
                await source.StopStream();
            }
            return (inner, stateFiles, writer);
        }

        private static DeltaLakeTestStream NewSource(string name, IFileStorage storage, KeepAliveMemoryFileProvider stateFiles, out TestLogCollector logs)
        {
            var source = new DeltaLakeTestStream(name, storage, oneVersionPerCheckpoint: false, stateFiles: stateFiles);
            logs = new TestLogCollector();
            source.AddLoggerProvider(logs);
            return source;
        }

        private static async Task WaitForRows(DeltaLakeTestStream source, IEnumerable<User> users)
        {
            var expected = users.Select(x => new { x.UserKey, x.FirstName }).ToList();
            for (int attempt = 0; ; attempt++)
            {
                await source.WaitForUpdate();
                try
                {
                    source.AssertCurrentDataEqual(expected);
                    return;
                }
                catch (Exception) when (attempt < 50)
                {
                }
            }
        }

        private static async Task HealthyFor(FlowtideTestStream stream, TestLogCollector logs, TimeSpan time)
        {
            try
            {
                await stream.HealthyFor(time);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Join(Environment.NewLine, logs.Errors), e);
            }
        }

        // A restored source sends no rows until something changes, its first poll shows it runs
        private static Task WaitForFirstPoll(FlowtideTestStream source, HookFileStorage storage, long nextVersion = 2)
        {
            return WaitUntil(source, () => storage.Requests.Contains($"Exists {CommitPath("test", nextVersion)}"));
        }

        private static bool TryCommitVersion(string path, out long version)
        {
            var name = path.Substring(path.LastIndexOf('/') + 1);
            version = -1;
            return name.Length > 20 && long.TryParse(name.AsSpan(0, 20), out version);
        }

        private static async Task WaitUntil(FlowtideTestStream stream, Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
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

        private static DeltaAction Foreign()
        {
            return new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } };
        }

        private static async Task CopyFile(IFileStorage storage, string from, string to)
        {
            using var source = (await storage.OpenRead(from))!;
            using var copy = new MemoryStream();
            await source.CopyToAsync(copy);
            using var target = await storage.OpenWrite(to);
            await target.WriteAsync(copy.ToArray());
        }

        private static decimal SourceHealth(FlowtideTestStream stream)
        {
            var node = stream.GetDiagnosticsGraph().Nodes.Values.Single(x => x.DisplayName.StartsWith("DeltaLakeTable"));
            return node.Gauges.First(x => x.Name == "flowtide_health").Dimensions[""].Value;
        }
    }
}
