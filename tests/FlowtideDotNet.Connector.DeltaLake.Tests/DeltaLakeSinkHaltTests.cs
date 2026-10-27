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
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSinkHaltTests
    {
        [Fact]
        public async Task ForeignCommitHaltsTheSinkAndKeepsItsRowsAcrossRestarts()
        {
            var testName = nameof(ForeignCommitHaltsTheSinkAndKeepsItsRowsAcrossRestarts);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // Another writer takes version 1 right before the sink publishes it
            var foreignWritten = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 1)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                {
                    await WriteCommit(inner, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                }
            };
            var foreign = new Func<Task<byte[]>>(() => ReadBytes(inner, CommitPath("test", 1)));
            stream.Generate(5);
            await WaitUntil(stream, () => logs.Errors.Count >= 1);
            var foreignBytes = await foreign();

            await stream.HealthyFor(TimeSpan.FromMilliseconds(500));
            Assert.Equal(0, stream.FailureNotificationCount);
            Assert.Equal(0, SinkHealth(stream));
            Assert.Single(logs.Errors);

            // Rows that arrive while halted, a completed checkpoint moves the source past them
            stream.Generate(5);
            await stream.WaitForUpdate();
            await stream.WaitForUpdate();

            await stream.Crash();
            await WaitUntil(stream, () => logs.Errors.Count >= 2);
            Assert.Equal(foreignBytes, await foreign());

            // The other writer's commit is removed, the sink publishes and continues
            await inner.Rm(CommitPath("test", 1));
            stream.Generate(1);
            await WaitForVersionCheckpointing(storage, "test", stream, 2);
            Assert.Equal(1, SinkHealth(stream));

            // A restart after the resume must not write the waiting rows again
            await stream.Crash();
            stream.Generate(1);
            await WaitForVersionCheckpointing(storage, "test", stream, 3);

            await AssertTableEquals(storage, stream, testName);
        }

        [Fact]
        public async Task BulkIntakeOnARestoredBufferMatchesPerRow()
        {
            var off = await HaltRestoreAndPublish(nameof(BulkIntakeOnARestoredBufferMatchesPerRow) + "_off", bulk: false);
            var on = await HaltRestoreAndPublish(nameof(BulkIntakeOnARestoredBufferMatchesPerRow) + "_on", bulk: true);

            Assert.Equal(off.History, on.History);
            Assert.Contains("cdc ", on.History[2]);
            Assert.Equal(0, off.RestoredBulkRows);
            Assert.True(on.RestoredBulkRows > 0);
        }

        [Fact]
        public async Task ARemovedConflictIsNotPublishedOverALaterVersion()
        {
            var testName = nameof(ARemovedConflictIsNotPublishedOverALaterVersion);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // Another writer takes version 1 right before the sink publishes it, and goes on to version 2
            var foreignWritten = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 1)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                {
                    await WriteCommit(inner, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                    await WriteCommit(inner, "test", 2, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                }
            };
            stream.Generate(5);
            await WaitUntil(stream, () => logs.Errors.Count >= 1);

            // Removing version 1 would let the sink's commit sit below the other writer's version 2
            await inner.Rm(CommitPath("test", 1));
            stream.Generate(1);
            await RunCheckpoints(stream, 3);

            Assert.False(await inner.Exists(CommitPath("test", 1)));
            Assert.Contains(logs.Errors, x => x.Contains("a later version"));
            Assert.Equal(0, SinkHealth(stream));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task TornPublishIsRepairedOnRestart()
        {
            var testName = nameof(TornPublishIsRepairedOnRestart);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // The publish of version 1 dies after the stage id is written
            storage.TearAfterBytes = 90;
            storage.TearWriteOf = "/_delta_log/00000000000000000001.json";
            stream.Generate(5);
            await WaitUntil(stream, () => Volatile.Read(ref storage.Tears) == 1 && stream.FailureNotificationCount >= 1);
            stream.Generate(1);
            await WaitForVersionCheckpointing(storage, "test", stream, 2);

            var commit = await DeltaTransactionReader.ReadVersionCommit(inner, "test", 1);
            Assert.NotNull(commit);
            Assert.NotEmpty(commit.AddedFiles);
            await AssertTableEquals(storage, stream, testName);
        }

        [Fact]
        public async Task TemporaryTreeIsCommittedOnlyWhileRowsWait()
        {
            // Clear does not delete committed pages, so a commit per checkpoint would leak one per checkpoint
            var testName = nameof(TemporaryTreeIsCommittedOnlyWhileRowsWait);
            const string table = "tree_commit_count";
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            var commits = 0;
            var hook = HookTreeCommits(name =>
            {
                if (name == table)
                {
                    Interlocked.Increment(ref commits);
                }
            });
            try
            {
                await using var stream = new DeltaLakeSinkStream(testName, storage);
                stream.AddLoggerProvider(logs);
                stream.Generate(10);
                stream.WaitForUpdateDoesNotRequireDataChange();
                await stream.StartStream(UserInsert.Replace("test", table));
                await WaitForVersion(storage, table, stream, 0);
                await RunCheckpoints(stream, 20);
                stream.Generate(5);
                await WaitForVersion(storage, table, stream, 1);
                Assert.Equal(0, Volatile.Read(ref commits));

                var foreignWritten = 0;
                storage.Before = async (verb, path) =>
                {
                    if (verb == "OpenRead" && path.Full.EndsWith(CommitPath(table, 2)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                    {
                        await WriteCommit(inner, table, 2, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                    }
                };
                stream.Generate(5);
                await WaitUntil(stream, () => logs.Errors.Count >= 1);
                stream.Generate(5);
                await stream.WaitForUpdate();
                await stream.WaitForUpdate();
                Assert.True(Volatile.Read(ref commits) >= 1);

                await inner.Rm(CommitPath(table, 2));
                stream.Generate(1);
                await WaitForVersionCheckpointing(storage, table, stream, 3);
                var afterResume = Volatile.Read(ref commits);
                await RunCheckpoints(stream, 20);
                stream.Generate(1);
                await WaitForVersion(storage, table, stream, 4);
                Assert.Equal(afterResume, Volatile.Read(ref commits));
            }
            finally
            {
                hook.Dispose();
            }
        }

        // Buffered rows survive a crash, then more arrive before publishing
        private static async Task<(List<string> History, long RestoredBulkRows)> HaltRestoreAndPublish(string testName, bool bulk)
        {
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Mapped tables get random physical names, so files would differ
                options.EnableColumnMappingOnNewTables = false;
                options.DeleteLimitsOverride = new DeleteLimits() { BulkIntake = bulk };
            });
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            // Fixed rows, the generator's seed is shared by every test
            var users = Enumerable.Range(1, 40).Select(x => new User() { UserKey = x, FirstName = $"First{x}" }).ToList();
            stream.AddUsers(users.Take(10).ToList());
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // Another writer takes version 1 right before the sink publishes it
            var foreignWritten = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 1)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                {
                    await WriteCommit(inner, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                }
            };
            stream.AddUsers(users.Skip(10).Take(10).ToList());
            await WaitUntil(stream, () => logs.Errors.Count >= 1);
            // One source fetch sees both changes
            stream.EnterDataWriteLock();
            stream.AddUsers(users.Skip(20).Take(10).ToList());
            stream.DeleteUsers(users.Take(3).ToList());
            stream.ExitDataWriteLock();
            await stream.WaitForUpdate();
            await stream.WaitForUpdate();

            // The restored sink has a new intake and halts again
            var before = stream.Sinks[^1].Intake;
            await stream.Crash();
            await WaitUntil(stream, () => stream.Sinks[^1].Intake != before && logs.Errors.Count(x => x.Contains("stopped publishing")) >= 2);
            var restored = stream.Sinks[^1].Intake!;
            // A halted restore completes no checkpoint, so wait on the intake
            stream.EnterDataWriteLock();
            stream.AddUsers(users.Skip(30).ToList());
            stream.DeleteUsers(users.Skip(20).Take(2).ToList());
            stream.ExitDataWriteLock();
            await WaitUntil(stream, () => restored.BulkRows + restored.PerRowRows >= 12);

            await inner.Rm(CommitPath("test", 1));
            await WaitForVersionCheckpointing(storage, "test", stream, 2);
            return (await TableHistory(storage, "test", 2), restored.BulkRows);
        }

        // Adds a handler to the sink's static hook beside those of tests running in parallel, until disposed
        internal static IDisposable HookTreeCommits(Action<string> handler)
        {
            UpdateTreeCommitHook(x => (Action<string>?)Delegate.Combine(x, handler));
            return new TreeCommitHandler(handler);
        }

        private static void UpdateTreeCommitHook(Func<Action<string>?, Action<string>?> change)
        {
            while (true)
            {
                var current = Volatile.Read(ref DeltaLakeSink.TemporaryTreeCommitHookForTests);
                if (Interlocked.CompareExchange(ref DeltaLakeSink.TemporaryTreeCommitHookForTests, change(current), current) == current)
                {
                    return;
                }
            }
        }

        private sealed class TreeCommitHandler : IDisposable
        {
            private readonly Action<string> _handler;

            public TreeCommitHandler(Action<string> handler)
            {
                _handler = handler;
            }

            public void Dispose() => UpdateTreeCommitHook(x => (Action<string>?)Delegate.Remove(x, _handler));
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

        private static decimal SinkHealth(FlowtideTestStream stream)
        {
            var node = stream.GetDiagnosticsGraph().Nodes.Values.Single(x => x.DisplayName.StartsWith("DeltaLakeSink("));
            return node.Gauges.First(x => x.Name == "flowtide_health").Dimensions[""].Value;
        }

        // Every user exactly once, read back through the source from the latest snapshot
        private static async Task AssertTableEquals(IFileStorage storage, DeltaLakeSinkStream stream, string testName)
        {
            await using var reader = new DeltaLakeTestStream(testName + "_compare", storage, oneVersionPerCheckpoint: false);
            await reader.StartStream("INSERT INTO result SELECT userkey, name FROM test");
            await reader.WaitForUpdate();
            reader.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName }));
        }
    }
}
