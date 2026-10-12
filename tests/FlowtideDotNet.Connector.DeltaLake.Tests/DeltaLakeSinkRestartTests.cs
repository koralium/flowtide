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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSinkRestartTests
    {
        // Same users in the same order, so a new stream's source resumes at the restored offset
        private static readonly List<User> Users = Enumerable.Range(1, 30).Select(i => new User() { UserKey = i, FirstName = $"user{i}" }).ToList();

        [Fact]
        public async Task NewStreamsWhileHaltedAndAfterTheDrainKeepEveryRowOnce()
        {
            var testName = nameof(NewStreamsWhileHaltedAndAfterTheDrainKeepEveryRowOnce);
            var stateFiles = new KeepAliveMemoryFileProvider();
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);

            await using (var first = NewStream(testName, storage, stateFiles, out var logs))
            {
                AddUsers(first, 0, 10);
                await first.StartStream(UserInsert);
                await WaitForVersion(storage, "test", first, 0);

                ForeignCommitBeforePublish(storage, inner, 1);
                AddUsers(first, 10, 15);
                await WaitUntil(first, () => logs.Errors.Count >= 1);
                // Rows that wait while halted, later checkpoints move the source past them
                AddUsers(first, 15, 20);
                await first.WaitForUpdate();
                await first.WaitForUpdate();
                await first.StopStream();
            }

            await using (var second = NewStream(testName, storage, stateFiles, out var logs))
            {
                AddUsers(second, 0, 20);
                await second.StartStream(UserInsert);
                await WaitUntil(second, () => logs.Errors.Count >= 1);

                await inner.Rm(CommitPath("test", 1));
                await WaitForVersionCheckpointing(storage, "test", second, 2);
                AddUsers(second, 20, 25);
                await WaitForVersion(storage, "test", second, 3);
                await second.StopStream();
            }

            await using (var third = NewStream(testName, storage, stateFiles, out _))
            {
                AddUsers(third, 0, 25);
                await third.StartStream(UserInsert);
                AddUsers(third, 25, 30);
                await WaitForVersion(storage, "test", third, 4);
                await third.StopStream();
            }

            await AssertTableHolds(testName, storage, Users);
        }

        [Fact]
        public async Task StateOfTheLegacySinkIsRestored()
        {
            var testName = nameof(StateOfTheLegacySinkIsRestored);
            var stateFiles = new KeepAliveMemoryFileProvider();
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);

            await using (var legacy = new DeltaLakeSinkStream(testName, storage, stateFiles: stateFiles, legacySink: true))
            {
                legacy.WaitForUpdateDoesNotRequireDataChange();
                AddUsers(legacy, 0, 10);
                await legacy.StartStream(UserInsert);
                await WaitForVersion(storage, "test", legacy, 0);
                AddUsers(legacy, 10, 15);
                await WaitForVersion(storage, "test", legacy, 1);

                // The stop stages version 2 and its publication fails, the next start publishes it
                var failures = 0;
                storage.Before = (verb, path) =>
                {
                    if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 2)))
                    {
                        Interlocked.Increment(ref failures);
                        throw new IOException("Simulated storage failure");
                    }
                    return Task.CompletedTask;
                };
                await legacy.StopStream();
                Assert.True(Volatile.Read(ref failures) >= 1);
                storage.Before = null;
            }
            Assert.False(await inner.Exists(CommitPath("test", 2)));
            Assert.Single(await Internal.Delta.DeltaTransactionReader.ListHiddenLogFiles(inner, "test"));

            await using (var current = NewStream(testName, storage, stateFiles, out var logs))
            {
                AddUsers(current, 0, 15);
                await current.StartStream(UserInsert);
                await WaitForVersion(storage, "test", current, 2);
                AddUsers(current, 15, 20);
                await WaitForVersion(storage, "test", current, 3);
                Assert.Empty(logs.Errors);
                await current.StopStream();
            }

            // The legacy stage is published as it was written, the first commit after it carries a stage id
            Assert.Null((await ReadCommitActions(storage, "test", 2))[0].CommitInfo?.StageId);
            var first = (await ReadCommitActions(storage, "test", 3))[0].CommitInfo!;
            Assert.NotNull(first.StageId);
            await AssertTableHolds(testName, storage, Users.Take(20));
        }

        [Fact]
        public async Task HaltEpisodesOrphanAtMostOneEmptyRootEach()
        {
            // Clear does not delete committed pages, every buffered page must be deleted before the tree is emptied
            var testName = nameof(HaltEpisodesOrphanAtMostOneEmptyRootEach);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var state = default(CountingPersistentStorage);
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage, wrapState: x => state = new CountingPersistentStorage(x));
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await RunCheckpoints(stream, 3);
            Assert.Equal(0, SinkTreePages(state!));

            long head = 0;
            const int episodes = 3;
            for (int episode = 0; episode < episodes; episode++)
            {
                ForeignCommitBeforePublish(storage, inner, head + 1);
                stream.Generate(10);
                await WaitUntil(stream, () => logs.Errors.Count >= episode + 1);
                // Enough waiting rows for several tree pages
                stream.Generate(3000);
                await stream.WaitForUpdate();
                await stream.WaitForUpdate();

                storage.ClearRequests();
                await inner.Rm(CommitPath("test", head + 1));
                var removed = !await inner.Exists(CommitPath("test", head + 1));
                try
                {
                    await WaitForVersionCheckpointing(storage, "test", stream, head + 2);
                }
                catch (TimeoutException e)
                {
                    var log = (await inner.Ls("/test/_delta_log/")).Select(x => x.Name).Order();
                    var target = await inner.ReadText(CommitPath("test", head + 1));
                    var requests = storage.Requests.Where(x => x.Contains($"{head + 1:D20}") || x.Contains($"{head + 2:D20}"));
                    throw new TimeoutException($"Episode {episode}, removed {removed}, errors: {string.Join(" | ", logs.Errors)}, log: {string.Join(", ", log)}, target: {target?.Substring(0, Math.Min(200, target.Length))}, requests: {string.Join(", ", requests)}", e);
                }
                head += 2;
                await RunCheckpoints(stream, 3);
            }

            // The tree metadata, its current root and the root each earlier episode left behind
            Assert.InRange(SinkTreePages(state!), 2, episodes + 1);
        }

        private static int SinkTreePages(CountingPersistentStorage state)
        {
            return state.LivePagesOf(origin => origin.Contains("DeltaLakeSink.InitializeOrRestore") && origin.Contains("GetOrCreateTree"));
        }

        private static DeltaLakeSinkStream NewStream(string testName, HookFileStorage storage, KeepAliveMemoryFileProvider stateFiles, out TestLogCollector logs)
        {
            var stream = new DeltaLakeSinkStream(testName, storage, stateFiles: stateFiles);
            logs = new TestLogCollector();
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            return stream;
        }

        private static void AddUsers(FlowtideTestStream stream, int from, int to)
        {
            for (int i = from; i < to; i++)
            {
                stream.AddOrUpdateUser(Users[i]);
            }
        }

        // Another writer takes the version right before the sink publishes it
        private static void ForeignCommitBeforePublish(HookFileStorage storage, IFileStorage inner, long version)
        {
            var written = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", version)) && Interlocked.Exchange(ref written, 1) == 0)
                {
                    await WriteCommit(inner, "test", version, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
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

        private static async Task AssertTableHolds(string testName, IFileStorage storage, IEnumerable<User> users)
        {
            await using var reader = new DeltaLakeTestStream(testName + "_compare", storage, oneVersionPerCheckpoint: false);
            await reader.StartStream("INSERT INTO result SELECT userkey, name FROM test");
            await reader.WaitForUpdate();
            reader.AssertCurrentDataEqual(users.Select(x => new { x.UserKey, x.FirstName }));
        }
    }
}
