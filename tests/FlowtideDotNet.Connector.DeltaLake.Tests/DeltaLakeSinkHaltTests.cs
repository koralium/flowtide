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
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
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
            DeltaLakeSink.TemporaryTreeCommitHookForTests = name =>
            {
                if (name == table)
                {
                    Interlocked.Increment(ref commits);
                }
            };
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
                DeltaLakeSink.TemporaryTreeCommitHookForTests = null;
            }
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
