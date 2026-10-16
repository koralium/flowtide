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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using System.Collections.Concurrent;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DiscardPathTests
    {
        [Fact]
        public async Task DiscardedCdcNeverReachesStage()
        {
            var testName = nameof(DiscardedCdcNeverReachesStage);
            // Owned, so the in-memory store below is classified: ineligible without the assertion, so discards are deleted inline by exact name
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            DeltaLakeSink? sink = null;
            var attempts = new ConcurrentQueue<string>();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 500;
                options.SinkCreatedForTests = created => sink = created;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            Assert.Equal(DiscardMode.Inline, sink!.DiscardModeForTests);

            // A reader at version 0 replays version 1 incrementally, from its commit
            await using var reader = new DeltaLakeTestStream(testName + "_reader", storage.Inner, oneVersionPerCheckpoint: false);
            await reader.StartStream("INSERT INTO result SELECT userkey, name FROM test");
            await ReaderSees(reader, stream);

            // Inserts only: every change data file of version 1 is discarded, its rows are in the add actions
            storage.Before = (verb, path) =>
            {
                if (verb == "Rm" && path.Full.Contains("/_change_data/"))
                {
                    attempts.Enqueue(path.Full);
                    if (attempts.Count == 2)
                    {
                        throw new IOException("Injected delete failure");
                    }
                }
                return Task.CompletedTask;
            };
            var leftBefore = (await storage.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder).Select(x => x.Path.Full).ToHashSet();
            stream.Generate(1000);
            await WaitForVersion(storage.Inner, "test", stream, 1);

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage.Inner, "test", 1);
            Assert.Empty(commit!.CdcFiles);
            Assert.NotEmpty(commit.AddedFiles);
            var created = new List<string>();
            await CommitHeaderReader.ReadAsync(storage.Inner, CommitPath("test", 1), HeaderMode.Strict, created.Add);
            Assert.DoesNotContain(created, x => x.StartsWith("_change_data/"));

            // Every path was tried; only the failed one is left behind
            Assert.True(attempts.Count >= 4, $"{attempts.Count} change data files");
            Assert.Equal(attempts.Count, attempts.Distinct().Count());
            var left = (await storage.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder && !leftBefore.Contains(x.Path.Full)).ToList();
            Assert.Equal(attempts.ElementAt(1), Assert.Single(left).Path.Full);
            Assert.Equal(1, sink!.DiscardDeleteFailures);

            // The incremental reader and a new one both see the rows of the adds
            await ReaderSees(reader, stream);
            await AssertReaderResult(testName, storage.Inner, "test", stream.Users.Select(x => new { userkey = x.UserKey, name = x.FirstName }));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        private static async Task ReaderSees(DeltaLakeTestStream reader, DeltaLakeSinkStream stream)
        {
            var expected = stream.Users.Select(x => new { userkey = x.UserKey, name = x.FirstName }).ToList();
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (true)
            {
                await reader.SchedulerTick();
                await Task.Delay(10);
                try
                {
                    reader.AssertCurrentDataEqual(expected);
                    return;
                }
                catch (Exception) when (DateTime.UtcNow < deadline)
                {
                }
            }
        }
    }
}
