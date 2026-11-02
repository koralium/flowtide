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
using Stowage.Impl;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class LocalCommitPublishTests : IDisposable
    {
        private readonly string _root = Path.Join(Path.GetTempPath(), "commit-" + ScratchOwners.ShortId());
        private readonly IFileStorage _disk;

        public LocalCommitPublishTests()
        {
            Directory.CreateDirectory(_root);
            _disk = Files.Of.LocalDisk(_root);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_root, true);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                // A folder the system still holds is left in temp
            }
        }

        private string Native(string path) => ((ILocalDiskFileStorage)_disk).ToNativeLocalPath(path);

        private static List<DeltaAction> Commit(string stageId)
        {
            return new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = stageId, Data = new Dictionary<string, object>() } },
                Protocol(),
                Metadata(UserSchema)
            };
        }

        [Fact]
        public async Task APublishOnLocalDiskNeverOpensTheCommit()
        {
            var storage = HookFileStorage.Wrap(_disk);
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, Commit("first"));
            var expected = await ReadBytes(_disk, $"/t/_delta_log/{staged.FileName}");
            storage.ClearRequests();

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", Pending(staged, 0), staged.Bytes, false);

            Assert.Equal(PublishOutcome.Published, result.Outcome);
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("OpenWrite "));
            Assert.Equal(expected, await ReadBytes(_disk, CommitPath("t", 0)));
            Assert.False(File.Exists(Native($"/t/_delta_log/{staged.FileName}")));
        }

        [Fact]
        public async Task APublishOnLocalDiskKeepsACommitThatAppeared()
        {
            var storage = HookFileStorage.Wrap(_disk);
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, Commit("first"));
            var foreign = "{\"commitInfo\":{\"engineInfo\":\"other\"}}\n"u8.ToArray();
            // Another writer takes the version after the target was found absent
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(".tmp"))
                {
                    File.WriteAllBytes(Native(CommitPath("t", 0)), foreign);
                }
                return Task.CompletedTask;
            };

            await Assert.ThrowsAsync<IOException>(() => DeltaTransactionWriter.PublishCommit(storage, "t", Pending(staged, 0), null, false));

            Assert.Equal(foreign, await ReadBytes(_disk, CommitPath("t", 0)));
            storage.Before = null;
            var retry = await DeltaTransactionWriter.PublishCommit(storage, "t", Pending(staged, 0), null, false);
            Assert.Equal(PublishOutcome.FailedClosed, retry.Outcome);
            Assert.Equal(foreign, await ReadBytes(_disk, CommitPath("t", 0)));
        }

        [Fact]
        public async Task ATornCommitIsRepairedOnLocalDisk()
        {
            var storage = HookFileStorage.Wrap(_disk);
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, Commit("first"));
            var expected = await ReadBytes(_disk, $"/t/_delta_log/{staged.FileName}");
            File.WriteAllBytes(Native(CommitPath("t", 0)), expected.Take(DeltaTransactionWriter.StagePrefix("first").Length).ToArray());

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", Pending(staged, 0), staged.Bytes, false);

            Assert.Equal(PublishOutcome.Repaired, result.Outcome);
            Assert.Equal(expected, await ReadBytes(_disk, CommitPath("t", 0)));
            Assert.False(File.Exists(Native($"/t/_delta_log/{staged.FileName}")));
        }

        // Local disk fails a listing when a file goes while it lists
        [Fact]
        public async Task AListingRetriesAFileThatWentAway()
        {
            await WriteCommit(_disk, "t", 0, Protocol(), Metadata(UserSchema));
            var storage = HookFileStorage.Wrap(_disk);
            var failures = 2;
            storage.Before = (verb, path) => verb == "Ls" && Interlocked.Decrement(ref failures) >= 0 ? throw new FileNotFoundException("Simulated rename") : Task.CompletedTask;

            var log = await DeltaTransactionReader.ListLog(storage, "t");

            Assert.Equal(0, log.Head);
            Assert.Equal(3, storage.Requests.Count(x => x.StartsWith("Ls ")));
        }

        [Fact]
        public async Task AListingThatKeepsFailingSurfaces()
        {
            await WriteCommit(_disk, "t", 0, Protocol(), Metadata(UserSchema));
            var storage = HookFileStorage.Wrap(_disk);
            storage.Before = (verb, path) => verb == "Ls" ? throw new FileNotFoundException("Simulated rename") : Task.CompletedTask;

            await Assert.ThrowsAsync<FileNotFoundException>(() => DeltaTransactionReader.ListLog(storage, "t"));
        }

        [Fact]
        public async Task ReadingACommitRefusesAnEmptyFile()
        {
            await WriteCommit(_disk, "t", 0, Protocol(), Metadata(UserSchema));
            File.Create(Native(CommitPath("t", 1))).Dispose();

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => DeltaTransactionReader.ReadVersionCommit(_disk, "t", 1));

            Assert.Contains("00000000000000000001.json", error.Message);
        }

        [Fact]
        public async Task ReadingATableRefusesAnEmptyCommit()
        {
            await WriteCommit(_disk, "t", 0, Protocol(), Metadata(UserSchema));
            File.Create(Native(CommitPath("t", 1))).Dispose();

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => DeltaTransactionReader.ReadTable(_disk, "t"));

            Assert.Contains("00000000000000000001.json", error.Message);
        }

        // An empty version must not be remembered and skipped for good
        [Fact]
        public async Task ASourceFailsOnAnEmptyCommitInsteadOfSkippingIt()
        {
            var name = nameof(ASourceFailsOnAnEmptyCommitInsteadOfSkippingIt);
            await using var writer = new DeltaLakeSinkStream($"{name}_writer", _disk, options => options.CheckpointInterval = 0);
            writer.WaitForUpdateDoesNotRequireDataChange();
            writer.AddOrUpdateUser(new FlowtideDotNet.AcceptanceTests.Entities.User() { UserKey = 1, FirstName = "first" });
            await writer.StartStream(UserInsert);
            await WaitForVersion(_disk, "test", writer, 0);
            await SettlePublications(writer);
            await using var source = new DeltaLakeTestStream(name, _disk, oneVersionPerCheckpoint: false, recordPublished: true);
            await source.StartStream("INSERT INTO result SELECT userkey, name FROM test");
            await source.WaitForPublished(1);

            File.Create(Native(CommitPath("test", 1))).Dispose();

            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            Exception? error = null;
            while (error == null)
            {
                Assert.True(DateTime.UtcNow < deadline, "The source did not fail on the empty commit");
                error = await Record.ExceptionAsync(() => source.SchedulerTick());
                await Task.Delay(50);
            }
            Assert.Contains("00000000000000000001.json", error.ToString());
        }

        private static DeltaLakePendingCommit Pending(StagedCommit staged, long version)
        {
            return new DeltaLakePendingCommit() { Version = version, StagedFile = staged.FileName, StageId = staged.StageId, Length = staged.Length };
        }
    }
}
