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
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeEpochTests
    {
        private static async Task<CommitHeader> Header(IFileStorage storage, long version)
        {
            return (await CommitHeaderReader.ReadAsync(storage, CommitPath("test", version), HeaderMode.Strict))!;
        }

        private static async Task Publish(IFileStorage storage, DeltaLakeSinkStream stream, long version)
        {
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, version);
        }

        [Fact]
        public async Task EpochNewTableIsZero()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(EpochNewTableIsZero)}");
            await using var stream = new DeltaLakeSinkStream(nameof(EpochNewTableIsZero), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await Publish(storage, stream, 1);

            foreach (var version in new[] { 0L, 1L })
            {
                var header = await Header(storage, version);
                Assert.True(header.IsCommitInfo);
                Assert.Equal(0, header.AdoptedAt);
                Assert.True(CommitProvenance.From(version, header).IsOwn);
            }
        }

        // A table a pre-Phase 2 sink or a Phase 3 sink wrote, neither records an epoch
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task EpochPrePhase4HeadStartsAtOwnVersion(bool legacySink)
        {
            var testName = nameof(EpochPrePhase4HeadStartsAtOwnVersion) + legacySink;
            var storage = Files.Of.InternalMemory($"./{testName}");
            if (legacySink)
            {
                await using var old = new DeltaLakeSinkStream(testName + "-old", storage, legacySink: true);
                old.Generate(10);
                await old.StartStream(UserInsert);
                await WaitForVersion(storage, "test", old, 0);
                await Publish(storage, old, 1);
            }
            else
            {
                // A Phase 3 commit: a stage id, a timestamp and an operation, no epoch
                await WriteCommit(storage, "test", 0, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = new string('c', 32), Timestamp = 5, Operation = "CREATE TABLE" } }, Protocol(), Metadata(UserSchema));
                await WriteCommit(storage, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = new string('d', 32), Timestamp = 6, Operation = "WRITE" } });
            }

            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 2);
            await Publish(storage, stream, 3);

            Assert.Equal(2, (await Header(storage, 2)).AdoptedAt);
            Assert.Equal(2, (await Header(storage, 3)).AdoptedAt);
        }

        [Fact]
        public async Task EpochForeignHeadStartsAtOwnVersion()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(EpochForeignHeadStartsAtOwnVersion)}");
            await using var stream = new DeltaLakeSinkStream(nameof(EpochForeignHeadStartsAtOwnVersion), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await Publish(storage, stream, 1);
            // Another writer commits; it may even copy a stage id, without an epoch it is not this lineage
            await WriteCommit(storage, "test", 2, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = new string('f', 32), Operation = "FOREIGN" } });

            await Publish(storage, stream, 3);
            await Publish(storage, stream, 4);

            Assert.Equal(0, (await Header(storage, 1)).AdoptedAt);
            Assert.Equal(3, (await Header(storage, 3)).AdoptedAt);
            Assert.Equal(3, (await Header(storage, 4)).AdoptedAt);
        }

        [Fact]
        public async Task EpochFromAnInvalidOwnHeadStartsAtOwnVersion()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(EpochFromAnInvalidOwnHeadStartsAtOwnVersion)}");
            // An epoch above its own version cannot be an own lineage
            await WriteCommit(storage, "test", 0, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = new string('c', 32), AdoptedAt = 7, Timestamp = 5, Operation = "CREATE TABLE" } }, Protocol(), Metadata(UserSchema));

            await using var stream = new DeltaLakeSinkStream(nameof(EpochFromAnInvalidOwnHeadStartsAtOwnVersion), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);

            Assert.Equal(1, (await Header(storage, 1)).AdoptedAt);
        }

        [Fact]
        public async Task EpochInheritedAcrossRestart()
        {
            var testName = nameof(EpochInheritedAcrossRestart);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using (var first = new DeltaLakeSinkStream(testName, storage))
            {
                first.Generate(10);
                await first.StartStream(UserInsert);
                await WaitForVersion(storage, "test", first, 0);
                await WriteCommit(storage, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Operation = "FOREIGN" } });
                await Publish(storage, first, 2);
            }
            Assert.Equal(2, (await Header(storage, 2)).AdoptedAt);

            // A new sink bootstraps its catalog from the log and inherits the epoch of its own head
            await using var second = new DeltaLakeSinkStream(testName + "-2", storage);
            second.Generate(5);
            await second.StartStream(UserInsert);
            await WaitForVersion(storage, "test", second, 3);

            Assert.Equal(2, (await Header(storage, 3)).AdoptedAt);
        }

        [Fact]
        public async Task HeadCheckpointBootstrapReadsProvenanceOnce()
        {
            var testName = nameof(HeadCheckpointBootstrapReadsProvenanceOnce);
            var inner = Files.Of.InternalMemory($"./{testName}");
            await using (var first = new DeltaLakeSinkStream(testName, inner, options => options.CheckpointInterval = 1))
            {
                first.Generate(10);
                await first.StartStream(UserInsert);
                await WaitForVersion(inner, "test", first, 0);
                await Publish(inner, first, 1);
                await first.Maintenance.TickAndSettleAsync(1);
                Assert.Equal(1, Assert.Single(first.Maintenance.Workers).LastCompleted);
            }

            var storage = new HookFileStorage(inner);
            await using var second = new DeltaLakeSinkStream(testName + "-2", storage, options => options.CheckpointInterval = 0);
            second.Generate(5);
            await second.StartStream(UserInsert);
            // Waits read the inner store, only the sink's requests are recorded
            await WaitForVersion(inner, "test", second, 2);
            await Publish(inner, second, 3);

            // The snapshot is rooted at the head's checkpoint, so the head's first line is read once for its provenance
            Assert.Single(storage.Requests, x => x == $"OpenRead {CommitPath("test", 1)}");
            Assert.Equal(0, (await Header(inner, 2)).AdoptedAt);
            Assert.Equal(0, (await Header(inner, 3)).AdoptedAt);
        }

        [Fact]
        public async Task NoPerCommitGetIsAdded()
        {
            var testName = nameof(NoPerCommitGetIsAdded);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 0);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            await Publish(storage.Inner, stream, 1);

            storage.ClearRequests();
            for (long version = 2; version <= 5; version++)
            {
                await Publish(storage.Inner, stream, version);
            }

            // Steady commits read only their own target at publication, never an earlier head
            var reads = storage.Requests.Where(x => (x.StartsWith("OpenRead ") || x.StartsWith("Exists ")) && x.EndsWith(".json")).ToList();
            Assert.All(reads, x => Assert.Contains(x.Split(' ')[1], Enumerable.Range(2, 4).Select(v => CommitPath("test", v))));
            Assert.Equal(4, reads.Count(x => x.StartsWith("OpenRead ")));
        }
    }
}
