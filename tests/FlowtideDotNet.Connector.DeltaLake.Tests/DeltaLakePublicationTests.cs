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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Text;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakePublicationTests
    {
        private const string Target = "/t/_delta_log/00000000000000000000.json";

        private static byte[] StagedBytes(StagedCommit staged) => staged.Bytes.ToArray();

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AnAbsentTargetBelowALaterVersionIsOnlyWrittenWithoutTheCheck(bool checkSuccessor)
        {
            var (storage, staged, pending) = await Stage(nameof(AnAbsentTargetBelowALaterVersionIsOnlyWrittenWithoutTheCheck) + checkSuccessor);
            await WriteCommit(storage.Inner, "t", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor);

            Assert.Equal(checkSuccessor ? PublishOutcome.FailedClosed : PublishOutcome.Published, result.Outcome);
            Assert.Equal(!checkSuccessor, await storage.Inner.Exists(Target));
            if (checkSuccessor)
            {
                Assert.Contains("a later version", result.Reason);
                Assert.True(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
            }
        }

        [Theory]
        [InlineData(false, "later")]
        [InlineData(false, "checkpoint")]
        [InlineData(false, "compaction")]
        [InlineData(true, "later")]
        [InlineData(true, "checkpoint")]
        public async Task AnAbsentTargetIsNotWrittenBelowAnotherWritersHistory(bool legacy, string history)
        {
            var name = $"{nameof(AnAbsentTargetIsNotWrittenBelowAnotherWritersHistory)}_{legacy}_{history}";
            var (storage, staged, pending) = legacy ? await StageLegacy(name) : await Stage(name);
            switch (history)
            {
                case "later":
                    // Another writer went on to 2 and its cleanup removed 0 and 1, only its checkpoint and commit 2 remain
                    await WriteCommit(storage.Inner, "t", 2, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                    await WriteBytes(storage.Inner, "/t/_delta_log/00000000000000000002.checkpoint.parquet", new byte[] { 1 });
                    break;
                case "checkpoint":
                    // Another writer checkpointed its version 0, then its commit file was removed
                    await WriteBytes(storage.Inner, "/t/_delta_log/00000000000000000000.checkpoint.parquet", new byte[] { 1 });
                    break;
                case "compaction":
                    await WriteBytes(storage.Inner, "/t/_delta_log/00000000000000000000.00000000000000000001.compacted.json", new byte[] { 1 });
                    break;
            }

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, legacy ? null : staged.Bytes, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.False(await storage.Inner.Exists(Target));
            Assert.True(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task ALogCompactionCoversUpToItsSecondVersion()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ALogCompactionCoversUpToItsSecondVersion)}");
            await WriteBytes(storage, "/t/_delta_log/00000000000000000001.00000000000000000003.compacted.json", new byte[] { 1 });
            await WriteBytes(storage, "/t/_delta_log/00000000000000000002.checkpoint.4b6a3c1e-8a52-4f8e-9d33-2c8a5a1f7b10.parquet", new byte[] { 1 });

            var log = await DeltaTransactionReader.ListLog(storage, "t");

            Assert.Equal(3, log.MaxOtherVersion);
        }

        [Fact]
        public async Task APublishedCommitWithALongFirstLineIsRecognizedWithoutItsStage()
        {
            // The commitInfo the branch wrote before Phase 4, longer than 64 KiB
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(APublishedCommitWithALongFirstLineIsRecognizedWithoutItsStage)}"));
            var stageId = Guid.NewGuid().ToString("N");
            var created = Enumerable.Range(0, 3000).Select(x => $"part-00000-{Guid.NewGuid()}.zstd.parquet").ToList();
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = stageId, Data = new Dictionary<string, object>() { ["operation"] = "WRITE", ["timestamp"] = 1L, ["flowtide.adoptedAt"] = 0L, ["flowtide.createdFiles"] = created } } },
                Protocol(),
                Metadata(UserSchema)
            };
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, actions);
            Assert.True(staged.Bytes.FirstLine()!.Length > 64 * 1024);
            var pending = new DeltaLakePendingCommit() { Version = 0, StagedFile = staged.FileName, StageId = staged.StageId, Length = staged.Length, CheckpointId = 1 };
            Assert.Equal(PublishOutcome.Published, (await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: false)).Outcome);

            // A restart before the next checkpoint publishes again, the stage is gone and nothing is cached
            var again = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.AlreadyPublished, again.Outcome);
        }

        [Fact]
        public async Task NormalPublishCostsThreeRequests()
        {
            var (storage, staged, pending) = await Stage(nameof(NormalPublishCostsThreeRequests));
            storage.ClearRequests();

            // The first attempt in the run that staged it, the listing before staging chose the version
            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: false);

            Assert.Equal(PublishOutcome.Published, result.Outcome);
            Assert.Equal(new[] { $"OpenRead {Target}", $"OpenWrite {Target}", $"Rm /t/_delta_log/{staged.FileName}" }, storage.Requests);
            Assert.Equal(StagedBytes(staged), await ReadBytes(storage, Target));
            Assert.False(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task StageIdIsFlushedBeforeTheRestOfTheCommit()
        {
            // A torn copy then always names its stage
            var (storage, staged, pending) = await Stage(nameof(StageIdIsFlushedBeforeTheRestOfTheCommit));
            storage.RecordWritesOf = Target;

            await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: true);

            var events = storage.WriteEvents.ToArray();
            Assert.Equal($"W{Prefix(staged).Length}", events[0]);
            Assert.Equal("F", events[1]);
            Assert.Equal(StagedBytes(staged).Length - Prefix(staged).Length, events.Skip(2).Where(x => x.StartsWith("W")).Sum(x => int.Parse(x.Substring(1))));
        }

        [Fact]
        public async Task PublishAfterARestartReadsTheStage()
        {
            var (storage, staged, pending) = await Stage(nameof(PublishAfterARestartReadsTheStage));

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.Published, result.Outcome);
            Assert.Equal(StagedBytes(staged), await ReadBytes(storage, Target));
        }

        [Fact]
        public async Task PublishingTwiceIsANoOp()
        {
            var (storage, staged, pending) = await Stage(nameof(PublishingTwiceIsANoOp));
            await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: true);

            var again = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.AlreadyPublished, again.Outcome);
            Assert.Equal(StagedBytes(staged), await ReadBytes(storage, Target));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(-1)]
        public async Task TornTargetWithoutTheWholeStageIdFailsClosed(int keep)
        {
            var (storage, staged, pending) = await Stage($"{nameof(TornTargetWithoutTheWholeStageIdFailsClosed)}_{keep}");
            // -1 stops one byte short of the stage id's closing quote
            var length = keep >= 0 ? keep : Prefix(staged).Length - 1;
            await WriteBytes(storage, Target, StagedBytes(staged).AsSpan(0, length).ToArray());

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(StagedBytes(staged).AsSpan(0, length).ToArray(), await ReadBytes(storage, Target));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        public async Task TornTargetHoldingTheStageIdIsRepaired(int cut)
        {
            var (storage, staged, pending) = await Stage($"{nameof(TornTargetHoldingTheStageIdIsRepaired)}_{cut}");
            var firstLine = Array.IndexOf(StagedBytes(staged), (byte)'\n') + 1;
            // Right after the stage id, at a line boundary, one byte short of the end
            var length = cut switch { 0 => Prefix(staged).Length, 1 => firstLine, _ => StagedBytes(staged).Length - 1 };
            await WriteBytes(storage, Target, StagedBytes(staged).AsSpan(0, length).ToArray());

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.Repaired, result.Outcome);
            Assert.Equal(StagedBytes(staged), await ReadBytes(storage, Target));
            Assert.False(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task FullLengthTargetIsAcknowledgedOnlyWhenItIsOurs()
        {
            var (storage, staged, pending) = await Stage(nameof(FullLengthTargetIsAcknowledgedOnlyWhenItIsOurs));
            await WriteBytes(storage, Target, StagedBytes(staged));
            await storage.Rm($"/t/_delta_log/{staged.FileName}");

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.AlreadyPublished, result.Outcome);
        }

        [Fact]
        public async Task MalformedFullLengthTargetWithoutTheStageFailsClosed()
        {
            var (storage, staged, pending) = await Stage(nameof(MalformedFullLengthTargetWithoutTheStageFailsClosed));
            var prefix = Prefix(staged);
            var malformed = prefix.Concat(Enumerable.Repeat((byte)'x', StagedBytes(staged).Length - prefix.Length)).ToArray();
            await WriteBytes(storage, Target, malformed);
            await storage.Rm($"/t/_delta_log/{staged.FileName}");

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(malformed, await ReadBytes(storage, Target));
        }

        [Fact]
        public async Task FullLengthTargetThatDiffersFromTheStageFailsClosed()
        {
            var (storage, staged, pending) = await Stage(nameof(FullLengthTargetThatDiffersFromTheStageFailsClosed));
            var different = StagedBytes(staged).ToArray();
            different[^3] = (byte)(different[^3] == (byte)'a' ? 'b' : 'a');
            await WriteBytes(storage, Target, different);

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(different, await ReadBytes(storage, Target));
        }

        [Fact]
        public async Task FullLengthTargetWithADamagedStageFailsClosed()
        {
            // The first line is ours, the rest differs, the damaged stage cannot vouch for it
            var (storage, staged, pending) = await Stage(nameof(FullLengthTargetWithADamagedStageFailsClosed));
            var different = StagedBytes(staged).ToArray();
            different[^3] = (byte)(different[^3] == (byte)'a' ? 'b' : 'a');
            await WriteBytes(storage, Target, different);
            var damaged = StagedBytes(staged).AsSpan(0, StagedBytes(staged).Length - 5).ToArray();
            await WriteBytes(storage, $"/t/_delta_log/{staged.FileName}", damaged);

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(different, await ReadBytes(storage, Target));
            Assert.Equal(damaged, await ReadBytes(storage, $"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task TruncatedStageFailsClosed()
        {
            var (storage, staged, pending) = await Stage(nameof(TruncatedStageFailsClosed));
            await WriteBytes(storage, $"/t/_delta_log/{staged.FileName}", StagedBytes(staged).AsSpan(0, StagedBytes(staged).Length - 5).ToArray());

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.False(await storage.Inner.Exists(Target));
        }

        [Fact]
        public async Task TruncatedStageCannotRepairATornTarget()
        {
            var (storage, staged, pending) = await Stage(nameof(TruncatedStageCannotRepairATornTarget));
            var torn = StagedBytes(staged).AsSpan(0, Prefix(staged).Length + 3).ToArray();
            await WriteBytes(storage, Target, torn);
            await WriteBytes(storage, $"/t/_delta_log/{staged.FileName}", StagedBytes(staged).AsSpan(0, StagedBytes(staged).Length - 5).ToArray());

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(torn, await ReadBytes(storage, Target));
        }

        [Fact]
        public async Task ForeignTargetIsNeverOverwritten()
        {
            var (storage, staged, pending) = await Stage(nameof(ForeignTargetIsNeverOverwritten));
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(UserSchema));
            var foreign = await ReadBytes(storage, Target);

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(foreign, await ReadBytes(storage, Target));
            Assert.True(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task MissingTargetAndMissingStageFailsClosed()
        {
            var (storage, staged, pending) = await Stage(nameof(MissingTargetAndMissingStageFailsClosed));
            await storage.Rm($"/t/_delta_log/{staged.FileName}");

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.False(await storage.Inner.Exists(Target));
        }

        [Fact]
        public async Task LegacyPendingPublishesAMissingTarget()
        {
            var (storage, staged, pending) = await StageLegacy(nameof(LegacyPendingPublishesAMissingTarget));

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.Published, result.Outcome);
            Assert.Equal(StagedBytes(staged), await ReadBytes(storage, Target));
            Assert.False(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task LegacyPendingAcknowledgesAnIdenticalTarget()
        {
            var (storage, staged, pending) = await StageLegacy(nameof(LegacyPendingAcknowledgesAnIdenticalTarget));
            await WriteBytes(storage, Target, StagedBytes(staged));

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.AlreadyPublished, result.Outcome);
            Assert.False(await storage.Inner.Exists($"/t/_delta_log/{staged.FileName}"));
        }

        [Fact]
        public async Task LegacyPendingNeverOverwritesAForeignTarget()
        {
            var (storage, staged, pending) = await StageLegacy(nameof(LegacyPendingNeverOverwritesAForeignTarget));
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(UserSchema), new DeltaAction() { Add = new DeltaAddAction() { Path = "foreign.parquet", Size = 1, DataChange = true, PartitionValues = new Dictionary<string, string>() } });
            var foreign = await ReadBytes(storage, Target);

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
            Assert.Equal(foreign, await ReadBytes(storage, Target));
        }

        [Fact]
        public async Task LegacyPendingWithoutStageAcknowledgesTheTarget()
        {
            // The stage is removed right after the copy, before the pending reset is checkpointed
            var (storage, staged, pending) = await StageLegacy(nameof(LegacyPendingWithoutStageAcknowledgesTheTarget));
            await WriteBytes(storage, Target, StagedBytes(staged));
            await storage.Rm($"/t/_delta_log/{staged.FileName}");

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.AlreadyPublished, result.Outcome);
        }

        [Theory]
        [InlineData(null, 5L, true)]
        [InlineData(5L, 5L, true)]
        [InlineData(3L, 5L, true)]
        [InlineData(6L, 5L, false)]
        public void OnlyCommitsStagedAtOrBeforeTheVersionArePublished(long? checkpointId, long version, bool expected)
        {
            var pending = new DeltaLakePendingCommit() { Version = 1, StagedFile = "x", CheckpointId = checkpointId };

            Assert.Equal(expected, DeltaLakeSink.IsPublishable(pending, version));
        }

        private static byte[] Prefix(StagedCommit staged)
        {
            return Encoding.UTF8.GetBytes($"{{\"commitInfo\":{{\"flowtide.stageId\":\"{staged.StageId}\"");
        }

        private static async Task<(HookFileStorage, StagedCommit, DeltaLakePendingCommit)> Stage(string name)
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{name}"));
            var stageId = Guid.NewGuid().ToString("N");
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = stageId, Data = new Dictionary<string, object>() { ["operation"] = "WRITE" } } },
                Protocol(),
                Metadata(UserSchema),
                new DeltaAction() { Add = new DeltaAddAction() { Path = "a.parquet", Size = 1, DataChange = true, PartitionValues = new Dictionary<string, string>() } }
            };
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, actions);
            Assert.Equal(stageId, staged.StageId);
            Assert.StartsWith($".00000000000000000000.json.{stageId}.", staged.FileName);
            var pending = new DeltaLakePendingCommit() { Version = 0, StagedFile = staged.FileName, StageId = staged.StageId, Length = staged.Length, CheckpointId = 1 };
            return (storage, staged, pending);
        }

        // Stages the way the sink did before stage ids existed
        private static async Task<(HookFileStorage, StagedCommit, DeltaLakePendingCommit)> StageLegacy(string name)
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{name}"));
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, new List<DeltaAction>() { Protocol(), Metadata(UserSchema) });
            Assert.Null(staged.StageId);
            var pending = new DeltaLakePendingCommit() { Version = 0, StagedFile = staged.FileName };
            return (storage, staged, pending);
        }

        private static async Task WriteBytes(IFileStorage storage, string path, byte[] bytes)
        {
            using var write = await storage.OpenWrite(path);
            await write.WriteAsync(bytes);
        }
    }
}
