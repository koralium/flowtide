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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeCheckpointBuildTests
    {
        [Fact]
        public async Task BlockedRootWithoutEarlierCommitsDeclinesAsNoAdmissibleRoot()
        {
            var storage = Memory(nameof(BlockedRootWithoutEarlierCommitsDeclinesAsNoAdmissibleRoot));
            await WriteHistory(storage, "t", 42, 12, 12);
            await OutOfEnvelopeRoot(storage, "t", 8);
            // Log cleanup removed the commits before the root, as Spark's does
            for (int version = 0; version < 8; version++)
            {
                await storage.Rm($"/t/_delta_log/{version:D20}.json");
            }

            // Without the option, history cannot change the outcome: the decline depends on the root and can be memoized
            var built = await Build(storage, "t", 11);
            Assert.Equal(BuildStatus.Declined, built.Result.Status);
            Assert.Equal(MaintenanceReason.NoAdmissibleRoot, built.Result.Reason);
            Assert.Equal(DeclineDependency.Root, built.Result.Dependency);
        }

        [Fact]
        public async Task CommitRemovedAfterTheListingFailsTheBuild()
        {
            var inner = Memory(nameof(CommitRemovedAfterTheListingFailsTheBuild));
            await WriteHistory(inner, "t", 36, 8, 8);
            var hooks = new HookFileStorage(inner);
            // Another writer's log cleanup removes commit 5 after the listing, before the tail reads it
            hooks.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith($"{5:D20}.json"))
                {
                    await inner.Rm(path);
                }
            };
            BuildResult? result = null;
            var error = await Record.ExceptionAsync(async () => result = (await Build(hooks, "t", 7)).Result);

            Assert.True(error != null || result!.Status is BuildStatus.Failed or BuildStatus.Declined, $"{result?.Status} {result?.Reason}");
            Assert.Null(result?.Checkpoint);
        }

        [Fact]
        public async Task UnmarkedRootWithDuplicateKeysIsReconciledInSortMode()
        {
            var storage = Memory(nameof(UnmarkedRootWithDuplicateKeysIsReconciledInSortMode));
            var expired = s_now.ToUnixTimeMilliseconds() - (long)TimeSpan.FromDays(8).TotalMilliseconds;
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("gone.parquet"));
            await DeltaTestKit.WriteCommit(storage, "t", 1, Remove("gone.parquet", expired));
            await DeltaTestKit.WriteCommit(storage, "t", 2, Add("other.parquet"));
            // An unreconciled root at 1 with no marker: the add and, after it, its expired remove
            await OwnRoot(storage, "t", 1, Records(DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("gone.parquet"), Remove("gone.parquet", expired)));
            var table = (await DeltaTransactionReader.ReadTable(storage, "t"))!;
            Assert.Equal(1, table.StartCheckpointVersion);
            Assert.DoesNotContain(table.AddFiles, x => x.Path == "gone.parquet");

            // A root we did not write is read in sort mode even when the tail fits, so expiry never sees the earlier add alone
            var built = await Build(storage, "t", 2);
            Assert.Equal(BuildMode.Sort, built.Result.Mode);
            Assert.DoesNotContain(built.Records.Select(x => CheckpointRecord.ToAction(x).Add), x => x?.Path == "gone.parquet");
        }

        [Theory]
        [InlineData(TailReducer.DefaultArenaCap)]
        [InlineData(4096L)]
        public async Task DuplicateRootExpiredRemoveDoesNotResurrectEarlierAdd(long arenaCap)
        {
            var storage = Memory(nameof(DuplicateRootExpiredRemoveDoesNotResurrectEarlierAdd));
            var protocol = DeltaTestKit.Protocol();
            var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            await DeltaTestKit.WriteCommit(storage, "t", 0, protocol, metadata);
            await DeltaTestKit.WriteCommit(storage, "t", 1, Add("unrelated.parquet"));
            var root = new[] { protocol, metadata, Add("deleted.parquet"), Remove("deleted.parquet", s_now.AddDays(-8).ToUnixTimeMilliseconds()) };
            await OwnRoot(storage, "t", 0, Records(root));

            var replayed = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.DoesNotContain(replayed!.AddFiles, x => x.Path == "deleted.parquet");
            var expected = await CheckpointOracle.RootPlusTail(root, storage, "t", 0, 1, s_now);
            var built = await Build(storage, "t", 1, arenaCap: arenaCap);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(expected, built.Lines);
        }

        [Fact]
        public async Task OwnMarkedRootIsReadInProbeMode()
        {
            var storage = Memory(nameof(OwnMarkedRootIsReadInProbeMode));
            await WriteHistory(storage, "t", 17, 8, 8);
            var first = await Build(storage, "t", 5);
            Assert.NotNull(first.Marker);
            await OwnRoot(storage, "t", 5, first.Records, first.Marker);

            // Our own outputs never repeat a key, so they keep the cheaper mode
            var built = await Build(storage, "t", 7);
            Assert.Equal(5, built.Result.RootVersion);
            Assert.Equal(BuildMode.Probe, built.Result.Mode);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 7, s_now), built.Lines);
        }
    }
}
