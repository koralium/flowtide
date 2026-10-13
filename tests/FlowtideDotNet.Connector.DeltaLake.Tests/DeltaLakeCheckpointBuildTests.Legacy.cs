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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeCheckpointBuildTests
    {
        // Builds and publishes the checkpoint, as a worker's job does
        private async Task<BuildResult> Publish(IFileStorage storage, string table, long version, bool rebuildLegacy = false)
        {
            var directory = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            BuildResult result;
            using (var scratch = directory.CreateJob())
            {
                result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = table,
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now),
                    RebuildLegacyCheckpoints = rebuildLegacy
                }, new BuildRequest() { Version = version });
            }
            Assert.Equal(0, directory.Ledger.Reserved);
            _owners.Release(directory);
            return result;
        }

        [Fact]
        public async Task BuildBelowTheHeadEqualsReplayAtT()
        {
            var storage = new HookFileStorage(Memory(nameof(BuildBelowTheHeadEqualsReplayAtT)));
            await WriteHistory(storage, "t", 51, 20, 20);
            Assert.Equal(BuildStatus.Completed, (await Publish(storage, "t", 19)).Status);

            storage.ClearRequests();
            var built = await Build(storage, "t", 12);
            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 12, s_now), built.Lines);
            // Nothing above the target is read
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("OpenRead") && Enumerable.Range(13, 7).Any(v => x.EndsWith($"{v:D20}.json") || x.EndsWith($"{v:D20}.checkpoint.parquet")));

            // Publishing below the head never moves the hint backward
            Assert.Equal(BuildStatus.Completed, (await Publish(storage, "t", 12)).Status);
            Assert.Equal(19, (await DeltaMaintenanceWorkerTests.Hint(storage, "t"))!.Version);
        }

        [Fact]
        public async Task ExactBuildReturnsACorrectDescriptor()
        {
            var storage = Memory(nameof(ExactBuildReturnsACorrectDescriptor));
            await WriteHistory(storage, "t", 52, 15, 20);
            await LegacyRoot(storage, "t", 6);
            var built = await Build(storage, "t", 11);

            var descriptor = built.Result.Checkpoint!;
            Assert.Equal(11, descriptor.Version);
            Assert.Equal(built.OutputLength, descriptor.SizeInBytes);
            Assert.Equal(built.Records.Count, descriptor.Actions);
            Assert.Equal(built.Records.Count(x => CheckpointRecord.Kind(x) == RecordKind.Add), descriptor.AddFiles);
            Assert.Equal(built.Marker, descriptor.Marker.ToJson());
            // The digest is a function of the content: the same build again gives the same one
            Assert.Equal(descriptor.Sha256, (await Build(storage, "t", 11)).Result.Checkpoint!.Sha256);
        }

        [Fact]
        public async Task LegacyRebuildRunsOncePerTable()
        {
            var storage = Memory(nameof(LegacyRebuildRunsOncePerTable));
            await WriteHistory(storage, "t", 53, 20, 20);
            await LegacyRoot(storage, "t", 10);

            // The unmarked root is replaced by a replay from 0, which covers the whole history
            var first = await Publish(storage, "t", 14, rebuildLegacy: true);
            Assert.Equal(BuildStatus.Completed, first.Status);
            Assert.Null(first.RootVersion);
            Assert.Null(first.Checkpoint!.Marker.Inherited);

            // From then on our own covering checkpoint is the root, so nothing replays again
            var second = await Publish(storage, "t", 19, rebuildLegacy: true);
            Assert.Equal(14, second.RootVersion);
            Assert.Null(second.Checkpoint!.Marker.Inherited);
        }

        [Fact]
        public async Task LegacyRebuildCoversMarkedRootsWithAGap()
        {
            var storage = Memory(nameof(LegacyRebuildCoversMarkedRootsWithAGap));
            await WriteHistory(storage, "t", 54, 20, 20);
            await LegacyRoot(storage, "t", 10);
            // Without the option our checkpoint over the legacy root records the gap it inherits
            var gap = await Publish(storage, "t", 14);
            Assert.Equal(10, gap.RootVersion);
            Assert.Equal(10, gap.Checkpoint!.Marker.Inherited!.AtOrBelow);

            // With it, a marked root that inherits a gap is not used either
            var rebuilt = await Build(storage, "t", 19, rebuildLegacy: true);
            Assert.Null(rebuilt.Result.RootVersion);
            Assert.Null(CoverageMarker.Parse(rebuilt.Marker)!.Inherited);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 19, s_now), rebuilt.Lines);
        }

        [Fact]
        public async Task LegacyRebuildFallsBackWhenHistoryIsMissing()
        {
            var storage = Memory(nameof(LegacyRebuildFallsBackWhenHistoryIsMissing));
            await WriteHistory(storage, "t", 55, 20, 20);
            await LegacyRoot(storage, "t", 10);
            for (int version = 0; version < 10; version++)
            {
                await storage.Rm($"/t/_delta_log/{version:D20}.json");
            }

            // Without the commits before the root a replay is impossible, so the root is kept and its gap recorded
            var built = await Build(storage, "t", 15, rebuildLegacy: true);
            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(10, built.Result.RootVersion);
            Assert.Equal(10, CoverageMarker.Parse(built.Marker)!.Inherited!.AtOrBelow);
        }

        [Fact]
        public async Task LegacyRebuildAlsoCoversOutOfEnvelopeRoots()
        {
            var storage = Memory(nameof(LegacyRebuildAlsoCoversOutOfEnvelopeRoots));
            await WriteHistory(storage, "t", 56, 12, 12);
            await LegacyRoot(storage, "t", 5);
            await OutOfEnvelopeRoot(storage, "t", 8);

            // Without the option the older legacy root is used and its gap recorded
            var kept = await Build(storage, "t", 11);
            Assert.Equal(5, kept.Result.RootVersion);
            Assert.NotNull(CoverageMarker.Parse(kept.Marker)!.Inherited);

            // With it, neither the inadmissible root nor the unmarked older one is used
            var rebuilt = await Build(storage, "t", 11, rebuildLegacy: true);
            Assert.Equal(BuildStatus.Completed, rebuilt.Result.Status);
            Assert.Null(rebuilt.Result.RootVersion);
            Assert.Null(CoverageMarker.Parse(rebuilt.Marker)!.Inherited);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 11, s_now), rebuilt.Lines);
        }
    }
}
