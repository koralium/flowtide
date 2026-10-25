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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RetentionIntegrationTests : IDisposable
    {
        private const string Published = "interval 168 hours";

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ri-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();

        public void Dispose()
        {
            _owners.Release();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        // Seven days on an in-memory store, which the assertion makes eligible
        private static void Retained(DeltaLakeOptions options)
        {
            options.HistoryRetention = TimeSpan.FromDays(7);
            options.AssumeCompleteStorageListing = true;
        }

        private static Dictionary<string, string> RetentionPublished() => new Dictionary<string, string>()
        {
            [RetentionProperties.LogKey] = Published,
            [RetentionProperties.DeletedFileKey] = Published
        };

        // Commits another writer left, without commitInfo; the first creates the table with the retention properties published
        private static async Task Legacy(IFileStorage storage, long from, long to)
        {
            for (var version = from; version <= to; version++)
            {
                var add = DeltaLakeCheckpointBuildTests.Add($"legacy-{version}.parquet");
                await (version == 0 ? WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema, RetentionPublished()), add) : WriteCommit(storage, "test", version, add));
            }
        }

        // A checkpoint built and published as a worker would
        private async Task<BuildResult> Build(IFileStorage storage, long version, RootPolicy root = RootPolicy.Auto)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            try
            {
                using var scratch = owner.CreateJob();
                var result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "test",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = TimeProvider.System
                }, new BuildRequest() { Version = version, Root = root });
                Assert.Equal(BuildStatus.Completed, result.Status);
                return result;
            }
            finally
            {
                _owners.Release(owner);
            }
        }

        private static Task<List<long>> Checkpoints(IFileStorage storage) => DeltaMaintenanceWorkerTests.Checkpoints(storage, "test");

        // Engine checkpoints until a worker's target reaches the version, which an idle stream hands off at its first checkpoint
        private static async Task CheckpointUntilTarget(DeltaLakeSinkStream stream, long version)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!stream.Maintenance.Workers.Any(x => x.Target >= version))
            {
                Assert.True(DateTime.UtcNow < deadline, $"no worker reached version {version}");
                await stream.TriggerCheckpoint();
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task IdleFullyCheckpointedTableStagesOneAnchor()
        {
            var testName = nameof(IdleFullyCheckpointedTableStagesOneAnchor);
            var storage = Files.Of.InternalMemory($"./{testName}");
            // Fully checkpointed by an earlier writer whose head carries no adoption epoch
            await Legacy(storage, 0, 3);
            await Build(storage, 3, RootPolicy.None);
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);

            // The worker discovers the head without an epoch and asks for an anchor, which the next idle checkpoint stages
            await CheckpointUntilTarget(stream, 3);
            await stream.Maintenance.TickAndSettleAsync(3);
            var worker = stream.Maintenance.Workers.Single();
            Assert.True(worker.RepairPending);
            await WaitForVersionCheckpointing(storage, "test", stream, 4);
            var actions = await ReadCommitActions(storage, "test", 4);
            Assert.Equal(2, actions.Count);
            Assert.NotNull(actions[0].CommitInfo);
            Assert.True(RetentionProperties.Matches(actions[1].MetaData!.Configuration, Published));
            Assert.Equal(0, worker.AnchorRequest);
            Assert.Equal(4, worker.AnchorStagedVersion);

            // Handed off, the anchor is the exact repair, rooted on the marked checkpoint 3, and its destination read confirms coverage
            await stream.Maintenance.TickAndSettleAsync(4);
            Assert.Equal(new long[] { 3, 4 }, await Checkpoints(storage));
            Assert.Equal(3, worker.LastBuild!.RootVersion);
            Assert.False(worker.LastBuild.InheritedGap);
            Assert.True(worker.CoverageConfirmed);
            Assert.False(worker.RepairPending);

            // Nothing more is staged while the table stays idle
            await RunCheckpoints(stream, 3);
            await stream.Maintenance.TickAndSettleAsync(4);
            Assert.False(await storage.Exists(CommitPath("test", 5)));
            Assert.Equal(0, worker.AnchorRequest);
        }

        [Fact]
        public async Task RepairAnchorSupersedesNewerGappedRoot()
        {
            var testName = nameof(RepairAnchorSupersedesNewerGappedRoot);
            var storage = Files.Of.InternalMemory($"./{testName}");
            // Gapped checkpoints 4 and 8 over a legacy root at 2, own commits 7 and 8 adopted at 7, head 8
            await Legacy(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "test", 2);
            Assert.NotNull((await Build(storage, 4)).Checkpoint!.Marker.Inherited);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await WriteOwnCommit(storage, "test", 7, 7, now, DeltaLakeCheckpointBuildTests.Add("own-7.parquet"));
            await WriteOwnCommit(storage, "test", 8, 7, now + 1, DeltaLakeCheckpointBuildTests.Add("own-8.parquet"));
            Assert.NotNull((await Build(storage, 8)).Checkpoint!.Marker.Inherited);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                Retained(options);
                options.CheckpointInterval = 1;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);

            // No handed-off version is above the gapped checkpoint 8: an idle checkpoint stages anchor 9
            await CheckpointUntilTarget(stream, 8);
            await stream.Maintenance.TickAndSettleAsync(8);
            await WaitForVersionCheckpointing(storage, "test", stream, 9);
            Assert.DoesNotContain(await ReadCommitActions(storage, "test", 9), x => x.Add != null);

            // The newest root is gapped: a replay from zero at exactly 9, confirmed from the destination
            await stream.Maintenance.TickAndSettleAsync(9);
            var worker = stream.Maintenance.Workers.Single();
            Assert.Equal(new long[] { 2, 4, 8, 9 }, await Checkpoints(storage));
            Assert.Null(worker.LastBuild!.RootVersion);
            Assert.False(worker.LastBuild.InheritedGap);
            Assert.True(worker.CoverageConfirmed);

            // The next data commit's periodic build roots on 9 and inherits its coverage
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 10);
            await stream.Maintenance.TickAndSettleAsync(10);
            Assert.Equal(new long[] { 2, 4, 8, 9, 10 }, await Checkpoints(storage));
            Assert.Equal(9, worker.LastBuild!.RootVersion);
            Assert.False(worker.LastBuild.InheritedGap);
            Assert.Contains(await ReadCommitActions(storage, "test", 10), x => x.Add != null);
            Assert.False(await storage.Exists(CommitPath("test", 11)));
        }

        [Fact]
        public async Task IntervalZeroPerformsTwoSuccessiveAdvances()
        {
            var testName = nameof(IntervalZeroPerformsTwoSuccessiveAdvances);
            var directory = StoreFixtures.TempDirectory(testName);
            using var storage = Files.Of.LocalDisk(directory);
            var start = new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new FakeTimeProvider(start);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.HistoryRetention = TimeSpan.FromDays(1);
                options.CheckpointInterval = 0;
                options.TimeProvider = clock;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();

            // A commit at midnight of each day; the first, at its hand-off, gets the coverage repair
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await stream.Maintenance.TickAndSettleAsync(0);
            var worker = stream.Maintenance.Workers.Single();
            var observed = BoundarySelectorTests.Observed((DeltaTableCleanup)worker.Cleanup!);
            Assert.True(worker.CoverageConfirmed);
            Assert.Equal(new long[] { 0 }, await Checkpoints(storage));

            // Each day passes the table's offset; the commit of the day the advance runs at is never at or before midnight a day ago
            async Task NextDay(long version)
            {
                clock.Advance(TimeSpan.FromDays(1));
                stream.Generate(5);
                await WaitForVersion(storage, "test", stream, version);
                await stream.Maintenance.TickAndSettleAsync(version);
            }

            // Day one: nothing above the coverage root is old enough
            await NextDay(1);
            Assert.Equal(new long[] { 0 }, await Checkpoints(storage));

            // Day two: with no eligible checkpoint the advance asks for a cutoff build at 1, built exactly
            await NextDay(2);
            Assert.Equal(new long[] { 0, 1 }, await Checkpoints(storage));
            Assert.Equal(0, worker.LastBuild!.RootVersion);
            Assert.Null(observed.Boundary);

            // Day three: the next advance selects that checkpoint, plans to it and deletes the log below it; the one after it, due at once, asks for a cutoff build at 2
            await NextDay(3);
            Assert.Equal(1, observed.Boundary!.C);
            var plan = observed.Plan!;
            Assert.Equal(AdvancePlanStatus.Planned, plan.Status);
            Assert.Equal(1, plan.Destination!.Proof!.Version);
            Assert.Equal(new long[] { 1, 2 }, await Checkpoints(storage));
            Assert.Equal(1, worker.LastBuild!.RootVersion);
            Assert.False(worker.LastBuild.InheritedGap);
            Assert.Equal(2, (await DeltaMaintenanceWorkerTests.Hint(storage, "test"))!.Version);
            for (long version = 0; version <= 3; version++)
            {
                Assert.Equal(version >= 1, await storage.Exists(CommitPath("test", version)));
            }
            Assert.Equal(1, await DoneAsync(storage));

            // Day four: the second advance selects that checkpoint, plans to it and deletes the log below it, and the one after it asks for a cutoff build at 3
            await NextDay(4);
            Assert.Equal((1L, 2L), (observed.Boundary!.O, observed.Boundary.C));
            var second = observed.Plan!;
            Assert.Equal(AdvancePlanStatus.Planned, second.Status);
            Assert.Equal(2, second.Destination!.Proof!.Version);
            Assert.Equal(new long[] { 2, 3 }, await Checkpoints(storage));
            Assert.Equal(2, worker.LastBuild!.RootVersion);
            for (long version = 1; version <= 4; version++)
            {
                Assert.Equal(version >= 2, await storage.Exists(CommitPath("test", version)));
            }
            Assert.Equal(2, await DoneAsync(storage));
            // Every advance and delete turn ran inside the ticks without a decline
            Assert.Equal((0, 0), (worker.CleanupFailures(JobKind.Advance), worker.CleanupFailures(JobKind.Deletion)));
        }

        // The boundary of the only cleanup record left, a done
        private static async Task<long> DoneAsync(IFileStorage storage)
        {
            var done = Assert.Single((await DeltaTransactionReader.ListLog(storage, "test")).Controls);
            Assert.True(ControlName.TryParse(done.Name, out var name) && name.Kind == ControlKind.Done, done.Name);
            return name.Boundary;
        }
    }
}
