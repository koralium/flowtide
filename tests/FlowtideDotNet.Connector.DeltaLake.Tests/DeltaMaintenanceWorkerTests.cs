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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // The profile keeps busy-spin race tests, such as the 10,000-iteration stop races, out of parallel runs
    [CollectionDefinition(nameof(MaintenanceRaceCollection), DisableParallelization = true)]
    public class MaintenanceRaceCollection
    {
    }

    [Collection(nameof(MaintenanceRaceCollection))]
    public partial class DeltaMaintenanceWorkerTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 12, 0, 0, 0, TimeSpan.Zero);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "wk-" + ScratchOwners.ShortId());

        public void Dispose()
        {
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

        internal sealed class CollectingLogger : ILogger
        {
            public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new ConcurrentQueue<(LogLevel, string)>();

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                Entries.Enqueue((logLevel, formatter(state, exception)));
            }

            public int Warnings(string contains) => Entries.Count(x => x.Level == LogLevel.Warning && x.Message.Contains(contains));
        }

        internal sealed class Rig
        {
            public required FakeTimeProvider BudgetClock { get; init; }

            public required FakeTimeProvider OptionsClock { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required IFileStorage Storage { get; init; }

            public HookFileStorage? Hooks { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public required CollectingLogger Logs { get; init; }

            // One tick, then the job it dispatched, if any
            public async Task TickAsync()
            {
                Budget.TickNow();
                await Worker.CurrentJob;
            }

            public void Advance(TimeSpan by)
            {
                BudgetClock.Advance(by);
                OptionsClock.Advance(by);
            }

            // Past the backoff on the options clock
            public void PastRetry()
            {
                var wait = Worker.RetryAt - OptionsClock.GetUtcNow();
                if (wait > TimeSpan.Zero)
                {
                    OptionsClock.Advance(wait);
                }
            }
        }

        // Unthrottled buckets: thousands of jobs on one budget would otherwise wait on the default rates
        private DeltaMaintenanceBudget NewBudget(int jobs = 2, FakeTimeProvider? clock = null)
        {
            return new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(jobs, Path.Combine(_directory, ScratchOwners.ShortId())), clock ?? new FakeTimeProvider(s_now), manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
        }

        // cached: the worker knows the table id, which the root cache requires
        private Rig Create(IFileStorage? storage = null, int interval = 2, TimeSpan? maxAge = null, int jobs = 2, DeltaMaintenanceBudget? budget = null, bool stamp = false,
            FakeTimeProvider? optionsClock = null, bool cached = false, bool periodic = true)
        {
            var hooks = storage == null ? new HookFileStorage(Files.Of.InternalMemory($"./worker-{Guid.NewGuid():N}")) { StampIdentity = stamp } : storage as HookFileStorage;
            storage ??= hooks!;
            budget ??= NewBudget(jobs);
            optionsClock ??= new FakeTimeProvider(s_now);
            var logs = new CollectingLogger();
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(interval, maxAge, optionsClock) { PeriodicEnabled = periodic }, storage, "t", "t", logs);
            budget.Register(worker);
            if (cached)
            {
                worker.SetTableId("table-id");
            }
            return new Rig()
            {
                BudgetClock = (FakeTimeProvider)budget.Clock,
                OptionsClock = optionsClock,
                Budget = budget,
                Storage = storage,
                Hooks = hooks,
                Worker = worker,
                Logs = logs
            };
        }

        // A rendezvous before an exclusion is asserted: the job waits at the gate or a lease, rather than not having run yet
        internal static async Task Queued(Func<bool> queued)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!queued())
            {
                Assert.True(DateTime.UtcNow < deadline, "The job never queued");
                await Task.Delay(5);
            }
        }

        // Commit 0 holds protocol and metaData; every commit adds one file
        internal static async Task Commits(IFileStorage storage, long from, long to, string table = "t")
        {
            for (long version = from; version <= to; version++)
            {
                if (version == 0)
                {
                    await DeltaTestKit.WriteCommit(storage, table, 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet"));
                }
                else
                {
                    await DeltaTestKit.WriteCommit(storage, table, version, DeltaLakeCheckpointBuildTests.Add($"f{version}.parquet"));
                }
            }
        }

        internal static async Task<List<long>> Checkpoints(IFileStorage storage, string table = "t")
        {
            var listed = await storage.Ls($"/{table}/_delta_log/");
            return listed.Where(x => x.Name.EndsWith(".checkpoint.parquet")).Select(x => long.Parse(x.Name.Substring(0, 20))).Order().ToList();
        }

        internal static async Task<LastCheckpointInfo?> Hint(IFileStorage storage, string table = "t")
        {
            var text = await storage.ReadText($"/{table}/_delta_log/_last_checkpoint");
            return text == null ? null : JsonSerializer.Deserialize<LastCheckpointInfo>(text);
        }

        internal static Task WriteHint(IFileStorage storage, long version, long? sizeInBytes = null)
        {
            return storage.WriteText("/t/_delta_log/_last_checkpoint", JsonSerializer.Serialize(new LastCheckpointInfo() { Version = version, Size = 3, SizeInBytes = sizeInBytes }));
        }

        // The cleanup's warn-once mask is a single 64-bit word, the worker's covers every byte value
        [Fact]
        public void ReasonsAreUniqueAndFitTheCleanupWarningMask()
        {
            var values = Enum.GetValues<MaintenanceReason>().Select(x => (int)x).Distinct().ToList();
            Assert.Equal(Enum.GetNames<MaintenanceReason>().Length, values.Count);
            Assert.All(values, x => Assert.InRange(x, 0, 63));
        }

        [Fact]
        public async Task IntervalCountsFromTheLastCompletedCheckpoint()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 5);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));

            await Commits(rig.Storage, 6, 6);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));

            await Commits(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5, 7 }, await Checkpoints(rig.Storage));
            Assert.Equal(7, rig.Worker.LastCompleted);
        }

        [Fact]
        public async Task InspectionOnlyWorkerOwesNoLagOrAge()
        {
            var rig = Create(interval: 2, maxAge: TimeSpan.FromHours(1), periodic: false);
            await Commits(rig.Storage, 0, 5);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Null(rig.Worker.OldestOrigin);
            Assert.Equal(0, rig.Worker.LagVersions);

            await Commits(rig.Storage, 6, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            rig.Advance(TimeSpan.FromHours(2));
            await rig.TickAsync();
            Assert.Empty(await Checkpoints(rig.Storage));
            Assert.Null(rig.Worker.OldestOrigin);
            Assert.Equal(0, rig.Worker.OldestUncheckpointedSeconds);
            Assert.Equal(0, rig.Worker.LagVersions);
        }

        [Fact]
        public async Task FirstCheckpointAtTheIntervalWithoutAnyCheckpoint()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 1);
            rig.Worker.Published(1);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Empty(await Checkpoints(rig.Storage));

            await Commits(rig.Storage, 2, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task IntervalNeverCheckpointsVersionZero()
        {
            var rig = Create(interval: 1);
            await Commits(rig.Storage, 0, 0);
            rig.Worker.Published(0);
            for (int i = 0; i < 3; i++)
            {
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(10));
            }
            Assert.Empty(await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task AgeTriggerCheckpointsAVersionZeroTable()
        {
            var rig = Create(interval: 10, maxAge: TimeSpan.FromHours(1));
            await Commits(rig.Storage, 0, 0);
            rig.Worker.Published(0);
            await rig.TickAsync();
            Assert.Empty(await Checkpoints(rig.Storage));

            rig.Advance(TimeSpan.FromHours(1));
            await rig.TickAsync();
            Assert.Equal(new long[] { 0 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task InspectionIsTheFirstJobAfterArming()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 3);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            rig.Hooks!.ClearRequests();

            // Unarmed: no job and no request
            await rig.TickAsync();
            Assert.Empty(rig.Hooks.Requests);
            Assert.False(rig.Worker.Inspected);

            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task InspectionWithAMatchingHintMakesTwoRequests()
        {
            // Local disk lists sizes, so a hint naming the checkpoint with its size is trusted without a download
            var root = Path.Combine(_directory, "hinted");
            Directory.CreateDirectory(root);
            var hooks = new HookFileStorage(Files.Of.LocalDisk(root));
            var rig = Create(hooks, interval: 2);
            await Commits(rig.Storage, 0, 3);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            var size = (await rig.Storage.Ls("/t/_delta_log/")).Single(x => x.Name.EndsWith(".checkpoint.parquet")).Size;
            await WriteHint(rig.Storage, 3, size);
            hooks.ClearRequests();

            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(2, hooks.Requests.Count);
            Assert.StartsWith("Ls ", hooks.Requests[0]);
            Assert.EndsWith("_last_checkpoint", hooks.Requests[1]);
        }

        [Fact]
        public async Task InspectionIgnoresCheckpointsAboveTheTarget()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 6);
            rig.Worker.Published(4);
            await rig.TickAsync();

            // Checkpoint 6 is above the target, so 4 has none and is due
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig.Storage));
            Assert.Equal(4, rig.Worker.LastCompleted);
            Assert.Equal(6, (await Hint(rig.Storage))!.Version);
        }

        [Fact]
        public async Task InspectionWaitsForTheGate()
        {
            var rig = Create(interval: 2, jobs: 1);
            await Commits(rig.Storage, 0, 3);
            var slot = await rig.Budget.Gate.AcquireAsync();
            rig.Hooks!.ClearRequests();
            rig.Worker.Published(3);
            rig.Budget.TickNow();
            await Queued(() => rig.Budget.Gate.QueueLength == 1);
            Assert.False(rig.Worker.CurrentJob.IsCompleted);
            Assert.Empty(rig.Hooks.Requests);

            slot.Dispose();
            await rig.Worker.CurrentJob;
            Assert.NotEmpty(rig.Hooks.Requests);
            Assert.True(rig.Worker.Inspected);
        }

        [Fact]
        public async Task EligibilityIsReevaluatedAfterInspection()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 5);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(5, rig.Worker.LastCompleted);
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task InspectionWithAHintAboveTheTargetNeverLowersIt()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            await WriteHint(rig.Storage, 9);
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(9, (await Hint(rig.Storage))!.Version);
        }

        [Fact]
        public async Task InspectionRepairsAMismatchedSameVersionHint()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 3);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            await WriteHint(rig.Storage, 3, sizeInBytes: 1);
            rig.Worker.Published(3);
            await rig.TickAsync();
            var hint = (await Hint(rig.Storage))!;
            Assert.Equal(3, hint.Version);
            Assert.Equal((await DeltaTestKit.ReadBytes(rig.Storage, "/t/_delta_log/00000000000000000003.checkpoint.parquet")).Length, hint.SizeInBytes);
        }

        [Fact]
        public async Task InspectionLeavesAnUnsupportedCheckpoint()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            await SidecarCheckpoint(rig.Storage, 3);
            await WriteHint(rig.Storage, 2);
            var before = await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(3));
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(before, await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(3)));
            // A hint below it is not dangling, since kernel lists forward to it
            Assert.Equal(2, (await Hint(rig.Storage))!.Version);
            Assert.Null(rig.Worker.RepairDue);
        }

        [Fact]
        public async Task InspectionRebuildsATornCheckpointInPlace()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            await rig.Storage.WriteText(CheckpointPath(4), "torn");
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(4, rig.Worker.LastCompleted);
            Assert.Equal(4, (await Hint(rig.Storage))!.Version);
            Assert.NotEqual("torn", await rig.Storage.ReadText(CheckpointPath(4)));
        }

        [Fact]
        public async Task TransientTornRebuildFailureRetriesInspection()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            await rig.Storage.WriteText(CheckpointPath(4), "torn");
            var faults = 1;
            rig.Hooks!.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith($"{2:D20}.json") && Interlocked.Decrement(ref faults) >= 0 ? throw new IOException("tail fault") : Task.CompletedTask;
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.False(rig.Worker.Inspected);
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);

            rig.PastRetry();
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(4, rig.Worker.LastCompleted);
        }

        [Fact]
        public async Task FailedTornRebuildResultRetriesInspection()
        {
            // The torn checkpoint names a version the log does not list yet, so the rebuild returns a failed result instead of throwing
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 3);
            await rig.Storage.WriteText(CheckpointPath(4), "torn");
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.False(rig.Worker.Inspected);
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);

            await Commits(rig.Storage, 4, 4);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(4, rig.Worker.LastCompleted);
        }

        [Fact]
        public async Task TornRebuildOverAnUnsupportedRootRecordsTheRoot()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 6);
            // The newest checkpoint is torn, and the only older root uses sidecars
            await SidecarCheckpoint(rig.Storage, 3);
            await rig.Storage.WriteText(CheckpointPath(5), "torn");
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);
            Assert.Single(rig.Worker.UnsupportedRoots);

            // The root that failed under the torn rebuild is recorded, so no later attempt downloads it again
            rig.Hooks!.ClearRequests();
            for (int i = 0; i < 3; i++)
            {
                rig.PastRetry();
                await rig.TickAsync();
            }
            Assert.DoesNotContain($"OpenRead {CheckpointPath(3)}", rig.Hooks.Requests);
        }

        [Fact]
        public async Task TornRepairDeclineKeepsTheNewestValidOlderCheckpoint()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 6);
            // The torn checkpoint's rebuild finds only a valid root it may not use, and the option is off
            await DeltaLakeCheckpointBuildTests.OutOfEnvelopeRoot(rig.Storage, "t", 3);
            await rig.Storage.WriteText(CheckpointPath(5), "torn");
            rig.Worker.Published(6);
            await rig.TickAsync();

            // Inspection is done, lastCompleted is the valid older checkpoint, one warning and backoff, no memo
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);
            Assert.Null(rig.Worker.Memo);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.TornRepairDeclined)));
            Assert.Equal("torn", await rig.Storage.ReadText(CheckpointPath(5)));
        }

        [Fact]
        public async Task UnknownCheckpointFailsTheInspectionAndBacksOff()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            rig.Hooks!.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith(".checkpoint.parquet") ? throw new IOException("read fault") : Task.CompletedTask;
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.False(rig.Worker.Inspected);
            Assert.True(rig.Worker.RetryAt > rig.OptionsClock.GetUtcNow());
        }

        [Fact]
        public async Task CrashBetweenPublicationAndBuildIsCaughtUp()
        {
            // The previous run published 4 and stopped before its build
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task RestartDoesNotCheckpointBeforeTheInterval()
        {
            var rig = Create(interval: 20);
            await Commits(rig.Storage, 0, 25);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 20);
            rig.Worker.Published(25);
            await rig.TickAsync();
            Assert.Equal(20, rig.Worker.LastCompleted);
            Assert.Equal(new long[] { 20 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task ExistingCheckpointAtTargetIsNotRebuiltAfterRestart()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.Hooks!.ClearRequests();
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(4, rig.Worker.LastCompleted);
            Assert.DoesNotContain(rig.Hooks.Requests, x => x.StartsWith("OpenWrite") && x.EndsWith(".checkpoint.parquet"));
        }

        [Fact]
        public async Task CoalescesToTheTargetAtAdmission()
        {
            var rig = Create(interval: 2, jobs: 1);
            await Commits(rig.Storage, 0, 7);
            var slot = await rig.Budget.Gate.AcquireAsync();
            rig.Worker.Published(3);
            rig.Budget.TickNow();
            rig.Worker.Published(7);
            slot.Dispose();
            await rig.Worker.CurrentJob;
            Assert.Equal(new long[] { 7 }, await Checkpoints(rig.Storage));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DanglingHintIsRepairedByABuildAtTheTarget(bool olderCheckpoint)
        {
            // Below the interval and with age off, only the repair makes 25 due
            var rig = Create(interval: olderCheckpoint ? 20 : 30);
            await Commits(rig.Storage, 0, 25);
            if (olderCheckpoint)
            {
                await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 10);
            }
            await WriteHint(rig.Storage, 15);
            rig.Worker.Published(25);
            await rig.TickAsync();

            Assert.Contains(25L, await Checkpoints(rig.Storage));
            Assert.Equal(25, (await Hint(rig.Storage))!.Version);
            Assert.Null(rig.Worker.RepairDue);
        }

        [Fact]
        public async Task DanglingHintRepairSurvivesATransientFailure()
        {
            var rig = Create(interval: 30);
            await Commits(rig.Storage, 0, 25);
            await WriteHint(rig.Storage, 15);
            var faults = 2;
            rig.Hooks!.Before = (verb, path) => verb == "OpenWrite" && path.Full.EndsWith($"{25:D20}.checkpoint.parquet") && Interlocked.Decrement(ref faults) >= 0 ? throw new IOException("upload fault") : Task.CompletedTask;
            rig.Worker.Published(25);
            await rig.TickAsync();
            Assert.Equal(15, rig.Worker.RepairDue);

            rig.PastRetry();
            await rig.TickAsync();
            Assert.Equal(15, rig.Worker.RepairDue);

            rig.PastRetry();
            await rig.TickAsync();
            Assert.Null(rig.Worker.RepairDue);
            Assert.Equal(25, (await Hint(rig.Storage))!.Version);
        }

        [Fact]
        public async Task DanglingHintResolvedByAnExistingCheckpointNeedsNoBuild()
        {
            var rig = Create(interval: 20);
            await Commits(rig.Storage, 0, 25);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 20);
            await WriteHint(rig.Storage, 15);
            rig.Worker.Published(25);
            await rig.TickAsync();

            Assert.Equal(new long[] { 20 }, await Checkpoints(rig.Storage));
            Assert.Equal(20, (await Hint(rig.Storage))!.Version);
            Assert.Null(rig.Worker.RepairDue);
        }

        [Fact]
        public async Task DanglingHintWithOnlyAnUnsupportedLaterCheckpointIsLeft()
        {
            var rig = Create(interval: 20);
            await Commits(rig.Storage, 0, 25);
            await SidecarCheckpoint(rig.Storage, 20);
            await WriteHint(rig.Storage, 15);
            rig.Worker.Published(25);
            await rig.TickAsync();

            Assert.Equal(15, (await Hint(rig.Storage))!.Version);
            Assert.Null(rig.Worker.RepairDue);
            Assert.Equal(new long[] { 20 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task DanglingHintGaugeReturnsToZero()
        {
            var rig = Create(interval: 30);
            await Commits(rig.Storage, 0, 25);
            await WriteHint(rig.Storage, 15);
            var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = point => point == "build" ? hold.Task : Task.CompletedTask };
            rig.Worker.Published(25);
            rig.Budget.TickNow();
            await WaitUntil(() => rig.Worker.RepairDue != null);
            Assert.Equal(15, rig.Worker.RepairDue);
            hold.SetResult();
            await rig.Worker.CurrentJob;
            Assert.Null(rig.Worker.RepairDue);
        }

        private static string CheckpointPath(long version) => $"/t/_delta_log/{version:D20}.checkpoint.parquet";

        // A checkpoint with a sidecar row: unsupported, never used and never replaced
        internal static Task SidecarCheckpoint(IFileStorage storage, long version)
        {
            return DeltaLogReplayTests.WriteClassicCheckpoint(storage, "t", version, new Dictionary<string, (string Name, Apache.Arrow.Types.IArrowType Type)[]>(DeltaLogReplayTests.CheckpointColumns)
            {
                ["sidecar"] = new (string, Apache.Arrow.Types.IArrowType)[] { ("path", Apache.Arrow.Types.StringType.Default) }
            },
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L }),
                ("sidecar", new object?[] { "part.parquet" }));
        }

        internal static async Task WaitUntil(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The condition did not hold within 30 s");
                }
                await Task.Delay(5);
            }
        }
    }
}
