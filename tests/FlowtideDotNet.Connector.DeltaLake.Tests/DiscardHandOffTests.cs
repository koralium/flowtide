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
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DiscardHandOffTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);
        private const string TableId = "table-a";
        private const string Log = "Ls /t/_delta_log/";

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "dh-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public DiscardHandOffTests()
        {
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
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

        // An eligible local table at t: the sink's owned view, one run's cleanup over the maintenance meter, and the run's producer handle
        internal sealed class Rig
        {
            public required string TableDir { get; init; }

            public required IFileStorage Local { get; init; }

            public required HookFileStorage Storage { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public required DiscardHandOff HandOff { get; init; }

            public required DiscardCounters Counters { get; init; }

            public DeltaTableCleanup Cleanup { get; set; } = null!;

            public DiscardHandle Producer { get; set; } = null!;

            // What the sink's options hold
            public IFileStorage Current { get; set; } = null!;

            public DiscardEntry Entry => Producer.Entry;

            public int Listings => Storage.Requests.Count(x => x == Log);

            public string Native(string relative) => Path.Combine(TableDir, relative.Replace('/', Path.DirectorySeparatorChar));

            public string Put(string relative)
            {
                var path = Native(relative);
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, "x");
                return relative;
            }

            public bool Exists(string relative) => File.Exists(Native(relative));

            public int Queue(long version, string stageId, params string[] paths) => Entry.Enqueue(new DiscardGroup(TableId, 0, version, stageId, paths));
        }

        // The cleanup reads retry times from the clock, the system clock unless given; a given hand-off is shared
        private Rig NewRig(long capacity = DiscardHandOff.DefaultCapacity, TimeProvider? clock = null, DiscardHandOff? handOff = null)
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var storage = OwnedHookFileStorage.Over(local);
            var configuration = CleanupConfiguration.Validate(storage, "t", false, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            var rig = new Rig()
            {
                TableDir = Path.Combine(root, "t"),
                Local = local,
                Storage = storage,
                Configuration = configuration,
                HandOff = handOff ?? new DiscardHandOff(capacity),
                Counters = new DiscardCounters()
            };
            Directory.CreateDirectory(rig.TableDir);
            rig.Current = storage;
            rig.Cleanup = NewCleanup(rig, storage, clock);
            rig.Producer = rig.HandOff.Acquire(DiscardRoute.Of(storage, "t"));
            return rig;
        }

        // One run's cleanup as the sink makes it, over the maintenance meter of the given storage
        private static DeltaTableCleanup NewCleanup(Rig rig, IFileStorage storage, TimeProvider? clock = null)
        {
            var metered = MeteredFileStorage.Wrap(storage, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
            return new DeltaTableCleanup(rig.Configuration, metered, clock ?? TimeProvider.System, "t", rig.HandOff, () => rig.Current, rig.Counters, null, "t");
        }

        // Contexts given one scratch owner share its disk ledger and cap
        internal CleanupContext Context(CancellationToken stop = default, long readLimit = MaintenanceWorkerSettings.DefaultShortReads, JobKind kind = JobKind.Deletion, long head = -1,
            long diskCap = DiskLedger.DefaultCap, ScratchDirectory? owner = null)
        {
            var clock = new FakeTimeProvider(s_now);
            owner ??= _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()), diskCap));
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(new JobRun(0, kind) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = kind,
                Long = readLimit == long.MaxValue,
                ReadLimit = readLimit,
                Deadline = s_now + TimeSpan.FromMinutes(15),
                Head = head,
                TableId = null,
                Scratch = scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop
            };
        }

        private static DeltaAction Own(long version, long adoptedAt = 0, string? stageId = null) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { StageId = stageId ?? $"stage-{version}", AdoptedAt = adoptedAt, Timestamp = 1000 + version, Operation = "WRITE" }
        };

        private static DeltaAction Foreign(long version) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { Timestamp = 1000 + version, Operation = "WRITE" }
        };

        // Own commits 0 to head of epoch 0: the table at 0, then one added file per commit
        private static async Task Create(IFileStorage storage, long head)
        {
            var metaData = Metadata(UserSchema);
            metaData.MetaData!.Id = TableId;
            await WriteCommit(storage, "t", 0, Own(0), Protocol(), metaData);
            for (long version = 1; version <= head; version++)
            {
                await Commit(storage, version, Own(version));
            }
        }

        private static Task Commit(IFileStorage storage, long version, DeltaAction commitInfo, string? add = null)
        {
            return WriteCommit(storage, "t", version, commitInfo, DeltaLakeCheckpointBuildTests.Add(add ?? $"part-{version:D4}.parquet"));
        }

        // A classic checkpoint of t at the version
        private async Task BuildCheckpoint(IFileStorage storage, long version)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            using (var scratch = owner.CreateJob())
            {
                var result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now)
                }, new BuildRequest() { Version = version });
                Assert.Equal(BuildStatus.Completed, result.Status);
            }
            _owners.Release(owner);
        }

        private static int Count(Rig rig, string request) => rig.Storage.Requests.Count(x => x == request);

        [Fact]
        public async Task HintKeptBeforeOwnCommitPublishes()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            Assert.Equal(0, rig.Queue(2, "stage-2", a));
            Assert.Equal(0, rig.Queue(3, "stage-3", b));
            var charged = rig.HandOff.Charged;
            Assert.True(charged > 0);
            rig.Storage.ClearRequests();

            // Neither own commit is published: both are kept, from one listing and without reading a commit
            var kept = await rig.Cleanup.DrainHintsAsync(Context(), 128);
            Assert.Equal(0, kept.Drained);
            Assert.Equal(2, rig.Entry.Hints);
            Assert.Equal(charged, rig.HandOff.Charged);
            Assert.True(rig.Cleanup.HasWork(JobKind.Deletion));
            Assert.True(rig.Exists(a));
            Assert.True(rig.Exists(b));
            Assert.Equal(1, rig.Listings);
            Assert.DoesNotContain(rig.Storage.Requests, x => x.StartsWith("OpenRead ", StringComparison.Ordinal));

            // Its own commit published, the first drains and the second still waits
            await Commit(rig.Storage, 2, Own(2));
            var first = await rig.Cleanup.DrainHintsAsync(Context(), 128);
            Assert.Equal(1, first.Drained);
            Assert.False(rig.Exists(a));
            Assert.True(rig.Exists(b));
            Assert.Equal(1, rig.Entry.Hints);

            await Commit(rig.Storage, 3, Own(3));
            var second = await rig.Cleanup.DrainHintsAsync(Context(), 128);
            Assert.Equal(1, second.Drained);
            Assert.False(rig.Exists(b));
            Assert.Equal(0, rig.Entry.Hints);
            Assert.False(rig.Cleanup.HasWork(JobKind.Deletion));
            Assert.Equal(0, rig.HandOff.Charged);
            Assert.Equal(2, rig.Counters.Deleted);
            Assert.Equal(0, rig.Counters.Dropped);
        }

        [Fact]
        public async Task HintGateDropsConflictsAndMismatchesWithoutDeleting()
        {
            var rig = NewRig();
            await Create(rig.Storage, 2);
            var conflict = rig.Put("_change_data/conflict.parquet");
            var otherTable = rig.Put("_change_data/other-table.parquet");
            var otherEpoch = rig.Put("_change_data/other-epoch.parquet");
            var own = rig.Put("_change_data/own.parquet");
            // Version 2 was published by another attempt
            rig.Queue(2, "stage-x", conflict);
            rig.Entry.Enqueue(new DiscardGroup("table-b", 0, 2, "stage-2", new[] { otherTable }));
            rig.Entry.Enqueue(new DiscardGroup(TableId, 1, 2, "stage-2", new[] { otherEpoch }));
            rig.Queue(2, "stage-2", own);

            var result = await rig.Cleanup.DrainHintsAsync(Context(), 128);

            Assert.Equal(4, result.Drained);
            Assert.True(rig.Exists(conflict));
            Assert.True(rig.Exists(otherTable));
            Assert.True(rig.Exists(otherEpoch));
            Assert.False(rig.Exists(own));
            Assert.Equal(3, rig.Counters.Dropped);
            Assert.Equal(3, rig.HandOff.Dropped);
            Assert.Equal(1, rig.Counters.Deleted);
            Assert.Equal(0, rig.Entry.Hints);
            Assert.Equal(0, rig.HandOff.Charged);

            // A foreign commit at the version
            var foreign = NewRig();
            await Create(foreign.Storage, 1);
            await Commit(foreign.Storage, 2, Foreign(2));
            await Commit(foreign.Storage, 3, Own(3, adoptedAt: 3));
            var notOwn = foreign.Put("_change_data/not-own.parquet");
            foreign.Queue(2, "stage-2", notOwn);
            Assert.Equal(1, (await foreign.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.True(foreign.Exists(notOwn));
            Assert.Equal(1, foreign.Counters.Dropped);
            Assert.Equal(0, foreign.Entry.Hints);

            // An own version missing although the head passed it
            var removed = NewRig();
            var metaData = Metadata(UserSchema);
            metaData.MetaData!.Id = TableId;
            await WriteCommit(removed.Storage, "t", 3, Own(3), Protocol(), metaData);
            await Commit(removed.Storage, 4, Own(4));
            var absent = removed.Put("_change_data/absent.parquet");
            removed.Queue(2, "stage-2", absent);
            Assert.Equal(1, (await removed.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.True(removed.Exists(absent));
            Assert.Equal(1, removed.Counters.Dropped);
            Assert.Equal(0, removed.Entry.Hints);
        }

        [Fact]
        public async Task DiscardHintsUnwrapMeteredStorage()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var file = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", file);

            // The sink routes by its own storage, the worker by its meter; both reach the raw storage below
            Assert.Equal(rig.Entry.Route, rig.Cleanup.Route);
            Assert.Same(rig.Local, rig.Cleanup.Route.Raw);
            Assert.True(rig.Cleanup.HasWork(JobKind.Deletion));

            // Another instance over the same folder is another route, which sees no hint
            var otherLocal = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(otherLocal);
            var other = NewCleanup(rig, OwnedHookFileStorage.Over(otherLocal));
            Assert.NotEqual(rig.Entry.Route, other.Route);
            Assert.False(other.HasWork(JobKind.Deletion));
            Assert.Equal(0, (await other.DrainHintsAsync(Context(), 128)).Drained);
            Assert.True(rig.Exists(file));

            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(file));
        }

        [Fact]
        public async Task OrphanedHintAllowanceReleasedAfterLastOwnerSettles()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(1, "stage-1", a, b);
            var entry = rig.Entry;
            var route = entry.Route;
            var charged = rig.HandOff.Charged;

            // A worker that stops while the sink runs releases nothing
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true) { Discards = rig.HandOff };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, new FakeTimeProvider(s_now)) { PeriodicEnabled = false, InspectionEnabled = false },
                rig.Storage, "t", "t", null) { Cleanup = rig.Cleanup };
            budget.Register(worker);
            await worker.StopAsync(TimeSpan.Zero);
            Assert.Equal(0, budget.Registered);
            Assert.Equal(2, entry.Hints);
            Assert.Equal(charged, rig.HandOff.Charged);

            // A drain in flight holds its own handle
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rig.Cleanup.DrainHookForTests = async _ =>
            {
                entered.TrySetResult();
                await release.Task;
            };
            using var stop = new CancellationTokenSource();
            var drain = rig.Cleanup.DrainHintsAsync(Context(stop.Token), 128);
            await entered.Task.WaitAsync(s_wait);

            // The sink run ends, the detached drain keeps the route and its charges
            rig.Producer.Dispose();
            Assert.Same(entry, rig.HandOff.Find(route));
            Assert.Equal(2, entry.Hints);
            Assert.Equal(charged, rig.HandOff.Charged);
            Assert.Equal(0, rig.HandOff.Dropped);

            // The drain settles without issuing, and with it the last handle: the hints are dropped and their charges released
            stop.Cancel();
            release.SetResult();
            Assert.Equal(0, (await drain.WaitAsync(s_wait)).Drained);
            Assert.Null(rig.HandOff.Find(route));
            Assert.Equal(0, rig.HandOff.Routes);
            Assert.Equal(0, rig.HandOff.Charged);
            Assert.Equal(0, rig.HandOff.Queued);
            Assert.Equal(2, rig.HandOff.Dropped);
            Assert.True(rig.Exists(a));
            Assert.True(rig.Exists(b));

            // A later run on the route starts empty
            using var next = rig.HandOff.Acquire(route);
            Assert.NotSame(entry, next.Entry);
            Assert.Equal(0, next.Entry.Hints);
        }

        [Fact]
        public async Task OverflowDropsAndCounts()
        {
            var first = new[] { "_change_data/a.parquet", "_change_data/b.parquet", "_change_data/c.parquet" };
            var group = new DiscardGroup(TableId, 0, 1, "stage-1", first);
            // Room in the route's share for the group and two of its paths
            var rig = NewRig(2 * (DiscardHandOff.GroupBytes(group) + DiscardHandOff.PathBytes(first[0]) + DiscardHandOff.PathBytes(first[1])));
            await Create(rig.Storage, 1);
            foreach (var path in first)
            {
                rig.Put(path);
            }

            Assert.Equal(1, rig.Entry.Enqueue(group));
            Assert.Equal(rig.HandOff.RouteCapacity, rig.HandOff.Charged);
            Assert.Equal(2, rig.Entry.Hints);
            // The group keeps only the strings it was charged for
            Assert.Equal(first.Take(2), group.Paths);
            Assert.Equal(2, rig.HandOff.Queued);
            Assert.Equal(1, rig.HandOff.Dropped);
            // Nothing more fits, not even a group's own record
            var late = rig.Put("_change_data/d.parquet");
            Assert.Equal(1, rig.Queue(1, "stage-1", late));
            Assert.Equal(2, rig.HandOff.Dropped);
            Assert.Equal(rig.HandOff.RouteCapacity, rig.HandOff.Charged);

            // The admitted hints drain; the dropped ones are left, never deleted
            Assert.Equal(2, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(first[0]));
            Assert.False(rig.Exists(first[1]));
            Assert.True(rig.Exists(first[2]));
            Assert.True(rig.Exists(late));
            Assert.Equal(0, rig.HandOff.Charged);
            Assert.Equal(0, rig.HandOff.Queued);
        }

        [Fact]
        public async Task OverflowNeverTurnsQueuedDiscardsIntoInlineDeletes()
        {
            var testName = nameof(OverflowNeverTurnsQueuedDiscardsIntoInlineDeletes);
            // An allowance too small for any hint
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), TimeProvider.System, manualTick: true)
            {
                Discards = new DiscardHandOff(1)
            };
            await using var run = SinkRun.Start(testName, _directory, options => options.MaintenanceBudgetOverride = budget);
            await run.Started();
            var sink = run.Sink;
            Assert.Equal(DiscardMode.Queued, sink.DiscardModeForTests);

            // The attempt's writes from the store's record, checked on disk since local deletes never go through Rm
            const string Written = "OpenWrite /test/";
            var written = run.Storage.Requests.Where(x => x.StartsWith(Written + "_change_data/", StringComparison.Ordinal)).Select(x => x.Substring(Written.Length)).Distinct().OrderBy(x => x, StringComparer.Ordinal).ToList();
            Assert.NotEmpty(written);
            Assert.All(written, x => Assert.True(run.Exists(x), x));
            Assert.Equal(written, run.ChangeData());
            Assert.Equal(written.Count, sink.DiscardsForTests.Dropped);
            Assert.Equal(written.Count, budget.Discards.Dropped);
            Assert.Equal(0, sink.DiscardsForTests.Queued);
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            Assert.Equal(0, sink.DiscardRouteForTests!.Entry.Hints);
            Assert.Equal(0, budget.Discards.Charged);
        }

        [Fact]
        public async Task DrainSettlesRejectedPathsWithTheCursor()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            // A payload inside a log is never a deletion target
            var misplaced = rig.Put("_delta_log/x.parquet");
            var b = rig.Put("_change_data/b.parquet");
            var c = rig.Put("_change_data/c.parquet");
            rig.Queue(1, "stage-1", a, misplaced, b, c);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 1 };

            // The first slice deletes a and settles the rejected path after it; the third listing fails, so the rest is kept
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "Stat" && path.Full.StartsWith("/t/_delta_log/", StringComparison.Ordinal) && rig.Listings == 3)
                {
                    throw new IOException("injected probe failure");
                }
                return Task.CompletedTask;
            };
            var context = Context();
            var result = await rig.Cleanup.DrainHintsAsync(context, 128);
            Assert.Equal(2, result.Drained);
            // A turn that settled hints is work, not a decline
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.False(rig.Exists(a));
            Assert.True(rig.Exists(misplaced));
            Assert.True(rig.Exists(b));
            Assert.Equal(2, rig.Entry.Hints);
            Assert.Equal(1, rig.Counters.Kept);
            Assert.Equal(new[] { b, c }, rig.Entry.QueuedForTests.SelectMany(x => x.Paths));

            // The next turn takes the rest, at most max hints of it
            rig.Storage.Before = null;
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 1)).Drained);
            Assert.False(rig.Exists(b));
            Assert.True(rig.Exists(c));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(c));
            Assert.True(rig.Exists(misplaced));
            Assert.Equal(0, rig.HandOff.Charged);
        }

        [Fact]
        public async Task TransientGateFailuresKeepHints()
        {
            var rig = NewRig();
            await Create(rig.Storage, 2);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(2, "stage-2", a);

            // A listing that throws keeps the hint, and the drain itself does not throw
            rig.Storage.Before = (verb, _) => verb == "Ls" ? throw new IOException("injected listing failure") : Task.CompletedTask;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(1, rig.Entry.Hints);
            Assert.True(rig.Exists(a));

            // An admission whose counted reads run out before the proof keeps it for a fresh admission
            rig.Storage.Before = null;
            var spent = Context(readLimit: 3);
            Assert.True(spent.TryCountRead());
            Assert.True(spent.TryCountRead());
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(spent, 128)).Drained);
            Assert.Equal(1, rig.Entry.Hints);
            Assert.True(rig.Exists(a));
            Assert.Equal(0, rig.Counters.Dropped);

            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(a));
        }

        [Fact]
        public async Task OneDrainAtATimePerRoute()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", a);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rig.Cleanup.DrainHookForTests = async _ =>
            {
                entered.TrySetResult();
                await release.Task;
            };
            var held = rig.Cleanup.DrainHintsAsync(Context(), 128);
            try
            {
                await entered.Task.WaitAsync(s_wait);

                // Another run's drain of the route and an inline start's drop both leave the hints to the running drain
                var other = NewCleanup(rig, rig.Storage);
                Assert.Equal(0, (await other.DrainHintsAsync(Context(), 128)).Drained);
                Assert.Equal(0, rig.HandOff.DropQueued(rig.Entry.Route));
                Assert.Equal(1, rig.Entry.Hints);
                Assert.True(rig.Exists(a));
                Assert.Equal(0, rig.Listings);
            }
            finally
            {
                release.TrySetResult();
            }
            Assert.Equal(1, (await held.WaitAsync(s_wait)).Drained);
            Assert.False(rig.Exists(a));
        }

        [Fact]
        public async Task DrainStopsAtAForeignHeadBetweenItsGates()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", a);
            // A foreign commit lands after the evidence was established and before the slice's own gate lists
            rig.Storage.Before = async (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && rig.Listings == 2)
                {
                    await Commit(rig.Local, 2, Foreign(2), a);
                }
            };
            var result = await rig.Cleanup.DrainHintsAsync(Context(), 128);
            Assert.Equal(1, result.Drained);
            Assert.True(rig.Exists(a));
            Assert.Equal(1, rig.Counters.Dropped);
            Assert.Equal(0, rig.Counters.Deleted);
            Assert.Equal(0, rig.Entry.Hints);
        }

        [Fact]
        public async Task DrainPromotesOnlyBeforeItCountsARead()
        {
            var rig = NewRig();
            await Create(rig.Storage, 2);
            await BuildCheckpoint(rig.Local, 0);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(2, "stage-2", a);
            rig.Storage.ClearRequests();

            // The root and the two commits after it need more reads than this admission allows: promoted before anything is read
            var promoted = await rig.Cleanup.DrainHintsAsync(Context(readLimit: 2), 128);
            Assert.True(promoted.NeedsLongAdmission);
            Assert.True(rig.Exists(a));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.DoesNotContain(rig.Storage.Requests, x => x.StartsWith("OpenRead ", StringComparison.Ordinal));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(readLimit: long.MaxValue), 128)).Drained);
            Assert.False(rig.Exists(a));

            // One establishment per turn: commits landing later never ask for a longer replay
            var c = rig.Put("_change_data/c.parquet");
            rig.Queue(2, "stage-2", c);
            await Commit(rig.Storage, 3, Own(3));
            rig.Queue(3, "stage-3", b);
            var start = rig.Listings;
            // The slice's gate sees a head whose replay would be longer than the admission allows
            rig.Storage.Before = async (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && rig.Listings == start + 2)
                {
                    for (long version = 4; version <= 12; version++)
                    {
                        await Commit(rig.Local, version, Own(version));
                    }
                }
            };
            var context = Context(readLimit: 8);
            var turn = await rig.Cleanup.DrainHintsAsync(context, 128);
            Assert.False(turn.NeedsLongAdmission);
            Assert.Equal(2, turn.Drained);
            // Four replayed commits, then the gate's two anchors, the metadata source and the new head
            Assert.Equal(8, context.ReadsUsed);
            Assert.Equal(start + 2, rig.Listings);
            Assert.False(rig.Exists(c));
            Assert.False(rig.Exists(b));
            Assert.Equal(0, rig.Entry.Hints);
        }

        [Fact]
        public async Task ChangedStorageInstanceCannotDrainOldOrNewRoute()
        {
            // Within one drain: the sink's storage changes while a slice's gate lists, so nothing is issued and the route's hints are dropped
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(1, "stage-1", a);
            rig.Queue(1, "stage-1", b);
            var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(replacement);
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && rig.Listings == 2)
                {
                    rig.Current = replacement;
                }
                return Task.CompletedTask;
            };
            var result = await rig.Cleanup.DrainHintsAsync(Context(), 128);
            Assert.Equal(2, result.Drained);
            Assert.True(rig.Exists(a));
            Assert.True(rig.Exists(b));
            Assert.Equal(2, rig.Counters.Dropped);
            Assert.Equal(0, rig.Entry.Hints);
            Assert.False(rig.Cleanup.HasWork(JobKind.Deletion));
            // A hint queued later is dropped by the next drain before it lists
            rig.Queue(1, "stage-1", a);
            var listings = rig.Listings;
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(listings, rig.Listings);
            Assert.True(rig.Exists(a));

            // Through the sink: new discards are not routed, the queued ones are dropped, and a restart keeps its inline discards
            await using var run = SinkRun.Start(nameof(ChangedStorageInstanceCannotDrainOldOrNewRoute), _directory);
            await run.Started();
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var before = run.ChangeData();
            Assert.Equal(before.Count, entry.Hints);

            var replaced = Files.Of.LocalDisk(run.Root);
            _disposables.Add(replaced);
            run.Options.StorageLocation = OwnedHookFileStorage.Over(replaced);
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 1);
            var after = run.ChangeData();
            Assert.True(after.Count > before.Count);
            Assert.Equal(DiscardMode.Queued, sink.DiscardModeForTests);
            Assert.Equal(before.Count, entry.Hints);
            Assert.Equal(after.Count - before.Count, sink.DiscardsForTests.Dropped);

            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(after.Count, sink.DiscardsForTests.Dropped);
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            Assert.All(after, x => Assert.True(run.Exists(x)));

            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            await run.Stream.Crash();
            await SinkRun.WaitForNewWorker(run.Stream, worker);
            Assert.Equal(DiscardMode.Inline, sink.DiscardModeForTests);
            Assert.Null(sink.DiscardRouteForTests);
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 2);
            var restarted = run.ChangeData();
            Assert.True(restarted.Count > after.Count);
            Assert.All(restarted, x => Assert.True(run.Exists(x)));
            Assert.Equal(restarted.Count - after.Count, sink.DiscardsForTests.Kept);
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            // Only the injected crash
            Assert.Equal(1, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task FirstCommitHintNeedsNoExtraGet()
        {
            await using var run = SinkRun.Start(nameof(FirstCommitHintNeedsNoExtraGet), _directory);
            List<string>? atEnqueue = null;
            run.Stream.Maintenance.Budget.Discards.EnqueuedHookForTests = _ => atEnqueue = run.Storage.Requests.ToList();
            await run.Started();
            var sink = run.Sink;
            var discarded = run.ChangeData();
            Assert.NotEmpty(discarded);

            // The hint takes the table id from the metadata the attempt built and the epoch of a new table, after the stage and without a read
            var group = Assert.Single(sink.DiscardRouteForTests!.Entry.QueuedForTests);
            var actions = await ReadCommitActions(run.Local, "test", 0);
            Assert.Equal(0, group.Version);
            Assert.Equal(0, group.AdoptedAt);
            Assert.Equal(actions.Single(x => x.MetaData != null).MetaData!.Id, group.TableId);
            Assert.Equal(actions[0].CommitInfo!.StageId, group.StageId);
            Assert.Equal(discarded, group.Paths.OrderBy(x => x, StringComparer.Ordinal));
            Assert.Contains(atEnqueue!, x => x.StartsWith("OpenWrite /test/_delta_log/.00000000000000000000.json.", StringComparison.Ordinal));
            Assert.DoesNotContain(atEnqueue!, x => x.StartsWith("OpenRead ", StringComparison.Ordinal) || x.StartsWith("ReadText ", StringComparison.Ordinal) || x.StartsWith("Stat ", StringComparison.Ordinal));

            // The staged sweep is due first; then the drain reads the commit once for each checked listing: the replay that proves it, then the slice's rehash
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            // The hand-off arms the worker after the commit file is visible
            await run.Stream.Maintenance.WaitForTargetAsync(0);
            Assert.True(await run.Stream.Maintenance.TickOnceAsync());
            Assert.NotNull(worker.SweepDueAt);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            run.Storage.ClearRequests();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.All(discarded, x => Assert.False(run.Exists(x)));
            // Only maintenance probes a successor, once for each checked listing
            Assert.Equal(2, run.Storage.Requests.Count(x => x == "Stat " + CommitPath("test", 1)));
            Assert.Equal(2, run.Storage.Requests.Count(x => x == "OpenRead " + CommitPath("test", 0)));
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Deleted);
        }

        [Fact]
        public async Task HintDroppedAfterForeignAdoptionAcrossRestart()
        {
            await using var run = SinkRun.Start(nameof(HintDroppedAfterForeignAdoptionAcrossRestart), _directory);
            await run.Started();
            var sink = run.Sink;
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 1);
            await SettlePublications(run.Stream);
            var discarded = run.ChangeData();
            var entry = sink.DiscardRouteForTests!.Entry;
            Assert.Equal(discarded.Count, entry.Hints);
            Assert.Equal(2, entry.QueuedForTests.Count);
            Assert.All(entry.QueuedForTests, x => Assert.Equal(0, x.AdoptedAt));

            // The drain is held after it took its handle, so the stop detaches it and the route outlives the run
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            DeltaMaintenanceWorker? restarted = null;
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ((DeltaTableCleanup)worker.Cleanup!).DrainHookForTests = async _ =>
            {
                entered.TrySetResult();
                await release.Task;
            };
            try
            {
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
                while (!entered.Task.IsCompleted)
                {
                    Assert.True(DateTime.UtcNow < deadline, "No drain started");
                    run.Stream.Maintenance.Budget.TickNow();
                    await Task.WhenAny(worker.CurrentJob, entered.Task).WaitAsync(s_wait);
                }
                await run.Stream.Crash();

                // A foreign commit keeps the table id and references a discarded file
                await WriteCommit(run.Local, "test", 2, Foreign(2), DeltaLakeCheckpointBuildTests.Add(discarded[0]));
                restarted = await SinkRun.WaitForNewWorker(run.Stream, worker);
                Assert.Equal(1, worker.DetachedJobs);
                Assert.Equal(DiscardMode.Queued, sink.DiscardModeForTests);
                Assert.Same(entry, sink.DiscardRouteForTests!.Entry);
                Assert.Equal(discarded.Count, entry.Hints);
            }
            finally
            {
                release.TrySetResult();
            }

            // The detached drain settles without issuing
            await worker.CurrentJob.WaitAsync(s_wait);
            Assert.Equal(discarded.Count, entry.Hints);

            // The restarted sink adopts the table: its own commit after the foreign one starts a new epoch
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 3);
            Assert.Equal(3, (await ReadCommitActions(run.Local, "test", 3))[0].CommitInfo!.AdoptedAt);
            var adopted = run.ChangeData().Except(discarded).ToList();
            Assert.NotEmpty(adopted);
            Assert.Equal(discarded.Count + adopted.Count, entry.Hints);

            // The head is an own commit of epoch 3: the epoch-0 hints are dropped, the new epoch's hints drain
            var verdicts = new List<(long Version, GateVerdict Verdict)>();
            ((DeltaTableCleanup)restarted!.Cleanup!).DroppedHookForTests = (group, verdict) => verdicts.Add((group.Version, verdict));
            await run.WaitForTarget(3);
            await run.Stream.Maintenance.TickAndSettleAsync(3);
            Assert.Equal(new[] { (0L, GateVerdict.NewEpoch), (1L, GateVerdict.NewEpoch) }, verdicts);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Equal(adopted.Count, sink.DiscardsForTests.Deleted);
            Assert.All(adopted, x => Assert.False(run.Exists(x)));
            Assert.Equal(1, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task InlineDiscardFailureNeverFailsIngestion()
        {
            var testName = nameof(InlineDiscardFailureNeverFailsIngestion);
            // In-memory storage without the assertion is ineligible, so discards are deleted inline
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            DeltaLakeSink? sink = null;
            var attempts = 0;
            storage.Before = (verb, path) =>
            {
                if (verb == "Rm" && path.Full.Contains("/_change_data/"))
                {
                    // Every delete fails, with an I/O error or any other exception
                    throw Interlocked.Increment(ref attempts) % 2 == 0 ? new IOException("Injected delete failure") : new InvalidOperationException("Injected delete fault");
                }
                return Task.CompletedTask;
            };
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Insert-only versions still write change files to discard
                options.DeleteLimitsOverride = new DeleteLimits() { InsertChangeData = true };
                options.MaxFileSizeBytes = 500;
                options.SinkCreatedForTests = created => sink = created;
            });
            stream.Generate(100);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            Assert.Equal(DiscardMode.Inline, sink!.DiscardModeForTests);
            for (long version = 1; version <= 2; version++)
            {
                stream.Generate(100);
                await WaitForVersion(storage.Inner, "test", stream, version);
            }
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));

            // Every discard was tried and left behind; no version publishes a change data file
            var left = (await storage.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder).ToList();
            Assert.True(attempts >= 3, $"{attempts} deletes");
            Assert.Equal(attempts, left.Count);
            Assert.Equal(attempts, sink.DiscardDeleteFailures);
            for (long version = 0; version <= 2; version++)
            {
                Assert.Empty((await DeltaTransactionReader.ReadVersionCommit(storage.Inner, "test", version))!.CdcFiles);
            }
            Assert.Equal(0, stream.FailureNotificationCount);
            await AssertReaderResult(testName, storage.Inner, "test", stream.Users.Select(x => new { userkey = x.UserKey, name = x.FirstName }));
        }

        [Fact]
        public async Task RuntimeListingFailureNeverFailsIngestion()
        {
            await using var run = SinkRun.Start(nameof(RuntimeListingFailureNeverFailsIngestion), _directory);
            await run.Started();
            var sink = run.Sink;
            // Only maintenance probes a successor; every probe fails
            run.Storage.Before = (verb, path) => verb == "Stat" ? throw new IOException("injected probe failure") : Task.CompletedTask;
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            for (long version = 1; version <= 2; version++)
            {
                run.Stream.Generate(100);
                await WaitForVersion(run.Local, "test", run.Stream, version);
            }
            await run.WaitForTarget(2);
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            await run.Stream.Maintenance.TickAndSettleAsync(2);
            await run.Stream.HealthyFor(TimeSpan.FromMilliseconds(300));

            // Every hint is kept and nothing was deleted
            var discarded = run.ChangeData();
            Assert.Equal(discarded.Count, sink.DiscardRouteForTests!.Entry.Hints);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Contains(run.Storage.Requests, x => x.StartsWith("Stat /test/_delta_log/", StringComparison.Ordinal));
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            Assert.Equal(0, sink.DiscardsForTests.Dropped);
            Assert.Equal(0, run.Stream.FailureNotificationCount);

            // Once listings pass their checks again, the kept hints drain
            run.Storage.Before = null;
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            await run.Stream.Maintenance.TickAndSettleAsync(2);
            Assert.Equal(0, sink.DiscardRouteForTests.Entry.Hints);
            Assert.All(discarded, x => Assert.False(run.Exists(x)));
        }

        [Fact]
        public async Task DrainTurnEstablishesOnceAndListsOncePerSlice()
        {
            const string Root = "OpenRead /t/_delta_log/00000000000000000005.checkpoint.parquet";
            foreach (var maxPaths in new[] { DeletionRunSettings.DefaultMaxPaths, 4 })
            {
                // Ten published one-path groups, the newest root at 5
                var rig = NewRig();
                await Create(rig.Storage, 10);
                await BuildCheckpoint(rig.Local, 5);
                var discarded = new List<string>();
                for (long version = 1; version <= 10; version++)
                {
                    discarded.Add(rig.Put($"_change_data/c{version}.parquet"));
                    rig.Queue(version, $"stage-{version}", discarded[^1]);
                }
                rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = maxPaths };
                rig.Storage.ClearRequests();

                Assert.Equal(10, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
                Assert.All(discarded, x => Assert.False(rig.Exists(x)));
                Assert.Equal(10, rig.Counters.Deleted);
                var slices = (10 + maxPaths - 1) / maxPaths;
                // One checked listing establishes every group, then each slice's gate lists once
                Assert.Equal(1 + slices, rig.Listings);
                Assert.Equal(1 + slices, Count(rig, "Stat " + CommitPath("t", 11)));
                // One capability replay reads the root, then each gate rehashes it once
                Assert.Equal(1 + slices, Count(rig, Root));
                Assert.Equal(0, Count(rig, "OpenRead " + CommitPath("t", 0)));
                // Each anchor is proven once and rehashed by the one gate whose slice holds it
                for (long version = 1; version < 10; version++)
                {
                    Assert.Equal(2, Count(rig, "OpenRead " + CommitPath("t", version)));
                }
                // The head is replayed once and rehashed once by each gate
                Assert.Equal(1 + slices, Count(rig, "OpenRead " + CommitPath("t", 10)));
            }

            // Without a root the replay starts at commit 0, once for the whole turn
            var unrooted = NewRig();
            await Create(unrooted.Storage, 30);
            for (long version = 26; version <= 30; version++)
            {
                unrooted.Queue(version, $"stage-{version}", unrooted.Put($"_change_data/c{version}.parquet"));
            }
            unrooted.Storage.ClearRequests();
            Assert.Equal(5, (await unrooted.Cleanup.DrainHintsAsync(Context(readLimit: 64), 128)).Drained);
            Assert.Equal(1, Count(unrooted, "OpenRead " + CommitPath("t", 3)));
            // The metadata source is replayed and rehashed once, each anchor replayed and rehashed once
            Assert.Equal(2, Count(unrooted, "OpenRead " + CommitPath("t", 0)));
            Assert.Equal(31 + 6, unrooted.Storage.Requests.Count(x => x.StartsWith("OpenRead /t/_delta_log/", StringComparison.Ordinal)));
        }

        [Fact]
        public async Task RootlessDrainBeyondShortAdmissionDropsInsteadOfPromoting()
        {
            // No checkpoint roots the head, and replaying from commit 0 needs more reads than short admission allows
            var rig = NewRig();
            await Create(rig.Storage, 30);
            var discarded = new List<string>();
            for (long version = 26; version <= 30; version++)
            {
                discarded.Add(rig.Put($"_change_data/c{version}.parquet"));
                rig.Queue(version, $"stage-{version}", discarded[^1]);
            }
            var verdicts = new List<(long Version, GateVerdict Verdict)>();
            rig.Cleanup.DroppedHookForTests = (group, verdict) => verdicts.Add((group.Version, verdict));
            rig.Storage.ClearRequests();

            // The turn's hints are dropped and counted before anything is read, instead of a promotion at every turn
            var context = Context(readLimit: 20);
            var turn = await rig.Cleanup.DrainHintsAsync(context, 128);
            Assert.False(turn.NeedsLongAdmission);
            Assert.Equal(5, turn.Drained);
            Assert.Equal(Enumerable.Range(26, 5).Select(x => ((long)x, GateVerdict.NeedsLongAdmission)), verdicts);
            Assert.All(discarded, x => Assert.True(rig.Exists(x)));
            Assert.Equal((5L, 0L), (rig.Counters.Dropped, rig.Counters.Deleted));
            Assert.Equal((0, 0L), (rig.Entry.Hints, rig.HandOff.Charged));
            Assert.Equal(0, context.ReadsUsed);
            Assert.DoesNotContain(rig.Storage.Requests, x => x.StartsWith("OpenRead ", StringComparison.Ordinal));
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);

            // A torn root read and skipped leaves a replay from commit 0 the admission cannot afford: dropped too, after the root's read
            var torn = NewRig();
            await Create(torn.Storage, 3);
            await BuildCheckpoint(torn.Local, 3);
            await Commit(torn.Storage, 4, Own(4));
            var checkpoint = torn.Native($"_delta_log/{3:D20}.checkpoint.parquet");
            File.WriteAllBytes(checkpoint, new byte[new FileInfo(checkpoint).Length]);
            var c = torn.Put("_change_data/c4.parquet");
            torn.Queue(4, "stage-4", c);
            context = Context(readLimit: 2);
            Assert.Equal(1, (await torn.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(1, context.ReadsUsed);
            Assert.True(torn.Exists(c));
            Assert.Equal((1L, 0L), (torn.Counters.Dropped, torn.Counters.Deleted));
            Assert.Equal(0, torn.Entry.Hints);
        }

        [Fact]
        public async Task TransientEstablishmentOfOneGroupHoldsBackNoLaterGroup()
        {
            // The root at 5 leaves anchor 3 to be read by its establishment, anchor 7 is proven by the replay
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            var early = rig.Put("_change_data/early.parquet");
            var late = rig.Put("_change_data/late.parquet");
            rig.Queue(3, "stage-3", early);
            rig.Queue(7, "stage-7", late);
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;

            // The earlier group is kept for another turn, the later one still drains
            var context = Context();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.True(rig.Exists(early));
            Assert.False(rig.Exists(late));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.Equal(0, rig.Counters.Dropped);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);

            // A turn after its retry takes it
            rig.Storage.Before = null;
            clock.Advance(DiscardHandOff.RetryDelay(1));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(early));
            Assert.Equal(0, rig.HandOff.Charged);
        }

        [Fact]
        public async Task KeptGroupWaitsItsOwnRetryOutsideDueChecksAndTurns()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            var spacing = rig.HandOff.DrainSpacing;
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            var earlyPath = rig.Put("_change_data/early.parquet");
            var early = new DiscardGroup(TableId, 0, 3, "stage-3", new[] { earlyPath }) { QueuedAt = s_now - spacing };
            var late = rig.Put("_change_data/late.parquet");
            // Both waited the spacing; anchor 3 cannot be read
            Assert.Equal(0, rig.Entry.Enqueue(early));
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 7, "stage-7", new[] { late }) { QueuedAt = s_now - spacing }));
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.True(rig.Entry.IsDue(10, s_now, 128));

            // The later group drains; the kept one makes no turn due before its retry a minute later
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(late));
            Assert.Equal((1, 1), (rig.Entry.Hints, early.KeptTurns));
            Assert.False(rig.Entry.IsDue(10, s_now, 128));
            Assert.False(rig.Entry.IsDue(10, s_now + DiscardHandOff.RetryDelay(1) - TimeSpan.FromTicks(1), 128));
            Assert.True(rig.Entry.IsDue(10, s_now + DiscardHandOff.RetryDelay(1), 128));

            // Fresh hints batch without it, and the turn they start leaves it out
            var fresh = Enumerable.Range(0, 127).Select(i => $"_change_data/fresh-{i}.parquet").ToArray();
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 8, "stage-8", fresh) { QueuedAt = s_now }));
            Assert.False(rig.Entry.IsDue(10, s_now, 128));
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 9, "stage-9", new[] { "_change_data/fresh-last.parquet" }) { QueuedAt = s_now }));
            Assert.True(rig.Entry.IsDue(10, s_now, 128));
            Assert.Equal(128, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal((1, 1), (rig.Entry.Hints, early.KeptTurns));

            // Each keep doubles its wait, and keeps are never failed deletes, so it is never left behind
            for (int keeps = 2; keeps <= DeletionRun.MaxFailedSlices + 1; keeps++)
            {
                clock.Advance(DiscardHandOff.RetryDelay(keeps - 1));
                Assert.True(rig.Entry.IsDue(10, clock.GetUtcNow(), 128));
                Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
                Assert.Equal(keeps, early.KeptTurns);
                Assert.Equal(clock.GetUtcNow() + DiscardHandOff.RetryDelay(keeps), early.ReadyAt);
            }
            Assert.Equal((1, 0L, 0L), (rig.Entry.Hints, rig.Counters.Leaked, rig.HandOff.Leaked));

            // Readable again, it is taken by the first turn after its retry, not before
            rig.Storage.Before = null;
            var listings = rig.Listings;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(listings, rig.Listings);
            clock.Advance(DiscardHandOff.RetryDelay(DeletionRun.MaxFailedSlices + 1));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(earlyPath));
            Assert.Equal(0, rig.HandOff.Charged);

            // Waiting for its retry, a kept group is still dropped and counted once the route's last handle settles
            var settled = NewRig(clock: clock);
            await Create(settled.Storage, 10);
            await BuildCheckpoint(settled.Local, 5);
            var waiting = new DiscardGroup(TableId, 0, 3, "stage-3", new[] { settled.Put("_change_data/kept.parquet") }) { Counters = settled.Counters };
            Assert.Equal(0, settled.Entry.Enqueue(waiting));
            settled.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await settled.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(1, waiting.KeptTurns);
            settled.Producer.Dispose();
            Assert.Equal((0, 1L, 1L), (settled.Entry.Hints, settled.HandOff.Dropped, settled.Counters.Dropped));
        }

        [Fact]
        public async Task NewestRootOverTheDiskCapDropsHintsAnOlderRootCannotAfford()
        {
            // The newest root at 25 is larger than the disk cap itself, and the root at 2 leaves more commits than the admission reads
            var rig = NewRig();
            await Create(rig.Storage, 30);
            await BuildCheckpoint(rig.Local, 2);
            await BuildCheckpoint(rig.Local, 25);
            var c = rig.Put("_change_data/c30.parquet");
            rig.Queue(30, "stage-30", c);
            var size = new FileInfo(rig.Native($"_delta_log/{25:D20}.checkpoint.parquet")).Length;
            var verdicts = new List<GateVerdict>();
            rig.Cleanup.DroppedHookForTests = (_, verdict) => verdicts.Add(verdict);

            // A root no turn can ever read: the hints are dropped and counted
            var context = Context(readLimit: 20, diskCap: size - 1);
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(GateVerdict.NeedsLongAdmission, Assert.Single(verdicts));
            Assert.Equal((0, 1L), (rig.Entry.Hints, rig.Counters.Dropped));
            Assert.True(rig.Exists(c));
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.Equal((0L, 0L), (rig.HandOff.Held, rig.HandOff.Charged));
        }

        [Fact]
        public async Task OnlyRootOverTheDiskCapWithoutHistoryDropsHints()
        {
            // Commits before 25 are gone and the only root, at 25, is larger than the disk cap itself
            var rig = NewRig();
            await Create(rig.Storage, 30);
            await BuildCheckpoint(rig.Local, 25);
            for (long version = 0; version < 25; version++)
            {
                File.Delete(rig.Native($"_delta_log/{version:D20}.json"));
            }
            var c = rig.Put("_change_data/c30.parquet");
            rig.Queue(30, "stage-30", c);
            var size = new FileInfo(rig.Native($"_delta_log/{25:D20}.checkpoint.parquet")).Length;
            var verdicts = new List<GateVerdict>();
            rig.Cleanup.DroppedHookForTests = (_, verdict) => verdicts.Add(verdict);

            var context = Context(diskCap: size - 1);
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(GateVerdict.NeedsLongAdmission, Assert.Single(verdicts));
            Assert.Equal((0, 1L), (rig.Entry.Hints, rig.Counters.Dropped));
            Assert.True(rig.Exists(c));
            Assert.Equal((0L, 0L), (rig.HandOff.Held, rig.HandOff.Charged));
        }

        [Fact]
        public async Task DiskLedgerInUseKeepsHintsForTheNextTurn()
        {
            var rig = NewRig();
            await Create(rig.Storage, 30);
            await BuildCheckpoint(rig.Local, 2);
            await BuildCheckpoint(rig.Local, 25);
            var c = rig.Put("_change_data/c30.parquet");
            rig.Queue(30, "stage-30", c);

            // Other jobs hold the whole shared disk ledger while the newest root downloads: the hints are kept and the turn declines
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var context = Context(readLimit: 20, owner: owner);
            context.Scratch.Ledger.Reserve(context.Scratch.Ledger.Cap);
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal((1, 0L), (rig.Entry.Hints, rig.Counters.Dropped));
            Assert.True(rig.Exists(c));
            var declined = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (declined.Kind, declined.Reason));
            Assert.Contains(nameof(CapabilityCheck.Unavailable), declined.Detail);
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);

            // Once the shared ledger has room, the hints drain at the table's next turn
            context.Scratch.Ledger.Release(context.Scratch.Ledger.Cap);
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(readLimit: 20, owner: owner), 128)).Drained);
            Assert.False(rig.Exists(c));
            Assert.Equal(0L, rig.Counters.Dropped);
        }

        [Fact]
        public async Task DrainTurnIsDueOnlyForDrainableHintsAfterTheSpacing()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            var spacing = rig.HandOff.DrainSpacing;
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", new[] { a }) { QueuedAt = s_now }));
            // Staged for version 2, which is not published yet
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 2, "stage-2", new[] { b }) { QueuedAt = s_now }));
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true) { Discards = rig.HandOff };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false },
                rig.Storage, "t", "t", null) { Cleanup = rig.Cleanup };
            budget.Register(worker);
            try
            {
                worker.Published(1);
                // The staged sweep is due at the first hand-off
                Assert.True(await TickOnce(budget, worker));

                // Within the spacing no turn starts
                Assert.False(await TickOnce(budget, worker));
                clock.Advance(spacing - TimeSpan.FromTicks(1));
                Assert.False(await TickOnce(budget, worker));
                clock.Advance(TimeSpan.FromTicks(1));
                Assert.True(await TickOnce(budget, worker));
                Assert.False(rig.Exists(a));
                Assert.True(rig.Exists(b));
                Assert.Equal(DateTimeOffset.MinValue, worker.CleanupRetryAt(JobKind.Deletion));

                // A hint above the head starts no turn however long it waits, so no floor delays it once its version publishes
                clock.Advance(spacing * 2);
                Assert.False(await TickOnce(budget, worker));
                await Commit(rig.Storage, 2, Own(2));
                worker.Published(2);
                Assert.True(await TickOnce(budget, worker));
                Assert.False(rig.Exists(b));
                Assert.Equal(0, rig.Entry.Hints);
                Assert.Equal(2, rig.Counters.Deleted);
            }
            finally
            {
                await worker.StopAsync(s_wait);
            }
        }

        [Fact]
        public async Task NoWorkTurnLeavingOnlyUnpublishedHintsSetsNoFloor()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", new[] { a }) { QueuedAt = s_now }));
            // Staged for version 2, which is not published yet
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 2, "stage-2", new[] { b }) { QueuedAt = s_now }));
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true) { Discards = rig.HandOff };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false },
                rig.Storage, "t", "t", null) { Cleanup = rig.Cleanup };
            budget.Register(worker);
            // Another run's drain of the route, held after it took the route
            var other = NewCleanup(rig, rig.Storage, clock);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            other.DrainHookForTests = async _ =>
            {
                entered.TrySetResult();
                await release.Task;
            };
            // The worker's turn, held before it settles
            var holdSettle = false;
            var settling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var settle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point != "settle" || !Volatile.Read(ref holdSettle))
                    {
                        return Task.CompletedTask;
                    }
                    settling.TrySetResult();
                    return settle.Task;
                }
            };
            try
            {
                worker.Published(1);
                Assert.True(await TickOnce(budget, worker));
                clock.Advance(rig.HandOff.DrainSpacing);
                var held = other.DrainHintsAsync(Context(), 128);
                await entered.Task.WaitAsync(s_wait);
                Volatile.Write(ref holdSettle, true);
                budget.TickNow();
                var job = worker.CurrentJob;
                await settling.Task.WaitAsync(s_wait);

                // The turn found the route draining and serviced nothing; the other drain then takes a, leaving b above the head
                release.SetResult();
                Assert.Equal(1, (await held.WaitAsync(s_wait)).Drained);
                settle.SetResult();
                await job.WaitAsync(s_wait);
                Assert.Equal(OutcomeKind.NoWork, worker.LastOutcomeForTests!.Kind);
                Assert.Equal(DateTimeOffset.MinValue, worker.CleanupRetryAt(JobKind.Deletion));

                // Once b's version publishes, the next tick drains it
                await Commit(rig.Storage, 2, Own(2));
                worker.Published(2);
                Assert.True(await TickOnce(budget, worker));
                Assert.False(rig.Exists(a));
                Assert.False(rig.Exists(b));
            }
            finally
            {
                release.TrySetResult();
                settle.TrySetResult();
                await worker.StopAsync(s_wait);
            }
        }

        [Fact]
        public async Task HintsNoTurnCanDrainPressNoOtherRoute()
        {
            // One route fills its half of the allowance with hints above its listed head
            var rig = NewRig(capacity: 64 << 10);
            await Create(rig.Storage, 1);
            for (var version = 5; rig.Entry.Enqueue(new DiscardGroup(TableId, 0, version, $"stage-{version}", Paths("held", version, 64))) == 0; version++)
            {
            }
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.True(rig.Entry.Charged > rig.HandOff.RouteCapacity - 4096);

            // Another route's fresh hints, fewer than a turn, take the charge past three quarters
            using var storage = new ScriptedStorage();
            using var other = rig.HandOff.Acquire(new DiscardRoute(storage, "other"));
            Assert.Equal(0, other.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", Paths("fresh", 1, 100)) { QueuedAt = s_now }));
            Assert.True(rig.HandOff.Charged >= rig.HandOff.Capacity - rig.HandOff.Capacity / 4);

            // The allowance is not short of hints a turn could drain, so the fresh ones wait for their spacing
            Assert.False(other.Entry.IsDue(1, s_now, 128));
            Assert.True(other.Entry.IsDue(1, s_now + rig.HandOff.DrainSpacing, 128));
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);

            // Hints a turn kept count as held too, and nothing stays held once they settle
            var kept = NewRig(capacity: 64 << 10, clock: new FakeTimeProvider(s_now));
            await Create(kept.Storage, 10);
            await BuildCheckpoint(kept.Local, 5);
            var group = new DiscardGroup(TableId, 0, 3, "stage-3", Paths("kept", 3, 10));
            Assert.Equal(0, kept.Entry.Enqueue(group));
            kept.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await kept.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(group.Charged, kept.HandOff.Held);
            kept.Producer.Dispose();
            Assert.Equal((0L, 0L), (kept.HandOff.Held, kept.HandOff.Charged));
        }

        [Fact]
        public async Task PressureIsMeasuredInTheRoomHeldHintsLeave()
        {
            // One route holds its half of the allowance with hints above its listed head
            var rig = NewRig(capacity: 64 << 10);
            await Create(rig.Storage, 1);
            for (var version = 5; rig.Entry.Enqueue(new DiscardGroup(TableId, 0, version, $"stage-{version}", Paths("held", version, 64))) == 0; version++)
            {
            }
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);
            Assert.True(rig.HandOff.Held > rig.HandOff.RouteCapacity - 4096);

            // Another route's fresh hints, fewer than a turn, are due once they fill three quarters of the rest
            using var storage = new ScriptedStorage();
            using var other = rig.HandOff.Acquire(new DiscardRoute(storage, "other"));
            var rest = rig.HandOff.Capacity - rig.HandOff.Held;
            for (var group = 0; rig.HandOff.Charged - rig.HandOff.Held < rest - rest / 4; group++)
            {
                Assert.False(other.Entry.IsDue(1, s_now, 128));
                Assert.Equal(0, other.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", Paths($"fresh{group}", 1, 8)) { QueuedAt = s_now }));
            }
            Assert.InRange(other.Entry.Hints, 1, 127);
            Assert.True(rig.HandOff.UnderPressure);
            Assert.True(other.Entry.IsDue(1, s_now, 128));
        }

        [Fact]
        public async Task TablesThatCannotBeCheckedPressNoOtherRouteUntilTheirNextTurn()
        {
            var clock = new FakeTimeProvider(s_now);
            var handOff = new DiscardHandOff(256 << 10);
            // Two routes whose capability replay cannot read commit 0, each with more one-hint groups than a turn takes
            var stuck = new List<Rig>();
            for (int i = 0; i < 2; i++)
            {
                var rig = NewRig(clock: clock, handOff: handOff);
                await Create(rig.Storage, 1);
                for (var group = 0; group < 248; group++)
                {
                    Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", Paths($"stuck{group}", 1, 1))));
                }
                rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 0) ? throw new IOException("injected read failure") : Task.CompletedTask;
                var context = Context();
                Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
                var declined = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
                Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (declined.Kind, declined.Reason));
                Assert.Contains(nameof(CapabilityCheck.Unavailable), declined.Detail);
                stuck.Add(rig);
            }
            Assert.True(handOff.Charged >= handOff.Capacity - handOff.Capacity / 4);

            // A healthy route commits ten times a second apart, each discarding one file, all well within the spacing
            var healthy = NewRig(clock: clock, handOff: handOff);
            await Create(healthy.Storage, 10);
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true) { Discards = handOff };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false },
                healthy.Storage, "t", "t", null) { Cleanup = healthy.Cleanup };
            budget.Register(worker);
            try
            {
                worker.Published(10);
                // The staged sweep is due at the first hand-off
                Assert.True(await TickOnce(budget, worker));
                var fresh = new List<string>();
                var turns = 0;
                healthy.Storage.ClearRequests();
                for (long version = 11; version <= 20; version++)
                {
                    await Commit(healthy.Storage, version, Own(version));
                    fresh.Add(healthy.Put($"_change_data/c{version}.parquet"));
                    Assert.Equal(0, healthy.Entry.Enqueue(new DiscardGroup(TableId, 0, version, $"stage-{version}", new[] { fresh[^1] }) { QueuedAt = clock.GetUtcNow() }));
                    worker.Published(version);
                    turns += await TickOnce(budget, worker) ? 1 : 0;
                    clock.Advance(TimeSpan.FromSeconds(1));
                }
                Assert.True(turns == 0, $"{turns} turns drained {fresh.Count(x => !healthy.Exists(x))} of 10 commits through {healthy.Listings} log listings, {handOff.Held} of {handOff.Charged} bytes held");
                Assert.False(handOff.UnderPressure);
                Assert.All(fresh, x => Assert.True(healthy.Exists(x)));
                // Due once the oldest waited the spacing
                Assert.False(healthy.Entry.IsDue(20, s_now + handOff.DrainSpacing - TimeSpan.FromTicks(1), 128));
                Assert.True(healthy.Entry.IsDue(20, s_now + handOff.DrainSpacing, 128));
            }
            finally
            {
                await worker.StopAsync(s_wait);
            }

            // Hints queued into a stuck route's inbox since its turn are held with the rest
            Assert.Equal(0, stuck[0].Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", Paths("late", 1, 1))));
            Assert.Equal(stuck[0].Entry.Charged + stuck[1].Entry.Charged, handOff.Held);

            // A settled route holds nothing back and its hints drop and count
            var hints = stuck[0].Entry.Hints;
            stuck[0].Producer.Dispose();
            Assert.Equal(hints, handOff.Dropped);
            Assert.Equal(stuck[1].Entry.Charged, handOff.Held);
        }

        [Fact]
        public async Task TableHoldLiftsWhenTheRoutesNextTurnStarts()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", a);
            // The capability replay cannot read commit 0
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 0) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(1, "stage-1", b);
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);

            // Lifted once the next turn starts, and a turn that drains leaves nothing held
            var heldAtListing = -1L;
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && heldAtListing < 0)
                {
                    heldAtListing = rig.HandOff.Held;
                }
                return Task.CompletedTask;
            };
            var context = Context();
            Assert.Equal(2, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.Equal(0L, heldAtListing);
            Assert.Equal((0L, 0L), (rig.HandOff.Held, rig.HandOff.Charged));

            // A turn whose only result is a failed delete holds the table again
            var c = rig.Put("_change_data/c.parquet");
            rig.Queue(1, "stage-1", c);
            rig.Storage.Before = null;
            rig.Cleanup.BeforeDeleteForTests = _ => throw new IOException("injected delete failure");
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var failed = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeleteFailed), (failed.Kind, failed.Reason));
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);

            // The next turn, with nothing due before the failed hint's retry, ends without work and leaves it lifted
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.Equal(1, rig.Entry.Hints);
            Assert.Equal(0L, rig.HandOff.Held);

            // At its retry, a log listing that throws holds the table again
            clock.Advance(DiscardHandOff.RetryDelay(1));
            rig.Storage.Before = (verb, _) => verb == "Ls" ? throw new IOException("injected listing failure") : Task.CompletedTask;
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.Declined, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);
        }

        [Fact]
        public async Task LongAdmissionNeededAfterACountedReadHoldsTheTableWithoutRetries()
        {
            var rig = NewRig(clock: new FakeTimeProvider(s_now));
            await Create(rig.Storage, 2);
            await BuildCheckpoint(rig.Local, 0);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(2, "stage-2", a);
            // Staged for version 3, which is not published yet
            rig.Queue(3, "stage-3", rig.Put("_change_data/unpublished.parquet"));

            // The replay needs more reads than the admission allows, and the admission already counted one
            var context = Context(readLimit: 2);
            Assert.True(context.TryCountRead());
            var kept = await rig.Cleanup.DrainHintsAsync(context, 128);
            Assert.Equal((0, false), (kept.Drained, kept.NeedsLongAdmission));
            var declined = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (declined.Kind, declined.Reason));
            Assert.Contains(nameof(CapabilityCheck.NeedsLongAdmission), declined.Detail);
            Assert.True(rig.Exists(a));
            Assert.Equal((2, 0L), (rig.Entry.Hints, rig.Counters.Dropped));

            // The table is held whole and counted once, hints queued since included, and no hint waits a retry of its own
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(2, "stage-2", b);
            Assert.Equal(rig.Entry.Charged, rig.HandOff.Held);
            Assert.All(rig.Entry.QueuedForTests, x => Assert.Equal(0, x.KeptTurns));

            // The next turn promotes before it reads, and the long one drains the published ones
            Assert.True((await rig.Cleanup.DrainHintsAsync(Context(readLimit: 2), 128)).NeedsLongAdmission);
            Assert.Equal(2, (await rig.Cleanup.DrainHintsAsync(Context(readLimit: long.MaxValue), 128)).Drained);
            Assert.False(rig.Exists(a));
            Assert.False(rig.Exists(b));
            Assert.Equal((1, rig.Entry.Charged), (rig.Entry.Hints, rig.HandOff.Held));
        }

        private static string[] Paths(string prefix, long version, int count) => Enumerable.Range(0, count).Select(i => $"_change_data/{prefix}-{version:D4}-{i:D3}-{new string('x', 40)}.parquet").ToArray();

        [Fact]
        public async Task KeptHintsCannotFillATurnAheadOfFresherGroups()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            // A full turn of hints whose anchor 3 cannot be read, kept by one turn
            var kept = new DiscardGroup(TableId, 0, 3, "stage-3", Enumerable.Range(0, 128).Select(i => $"_change_data/kept-{i}.parquet").ToArray());
            Assert.Equal(0, rig.Entry.Enqueue(kept));
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(1, kept.KeptTurns);
            var fresh = rig.Put("_change_data/fresh.parquet");
            rig.Queue(7, "stage-7", fresh);

            // Both due, the fresher group goes first and the kept one takes what the budget leaves
            clock.Advance(DiscardHandOff.RetryDelay(1));
            rig.Storage.ClearRequests();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(fresh));
            Assert.Equal(128, rig.Entry.Hints);
            // The same turn retried the kept group and kept it again
            Assert.Equal(1, Count(rig, "OpenRead " + CommitPath("t", 3)));
            Assert.Equal(2, kept.KeptTurns);
        }

        [Fact]
        public async Task ReserveGoesToTheKeptGroupWhoseRetryPassedFirst()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            // Two groups queued in this order whose anchors 3 and 4 cannot be read
            var first = new DiscardGroup(TableId, 0, 3, "stage-3", new[] { rig.Put("_change_data/first.parquet") });
            var secondPath = rig.Put("_change_data/second.parquet");
            var second = new DiscardGroup(TableId, 0, 4, "stage-4", new[] { secondPath });
            Assert.Equal(0, rig.Entry.Enqueue(first));
            Assert.Equal(0, rig.Entry.Enqueue(second));
            var secondFails = true;
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && (path.Full == CommitPath("t", 3) || (secondFails && path.Full == CommitPath("t", 4)))
                ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal((1, 1), (first.KeptTurns, second.KeptTurns));

            // A one-hint turn retries only the first and keeps it longer; the second's retry passes first
            clock.Advance(DiscardHandOff.RetryDelay(1));
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 1)).Drained);
            Assert.Equal((2, 1), (first.KeptTurns, second.KeptTurns));
            Assert.True(second.ReadyAt < first.ReadyAt);

            // Both ready, fresh hints fill all but the reserve, and the reserve goes to the second
            clock.Advance(DiscardHandOff.RetryDelay(2));
            secondFails = false;
            var fresh = Enumerable.Range(0, 3).Select(i => rig.Put($"_change_data/fresh-{i}.parquet")).ToArray();
            rig.Queue(7, "stage-7", fresh);
            Assert.Equal(4, (await rig.Cleanup.DrainHintsAsync(Context(), 4)).Drained);
            Assert.All(fresh, x => Assert.False(rig.Exists(x)));
            Assert.False(rig.Exists(secondPath));
            Assert.Equal(2, first.KeptTurns);
        }

        [Fact]
        public async Task KeptGroupWaitsAtLeastThreeTimesAsLongAsTheTurnThatKeptIt()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            var group = new DiscardGroup(TableId, 0, 3, "stage-3", new[] { rig.Put("_change_data/slow.parquet") }) { QueuedAt = s_now - rig.HandOff.DrainSpacing };
            Assert.Equal(0, rig.Entry.Enqueue(group));
            // The read of anchor 3 fails after a hundred seconds
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full == CommitPath("t", 3))
                {
                    clock.Advance(TimeSpan.FromSeconds(100));
                    throw new IOException("injected slow read failure");
                }
                return Task.CompletedTask;
            };

            // Kept by a turn that took longer than its first retry delay, it waits three times the turn instead
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            var end = clock.GetUtcNow();
            var took = end - s_now;
            Assert.True(took * 3 > DiscardHandOff.RetryDelay(1));
            Assert.Equal(1, group.KeptTurns);
            Assert.Equal(end + took * 3, group.ReadyAt);
            Assert.False(rig.Entry.IsDue(10, end + took * 3 - TimeSpan.FromTicks(1), 128));
            Assert.True(rig.Entry.IsDue(10, end + took * 3, 128));

            // A quick turn keeps the doubling delay
            clock.Advance(took * 3);
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(2, group.KeptTurns);
            Assert.Equal(clock.GetUtcNow() + DiscardHandOff.RetryDelay(2), group.ReadyAt);
        }

        [Fact]
        public async Task KeptGroupDoesNotMakeEveryCommitADrainTurn()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            var spacing = rig.HandOff.DrainSpacing;
            // The root at 5 leaves anchor 3 to its establishment, whose read keeps failing
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            var stuck = rig.Put("_change_data/stuck.parquet");
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 3, "stage-3", new[] { stuck }) { QueuedAt = s_now }));
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true) { Discards = rig.HandOff };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false },
                rig.Storage, "t", "t", null) { Cleanup = rig.Cleanup };
            budget.Register(worker);
            try
            {
                worker.Published(10);
                // The staged sweep is due at the first hand-off
                Assert.True(await TickOnce(budget, worker));
                // Once the kept group waited the spacing, its own turn keeps it until its retry
                clock.Advance(spacing);
                Assert.True(await TickOnce(budget, worker));
                Assert.True(rig.Exists(stuck));
                clock.Advance(DiscardHandOff.RetryDelay(1));

                // Ten commits a second apart, each discarding one file, all well within their own hints' spacing
                var fresh = new List<string>();
                rig.Storage.ClearRequests();
                for (long version = 11; version <= 20; version++)
                {
                    await Commit(rig.Storage, version, Own(version));
                    fresh.Add(rig.Put($"_change_data/c{version}.parquet"));
                    Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, version, $"stage-{version}", new[] { fresh[^1] }) { QueuedAt = clock.GetUtcNow() }));
                    worker.Published(version);
                    await TickOnce(budget, worker);
                    clock.Advance(TimeSpan.FromSeconds(1));
                }
                // Only the turn the kept group is due in takes a fresh hint along
                var drained = fresh.Count(x => !rig.Exists(x));
                Assert.True(drained == 1, $"{drained} of 10 commits drained within the spacing, through {rig.Listings} log listings and {Count(rig, "OpenRead " + $"/t/_delta_log/{5:D20}.checkpoint.parquet")} checkpoint downloads");
            }
            finally
            {
                await worker.StopAsync(s_wait);
            }
        }

        [Fact]
        public async Task KeptGroupThatPassesItsChecksIsOrdinaryAgain()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            // More hints than a turn takes, whose anchor 3 cannot be read in the first turn
            var group = new DiscardGroup(TableId, 0, 3, "stage-3", Enumerable.Range(0, 130).Select(i => rig.Put($"_change_data/k-{i}.parquet")).ToArray());
            Assert.Equal(0, rig.Entry.Enqueue(group));
            Func<string, IOPath, Task> failAnchor = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            rig.Storage.Before = failAnchor;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(1, group.KeptTurns);

            // Readable at its retry, it passes and what the turn left is neither kept nor held back
            rig.Storage.Before = null;
            clock.Advance(DiscardHandOff.RetryDelay(1));
            Assert.Equal(128, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal((2, 0), (group.Remaining, group.KeptTurns));
            Assert.Equal(0, rig.HandOff.Held);

            // Kept again, it waits one minute again
            rig.Storage.Before = failAnchor;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(1, group.KeptTurns);
            Assert.Equal(clock.GetUtcNow() + DiscardHandOff.RetryDelay(1), group.ReadyAt);
        }

        [Fact]
        public async Task KeptGroupIsRetriedUnderSustainedLoad()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            var kept = new DiscardGroup(TableId, 0, 3, "stage-3", Enumerable.Range(0, 64).Select(i => rig.Put($"_change_data/kept-{i}.parquet")).ToArray());
            Assert.Equal(0, rig.Entry.Enqueue(kept));
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(1, kept.KeptTurns);
            rig.Storage.Before = null;
            clock.Advance(DiscardHandOff.RetryDelay(1));

            // More than a turn of fresher hints leaves the kept group its quarter of the turn, no more
            var load = new DiscardGroup(TableId, 0, 7, "stage-7", Enumerable.Range(0, 128).Select(i => rig.Put($"_change_data/load-{i}.parquet")).ToArray());
            var later = new DiscardGroup(TableId, 0, 8, "stage-8", new[] { rig.Put("_change_data/later.parquet") });
            Assert.Equal(0, rig.Entry.Enqueue(load));
            Assert.Equal(0, rig.Entry.Enqueue(later));
            Assert.Equal(128, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal((32, 32, 1), (kept.Remaining, load.Remaining, later.Remaining));
            Assert.Equal(0, rig.Counters.Dropped);
        }

        [Fact]
        public async Task TurnsThatOnlyKeepGroupsDoNotBackOffTheDrain()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            var spacing = rig.HandOff.DrainSpacing;
            // The root at 5 leaves anchor 3 to its establishment, whose read keeps failing
            await Create(rig.Storage, 10);
            await BuildCheckpoint(rig.Local, 5);
            var stuck = rig.Put("_change_data/stuck.parquet");
            Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 3, "stage-3", new[] { stuck }) { QueuedAt = s_now - spacing }));
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3) ? throw new IOException("injected read failure") : Task.CompletedTask;
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true) { Discards = rig.HandOff };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false },
                rig.Storage, "t", "t", null) { Cleanup = rig.Cleanup };
            budget.Register(worker);
            try
            {
                worker.Published(10);
                // The staged sweep is due at the first hand-off
                Assert.True(await TickOnce(budget, worker));

                // Each of the group's own retries keeps it again, and none backs off the drain
                for (int keeps = 1; keeps <= 4; keeps++)
                {
                    Assert.True(await TickOnce(budget, worker));
                    Assert.Equal(OutcomeKind.NoWork, worker.LastOutcomeForTests!.Kind);
                    Assert.Equal(DateTimeOffset.MinValue, worker.CleanupRetryAt(JobKind.Deletion));
                    if (keeps < 4)
                    {
                        clock.Advance(DiscardHandOff.RetryDelay(keeps));
                    }
                }

                // A fresh hint drains once it waited the spacing, while the kept group waits its eight minutes
                var fresh = rig.Put("_change_data/fresh.parquet");
                Assert.Equal(0, rig.Entry.Enqueue(new DiscardGroup(TableId, 0, 8, "stage-8", new[] { fresh }) { QueuedAt = clock.GetUtcNow() }));
                clock.Advance(spacing);
                Assert.True(await TickOnce(budget, worker));
                Assert.False(rig.Exists(fresh));
                Assert.True(rig.Exists(stuck));
            }
            finally
            {
                await worker.StopAsync(s_wait);
            }
        }

        // One tick; true when it started a job of the worker, which then settled
        private static async Task<bool> TickOnce(DeltaMaintenanceBudget budget, DeltaMaintenanceWorker worker)
        {
            var before = worker.CurrentJob;
            budget.TickNow();
            var job = worker.CurrentJob;
            if (ReferenceEquals(before, job))
            {
                return false;
            }
            await job.WaitAsync(s_wait);
            return true;
        }

        [Fact]
        public async Task GateStopOfOneGroupDropsOnlyThatGroup()
        {
            var rig = NewRig();
            await Create(rig.Storage, 3);
            var a = new[] { rig.Put("_change_data/a1.parquet"), rig.Put("_change_data/a2.parquet"), rig.Put("_change_data/a3.parquet") };
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(2, "stage-2", a);
            rig.Queue(3, "stage-3", b);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 2 };
            var verdicts = new List<(long Version, GateVerdict Verdict)>();
            rig.Cleanup.DroppedHookForTests = (group, verdict) => verdicts.Add((group.Version, verdict));
            // The second slice's gate sees commit 2 replaced by a foreign commit adding a3
            rig.Storage.Before = async (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && rig.Listings == 3)
                {
                    await Commit(rig.Storage, 2, Foreign(2), a[2]);
                }
            };

            // Only the replaced anchor's group is dropped; the other drains through a run of its own
            Assert.Equal(4, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(a[0]));
            Assert.False(rig.Exists(a[1]));
            Assert.True(rig.Exists(a[2]));
            Assert.False(rig.Exists(b));
            Assert.Equal(new[] { (2L, GateVerdict.AnchorReplaced) }, verdicts);
            Assert.Equal(1, rig.Counters.Dropped);
            Assert.Equal(3, rig.Counters.Deleted);
            Assert.Equal(4, rig.Listings);
            Assert.Equal(0, rig.Entry.Hints);
        }

        [Fact]
        public async Task ReplayProofsServeOnlyTheirOwnListing()
        {
            var rig = NewRig();
            await Create(rig.Storage, 3);
            var (check, first) = await ListingEvidence.ReadLogAsync(rig.Configuration, rig.Storage, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            var capability = await CleanupCapability.CheckAsync(rig.Configuration, rig.Storage, first!, Context(), new[] { 2L });
            Assert.Equal(CapabilityCheck.Capable, capability.Check);
            Assert.True(capability.Proofs.ContainsKey(2));
            var established = await CleanupEvidence.EstablishAsync(rig.Configuration, rig.Storage, first!, 2, "stage-2", false, Context(), capability.Capability!, capability.Proofs);
            Assert.Equal(GateVerdict.Passed, established.Verdict);

            // Commit 2 replaced: a later listing reads it again instead of taking the replay's proof
            await Commit(rig.Storage, 2, Foreign(2));
            (check, var second) = await ListingEvidence.ReadLogAsync(rig.Configuration, rig.Storage, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            var later = await CleanupEvidence.EstablishAsync(rig.Configuration, rig.Storage, second!, 2, "stage-2", false, Context(), capability.Capability!, capability.Proofs);
            Assert.Equal(GateVerdict.NotOwn, later.Verdict);
        }

        [Fact]
        public async Task EvidenceSetGateNeverPassesWithoutVolatileEvidence()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var metered = MeteredFileStorage.Wrap(rig.Storage, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
            var (_, log) = await ListingEvidence.ReadLogAsync(rig.Configuration, metered, -1, default);
            var durable = (await CleanupEvidence.EstablishAsync(rig.Configuration, metered, log!, 1, "stage-1", true, Context())).Evidence!;
            var executor = new DeletionExecutor(rig.Configuration, metered);
            Assert.Equal(KeepReason.None, executor.TryTarget(a, TargetKind.Payload, out var target));

            // A slice relying on no evidence, or on a durable one, issues nothing
            foreach (var relied in new[] { Array.Empty<CleanupEvidence>(), new[] { durable } })
            {
                var run = new DeletionRun(executor, ChunkGate.ForEvidences(rig.Configuration, metered, () => relied), new ListDeletionSource(new[] { target }), TimeProvider.System);
                var slice = await run.RunSliceAsync(Context());
                Assert.Equal(SliceStatus.Retry, slice.Status);
                Assert.Equal(GateVerdict.Unavailable, slice.Gate!.Verdict);
                Assert.Equal(0, slice.Issued);
                Assert.True(rig.Exists(a));
            }
        }

        [Fact]
        public async Task GateKeepsHintsWhenItsHeadChangesAfterTheListing()
        {
            var rig = NewRig();
            await Create(rig.Storage, 2);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", a);
            // The head, neither an anchor nor a source, grows between the gate's listing and its read
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full == CommitPath("t", 2) && rig.Listings == 2)
                {
                    File.AppendAllText(rig.Native($"_delta_log/{2:D20}.json"), "\n");
                }
                return Task.CompletedTask;
            };
            var context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var kept = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (kept.Kind, kept.Reason));
            Assert.True(rig.Exists(a));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.Equal(0, rig.Counters.Dropped);
        }

        [Fact]
        public async Task PartiallyAdmittedAttemptKeepsAndCountsOnlyItsAdmittedPaths()
        {
            await using var run = SinkRun.Start(nameof(PartiallyAdmittedAttemptKeepsAndCountsOnlyItsAdmittedPaths), _directory);
            var handOff = run.Stream.Maintenance.Budget.Discards;
            // The first attempt finds room for its group and its first path only
            var filled = 0;
            handOff.EnqueuedHookForTests = group =>
            {
                if (Interlocked.Exchange(ref filled, 1) == 0)
                {
                    handOff.TryCharge(handOff.Capacity - handOff.Charged - DiscardHandOff.GroupBytes(group) - DiscardHandOff.PathBytes(group.Paths[0]));
                }
            };
            await run.Started();
            var sink = run.Sink;
            var discarded = run.ChangeData();
            Assert.True(discarded.Count > 1);
            var group = Assert.Single(sink.DiscardRouteForTests!.Entry.QueuedForTests);
            Assert.Single(group.Paths);
            Assert.Equal(1, sink.DiscardRouteForTests.Entry.Hints);
            Assert.Equal(1, sink.DiscardsForTests.Queued);
            Assert.Equal(discarded.Count - 1, sink.DiscardsForTests.Dropped);
            Assert.Equal(discarded.Count - 1, handOff.Dropped);
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task DrainThatServicesNothingDeclinesWithItsReason()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(2, "stage-2", a);

            // Nothing eligible yet is no work
            var context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);

            // A listing that throws keeps the hint and declines the turn, so the worker backs off
            await Commit(rig.Storage, 2, Own(2));
            rig.Storage.Before = (verb, _) => verb == "Ls" ? throw new IOException("injected listing failure") : Task.CompletedTask;
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            // The decline is the turn's own, reported once
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, Context())).Kind);
            var listingFailed = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (listingFailed.Kind, listingFailed.Reason));
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);

            // A listing that fails its checks
            rig.Storage.Before = (verb, _) => verb == "Stat" ? throw new IOException("injected probe failure") : Task.CompletedTask;
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var probeFailed = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (probeFailed.Kind, probeFailed.Reason));

            // A slice gate that concludes nothing, at the fifth listing
            rig.Storage.Before = (verb, path) => verb == "Stat" && rig.Listings == 5 ? throw new IOException("injected gate probe failure") : Task.CompletedTask;
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var gateFailed = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (gateFailed.Kind, gateFailed.Reason));

            // A capability replay that cannot read a commit
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 0) ? throw new IOException("injected read failure") : Task.CompletedTask;
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var replayFailed = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (replayFailed.Kind, replayFailed.Reason));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.True(rig.Exists(a));
            Assert.Equal(0, rig.Counters.Dropped);

            // A turn that drains completes as before
            rig.Storage.Before = null;
            context = Context();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.False(rig.Exists(a));
        }

        [Fact]
        public async Task FencedDrainDropsItsHintsAndReleasesTheAllowance()
        {
            var a = "_change_data/a.parquet";
            var group = new DiscardGroup(TableId, 0, 1, "stage-1", new[] { a });
            // Room for two groups of one path, one of them another table's
            var rig = NewRig(2 * (DiscardHandOff.GroupBytes(group) + DiscardHandOff.PathBytes(a)));
            await Create(rig.Storage, 1);
            rig.Put(a);
            Assert.Equal(0, rig.Entry.Enqueue(group));
            using var filled = rig.HandOff.Acquire(new DiscardRoute(rig.Local, "filled"));
            Assert.Equal(0, filled.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", new[] { a })));
            using var other = rig.HandOff.Acquire(new DiscardRoute(rig.Local, "other"));
            Assert.Equal(1, other.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", new[] { a })));

            // An uninterpreted control record drops the hints without a delete and declines the turn
            await rig.Local.WriteText("/t/_delta_log/.flowtide-cleanup.future", "{}");
            var context = Context();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var fenced = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (fenced.Kind, fenced.Reason));
            Assert.True(rig.Exists(a));
            Assert.Equal(0, rig.Entry.Hints);
            Assert.Equal(1, rig.Counters.Dropped);
            Assert.Equal(0, rig.Counters.Deleted);
            Assert.DoesNotContain(rig.Storage.Requests, x => x.StartsWith("OpenRead ", StringComparison.Ordinal));

            // The released allowance takes another table's hint
            Assert.Equal(0, rig.Entry.Charged);
            Assert.Equal(filled.Entry.Charged, rig.HandOff.Charged);
            Assert.Equal(0, other.Entry.Enqueue(new DiscardGroup(TableId, 0, 1, "stage-1", new[] { a })));
        }

        [Fact]
        public async Task DeclinedDrainTurnsBackOff()
        {
            await using var run = SinkRun.Start(nameof(DeclinedDrainTurnsBackOff), _directory);
            await run.Started();
            var entry = run.Sink.DiscardRouteForTests!.Entry;
            var discarded = run.ChangeData();
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            // Only maintenance probes a successor; every probe fails
            run.Storage.Before = (verb, path) => verb == "Stat" ? throw new IOException("injected probe failure") : Task.CompletedTask;

            // Each declined turn keeps the hints and doubles the wait, from one minute
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(1, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(1), worker.CleanupRetryAt(JobKind.Deletion));
            run.Clock.Advance(TimeSpan.FromMinutes(1));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(2, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(2), worker.CleanupRetryAt(JobKind.Deletion));
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (worker.LastOutcomeForTests!.Kind, worker.LastOutcomeForTests.Reason));
            Assert.Equal(discarded.Count, entry.Hints);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));

            // A turn that drains resets the backoff
            run.Storage.Before = null;
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(0, worker.CleanupFailures(JobKind.Deletion));
            Assert.All(discarded, x => Assert.False(run.Exists(x)));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task HintsDroppedAtTheLastReleaseReachTheSinkCounter()
        {
            await using var run = SinkRun.Start(nameof(HintsDroppedAtTheLastReleaseReachTheSinkCounter), _directory);
            await run.Started();
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var discarded = run.ChangeData();
            Assert.NotEmpty(discarded);
            Assert.Equal(discarded.Count, entry.Hints);
            var handOff = run.Stream.Maintenance.Budget.Discards;
            Assert.Equal(0, handOff.Dropped);

            // The stop releases the route's only handle: every hint is dropped, counted once by each counter
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            await run.Stream.Crash();
            await SinkRun.WaitForNewWorker(run.Stream, worker);
            Assert.Equal(0, entry.Hints);
            Assert.NotSame(entry, sink.DiscardRouteForTests!.Entry);
            Assert.Equal(discarded.Count, handOff.Dropped);
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Equal(1, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task HintsOfAPublishThatFailedClosedAreDroppedWhileTheSinkWaits()
        {
            var failedClosed = 0;
            await using var run = SinkRun.Start(nameof(HintsOfAPublishThatFailedClosedAreDroppedWhileTheSinkWaits), _directory, options => options.CommitVersionHookForTests = (_, outcome, _) =>
            {
                if (outcome == PublishOutcome.FailedClosed)
                {
                    Interlocked.Increment(ref failedClosed);
                }
            });
            await run.Started();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            Assert.Equal(0, entry.Hints);
            var deleted = sink.DiscardsForTests.Deleted;
            var before = run.ChangeData();

            // Another writer takes version 1 right before the sink publishes it, so the sink waits and retries at every checkpoint
            var foreignWritten = 0;
            run.Storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 1)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                {
                    await WriteCommit(run.Local, "test", 1, Foreign(1));
                }
            };
            run.Stream.Generate(20);
            await WaitForFailedClosed(1);
            var discarded = run.ChangeData().Except(before).ToList();
            Assert.NotEmpty(discarded);
            Assert.Equal(discarded.Count, entry.Hints);
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            Assert.Equal(0, worker.Target);

            // The waiting sink's next checkpoint marks the commit
            run.Stream.Generate(20);
            await WaitForFailedClosed(2);
            Assert.Equal(1, entry.FailedClosed?.Version);

            // The turn drops the attempt's hints by the mark, without a delete and without a gate
            var verdicts = new ConcurrentQueue<GateVerdict>();
            ((DeltaTableCleanup)worker.Cleanup!).DroppedHookForTests = (_, verdict) => verdicts.Enqueue(verdict);
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(0, entry.Hints);
            Assert.Empty(verdicts);
            Assert.Null(entry.FailedClosed);
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);
            Assert.Equal(deleted, sink.DiscardsForTests.Deleted);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));

            // Once the conflict is gone the sink publishes, and a later attempt's hints are deleted again
            File.Delete(Path.Combine(run.TableDir, "_delta_log", $"{1:D20}.json"));
            run.Stream.Generate(20);
            await WaitForVersionCheckpointing(run.Local, "test", run.Stream, 2);
            await run.Stream.Maintenance.TickAndSettleAsync(2);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);
            Assert.True(sink.DiscardsForTests.Deleted > deleted);
            Assert.Equal(0, run.Stream.FailureNotificationCount);

            async Task WaitForFailedClosed(int count)
            {
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
                while (Volatile.Read(ref failedClosed) < count)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"The publish of version 1 did not fail closed {count} times");
                    await run.Stream.SchedulerTick();
                    await Task.Delay(20);
                }
            }
        }

        [Fact]
        public async Task HintsFromAFailedClosedVersionDropWithoutAListing()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var published = rig.Put("_change_data/published.parquet");
            var waiting = rig.Put("_change_data/waiting.parquet");
            rig.Queue(1, "stage-1", published);
            // The log never shows version 2, which a gate would keep for another turn
            rig.Queue(2, "stage-2", waiting);
            rig.Entry.MarkFailedClosed(new FailedClosedMark(2, "stage-2"));
            rig.Storage.ClearRequests();

            // Only the failed commit's hints are dropped, before the turn lists; the earlier ones still drain
            Assert.Equal(2, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(published));
            Assert.True(rig.Exists(waiting));
            Assert.Equal((1L, 1L), (rig.Counters.Deleted, rig.Counters.Dropped));
            Assert.Equal((0, 0L), (rig.Entry.Hints, rig.HandOff.Charged));
            Assert.Null(rig.Entry.FailedClosed);

            // Alone, they drop without a listing
            var alone = rig.Put("_change_data/alone.parquet");
            rig.Queue(2, "stage-2", alone);
            rig.Entry.MarkFailedClosed(new FailedClosedMark(2, "stage-2"));
            rig.Storage.ClearRequests();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.True(rig.Exists(alone));
            Assert.Equal(0, rig.Listings);
            Assert.Equal(2L, rig.Counters.Dropped);
        }

        [Fact]
        public async Task AFailedClosedPublishDropsOnlyItsOwnHintsOnce()
        {
            var failedClosed = 0;
            await using var run = SinkRun.Start(nameof(AFailedClosedPublishDropsOnlyItsOwnHintsOnce), _directory, options => options.CommitVersionHookForTests = (_, outcome, _) =>
            {
                if (outcome == PublishOutcome.FailedClosed)
                {
                    Interlocked.Increment(ref failedClosed);
                }
            });
            await run.Started();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var handOff = run.Stream.Maintenance.Budget.Discards;
            var before = run.ChangeData();

            // Another writer of the route queued a later version's hints
            Assert.True(handOff.TryAcquire(entry, out var other));
            using (other)
            {
                Assert.Equal(0, other!.Entry.Enqueue(new DiscardGroup("table-b", 0, 2, "stage-b", new[] { "_change_data/b.parquet" }) { QueuedAt = run.Clock.GetUtcNow() }));

                // Another writer takes version 1 right before the sink publishes it
                var foreignWritten = 0;
                run.Storage.Before = async (verb, path) =>
                {
                    if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 1)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                    {
                        await WriteCommit(run.Local, "test", 1, Foreign(1));
                    }
                };
                run.Stream.Generate(20);
                await WaitForFailedClosed(1);
                var discarded = run.ChangeData().Except(before).ToList();
                Assert.NotEmpty(discarded);
                Assert.Equal(discarded.Count + 1, entry.Hints);
                // Marked at the waiting sink's next checkpoint
                run.Stream.Generate(20);
                await WaitForFailedClosed(2);
                Assert.NotNull(entry.FailedClosed);

                // One turn drops the failed commit's hints by the mark and none of the other writer's
                var verdicts = new ConcurrentQueue<GateVerdict>();
                ((DeltaTableCleanup)Assert.Single(run.Stream.Maintenance.Workers).Cleanup!).DroppedHookForTests = (_, verdict) => verdicts.Enqueue(verdict);
                await run.Stream.Maintenance.TickAndSettleAsync(0);
                Assert.Equal(1, entry.Hints);
                Assert.Empty(verdicts);
                Assert.Null(entry.FailedClosed);
                Assert.Equal(discarded.Count, handOff.Dropped);
                Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);

                // The sink's retries of the same commit start no further turn, also once any floor passed
                for (int retries = 3; retries <= 4; retries++)
                {
                    run.Stream.Generate(20);
                    await WaitForFailedClosed(retries);
                    run.Clock.Advance(TimeSpan.FromMinutes(2));
                    Assert.False(await run.Stream.Maintenance.TickOnceAsync());
                }
                Assert.Null(entry.FailedClosed);
                Assert.Equal(1, entry.Hints);
                Assert.Equal(discarded.Count, handOff.Dropped);
                Assert.Equal(0, run.Stream.FailureNotificationCount);
            }

            async Task WaitForFailedClosed(int count)
            {
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
                while (Volatile.Read(ref failedClosed) < count)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"The publish of version 1 did not fail closed {count} times");
                    await run.Stream.SchedulerTick();
                    await Task.Delay(20);
                }
            }
        }

        [Fact]
        public async Task AStaleFailedClosedMarkDropsNoNewStageOfItsVersion()
        {
            var rig = NewRig();
            await Create(rig.Storage, 0);
            // An earlier run's stage of version 1 failed closed; this run staged version 1 again and has not published it
            rig.Entry.MarkFailedClosed(new FailedClosedMark(1, "stage-old"));
            var fresh = rig.Put("_change_data/fresh.parquet");
            rig.Queue(1, "stage-1", fresh);

            // The turn keeps the new stage's hints for its publish and clears the mark
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal((1, 0L), (rig.Entry.Hints, rig.Counters.Dropped));
            Assert.Null(rig.Entry.FailedClosed);

            // Once published, they are deleted
            await Commit(rig.Storage, 1, Own(1));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(fresh));
            Assert.Equal((1L, 0L), (rig.Counters.Deleted, rig.Counters.Dropped));
        }

        [Fact]
        public async Task AFailedClosedMarkReplacedDuringATurnWaitsForTheNext()
        {
            var rig = NewRig();
            await Create(rig.Storage, 0);
            rig.Queue(1, "stage-1", rig.Put("_change_data/one.parquet"));
            rig.Queue(2, "stage-2", rig.Put("_change_data/two.parquet"));
            rig.Entry.MarkFailedClosed(new FailedClosedMark(1, "stage-1"));
            // Another stage fails closed after the turn read the mark
            var second = new FailedClosedMark(2, "stage-2");
            rig.Cleanup.DrainHookForTests = _ =>
            {
                rig.Entry.MarkFailedClosed(second);
                return Task.CompletedTask;
            };

            // The turn drops what it read the mark for and leaves the newer mark
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Same(second, rig.Entry.FailedClosed);
            Assert.Equal((1, 1L), (rig.Entry.Hints, rig.Counters.Dropped));

            // The next turn drops that stage's hints
            rig.Cleanup.DrainHookForTests = null;
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Null(rig.Entry.FailedClosed);
            Assert.Equal((0, 2L), (rig.Entry.Hints, rig.Counters.Dropped));
        }

        [Fact]
        public async Task FailedClosedMarkIsSetByTheNextCheckpointAndLeftByAPublishingRetry()
        {
            var failedClosed = 0;
            DiscardEntry? route = null;
            // The route's mark as each failed CommitVersion leaves it
            var marks = new ConcurrentQueue<FailedClosedMark?>();
            await using var run = SinkRun.Start(nameof(FailedClosedMarkIsSetByTheNextCheckpointAndLeftByAPublishingRetry), _directory, options => options.CommitVersionHookForTests = (_, outcome, _) =>
            {
                if (outcome == PublishOutcome.FailedClosed)
                {
                    marks.Enqueue(Volatile.Read(ref route)?.FailedClosed);
                    Interlocked.Increment(ref failedClosed);
                }
            });
            await run.Started();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            Volatile.Write(ref route, entry);

            // Another writer's stage on the route failed closed; the sink's own publish leaves its mark
            var foreign = new FailedClosedMark(7, "stage-b");
            entry.MarkFailedClosed(foreign);
            run.Stream.Generate(20);
            await WaitForVersion(run.Local, "test", run.Stream, 1);
            await run.Stream.Maintenance.WaitForTargetAsync(1);
            Assert.Same(foreign, entry.FailedClosed);
            Assert.True(entry.ClearFailedClosed(foreign));
            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.Equal(0, entry.Hints);

            // Another writer takes version 2 right before the sink publishes it, and no turn runs before the conflict is gone
            var before = run.ChangeData();
            var foreignWritten = 0;
            run.Storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 2)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                {
                    await WriteCommit(run.Local, "test", 2, Foreign(2));
                }
            };
            run.Stream.Generate(20);
            await WaitForFailedClosed(1);
            var discarded = run.ChangeData().Except(before).ToList();
            Assert.NotEmpty(discarded);
            Assert.Equal(discarded.Count, entry.Hints);

            // The failed CommitVersion marks nothing; the waiting sink's next checkpoint marks the stage, once
            for (int retries = 2; retries <= 3; retries++)
            {
                run.Stream.Generate(20);
                await WaitForFailedClosed(retries);
            }
            var mark = entry.FailedClosed;
            Assert.NotNull(mark);
            Assert.Equal(2, mark.Version);
            var seen = marks.ToArray();
            Assert.Equal(3, seen.Length);
            Assert.Null(seen[0]);
            Assert.Same(mark, seen[1]);
            Assert.Same(mark, seen[2]);

            // The retry that publishes leaves the mark; the next drain drops the published commit's hints once and counts them
            File.Delete(Path.Combine(run.TableDir, "_delta_log", $"{2:D20}.json"));
            run.Stream.Generate(20);
            await WaitForVersionCheckpointing(run.Local, "test", run.Stream, 3);
            Assert.Same(mark, entry.FailedClosed);
            await run.Stream.Maintenance.TickAndSettleAsync(3);
            Assert.Null(entry.FailedClosed);
            Assert.Equal(0, entry.Hints);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);
            Assert.Equal(0, run.Stream.FailureNotificationCount);

            async Task WaitForFailedClosed(int count)
            {
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
                while (Volatile.Read(ref failedClosed) < count)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"The publish of version 2 did not fail closed {count} times");
                    await run.Stream.SchedulerTick();
                    await Task.Delay(20);
                }
            }
        }

        [Fact]
        public async Task HintOfATableWithoutIdNeverFailsIngestion()
        {
            await using var run = SinkRun.Start(nameof(HintOfATableWithoutIdNeverFailsIngestion), _directory);
            // An existing change data table whose metaData has no id
            var metaData = Metadata(UserSchema, new Dictionary<string, string>() { ["delta.enableChangeDataFeed"] = "true" });
            metaData.MetaData!.Id = null;
            await WriteCommit(run.Local, "test", 0, Foreign(0), ProtocolWith("changeDataFeed"), metaData);

            // An insert-only attempt discards its change data and hands it off after it stages
            run.Stream.WaitForUpdateDoesNotRequireDataChange();
            run.Stream.Generate(200);
            await run.Stream.StartStream(UserInsert);
            var published = Path.Combine(run.TableDir, "_delta_log", $"{1:D20}.json");
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!File.Exists(published) && run.Stream.FailureNotificationCount == 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "Version 1 was not written");
                await run.Stream.SchedulerTick();
                await Task.Delay(20);
            }
            Assert.Equal(0, run.Stream.FailureNotificationCount);
            var sink = run.Sink;
            Assert.Equal(DiscardMode.Queued, sink.DiscardModeForTests);
            var discarded = run.ChangeData();
            Assert.NotEmpty(discarded);
            var entry = sink.DiscardRouteForTests!.Entry;
            Assert.Null(Assert.Single(entry.QueuedForTests).TableId);

            // The drain drops the hint without a delete
            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Dropped);
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            await run.Stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task FailedDeleteHoldsBackNoOtherHint()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 3);
            var bad = rig.Put("_change_data/bad.parquet");
            var siblings = new[] { rig.Put("_change_data/a2.parquet"), rig.Put("_change_data/a3.parquet") };
            var b = rig.Put("_change_data/b.parquet");
            var c = rig.Put("_change_data/c.parquet");
            rig.Queue(1, "stage-1", bad, siblings[0], siblings[1]);
            var second = new DiscardGroup(TableId, 0, 2, "stage-2", new[] { b });
            Assert.Equal(0, rig.Entry.Enqueue(second));
            rig.Queue(3, "stage-3", c);
            var charged = rig.HandOff.Charged;
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 2 };
            // Every delete of bad fails, as for a principal without delete rights
            var attempts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
            rig.Cleanup.BeforeDeleteForTests = target =>
            {
                var name = "_change_data/" + Path.GetFileName(target.Key);
                attempts.AddOrUpdate(name, 1, (_, count) => count + 1);
                return name == bad ? throw new IOException("injected delete failure") : Task.CompletedTask;
            };

            // The failing first slice holds back neither its sibling paths nor the turn's next group
            var context = Context();
            Assert.Equal(3, (await rig.Cleanup.DrainHintsAsync(context, 4)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.True(rig.Exists(bad));
            Assert.All(siblings.Append(b), x => Assert.False(rig.Exists(x)));
            Assert.True(rig.Exists(c));
            Assert.Equal(2, rig.Entry.Hints);
            Assert.Equal(charged - siblings.Append(b).Sum(DiscardHandOff.PathBytes) - DiscardHandOff.GroupBytes(second), rig.HandOff.Charged);

            // The failed path waits behind the groups queued before it
            Assert.Equal(new[] { c, bad }, rig.Entry.QueuedForTests.SelectMany(x => x.Paths));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 1)).Drained);
            Assert.False(rig.Exists(c));
            Assert.Equal(1, attempts[bad]);

            // A turn whose only result is the failure declines, so the worker backs off
            clock.Advance(TimeSpan.FromMinutes(1));
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var failed = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeleteFailed), (failed.Kind, failed.Reason));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.Equal(0, rig.Counters.Leaked);

            // Its third failed turn leaves it behind, counted once and no longer charged
            clock.Advance(TimeSpan.FromMinutes(2));
            context = Context();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.True(rig.Exists(bad));
            Assert.Equal(0, rig.Entry.Hints);
            Assert.Equal(0, rig.HandOff.Charged);
            Assert.False(rig.Cleanup.HasWork(JobKind.Deletion));
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);

            // Each succeeded delete was issued once, the failing one in exactly three turns
            Assert.Equal(5, attempts.Count);
            Assert.Equal(3, attempts[bad]);
            Assert.All(siblings.Append(b).Append(c), x => Assert.Equal(1, attempts[x]));
            Assert.Equal((4L, 3L, 1L, 0L), (rig.Counters.Deleted, rig.Counters.Failed, rig.Counters.Leaked, rig.Counters.Dropped));
        }

        [Fact]
        public void ConcludeSettlesOutOfOrderAndMovesFailuresLast()
        {
            var rig = NewRig();
            var first = new DiscardGroup(TableId, 0, 1, "stage-1", new[] { "p0", "p1", "p2", "p3" });
            var second = new DiscardGroup(TableId, 0, 2, "stage-2", new[] { "q0" });
            Assert.Equal(0, rig.Entry.Enqueue(first));
            Assert.Equal(0, rig.Entry.Enqueue(second));
            var charged = rig.HandOff.Charged;
            Assert.Equal(new[] { first, second }, rig.Entry.Pending());
            IEnumerable<string> Queued() => first.Paths;
            IEnumerable<int> FailedTurns() => Enumerable.Range(0, first.Remaining).Select(first.FailedTurns);
            IEnumerable<DateTimeOffset> RetryAt() => Enumerable.Range(0, first.Remaining).Select(first.RetryAt);
            var never = DateTimeOffset.MinValue;

            // Of the two taken, the failed one moves behind the untaken ones, retried a minute on, and its group behind the other group
            Assert.Equal(1, rig.Entry.Conclude(first, new[] { HintState.Failed, HintState.Settled }, s_now));
            Assert.Equal(new[] { "p2", "p3", "p0" }, Queued());
            Assert.Equal(new[] { 0, 0, 1 }, FailedTurns());
            Assert.Equal(new[] { never, never, s_now + TimeSpan.FromMinutes(1) }, RetryAt());
            Assert.Equal(new[] { second, first }, rig.Entry.Pending());
            Assert.Equal(charged - DiscardHandOff.PathBytes("p1"), rig.HandOff.Charged);
            Assert.Equal(4, rig.Entry.Hints);
            Assert.Equal(2, first.Due(s_now, 128));
            Assert.Equal(3, first.Due(s_now + TimeSpan.FromMinutes(1), 128));

            // A settled hint goes, a failed one counts its turn, an open one keeps its place and count
            Assert.Equal(1, rig.Entry.Conclude(first, new[] { HintState.Settled, HintState.Failed, HintState.Open }, s_now));
            Assert.Equal(new[] { "p0", "p3" }, Queued());
            Assert.Equal(new[] { 1, 1 }, FailedTurns());
            Assert.Equal(new[] { s_now + TimeSpan.FromMinutes(1), s_now + TimeSpan.FromMinutes(1) }, RetryAt());
            Assert.Equal(charged - DiscardHandOff.PathBytes("p1") - DiscardHandOff.PathBytes("p2"), rig.HandOff.Charged);
            Assert.Equal(3, rig.Entry.Hints);

            // Failing again moves it behind the open one with its turns adding up and its wait doubled
            var later = s_now + TimeSpan.FromMinutes(1);
            Assert.Equal(0, rig.Entry.Conclude(first, new[] { HintState.Failed, HintState.Open }, later));
            Assert.Equal(new[] { "p3", "p0" }, Queued());
            Assert.Equal(new[] { 1, 2 }, FailedTurns());
            Assert.Equal(new[] { s_now + TimeSpan.FromMinutes(1), later + TimeSpan.FromMinutes(2) }, RetryAt());
            Assert.Equal(3, rig.Entry.Hints);

            // Settling the rest releases the group's own charge too
            Assert.Equal(2, rig.Entry.Conclude(first, new[] { HintState.Settled, HintState.Settled }, later));
            rig.Entry.Prune();
            Assert.Equal(new[] { second }, rig.Entry.Pending());
            Assert.Equal(DiscardHandOff.GroupBytes(second) + DiscardHandOff.PathBytes("q0"), rig.HandOff.Charged);
            Assert.Equal(1, rig.Entry.Hints);
        }

        [Fact]
        public void FailedHintsWaitInTheOrderTheyFallDue()
        {
            var rig = NewRig();
            var group = new DiscardGroup(TableId, 0, 1, "stage-1", new[] { "y0", "y1", "y2" });
            Assert.Equal(0, rig.Entry.Enqueue(group));
            Assert.Equal(new[] { group }, rig.Entry.Pending());
            IEnumerable<string> Queued() => group.Paths;
            IEnumerable<DateTimeOffset> RetryAt() => Enumerable.Range(0, group.Remaining).Select(group.RetryAt);

            // y0 fails in two turns, then waits two minutes
            Assert.Equal(0, rig.Entry.Conclude(group, new[] { HintState.Failed }, s_now));
            var second = s_now + TimeSpan.FromMinutes(1);
            Assert.Equal(0, rig.Entry.Conclude(group, new[] { HintState.Open, HintState.Open, HintState.Failed }, second));
            Assert.Equal(new[] { "y1", "y2", "y0" }, Queued());

            // y1's first failure falls due before y0, so it waits ahead of it
            Assert.Equal(0, rig.Entry.Conclude(group, new[] { HintState.Failed }, second));
            Assert.Equal(new[] { "y2", "y1", "y0" }, Queued());
            Assert.Equal(new[] { DateTimeOffset.MinValue, second + TimeSpan.FromMinutes(1), second + TimeSpan.FromMinutes(2) }, RetryAt());

            // A turn takes only the due ones, which lead
            Assert.Equal(1, group.Due(second, 128));
            Assert.Equal(2, group.Due(second + TimeSpan.FromMinutes(1), 128));
            Assert.Equal(1, group.Due(second + TimeSpan.FromMinutes(1), 1));
            Assert.Equal(3, group.Due(second + TimeSpan.FromMinutes(2), 128));

            // One minute doubling, capped at thirty
            Assert.Equal(new[] { 1, 2, 4, 8, 16, 30, 30 }.Select(x => TimeSpan.FromMinutes(x)), Enumerable.Range(1, 7).Select(DiscardHandOff.RetryDelay));
        }

        [Fact]
        public async Task FailedHintWaitsForItsRetryWhileOtherGroupsDrain()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 3);
            var bad = rig.Put("_change_data/bad.parquet");
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            var c = rig.Put("_change_data/c.parquet");
            rig.Queue(1, "stage-1", bad, a);
            // Every delete of bad fails, each attempt recorded at its time
            var tried = new ConcurrentQueue<DateTimeOffset>();
            rig.Cleanup.BeforeDeleteForTests = target =>
            {
                if ("_change_data/" + Path.GetFileName(target.Key) != bad)
                {
                    return Task.CompletedTask;
                }
                tried.Enqueue(clock.GetUtcNow());
                throw new IOException("injected delete failure");
            };
            using var meter = new Meter("discard-leak-" + ScratchOwners.ShortId());
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true) { Discards = rig.HandOff };
            DeltaMaintenanceBudget.SharedBudget.RegisterGauges(meter, budget);

            // A turn that also deletes another path completes, and the failure waits a minute
            var context = Context();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.False(rig.Exists(a));
            var waiting = Assert.Single(rig.Entry.QueuedForTests);
            Assert.Equal(s_now + TimeSpan.FromMinutes(1), waiting.RetryAt(0));

            // Another group queued now drains at once while the failure waits
            rig.Queue(2, "stage-2", b);
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(b));
            Assert.Single(tried);

            // A turn left with only the waiting failure lists nothing and reports no work, so the worker's floor applies
            var listings = rig.Listings;
            context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(OutcomeKind.NoWork, (await rig.Cleanup.RunAsync(JobKind.Deletion, context)).Kind);
            Assert.Equal(listings, rig.Listings);
            Assert.True(rig.Cleanup.HasWork(JobKind.Deletion));
            Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, 3, clock.GetUtcNow()));
            Assert.True(rig.Cleanup.IsDue(JobKind.Deletion, 3, clock.GetUtcNow() + TimeSpan.FromMinutes(1)));
            Assert.False(rig.Cleanup.IsDue(JobKind.Advance, 3, clock.GetUtcNow() + TimeSpan.FromMinutes(1)));

            // Retried a minute after its first failure, beside a group queued in the meantime
            clock.Advance(TimeSpan.FromSeconds(59));
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Single(tried);
            clock.Advance(TimeSpan.FromSeconds(1));
            rig.Queue(3, "stage-3", c);
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(c));
            Assert.Equal(2, tried.Count);

            // Then two minutes after its second, when its third failure leaves it behind
            clock.Advance(TimeSpan.FromSeconds(119));
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(2, tried.Count);
            Assert.Equal(0, rig.Counters.Leaked);
            clock.Advance(TimeSpan.FromSeconds(1));
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.Equal(new[] { s_now, s_now + TimeSpan.FromMinutes(1), s_now + TimeSpan.FromMinutes(3) }, tried);
            Assert.True(rig.Exists(bad));
            Assert.Equal(0, rig.Entry.Hints);
            Assert.Equal(0, rig.HandOff.Charged);
            Assert.Equal(1, rig.Counters.Leaked);
            Assert.Equal(1, rig.HandOff.Leaked);
            Assert.Equal(1, Gauge(meter, "flowtide_delta_maintenance_discard_leaked"));
        }

        [Fact]
        public async Task EachLeakKindWarnsOncePerRun()
        {
            var clock = new FakeTimeProvider(s_now);
            var rig = NewRig(clock: clock);
            await Create(rig.Storage, 3);
            var logs = new TestLogCollector();
            var metered = MeteredFileStorage.Wrap(rig.Storage, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
            var cleanup = new DeltaTableCleanup(rig.Configuration, metered, clock, "t", rig.HandOff, () => rig.Current, rig.Counters, logs.CreateLogger("t"), "t");
            cleanup.BeforeDeleteForTests = _ => throw new IOException("injected delete failure");

            // Two hints whose deletes fail in three turns a retry apart are left behind, with one warning
            rig.Queue(1, "stage-1", rig.Put("_change_data/a.parquet"));
            rig.Queue(2, "stage-2", rig.Put("_change_data/b.parquet"));
            foreach (var minutes in new[] { 0, 1, 2 })
            {
                clock.Advance(TimeSpan.FromMinutes(minutes));
                await cleanup.DrainHintsAsync(Context(), 128);
            }
            Assert.Equal(2, rig.Counters.Leaked);
            Assert.Single(logs.Warnings, x => x.Contains("discarded outputs"));

            // A dead stage at 1 that proves no ownership leaves its output behind, which warns although a discard leak did
            var id = Guid.NewGuid().ToString("N");
            var stage = $".{1:D20}.json.{id}.tmp";
            File.WriteAllText(rig.Native("_delta_log/" + stage), $"{{\"commitInfo\":{{\"flowtide.stageId\":\"{id}\",\"timestamp\":1,\"operation\":\"WRITE\"}}}}\n");
            await cleanup.RunAsync(JobKind.StagedSweep, Context(kind: JobKind.StagedSweep, head: 3));
            Assert.Equal(1, cleanup.LastSweepForTests!.LeakedStages);
            Assert.Single(logs.Warnings, x => x.Contains("left output behind"));
            Assert.Single(logs.Warnings, x => x.Contains("discarded outputs"));
        }

        // One observation of a gauge registered on the meter
        private static long Gauge(Meter meter, string name)
        {
            long? value = null;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, owner) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && instrument.Name == name)
                {
                    owner.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => value = measurement);
            listener.Start();
            listener.RecordObservableInstruments();
            Assert.True(value.HasValue, $"No {name} gauge");
            return value.Value;
        }

        // One observation of a gauge of the stream's sink
        internal static long SinkGauge(DeltaLakeSinkStream stream, string name)
        {
            long? value = null;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, owner) =>
            {
                if (instrument.Meter.Name.StartsWith($"flowtide.{stream.StreamName}.operator.", StringComparison.Ordinal) && instrument.Name == "flowtide_" + name)
                {
                    owner.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => value = measurement);
            listener.Start();
            listener.RecordObservableInstruments();
            Assert.True(value.HasValue, $"No {name} gauge");
            return value.Value;
        }

        [Fact]
        public async Task StorageChangeAfterADeleteSettlesItBeforeTheRestIsDropped()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            rig.Queue(1, "stage-1", a, b);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 1 };
            var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(replacement);
            // The sink's storage changes while the second slice's gate lists
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && rig.Listings == 3)
                {
                    rig.Current = replacement;
                }
                return Task.CompletedTask;
            };
            Assert.Equal(2, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(a));
            Assert.True(rig.Exists(b));
            Assert.Equal((1L, 1L), (rig.Counters.Deleted, rig.Counters.Dropped));
            Assert.Equal(0, rig.Entry.Hints);
            Assert.Equal(0, rig.HandOff.Charged);
        }

        [Fact]
        public async Task RejectedPathsSettleOnceWhereverTheyLie()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            // A payload inside a log is never a deletion target
            var misplaced = rig.Put("_delta_log/x.parquet");
            rig.Queue(1, "stage-1", a, b, misplaced);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 1 };

            // The second slice's gate fails, so b stays open while the rejected path behind it settles
            rig.Storage.Before = (verb, path) => verb == "Stat" && path.Full.StartsWith("/t/_delta_log/", StringComparison.Ordinal) && rig.Listings == 3
                ? throw new IOException("injected probe failure") : Task.CompletedTask;
            Assert.Equal(2, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(a));
            Assert.Equal(new[] { b }, rig.Entry.QueuedForTests.SelectMany(x => x.Paths));
            Assert.Equal(1, rig.Counters.Kept);

            rig.Storage.Before = null;
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(b));
            Assert.True(rig.Exists(misplaced));
            Assert.Equal(1, rig.Counters.Kept);
            Assert.Equal(0, rig.HandOff.Charged);
        }

        [Fact]
        public async Task GateWhoseReadsRunOutDeclinesTheTurn()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", a);

            // The replay reads commits 0 and 1, and the slice's gate has one read left for rehashing both
            var context = Context(readLimit: 3);
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            Assert.Equal(2, rig.Listings);
            Assert.Equal(3, context.ReadsUsed);
            var spent = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (spent.Kind, spent.Reason));
            Assert.Contains("spent", spent.Detail);
            Assert.True(rig.Exists(a));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.Equal(0, rig.Counters.Dropped);

            // A fresh admission drains it
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(Context(), 128)).Drained);
            Assert.False(rig.Exists(a));
        }

        [Fact]
        public async Task FenceKeepsHintsAtAGateListingAndDropsThemAtATurnListing()
        {
            var rig = NewRig();
            await Create(rig.Storage, 1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, "stage-1", a);
            var charged = rig.HandOff.Charged;

            // A record this version cannot interpret appears after the turn's listing and before the slice's gate lists: the hints wait for a retry
            rig.Storage.Before = async (verb, path) =>
            {
                if (verb == "Ls" && path.Full == "/t/_delta_log/" && rig.Listings == 2)
                {
                    await rig.Local.WriteText("/t/_delta_log/.flowtide-cleanup.future", "{}");
                }
            };
            var context = Context();
            Assert.Equal(0, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var kept = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (kept.Kind, kept.Reason));
            Assert.True(rig.Exists(a));
            Assert.Equal(1, rig.Entry.Hints);
            Assert.Equal(charged, rig.HandOff.Charged);
            Assert.Equal(0, rig.Counters.Dropped);

            // The next turn's own listing finds it, and such a record never clears: dropped and counted without a delete, the allowance released
            rig.Storage.Before = null;
            context = Context();
            Assert.Equal(1, (await rig.Cleanup.DrainHintsAsync(context, 128)).Drained);
            var fenced = await rig.Cleanup.RunAsync(JobKind.Deletion, context);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (fenced.Kind, fenced.Reason));
            Assert.True(rig.Exists(a));
            Assert.Equal(0, rig.Entry.Hints);
            Assert.Equal(0, rig.HandOff.Charged);
            Assert.Equal((1L, 0L), (rig.Counters.Dropped, rig.Counters.Deleted));
        }

        [Fact]
        public async Task AlwaysFailingDeletesLeakAfterThreeBackedOffTurns()
        {
            await using var run = SinkRun.Start(nameof(AlwaysFailingDeletesLeakAfterThreeBackedOffTurns), _directory);
            await run.Started();
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var discarded = run.ChangeData();
            Assert.NotEmpty(discarded);
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            var handOff = run.Stream.Maintenance.Budget.Discards;
            // A principal without delete rights: every delete fails
            var attempts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
            ((DeltaTableCleanup)worker.Cleanup!).BeforeDeleteForTests = target =>
            {
                attempts.AddOrUpdate("_change_data/" + Path.GetFileName(target.Key), 1, (_, count) => count + 1);
                throw new UnauthorizedAccessException("injected delete failure");
            };
            void Attempted(int turns) => Assert.All(discarded, x => Assert.Equal(turns, attempts.GetValueOrDefault(x)));

            // Each turn that only failed keeps the hints and backs off, one minute doubling
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Attempted(1);
            Assert.Equal(1, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(1), worker.CleanupRetryAt(JobKind.Deletion));
            run.Clock.Advance(TimeSpan.FromSeconds(59));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Attempted(1);
            run.Clock.Advance(TimeSpan.FromSeconds(1));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Attempted(2);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeleteFailed), (worker.LastOutcomeForTests!.Kind, worker.LastOutcomeForTests.Reason));
            Assert.Equal(2, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(2), worker.CleanupRetryAt(JobKind.Deletion));
            Assert.Equal(discarded.Count, entry.Hints);
            Assert.Equal(0, sink.DiscardsForTests.Leaked);

            // The third failed turn leaves them behind, each counted once, and releases their allowance
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Attempted(3);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(0, handOff.Charged);
            Assert.Equal(discarded.Count, sink.DiscardsForTests.Leaked);
            Assert.Equal(3L * discarded.Count, sink.DiscardsForTests.Failed);
            Assert.Equal(0, sink.DiscardsForTests.Dropped);
            Assert.Equal(0, worker.CleanupFailures(JobKind.Deletion));

            // Nothing is tried again
            run.Clock.Advance(TimeSpan.FromMinutes(30));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Attempted(3);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task PartlyFailedTurnRetriesItsFailureOneThenTwoMinutesLater()
        {
            await using var run = SinkRun.Start(nameof(PartlyFailedTurnRetriesItsFailureOneThenTwoMinutesLater), _directory);
            var logs = new TestLogCollector();
            run.Stream.AddLoggerProvider(logs);
            await run.Started();
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var discarded = run.ChangeData();
            Assert.True(discarded.Count >= 2, $"{discarded.Count} discards");
            var bad = discarded[0];
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            // Only bad's deletes fail, each attempt recorded at its time
            var tried = new ConcurrentQueue<DateTimeOffset>();
            ((DeltaTableCleanup)worker.Cleanup!).BeforeDeleteForTests = target =>
            {
                if ("_change_data/" + Path.GetFileName(target.Key) != bad)
                {
                    return Task.CompletedTask;
                }
                tried.Enqueue(run.Clock.GetUtcNow());
                throw new UnauthorizedAccessException("injected delete failure");
            };
            var start = run.Clock.GetUtcNow();

            // The turn deletes the others, and the failure waiting for its retry starts no turn, so no floor holds it back
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.All(discarded.Skip(1), x => Assert.False(run.Exists(x)));
            Assert.Single(tried);
            Assert.Equal(1, entry.Hints);
            Assert.Equal(0, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(DateTimeOffset.MinValue, worker.CleanupRetryAt(JobKind.Deletion));

            // A worker ticking every ten seconds tries it a minute after its first failure and two after its second
            for (int step = 0; step < 24; step++)
            {
                run.Clock.Advance(TimeSpan.FromSeconds(10));
                await run.Stream.Maintenance.TickAndSettleAsync(0);
            }
            Assert.Equal(new[] { start, start + TimeSpan.FromMinutes(1), start + TimeSpan.FromMinutes(3) }, tried);
            Assert.True(run.Exists(bad));
            Assert.Equal(0, entry.Hints);

            // The leak is counted, shown by the sink's gauge and logged as a warning although the failures warned before
            Assert.Equal(1, sink.DiscardsForTests.Leaked);
            Assert.Equal(1, SinkGauge(run.Stream, "delta_cleanup_discard_leaked"));
            Assert.Single(logs.Warnings, x => x.Contains("discarded outputs"));
            Assert.Contains(logs.Warnings, x => x.Contains("kept for a later turn"));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }
    }

    [Collection(nameof(PinnedClientCollection))]
    public class DiscardInlineKeyTests
    {
        [Fact]
        public void PinnedS3CleanupDeletesOneAtATime()
        {
            using var s3 = StoreFixtures.S3();
            using var azure = StoreFixtures.Azure();
            var objects = new ScriptedStorage();
            foreach (var (pinned, outstanding) in new[] { (s3, 1), (azure, DeletionRun.MaxOutstanding) })
            {
                var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, StoreDescriptor.Of(pinned), "c/t", false), CleanupOptIns.Off);
                var cleanup = new DeltaTableCleanup(configuration, MeteredFileStorage.Wrap(objects, new MaintenanceBuckets(TimeProvider.System), ListingPaging.S3), TimeProvider.System, "c/t",
                    new DiscardHandOff(), () => objects, new DiscardCounters(), null, "t");
                Assert.Equal(outstanding, cleanup.Settings.MaxOutstanding);
            }
        }

        [Fact]
        public async Task AzureAndGcsInlineRejectedKeysAreKeptAndCounted()
        {
            using var azure = StoreFixtures.Azure();
            using var gcs = StoreFixtures.Gcs();
            const string Own = "_change_data/cdc-00000-0f8fad5b-d9cb-469f-a165-70867728950e.parquet";

            // Azure drops a trailing dot, so neither a table path nor a name with one may be deleted
            var (dotted, dottedObjects, dottedCounters) = await Inline(azure, "c/t.", Own);
            Assert.Equal((0, 1), (dotted.Deleted, dotted.Kept));
            Assert.Empty(dottedObjects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));
            Assert.Equal(1, dottedCounters.Kept);

            var names = new[] { "_change_data/x./cdc-1.parquet", Own, "_change_data/cdc-2.parquet.", "_change_data/cdc#3.parquet" };
            var (azureResult, azureObjects, azureCounters) = await Inline(azure, "c/t", names);
            Assert.Equal((1, 3, 0), (azureResult.Deleted, azureResult.Kept, azureResult.Failed));
            Assert.Equal(KeepReason.Unqualified, azureResult.FirstKept);
            Assert.Equal(new[] { "Rm /c/t/" + Own }, azureObjects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));
            Assert.Equal((1L, 3L), (azureCounters.Deleted, azureCounters.Kept));

            // Pinned GCS deletes its own fresh names by exact key, but keeps every target under an unsafe table prefix
            var (gcsResult, gcsObjects, _) = await Inline(gcs, "b/t", Own, "_change_data/cdc 4.parquet");
            Assert.Equal((1, 1), (gcsResult.Deleted, gcsResult.Kept));
            Assert.Equal(new[] { "Rm /b/t/" + Own }, gcsObjects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));
            var (unsafeResult, unsafeObjects, unsafeCounters) = await Inline(gcs, "b/t+1", Own);
            Assert.Equal((0, 1), (unsafeResult.Deleted, unsafeResult.Kept));
            Assert.Equal(KeepReason.StoreUnsupported, unsafeResult.FirstKept);
            Assert.Empty(unsafeObjects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));
            Assert.Equal(1, unsafeCounters.Kept);

            // A failed delete is counted and the remaining paths are still tried
            var objects = new ScriptedStorage();
            var owned = new TestStorage(objects) { BeforeRm = (path, _) => path.Full.EndsWith("cdc-a.parquet", StringComparison.Ordinal) ? throw new IOException("injected") : Task.CompletedTask };
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, StoreDescriptor.Of(azure), "c/t", false), CleanupOptIns.Off);
            var counters = new DiscardCounters();
            var tried = await InlineDiscards.DeleteAsync(new DeletionExecutor(configuration, owned), new[] { "_change_data/cdc-a.parquet", "_change_data/x./cdc-b.parquet", "_change_data/cdc-c.parquet" }, counters);
            Assert.Equal((1, 1, 1), (tried.Deleted, tried.Kept, tried.Failed));
            Assert.IsType<IOException>(tried.FirstError);
            Assert.Equal(new[] { "Rm /c/t/_change_data/cdc-a.parquet", "Rm /c/t/_change_data/cdc-c.parquet" }, owned.Calls.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));
            Assert.Equal((1L, 1L, 1L), (counters.Deleted, counters.Kept, counters.Failed));
        }

        // The inline fallback over scripted objects classified as the given pinned client, with every path present
        private static async Task<(InlineDiscardResult Result, ScriptedStorage Objects, DiscardCounters Counters)> Inline(IFileStorage pinned, string table, params string[] paths)
        {
            var objects = new ScriptedStorage();
            foreach (var path in paths)
            {
                objects.Put("/" + table + "/" + path);
            }
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, StoreDescriptor.Of(pinned), table, false), CleanupOptIns.Off);
            var counters = new DiscardCounters();
            var result = await InlineDiscards.DeleteAsync(new DeletionExecutor(configuration, objects), paths, counters);
            return (result, objects, counters);
        }
    }

    /// <summary>
    /// A sink stream over an eligible local disk table at test, seen through an owned hook wrapper, with change data and an options clock a test advances.
    /// </summary>
    internal sealed class SinkRun : IAsyncDisposable
    {
        private DeltaLakeSink? _sink;
        private DeltaLakeOptions? _options;

        private SinkRun(string root, IFileStorage local, HookFileStorage storage, FakeTimeProvider clock, DeltaLakeSinkStream stream)
        {
            Root = root;
            Local = local;
            Storage = storage;
            Clock = clock;
            Stream = stream;
        }

        public string Root { get; }

        public IFileStorage Local { get; }

        public HookFileStorage Storage { get; }

        public FakeTimeProvider Clock { get; }

        public DeltaLakeSinkStream Stream { get; }

        public DeltaLakeSink Sink => _sink ?? throw new InvalidOperationException("No sink was constructed");

        public DeltaLakeOptions Options => _options ?? throw new InvalidOperationException("No options were built");

        // Restarts reuse the vertex, so this stays at one
        public int SinksConstructed { get; private set; }

        public string TableDir => Path.Combine(Root, "test");

        public static SinkRun Start(string testName, string directory, Action<DeltaLakeOptions>? configure = null)
        {
            var root = Path.Combine(directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            var storage = OwnedHookFileStorage.Over(local);
            var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
            SinkRun? run = null;
            var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Insert-only versions still write change files to discard
                options.DeleteLimitsOverride = new DeleteLimits() { InsertChangeData = true };
                options.MaxFileSizeBytes = 500;
                // Checkpoints off, so only the drain and the staged sweep run
                options.CheckpointInterval = 0;
                options.TimeProvider = clock;
                options.SinkCreatedForTests = created =>
                {
                    run!._sink = created;
                    run.SinksConstructed++;
                };
                run!._options = options;
                configure?.Invoke(options);
            });
            run = new SinkRun(root, local, storage, clock, stream);
            return run;
        }

        // Starts the stream and waits for its first version, which discards every change data file it wrote
        public async Task Started()
        {
            // The sink is the only egress, so a checkpoint counts as an update
            Stream.WaitForUpdateDoesNotRequireDataChange();
            Stream.Generate(200);
            await Stream.StartStream(UserInsert);
            await WaitForVersion(Local, "test", Stream, 0);
        }

        // Table-relative names of the change data files on disk
        public List<string> ChangeData()
        {
            var folder = Path.Combine(TableDir, "_change_data");
            return Directory.Exists(folder) ? Directory.GetFiles(folder).Select(x => "_change_data/" + Path.GetFileName(x)).OrderBy(x => x, StringComparer.Ordinal).ToList() : new List<string>();
        }

        public bool Exists(string relative) => File.Exists(Path.Combine(TableDir, relative.Replace('/', Path.DirectorySeparatorChar)));

        // Checkpoints until the sink handed off the version, which an idle sink does from its validated head
        public async Task WaitForTarget(long version)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!Stream.Maintenance.Workers.Any(x => x.Target >= version))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"No worker reached version {version}");
                }
                await Stream.SchedulerTick();
                await Stream.TriggerCheckpoint();
                await Task.Delay(20);
            }
        }

        // Waits until the restarted run registered a worker other than the given one and its sink set that run's discard mode and route
        public static async Task<DeltaMaintenanceWorker> WaitForNewWorker(DeltaLakeSinkStream stream, DeltaMaintenanceWorker old)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (true)
            {
                var worker = stream.Maintenance.Workers.FirstOrDefault(x => !ReferenceEquals(x, old));
                if (worker != null && stream.Sinks.Any(x => ReferenceEquals(x.StartedWorkerForTests, worker)))
                {
                    return worker;
                }
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The sink did not restart");
                }
                await stream.SchedulerTick();
                await Task.Delay(20);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Stream.DisposeAsync();
            Local.Dispose();
        }
    }
}
