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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.ExceptionServices;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AdvanceExecutionTests
    {
        // Each log delete's issue and settlement by file name, in order, and the most outstanding at once
        private sealed class LogDeletes
        {
            private int _outstanding;
            private int _peak;

            public LogDeletes(AdvanceHooks hooks)
            {
                hooks.BeforeDelete = async target =>
                {
                    var outstanding = Interlocked.Increment(ref _outstanding);
                    var seen = Volatile.Read(ref _peak);
                    while (outstanding > seen && Interlocked.CompareExchange(ref _peak, outstanding, seen) != seen)
                    {
                        seen = Volatile.Read(ref _peak);
                    }
                    var name = Path.GetFileName(target.Key);
                    Events.Enqueue($"issue {name}");
                    if (Hold is { } hold)
                    {
                        await hold(name);
                    }
                };
                hooks.Settled = (target, _) =>
                {
                    Interlocked.Decrement(ref _outstanding);
                    Events.Enqueue($"settle {Path.GetFileName(target.Key)}");
                };
            }

            public ConcurrentQueue<string> Events { get; } = new ConcurrentQueue<string>();

            // Awaited by each delete once its issue is recorded
            public Func<string, Task>? Hold { get; set; }

            public int Peak => Volatile.Read(ref _peak);

            public string[] Issued => Events.Where(x => x.StartsWith("issue ", StringComparison.Ordinal)).Select(x => x.Substring(6)).ToArray();

            // Each issue followed by its own settlement before the next issue
            public void AssertOneAtATime()
            {
                var events = Events.ToArray();
                Assert.True(events.Length > 0 && events.Length % 2 == 0, string.Join(", ", events));
                for (int i = 0; i < events.Length; i += 2)
                {
                    Assert.StartsWith("issue ", events[i]);
                    Assert.Equal("settle " + events[i].Substring(6), events[i + 1]);
                }
            }
        }

        private static string Json(long version) => $"{version:D20}.json";

        private static TaskCompletionSource Signal() => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The hold reached, failing instead when the turn ends first
        private static async Task ReachedAsync(TaskCompletionSource held, Task turn)
        {
            await Task.WhenAny(held.Task, turn);
            Assert.True(held.Task.IsCompleted, "The turn ended before it reached the held delete");
        }

        // The rig's advance taken over by a new cleanup that meters the table through these buckets over the given storage
        private static Rig Remetered(Rig rig, MaintenanceBuckets buckets, IFileStorage? below = null)
        {
            var table = rig.Table;
            var metered = new ExecutionTable()
            {
                Raw = table.Raw,
                Metered = MeteredFileStorage.Wrap(below ?? table.Raw, buckets, table.Configuration.Store.Store.Paging),
                Configuration = table.Configuration,
                Hooks = table.Hooks,
                Root = table.Root,
                Head = table.Head
            };
            var cleanup = AdvanceExecutionKit.Cleanup(metered, rig.Logs);
            cleanup.Adopt(rig.Active);
            return new Rig() { Table = metered, Cleanup = cleanup, Active = rig.Active, Ledger = rig.Ledger, Logs = rig.Logs };
        }

        // What the log phase would delete from the table now, in its order
        private static async Task<string[]> LogTargetNamesAsync(Rig rig)
        {
            var listed = await rig.Table.Raw.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}");
            return LogTargets.Order(LogClassifier.Classify(listed), rig.Active.C).Select(x => x.Entry.Name).ToArray();
        }

        // Removals the in-memory store was asked for, by file name in order
        private static string[] Removed(HookFileStorage hooks) => hooks.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)).Select(x => Path.GetFileName(x.Substring(3))).ToArray();

        private static async Task<string?> LogsCompleteAsync(ExecutionTable table) =>
            (await table.ControlsAsync()).SingleOrDefault(x => x.EndsWith(ControlName.LogsCompleteSuffix, StringComparison.Ordinal));

        // Delete turns until the log phase publishes logs-complete, each completed; returns how many ran
        private async Task<int> FinishLogPhaseAsync(Rig rig)
        {
            for (int turns = 1; turns <= 20; turns++)
            {
                var result = await TurnAsync(rig);
                Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
                if (rig.Active.Phase == AdvancePhase.Payload)
                {
                    return turns;
                }
            }
            Assert.Fail("The log phase never published logs-complete");
            return 0;
        }

        // Every log target gone and logs-complete published, while C's commit and checkpoint, the commits after it and the hint stay
        private static async Task AssertLogPhaseDoneAsync(Rig rig)
        {
            var c = rig.Active.C;
            Assert.Empty(await LogTargetNamesAsync(rig));
            var kept = Enumerable.Range(0, (int)(rig.Table.Head - c + 1)).Select(x => Json(c + x)).Append($"{c:D20}.checkpoint.parquet").Append("_last_checkpoint");
            foreach (var name in kept)
            {
                Assert.True(await rig.Table.Raw.Exists(rig.Table.LogPath(name)), name);
            }
            Assert.NotNull(await LogsCompleteAsync(rig.Table));
            Assert.Equal(AdvancePhase.Payload, rig.Active.Phase);
            Assert.Equal(0, rig.Active.LogTargetsLeft);
        }

        private static void StopIssuance(IssuanceStop stop, JobRun job, FakeTimeProvider clock, CancellationTokenSource worker)
        {
            switch (stop)
            {
                case IssuanceStop.BuildDue:
                    job.YieldRequested = true;
                    break;
                case IssuanceStop.Deadline:
                    clock.Advance(TimeSpan.FromMinutes(16));
                    break;
                default:
                    worker.Cancel();
                    break;
            }
        }

        [Fact]
        public void OwnerDeletesOnlyBelowItsOwnReservation()
        {
            static IOEntry Entry(string name) => new IOEntry($"/t/{DeltaTransactionReader.DeltaLogDirName}{name}");
            var digest = new byte[32];
            var started = Entry(ControlName.Group(ControlKind.Started, 12, "a", digest, "b"));
            var own = ControlFence.Of(new[] { started });

            Assert.True(own.AllowsOwnerDelete(11, 12));
            Assert.True(own.AllowsOwnerDelete(-1, 12));
            // C itself, and another boundary's owner
            Assert.False(own.AllowsOwnerDelete(12, 12));
            Assert.False(own.AllowsOwnerDelete(7, 8));
            // A higher reservation, an uninterpreted record, or no reservation at all
            Assert.False(ControlFence.Of(new[] { started, Entry(ControlName.Group(ControlKind.Started, 14, "c", digest, "d")) }).AllowsOwnerDelete(11, 12));
            Assert.False(ControlFence.Of(new[] { started, Entry($"{DeltaTransactionReader.CleanupControlPrefix}{12:D20}.unknown") }).AllowsOwnerDelete(11, 12));
            Assert.False(ControlFence.None.AllowsOwnerDelete(11, 12));
        }

        [Theory]
        [InlineData("no-hint", nameof(MaintenanceReason.CleanupGate))]
        [InlineData("hint-below-c", nameof(MaintenanceReason.CleanupGate))]
        [InlineData("multipart-hint", nameof(MaintenanceReason.CleanupGate))]
        [InlineData("unbacked-hint", nameof(MaintenanceReason.CleanupGate))]
        [InlineData("unknown-below-c", nameof(MaintenanceReason.UnclassifiedLogFile))]
        [InlineData("uuid-checkpoint", nameof(MaintenanceReason.UnclassifiedLogFile))]
        public async Task LogGateSuspendsOnAnUnusableHintOrAnUnclassifiedName(string change, string expected)
        {
            var rig = await StartedAsync(local: false);
            var hooks = rig.Table.Hooks!;
            var c = rig.Active.C;
            var hint = rig.Table.LogPath("_last_checkpoint");
            switch (change)
            {
                case "no-hint":
                    await rig.Table.Raw.Rm(hint);
                    break;
                case "hint-below-c":
                    await rig.Table.Raw.WriteText(hint, $"{{\"version\":{c - 1},\"size\":1}}");
                    break;
                case "multipart-hint":
                    await rig.Table.Raw.WriteText(hint, $"{{\"version\":{c},\"size\":1,\"parts\":2}}");
                    break;
                case "unbacked-hint":
                    // Above the newest checkpoint
                    await rig.Table.Raw.WriteText(hint, "{\"version\":13,\"size\":1}");
                    break;
                case "unknown-below-c":
                    await CleanupRecordKit.PlantAsync(rig.Table.Raw, "t", $"{c - 1:D20}.unknown", new byte[1]);
                    break;
                default:
                    await CleanupRecordKit.PlantAsync(rig.Table.Raw, "t", $"{c - 1:D20}.checkpoint.{Guid.NewGuid():D}.parquet", new byte[1]);
                    break;
            }
            var records = await rig.Table.SnapshotAsync();
            hooks.ClearRequests();

            AssertDeclined(await TurnAsync(rig), Enum.Parse<MaintenanceReason>(expected));

            Assert.Empty(Removed(hooks));
            Assert.Equal(AdvanceState.Suspended, rig.Active.State);
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.Equal(records, await rig.Table.SnapshotAsync());
            Assert.Equal(c, await AdvanceExecutionKit.FloorAsync(rig.Table));
        }

        [Fact]
        public async Task LogTurnWhoseKeptExclusionsDoNotFitDeclinesBeforeAnyDelete()
        {
            var rig = await StartedAsync(local: false);
            var targets = await LogTargetNamesAsync(rig);
            var records = await rig.Table.SnapshotAsync();
            var left = rig.Active.LogTargetsLeft;
            // A resumed set held through the log phase
            rig.Active.Exclusions = LongExclusions(10_000);
            var deletes = new LogDeletes(rig.Cleanup.AdvanceHooksForTests);

            // Room for the turn's own work, not for the kept set
            var result = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger, memory: new JobMemory(managedCap: 16 << 20)));

            AssertDeclined(result, MaintenanceReason.ManagedMemoryCap);
            Assert.Empty(deletes.Issued);
            Assert.Equal((AdvancePhase.Log, AdvanceState.Runnable, CapacityState.Runnable, left), (rig.Active.Phase, rig.Active.State, rig.Active.Lease!.State, rig.Active.LogTargetsLeft));
            Assert.Equal(targets, await LogTargetNamesAsync(rig));
            Assert.Equal(records, await rig.Table.SnapshotAsync());
            // The same allowance without the kept set
            rig.Active.Exclusions = ExclusionSet.Empty;
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger, memory: new JobMemory(managedCap: 16 << 20)))).Kind);
            Assert.NotEmpty(deletes.Issued);
        }

        [Fact]
        public async Task LogTargetsRunOneAtATime()
        {
            // Each delete queues for one request token
            var rig = Remetered(await StartedAsync(local: false), new MaintenanceBuckets(TimeProvider.System, requestsPerSecond: 100, requestBurst: 1));
            await AdvanceExecutionKit.PlantCrcAsync(rig.Table, rig.Active.C - 1);
            var expected = await LogTargetNamesAsync(rig);
            var deletes = new LogDeletes(rig.Cleanup.AdvanceHooksForTests);
            var held = Signal();
            var release = Signal();
            deletes.Hold = async _ =>
            {
                if (held.TrySetResult())
                {
                    await release.Task;
                }
            };
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger);

            var turn = AdvanceExecutionKit.TurnAsync(rig.Cleanup, context);
            await ReachedAsync(held, turn);
            // The first delete is the only one submitted while it waits
            Assert.Equal(1, context.Tracked);
            release.SetResult();
            Assert.Equal(OutcomeKind.Completed, (await turn).Kind);

            Assert.Equal(1, deletes.Peak);
            Assert.Equal(expected, deletes.Issued);
            deletes.AssertOneAtATime();
            await FinishLogPhaseAsync(rig);
            await AssertLogPhaseDoneAsync(rig);
        }

        [Fact]
        public async Task HigherLogDeleteMustSettleBeforeLowerIssuance()
        {
            var rig = await StartedAsync(local: false);
            var hooks = rig.Table.Hooks!;
            // A version below C with its own checksum and checkpoint
            const long k = 8;
            Assert.True(k < rig.Active.C - 1);
            await AdvanceExecutionKit.PlantCrcAsync(rig.Table, k);
            var dependents = new[] { $"{k:D20}.crc", $"{k:D20}.checkpoint.parquet", Json(k - 1) };
            var deletes = new LogDeletes(rig.Cleanup.AdvanceHooksForTests);
            var held = Signal();
            var release = Signal();
            deletes.Hold = async name =>
            {
                if (name == Json(k))
                {
                    held.SetResult();
                    await release.Task;
                }
            };
            hooks.ClearRequests();
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger);

            var turn = AdvanceExecutionKit.TurnAsync(rig.Cleanup, context);
            await ReachedAsync(held, turn);
            // The higher versions are gone, and nothing that depends on k's commit was asked for while its delete is outstanding
            Assert.Equal($"issue {Json(k)}", deletes.Events.Last());
            Assert.Contains(Json(k + 1), Removed(hooks));
            Assert.DoesNotContain(Removed(hooks), x => dependents.Contains(x));
            release.SetResult();
            Assert.Equal(OutcomeKind.Completed, (await turn).Kind);

            // Commit, checksum, checkpoint, then the version below
            var removed = Removed(hooks).ToList();
            var at = dependents.Prepend(Json(k)).Select(x => removed.IndexOf(x)).ToArray();
            Assert.True(at[0] >= 0 && at.Zip(at.Skip(1)).All(x => x.First < x.Second), string.Join(", ", removed));
            var versions = removed.Where(x => x.EndsWith(".json", StringComparison.Ordinal)).Select(x => long.Parse(x.Substring(0, 20), CultureInfo.InvariantCulture)).ToArray();
            Assert.Equal(versions.OrderDescending(), versions);
            deletes.AssertOneAtATime();
        }

        [Fact]
        public async Task CompactionDeleteSettlesBeforeCoveredJson()
        {
            var rig = await StartedAsync(local: false);
            var hooks = rig.Table.Hooks!;
            var c = rig.Active.C;
            var admitted = rig.Table.Head;
            await AdvanceExecutionKit.CommitAsync(rig.Table, 2);
            // One spanning C that ends past the head the first turn is admitted at, and one below C
            await AdvanceExecutionKit.PlantCompactionAsync(rig.Table, c - 1, admitted + 1);
            await AdvanceExecutionKit.PlantCompactionAsync(rig.Table, 5, 9);
            var spanning = $"{c - 1:D20}.{admitted + 1:D20}.compacted.json";
            var below = $"{5:D20}.{9:D20}.compacted.json";
            hooks.ClearRequests();

            // The first compaction waits for an admission that published its end, with nothing issued and no failure counted
            AssertDeclined(await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(admitted, rig.Ledger)), MaintenanceReason.CleanupGate);
            Assert.Empty(Removed(hooks));
            Assert.Equal((AdvanceState.Runnable, 0, -1L), (rig.Active.State, rig.Active.Failures, rig.Active.FailedAt));

            var deletes = new LogDeletes(rig.Cleanup.AdvanceHooksForTests);
            var held = Signal();
            var release = Signal();
            deletes.Hold = async name =>
            {
                if (name == below)
                {
                    held.SetResult();
                    await release.Task;
                }
            };
            var turn = TurnAsync(rig);
            await ReachedAsync(held, turn);
            // Only the first compaction went while the second is outstanding
            Assert.Equal(new[] { spanning }, Removed(hooks));
            release.SetResult();
            Assert.Equal(OutcomeKind.Completed, (await turn).Kind);

            // Each compaction settled before any commit it covers was issued
            var events = deletes.Events.ToList();
            Assert.True(events.IndexOf($"settle {spanning}") < events.IndexOf($"issue {Json(c - 1)}"), string.Join(", ", events));
            var settled = events.IndexOf($"settle {below}");
            Assert.True(settled >= 0 && Enumerable.Range(5, 5).All(x => events.IndexOf($"issue {Json(x)}") > settled), string.Join(", ", events));
            deletes.AssertOneAtATime();
            await FinishLogPhaseAsync(rig);
            await AssertLogPhaseDoneAsync(rig);
        }

        [Fact]
        public async Task LogPhaseChecksOnlyRetainedRangeAfterIntentionalDeletes()
        {
            var rig = await StartedAsync(local: false);
            var hooks = rig.Table.Hooks!;
            var c = rig.Active.C;
            var all = await LogTargetNamesAsync(rig);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 3 };

            // Three a slice; the second gate passes over the gap the first left below C
            for (int slice = 1; slice <= 2; slice++)
            {
                Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
                Assert.Equal(all.Skip(3 * slice), await LogTargetNamesAsync(rig));
            }

            // A commit missing from the retained range declines with nothing removed
            hooks.Hidden = path => path.Full.EndsWith(Json(c + 1), StringComparison.Ordinal);
            hooks.ClearRequests();
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.ListingIneligible);
            Assert.Empty(Removed(hooks));
            Assert.Equal((AdvanceState.Runnable, AdvancePhase.Log), (rig.Active.State, rig.Active.Phase));

            hooks.Hidden = null;
            await FinishLogPhaseAsync(rig);
            await AssertLogPhaseDoneAsync(rig);
        }

        [Fact]
        public async Task FailedLogDeleteNeverWritesLogsComplete()
        {
            var rig = await StartedAsync(local: false);
            var hooks = rig.Table.Hooks!;
            var c = rig.Active.C;
            var records = await rig.Table.SnapshotAsync();
            var failing = Json(c - 3);
            var fail = true;
            hooks.Before = (verb, path) => fail && verb == "Rm" && path.Full.EndsWith(failing, StringComparison.Ordinal) ? throw new IOException("injected") : Task.CompletedTask;
            hooks.ClearRequests();

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeleteFailed);
            // The targets above it went, and nothing behind it was asked for
            Assert.Equal(new[] { Json(c - 1), Json(c - 2), failing }, Removed(hooks));
            var left = await LogTargetNamesAsync(rig);
            Assert.Equal(failing, left[0]);
            Assert.Equal((1, left.Length), (rig.Active.Failures, rig.Active.LogTargetsLeft));

            // Retried first on the next turn, at the same position
            hooks.ClearRequests();
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeleteFailed);
            Assert.Equal(new[] { failing }, Removed(hooks));
            Assert.Equal((2, left.Length), (rig.Active.Failures, rig.Active.LogTargetsLeft));
            Assert.Equal(left, await LogTargetNamesAsync(rig));
            Assert.Equal((AdvanceState.Runnable, AdvancePhase.Log), (rig.Active.State, rig.Active.Phase));
            Assert.Null(await LogsCompleteAsync(rig.Table));
            Assert.Equal(records, await rig.Table.SnapshotAsync());

            fail = false;
            hooks.ClearRequests();
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Equal(failing, Removed(hooks)[0]);
            Assert.Equal(0, rig.Active.Failures);
            await FinishLogPhaseAsync(rig);
            await AssertLogPhaseDoneAsync(rig);
        }

        [Theory]
        [InlineData("successor")]
        [InlineData("probe")]
        [InlineData("duplicate")]
        [InlineData("hidden")]
        public async Task FailedCompletionListingNeverWritesLogsComplete(string fault)
        {
            var started = await StartedAsync(local: false);
            var relisted = new RelistedStorage(started.Table.Hooks!);
            var rig = Remetered(started, MaintenanceBucketsTests.Unthrottled(), relisted);
            var hooks = rig.Table.Hooks!;
            var c = rig.Active.C;
            var head = rig.Table.Head;

            // The turn that deleted every target verifies nothing
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Empty(await LogTargetNamesAsync(rig));
            Assert.Null(await LogsCompleteAsync(rig.Table));
            Assert.Equal(AdvancePhase.Log, rig.Active.Phase);
            var records = await rig.Table.SnapshotAsync();

            var successor = Json(head + 1);
            switch (fault)
            {
                case "successor":
                    // A commit the listing does not show and the successor probe finds
                    await AdvanceExecutionKit.CommitAsync(rig.Table, 1);
                    hooks.Hidden = path => path.Full.EndsWith(successor, StringComparison.Ordinal);
                    break;
                case "probe":
                    hooks.Before = (verb, path) => verb == "Stat" && path.Full.EndsWith(successor, StringComparison.Ordinal) ? throw new IOException("injected") : Task.CompletedTask;
                    break;
                case "duplicate":
                    relisted.Relist = entries => entries.Append(entries.Single(x => x.Name == Json(c + 1))).ToList();
                    break;
                default:
                    hooks.Hidden = path => path.Full.EndsWith(Json(c + 1), StringComparison.Ordinal);
                    break;
            }
            var verified = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(head, rig.Ledger));

            AssertDeclined(verified, MaintenanceReason.ListingIneligible);
            Assert.Null(await LogsCompleteAsync(rig.Table));
            Assert.Equal((AdvanceState.Runnable, AdvancePhase.Log), (rig.Active.State, rig.Active.Phase));
            Assert.Equal(records, await rig.Table.SnapshotAsync());

            // The same verification at a listing that passes
            hooks.Hidden = null;
            hooks.Before = null;
            relisted.Relist = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            await AssertLogPhaseDoneAsync(rig);
        }

        [Theory]
        [InlineData("BuildDue")]
        [InlineData("Deadline")]
        [InlineData("Stopped")]
        public async Task CancellationStopsIssuanceAndSettlesOperations(string stopName)
        {
            var stop = Enum.Parse<IssuanceStop>(stopName);
            var rig = await StartedAsync(local: false);
            var hooks = rig.Table.Hooks!;
            var c = rig.Active.C;
            var deletes = new LogDeletes(rig.Cleanup.AdvanceHooksForTests);
            var held = Signal();
            var release = Signal();
            deletes.Hold = async name =>
            {
                if (name == Json(c - 2))
                {
                    held.SetResult();
                    await release.Task;
                }
            };
            hooks.ClearRequests();
            var job = new JobRun(rig.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var clock = new FakeTimeProvider(s_now);
            using var worker = new CancellationTokenSource();

            var turn = AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger, run: job, clock: clock, stop: worker.Token));
            await ReachedAsync(held, turn);
            StopIssuance(stop, job, clock, worker);
            // The submitted delete holds the turn, and with it the delete permit, until it settles
            Assert.False(turn.IsCompleted);
            release.SetResult();
            Assert.Equal(OutcomeKind.Completed, (await turn).Kind);

            Assert.Equal(new[] { Json(c - 1), Json(c - 2) }, Removed(hooks));
            Assert.False(await hooks.Exists(rig.Table.LogPath(Json(c - 2))));
            deletes.AssertOneAtATime();
            Assert.Equal((AdvanceState.Runnable, AdvancePhase.Log), (rig.Active.State, rig.Active.Phase));
            Assert.Equal(CapacityState.Runnable, rig.Active.Lease!.State);
            Assert.Equal(rig.Active.Lease.Demand, rig.Ledger.Current);

            // The rest go; a verification whose publication meets the same stop publishes nothing
            deletes.Hold = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Empty(await LogTargetNamesAsync(rig));
            var verifyJob = new JobRun(rig.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var verifyClock = new FakeTimeProvider(s_now);
            using var verifyWorker = new CancellationTokenSource();
            rig.Cleanup.AdvanceHooksForTests.Step = step =>
            {
                if (step == AdvanceSteps.LogVerified)
                {
                    StopIssuance(stop, verifyJob, verifyClock, verifyWorker);
                }
                return Task.CompletedTask;
            };
            var verified = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger, run: verifyJob, clock: verifyClock, stop: verifyWorker.Token));
            AssertDeclined(verified, stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.Transient);
            Assert.Equal(AdvancePhase.Log, rig.Active.Phase);
            Assert.Null(await LogsCompleteAsync(rig.Table));

            rig.Cleanup.AdvanceHooksForTests.Step = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            await AssertLogPhaseDoneAsync(rig);
        }

        [Fact]
        public Task EveryIntermediateLogStateIsReadableOrFailsLoudly() => IntermediateLogStatesAsync(duckDb: false);

        [DuckDbExtensionFact]
        public Task EveryIntermediateLogStateIsReadableOrFailsLoudlyThroughDuckDb() => IntermediateLogStatesAsync(duckDb: true);

        // The log phase over checksums and compactions, every version read through our reader and, if asked, DuckDB before each delete
        private async Task IntermediateLogStatesAsync(bool duckDb)
        {
            var root = Path.Combine(Path.GetTempPath(), "ae-log-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var table = await _kit.ReadableAsync(root);
            try
            {
                foreach (var version in new long[] { 3, 9, 11 })
                {
                    await AdvanceExecutionKit.PlantCrcAsync(table, version);
                }
                await AdvanceExecutionKit.PlantCompactionAsync(table, 5, 7);
                await AdvanceExecutionKit.PlantCompactionAsync(table, 10, 13);
                var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
                var cleanup = AdvanceExecutionKit.Cleanup(table, logs);
                await _kit.PublishAsync(cleanup, table);
                var ledger = new CapacityLedger(() => default);
                var rig = new Rig() { Table = table, Cleanup = cleanup, Active = await _kit.AdoptAsync(cleanup, table, ledger), Ledger = ledger, Logs = logs };
                var c = rig.Active.C;
                var targets = await LogTargetNamesAsync(rig);
                Assert.Contains($"{3:D20}.crc", targets);
                Assert.Contains($"{5:D20}.{7:D20}.compacted.json", targets);
                Assert.Contains($"{10:D20}.{13:D20}.compacted.json", targets);
                var states = new List<string>();

                // Every retained version reads as before; an older one reads as before or fails, never as another version
                async Task<(string?[] Ours, string?[] Duck)> ReadAsync(string state)
                {
                    var ours = await AdvanceExecutionKit.ReaderSnapshotsAsync(table.Raw, table.Head);
                    var duck = duckDb ? AdvanceExecutionKit.DuckDbSnapshots(Path.Combine(root, "t"), table.Head) : new string?[table.Head + 1];
                    for (long version = 0; version <= table.Head; version++)
                    {
                        var expected = (Ours: version == 0 ? null : $"{version}:{AdvanceExecutionKit.DataName(version)}", Duck: duckDb ? $"{version}" : null);
                        var read = (Ours: ours[version], Duck: duck[version]);
                        if (version >= c)
                        {
                            Assert.True(read == expected, $"{state}: version {version} read {read}");
                        }
                        else
                        {
                            Assert.True((read.Ours == null || read.Ours == expected.Ours) && (read.Duck == null || read.Duck == expected.Duck), $"{state}: version {version} read {read}");
                        }
                    }
                    states.Add(state);
                    return (ours, duck);
                }

                var before = await ReadAsync("before the log phase");
                Assert.All(Enumerable.Range(1, (int)table.Head), x => Assert.NotNull(before.Ours[x]));
                if (duckDb)
                {
                    Assert.All(before.Duck, Assert.NotNull);
                }
                // Each delete waits while the state its predecessors left is read
                var failed = new ConcurrentQueue<Exception>();
                rig.Cleanup.AdvanceHooksForTests.BeforeDelete = async target =>
                {
                    try
                    {
                        await ReadAsync($"before deleting {Path.GetFileName(target.Key)}");
                    }
                    catch (Exception e)
                    {
                        failed.Enqueue(e);
                        throw;
                    }
                };
                try
                {
                    await FinishLogPhaseAsync(rig);
                }
                catch (Exception) when (failed.TryPeek(out var read))
                {
                    // The read that failed the delete
                    ExceptionDispatchInfo.Throw(read);
                }
                var after = await ReadAsync("after the log phase");

                await AssertLogPhaseDoneAsync(rig);
                Assert.Equal(targets.Length + 2, states.Count);
                // No version below C reads at the end
                Assert.All(Enumerable.Range(0, (int)c), x => Assert.True(after.Ours[x] == null && after.Duck[x] == null, $"version {x}"));
            }
            finally
            {
                table.Raw.Dispose();
                try
                {
                    Directory.Delete(root, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }
    }
}
