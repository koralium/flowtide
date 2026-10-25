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
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public enum WriteFault
    {
        // Every attempt of the record fails
        Absent,
        // A truncated sibling lands beside the record
        Torn,
        // Attempt one lands after the record's successor validated
        Late
    }

    // Cells over clones of the churned table: processes, restarts, faults and the invariants after them
    public abstract class RetentionCells : IDisposable
    {
        // The churned table's newest checkpoint, the boundary its advance selects
        private protected const long C = 12;

        // Paths per part: the eleven planned paths in three parts
        private const int PartPaths = 4;

        // Bytes a torn in-memory record keeps
        private protected const int TornBytes = 40;

        private protected readonly MatrixTemplate _template;
        private protected readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();

        private protected RetentionCells(MatrixTemplate template)
        {
            _template = template;
        }

        public void Dispose()
        {
            _kit.Dispose();
        }

        private protected sealed class Cell
        {
            public required ExecutionTable Table { get; init; }

            public required Baseline Baseline { get; set; }

            public required CellOutcome Outcome { get; init; }
        }

        // One process's cleanup and ledger, and the context of its latest advance job
        private protected sealed class Process
        {
            public required DeltaTableCleanup Cleanup { get; init; }

            public required CapacityLedger Ledger { get; init; }

            public CleanupContext? Job { get; set; }

            public AdvanceHooks Hooks => Cleanup.AdvanceHooksForTests;

            public ActiveAdvance? Active => Cleanup.ActiveForTests;
        }

        // A clone on local disk or an owned in-memory store, changed before its baseline when asked
        private protected async Task<Cell> CellAsync(bool local, Func<ExecutionTable, Task>? before = null)
        {
            var table = await _template.CloneAsync(_kit, local);
            if (before != null)
            {
                await before(table);
            }
            return new Cell() { Table = table, Baseline = await Invariants.CaptureAsync(table, C, table.Head), Outcome = new CellOutcome(table) };
        }

        // A new process over the cell's table, its runs recorded
        private protected static Process Start(Cell cell, CapacityLedger? ledger = null, Func<ExecutionTable, DeltaTableCleanup>? cleanupOf = null)
        {
            var cleanup = cell.Outcome.Attach((cleanupOf ?? (x => AdvanceExecutionKit.Cleanup(x)))(cell.Table));
            cleanup.AdvanceForTests.PlanForTests = (configuration, storage, path, boundary, classes, context) =>
                AdvancePlanner.PlanAsync(configuration, storage, path, boundary, classes, context, new AdvanceWindowLimits() { PartPaths = PartPaths });
            ledger ??= new CapacityLedger(() => default);
            cell.Outcome.Ledger = ledger;
            return new Process() { Cleanup = cleanup, Ledger = ledger };
        }

        private protected async Task<CleanupResult?> JobAsync(Cell cell, Process process)
        {
            var context = _kit.AdvanceContext(cell.Table.Head, process.Ledger);
            process.Job = context;
            return await cell.Outcome.RunAsync(() => AdvanceExecutionKit.JobAsync(process.Cleanup, context));
        }

        // A delete turn, or the advance job that rebases
        private protected Task<CleanupResult?> NextAsync(Cell cell, Process process)
        {
            if (process.Active is { State: AdvanceState.Rebasing })
            {
                return JobAsync(cell, process);
            }
            return cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(process.Cleanup, _kit.DeletionContext(cell.Table.Head, process.Ledger)));
        }

        // The job, then turns until the process crashed or the advance left its turns; the last result
        private protected async Task<CleanupResult?> RunAsync(Cell cell, Process process, Func<bool> crashed)
        {
            var result = await JobAsync(cell, process);
            for (int turn = 0; turn < 40 && !crashed() && process.Active != null; turn++)
            {
                result = await NextAsync(cell, process);
            }
            return result;
        }

        // A restarted process's job, then its turns until the advance left them; the job's result
        private protected async Task<CleanupResult?> RestartAsync(Cell cell, CapacityLedger? ledger = null)
        {
            var process = Start(cell, ledger);
            var resumed = await JobAsync(cell, process);
            for (int turn = 0; turn < 40 && process.Active != null; turn++)
            {
                await NextAsync(cell, process);
            }
            Assert.True(process.Active == null, $"The advance is still {process.Active?.Phase} {process.Active?.State}");
            return resumed;
        }

        // Records from here on are kept and nothing is deleted after it
        private protected static async Task InvalidateAsync(Cell cell)
        {
            cell.Baseline = cell.Baseline with { Records = await cell.Table.ControlsAsync() };
            cell.Outcome.Invalidate();
        }

        // Every invariant and the advance complete: no log name below C, retired payload gone and only the done at C
        private protected static async Task AssertDoneAsync(Cell cell, params string[] kept)
        {
            cell.Outcome.Ending = CellEnding.Done;
            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
            await AdvanceExecutionKit.AssertAdvancedAsync(cell.Table, C, true, kept);
        }

        private protected static Task AssertUnresolvedAsync(Cell cell)
        {
            cell.Outcome.Ending = CellEnding.Unresolved;
            return Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
        }

        private protected static void AssertResult(CleanupResult? result, OutcomeKind kind, MaintenanceReason reason)
        {
            Assert.True(result != null && result.Kind == kind && result.Reason == reason, $"{result?.Kind} {result?.Reason}: {result?.Detail}");
        }

        // A job that resumed the crashed advance, or started another
        private protected static void AssertHandedOff(CleanupResult? result, bool resumed)
        {
            Assert.True(result is { Kind: OutcomeKind.Completed, Detail: { } detail } && detail.StartsWith(resumed ? "Resumed the advance" : "Started the advance", StringComparison.Ordinal),
                $"{result?.Kind} {result?.Reason}: {result?.Detail}");
        }

        // A commit after the head adding a file, another writer's or an own one in a new epoch, which the baseline then retains
        private protected static async Task AppendAsync(Cell cell, string name, bool own, long? epoch = null)
        {
            await WriteAppendAsync(cell.Table.Raw, cell.Table.Head + 1, name, own, epoch);
            await RetainAppendedAsync(cell, name);
        }

        // The commit at the version through the storage, the file it adds written first
        private protected static async Task WriteAppendAsync(IFileStorage storage, long version, string name, bool own, long? epoch = null)
        {
            if (!await storage.Exists($"/t/{name}"))
            {
                await using var stream = await storage.OpenWrite($"/t/{name}");
                await stream.WriteAsync(new byte[1]);
            }
            var add = DeltaLakeCheckpointBuildTests.Add(name);
            await (own ? DeltaTestKit.WriteOwnCommit(storage, "t", version, epoch ?? version, BoundarySelectorTests.Old(version), add) : DeltaTestKit.WriteCommit(storage, "t", version, add));
        }

        // The table's next commit, landed, and its added file in the baseline
        private protected static async Task RetainAppendedAsync(Cell cell, string name)
        {
            var table = cell.Table;
            table.Head++;
            var read = await DeltaTransactionReader.ReadTable(Invariants.View(table), "t", table.Head);
            var baseline = cell.Baseline;
            var retained = table.Root != null ? RetainedReferenceOracle.Upper(name) : name;
            cell.Baseline = baseline with
            {
                Head = table.Head,
                Versions = new Dictionary<long, VersionSnapshot?>(baseline.Versions) { [table.Head] = VersionSnapshot.Of(read!) },
                Oracle = baseline.Oracle is { } oracle ? oracle with { Retained = new SortedSet<string>(oracle.Retained.Append(retained), StringComparer.Ordinal) } : null,
                Retained = baseline.Retained.Append(retained).ToHashSet(StringComparer.Ordinal)
            };
        }

        // Another table's thirteen hours, which close admission
        private protected static CapacityLedger Closed()
        {
            var ledger = new CapacityLedger(() => default);
            ledger.Restore(default, "other-table", ControlTokens.New(), new CapacityDemand((long)(13 * 3600 * MaintenanceBuckets.DefaultRequestsPerSecond), 0), CapacityState.Runnable);
            Assert.False(ledger.Admits());
            return ledger;
        }

        private protected static bool Is(string name, ControlKind kind) => ControlName.TryParse(name, out var parsed) && parsed.Kind == kind;

        private protected static string NativeLog(Cell cell, string name) => Path.Combine(Invariants.NativeTable(cell.Table), "_delta_log", name);

        // One grid fault: the kind's record failing, torn or landing late, and whether the process may crash
        private protected sealed class GridFault
        {
            private readonly Cell _cell;
            private readonly ControlKind _kind;
            private readonly WriteFault _fault;
            private readonly LandsLater? _late;
            private int _attempts;
            private int _progressSteps;
            private bool _hit;

            public GridFault(Cell cell, ControlKind kind, WriteFault fault)
            {
                _cell = cell;
                _kind = kind;
                _fault = fault;
                // Planted on local disk, whose publication cannot tear
                if (fault == WriteFault.Late || (fault == WriteFault.Torn && cell.Table.Root != null))
                {
                    _late = new LandsLater(cell.Table, kind, torn: fault == WriteFault.Torn);
                }
            }

            public bool Fired { get; private set; }

            // The record left beside the published one
            public string? Sibling => _late?.First ?? _torn;

            private string? _torn;

            public void Attach(Process process)
            {
                process.Hooks.BeforePublish = BeforePublishAsync;
                process.Hooks.Step = StepAsync;
            }

            // The second part or progress, else the kind's first record
            private bool Targeted(string name)
            {
                Assert.True(ControlName.TryParse(name, out var parsed));
                return _kind switch
                {
                    ControlKind.Part => parsed.Ordinal == 1,
                    ControlKind.Progress => parsed.Ordinal == 2,
                    _ => true
                };
            }

            private async Task BeforePublishAsync(ControlKind kind, string name)
            {
                if (_late != null)
                {
                    await _late.Hook(kind, name);
                }
                if (kind != _kind || _hit)
                {
                    return;
                }
                if (_fault == WriteFault.Absent && Targeted(name))
                {
                    if (++_attempts == ControlPublisher.MaxAttempts)
                    {
                        _hit = true;
                        Fired = true;
                    }
                    throw new IOException($"{name} does not land");
                }
                if (_fault == WriteFault.Torn && _late == null)
                {
                    _hit = true;
                    _torn = name;
                    var hooks = _cell.Table.Hooks!;
                    hooks.TearAfterBytes = TornBytes;
                    hooks.TearWriteOf = name;
                    // A done crashes at its teardown
                    Fired = kind != ControlKind.Done;
                }
            }

            private async Task StepAsync(string step)
            {
                if (_kind == ControlKind.Done && _fault == WriteFault.Torn && step == AdvanceSteps.Teardown)
                {
                    if (_late != null)
                    {
                        await _late.ReleaseAsync();
                    }
                    Fired = true;
                    throw new IOException("The process ends before its teardown");
                }
                if (_late == null || Fired || step != ReleaseStep())
                {
                    return;
                }
                // A late progress lands once its successor validated
                if (_kind == ControlKind.Progress && _fault == WriteFault.Late && ++_progressSteps < 2)
                {
                    return;
                }
                await _late.ReleaseAsync();
                Fired = true;
            }

            // A late record lands after its successor validated, a planted torn one after its own publication
            private string? ReleaseStep() => (_kind, _fault) switch
            {
                (ControlKind.Part, _) => AdvanceSteps.Header,
                (ControlKind.Header, WriteFault.Late) => AdvanceSteps.Started,
                (ControlKind.Header, _) => AdvanceSteps.Header,
                (ControlKind.Started, WriteFault.Late) => AdvanceSteps.LogsComplete,
                (ControlKind.Started, _) => AdvanceSteps.Started,
                (ControlKind.LogsComplete, WriteFault.Late) => AdvanceSteps.Progress,
                (ControlKind.LogsComplete, _) => AdvanceSteps.LogsComplete,
                (ControlKind.Progress, _) => AdvanceSteps.Progress,
                _ => null
            };

            // A late done lands after the turn that published it tore down
            public async Task AfterAsync(Process process)
            {
                if (_kind == ControlKind.Done && _fault == WriteFault.Late && !Fired && process.Active == null && _late!.First != null)
                {
                    await _late.ReleaseAsync();
                    Fired = true;
                }
            }

            // The result of the job or turn the absent record ended
            public void AssertRunEnded(CleanupResult? result)
            {
                if (_fault == WriteFault.Absent)
                {
                    AssertResult(result, OutcomeKind.Declined, _kind == ControlKind.Progress ? MaintenanceReason.Transient : MaintenanceReason.PublishFailed);
                }
            }

            // A torn started, logs-complete or progress leaves its group unknown
            public bool Suspends => _fault == WriteFault.Torn && _kind is ControlKind.Started or ControlKind.LogsComplete or ControlKind.Progress;
        }

        // The fault's process up to its crash, which a restart follows
        private protected async Task<GridFault> FaultedAsync(Cell cell, ControlKind kind, WriteFault fault)
        {
            var process = Start(cell);
            var grid = new GridFault(cell, kind, fault);
            grid.Attach(process);
            var result = await RunAsync(cell, process, () => grid.Fired);
            await grid.AfterAsync(process);
            Assert.True(grid.Fired, $"The {fault} {kind} fault never fired: {result?.Kind} {result?.Reason}: {result?.Detail}");
            grid.AssertRunEnded(result);
            if (fault != WriteFault.Absent)
            {
                Assert.Contains(grid.Sibling!, await cell.Table.ControlsAsync());
            }
            return grid;
        }
    }

    public abstract class RetentionCrashMatrix : RetentionCells
    {
        private protected RetentionCrashMatrix(MatrixTemplate template) : base(template)
        {
        }

        // On local disk, or on an owned in-memory store
        private protected abstract bool Local { get; }

        private protected Task<Cell> CellAsync(Func<ExecutionTable, Task>? before = null) => CellAsync(Local, before);

        [Theory]
        [InlineData(nameof(ControlKind.Part), WriteFault.Absent)]
        [InlineData(nameof(ControlKind.Part), WriteFault.Torn)]
        [InlineData(nameof(ControlKind.Part), WriteFault.Late)]
        [InlineData(nameof(ControlKind.Header), WriteFault.Absent)]
        [InlineData(nameof(ControlKind.Header), WriteFault.Torn)]
        [InlineData(nameof(ControlKind.Header), WriteFault.Late)]
        [InlineData(nameof(ControlKind.Started), WriteFault.Absent)]
        [InlineData(nameof(ControlKind.Started), WriteFault.Torn)]
        [InlineData(nameof(ControlKind.Started), WriteFault.Late)]
        [InlineData(nameof(ControlKind.LogsComplete), WriteFault.Absent)]
        [InlineData(nameof(ControlKind.LogsComplete), WriteFault.Torn)]
        [InlineData(nameof(ControlKind.LogsComplete), WriteFault.Late)]
        [InlineData(nameof(ControlKind.Progress), WriteFault.Absent)]
        [InlineData(nameof(ControlKind.Progress), WriteFault.Torn)]
        [InlineData(nameof(ControlKind.Progress), WriteFault.Late)]
        [InlineData(nameof(ControlKind.Done), WriteFault.Absent)]
        [InlineData(nameof(ControlKind.Done), WriteFault.Torn)]
        [InlineData(nameof(ControlKind.Done), WriteFault.Late)]
        public async Task WriteFaultKeepsInvariants(string kindName, WriteFault fault)
        {
            var kind = Enum.Parse<ControlKind>(kindName);
            var cell = await CellAsync();
            var grid = await FaultedAsync(cell, kind, fault);
            if (grid.Suspends)
            {
                await InvalidateAsync(cell);
            }

            var resumed = await RestartAsync(cell);

            if (grid.Suspends)
            {
                AssertResult(resumed, OutcomeKind.Declined, MaintenanceReason.ManifestSuspended);
                await AssertUnresolvedAsync(cell);
                return;
            }
            // A completed advance has nothing to resume, one without a durable start starts again
            if (kind == ControlKind.Done && fault != WriteFault.Absent)
            {
                AssertResult(resumed, OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint);
            }
            else
            {
                AssertHandedOff(resumed, !(fault == WriteFault.Absent && kind is ControlKind.Part or ControlKind.Header or ControlKind.Started));
            }
            await AssertDoneAsync(cell);
        }
    }

    public class RetentionCrashMatrixLocal : RetentionCrashMatrix, IClassFixture<MatrixTemplate>
    {
        public RetentionCrashMatrixLocal(MatrixTemplate template) : base(template)
        {
        }

        private protected override bool Local => true;

        // The advance to 8 below a churned table whose checkpoint 12 is gone until it is built again
        private async Task<Cell> TwoBoundariesAsync()
        {
            return await CellAsync(table =>
            {
                File.Delete(Path.Combine(Invariants.NativeTable(table), "_delta_log", $"{C:D20}.checkpoint.parquet"));
                return Task.CompletedTask;
            });
        }

        // Every attempt of the process's started record fails, its bytes taken from the job's scratch for a later plant
        private static Func<ControlKind, string, Task> StartedNeverLands(Process process, Action<string, byte[]> taken)
        {
            return (kind, name) =>
            {
                if (kind != ControlKind.Started)
                {
                    return Task.CompletedTask;
                }
                taken(name, File.ReadAllBytes(process.Job!.Scratch.Files.Single(x => x.EndsWith(".record", StringComparison.Ordinal))));
                throw new IOException($"{name} lands later");
            };
        }

        public enum LateStartCase
        {
            AfterTeardown,
            AfterSameCReplan,
            AfterHigherCompletion
        }

        [Theory]
        [InlineData(LateStartCase.AfterTeardown)]
        [InlineData(LateStartCase.AfterSameCReplan)]
        [InlineData(LateStartCase.AfterHigherCompletion)]
        public async Task LateStartKeepsInvariants(LateStartCase late)
        {
            var cell = late == LateStartCase.AfterHigherCompletion ? await TwoBoundariesAsync() : await CellAsync();
            var first = Start(cell);
            string? name = null;
            byte[]? bytes = null;
            if (late == LateStartCase.AfterTeardown)
            {
                // Attempt one lands after the advance it started is torn down
                var landsLater = new LandsLater(cell.Table, ControlKind.Started);
                first.Hooks.BeforePublish = landsLater.Hook;
                await RunAsync(cell, first, () => false);
                Assert.Single(await cell.Table.ControlsAsync());
                name = await landsLater.ReleaseAsync();
            }
            else
            {
                // No attempt lands before a replan completes
                first.Hooks.BeforePublish = StartedNeverLands(first, (attempt, taken) => (name, bytes) = (name ?? attempt, bytes ?? taken));
                AssertResult(await JobAsync(cell, first), OutcomeKind.Declined, MaintenanceReason.PublishFailed);
                Assert.True(ControlName.TryParse(name!, out var planned));
                Assert.Equal(late == LateStartCase.AfterSameCReplan ? C : 8, planned.Boundary);
                if (late == LateStartCase.AfterHigherCompletion)
                {
                    await _kit.BuildAsync(cell.Table.Raw, C);
                }
                AssertHandedOff(await RestartAsync(cell), resumed: false);
                Assert.Single(await cell.Table.ControlsAsync());
                await CleanupRecordKit.PlantAsync(cell.Table.Raw, "t", name!, bytes!);
            }
            // The late start reserves its boundary again
            Assert.True(ControlName.TryParse(name!, out var started));
            Assert.Equal(started.Boundary, await AdvanceExecutionKit.FloorAsync(cell.Table));

            // The next recovery releases it by a completion at or above its boundary
            AssertResult(await RestartAsync(cell), OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint);

            Assert.DoesNotContain(name!, await cell.Table.ControlsAsync());
            await AssertDoneAsync(cell);
        }

        [Fact]
        public async Task LateStartOfEarlierAdvanceAfterLaterDone()
        {
            var cell = await TwoBoundariesAsync();
            // The advance to 8 completes, its started's attempt one still in flight
            var first = Start(cell);
            var landsLater = new LandsLater(cell.Table, ControlKind.Started);
            first.Hooks.BeforePublish = landsLater.Hook;
            await RunAsync(cell, first, () => false);
            Assert.True(ControlName.TryParse(Assert.Single(await cell.Table.ControlsAsync()), out var earlier) && earlier.Boundary == 8);
            await _kit.BuildAsync(cell.Table.Raw, C);
            // The advance to C completes in a restarted process
            AssertHandedOff(await RestartAsync(cell), resumed: false);

            var late = await landsLater.ReleaseAsync();
            Assert.Equal(8, await AdvanceExecutionKit.FloorAsync(cell.Table));
            AssertResult(await RestartAsync(cell), OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint);

            Assert.DoesNotContain(late, await cell.Table.ControlsAsync());
            await AssertDoneAsync(cell);
        }
    }

    public class RetentionCrashMatrixSteps : RetentionCells, IClassFixture<MatrixTemplate>
    {
        public RetentionCrashMatrixSteps(MatrixTemplate template) : base(template)
        {
        }

        private Task<Cell> CellAsync(Func<ExecutionTable, Task>? before = null) => CellAsync(local: true, before);

        [Theory]
        [InlineData(nameof(AdvanceSteps.Hint))]
        [InlineData(nameof(AdvanceSteps.Header))]
        [InlineData(nameof(AdvanceSteps.Recheck))]
        [InlineData(nameof(AdvanceSteps.Reserved))]
        [InlineData(nameof(AdvanceSteps.HandedOff))]
        [InlineData(nameof(AdvanceSteps.Resumed))]
        [InlineData(nameof(AdvanceSteps.LogSlice))]
        [InlineData(nameof(AdvanceSteps.LogVerified))]
        [InlineData(nameof(AdvanceSteps.PayloadSlice))]
        [InlineData(nameof(AdvanceSteps.Progress))]
        [InlineData(nameof(AdvanceSteps.Teardown))]
        [InlineData(nameof(AdvanceSteps.Rebase))]
        public async Task StepCrashKeepsInvariants(string step)
        {
            var cell = await CellAsync();
            var crashed = false;
            Task Crash(string at)
            {
                if (at == step && !crashed)
                {
                    crashed = true;
                    throw new IOException($"The process ends at {at}");
                }
                return Task.CompletedTask;
            }
            var first = Start(cell);
            string[] kept = Array.Empty<string>();
            if (step == AdvanceSteps.Resumed)
            {
                // Started, then a restart that ends where it resumes
                await RunAsync(cell, first, () => true);
                var resuming = Start(cell);
                resuming.Hooks.Step = Crash;
                AssertResult(await JobAsync(cell, resuming), OutcomeKind.Failed, MaintenanceReason.Transient);
            }
            else if (step == AdvanceSteps.Rebase)
            {
                // Past its first progress another writer re-adds a path of the last part, and the rebase ends the process
                var progressed = false;
                first.Hooks.Step = at =>
                {
                    progressed |= at == AdvanceSteps.Progress;
                    return Task.CompletedTask;
                };
                await RunAsync(cell, first, () => progressed);
                kept = new[] { "/t/part-0009.parquet" };
                await AppendAsync(cell, "part-0009.parquet", own: false);
                first.Hooks.Step = Crash;
                Assert.Equal(OutcomeKind.NoWork, (await NextAsync(cell, first))!.Kind);
                Assert.Equal(AdvanceState.Rebasing, first.Active!.State);
                AssertResult(await NextAsync(cell, first), OutcomeKind.Declined, MaintenanceReason.Transient);
                // The restarted sink anchors its own commit above the foreign one
                await AppendAsync(cell, "part-0099.parquet", own: true);
            }
            else
            {
                first.Hooks.Step = Crash;
                await RunAsync(cell, first, () => crashed);
            }
            Assert.True(crashed, $"The process never reached {step}");
            if (step == AdvanceSteps.Reserved)
            {
                // Reserved and released with no started record published
                Assert.Equal(default, first.Ledger.Current);
                Assert.DoesNotContain(await cell.Table.ControlsAsync(), x => Is(x, ControlKind.Started));
                Assert.Equal(ControlFence.NoReservation, await AdvanceExecutionKit.FloorAsync(cell.Table));
            }

            var resumed = await RestartAsync(cell);

            // A done whose teardown ended leaves nothing to resume, a crash before the started record starts again
            if (step == AdvanceSteps.Teardown)
            {
                AssertResult(resumed, OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint);
            }
            else
            {
                AssertHandedOff(resumed, step is not (AdvanceSteps.Hint or AdvanceSteps.Header or AdvanceSteps.Recheck or AdvanceSteps.Reserved));
            }
            await AssertDoneAsync(cell, kept);
        }

        public enum TwoRestartCase
        {
            StartedLate,
            ProgressAbsent,
            DoneTorn,
            LogSliceCrash
        }

        [Theory]
        [InlineData(TwoRestartCase.StartedLate)]
        [InlineData(TwoRestartCase.ProgressAbsent)]
        [InlineData(TwoRestartCase.DoneTorn)]
        [InlineData(TwoRestartCase.LogSliceCrash)]
        public async Task TwoRestartsKeepInvariants(TwoRestartCase twice)
        {
            var cell = await CellAsync();
            // The first crash, then a restart that ends at its next step of the kind
            var again = AdvanceSteps.LogSlice;
            switch (twice)
            {
                case TwoRestartCase.StartedLate:
                    await FaultedAsync(cell, ControlKind.Started, WriteFault.Late);
                    again = AdvanceSteps.PayloadSlice;
                    break;
                case TwoRestartCase.ProgressAbsent:
                    await FaultedAsync(cell, ControlKind.Progress, WriteFault.Absent);
                    again = AdvanceSteps.Progress;
                    break;
                case TwoRestartCase.DoneTorn:
                    await FaultedAsync(cell, ControlKind.Done, WriteFault.Torn);
                    again = AdvanceSteps.Teardown;
                    break;
                default:
                    var first = Start(cell);
                    var logSlices = 0;
                    first.Hooks.Step = step => step == AdvanceSteps.LogSlice && ++logSlices == 2 ? throw new IOException("The process ends at its second log slice") : Task.CompletedTask;
                    await RunAsync(cell, first, () => logSlices >= 2);
                    Assert.Equal(2, logSlices);
                    break;
            }
            var restarted = Start(cell);
            var crashed = false;
            restarted.Hooks.Step = step =>
            {
                if (step == again && !crashed)
                {
                    crashed = true;
                    throw new IOException($"The restart ends at {step}");
                }
                return Task.CompletedTask;
            };
            await RunAsync(cell, restarted, () => crashed);
            Assert.Equal(twice != TwoRestartCase.DoneTorn, crashed);

            // The second restart resumes under admission another table closed
            var resumed = await RestartAsync(cell, Closed());

            if (twice == TwoRestartCase.DoneTorn)
            {
                // The first restart's recovery tore the advance down, and a new one waits on the closed admission
                AssertResult(resumed, OutcomeKind.Declined, MaintenanceReason.DeleteBacklog);
            }
            else
            {
                AssertHandedOff(resumed, resumed: true);
            }
            await AssertDoneAsync(cell);
        }
    }

    public class RetentionCrashMatrixDeletes : RetentionCells, IClassFixture<MatrixTemplate>
    {
        public RetentionCrashMatrixDeletes(MatrixTemplate template) : base(template)
        {
        }

        private Task<Cell> CellAsync(Func<ExecutionTable, Task>? before = null) => CellAsync(local: true, before);

        public enum DeleteTarget
        {
            Json,
            Crc,
            Checkpoint,
            Compaction,
            Payload,
            ControlPredecessor
        }

        [Theory]
        [InlineData(DeleteTarget.Json, false)]
        [InlineData(DeleteTarget.Json, true)]
        [InlineData(DeleteTarget.Crc, false)]
        [InlineData(DeleteTarget.Crc, true)]
        [InlineData(DeleteTarget.Checkpoint, false)]
        [InlineData(DeleteTarget.Checkpoint, true)]
        [InlineData(DeleteTarget.Compaction, false)]
        [InlineData(DeleteTarget.Compaction, true)]
        [InlineData(DeleteTarget.Payload, false)]
        [InlineData(DeleteTarget.Payload, true)]
        [InlineData(DeleteTarget.ControlPredecessor, false)]
        [InlineData(DeleteTarget.ControlPredecessor, true)]
        public async Task DeleteTransitionKeepsInvariants(DeleteTarget target, bool settled)
        {
            // A checksum and a compaction below C, each a log target of its own
            var cell = await CellAsync(async table =>
            {
                await AdvanceExecutionKit.PlantCrcAsync(table, 6);
                await AdvanceExecutionKit.PlantCompactionAsync(table, 5, 6);
            });
            var name = target switch
            {
                DeleteTarget.Json => $"{10:D20}.json",
                DeleteTarget.Crc => $"{6:D20}.crc",
                DeleteTarget.Checkpoint => $"{8:D20}.checkpoint.parquet",
                DeleteTarget.Compaction => $"{5:D20}.{6:D20}.compacted.json",
                DeleteTarget.Payload => "part-0004.parquet",
                _ => null
            };
            bool Matches(DeletionTarget deletion) => name != null ? Path.GetFileName(deletion.Key) == name : deletion.Kind == TargetKind.Control;
            var process = Start(cell);
            // One payload delete at a time, the slice ending right after the target
            process.Cleanup.Settings = process.Cleanup.Settings with { MaxOutstanding = 1 };
            DeleteStatus? reached = null;
            var crashed = false;
            process.Hooks.BeforeDelete = deletion =>
            {
                if (crashed)
                {
                    throw new IOException("The process ends before this delete");
                }
                if (!settled && Matches(deletion))
                {
                    crashed = true;
                    throw new IOException($"The process ends before {deletion.Key} is deleted");
                }
                return Task.CompletedTask;
            };
            var recorded = process.Hooks.Settled!;
            process.Hooks.Settled = (deletion, outcome) =>
            {
                recorded(deletion, outcome);
                if (Matches(deletion) && reached == null)
                {
                    reached = outcome.Status;
                    crashed = true;
                }
            };
            // A payload slice that deleted its target ends before its progress
            process.Hooks.BeforePublish = (kind, _) => crashed && settled && kind == ControlKind.Progress && target == DeleteTarget.Payload ? throw new IOException("The process ends before its progress") : Task.CompletedTask;

            await RunAsync(cell, process, () => crashed);

            Assert.Equal(settled ? DeleteStatus.Deleted : DeleteStatus.Failed, reached);
            var listed = await cell.Table.ControlsAsync();
            if (target == DeleteTarget.ControlPredecessor)
            {
                // The predecessor goes with the teardown when its delete failed
                Assert.Equal(settled ? 1 : 2, listed.Count(x => Is(x, ControlKind.Progress)));
            }
            else
            {
                Assert.Equal(!settled, File.Exists(name!.StartsWith("part-", StringComparison.Ordinal) ? Path.Combine(Invariants.NativeTable(cell.Table), name) : NativeLog(cell, name)));
            }

            AssertHandedOff(await RestartAsync(cell), resumed: true);

            await AssertDoneAsync(cell);
        }
    }

    public class RetentionCrashMatrixMemory : RetentionCrashMatrix, IClassFixture<MatrixTemplate>
    {
        public RetentionCrashMatrixMemory(MatrixTemplate template) : base(template)
        {
        }

        private protected override bool Local => false;

        public enum TeardownPhase
        {
            Manifest,
            Progress,
            Start,
            OlderCompletion
        }

        [Theory]
        [InlineData(TeardownPhase.Manifest)]
        [InlineData(TeardownPhase.Progress)]
        [InlineData(TeardownPhase.Start)]
        [InlineData(TeardownPhase.OlderCompletion)]
        public async Task TeardownFailureFinishesOnNextRecovery(TeardownPhase phase)
        {
            // Teardown deletes on local disk bypass every hook
            var cell = await CellAsync();
            var hooks = cell.Table.Hooks!;
            var process = Start(cell);
            string? torn = null;
            if (phase == TeardownPhase.OlderCompletion)
            {
                // The done's first attempt tears, an invalid copy the teardown removes last
                process.Hooks.BeforePublish = (kind, name) =>
                {
                    if (kind == ControlKind.Done && torn == null)
                    {
                        torn = name;
                        hooks.TearAfterBytes = TornBytes;
                        hooks.TearWriteOf = name;
                    }
                    return Task.CompletedTask;
                };
            }
            var kind = phase switch
            {
                TeardownPhase.Manifest => ControlKind.Part,
                TeardownPhase.Progress => ControlKind.Progress,
                TeardownPhase.Start => ControlKind.Started,
                _ => ControlKind.Done
            };
            string? failed = null;
            hooks.Before = (verb, path) =>
            {
                if (verb == "Rm" && failed == null && process.Active?.Phase == AdvancePhase.Done && Is(path.Name, kind))
                {
                    failed = path.Name;
                    throw new IOException($"Removing {path.Name} fails");
                }
                return Task.CompletedTask;
            };

            await RunAsync(cell, process, () => false);

            Assert.NotNull(failed);
            Assert.Equal(torn ?? failed, failed);
            // Done, with the failed record and every later phase's left for the next recovery
            var listed = await cell.Table.ControlsAsync();
            Assert.Contains(failed, listed);
            Assert.Contains(listed, x => Is(x, ControlKind.Done) && x != torn);
            if (phase < TeardownPhase.Start)
            {
                Assert.Contains(listed, x => Is(x, ControlKind.Started));
            }
            hooks.Before = null;

            AssertResult(await RestartAsync(cell), OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint);

            await AssertDoneAsync(cell);
        }
    }
}
