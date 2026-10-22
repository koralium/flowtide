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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using System.Collections.Concurrent;
using Xunit.Sdk;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public abstract class RetentionMatrixKitBase : IDisposable
    {
        // The churned table's newest checkpoint, the boundary its advance selects
        private protected const long C = 12;

        private protected readonly MatrixTemplate _template;
        private protected readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();

        protected RetentionMatrixKitBase(MatrixTemplate template)
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

            public required DeltaTableCleanup Cleanup { get; init; }

            public required CapacityLedger Ledger { get; init; }
        }

        // A clone captured, then its advance planned and handed off by one recorded job
        private protected async Task<Cell> StartedAsync(bool local = true, Action<Cell>? before = null)
        {
            var table = await _template.CloneAsync(_kit, local);
            var outcome = new CellOutcome(table);
            var cell = new Cell()
            {
                Table = table,
                Baseline = await Invariants.CaptureAsync(table, C, table.Head),
                Outcome = outcome,
                Cleanup = AdvanceExecutionKit.Cleanup(table),
                Ledger = new CapacityLedger(() => default)
            };
            before?.Invoke(cell);
            outcome.Attach(cell.Cleanup);
            outcome.Ledger = cell.Ledger;
            var started = await outcome.RunAsync(() => AdvanceExecutionKit.JobAsync(cell.Cleanup, _kit.AdvanceContext(table.Head, cell.Ledger)));
            Assert.True(started is { Kind: OutcomeKind.Completed }, $"{started?.Kind} {started?.Reason}: {started?.Detail}");
            Assert.Equal(C, cell.Cleanup.ActiveForTests!.C);
            return cell;
        }

        private protected async Task<Cell> DoneAsync(bool local = true, Action<Cell>? before = null)
        {
            var cell = await StartedAsync(local, before);
            await _kit.ExecuteAsync(cell.Cleanup, cell.Table, cell.Ledger);
            return cell;
        }

        // Recorded delete turns until the payload has the given paths processed
        private protected async Task ProcessAsync(Cell cell, long paths)
        {
            var active = cell.Cleanup.ActiveForTests!;
            for (int turn = 0; turn < 40 && !(active.Phase == AdvancePhase.Payload && active.Processed >= paths); turn++)
            {
                var result = await cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(cell.Cleanup, _kit.DeletionContext(cell.Table.Head, cell.Ledger)));
                Assert.True(result is { Kind: OutcomeKind.Completed }, $"{result?.Kind} {result?.Reason}: {result?.Detail}");
            }
            Assert.Equal(paths, active.Processed);
        }

        // A restarted process's cleanup and ledger resuming the advance to done through its own job
        private protected async Task RestartToDoneAsync(Cell cell)
        {
            var cleanup = cell.Outcome.Attach(AdvanceExecutionKit.Cleanup(cell.Table));
            var ledger = new CapacityLedger(() => default);
            cell.Outcome.Ledger = ledger;
            var resumed = await cell.Outcome.RunAsync(() => AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(cell.Table.Head, ledger)));
            Assert.True(resumed is { Kind: OutcomeKind.Completed }, $"{resumed?.Kind} {resumed?.Reason}: {resumed?.Detail}");
            await _kit.ExecuteAsync(cleanup, cell.Table, ledger);
        }

        private protected static async Task AssertBrokenAsync(Cell cell, int invariant, string detail)
        {
            var failure = await Assert.ThrowsAnyAsync<XunitException>(() => Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome));
            Assert.StartsWith($"Invariant {invariant}: {detail}", failure.Message);
        }

        private protected static string Native(Cell cell, string name) => Path.Combine(Invariants.NativeTable(cell.Table), name);

        private protected static string NativeLog(Cell cell, string name) => Path.Combine(Invariants.NativeTable(cell.Table), "_delta_log", name);

        // A commit whose add names another file
        private protected static void Readd(Cell cell, long version, string file)
        {
            var path = NativeLog(cell, $"{version:D20}.json");
            File.WriteAllText(path, File.ReadAllText(path).Replace($"\"part-{version:D4}.parquet\"", $"\"{file}\""));
        }

        // A commit written again through the table's storage with its actions changed
        private protected static async Task RewriteAsync(Cell cell, long version, Func<List<DeltaAction>, IEnumerable<DeltaAction>> change)
        {
            var actions = change(await DeltaTestKit.ReadCommitActions(cell.Table.Raw, "t", version)).ToArray();
            await cell.Table.Raw.Rm(DeltaTestKit.CommitPath("t", version));
            await DeltaTestKit.WriteCommit(cell.Table.Raw, "t", version, actions);
        }

        // A one-row deletion vector file on the table's storage
        private protected static async Task<DeletionVector> VectorAsync(Cell cell)
        {
            var (name, id) = DeletionVectorWriter.GenerateDestination();
            var (_, size) = await DeletionVectorWriter.WriteDeletionVector(cell.Table.Raw, "t", name, RoaringBitmapArray.Create(new long[] { 0 }));
            return new DeletionVector() { StorageType = "u", PathOrInlineDv = id, Offset = 1, SizeInBytes = size, Cardinality = 1 };
        }

        // A one-byte file at a store path
        private protected static async Task WriteAsync(Cell cell, string path)
        {
            await using var stream = await cell.Table.Raw.OpenWrite(path);
            await stream.WriteAsync(new byte[1]);
        }
    }

    public class RetentionMatrixKitTests : RetentionMatrixKitBase, IClassFixture<MatrixTemplate>
    {
        public RetentionMatrixKitTests(MatrixTemplate template) : base(template)
        {
        }

        public enum DoneViolation
        {
            ChangedAddAtRetainedVersion,
            RetainedCommitDeleted,
            BoundaryCheckpointDeleted,
            VectorAtRetainedVersion,
            SchemaAtRetainedVersion,
            ProtocolAtRetainedVersion,
            ConfigurationAtRetainedVersion,
            TransactionAtRetainedVersion,
            DomainAtRetainedVersion,
            RetainedFileDeleted,
            RetainedFileDeletedInMemory,
            RetainedVectorDeletedInMemory,
            StrayPayloadDeleted,
            RetiredFileNeverDeleted,
            RetiredFileBack,
            DeleteAfterInvalidation,
            RemovalAfterInvalidationInMemory,
            ReservationAfterTeardown,
            SecondDone,
            DoneAtAnotherBoundary,
            Escape,
            CancellationWithoutStop,
            KeptTargetGone,
            UnqualifiedKeptTargetGone,
            UnqualifiedKeptTargetGoneInMemory
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CleanAdvanceKeepsEveryInvariant(bool local)
        {
            // One payload target is reported kept once before its delete settles
            var kept = 0;
            var cell = await DoneAsync(local, x => x.Cleanup.AdvanceHooksForTests.BeforeDelete = target =>
            {
                if (target.Kind == TargetKind.Payload && Interlocked.Exchange(ref kept, 1) == 0)
                {
                    x.Cleanup.AdvanceHooksForTests.Settled!(target, DeleteOutcome.Keep(KeepReason.StorageChanged));
                }
                return Task.CompletedTask;
            });
            var stop = new CancellationTokenSource();
            stop.Cancel();
            Assert.Null(await cell.Outcome.RunAsync(() => throw new OperationCanceledException(stop.Token), stop.Token));

            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);

            // Eleven paths and fourteen log names deleted
            var deleted = cell.Outcome.Settled.Where(x => x.Outcome.Status == DeleteStatus.Deleted).GroupBy(x => x.Target.Kind).ToDictionary(x => x.Key, x => x.Count());
            Assert.Equal((11, 14), (deleted[TargetKind.Payload], deleted[TargetKind.Log]));
            Assert.Equal(local ? 11 : 0, cell.Baseline.Oracle?.Retired.Count ?? 0);
            Assert.Single(cell.Outcome.Settled, x => x.Outcome.Status == DeleteStatus.Kept);
        }

        [Theory]
        [InlineData(DoneViolation.ChangedAddAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.RetainedCommitDeleted, 1, "version 13 reads nothing")]
        [InlineData(DoneViolation.BoundaryCheckpointDeleted, 1, "version 12 reads nothing")]
        [InlineData(DoneViolation.VectorAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.SchemaAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.ProtocolAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.ConfigurationAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.TransactionAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.DomainAtRetainedVersion, 1, "version 13 reads")]
        [InlineData(DoneViolation.RetainedFileDeleted, 2, "retained files are gone: PART-0013.PARQUET")]
        [InlineData(DoneViolation.RetainedFileDeletedInMemory, 2, "retained files are gone: part-0013.parquet")]
        [InlineData(DoneViolation.RetainedVectorDeletedInMemory, 2, "retained files are gone: ")]
        [InlineData(DoneViolation.StrayPayloadDeleted, 2, "deleted payload outside the retired set: PART-0099.PARQUET")]
        [InlineData(DoneViolation.RetiredFileNeverDeleted, 2, "retired payload this advance never deleted: PART-0001.PARQUET")]
        [InlineData(DoneViolation.RetiredFileBack, 2, "retired payload still present: PART-0001.PARQUET")]
        [InlineData(DoneViolation.DeleteAfterInvalidation, 3, "deleted after evidence was invalid")]
        [InlineData(DoneViolation.RemovalAfterInvalidationInMemory, 3, "1 removals after evidence was invalid")]
        [InlineData(DoneViolation.ReservationAfterTeardown, 6, "the floor is 12 after the teardown")]
        [InlineData(DoneViolation.SecondDone, 6, "2 records after the teardown")]
        [InlineData(DoneViolation.DoneAtAnotherBoundary, 6, ".flowtide-cleanup.00000000000000000008.")]
        [InlineData(DoneViolation.Escape, 7, "escaped: InvalidOperationException: planted")]
        [InlineData(DoneViolation.CancellationWithoutStop, 7, "escaped: OperationCanceledException")]
        [InlineData(DoneViolation.KeptTargetGone, 7, "")]
        [InlineData(DoneViolation.UnqualifiedKeptTargetGone, 7, "/t/sub/../kept.parquet, kept for Unqualified, is gone")]
        [InlineData(DoneViolation.UnqualifiedKeptTargetGoneInMemory, 7, "/t/sub/../kept.parquet, kept for Unqualified, is gone")]
        public async Task DoneCellFailsOnItsViolation(DoneViolation violation, int invariant, string detail)
        {
            var local = violation is not (DoneViolation.RetainedFileDeletedInMemory or DoneViolation.RetainedVectorDeletedInMemory or DoneViolation.RemovalAfterInvalidationInMemory
                or DoneViolation.UnqualifiedKeptTargetGoneInMemory);
            // Planted before or during the advance, or after it
            var cell = await DoneAsync(local, violation switch
            {
                DoneViolation.RetiredFileNeverDeleted => x => File.Delete(Native(x, "part-0001.parquet")),
                DoneViolation.DeleteAfterInvalidation => x => x.Cleanup.AdvanceHooksForTests.Step = step =>
                {
                    if (step == AdvanceSteps.LogsComplete)
                    {
                        x.Outcome.Invalidate();
                    }
                    return Task.CompletedTask;
                },
                _ => null
            });
            var done = (await cell.Table.ControlsAsync()).Single();
            switch (violation)
            {
                case DoneViolation.ChangedAddAtRetainedVersion:
                    Readd(cell, 13, "part-0099.parquet");
                    break;
                case DoneViolation.RetainedCommitDeleted:
                    File.Delete(NativeLog(cell, $"{13:D20}.json"));
                    break;
                case DoneViolation.BoundaryCheckpointDeleted:
                    File.Delete(NativeLog(cell, $"{C:D20}.checkpoint.parquet"));
                    break;
                case DoneViolation.VectorAtRetainedVersion:
                    var vector = await VectorAsync(cell);
                    await RewriteAsync(cell, 13, x => x.Select(a => a.Add != null ? new DeltaAction() { Add = a.Add.WithDeletionVector(vector) } : a));
                    break;
                case DoneViolation.SchemaAtRetainedVersion:
                case DoneViolation.ConfigurationAtRetainedVersion:
                    var metadata = DeltaMetadataCopy.Copy((await DeltaTransactionReader.ReadTable(cell.Table.Raw, "t", 13))!.Metadata, null);
                    if (violation == DoneViolation.SchemaAtRetainedVersion)
                    {
                        metadata.SchemaString = metadata.SchemaString!.Replace("\"userkey\"", "\"plantedkey\"");
                    }
                    else
                    {
                        metadata.Configuration!["planted"] = "true";
                    }
                    await RewriteAsync(cell, 13, x => x.Append(new DeltaAction() { MetaData = metadata }));
                    break;
                case DoneViolation.ProtocolAtRetainedVersion:
                    await RewriteAsync(cell, 13, x => x.Append(new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 3 } }));
                    break;
                case DoneViolation.TransactionAtRetainedVersion:
                    await RewriteAsync(cell, 13, x => x.Append(new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = "planted", Version = 1 } }));
                    break;
                case DoneViolation.DomainAtRetainedVersion:
                    await RewriteAsync(cell, 13, x => x.Append(new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "planted", Configuration = "{}" } }));
                    break;
                case DoneViolation.RetainedFileDeleted:
                case DoneViolation.RetainedFileDeletedInMemory:
                    await cell.Table.Raw.Rm("/t/part-0013.parquet");
                    break;
                case DoneViolation.RetainedVectorDeletedInMemory:
                    var retained = await VectorAsync(cell);
                    await RewriteAsync(cell, 13, x => x.Select(a => a.Add != null ? new DeltaAction() { Add = a.Add.WithDeletionVector(retained) } : a));
                    // The vector belongs to the baseline
                    cell.Baseline = await Invariants.CaptureAsync(cell.Table, C, cell.Table.Head);
                    await cell.Table.Raw.Rm($"/t/{retained.GetAbsolutePath()}");
                    detail += retained.GetAbsolutePath();
                    break;
                case DoneViolation.StrayPayloadDeleted:
                    cell.Cleanup.AdvanceHooksForTests.Settled!(new DeletionTarget(Native(cell, "part-0099.parquet"), TargetKind.Payload), new DeleteOutcome(DeleteStatus.Deleted));
                    break;
                case DoneViolation.RetiredFileBack:
                    File.WriteAllBytes(Native(cell, "part-0001.parquet"), new byte[1]);
                    break;
                case DoneViolation.RemovalAfterInvalidationInMemory:
                    cell.Outcome.Invalidate();
                    await cell.Table.Raw.Rm("/t/unreferenced.parquet");
                    break;
                case DoneViolation.ReservationAfterTeardown:
                    File.Copy(NativeLog(cell, done), NativeLog(cell, done.Replace($".{ControlName.DoneSuffix}", $".{ControlName.StartedSuffix}")));
                    break;
                case DoneViolation.SecondDone:
                    File.Copy(NativeLog(cell, done), NativeLog(cell, CleanupRecordKit.Rename(done, NameField.AttemptId, ControlTokens.New())));
                    break;
                case DoneViolation.DoneAtAnotherBoundary:
                    File.Move(NativeLog(cell, done), NativeLog(cell, CleanupRecordKit.Rename(done, NameField.Boundary, $"{8:D20}")));
                    break;
                case DoneViolation.Escape:
                    await cell.Outcome.RunAsync(() => throw new InvalidOperationException("planted"));
                    break;
                case DoneViolation.CancellationWithoutStop:
                    await cell.Outcome.RunAsync(() => throw new OperationCanceledException());
                    break;
                case DoneViolation.KeptTargetGone:
                    var kept = Native(cell, "kept.parquet");
                    File.WriteAllBytes(kept, new byte[1]);
                    cell.Cleanup.AdvanceHooksForTests.Settled!(new DeletionTarget(kept, TargetKind.Payload), DeleteOutcome.Keep(KeepReason.Folder));
                    File.Delete(kept);
                    detail = $"{kept}, kept for Folder, is gone";
                    break;
                case DoneViolation.UnqualifiedKeptTargetGone:
                case DoneViolation.UnqualifiedKeptTargetGoneInMemory:
                    await WriteAsync(cell, "/t/kept.parquet");
                    var executor = new DeletionExecutor(cell.Table.Configuration, cell.Table.Raw);
                    var unqualified = executor.TargetOf("/t/sub/../kept.parquet");
                    cell.Cleanup.AdvanceHooksForTests.Settled!(unqualified, await executor.DeleteAsync(unqualified, false, CancellationToken.None));
                    await cell.Table.Raw.Rm("/t/kept.parquet");
                    break;
            }

            await AssertBrokenAsync(cell, invariant, detail);
        }

        [Theory]
        [InlineData(true, "sub/../part-0013.parquet", nameof(KeepReason.Unqualified))]
        [InlineData(false, "sub/../part-0013.parquet", nameof(KeepReason.Unqualified))]
        [InlineData(true, "sub_$folder$", nameof(KeepReason.Folder))]
        public async Task KeptTargetStillThereKeepsEveryInvariant(bool local, string name, string reasonName)
        {
            var cell = await DoneAsync(local);
            // A folder placeholder the advance never planned
            await WriteAsync(cell, "/t/sub_$folder$");
            var executor = new DeletionExecutor(cell.Table.Configuration, cell.Table.Raw);
            var target = executor.TargetOf($"/t/{name}");
            var outcome = await executor.DeleteAsync(target, false, CancellationToken.None);
            Assert.Equal((DeleteStatus.Kept, Enum.Parse<KeepReason>(reasonName)), (outcome.Status, outcome.Reason));
            // Only a qualified target on local disk has a native key
            Assert.Equal(local && outcome.Reason == KeepReason.Folder, target.Key != $"/t/{name}");
            cell.Cleanup.AdvanceHooksForTests.Settled!(target, outcome);

            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
        }
    }

    public class RetentionMatrixKitRunTests : RetentionMatrixKitBase, IClassFixture<MatrixTemplate>
    {
        public RetentionMatrixKitRunTests(MatrixTemplate template) : base(template)
        {
        }

        public enum UnresolvedViolation
        {
            DifferentVersionBelowC,
            FloorBelowC,
            LedgerReleased,
            RecordRemoved
        }

        [Theory]
        [InlineData(UnresolvedViolation.DifferentVersionBelowC, 1, "version 3 reads")]
        [InlineData(UnresolvedViolation.FloorBelowC, 4, "the floor is -1, not C 12")]
        [InlineData(UnresolvedViolation.LedgerReleased, 4, "the ledger holds no demand")]
        [InlineData(UnresolvedViolation.RecordRemoved, 4, "records are gone: ")]
        public async Task UnresolvedCellFailsOnItsViolation(UnresolvedViolation violation, int invariant, string detail)
        {
            var cell = await StartedAsync();
            cell.Outcome.Ending = CellEnding.Unresolved;
            // The records the job published belong to the baseline
            cell.Baseline = await Invariants.CaptureAsync(cell.Table, C, cell.Table.Head);
            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
            var controls = await cell.Table.ControlsAsync();
            string Named(ControlKind kind) => controls.Single(x => ControlName.TryParse(x, out var name) && name.Kind == kind);
            switch (violation)
            {
                case UnresolvedViolation.DifferentVersionBelowC:
                    Readd(cell, 3, "part-0098.parquet");
                    break;
                case UnresolvedViolation.FloorBelowC:
                    cell.Baseline = cell.Baseline with { Records = Array.Empty<string>() };
                    File.Delete(NativeLog(cell, Named(ControlKind.Started)));
                    break;
                case UnresolvedViolation.LedgerReleased:
                    cell.Outcome.Ledger = new CapacityLedger(() => default);
                    break;
                case UnresolvedViolation.RecordRemoved:
                    var header = Named(ControlKind.Header);
                    File.Delete(NativeLog(cell, header));
                    detail += header;
                    break;
            }

            await AssertBrokenAsync(cell, invariant, detail);
        }

        [Fact]
        public async Task UnresolvedCellKeepsItsRecordsButAReplacedProgress()
        {
            var cell = await StartedAsync(local: false);
            cell.Outcome.Ending = CellEnding.Unresolved;
            await ProcessAsync(cell, Invariants.SlicePaths);
            var first = cell.Cleanup.ActiveForTests!.ProgressName!;
            cell.Baseline = cell.Baseline with { Records = await cell.Table.ControlsAsync() };

            await ProcessAsync(cell, 2 * Invariants.SlicePaths);

            // The first progress went once its successor validated
            Assert.DoesNotContain(first, await cell.Table.ControlsAsync());
            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
        }

        [Fact]
        public async Task RestartRepeatingTheInterruptedSliceKeepsEveryInvariant()
        {
            var cell = await StartedAsync();
            await ProcessAsync(cell, Invariants.SlicePaths);
            // The next slice deletes without its progress
            cell.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) => kind == ControlKind.Progress ? throw new IOException("injected") : Task.CompletedTask;
            var failed = await cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(cell.Cleanup, _kit.DeletionContext(cell.Table.Head, cell.Ledger)));
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.Transient), (failed!.Kind, failed.Reason));

            await RestartToDoneAsync(cell);

            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
            Assert.Equal(Invariants.SlicePaths, cell.Outcome.Settled.Count(x => x.Run == 1 && x.Target.Kind == TargetKind.Payload && x.Outcome.Status == DeleteStatus.Absent));
        }

        [Fact]
        public async Task RestartRepeatingOnePathMoreThanASliceBreaksTheRepeatInvariant()
        {
            var cell = await StartedAsync();
            // One slice a path longer than the matrix's, then its progress lost
            cell.Cleanup.Settings = cell.Cleanup.Settings with { MaxPaths = Invariants.SlicePaths + 1 };
            await ProcessAsync(cell, Invariants.SlicePaths + 1);
            File.Delete(NativeLog(cell, (await cell.Table.ControlsAsync()).Single(x => ControlName.TryParse(x, out var name) && name.Kind == ControlKind.Progress)));

            await RestartToDoneAsync(cell);

            await AssertBrokenAsync(cell, 5, $"run 1 repeated {Invariants.SlicePaths + 1} Payload deletes");
        }

        [Fact]
        public async Task RestartRepeatingOneLogDeleteKeepsEveryInvariant()
        {
            var cell = await LogDeletesBackAsync(1);

            await Invariants.AssertAsync(cell.Table, cell.Baseline, cell.Outcome);
        }

        [Fact]
        public async Task RestartRepeatingTwoLogDeletesBreaksTheRepeatInvariant()
        {
            var cell = await LogDeletesBackAsync(2);

            await AssertBrokenAsync(cell, 5, "run 1 repeated 2 Log deletes");
        }

        // One log turn, the oldest of its deleted names back, then a restart to done
        private async Task<Cell> LogDeletesBackAsync(int names)
        {
            var cell = await StartedAsync();
            var log = Path.Combine(Invariants.NativeTable(cell.Table), "_delta_log");
            var before = Directory.GetFiles(log).ToDictionary(x => x, File.ReadAllBytes);
            Assert.Equal(OutcomeKind.Completed, (await cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(cell.Cleanup, _kit.DeletionContext(cell.Table.Head, cell.Ledger))))!.Kind);
            var deleted = before.Where(x => !File.Exists(x.Key)).OrderBy(x => x.Key, StringComparer.Ordinal).ToArray();
            Assert.Equal(Invariants.SlicePaths, deleted.Length);
            foreach (var (path, bytes) in deleted.Take(names))
            {
                File.WriteAllBytes(path, bytes);
            }

            await RestartToDoneAsync(cell);
            return cell;
        }

        [Theory]
        [InlineData("part-0013.parquet", "The table lacks files its retained versions reference: PART-0013.PARQUET")]
        [InlineData("_delta_log/00000000000000000013.json", "Version 13 at or above C does not read before the cell")]
        public async Task CaptureRefusesATableItCannotHold(string missing, string message)
        {
            var table = await _template.CloneAsync(_kit);
            File.Delete(Path.Combine(Invariants.NativeTable(table), missing));

            var failure = await Assert.ThrowsAnyAsync<XunitException>(() => Invariants.CaptureAsync(table, C, table.Head));

            Assert.Contains(message, failure.Message);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ClonesPlanAndCompleteLikeAFreshChurnedTable(bool local)
        {
            var fresh = await _kit.ChurnedAsync(local);
            await AdvanceExecutionKit.PayloadFilesAsync(fresh);
            var expected = await TraceAsync(fresh);

            // A second clone after the first completed: the template is untouched
            Assert.Equal(expected, await TraceAsync(await _template.CloneAsync(_kit, local)));
            Assert.Equal(expected, await TraceAsync(await _template.CloneAsync(_kit, local)));
        }

        // The advance's boundary and totals, its settled deletes, its turns and what the table holds after it
        private async Task<string> TraceAsync(ExecutionTable table)
        {
            var events = new ConcurrentQueue<string>();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            cleanup.AdvanceHooksForTests.Settled = (target, outcome) =>
            {
                var name = Path.GetFileName(target.Key);
                events.Enqueue($"{target.Kind} {(ControlName.TryParse(name, out var control) ? control.Kind : name)} {outcome.Status}");
            };
            var ledger = new CapacityLedger(() => default);
            var started = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal(OutcomeKind.Completed, started.Kind);
            var active = cleanup.ActiveForTests!;
            var record = active.Started;
            var plan = $"C {active.C} O {record.O} A {record.A} head {record.Head} first {record.FirstJson} anchor {record.Anchor} totals {record.Totals}";
            var turns = await _kit.ExecuteAsync(cleanup, table, ledger);
            var left = (await Invariants.View(table).Ls("/t/", recurse: true)).Where(x => !x.Path.IsFolder).Select(x => x.Name)
                .Select(x => ControlName.TryParse(x, out var control) ? $"{control.Kind} at {control.Boundary}" : x).Order(StringComparer.Ordinal);
            return string.Join("\n", new[] { plan, $"{turns} turns" }.Concat(events.Order(StringComparer.Ordinal)).Concat(left));
        }

        [Fact]
        public async Task RowsAtReadEachVersionThroughItsDeletionVector()
        {
            var table = await _kit.ReadableAsync(_kit.NewRoot());
            try
            {
                // The last data file again with its only row deleted
                var (name, id) = DeletionVectorWriter.GenerateDestination();
                var (_, size) = await DeletionVectorWriter.WriteDeletionVector(table.Raw, "t", name, RoaringBitmapArray.Create(new long[] { 0 }));
                var last = (await DeltaTransactionReader.ReadTable(table.Raw, "t", 14))!.AddFiles.Single();
                var vector = new DeletionVector() { StorageType = "u", PathOrInlineDv = id, Offset = 1, SizeInBytes = size, Cardinality = 1 };
                await DeltaTestKit.WriteOwnCommit(table.Raw, "t", 15, 0, BoundarySelectorTests.Old(15),
                    DeltaLakeCheckpointBuildTests.Remove(last.Path!, BoundarySelectorTests.Old(15)), new DeltaAction() { Add = last.WithDeletionVector(vector) });

                for (long version = 1; version <= 14; version++)
                {
                    var rows = await RetainedRows.RowsAtAsync(table.Raw, version);
                    Assert.Equal(version, rows.Version);
                    Assert.Equal(new[] { $"{version}|user-{version}" }, rows.Rows);
                    Assert.Contains("userkey", rows.Schema);
                }
                var deleted = await RetainedRows.RowsAtAsync(table.Raw, 15);
                Assert.Equal(15, deleted.Version);
                Assert.Empty(deleted.Rows);
            }
            finally
            {
                table.Raw.Dispose();
            }
        }
    }

    public class LandsLaterTests : IClassFixture<MatrixTemplate>, IDisposable
    {
        private readonly MatrixTemplate _template;
        private readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();

        public LandsLaterTests(MatrixTemplate template)
        {
            _template = template;
        }

        public void Dispose()
        {
            _kit.Dispose();
        }

        [Theory]
        [InlineData(nameof(ControlKind.Part), true, false)]
        [InlineData(nameof(ControlKind.Header), true, false)]
        [InlineData(nameof(ControlKind.Started), true, false)]
        [InlineData(nameof(ControlKind.LogsComplete), true, false)]
        [InlineData(nameof(ControlKind.Progress), true, false)]
        [InlineData(nameof(ControlKind.Done), true, false)]
        [InlineData(nameof(ControlKind.Part), false, false)]
        [InlineData(nameof(ControlKind.Header), false, false)]
        [InlineData(nameof(ControlKind.Started), false, false)]
        [InlineData(nameof(ControlKind.LogsComplete), false, false)]
        [InlineData(nameof(ControlKind.Progress), false, false)]
        [InlineData(nameof(ControlKind.Done), false, false)]
        [InlineData(nameof(ControlKind.Progress), true, true)]
        [InlineData(nameof(ControlKind.Progress), false, true)]
        public async Task ReleasePlantsThePublishedRecordUnderAttemptOnesName(string kindName, bool local, bool torn)
        {
            var kind = Enum.Parse<ControlKind>(kindName);
            var table = await _template.CloneAsync(_kit, local);
            var late = new LandsLater(table, kind, torn);
            // Every record seen at each publication, step and the end
            var seen = new Dictionary<string, byte[]>();
            async Task ObserveAsync()
            {
                foreach (var (name, bytes) in await table.SnapshotAsync())
                {
                    seen[name] = bytes;
                }
            }
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            cleanup.Settings = cleanup.Settings with { MaxPaths = Invariants.SlicePaths };
            cleanup.AdvanceHooksForTests.BeforePublish = async (published, name) =>
            {
                await ObserveAsync();
                await late.Hook(published, name);
            };
            cleanup.AdvanceHooksForTests.Step = _ => ObserveAsync();
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            await _kit.ExecuteAsync(cleanup, table, ledger);
            await ObserveAsync();

            var released = await late.ReleaseAsync();

            // The same record as the attempt that published, under attempt one's name, which never landed before
            Assert.Equal(late.First, released);
            Assert.True(ControlName.TryParse(released, out var first));
            Assert.True(ControlName.TryParse(late.Published!, out var published));
            Assert.Equal((kind, published.Boundary, published.AdvanceId, published.Digest, published.Ordinal), (first.Kind, first.Boundary, first.AdvanceId, first.Digest, first.Ordinal));
            Assert.NotEqual(published.AttemptId, first.AttemptId);
            Assert.DoesNotContain(released, seen.Keys);
            var expected = seen[late.Published!];
            Assert.Equal(torn ? expected[..^1] : expected, await DeltaTestKit.ReadBytes(table.Raw, table.LogPath(released)));
        }
    }
}
