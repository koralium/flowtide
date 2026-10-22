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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AdvanceExecutionTests
    {
        private static readonly TimeSpan s_payloadWait = TimeSpan.FromSeconds(30);

        // A started advance past its log phase whose payload files exist, adopted by a restarted cleanup; eleven paths in parts of the given size
        private async Task<Rig> PayloadAdvanceAsync(int partPaths = 4, bool local = true)
        {
            var table = await _kit.ChurnedAsync(local);
            for (long version = 0; version <= table.Head; version++)
            {
                await using var stream = await table.Raw.OpenWrite($"/t/part-{version:D4}.parquet");
                await stream.WriteAsync(new byte[1]);
            }
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths, logsComplete: true);
            var rig = await RestartPayloadAsync(table);
            Assert.Equal(11, rig.Active.Started.Totals.Emitted);
            return rig;
        }

        // A new cleanup and ledger adopting the advance from its records, as a restarted process does
        private async Task<Rig> RestartPayloadAsync(ExecutionTable table, Action<DeltaTableCleanup>? configure = null)
        {
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var cleanup = AdvanceExecutionKit.Cleanup(table, logs);
            configure?.Invoke(cleanup);
            var ledger = new CapacityLedger(() => default);
            var active = await _kit.AdoptAsync(cleanup, table, ledger);
            Assert.Equal(AdvancePhase.Payload, active.Phase);
            return new Rig() { Table = table, Cleanup = cleanup, Active = active, Ledger = ledger, Logs = logs };
        }

        // The planned paths in cursor order, read from the advance's parts
        private static async Task<string[]> PlannedPayloadAsync(Rig rig)
        {
            var association = rig.Active.Association;
            var descriptors = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(await DeltaTestKit.ReadBytes(rig.Table.Raw, rig.Table.LogPath(association.HeaderName)), out _, descriptors));
            var paths = new List<string>();
            foreach (var descriptor in descriptors)
            {
                var part = await DeltaTestKit.ReadBytes(rig.Table.Raw, rig.Table.LogPath(ControlName.Part(association.C, association.AdvanceId, descriptor.Ordinal, descriptor.AttemptId)));
                paths.AddRange(PathsOf(part));
            }
            Assert.Equal(Enumerable.Range(0, 11).Select(x => $"/t/part-{x:D4}.parquet"), paths);
            return paths.ToArray();

            static IEnumerable<string> PathsOf(byte[] part)
            {
                var paths = new List<string>();
                for (var offset = ControlRecords.FirstPathOffset; offset < part.Length - ControlRecords.EnvelopeBytes + 12;)
                {
                    var length = (int)BinaryPrimitives.ReadUInt32LittleEndian(part.AsSpan(offset));
                    paths.Add(Encoding.UTF8.GetString(part, offset + 4, length));
                    offset += 4 + length;
                }
                return paths;
            }
        }

        // A key's or path's file name, native or object
        private static string PayloadName(string key) => key.Substring(key.LastIndexOfAny(new[] { '/', '\\' }) + 1);

        // The highest valid progress sequence listed
        private static async Task<ProgressRecord?> NewestPayloadProgressAsync(ExecutionTable table)
        {
            ProgressRecord? newest = null;
            foreach (var name in await table.ControlsAsync())
            {
                if (!ControlName.TryParse(name, out var parsed) || parsed.Kind != ControlKind.Progress)
                {
                    continue;
                }
                var bytes = await DeltaTestKit.ReadBytes(table.Raw, table.LogPath(name));
                if (ControlRecords.Decode(bytes, out ProgressRecord? record) == RecordStatus.Valid && (newest == null || record!.Sequence > newest.Sequence))
                {
                    newest = record;
                }
            }
            return newest;
        }

        private static async Task<bool> PayloadDoneListedAsync(ExecutionTable table) => (await table.ControlsAsync()).Any(x => x.EndsWith(ControlName.DoneSuffix, StringComparison.Ordinal));

        // A copy of a progress record under a fresh attempt id
        private static async Task<string> PlantProgressAsync(ExecutionTable table, ProgressRecord record, Func<byte[], byte[]>? tamper = null)
        {
            var association = record.Association;
            var name = ControlName.Group(ControlKind.Progress, association.C, association.AdvanceId, association.HeaderSha256, ControlTokens.New(), record.Sequence);
            var bytes = ControlRecords.Encode(record);
            await CleanupRecordKit.PlantAsync(table.Raw, "t", name, tamper?.Invoke(bytes) ?? bytes);
            return name;
        }

        private static async Task<List<PartDescriptor>> DescriptorsAsync(Rig rig)
        {
            var descriptors = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(await DeltaTestKit.ReadBytes(rig.Table.Raw, rig.Table.LogPath(rig.Active.Association.HeaderName)), out _, descriptors));
            return descriptors;
        }

        // The advance's header, started and logs-complete replaced by a consistent chain whose header names these parts
        private static async Task<Association> RechainAsync(Rig rig, IReadOnlyList<PartDescriptor> descriptors)
        {
            var table = rig.Table;
            var association = rig.Active.Association;
            var started = rig.Active.Started;
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(await DeltaTestKit.ReadBytes(table.Raw, table.LogPath(association.HeaderName)), out var header, null));
            var totals = new RecordTotals(descriptors.Sum(x => (long)x.Paths), descriptors.Count, descriptors.Sum(x => (long)x.Bytes));
            var rechained = header! with { Totals = totals };
            var bytes = new byte[ControlRecords.HeaderLength(rechained, descriptors.Count)];
            ControlRecords.WriteHeader(bytes, rechained, descriptors, out var sha);
            var chain = new Association(association.C, association.AdvanceId, ControlName.Header(association.C, association.AdvanceId, ControlTokens.New()), sha);
            foreach (var name in await table.ControlsAsync())
            {
                if (ControlName.TryParse(name, out var parsed) && parsed.Kind is ControlKind.Header or ControlKind.Started or ControlKind.LogsComplete)
                {
                    await table.Raw.Rm(table.LogPath(name));
                }
            }
            await CleanupRecordKit.PlantAsync(table.Raw, "t", chain.HeaderName, bytes);
            await CleanupRecordKit.PlantAsync(table.Raw, "t", ControlName.Group(ControlKind.Started, chain.C, chain.AdvanceId, chain.HeaderSha256, ControlTokens.New()),
                ControlRecords.Encode(started with { Association = chain, Totals = totals }));
            await CleanupRecordKit.PlantAsync(table.Raw, "t", ControlName.Group(ControlKind.LogsComplete, chain.C, chain.AdvanceId, chain.HeaderSha256, ControlTokens.New()),
                ControlRecords.Encode(new LogsCompleteRecord(chain, started.TableId, started.O)));
            return chain;
        }

        // A checkpoint build at the version, as a worker's build of the table runs it
        private async Task<BuildResult> BuildAtAsync(ExecutionTable table, long version)
        {
            var context = _kit.DeletionContext(table.Head);
            return await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
            {
                Storage = table.Raw,
                Table = "t",
                Scratch = context.Scratch,
                Memory = new JobMemory(),
                Clock = new FakeTimeProvider(s_now)
            }, new BuildRequest() { Version = version, Root = RootPolicy.Auto, KnownUnsupported = new HashSet<ObjectIdentity>() });
        }

        // A restarted advance job finds the group unresolvable: it declines with every record, the reservation and the demand kept apart
        private async Task AssertResumeSuspendsAsync(ExecutionTable table, long c)
        {
            var records = await table.SnapshotAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            var context = _kit.AdvanceContext(table.Head, ledger);
            var result = await cleanup.RunAsync(JobKind.Advance, context);
            await context.SettleAsync();

            AssertDeclined(result, MaintenanceReason.ManifestSuspended);
            Assert.Null(cleanup.ActiveForTests);
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.Equal(c, await AdvanceExecutionKit.FloorAsync(table));
            Assert.Equal(default, ledger.Current);
            Assert.NotEqual(default, ledger.Total(CapacityState.Suspended));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task PayloadPhaseDeletesEachPartThenPublishesDoneAndTearsDown(bool local)
        {
            var rig = await PayloadAdvanceAsync(local: local);
            var planned = await PlannedPayloadAsync(rig);
            var events = new ConcurrentQueue<string>();
            rig.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) =>
            {
                events.Enqueue($"Publish {kind}");
                return Task.CompletedTask;
            };
            var progress = new List<(RecordCursor, long)>();

            for (int turn = 0; turn < 3; turn++)
            {
                Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
                if (rig.Active.Phase != AdvancePhase.Done)
                {
                    var newest = (await NewestPayloadProgressAsync(rig.Table))!;
                    progress.Add((newest.Next, newest.Processed));
                    Assert.False(await PayloadDoneListedAsync(rig.Table));
                }
            }

            // A canonical cursor at each part's end, then the terminal progress before the done
            Assert.Equal(new[] { (new RecordCursor(1, 0, ControlRecords.FirstPathOffset), 4L), (new RecordCursor(2, 0, ControlRecords.FirstPathOffset), 8L) }, progress);
            Assert.Equal(new[] { "Publish Progress", "Publish Progress", "Publish Progress", "Publish Done" }, events);
            Assert.Equal(AdvancePhase.Done, rig.Active.Phase);
            Assert.Null(rig.Cleanup.ActiveForTests);
            // Only the done stays, describing every path processed
            var done = Assert.Single(await rig.Table.ControlsAsync());
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(await DeltaTestKit.ReadBytes(rig.Table.Raw, rig.Table.LogPath(done)), out DoneRecord? record));
            Assert.Equal((rig.Active.Association, new RecordCursor(3, 0, 0), 11L), (record!.Association, record.Final, record.Processed));
            foreach (var path in planned)
            {
                Assert.False(await rig.Table.Raw.Exists(path), path);
            }
            for (long version = 11; version <= rig.Table.Head; version++)
            {
                Assert.True(await rig.Table.Raw.Exists($"/t/part-{version:D4}.parquet"));
            }
            Assert.Equal(ControlFence.NoReservation, await AdvanceExecutionKit.FloorAsync(rig.Table));
            Assert.Equal((default(CapacityDemand), default(CapacityDemand), default(CapacityDemand)), (rig.Ledger.Current, rig.Ledger.Total(CapacityState.Paused), rig.Ledger.Total(CapacityState.Suspended)));
        }

        [Fact]
        public async Task UnpublishedDoneIsPublishedAgainAtTheNextTurnsGate()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var attempts = 0;
            rig.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) => kind == ControlKind.Done ? throw new IOException($"Done attempt {Interlocked.Increment(ref attempts)} fails") : Task.CompletedTask;

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.PublishFailed);

            // Every path processed and recorded, the advance still in its payload phase
            Assert.Equal(ControlPublisher.MaxAttempts, attempts);
            Assert.Equal((new RecordCursor(1, 0, 0), 11L), ((await NewestPayloadProgressAsync(rig.Table))!.Next, rig.Active.Processed));
            Assert.Equal(AdvancePhase.Payload, rig.Active.Phase);
            Assert.False(await PayloadDoneListedAsync(rig.Table));
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);

            rig.Cleanup.AdvanceHooksForTests.BeforePublish = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            Assert.Equal(AdvancePhase.Done, rig.Active.Phase);
            Assert.EndsWith(ControlName.DoneSuffix, Assert.Single(await rig.Table.ControlsAsync()), StringComparison.Ordinal);
        }

        [Fact]
        public async Task UnpublishedDoneWaitsForAPassedGate()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            rig.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) => kind == ControlKind.Done ? throw new IOException("injected") : Task.CompletedTask;
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.PublishFailed);
            rig.Cleanup.AdvanceHooksForTests.BeforePublish = null;

            // The advance's authority changes before the next turn
            var logsComplete = (await rig.Table.ControlsAsync()).Single(x => ControlName.TryParse(x, out var name) && name.Kind == ControlKind.LogsComplete);
            await rig.Table.Raw.Rm(rig.Table.LogPath(logsComplete));

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.ManifestSuspended);

            // No done and no teardown: the reservation stays
            Assert.False(await PayloadDoneListedAsync(rig.Table));
            Assert.Equal((AdvancePhase.Payload, AdvanceState.Suspended), (rig.Active.Phase, rig.Active.State));
            Assert.Equal(rig.Active.C, await AdvanceExecutionKit.FloorAsync(rig.Table));
        }

        [Fact]
        public async Task DoneAfterADisabledTurnLeavesNoLease()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var disabled = true;
            rig.Cleanup.Settings = rig.Cleanup.Settings with { DeletesDisabled = () => Volatile.Read(ref disabled) };
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeletesDisabled);
            Assert.Equal(CapacityState.Paused, rig.Active.Lease!.State);
            Volatile.Write(ref disabled, false);

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            // The teardown released the lease, and nothing restored it
            Assert.Equal(AdvancePhase.Done, rig.Active.Phase);
            Assert.Equal((default(CapacityDemand), default(CapacityDemand), default(CapacityDemand)), (rig.Ledger.Current, rig.Ledger.Total(CapacityState.Paused), rig.Ledger.Total(CapacityState.Suspended)));
        }

        [Fact]
        public async Task AdvanceWithoutPayloadGoesStraightToDone()
        {
            var rig = await PayloadAdvanceAsync(local: false);
            var planned = await PlannedPayloadAsync(rig);
            // An advance whose header names no part
            foreach (var name in await rig.Table.ControlsAsync())
            {
                if (ControlName.TryParse(name, out var parsed) && parsed.Kind == ControlKind.Part)
                {
                    await rig.Table.Raw.Rm(rig.Table.LogPath(name));
                }
            }
            await RechainAsync(rig, Array.Empty<PartDescriptor>());
            rig = await RestartPayloadAsync(rig.Table);
            Assert.Equal((new RecordCursor(0, 0, 0), 0L), (rig.Active.Cursor, rig.Active.Processed));
            var settled = 0;
            rig.Cleanup.AdvanceHooksForTests.Settled = (target, _) => Interlocked.Add(ref settled, target.Kind == TargetKind.Payload ? 1 : 0);

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            // Done at the terminal position of no parts, nothing deleted, only the done kept
            Assert.Equal((AdvancePhase.Done, 0), (rig.Active.Phase, settled));
            var done = Assert.Single(await rig.Table.ControlsAsync());
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(await DeltaTestKit.ReadBytes(rig.Table.Raw, rig.Table.LogPath(done)), out DoneRecord? record));
            Assert.Equal((new RecordCursor(0, 0, 0), 0L, rig.Active.Association), (record!.Final, record.Processed, record.Association));
            foreach (var path in planned)
            {
                Assert.True(await rig.Table.Raw.Exists(path), path);
            }
        }

        [Fact]
        public async Task PayloadTurnWhoseKeptExclusionsDoNotFitDeclinesBeforeAnyDelete()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var planned = await PlannedPayloadAsync(rig);
            var records = await rig.Table.SnapshotAsync();
            rig.Active.Exclusions = LongExclusions(10_000);
            var settled = 0;
            rig.Cleanup.AdvanceHooksForTests.Settled = (target, _) => Interlocked.Add(ref settled, target.Kind == TargetKind.Payload ? 1 : 0);
            // Room for the turn's own work, not for the kept set
            var memory = new JobMemory(managedCap: 16 << 20);

            var result = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger, memory: memory));

            AssertDeclined(result, MaintenanceReason.ManagedMemoryCap);
            Assert.Equal((AdvanceState.Runnable, CapacityState.Runnable, 0L, 0), (rig.Active.State, rig.Active.Lease!.State, rig.Active.Processed, settled));
            Assert.Equal(records, await rig.Table.SnapshotAsync());
            foreach (var path in planned)
            {
                Assert.True(await rig.Table.Raw.Exists(path), path);
            }
            // A turn with the job's full allowance goes on
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Equal((AdvancePhase.Done, 11), (rig.Active.Phase, settled));
        }

        [Fact]
        public async Task PayloadTurnChargesItsKeptExclusionsUntilItEnds()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var kept = LongExclusions(10_000);
            rig.Active.Exclusions = kept;
            var memory = new JobMemory();
            var charged = new ConcurrentQueue<long>();
            rig.Cleanup.AdvanceHooksForTests.BeforeDelete = target =>
            {
                if (target.Kind == TargetKind.Payload)
                {
                    charged.Enqueue(memory.Managed);
                }
                return Task.CompletedTask;
            };

            // The kit's turn finds the ledger back at zero
            var result = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger, memory: memory));

            Assert.Equal((OutcomeKind.Completed, AdvancePhase.Done, 11), (result.Kind, rig.Active.Phase, charged.Count));
            Assert.All(charged, x => Assert.InRange(x, kept.Bytes, memory.ManagedCap));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PayloadTurnReleasesItsKeptExclusionsWhenItsSliceThrowsOrStops(bool stops)
        {
            var rig = await StartedAsync(logsComplete: true);
            var kept = LongExclusions(10_000);
            rig.Active.Exclusions = kept;
            using var worker = new CancellationTokenSource();
            var memory = new JobMemory();
            long held = -1;
            rig.Cleanup.PayloadPhaseForTests = (_, _, _) =>
            {
                held = memory.Managed;
                if (stops)
                {
                    worker.Cancel();
                    return Task.FromCanceled<SliceResult>(worker.Token);
                }
                return Task.FromException<SliceResult>(new IOException("injected"));
            };
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger, stop: worker.Token, memory: memory);

            if (stops)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AdvanceExecutionKit.TurnAsync(rig.Cleanup, context));
            }
            else
            {
                var result = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, context);
                Assert.Equal((OutcomeKind.Failed, MaintenanceReason.Unexpected), (result.Kind, result.Reason));
            }

            Assert.Equal((kept.Bytes, 0L), (held, memory.Managed));
        }

        [Fact]
        public async Task FailedTeardownLeavesTheDoneForTheNextRecovery()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            rig.Cleanup.AdvanceHooksForTests.Step = step => step == AdvanceSteps.Teardown ? throw new IOException("injected") : Task.CompletedTask;

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            // Done, out of the turns and without demand, with its group's records still listed
            Assert.Equal(AdvancePhase.Done, rig.Active.Phase);
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.Equal(default, rig.Ledger.Current);
            var controls = await rig.Table.ControlsAsync();
            Assert.Contains(controls, x => x.EndsWith(ControlName.DoneSuffix, StringComparison.Ordinal));
            Assert.Contains(controls, x => x.EndsWith(ControlName.StartedSuffix, StringComparison.Ordinal));
            // A later recovery releases it by its done and keeps only the done
            var context = _kit.AdvanceContext(rig.Table.Head);
            var log = await AdvancePreparation.ListAsync(rig.Table.Configuration, rig.Table.Metered, context.Head, context);
            var recovered = await AdvanceRecovery.RecoverAsync(rig.Table.Configuration, rig.Table.Metered, log, rig.Active.Evidence, context);
            await context.SettleAsync();
            Assert.Empty(recovered.Unresolved);
            Assert.EndsWith(ControlName.DoneSuffix, Assert.Single(await rig.Table.ControlsAsync()), StringComparison.Ordinal);
        }

        [Fact]
        public async Task FailedDonePublicationRetriesWithTheAdvanceKept()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger);
            // The done's scratch folder is gone
            rig.Cleanup.AdvanceHooksForTests.Step = step =>
            {
                if (step == AdvanceSteps.Done)
                {
                    Directory.Delete(context.Scratch.DirectoryPath, true);
                }
                return Task.CompletedTask;
            };

            AssertDeclined(await AdvanceExecutionKit.TurnAsync(rig.Cleanup, context), MaintenanceReason.Transient);

            // Kept at the terminal position with its demand, done on the next turn
            Assert.Equal(AdvancePhase.Payload, rig.Active.Phase);
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
            Assert.Equal(rig.Active.Lease!.Demand, rig.Ledger.Current);
            Assert.False(await PayloadDoneListedAsync(rig.Table));
            rig.Cleanup.AdvanceHooksForTests.Step = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.True(await PayloadDoneListedAsync(rig.Table));
            Assert.Equal(default, rig.Ledger.Current);
        }

        [Fact]
        public async Task WorkerStopDuringTheTeardownStopsTheTurn()
        {
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            using var worker = new CancellationTokenSource();
            rig.Cleanup.AdvanceHooksForTests.Step = step =>
            {
                if (step == AdvanceSteps.Teardown)
                {
                    worker.Cancel();
                    worker.Token.ThrowIfCancellationRequested();
                }
                return Task.CompletedTask;
            };
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger, stop: worker.Token);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AdvanceExecutionKit.TurnAsync(rig.Cleanup, context));

            // Done without demand, its records left for a later recovery
            Assert.Equal(AdvancePhase.Done, rig.Active.Phase);
            Assert.Equal(default, rig.Ledger.Current);
            Assert.True(await PayloadDoneListedAsync(rig.Table));
        }

        [Fact]
        public async Task DurablePayloadFailureNeverAdvancesCursor()
        {
            // One part of eleven paths, four deletes outstanding
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var planned = await PlannedPayloadAsync(rig);
            Assert.Equal(DeletionRun.MaxOutstanding, rig.Cleanup.Settings.MaxOutstanding);
            const int k = 2;
            var failing = PayloadName(planned[k]);
            var failures = 0;
            rig.Cleanup.AdvanceHooksForTests.BeforeDelete = target => PayloadName(target.Key) == failing ? throw new IOException("injected") : Task.CompletedTask;
            rig.Cleanup.AdvanceHooksForTests.Settled = (target, outcome) =>
            {
                if (PayloadName(target.Key) == failing && outcome.Status == DeleteStatus.Failed)
                {
                    Interlocked.Increment(ref failures);
                }
            };

            for (int slice = 1; slice <= AdvanceExecution.MaxDurableFailures; slice++)
            {
                AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeleteFailed);

                // Retried each slice and never passed: no progress past it and no done
                Assert.Equal(slice, failures);
                var newest = await NewestPayloadProgressAsync(rig.Table);
                Assert.True(newest != null && newest.Processed <= k, $"{newest?.Next} {newest?.Processed}");
                Assert.Equal(k, rig.Active.Processed);
                Assert.False(await PayloadDoneListedAsync(rig.Table));
                Assert.True(await rig.Table.Raw.Exists(planned[k]));
            }
            Assert.Equal(AdvanceState.Suspended, rig.Active.State);
            Assert.Equal(new RecordCursor(0, k, (await NewestPayloadProgressAsync(rig.Table))!.Next.Byte), rig.Active.Cursor);
        }

        [Fact]
        public async Task CrashAfterFailedPayloadBeforeProgress()
        {
            // Parts of six and five paths
            var rig = await PayloadAdvanceAsync(partPaths: 6);
            var planned = await PlannedPayloadAsync(rig);
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var earlier = (await NewestPayloadProgressAsync(rig.Table))!;
            Assert.Equal((new RecordCursor(1, 0, ControlRecords.FirstPathOffset), 6L), (earlier.Next, earlier.Processed));

            // The second part's third target fails once its fourth and fifth settled, and every progress attempt fails
            var failing = PayloadName(planned[8]);
            var behind = new[] { PayloadName(planned[9]), PayloadName(planned[10]) };
            var settledBehind = 0;
            var bothSettled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var hooks = rig.Cleanup.AdvanceHooksForTests;
            hooks.Settled = (target, outcome) =>
            {
                if (behind.Contains(PayloadName(target.Key)) && outcome.Done && Interlocked.Increment(ref settledBehind) == behind.Length)
                {
                    bothSettled.TrySetResult();
                }
            };
            hooks.BeforeDelete = async target =>
            {
                if (PayloadName(target.Key) == failing)
                {
                    await bothSettled.Task.WaitAsync(s_payloadWait);
                    throw new IOException("injected");
                }
            };
            var attempts = 0;
            hooks.BeforePublish = (kind, _) => kind == ControlKind.Progress ? throw new IOException($"Progress attempt {Interlocked.Increment(ref attempts)} fails") : Task.CompletedTask;

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.Transient);

            Assert.Equal(ControlPublisher.MaxAttempts, attempts);
            Assert.True(await rig.Table.Raw.Exists(planned[8]));
            Assert.False(await rig.Table.Raw.Exists(planned[9]));
            Assert.False(await rig.Table.Raw.Exists(planned[10]));
            Assert.Equal(earlier, await NewestPayloadProgressAsync(rig.Table));
            Assert.Equal(6, rig.Active.Processed);
            Assert.False(await PayloadDoneListedAsync(rig.Table));
            // The failed publication's sequence is never reused
            Assert.Equal(earlier.Sequence + 1, rig.Active.Sequence);

            // A restarted cleanup resumes from the earlier progress
            var restarted = await RestartPayloadAsync(rig.Table);
            Assert.Equal((earlier.Next, earlier.Processed), (restarted.Active.Cursor, restarted.Active.Processed));
            var events = new ConcurrentQueue<string>();
            restarted.Cleanup.AdvanceHooksForTests.Settled = (target, outcome) => events.Enqueue($"{target.Kind} {PayloadName(target.Key)} {outcome.Status}");
            restarted.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) =>
            {
                events.Enqueue($"Publish {kind}");
                return Task.CompletedTask;
            };

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(restarted)).Kind);

            // Only the second part again, its failed target deleted before the done
            var journal = events.ToList();
            Assert.Equal(planned.Skip(6).Select(PayloadName).Order(StringComparer.Ordinal),
                journal.Where(x => x.StartsWith("Payload ", StringComparison.Ordinal)).Select(x => x.Split(' ')[1]).Order(StringComparer.Ordinal));
            var deleted = journal.IndexOf($"Payload {failing} Deleted");
            Assert.True(deleted >= 0 && deleted < journal.IndexOf("Publish Done"), string.Join("\n", journal));
            Assert.Equal(AdvancePhase.Done, restarted.Active.Phase);
            Assert.EndsWith(ControlName.DoneSuffix, Assert.Single(await rig.Table.ControlsAsync()), StringComparison.Ordinal);
        }

        [Fact]
        public async Task PermanentPayloadFailureKeepsAuthorityAcrossRestarts()
        {
            var rig = await PayloadAdvanceAsync();
            var planned = await PlannedPayloadAsync(rig);
            var c = rig.Active.C;
            // The second part's first path
            var failing = PayloadName(planned[4]);
            Func<DeletionTarget, Task> fault = target => PayloadName(target.Key) == failing ? throw new IOException("injected") : Task.CompletedTask;
            Dictionary<string, byte[]>? records = null;

            for (int run = 0; run < 3; run++)
            {
                if (run > 0)
                {
                    rig = await RestartPayloadAsync(rig.Table);
                }
                rig.Cleanup.AdvanceHooksForTests.BeforeDelete = fault;
                for (int turn = 0; rig.Cleanup.ActiveForTests != null; turn++)
                {
                    Assert.True(turn < 4, $"Still active after {turn} turns");
                    await TurnAsync(rig);
                }

                // Suspended at the failed path once per run, its demand reported apart, its records and reservation kept
                Assert.Equal((AdvanceState.Suspended, 4L), (rig.Active.State, rig.Active.Processed));
                Assert.Equal(CapacityState.Suspended, rig.Active.Lease!.State);
                Assert.Equal(default, rig.Ledger.Current);
                Assert.NotEqual(default, rig.Ledger.Total(CapacityState.Suspended));
                Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.DeleteFailed)));
                records ??= await rig.Table.SnapshotAsync();
                Assert.Equal(records, await rig.Table.SnapshotAsync());
                Assert.Equal(c, await AdvanceExecutionKit.FloorAsync(rig.Table));
                var below = await BuildAtAsync(rig.Table, c - 1);
                Assert.Equal((BuildStatus.Declined, MaintenanceReason.CleanupGate), (below.Status, below.Reason));
                Assert.True(await rig.Table.Raw.Exists(planned[4]));
                Assert.False(await PayloadDoneListedAsync(rig.Table));
            }

            // With the fault gone, a restart completes, tears down and keeps its done
            rig = await RestartPayloadAsync(rig.Table);
            for (int turn = 0; rig.Cleanup.ActiveForTests != null; turn++)
            {
                Assert.True(turn < 3, $"Still active after {turn} turns");
                Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            }
            Assert.Equal(AdvancePhase.Done, rig.Active.Phase);
            Assert.EndsWith(ControlName.DoneSuffix, Assert.Single(await rig.Table.ControlsAsync()), StringComparison.Ordinal);
            foreach (var path in planned)
            {
                Assert.False(await rig.Table.Raw.Exists(path), path);
            }
            Assert.Equal(ControlFence.NoReservation, await AdvanceExecutionKit.FloorAsync(rig.Table));
            Assert.Equal((default(CapacityDemand), default(CapacityDemand)), (rig.Ledger.Current, rig.Ledger.Total(CapacityState.Suspended)));
        }

        [Theory]
        [InlineData("BuildDue")]
        [InlineData("Deadline")]
        [InlineData("Stopped")]
        public async Task PayloadCancellationStopsIssuanceAndSettlesOperations(string stopName)
        {
            var stop = Enum.Parse<IssuanceStop>(stopName);
            var rig = await PayloadAdvanceAsync();
            var planned = await PlannedPayloadAsync(rig);
            // One delete outstanding, the first held until issuance stopped
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxOutstanding = 1 };
            var records = await rig.Table.SnapshotAsync();
            var demand = rig.Active.Lease!.Demand;
            var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var stopped = 0;
            var issuedAfterStop = 0;
            var settled = new ConcurrentQueue<string>();
            var hooks = rig.Cleanup.AdvanceHooksForTests;
            hooks.BeforeDelete = async target =>
            {
                if (Volatile.Read(ref stopped) == 1)
                {
                    Interlocked.Increment(ref issuedAfterStop);
                }
                if (PayloadName(target.Key) == PayloadName(planned[0]))
                {
                    held.TrySetResult();
                    await release.Task;
                }
            };
            hooks.Settled = (target, outcome) => settled.Enqueue($"{PayloadName(target.Key)} {outcome.Status}");
            var job = new JobRun(rig.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var clock = new FakeTimeProvider(s_now);
            using var worker = new CancellationTokenSource();
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger, run: job, clock: clock, stop: worker.Token);

            var turn = AdvanceExecutionKit.TurnAsync(rig.Cleanup, context);
            await held.Task.WaitAsync(s_payloadWait);
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
            Volatile.Write(ref stopped, 1);
            release.SetResult();
            var result = await turn.WaitAsync(s_payloadWait);

            // The held delete settled within the turn and nothing was issued after the stop
            Assert.Equal(new[] { $"{PayloadName(planned[0])} {DeleteStatus.Deleted}" }, settled);
            Assert.Equal(0, issuedAfterStop);
            Assert.Equal(AdvanceState.Runnable, rig.Active.State);
            Assert.Equal(CapacityState.Runnable, rig.Active.Lease!.State);
            Assert.False(await rig.Table.Raw.Exists(planned[0]));
            Assert.True(await rig.Table.Raw.Exists(planned[1]));
            if (stop == IssuanceStop.Stopped)
            {
                // A worker stop also refuses its progress
                Assert.Equal(OutcomeKind.Declined, result.Kind);
                Assert.Equal(records, await rig.Table.SnapshotAsync());
                Assert.Equal((ActiveAdvance.Start(rig.Active.Started.Totals), 0L), (rig.Active.Cursor, rig.Active.Processed));
                Assert.Equal(demand, rig.Active.Lease.Demand);
            }
            else
            {
                // A due build or the deadline still records it
                Assert.Equal(OutcomeKind.Completed, result.Kind);
                Assert.Equal(1L, rig.Active.Processed);
                Assert.Equal(1L, (await NewestPayloadProgressAsync(rig.Table))?.Processed);
            }

            // The next turn goes on from the cursor
            hooks.BeforeDelete = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Equal(4, (await NewestPayloadProgressAsync(rig.Table))!.Processed);
        }

        [Theory]
        [InlineData("BuildDue")]
        [InlineData("Deadline")]
        public async Task YieldedPayloadSliceKeepsTheProgressItsSettledDeletesEarned(string stopName)
        {
            var stop = Enum.Parse<IssuanceStop>(stopName);
            var rig = await PayloadAdvanceAsync();
            var planned = await PlannedPayloadAsync(rig);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxOutstanding = 1 };
            var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // The second delete is outstanding when the stop comes
            rig.Cleanup.AdvanceHooksForTests.BeforeDelete = async target =>
            {
                if (PayloadName(target.Key) == PayloadName(planned[1]))
                {
                    held.TrySetResult();
                    await release.Task;
                }
            };
            var job = new JobRun(rig.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var clock = new FakeTimeProvider(s_now);
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger, run: job, clock: clock);

            var turn = AdvanceExecutionKit.TurnAsync(rig.Cleanup, context);
            await held.Task.WaitAsync(s_payloadWait);
            if (stop == IssuanceStop.BuildDue)
            {
                job.YieldRequested = true;
            }
            else
            {
                clock.Advance(TimeSpan.FromMinutes(16));
            }
            release.SetResult();
            await turn.WaitAsync(s_payloadWait);

            // Both deletes settled and their progress recorded
            Assert.Equal(stop, context.Stopped);
            Assert.False(await rig.Table.Raw.Exists(planned[0]));
            Assert.False(await rig.Table.Raw.Exists(planned[1]));
            Assert.Equal(2L, rig.Active.Processed);
            Assert.Equal(2L, (await NewestPayloadProgressAsync(rig.Table))?.Processed);
        }

        [Fact]
        public async Task SettledWorkIssuesPastADueBuildAndTheDeadlineButNotAStopOrTheJobsReturn()
        {
            using var worker = new CancellationTokenSource();
            var job = new JobRun(14, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var clock = new FakeTimeProvider(s_now);
            var context = _kit.DeletionContext(14, run: job, clock: clock, stop: worker.Token);
            job.YieldRequested = true;
            clock.Advance(TimeSpan.FromMinutes(16));

            Assert.Null(context.TryIssue(_ => Task.CompletedTask));
            Assert.NotNull(context.TryIssueSettled(_ => Task.CompletedTask));
            Assert.Equal(IssuanceStop.BuildDue, context.Stopped);
            worker.Cancel();
            Assert.Null(context.TryIssueSettled(_ => Task.CompletedTask));

            // A job that returned issues nothing
            var returned = _kit.DeletionContext(14);
            await returned.SettleAsync();
            Assert.Null(returned.TryIssueSettled(_ => Task.CompletedTask));
        }

        [Theory]
        [InlineData("BuildDue")]
        [InlineData("Deadline")]
        public async Task SettledWorkCannotIssueAfterAYieldedJobReturns(string stop)
        {
            var job = new JobRun(14, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var clock = new FakeTimeProvider(s_now);
            var context = _kit.DeletionContext(14, run: job, clock: clock);
            if (stop == "BuildDue")
            {
                job.YieldRequested = true;
            }
            else
            {
                clock.Advance(TimeSpan.FromMinutes(16));
            }
            Assert.Equal(Enum.Parse<IssuanceStop>(stop), context.CheckStop());

            await context.SettleAsync();
            var invoked = false;
            var issued = context.TryIssueSettled(_ =>
            {
                invoked = true;
                return Task.CompletedTask;
            });

            Assert.False(invoked);
            Assert.Null(issued);
            Assert.Equal(0, context.Tracked);
        }

        [Fact]
        public async Task SettledWorkCannotIssueWhileTheContextSettles()
        {
            var job = new JobRun(14, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var context = _kit.DeletionContext(14, run: job, clock: new FakeTimeProvider(s_now));
            var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.NotNull(context.TryIssue(_ => pending.Task));
            var settling = context.SettleAsync();
            Assert.False(settling.IsCompleted);

            var invoked = false;
            var issued = context.TryIssueSettled(_ =>
            {
                invoked = true;
                return Task.CompletedTask;
            });
            pending.SetResult();
            await settling;

            Assert.False(invoked);
            Assert.Null(issued);
        }

        [Fact]
        public async Task ProgressWaitsForSettlement()
        {
            var rig = await PayloadAdvanceAsync(local: false);
            var planned = await PlannedPayloadAsync(rig);
            var storage = rig.Table.Hooks!;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var first = rig.Active.ProgressName!;

            // The second part's last delete is held once the others settled
            var last = planned[7];
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var others = 0;
            var othersSettled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rig.Cleanup.AdvanceHooksForTests.Settled = (target, outcome) =>
            {
                if (target.Kind == TargetKind.Payload && PayloadName(target.Key) != PayloadName(last) && Interlocked.Increment(ref others) == 3)
                {
                    othersSettled.TrySetResult();
                }
            };
            var publishedWhileHeld = false;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenWrite" && !release.Task.IsCompleted && ControlName.TryParse(PayloadName(path.Full), out var name) && name.Kind == ControlKind.Progress)
                {
                    publishedWhileHeld = true;
                }
                if (verb == "Rm" && path.Full == last)
                {
                    entered.TrySetResult();
                    await release.Task;
                }
            };
            storage.ClearRequests();

            var turn = TurnAsync(rig);
            await entered.Task.WaitAsync(s_payloadWait);
            await othersSettled.Task.WaitAsync(s_payloadWait);
            var held = storage.Requests.Count;
            release.SetResult();
            Assert.Equal(OutcomeKind.Completed, (await turn.WaitAsync(s_payloadWait)).Kind);
            storage.Before = null;
            // Nothing is published while a delete of the slice is outstanding
            Assert.False(publishedWhileHeld);
            Assert.Equal(8, rig.Active.Processed);
            var second = rig.Active.ProgressName!;
            Assert.NotEqual(first, second);

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            // Written after the release and read back, then its predecessor removed, then the next slice lists
            var requests = storage.Requests.ToList();
            var written = requests.IndexOf($"OpenWrite {rig.Table.LogPath(second)}");
            var readBack = requests.IndexOf($"OpenRead {rig.Table.LogPath(second)}");
            var removed = requests.IndexOf($"Rm {rig.Table.LogPath(first)}");
            var listed = readBack < 0 ? -1 : requests.FindIndex(readBack, x => x.StartsWith("Ls ", StringComparison.Ordinal));
            Assert.True(held <= written && written < readBack && readBack < removed && removed < listed, $"{held} {written} {readBack} {removed} {listed}\n{string.Join("\n", requests)}");
        }

        [Fact]
        public async Task DueBuildAfterTheReadBackStillRemovesThePredecessor()
        {
            var rig = await PayloadAdvanceAsync(local: false);
            var storage = rig.Table.Hooks!;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var first = rig.Active.ProgressName!;
            var firstPath = rig.Table.LogPath(first);
            var job = new JobRun(rig.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };
            var context = _kit.DeletionContext(rig.Table.Head, rig.Ledger, run: job);
            // A build is due once the successor's read-back starts
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full != firstPath && ControlName.TryParse(PayloadName(path.Full), out var name) && name.Kind == ControlKind.Progress)
                {
                    job.YieldRequested = true;
                }
                return Task.CompletedTask;
            };

            await AdvanceExecutionKit.TurnAsync(rig.Cleanup, context);

            // The successor recorded and its predecessor removed under the latched yield
            Assert.Equal(IssuanceStop.BuildDue, context.Stopped);
            Assert.NotEqual(first, rig.Active.ProgressName);
            Assert.False(await rig.Table.Raw.Exists(firstPath));
            Assert.Contains($"Rm {firstPath}", storage.Requests);
        }

        [Fact]
        public async Task ResumeRepeatsAtMostOneSlice()
        {
            // One part of eleven paths in slices of three
            static void Slices(DeltaTableCleanup cleanup) => cleanup.Settings = cleanup.Settings with { MaxPaths = 3 };
            var rig = await PayloadAdvanceAsync(partPaths: 11);
            var planned = await PlannedPayloadAsync(rig);
            Slices(rig.Cleanup);
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var recorded = (await NewestPayloadProgressAsync(rig.Table))!;
            Assert.Equal(3, recorded.Processed);

            // The next slice deletes without its progress
            rig.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) => kind == ControlKind.Progress ? throw new IOException("injected") : Task.CompletedTask;
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.Transient);
            Assert.False(await rig.Table.Raw.Exists(planned[5]));

            var restarted = await RestartPayloadAsync(rig.Table, Slices);
            Assert.Equal((recorded.Next, 3L), (restarted.Active.Cursor, restarted.Active.Processed));
            var issued = new ConcurrentQueue<string>();
            restarted.Cleanup.AdvanceHooksForTests.Settled = (target, _) =>
            {
                if (target.Kind == TargetKind.Payload)
                {
                    issued.Enqueue(PayloadName(target.Key));
                }
            };
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(restarted)).Kind);

            // The interrupted slice again from the recorded cursor, nothing before it
            Assert.Equal(planned.Skip(3).Take(3).Select(PayloadName).Order(StringComparer.Ordinal), issued.Order(StringComparer.Ordinal));
            var current = (await NewestPayloadProgressAsync(rig.Table))!;
            Assert.Equal(6, current.Processed);

            // A digest-valid newer progress whose byte offset is not its record's suspends the next resume
            var misaligned = current with { Sequence = current.Sequence + 1, Next = current.Next with { Byte = current.Next.Byte + 1 } };
            await PlantProgressAsync(rig.Table, misaligned);
            var resumed = await RestartPayloadAsync(rig.Table, Slices);
            Assert.Equal(misaligned.Next, resumed.Active.Cursor);
            var after = 0;
            resumed.Cleanup.AdvanceHooksForTests.Settled = (target, _) => Interlocked.Increment(ref after);

            AssertDeclined(await TurnAsync(resumed), MaintenanceReason.ManifestSuspended);

            Assert.Equal(0, after);
            Assert.Equal(AdvanceState.Suspended, resumed.Active.State);
            Assert.True(await rig.Table.Raw.Exists(planned[6]));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task LateProgressCannotOverwriteSuccessor(bool torn)
        {
            var rig = await PayloadAdvanceAsync();
            var planned = await PlannedPayloadAsync(rig);
            var removed = new ConcurrentQueue<string>();
            rig.Cleanup.AdvanceHooksForTests.Settled = (target, outcome) =>
            {
                if (target.Kind == TargetKind.Control && outcome.Done)
                {
                    removed.Enqueue(PayloadName(target.Key));
                }
            };
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var first = rig.Active.ProgressName!;
            var late = (await NewestPayloadProgressAsync(rig.Table))!;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var successor = rig.Active.ProgressName!;
            // The predecessor went once its successor validated
            Assert.Equal(new[] { first }, removed);
            Assert.Equal(2, rig.Active.Sequence);

            // A copy of the first progress lands after its successor validated
            var copy = await PlantProgressAsync(rig.Table, late, torn ? x => CleanupRecordKit.Truncate(x, x.Length - 1) : null);

            if (torn)
            {
                // A torn copy leaves the group unknown, however valid its siblings
                await AssertResumeSuspendsAsync(rig.Table, rig.Active.C);
                return;
            }
            var restarted = await RestartPayloadAsync(rig.Table);
            Assert.Equal((2L, successor, new RecordCursor(2, 0, ControlRecords.FirstPathOffset), 8L),
                (restarted.Active.Sequence, restarted.Active.ProgressName, restarted.Active.Cursor, restarted.Active.Processed));
            var events = new ConcurrentQueue<string>();
            restarted.Cleanup.AdvanceHooksForTests.Settled = (target, outcome) => events.Enqueue($"{target.Kind} {PayloadName(target.Key)}");
            restarted.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, name) =>
            {
                events.Enqueue($"Publish {kind} {name}");
                return Task.CompletedTask;
            };

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(restarted)).Kind);

            // Only the last part again; the successor is removed after its own successor, and the late copy never as a predecessor
            var journal = events.ToList();
            Assert.Equal(planned.Skip(8).Select(PayloadName).Order(StringComparer.Ordinal),
                journal.Where(x => x.StartsWith("Payload ", StringComparison.Ordinal)).Select(x => x.Split(' ')[1]).Order(StringComparer.Ordinal));
            var terminal = journal.FindIndex(x => x.StartsWith("Publish Progress ", StringComparison.Ordinal));
            var predecessor = journal.IndexOf($"Control {successor}");
            var done = journal.FindIndex(x => x.StartsWith("Publish Done ", StringComparison.Ordinal));
            Assert.True(terminal >= 0 && terminal < predecessor && predecessor < done, string.Join("\n", journal));
            Assert.Single(journal, x => x.StartsWith("Control ", StringComparison.Ordinal));
            // The teardown removes the late copy with the rest and keeps the done
            Assert.EndsWith(ControlName.DoneSuffix, Assert.Single(await rig.Table.ControlsAsync()), StringComparison.Ordinal);
            Assert.False(await rig.Table.Raw.Exists(rig.Table.LogPath(copy)));
        }

        [Fact]
        public async Task TornStartedSiblingKeepsTheAdvanceSuspended()
        {
            var rig = await PayloadAdvanceAsync();
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            var association = rig.Active.Association;
            var started = (await rig.Table.ControlsAsync()).Single(x => x.EndsWith(ControlName.StartedSuffix, StringComparison.Ordinal));
            var bytes = await DeltaTestKit.ReadBytes(rig.Table.Raw, rig.Table.LogPath(started));

            // A torn copy of the started record lands beside the valid one
            await CleanupRecordKit.PlantAsync(rig.Table.Raw, "t", ControlName.Group(ControlKind.Started, association.C, association.AdvanceId, association.HeaderSha256, ControlTokens.New()),
                CleanupRecordKit.Truncate(bytes, bytes.Length - 1));

            await AssertResumeSuspendsAsync(rig.Table, association.C);
        }

        [Theory]
        [InlineData("part-replaced")]
        [InlineData("part-missing")]
        [InlineData("header-replaced")]
        [InlineData("started-differs")]
        [InlineData("record-past-part")]
        [InlineData("processed-differs")]
        [InlineData("part-past-parts")]
        [InlineData("terminal-short")]
        public async Task PayloadGateSuspendsWhenItsPartHeaderOrCursorDisagree(string change)
        {
            var rig = await PayloadAdvanceAsync(local: false);
            var planned = await PlannedPayloadAsync(rig);
            var active = rig.Active;
            var association = active.Association;
            var storage = rig.Table.Raw;
            async Task ReplaceAsync(string name, byte[] bytes)
            {
                var path = rig.Table.LogPath(name);
                Assert.Equal((await DeltaTestKit.ReadBytes(storage, path)).Length, bytes.Length);
                await storage.Rm(path);
                await CleanupRecordKit.PlantAsync(storage, "t", name, bytes);
            }
            var part = (await rig.Table.ControlsAsync()).Single(x => ControlName.TryParse(x, out var name) && name.Kind == ControlKind.Part && name.Ordinal == 0);
            switch (change)
            {
                case "part-replaced":
                    // A valid part at its name with as many paths of the same lengths, all other paths
                    var other = CleanupRecordKit.Sorted(planned.Take(4).Select(x => x.Replace("/part-", "/gone-", StringComparison.Ordinal)));
                    var replacement = new byte[ControlRecords.PartLength(other.Sum(x => x.Length), other.Count)];
                    ControlRecords.WritePart(replacement, association.C, association.AdvanceId, 0, other, out _);
                    await ReplaceAsync(part, replacement);
                    break;
                case "part-missing":
                    await storage.Rm(rig.Table.LogPath(part));
                    break;
                case "header-replaced":
                    // A valid header at its name naming the same parts with another count
                    var descriptors = new List<PartDescriptor>();
                    Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(await DeltaTestKit.ReadBytes(storage, rig.Table.LogPath(association.HeaderName)), out var header, descriptors));
                    var changed = header! with { Candidates = header.Candidates + 1 };
                    var rewritten = new byte[ControlRecords.HeaderLength(changed, descriptors.Count)];
                    ControlRecords.WriteHeader(rewritten, changed, descriptors, out _);
                    await ReplaceAsync(association.HeaderName, rewritten);
                    break;
                case "started-differs":
                    // A summary whose start its header does not describe
                    active = new ActiveAdvance()
                    {
                        Association = association,
                        Started = active.Started with { Head = active.Started.Head + 1 },
                        Destination = active.Destination,
                        CutoffTicks = active.CutoffTicks,
                        Evidence = active.Evidence,
                        Phase = active.Phase,
                        Cursor = active.Cursor,
                        Lease = active.Lease
                    };
                    rig.Cleanup.Adopt(active);
                    break;
                case "record-past-part":
                    // The first part's end at its walked offset, not in the next part's canonical form
                    active.Cursor = new RecordCursor(0, 4, ControlRecords.FirstPathOffset + planned.Take(4).Sum(x => 4 + Encoding.UTF8.GetByteCount(x)));
                    active.Processed = 4;
                    break;
                case "processed-differs":
                    active.Processed = 1;
                    break;
                case "part-past-parts":
                    active.Cursor = new RecordCursor(active.Started.Totals.Parts + 1, 0, ControlRecords.FirstPathOffset);
                    break;
                case "terminal-short":
                    // The terminal position with a path unprocessed
                    active.Cursor = new RecordCursor(active.Started.Totals.Parts, 0, 0);
                    active.Processed = active.Started.Totals.Emitted - 1;
                    break;
            }
            var settled = 0;
            rig.Cleanup.AdvanceHooksForTests.Settled = (_, _) => Interlocked.Increment(ref settled);

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.ManifestSuspended);

            Assert.Equal(0, settled);
            Assert.Equal(AdvanceState.Suspended, active.State);
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.Null(await NewestPayloadProgressAsync(rig.Table));
        }

        [Theory]
        [InlineData("other-advance")]
        [InlineData("unsorted")]
        [InlineData("other-length")]
        public async Task PayloadGateSuspendsOnAPartItsDigestAloneDoesNotValidate(string forgery)
        {
            var rig = await PayloadAdvanceAsync(local: false);
            var planned = await PlannedPayloadAsync(rig);
            var table = rig.Table;
            var association = rig.Active.Association;
            var paths = CleanupRecordKit.Sorted(planned.Take(4));
            byte[] part;
            switch (forgery)
            {
                case "other-advance":
                    part = new byte[ControlRecords.PartLength(paths.Sum(x => x.Length), paths.Count)];
                    ControlRecords.WritePart(part, association.C, ControlTokens.New(), 0, paths, out _);
                    break;
                case "unsorted":
                    part = new byte[ControlRecords.PartLength(paths.Sum(x => x.Length), paths.Count)];
                    ControlRecords.WritePart(part, association.C, association.AdvanceId, 0, paths.Reverse().ToList(), out _);
                    break;
                default:
                    part = await DeltaTestKit.ReadBytes(table.Raw, table.LogPath((await table.ControlsAsync()).Single(x => ControlName.TryParse(x, out var name) && name.Kind == ControlKind.Part && name.Ordinal == 0)));
                    break;
            }

            // The header names that part first by its digest
            var descriptors = await DescriptorsAsync(rig);
            var attempt = ControlTokens.New();
            await CleanupRecordKit.PlantAsync(table.Raw, "t", ControlName.Part(association.C, association.AdvanceId, 0, attempt), part);
            descriptors[0] = new PartDescriptor(0, attempt, paths.Count, part.Length + (forgery == "other-length" ? 4 : 0), SHA256.HashData(part));
            var chain = await RechainAsync(rig, descriptors);
            var forgedRig = await RestartPayloadAsync(table);
            Assert.Equal(chain, forgedRig.Active.Association);
            var settled = 0;
            forgedRig.Cleanup.AdvanceHooksForTests.Settled = (_, _) => Interlocked.Increment(ref settled);

            AssertDeclined(await TurnAsync(forgedRig), MaintenanceReason.ManifestSuspended);

            Assert.Equal(0, settled);
            Assert.Equal(AdvanceState.Suspended, forgedRig.Active.State);
            foreach (var path in planned)
            {
                Assert.True(await table.Raw.Exists(path), path);
            }
        }
    }
}
