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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics.Metrics;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaMaintenanceLifecycleTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "lc-" + ScratchOwners.ShortId());

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

        private static async Task WaitUntil(FlowtideTestStream stream, Func<bool> condition, TimeSpan? timeout = null)
        {
            var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromMinutes(1));
            while (!condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The condition did not hold in time");
                }
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
        }

        // An idle stream checkpoints only when asked, and the sink hands off at its checkpoints
        private static async Task WaitUntilCheckpointing(FlowtideTestStream stream, Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The condition did not hold in time");
                }
                await stream.SchedulerTick();
                await stream.TriggerCheckpoint();
                await Task.Delay(20);
            }
        }

        private static DeltaMaintenanceWorker Worker(DeltaLakeSinkStream stream) => stream.Maintenance.Workers.Single();

        // The worker a restart registered in place of the previous one, read in one snapshot; null until the condition holds for it
        private static DeltaMaintenanceWorker? Successor(DeltaLakeSinkStream stream, DeltaMaintenanceWorker previous, Func<DeltaMaintenanceWorker, bool> condition)
        {
            return stream.Maintenance.Workers is [var worker] && worker != previous && condition(worker) ? worker : null;
        }

        // Holds a named point of the run's worker until released
        private sealed class Hold
        {
            private readonly TaskCompletionSource _reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            // Bounded, so a hold that is never reached fails the test instead of hanging it
            public Task WhenReached => _reached.Task.WaitAsync(TimeSpan.FromMinutes(1));

            public void Release() => _release.TrySetResult();

            public static Hold At(DeltaMaintenanceWorker worker, string point)
            {
                var hold = new Hold();
                worker.HooksForTests = new WorkerHooks()
                {
                    Hold = at =>
                    {
                        if (at != point)
                        {
                            return Task.CompletedTask;
                        }
                        hold._reached.TrySetResult();
                        return hold._release.Task;
                    }
                };
                return hold;
            }
        }

        [Fact]
        public void NoHandOffWhileTheCommitIsNotPublishable()
        {
            // Staged by the stop checkpoint 10, so a commit of version 9 is replayed after the restart instead
            var pending = new DeltaLakePendingCommit() { Version = 5, CheckpointId = 10 };
            Assert.Equal(-1, DeltaLakeSink.HandOffVersion(pending, 9, null, 4));
            Assert.Equal(-1, DeltaLakeSink.HandOffVersion(pending, 9, PublishOutcome.Published, 4));
        }

        [Fact]
        public void NoHandOffAfterAFailedClosedPublication()
        {
            var pending = new DeltaLakePendingCommit() { Version = 5, CheckpointId = 9 };
            Assert.Equal(-1, DeltaLakeSink.HandOffVersion(pending, 9, PublishOutcome.FailedClosed, 4));
        }

        [Theory]
        [InlineData(nameof(PublishOutcome.Published))]
        [InlineData(nameof(PublishOutcome.AlreadyPublished))]
        [InlineData(nameof(PublishOutcome.Repaired))]
        public void RepairedCommitIsHandedOff(string outcome)
        {
            var pending = new DeltaLakePendingCommit() { Version = 5, CheckpointId = 9 };
            Assert.Equal(5, DeltaLakeSink.HandOffVersion(pending, 9, Enum.Parse<PublishOutcome>(outcome), 4));
        }

        [Fact]
        public void NothingPendingHandsOffTheValidatedHead()
        {
            Assert.Equal(7, DeltaLakeSink.HandOffVersion(null, 9, null, 7));
            Assert.Equal(-1, DeltaLakeSink.HandOffVersion(null, 9, null, -1));
        }

        [Fact]
        public async Task CommitVersionReturnsBeforeAHeldBuildCompletes()
        {
            var testName = nameof(CommitVersionReturnsBeforeAHeldBuildCompletes);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 1);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            await stream.Maintenance.TickAndSettleAsync(1);

            var worker = Worker(stream);
            var hold = Hold.At(worker, "build");
            try
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, 2);
                await WaitUntil(stream, () => worker.Target >= 2);
                stream.Maintenance.Budget.TickNow();
                await hold.WhenReached.WaitAsync(TimeSpan.FromSeconds(30));

                // Publication goes on while the build of 2 is held
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, 3, TimeSpan.FromSeconds(30));
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, 4, TimeSpan.FromSeconds(30));
                Assert.False(worker.CurrentJob.IsCompleted);
            }
            finally
            {
                // A failed wait must not leave a commit waiting on the held build while the stream disposes
                hold.Release();
            }
            await worker.CurrentJob;
            Assert.Contains(2L, await DeltaMaintenanceWorkerTests.Checkpoints(storage, "test"));
        }

        [Fact]
        public async Task IdleFirstSaveDataArmsTheWorker()
        {
            var testName = nameof(IdleFirstSaveDataArmsTheWorker);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 10);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            // A later checkpoint persists that nothing is pending, so the restart publishes nothing
            await SettlePublications(stream);

            // After the restart nothing is new, so every SaveData is idle and only the validated head can arm the new worker
            var before = Worker(stream);
            await stream.Crash();
            DeltaMaintenanceWorker? worker = null;
            await WaitUntilCheckpointing(stream, () => (worker = Successor(stream, before, x => x.Target == 1)) != null);
            await stream.Maintenance.TickAndSettleAsync(1);
            Assert.True(worker!.Inspected);
        }

        // Same users in the same order, so a new stream's source resumes at the restored offset
        private static readonly List<User> s_users = Enumerable.Range(1, 20).Select(i => new User() { UserKey = i, FirstName = $"user{i}" }).ToList();

        private static void AddUsers(FlowtideTestStream stream, int from, int to)
        {
            for (int i = from; i < to; i++)
            {
                stream.AddOrUpdateUser(s_users[i]);
            }
        }

        [Fact]
        public async Task GracefulStopLastCommitCheckpointedAfterNextStart()
        {
            var testName = nameof(GracefulStopLastCommitCheckpointedAfterNextStart);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var stateFiles = new KeepAliveMemoryFileProvider();
            await using (var first = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 2, stateFiles: stateFiles))
            {
                AddUsers(first, 0, 10);
                await first.StartStream(UserInsert);
                await WaitForVersion(storage, "test", first, 0);
                for (int version = 1; version <= 2; version++)
                {
                    AddUsers(first, 9 + version, 10 + version);
                    await WaitForVersion(storage, "test", first, version);
                }
                // The run stops before any tick, so 2 has no checkpoint yet
                await first.StopStream();
            }
            Assert.Empty(await DeltaMaintenanceWorkerTests.Checkpoints(storage, "test"));

            await using (var second = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 2, stateFiles: stateFiles))
            {
                AddUsers(second, 0, 12);
                await second.StartStream(UserInsert);
                await WaitUntilCheckpointing(second, () => second.Maintenance.Workers.Count == 1 && Worker(second).Target >= 2);
                await second.Maintenance.TickAndSettleAsync(2);
                Assert.Contains(2L, await DeltaMaintenanceWorkerTests.Checkpoints(storage, "test"));
                await second.StopStream();
            }
        }

        [Fact]
        public async Task TwoRestartsKeepCheckpointing()
        {
            var testName = nameof(TwoRestartsKeepCheckpointing);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 2);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            long version = 0;
            for (int run = 0; run < 3; run++)
            {
                for (int i = 0; i < 2; i++)
                {
                    stream.Generate(1);
                    await WaitForVersion(storage, "test", stream, ++version);
                }
                await stream.Maintenance.TickAndSettleAsync(version);
                Assert.Contains(version, await DeltaMaintenanceWorkerTests.Checkpoints(storage, "test"));
                if (run < 2)
                {
                    var before = Worker(stream);
                    await stream.Crash();
                    await WaitUntil(stream, () => Successor(stream, before, _ => true) != null);
                }
            }
            Assert.Equal(1, stream.Maintenance.Budget.Registered);
        }

        [Fact]
        public async Task HaltedSinkNeverCheckpointsTheConflictingVersion()
        {
            var testName = nameof(HaltedSinkNeverCheckpointsTheConflictingVersion);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 1);
            stream.AddLoggerProvider(logs);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            for (int version = 1; version <= 2; version++)
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, version);
            }

            // Another writer takes version 3 right before the sink publishes it
            var foreignWritten = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath("test", 3)) && Interlocked.Exchange(ref foreignWritten, 1) == 0)
                {
                    await WriteCommit(inner, "test", 3, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                }
            };
            stream.Generate(1);
            await WaitUntil(stream, () => logs.Errors.Count >= 1);
            storage.Before = null;

            await stream.Maintenance.TickAndSettleAsync(2);
            var worker = Worker(stream);
            Assert.Equal(2, worker.Target);
            var checkpoints = await DeltaMaintenanceWorkerTests.Checkpoints(storage, "test");
            Assert.Contains(2L, checkpoints);
            Assert.DoesNotContain(3L, checkpoints);
        }

        [Fact]
        public async Task IntervalZeroNeverDispatchesPeriodicBuilds()
        {
            var testName = nameof(IntervalZeroNeverDispatchesPeriodicBuilds);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 0;
                options.CheckpointMaxAge = TimeSpan.Zero;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            for (long version = 1; version <= 3; version++)
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, version);
            }
            // The worker exists for the cleanup steps but inspects and builds nothing, also with an age trigger due
            var worker = Assert.Single(stream.Maintenance.Workers);
            await stream.Maintenance.TickAndSettleAsync(3);
            Assert.False(await stream.Maintenance.TickOnceAsync());
            Assert.False(worker.Inspected);
            Assert.Equal(-1, worker.LastCompleted);
            // Checkpoints are off, so no lag accrues although every version was handed off
            Assert.Equal(3, worker.Target);
            Assert.Equal(0, worker.LagVersions);
            Assert.DoesNotContain(storage.Requests, x => x.Contains("_last_checkpoint") || x.Contains(".checkpoint."));
        }

        [Fact]
        public async Task IntervalZeroRunsOnlyStepsOneAndTwo()
        {
            var directory = Path.Combine(Path.GetTempPath(), "iz-" + ScratchOwners.ShortId());
            try
            {
                // Checkpoints off and no retention: the worker runs the discard drain and the staged sweep only
                await using var run = SinkRun.Start(nameof(IntervalZeroRunsOnlyStepsOneAndTwo), directory);
                await run.Started();
                var sink = run.Sink;
                var entry = sink.DiscardRouteForTests!.Entry;
                var discarded = run.ChangeData();
                Assert.NotEmpty(discarded);
                Assert.Equal(discarded.Count, entry.Hints);
                var worker = Assert.Single(run.Stream.Maintenance.Workers);
                var cleanup = (DeltaTableCleanup)worker.Cleanup!;

                // A published copy of commit 0 left as a stage, which only the staged sweep removes
                var log = Path.Combine(run.TableDir, "_delta_log");
                var stage = $".{0:D20}.json.{Guid.NewGuid():N}.tmp";
                File.Copy(Path.Combine(log, $"{0:D20}.json"), Path.Combine(log, stage));
                await run.Stream.Maintenance.TickAndSettleAsync(0);
                // Step two: the staged sweep removed the stage; step one: the drain deleted the discards
                Assert.False(File.Exists(Path.Combine(log, stage)));
                Assert.Equal(StageVerdict.Published, cleanup.LastSweepForTests!.Verdicts[stage]);
                Assert.Equal(1, cleanup.LastSweepForTests.LogFilesDeleted);
                Assert.Equal(0, entry.Hints);
                Assert.All(discarded, x => Assert.False(run.Exists(x)));

                // An uninterpreted control record found by the drain's own listing drops the next attempt's hints
                await run.Local.WriteText("/test/_delta_log/.flowtide-cleanup.future", "{}");
                run.Stream.Generate(100);
                await WaitForVersion(run.Local, "test", run.Stream, 1);
                var fenced = run.ChangeData();
                Assert.NotEmpty(fenced);
                Assert.Equal(fenced.Count, entry.Hints);
                await run.Stream.Maintenance.TickAndSettleAsync(1);
                Assert.Equal(0, entry.Hints);
                Assert.Equal(fenced.Count, sink.DiscardsForTests.Dropped);
                Assert.All(fenced, x => Assert.True(run.Exists(x)));
                Assert.Equal(1, worker.CleanupFailures(JobKind.Deletion));

                // No inspection, repair or build ran
                Assert.False(worker.Inspected);
                Assert.Equal(-1, worker.LastCompleted);
                Assert.Null(worker.LastBuild);
                Assert.DoesNotContain(run.Storage.Requests, x => x.Contains("_last_checkpoint") || x.Contains(".checkpoint."));
                Assert.Empty(Directory.GetFiles(Path.Combine(run.TableDir, "_delta_log"), "*checkpoint*"));
                Assert.Equal(0, run.Stream.FailureNotificationCount);
            }
            finally
            {
                try
                {
                    Directory.Delete(directory, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        [Fact]
        public async Task MaintenanceInitFailureLeavesTheStreamHealthy()
        {
            var testName = nameof(MaintenanceInitFailureLeavesTheStreamHealthy);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 1;
                options.MaintenanceInitFailsForTests = true;
            });
            stream.AddLoggerProvider(logs);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, stream.FailureNotificationCount);
            Assert.Equal(0, stream.Maintenance.Budget.Registered);
            Assert.True(stream.Maintenance.Budget.InitFailed);
            Assert.Single(logs.Warnings, x => x.Contains(nameof(MaintenanceReason.InitFailed)));
        }

        [Fact]
        public async Task InitializationFaultStaysWithItsOwnSink()
        {
            var testName = nameof(InitializationFaultStaysWithItsOwnSink);
            var firstStorage = Files.Of.InternalMemory($"./{testName}-first");
            var secondStorage = Files.Of.InternalMemory($"./{testName}-second");
            await using var first = new DeltaLakeSinkStream(testName + "_first", firstStorage, options =>
            {
                options.CheckpointInterval = 1;
                options.MaintenanceInitFailsForTests = true;
            });
            await using var second = new DeltaLakeSinkStream(testName + "_second", secondStorage, options => options.CheckpointInterval = 1);

            // Both write a table of the same name; only the first sink's options carry the fault
            first.Generate(1);
            await first.StartStream(UserInsert);
            await WaitForVersion(firstStorage, "test", first, 0);
            second.Generate(1);
            await second.StartStream(UserInsert);
            await WaitForVersion(secondStorage, "test", second, 0);
            Assert.True(first.Maintenance.Budget.InitFailed);
            Assert.Equal(0, first.Maintenance.Budget.Registered);
            Assert.False(second.Maintenance.Budget.InitFailed);
            Assert.Equal(1, second.Maintenance.Budget.Registered);
        }

        // The sink's detached-jobs gauge, read once
        private static List<int> ReadDetachedGauge(DeltaLakeSinkStream stream)
        {
            var readings = new List<int>();
            using (var listener = new MeterListener())
            {
                listener.InstrumentPublished = (instrument, meterListener) =>
                {
                    if (instrument.Meter.Name.StartsWith($"flowtide.{stream.StreamName}.operator.", StringComparison.Ordinal) && instrument.Name == "flowtide_delta_checkpoint_detached_jobs")
                    {
                        meterListener.EnableMeasurementEvents(instrument);
                    }
                };
                listener.SetMeasurementEventCallback<int>((instrument, value, tags, state) => readings.Add(value));
                listener.Start();
                listener.RecordObservableInstruments();
            }
            return readings;
        }

        [Fact]
        public async Task DetachedJobOfACrashedRunIsCountedByTheTableGauge()
        {
            var testName = nameof(DetachedJobOfACrashedRunIsCountedByTheTableGauge);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var upload = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var uploading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 2);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            for (int version = 1; version <= 2; version++)
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, version);
            }
            await WaitUntil(stream, () => stream.Maintenance.Workers.Count == 1 && stream.Maintenance.Workers[0].Target >= 2);
            var held = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith(".checkpoint.parquet") && Interlocked.Exchange(ref held, 1) == 0)
                {
                    uploading.TrySetResult();
                    await upload.Task;
                }
            };
            var first = stream.Maintenance.Workers[0];
            stream.Maintenance.Budget.TickNow();
            await uploading.Task.WaitAsync(TimeSpan.FromSeconds(30));

            try
            {
                // The engine disposes the sink on a crash and initializes the same instance again
                await stream.Crash();
                await WaitUntil(stream, () => stream.Maintenance.Workers is [var worker] && worker != first);
                Assert.Equal(1, first.DetachedJobs);
                Assert.Equal(new[] { 1 }, ReadDetachedGauge(stream));
            }
            finally
            {
                upload.TrySetResult();
                await first.CurrentJob;
            }
        }

        [Fact]
        public async Task FinalDisposeNeverThrows()
        {
            var testName = nameof(FinalDisposeNeverThrows);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 1);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            await stream.Maintenance.TickAndSettleAsync(1);
            var worker = Worker(stream);
            var hold = Hold.At(worker, "release");
            worker.HooksForTests = new WorkerHooks() { Hold = worker.HooksForTests!.Hold, Fault = point => { if (point == "slot") throw new InvalidOperationException("slot fault"); } };
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 2);
            await WaitUntil(stream, () => worker.Target >= 2);
            stream.Maintenance.Budget.TickNow();
            await hold.WhenReached.WaitAsync(TimeSpan.FromSeconds(30));

            // The dispose stops a job that is still releasing, and that job later fails one of its release steps
            await stream.DisposeAsync();
            Assert.Equal(1, worker.DetachedJobs);
            hold.Release();
            await worker.CurrentJob;
            Assert.Equal(0, worker.DetachedJobs);
        }

        [Fact]
        public async Task DetachedJobKeepsItsSlotUntilItSettles()
        {
            // One slot in the process: the next run's job waits until the detached job has settled
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, "slot")), TimeProvider.System, manualTick: true);
            var storage = Files.Of.InternalMemory($"./{nameof(DetachedJobKeepsItsSlotUntilItSettles)}");
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 4);
            var first = NewWorker(budget, storage, "t");
            var hold = Hold.At(first, "build");
            first.Published(4);
            budget.TickNow();
            await hold.WhenReached.WaitAsync(TimeSpan.FromSeconds(30));
            await first.StopAsync(TimeSpan.FromMilliseconds(50));
            Assert.Equal(1, first.DetachedJobs);

            // Another table, so the detached job's lease cannot hold it back, only the slot
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 4, "u");
            var second = NewWorker(budget, storage, "u");
            second.Published(4);
            budget.TickNow();
            await DeltaMaintenanceWorkerTests.Queued(() => budget.Gate.QueueLength == 1);
            Assert.False(second.CurrentJob.IsCompleted);
            Assert.False(second.Inspected);

            hold.Release();
            await first.CurrentJob;
            await second.CurrentJob.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(second.Inspected);
        }

        [Fact]
        public async Task DetachedJobWithAFailedDeletionLeavesItsChargeForTheNextRun()
        {
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, "charge")), TimeProvider.System, manualTick: true);
            var storage = Files.Of.InternalMemory($"./{nameof(DetachedJobWithAFailedDeletionLeavesItsChargeForTheNextRun)}");
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 4);
            var first = NewWorker(budget, storage, "t");
            var hold = Hold.At(first, "release");
            first.Published(4);
            budget.TickNow();
            await hold.WhenReached.WaitAsync(TimeSpan.FromSeconds(30));
            // The scratch file of the detached job cannot be deleted
            var folder = first.LastJobFolderForTests!;
            var stuck = Path.Combine(folder, "stuck.bin");
            var job = budget.Scratch;
            await File.WriteAllBytesAsync(stuck, new byte[1000]);
            job.Ledger.Reserve(1000);
            job.QueuePending(stuck, 1000);
            job.DeleteFailsForTests = path => path == stuck;
            await first.StopAsync(TimeSpan.Zero);
            hold.Release();
            await first.CurrentJob;
            Assert.Equal(1000, job.PendingBytes);

            // The next run's first job retries the list before it reserves anything
            job.DeleteFailsForTests = null;
            var second = NewWorker(budget, storage, "t");
            second.Published(4);
            budget.TickNow();
            await second.CurrentJob;
            Assert.Equal(0, job.PendingBytes);
            Assert.False(File.Exists(stuck));
        }

        [Fact]
        public async Task SuccessorWithANewStorageInstanceWaitsForTheLease()
        {
            var root = Path.Combine(_directory, "instances");
            Directory.CreateDirectory(root);
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, "instances-scratch")), TimeProvider.System, manualTick: true);
            await DeltaMaintenanceWorkerTests.Commits(Files.Of.LocalDisk(root), 0, 4);
            var first = NewWorker(budget, Files.Of.LocalDisk(root), "t");
            var hold = Hold.At(first, "build");
            first.Published(4);
            budget.TickNow();
            await hold.WhenReached.WaitAsync(TimeSpan.FromSeconds(30));

            // Another storage instance over the same folder is the same table
            var second = NewWorker(budget, Files.Of.LocalDisk(root), "t");
            second.Published(4);
            budget.TickNow();
            await DeltaMaintenanceWorkerTests.Queued(() => budget.Leases.Waiting == 1);
            Assert.False(second.Inspected);

            hold.Release();
            await first.CurrentJob;
            await second.CurrentJob.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(second.Inspected);
        }

        [Fact]
        public async Task RecreatedTableStillExcludesTheDetachedJob()
        {
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, "recreated")), TimeProvider.System, manualTick: true);
            var storage = Files.Of.InternalMemory($"./{nameof(RecreatedTableStillExcludesTheDetachedJob)}");
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 4);
            var first = NewWorker(budget, storage, "t");
            first.SetTableId("old-table");
            var hold = Hold.At(first, "build");
            first.Published(4);
            budget.TickNow();
            await hold.WhenReached.WaitAsync(TimeSpan.FromSeconds(30));
            await first.StopAsync(TimeSpan.Zero);

            // The table was recreated with a new id while the detached job still runs
            var second = NewWorker(budget, storage, "t");
            second.SetTableId("new-table");
            second.Published(4);
            budget.TickNow();
            await DeltaMaintenanceWorkerTests.Queued(() => budget.Leases.Waiting == 1);
            Assert.False(second.Inspected);

            hold.Release();
            await first.CurrentJob;
            await second.CurrentJob.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(second.Inspected);
        }

        [Fact]
        public async Task TenSinksStayWithinTheGateAndAllowance()
        {
            // Local disk lists sizes, which admission charges
            var root = Path.Combine(_directory, "ten-tables");
            Directory.CreateDirectory(root);
            var storage = new HookFileStorage(Files.Of.LocalDisk(root));
            var tables = Enumerable.Range(0, 10).Select(i => $"t{i}").ToList();
            foreach (var table in tables)
            {
                for (long version = 0; version <= 3; version++)
                {
                    await DeltaTestKit.WriteCommit(storage, table, version, version == 0
                        ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet") }
                        : new[] { DeltaLakeCheckpointBuildTests.Add($"f{version}.parquet") });
                }
            }
            // The allowance fits one first commit, so two jobs' first commits never download at once
            var cap = new FileInfo(Path.Combine(root, "t0", "_delta_log", $"{0:D20}.json")).Length;
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, "ten")), TimeProvider.System, manualTick: true)
            {
                Transfer = new TransferAllowance(cap)
            };
            var maxRunning = 0;
            long maxCharged = 0;
            var running = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
            var overlapping = 0;
            // Tail reads are admitted by listed size, so a commit read happens with a charge held; the delay lets the jobs' reads overlap
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(".json"))
                {
                    InterlockedMax(ref maxCharged, budget.Transfer.Charged);
                    await Task.Delay(10);
                }
            };
            var workers = new List<DeltaMaintenanceWorker>();
            foreach (var table in tables)
            {
                var worker = NewWorker(budget, storage, table);
                worker.HooksForTests = new WorkerHooks()
                {
                    Hold = async point =>
                    {
                        if (point == "build")
                        {
                            if (running.AddOrUpdate(table, 1, (_, n) => n + 1) > 1)
                            {
                                Interlocked.Increment(ref overlapping);
                            }
                            InterlockedMax(ref maxRunning, budget.Gate.Running);
                            await Task.Delay(20);
                            running.AddOrUpdate(table, 0, (_, n) => n - 1);
                        }
                    }
                };
                worker.Published(3);
                workers.Add(worker);
            }
            budget.TickNow();
            await Task.WhenAll(workers.Select(x => x.CurrentJob)).WaitAsync(TimeSpan.FromMinutes(1));

            Assert.All(tables, table => Assert.True(storage.Requests.Any(x => x.Contains($"/{table}/_delta_log/") && x.EndsWith(".checkpoint.parquet"))));
            Assert.InRange(maxRunning, 1, 2);
            Assert.Equal(0, overlapping);
            Assert.InRange(maxCharged, 1, budget.Transfer.Cap);
        }

        private static void InterlockedMax(ref int target, int value)
        {
            var current = Volatile.Read(ref target);
            while (value > current && Interlocked.CompareExchange(ref target, value, current) != current)
            {
                current = Volatile.Read(ref target);
            }
        }

        private static void InterlockedMax(ref long target, long value)
        {
            var current = Interlocked.Read(ref target);
            while (value > current && Interlocked.CompareExchange(ref target, value, current) != current)
            {
                current = Interlocked.Read(ref target);
            }
        }

        [Fact]
        public async Task PublishFaultLeavesTheStreamHealthyWithOneWarningAndAGauge()
        {
            var testName = nameof(PublishFaultLeavesTheStreamHealthyWithOneWarningAndAGauge);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 1);
            stream.AddLoggerProvider(logs);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            await WaitUntil(stream, () => stream.Maintenance.Workers.Count == 1 && Worker(stream).Target >= 1);
            var worker = Worker(stream);
            worker.HooksForTests = new WorkerHooks() { PublishFault = point => { if (point == "hint") throw new IOException("hint fault"); } };
            await stream.Maintenance.TickAndSettleAsync(1);

            Assert.Equal(1, worker.ConsecutiveFailures);
            Assert.Single(logs.Warnings, x => x.Contains(nameof(MaintenanceReason.Transient)));
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task RealTickCheckpointsEndToEnd()
        {
            var testName = nameof(RealTickCheckpointsEndToEnd);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, "real-tick")), TimeProvider.System, manualTick: false, tickPeriod: TimeSpan.FromMilliseconds(50));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 2;
                options.MaintenanceBudgetOverride = budget;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            for (int version = 1; version <= 2; version++)
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, version);
            }
            Assert.True(budget.TimerActive);
            Assert.Equal(2, await WaitForCheckpoint(storage, "test", stream, 2, TimeSpan.FromSeconds(30)));
        }

        [Fact]
        public async Task RecoveryWhileAnUploadIsHeld()
        {
            var testName = nameof(RecoveryWhileAnUploadIsHeld);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var upload = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var uploading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 2);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            for (int version = 1; version <= 2; version++)
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, version);
            }
            await WaitUntil(stream, () => stream.Maintenance.Workers.Count == 1 && Worker(stream).Target >= 2);
            var held = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith(".checkpoint.parquet") && Interlocked.Exchange(ref held, 1) == 0)
                {
                    uploading.TrySetResult();
                    await upload.Task;
                }
            };
            var first = Worker(stream);
            stream.Maintenance.Budget.TickNow();
            await uploading.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // The stream recovers while the first run's upload is held; that job is detached and keeps its lease
            await stream.Crash();
            await WaitUntil(stream, () => Successor(stream, first, _ => true) != null);
            Assert.Equal(1, first.DetachedJobs);
            upload.SetResult();
            await first.CurrentJob;
            await stream.Maintenance.TickAndSettleAsync(2);

            var hint = (await DeltaMaintenanceWorkerTests.Hint(storage, "test"))!;
            Assert.Equal(2, hint.Version);
            Assert.Equal((await ReadBytes(storage, $"/test/_delta_log/{2:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);
        }

        [Fact]
        public async Task RestoredCommitPublishedAtStartIsCheckpointed()
        {
            var testName = nameof(RestoredCommitPublishedAtStartIsCheckpointed);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 2);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);

            // No later checkpoint persists that version 2 was published, so the restored state still holds it pending
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 2);
            var before = Worker(stream);
            await stream.Crash();
            // The restored pending commit is published again at start, already published, and handed off
            await WaitUntilCheckpointing(stream, () => Successor(stream, before, x => x.Target >= 2) != null);
            await stream.Maintenance.TickAndSettleAsync(2);
            Assert.Contains(2L, await DeltaMaintenanceWorkerTests.Checkpoints(storage, "test"));
        }

        // The user insert into a table of the test's own, so hooks keyed by table name see only this test
        private static string InsertInto(string table) => UserInsert.Replace("TABLE test", $"TABLE {table}").Replace("INTO test", $"INTO {table}");

        // Records every CommitVersion of the sink: its pending version, the publication outcome, and the worker's target right after
        private static Action<long?, PublishOutcome?, long> RecordCommits(System.Collections.Concurrent.ConcurrentQueue<(long? Pending, PublishOutcome? Outcome, long Target)> commits, Action<long?, PublishOutcome?>? after = null)
        {
            return (pending, outcome, target) =>
            {
                commits.Enqueue((pending, outcome, target));
                after?.Invoke(pending, outcome);
            };
        }

        [Fact]
        public async Task RepairedPublicationIsHandedOffAtOnce()
        {
            const string table = "repairedpublication";
            var testName = nameof(RepairedPublicationIsHandedOffAtOnce);
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var commits = new System.Collections.Concurrent.ConcurrentQueue<(long? Pending, PublishOutcome? Outcome, long Target)>();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 100;
                options.CommitVersionHookForTests = RecordCommits(commits);
            });
            stream.Generate(10);
            await stream.StartStream(InsertInto(table));
            await WaitForVersion(storage, table, stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, table, stream, 1);

            // Version 2 tears on its way out: a prefix lands, then the process dies with the stage still there
            var torn = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith(CommitPath(table, 2)) && Interlocked.Exchange(ref torn, 1) == 0)
                {
                    var staged = (await inner.Ls($"/{table}/_delta_log/")).Single(x => x.Name.StartsWith($".{2:D20}.json."));
                    var bytes = await ReadBytes(inner, staged.Path.Full);
                    await using (var target = await inner.OpenWrite(CommitPath(table, 2)))
                    {
                        await target.WriteAsync(bytes.AsMemory(0, bytes.Length - 8));
                    }
                    throw new CrashException();
                }
            };
            stream.Generate(1);
            // After the restart the restored commit finds its own torn copy and repairs it from the stage
            await WaitUntil(stream, () => commits.Any(x => x.Outcome == PublishOutcome.Repaired));
            var repaired = commits.First(x => x.Outcome == PublishOutcome.Repaired);
            Assert.Equal(2, repaired.Pending);
            // Handed off by that CommitVersion itself, before any later checkpoint could arm the worker
            Assert.Equal(2, repaired.Target);
        }

        [Fact]
        public async Task AlreadyPublishedCommitIsHandedOffAtOnce()
        {
            const string table = "alreadypublished";
            var testName = nameof(AlreadyPublishedCommitIsHandedOffAtOnce);
            var storage = Files.Of.InternalMemory($"./{testName}");
            // Version 2 is published, then the process dies before a checkpoint records it, so the restart publishes it again
            var crashed = 0;
            var commits = new System.Collections.Concurrent.ConcurrentQueue<(long? Pending, PublishOutcome? Outcome, long Target)>();
            var record = RecordCommits(commits, (pending, outcome) =>
            {
                if (pending == 2 && outcome == PublishOutcome.Published && Interlocked.Exchange(ref crashed, 1) == 0)
                {
                    throw new CrashException();
                }
            });
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 100;
                options.CommitVersionHookForTests = record;
            });
            stream.Generate(10);
            await stream.StartStream(InsertInto(table));
            await WaitForVersion(storage, table, stream, 0);
            stream.Generate(1);
            await WaitForVersion(storage, table, stream, 1);
            stream.Generate(1);
            await WaitUntil(stream, () => commits.Any(x => x.Outcome == PublishOutcome.AlreadyPublished));
            var restored = commits.First(x => x.Outcome == PublishOutcome.AlreadyPublished);
            Assert.Equal(2, restored.Pending);
            Assert.Equal(2, restored.Target);
        }

        private DeltaMaintenanceWorker NewWorker(DeltaMaintenanceBudget budget, IFileStorage storage, string table)
        {
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(2, null, TimeProvider.System), storage, table, table, null);
            budget.Register(worker);
            return worker;
        }
    }
}
