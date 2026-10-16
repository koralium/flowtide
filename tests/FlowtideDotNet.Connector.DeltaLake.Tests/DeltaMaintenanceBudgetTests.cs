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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaMaintenanceBudgetTests
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 12, 0, 0, 0, TimeSpan.Zero);

        private static DeltaMaintenanceBudget Budget(int jobs = 2, TimeProvider? clock = null, bool manualTick = true, TimeSpan? tickPeriod = null)
        {
            return new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(jobs, Path.GetFullPath("./maintenance-tests")), clock ?? new FakeTimeProvider(s_now), manualTick, tickPeriod);
        }

        private sealed class RecordingWorker : IMaintenanceWorker
        {
            private readonly Action<DateTimeOffset>? _tick;

            public RecordingWorker(Action<DateTimeOffset>? tick = null)
            {
                _tick = tick;
            }

            public int Ticks;

            public int? Thread;

            public void Tick(DateTimeOffset now)
            {
                Thread = Environment.CurrentManagedThreadId;
                Interlocked.Increment(ref Ticks);
                _tick?.Invoke(now);
            }
        }

        [Fact]
        public async Task GateIsFifo()
        {
            var gate = new FifoGate(1, new FakeTimeProvider(s_now));
            var held = await gate.AcquireAsync();
            var order = new List<string>();
            using var cancelB = new CancellationTokenSource();
            var a = gate.AcquireAsync().ContinueWith(t => { lock (order) { order.Add("a"); } return t.Result; }, TaskScheduler.Default);
            var b = gate.AcquireAsync(cancelB.Token);
            var c = gate.AcquireAsync().ContinueWith(t => { lock (order) { order.Add("c"); } return t.Result; }, TaskScheduler.Default);
            Assert.Equal(3, gate.QueueLength);

            // A cancelled waiter leaves the queue and is never handed a slot
            cancelB.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => b);
            Assert.Equal(2, gate.QueueLength);

            held.Dispose();
            (await a).Dispose();
            (await c).Dispose();
            Assert.Equal(new[] { "a", "c" }, order);
            Assert.Equal(0, gate.Running);
            Assert.Equal(0, gate.QueueLength);
        }

        [Fact]
        public async Task LeaseIsPerNormalizedPath()
        {
            var root = Path.GetFullPath("./lease-root");
            Directory.CreateDirectory(root);
            var local = TableLeases.KeyOf(Files.Of.LocalDisk(root), "t");
            var sameLocal = TableLeases.KeyOf(Files.Of.LocalDisk(root + Path.DirectorySeparatorChar), "/t/");
            Assert.Equal(local, sameLocal);
            if (OperatingSystem.IsWindows())
            {
                Assert.Equal(local, TableLeases.KeyOf(Files.Of.LocalDisk(root.ToUpperInvariant()), "T"));
            }
            // Object stores keep their case
            var memory = Files.Of.InternalMemory("./lease-memory");
            Assert.Equal(TableLeases.KeyOf(memory, "/t"), TableLeases.KeyOf(memory, "t/"));
            Assert.NotEqual(TableLeases.KeyOf(memory, "/t"), TableLeases.KeyOf(memory, "/T"));

            var leases = new TableLeases();
            var first = await leases.AcquireAsync(local);
            var second = leases.AcquireAsync(sameLocal);
            var other = leases.AcquireAsync(TableLeases.KeyOf(memory, "/t"));
            Assert.False(second.IsCompleted);
            Assert.True(other.IsCompleted);
            first.Dispose();
            (await second).Dispose();
            (await other).Dispose();
        }

        [Fact]
        public async Task ASameTableWaitDoesNotBlockOtherTables()
        {
            var budget = Budget(jobs: 2);
            var a = new JobAdmission();
            await budget.AdmitAsync("x", a, default);
            var b = new JobAdmission();
            var waiting = budget.AdmitAsync("x", b, default);
            // b waits for its table's lease and holds no slot, so another table still gets the second slot
            Assert.False(waiting.IsCompleted);
            Assert.Null(b.Slot);
            var c = new JobAdmission();
            var other = budget.AdmitAsync("y", c, default);
            Assert.True(other.IsCompleted, "A job on another table waited behind a same-table wait");
            Assert.Equal(2, budget.Gate.Running);

            c.Slot!.Dispose();
            c.Lease!.Dispose();
            a.Slot!.Dispose();
            a.Lease!.Dispose();
            await waiting;
            b.Slot!.Dispose();
            b.Lease!.Dispose();
            Assert.Equal(0, budget.Gate.Running);
        }

        [Fact]
        public async Task AdmissionClassesTakeTheirGates()
        {
            var budget = Budget(jobs: 1);
            Assert.Equal(1, budget.LongGate.Slots);
            Assert.Equal(2, budget.DeleteGate.Slots);
            var ordinary = new JobAdmission();
            await budget.AdmitAsync("a", ordinary, AdmissionClass.Ordinary, default);
            Assert.NotNull(ordinary.Slot);
            Assert.Null(ordinary.LongSlot);

            // A long job holds the long slot while it queues for the main slot
            var longJob = new JobAdmission();
            var longWait = budget.AdmitAsync("b", longJob, AdmissionClass.Long, default);
            Assert.False(longWait.IsCompleted);
            Assert.Equal(1, budget.LongGate.Running);
            Assert.Equal(1, budget.Gate.QueueLength);

            // Deletion takes neither the main nor the long gate
            var deletions = new[] { new JobAdmission(), new JobAdmission(), new JobAdmission() };
            await budget.AdmitAsync("c", deletions[0], AdmissionClass.Delete, default);
            await budget.AdmitAsync("d", deletions[1], AdmissionClass.Delete, default);
            var thirdDeletion = budget.AdmitAsync("e", deletions[2], AdmissionClass.Delete, default);
            Assert.False(thirdDeletion.IsCompleted);
            Assert.Equal(2, budget.DeleteGate.Running);
            Assert.Equal(1, budget.Gate.Running);
            Assert.Equal(1, budget.Gate.QueueLength);

            ordinary.Slot!.Dispose();
            await longWait;
            Assert.NotNull(longJob.Slot);
            deletions[0].Slot!.Dispose();
            await thirdDeletion;
            Assert.Equal(1, budget.Gate.Running);
            Assert.Equal(2, budget.DeleteGate.Running);

            // Long deletion queues for the long slot first, then holds it while it queues for a delete slot
            var longDeletion = new JobAdmission();
            var longDeletionWait = budget.AdmitAsync("f", longDeletion, AdmissionClass.LongDelete, default);
            Assert.False(longDeletionWait.IsCompleted);
            Assert.Equal(1, budget.LongGate.QueueLength);
            Assert.Equal(0, budget.DeleteGate.QueueLength);
            longJob.LongSlot!.Dispose();
            longJob.LongSlot = null;
            await DeltaMaintenanceWorkerTests.Queued(() => budget.DeleteGate.QueueLength == 1);
            Assert.Equal(1, budget.LongGate.Running);
            Assert.False(longDeletionWait.IsCompleted);
            deletions[1].Slot!.Dispose();
            deletions[1].Slot = null;
            await longDeletionWait;
            Assert.NotNull(longDeletion.LongSlot);
            Assert.Equal(1, budget.Gate.Running);
            Assert.Equal(2, budget.DeleteGate.Running);
            foreach (var admission in deletions.Append(ordinary).Append(longJob).Append(longDeletion))
            {
                admission.Slot?.Dispose();
                admission.LongSlot?.Dispose();
                admission.Lease?.Dispose();
            }
            Assert.Equal(0, budget.Gate.Running);
            Assert.Equal(0, budget.LongGate.Running);
            Assert.Equal(0, budget.DeleteGate.Running);
            Assert.Equal(0, budget.Leases.Count);
        }

        [Theory]
        [InlineData("main")]
        [InlineData("long")]
        [InlineData("delete")]
        public async Task OldestWaitGaugeCoversEveryGate(string wait)
        {
            var clock = new FakeTimeProvider(s_now);
            var budget = Budget(jobs: 1, clock: clock);
            using var meter = new Meter("oldest-wait-" + Guid.NewGuid().ToString("N"));
            DeltaMaintenanceBudget.SharedBudget.RegisterGauges(meter, budget);
            var gate = wait switch
            {
                "main" => budget.Gate,
                "long" => budget.LongGate,
                _ => budget.DeleteGate
            };
            var held = new List<IDisposable>();
            for (int i = 0; i < gate.Slots; i++)
            {
                held.Add(await gate.AcquireAsync());
            }
            using var cancel = new CancellationTokenSource();
            var waiter = gate.AcquireAsync(cancel.Token);
            Assert.Equal(0, Gauge(meter, "flowtide_delta_maintenance_oldest_wait_seconds"));
            clock.Advance(TimeSpan.FromSeconds(5));

            Assert.Equal(5, Gauge(meter, "flowtide_delta_maintenance_oldest_wait_seconds"));
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter);
            held.ForEach(x => x.Dispose());
            Assert.Equal(0, Gauge(meter, "flowtide_delta_maintenance_oldest_wait_seconds"));
        }

        // One observation of a gauge registered on this meter
        private static double Gauge(Meter meter, string name)
        {
            double? value = null;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, owner) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && instrument.Name == name)
                {
                    owner.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<double>((_, measurement, _, _) => value = measurement);
            listener.Start();
            listener.RecordObservableInstruments();
            Assert.True(value.HasValue, $"No {name} gauge");
            return value.Value;
        }

        [Fact]
        public async Task LeaseRecordsAreRemovedWhenIdle()
        {
            var leases = new TableLeases();
            for (int i = 0; i < 100; i++)
            {
                (await leases.AcquireAsync("table-" + i)).Dispose();
            }
            Assert.Equal(0, leases.Count);

            // A cancelled waiter and a handed-over lease leave no record either
            var held = await leases.AcquireAsync("x");
            using var cancel = new CancellationTokenSource();
            var cancelled = leases.AcquireAsync("x", cancel.Token);
            var next = leases.AcquireAsync("x");
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
            held.Dispose();
            (await next).Dispose();
            Assert.Equal(0, leases.Count);

            var alone = await leases.AcquireAsync("y");
            using var cancelAlone = new CancellationTokenSource();
            var waiter = leases.AcquireAsync("y", cancelAlone.Token);
            alone.Dispose();
            (await waiter).Dispose();
            Assert.Equal(0, leases.Count);
        }

        [Fact]
        public void TickNeverAwaits()
        {
            var budget = Budget();
            var workers = Enumerable.Range(0, 3).Select(_ => new RecordingWorker()).ToList();
            foreach (var worker in workers)
            {
                budget.Register(worker);
            }
            budget.TickNow();
            // Every worker was visited before the tick returned, on the calling thread
            Assert.All(workers, x => Assert.Equal(1, x.Ticks));
            Assert.All(workers, x => Assert.Equal(Environment.CurrentManagedThreadId, x.Thread));
        }

        [Fact]
        public void ATickFailureDoesNotStopOtherWorkers()
        {
            var budget = Budget();
            var first = new RecordingWorker();
            var failing = new RecordingWorker(_ => throw new InvalidOperationException("worker fault"));
            var last = new RecordingWorker();
            budget.Register(first);
            budget.Register(failing);
            budget.Register(last);

            budget.TickNow();
            budget.TickNow();
            Assert.Equal(2, first.Ticks);
            Assert.Equal(2, failing.Ticks);
            Assert.Equal(2, last.Ticks);
            Assert.Equal(2, budget.TickFailures);
        }

        [Fact]
        public void OverlappingTickIsSkipped()
        {
            var budget = Budget();
            var inner = 0;
            budget.Register(new RecordingWorker(_ =>
            {
                // A tick that arrives while one runs does nothing
                budget.TickNow();
                inner++;
            }));
            budget.TickNow();
            Assert.Equal(1, inner);
        }

        private static readonly AsyncLocal<string?> s_caller = new AsyncLocal<string?>();

        [Fact]
        public async Task JobsDoNotSeeTheCallersAsyncLocal()
        {
            var budget = Budget(clock: TimeProvider.System, manualTick: false, tickPeriod: TimeSpan.FromMilliseconds(20));
            var seen = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var worker = new RecordingWorker(_ => seen.TrySetResult(s_caller.Value));
            s_caller.Value = "registering stream";
            budget.Register(worker);
            s_caller.Value = null;
            try
            {
                var value = await seen.Task.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Null(value);
            }
            finally
            {
                budget.Unregister(worker);
            }
        }

        [Fact]
        public void TimerRunsOnlyWhileWorkersAreRegistered()
        {
            var clock = new FakeTimeProvider(s_now);
            var budget = Budget(clock: clock, manualTick: false);
            Assert.False(budget.TimerActive);

            var first = new RecordingWorker();
            var second = new RecordingWorker();
            budget.Register(first);
            budget.Register(second);
            Assert.True(budget.TimerActive);
            clock.Advance(DeltaMaintenanceBudget.TickPeriod);
            Assert.Equal(1, first.Ticks);

            budget.Unregister(first);
            Assert.True(budget.TimerActive);
            budget.Unregister(second);
            Assert.False(budget.TimerActive);
            clock.Advance(TimeSpan.FromSeconds(10));
            Assert.Equal(1, first.Ticks);
            Assert.Equal(1, second.Ticks);
        }

        [Fact]
        public void SharedSettingsFirstValueWins()
        {
            var shared = new DeltaMaintenanceBudget.SharedBudget();
            var settings = new MaintenanceBudgetSettings(2, Path.GetFullPath("./shared-a"));
            var budget = shared.Get(settings, out var matches);
            Assert.True(matches);
            Assert.Same(budget, shared.Get(settings with { MaxConcurrentJobs = 3 }, out matches));
            Assert.False(matches);
            Assert.Equal(2, budget.Gate.Slots);
        }

        [Fact]
        public void FactSequenceNumbersIncrease()
        {
            var budget = Budget();
            var first = budget.NextFactSequence();
            Assert.True(budget.NextFactSequence() > first);
        }
    }
}
