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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ProducerGenerationTests
    {
        private static readonly TimeSpan Long = TimeSpan.FromMinutes(1);

        private static ProducerEntry Entry() => new ProducerGenerations().Acquire("t").Entry;

        // The sink's outputs and its staged commit; the worker's checkpoint files are none of them
        private static string? KindOf(IOPath path)
        {
            var full = path.Full;
            if (full.Contains("/_delta_log/"))
            {
                return full.EndsWith(".tmp") && full.Contains(".json.") ? "stage" : null;
            }
            return full.Contains("/_change_data/") ? "cdc" : full.EndsWith(".bin") ? "dv" : full.EndsWith(".parquet") ? "data" : null;
        }

        private static async Task WaitUntil(FlowtideTestStream stream, Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (!condition())
            {
                Assert.True(DateTime.UtcNow < deadline, "The condition was not reached in time");
                await stream.SchedulerTick();
                await Task.Delay(50);
            }
        }

        [Fact]
        public async Task EnterFlipRaceNeverMissesOldGeneration()
        {
            var entry = Entry();
            Task<BarrierOutcome>? barrier = null;
            var raced = false;
            // The flip lands between the producer's increment and its recheck
            entry.AfterIncrementHookForTests = () =>
            {
                if (!raced)
                {
                    raced = true;
                    barrier = entry.FlipAndDrainAsync(Long, TimeProvider.System, default);
                    Assert.False(barrier.IsCompleted);
                }
            };
            var token = entry.Enter();

            // The producer retried into the new generation, its decrement released the barrier
            Assert.Equal(BarrierOutcome.Drained, await barrier!.WaitAsync(Long));
            Assert.Equal(1, entry.Generation);
            Assert.Equal(0, entry.Live(0));
            Assert.Equal(1, entry.Live(1));
            token.Exit();
            Assert.Equal(0, entry.Live(1));
        }

        [Fact]
        public async Task RetryDecrementWakesBarrier()
        {
            var entry = Entry();
            // A clock that never moves: only a signal can end the wait
            var clock = new FakeTimeProvider();
            Task<BarrierOutcome>? barrier = null;
            entry.AfterIncrementHookForTests = () =>
            {
                entry.AfterIncrementHookForTests = null;
                barrier = entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default);
            };
            var token = entry.Enter();

            // The barrier waited on the racing producer's count; the retry's decrement to zero woke it
            Assert.Equal(BarrierOutcome.Drained, await barrier!.WaitAsync(Long));
            token.Exit();
        }

        [Fact]
        public async Task ExitWakesBarrierWithoutAClock()
        {
            var entry = Entry();
            // A clock that never moves: only Exit's signal can end the wait
            var clock = new FakeTimeProvider();
            var holder = entry.Enter();
            var barrier = entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default);
            Assert.False(barrier.IsCompleted);
            holder.Exit();
            Assert.Equal(BarrierOutcome.Drained, await barrier.WaitAsync(TimeSpan.FromSeconds(10)));
        }

        [Fact]
        public void RetryingProducerCanMakeTheNextBarrierDeclineSpuriously()
        {
            var entry = Entry();
            var reads = 0;
            BarrierOutcome? second = null;
            // A whole barrier runs between the producer's read of generation 0 and its increment
            entry.BeforeIncrementHookForTests = () =>
            {
                if (++reads == 1)
                {
                    Assert.Equal(BarrierOutcome.Drained, entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult());
                }
            };
            // The stale increment sits in the next barrier's destination slot until the recheck undoes it
            entry.AfterIncrementHookForTests = () => second ??= entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult();
            var token = entry.Enter();

            // Safe, but a drain is not guaranteed while producers retry, so a stress test cannot assert Drained on every flip
            Assert.Equal(BarrierOutcome.StuckProducer, second);
            Assert.Equal(1, entry.Generation);
            token.Exit();
        }

        [Fact]
        public async Task EntryOutlivesItsLastHandleOnlyWhileInUse()
        {
            var generations = new ProducerGenerations();
            var handle = generations.Acquire("t");
            var token = handle.Entry.Enter();
            handle.Dispose();
            // A live producer keeps the entry, and its exit removes it
            Assert.Equal(1, generations.Count);
            token.Exit();
            Assert.Equal(0, generations.Count);

            handle = generations.Acquire("t");
            var entry = handle.Entry;
            var holder = entry.Enter();
            var barrier = entry.FlipAndDrainAsync(Long, new FakeTimeProvider(), default);
            handle.Dispose();
            holder.Exit();
            // A running barrier keeps it until the barrier ends
            Assert.Equal(BarrierOutcome.Drained, await barrier.WaitAsync(Long));
            Assert.Equal(0, generations.Count);
        }

        [Fact]
        public void AStalledRetryCanDeclineEveryFlipUntilItRechecks()
        {
            var generations = new ProducerGenerations();
            using var handle = generations.Acquire("t");
            var entry = handle.Entry;
            var declined = 0;
            entry.BeforeIncrementHookForTests = () =>
            {
                entry.BeforeIncrementHookForTests = null;
                Assert.Equal(BarrierOutcome.Drained, entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult());
            };
            // The producer stalls after its stale increment, before its generation recheck
            entry.AfterIncrementHookForTests = () =>
            {
                entry.AfterIncrementHookForTests = null;
                for (int i = 0; i < 200; i++)
                {
                    Assert.Equal(BarrierOutcome.StuckProducer, entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult());
                    declined++;
                }
            };

            var token = handle.Enter();
            token.Exit();
            Assert.Equal(200, declined);
            Assert.Equal(1, token.Generation);
            // Once it rechecks, the next barrier drains
            Assert.Equal(BarrierOutcome.Drained, entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult());
        }

        [Fact]
        public async Task TwoFlipAbaNeverAliasesOldGeneration()
        {
            var entry = Entry();
            var reads = 0;
            // Two whole flips land between the producer's read and its increment, the parity is the same again
            entry.BeforeIncrementHookForTests = () =>
            {
                reads++;
                if (reads == 1)
                {
                    Assert.Equal(BarrierOutcome.Drained, entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult());
                    Assert.Equal(BarrierOutcome.Drained, entry.FlipAndDrainAsync(Long, TimeProvider.System, default).GetAwaiter().GetResult());
                }
            };
            var token = entry.Enter();

            // The full generation differs, so the producer retried instead of entering generation 0's slot under generation 2
            Assert.Equal(2, reads);
            Assert.Equal(2, entry.Generation);
            Assert.Equal(1, entry.Live(2));
            token.Exit();
            await Task.CompletedTask;
        }

        [Fact]
        public async Task OneBarrierAtATime()
        {
            var entry = Entry();
            var holder = entry.Enter();
            var first = entry.FlipAndDrainAsync(Long, new FakeTimeProvider(), default);
            Assert.Equal(BarrierOutcome.Busy, await entry.FlipAndDrainAsync(Long, TimeProvider.System, default));
            holder.Exit();
            Assert.Equal(BarrierOutcome.Drained, await first.WaitAsync(Long));
            Assert.Equal(BarrierOutcome.Drained, await entry.FlipAndDrainAsync(Long, TimeProvider.System, default));
        }

        [Fact]
        public async Task TimeoutKeepsLiveCount()
        {
            var entry = Entry();
            var clock = new FakeTimeProvider();
            var holder = entry.Enter();
            var barrier = entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default);
            Assert.False(barrier.IsCompleted);
            clock.Advance(TimeSpan.FromSeconds(61));

            Assert.Equal(BarrierOutcome.Timeout, await barrier.WaitAsync(Long));
            Assert.Equal(1, entry.Live(0));
            Assert.Equal(1, entry.Generation);
            holder.Exit();
            Assert.Equal(0, entry.Live(0));
        }

        [Fact]
        public async Task StuckProducerDeclinesNextBarrier()
        {
            var entry = Entry();
            var clock = new FakeTimeProvider();
            var stuck = entry.Enter();
            var first = entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default);
            clock.Advance(TimeSpan.FromSeconds(61));
            Assert.Equal(BarrierOutcome.Timeout, await first.WaitAsync(Long));

            // The next flip would land the generation on the stuck producer's slot
            Assert.Equal(BarrierOutcome.StuckProducer, await entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default));
            Assert.Equal(1, entry.Generation);

            stuck.Exit();
            Assert.Equal(BarrierOutcome.Drained, await entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default));
        }

        [Fact]
        public async Task GenerationExhaustionDeclinesBarrier()
        {
            var entry = Entry();
            entry.SetGenerationForTests(long.MaxValue);
            Assert.Equal(BarrierOutcome.Exhausted, await entry.FlipAndDrainAsync(Long, TimeProvider.System, default));
            Assert.Equal(long.MaxValue, entry.Generation);
            // Producers still enter and leave
            var token = entry.Enter();
            Assert.Equal(1, entry.Live(long.MaxValue));
            token.Exit();
        }

        [Fact]
        public async Task CancelledBarrierKeepsTheFlipAndTheCount()
        {
            var entry = Entry();
            var holder = entry.Enter();
            using var cancellation = new CancellationTokenSource();
            var barrier = entry.FlipAndDrainAsync(Long, TimeProvider.System, cancellation.Token);
            cancellation.Cancel();
            Assert.Equal(BarrierOutcome.Cancelled, await barrier.WaitAsync(Long));
            Assert.Equal(1, entry.Live(0));
            holder.Exit();
        }

        [Fact]
        public async Task SamePathCloudTablesShareBarrierWithoutUnsafeRelease()
        {
            var first = Files.Of.InternalMemory($"./{nameof(SamePathCloudTablesShareBarrierWithoutUnsafeRelease)}-a");
            var second = Files.Of.InternalMemory($"./{nameof(SamePathCloudTablesShareBarrierWithoutUnsafeRelease)}-b");
            var generations = new ProducerGenerations();
            using var producerHandle = generations.Acquire(TableLeases.KeyOf(first, "t"));
            using var sweepHandle = generations.Acquire(TableLeases.KeyOf(second, "t"));
            Assert.Same(producerHandle.Entry, sweepHandle.Entry);

            var clock = new FakeTimeProvider();
            var other = producerHandle.Enter();
            // The other table's producer is waited for, never released early
            var barrier = sweepHandle.Entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default);
            Assert.False(barrier.IsCompleted);
            clock.Advance(TimeSpan.FromSeconds(61));
            Assert.Equal(BarrierOutcome.Timeout, await barrier.WaitAsync(Long));
            other.Exit();
            Assert.Equal(BarrierOutcome.Drained, await sweepHandle.Entry.FlipAndDrainAsync(TimeSpan.FromSeconds(60), clock, default));
        }

        [Fact]
        public void EntryLivesWhileAHandleHoldsIt()
        {
            var generations = new ProducerGenerations();
            var first = generations.Acquire("t");
            var second = generations.Acquire("t");
            first.Entry.SetGenerationForTests(5);
            first.Dispose();
            first.Dispose();
            Assert.Equal(1, generations.Count);
            Assert.Equal(5, generations.Acquire("t").Entry.Generation);
            second.Dispose();
        }

        [Fact]
        public async Task SaveDataWritesInsideAGeneration()
        {
            var testName = nameof(SaveDataWritesInsideAGeneration);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            DeltaLakeSink? sink = null;
            var writes = new ConcurrentQueue<string>();
            var outside = new ConcurrentQueue<string>();
            var phase = 0;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Small files, so the delete loop and the insert loop both roll their change data over
                options.MaxFileSizeBytes = 500;
                // Insert-only versions still write change files
                options.DeleteLimitsOverride = new DeleteLimits() { InsertChangeData = true };
                options.SinkCreatedForTests = created => sink = created;
            });
            void Check(string when, IOPath path)
            {
                var producers = sink?.ProducersForTests;
                if (producers == null || producers.Entry.Live(producers.Entry.Generation) == 0)
                {
                    outside.Enqueue($"{when} {path.Full}");
                }
            }
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && KindOf(path) is { } kind)
                {
                    writes.Enqueue($"{Volatile.Read(ref phase)} {kind}");
                    Check("open", path);
                }
                return Task.CompletedTask;
            };
            // A completed deferral still parks each output's disposal, which is where its publication settles
            storage.DeferPublication = path => KindOf(path) != null ? Task.CompletedTask : null;
            storage.PublicationParked = path => Check("dispose", path);
            stream.Generate(200);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            // A few deletes write deletion vectors and change data, many rewrite files
            Volatile.Write(ref phase, 1);
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);
            Volatile.Write(ref phase, 2);
            for (int i = 1; i < 60; i++)
            {
                stream.DeleteUser(stream.Users[i]);
            }
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Empty(outside);
            Assert.Contains("0 data", writes);
            Assert.Contains("0 cdc", writes);
            Assert.Contains("0 stage", writes);
            Assert.Contains("1 dv", writes);
            Assert.Contains("2 data", writes);
            // The delete loop rolled its change data over at least once
            Assert.True(writes.Count(x => x == "2 cdc") >= 2, string.Join(", ", writes));
            // Every token was released after staging
            var entry = sink!.ProducersForTests!.Entry;
            Assert.Equal(0, entry.Live(entry.Generation));
        }

        // An attempt keeps its token until each output's disposal settles, and a disposal that faults releases it as well
        [Theory]
        [InlineData("data", false)]
        [InlineData("cdc", false)]
        [InlineData("dv", false)]
        [InlineData("stage", false)]
        [InlineData("data", true)]
        [InlineData("dv", true)]
        [InlineData("stage", true)]
        public async Task DrainWaitsForOutputDisposal(string kind, bool fault)
        {
            var testName = nameof(DrainWaitsForOutputDisposal) + kind + fault;
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.SinkCreatedForTests = created => sink = created;
            });
            stream.Generate(100);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            var entry = sink!.ProducersForTests!.Entry;
            // Keeps the entry while a fault restarts the vertex
            using var sweep = stream.Maintenance.Budget.Producers.Acquire(entry.Key);

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var armed = 1;
            storage.DeferPublication = path => KindOf(path) == kind && Interlocked.Exchange(ref armed, 0) == 1 ? gate.Task : null;
            storage.PublicationParked = _ => parked.TrySetResult();
            var failures = stream.FailureNotificationCount;
            // One delete and one insert: a deletion vector, change data, a data file and the staged commit
            stream.DeleteUser(stream.Users[0]);
            stream.Generate(1);
            var published = WaitUntil(stream, () => parked.Task.IsCompleted);
            await parked.Task.WaitAsync(Long);
            await published;

            var generation = entry.Generation;
            Assert.Equal(1, entry.Live(generation));
            var barrier = sweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default);
            Assert.False(barrier.IsCompleted);
            if (fault)
            {
                gate.SetException(new CrashException());
            }
            else
            {
                gate.SetResult();
            }
            Assert.Equal(BarrierOutcome.Drained, await barrier.WaitAsync(Long));
            Assert.Equal(0, entry.Live(generation));

            if (fault)
            {
                await WaitUntil(stream, () => stream.FailureNotificationCount > failures);
                stream.Generate(1);
            }
            await WaitForVersionCheckpointing(storage, "test", stream, 1);
            Assert.Equal(0, entry.Live(entry.Generation));
            Assert.Equal(BarrierOutcome.Drained, await sweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default));
        }

        [Fact]
        public async Task GenerationsSurviveWorkerReplacement()
        {
            var testName = nameof(GenerationsSurviveWorkerReplacement);
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.SinkCreatedForTests = created => sink = created);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            var entry = sink!.ProducersForTests!.Entry;
            using var sweep = stream.Maintenance.Budget.Producers.Acquire(entry.Key);
            Assert.Equal(BarrierOutcome.Drained, await sweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default));

            // The engine disposes and initializes the reused vertex; the replacement producer joins the same entry
            await stream.Crash();
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            Assert.Same(entry, sink!.ProducersForTests!.Entry);
            Assert.Equal(1, entry.Generation);
        }

        [Fact]
        public async Task StuckProducerAndGenerationSurviveSinkRestartAfterSweepReleasesHandle()
        {
            var testName = nameof(StuckProducerAndGenerationSurviveSinkRestartAfterSweepReleasesHandle);
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.SinkCreatedForTests = created => sink = created);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            var entry = sink!.ProducersForTests!.Entry;
            var key = sink.ProducersForTests.Key;

            // A first attempt's token stuck past a barrier timeout, then the sweep's handle released
            var stuck = entry.Enter();
            using (var firstSweep = stream.Maintenance.Budget.Producers.Acquire(key))
            {
                var clock = new FakeTimeProvider();
                var wait = firstSweep.Entry.FlipAndDrainAsync(TimeSpan.FromSeconds(5), clock, default);
                clock.Advance(TimeSpan.FromSeconds(5));
                Assert.Equal(BarrierOutcome.Timeout, await wait.WaitAsync(Long));
            }
            Assert.Equal(1, entry.Generation);
            Assert.Equal(1, entry.Live(0));

            try
            {
                // The replacement sink and the next sweep's barrier still see the stuck producer and its generation
                await stream.Crash();
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, 1);

                using var nextSweep = stream.Maintenance.Budget.Producers.Acquire(key);
                Assert.Same(entry, sink!.ProducersForTests!.Entry);
                Assert.Equal(1, sink.ProducersForTests.Entry.Generation);
                Assert.Equal(BarrierOutcome.StuckProducer, await nextSweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default));
            }
            finally
            {
                stuck.Exit();
            }
        }

        // The settings fail with a test budget or with the process budget a sink finds
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public Task GenerationsSurviveSettingsFailure(bool shared)
        {
            return GenerationsSurviveInitFailure(nameof(GenerationsSurviveSettingsFailure) + shared, options =>
            {
                options.MaintenanceSettingsFailForTests = true;
                if (shared)
                {
                    options.SharedBudgetForTests = new DeltaMaintenanceBudget.SharedBudget(existing: options.MaintenanceBudgetOverride);
                    options.MaintenanceBudgetOverride = null;
                }
            });
        }

        [Fact]
        public Task GenerationsSurviveWorkerInitFailure()
        {
            return GenerationsSurviveInitFailure(nameof(GenerationsSurviveWorkerInitFailure), options => options.MaintenanceInitFailsForTests = true);
        }

        // The first sink in a process creates the budget and holds its producer entry, whether or not its worker starts
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task FirstSinkHoldsTheBudgetItCreates(bool workerFails)
        {
            var testName = nameof(FirstSinkHoldsTheBudgetItCreates) + workerFails;
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var source = new DeltaMaintenanceBudget.SharedBudget();
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.MaintenanceBudgetOverride = null;
                options.SharedBudgetForTests = source;
                options.MaintenanceTempDirectory = Path.Combine(Path.GetTempPath(), "flowtide-tests", testName);
                options.MaintenanceInitFailsForTests = workerFails;
                options.SinkCreatedForTests = created => sink = created;
            });
            Assert.Null(source.Existing);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            var budget = source.Existing;
            Assert.NotNull(budget);
            var handle = sink!.ProducersForTests;
            Assert.NotNull(handle);
            Assert.Equal(1, budget!.Producers.Count);
            using var sweep = budget.Producers.Acquire(TableLeases.KeyOf(storage, "/test"));
            Assert.Same(handle!.Entry, sweep.Entry);
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith(".tmp"))
                {
                    entered.TrySetResult();
                    await gate.Task;
                }
            };
            stream.Generate(1);
            var published = WaitForVersion(storage, "test", stream, 1);
            await entered.Task.WaitAsync(Long);
            var barrier = sweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default);
            Assert.False(barrier.IsCompleted);
            gate.SetResult();
            Assert.Equal(BarrierOutcome.Drained, await barrier.WaitAsync(Long));
            await published;
        }

        private static async Task GenerationsSurviveInitFailure(string testName, Action<DeltaLakeOptions> fail)
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                fail(options);
                options.SinkCreatedForTests = created => sink = created;
            });
            // A sweep that flipped before the sink started
            using var sweep = stream.Maintenance.Budget.Producers.Acquire(TableLeases.KeyOf(storage, "/test"));
            Assert.Equal(BarrierOutcome.Drained, await sweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default));
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            Assert.Empty(stream.Maintenance.Workers);

            // The sink joined the sweep's generation and its next barrier still waits for the producer
            var handle = sink!.ProducersForTests;
            Assert.NotNull(handle);
            Assert.Same(sweep.Entry, handle!.Entry);
            Assert.Equal(1, handle.Entry.Generation);
            Assert.Equal(1, stream.Maintenance.Budget.Producers.Count);
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith(".tmp"))
                {
                    entered.TrySetResult();
                    await gate.Task;
                }
            };
            stream.Generate(1);
            // Ticks the scheduler, so the checkpoint that stages version 1 starts
            var published = WaitForVersion(storage, "test", stream, 1);
            await entered.Task.WaitAsync(Long);
            var barrier = sweep.Entry.FlipAndDrainAsync(Long, TimeProvider.System, default);
            Assert.False(barrier.IsCompleted);
            gate.SetResult();
            Assert.Equal(BarrierOutcome.Drained, await barrier.WaitAsync(Long));
            await published;
        }
    }

    // Busy producers starve parallel tests, so these run alone
    [Collection(nameof(MaintenanceRaceCollection))]
    public class ProducerGenerationRaceTests
    {
        [Fact]
        public async Task ConcurrentProducersNeverHoldTheOldGenerationAfterADrain()
        {
            var entry = new ProducerGenerations().Acquire("t").Entry;
            var stop = 0;
            // The generation each producer holds, -1 while it holds none; set after Enter, cleared before Exit
            var held = new long[4];
            Array.Fill(held, -1L);
            var entries = new long[held.Length];
            using var ready = new CountdownEvent(held.Length);
            var producers = Enumerable.Range(0, held.Length).Select(index => Task.Factory.StartNew(() =>
            {
                var signalled = false;
                while (Volatile.Read(ref stop) == 0)
                {
                    var token = entry.Enter();
                    Volatile.Write(ref held[index], token.Generation);
                    Interlocked.Increment(ref entries[index]);
                    if (!signalled)
                    {
                        signalled = true;
                        ready.Signal();
                    }
                    Thread.SpinWait(20);
                    Volatile.Write(ref held[index], -1L);
                    token.Exit();
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToList();
            try
            {
                // Every producer has entered before the first flip
                Assert.True(ready.Wait(TimeSpan.FromMinutes(1)), "The producers never entered");
                var enteredBefore = entries.Sum();
                var drained = 0;
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
                while (drained < 200)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"only {drained} of 200 flips drained in time");
                    var before = entry.Generation;
                    var outcome = await entry.FlipAndDrainAsync(TimeSpan.FromMinutes(1), TimeProvider.System, default);
                    // A producer retrying across two flips declines barriers until it rechecks, so it gets the processor and only drains count
                    if (outcome == BarrierOutcome.StuckProducer)
                    {
                        await Task.Delay(1);
                        continue;
                    }
                    Assert.Equal(BarrierOutcome.Drained, outcome);
                    drained++;
                    // No producer still holds a generation the drain finished
                    for (int p = 0; p < held.Length; p++)
                    {
                        var generation = Volatile.Read(ref held[p]);
                        Assert.True(generation < 0 || generation > before, $"producer {p} holds {generation} after draining {before}");
                    }
                }
                Assert.True(entries.Sum() > enteredBefore, "No producer entered while the flips ran");
            }
            finally
            {
                Volatile.Write(ref stop, 1);
                await Task.WhenAll(producers).WaitAsync(TimeSpan.FromMinutes(1));
            }
        }
    }
}
