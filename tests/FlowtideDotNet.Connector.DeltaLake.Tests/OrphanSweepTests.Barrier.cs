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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // Well past the moment a running sweep reaches its barrier
        private static readonly TimeSpan s_timerWait = TimeSpan.FromSeconds(30);

        // The barrier's wait started while the sweep still ran
        private static async Task BarrierWaitsAsync(TimerClock clock, Task sweeping)
        {
            var timer = clock.NextTimerAsync(s_timerWait);
            Assert.Same(timer, await Task.WhenAny(timer, sweeping));
            await timer;
        }

        [Fact]
        public async Task BlockedOldProducerTimesOutWithoutDeleting()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var clock = new TimerClock(Now);
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(ProducerKey(table));
            var old = holder.Enter();
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, clock, counters);
            var runs = Registered(sweep);

            var sweeping = RunAsync(sweep, table, OrphanContext(2, producers: producers));
            await BarrierWaitsAsync(clock, sweeping);
            Assert.Equal(1, holder.Entry.Generation);
            clock.Fake.Advance(OrphanSweep.BarrierTimeout + TimeSpan.FromSeconds(1));
            var (result, seen) = await sweeping;
            old.Exit();

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.BarrierTimeout);
            Assert.Equal(1, counters.Barrier(BarrierOutcome.Timeout));
            Assert.Equal("old.parquet", Assert.Single(seen!.Frozen).Relative);
            // No log listing follows the root listing
            Assert.DoesNotContain(table.Owned.CallsAfterRecursive(), x => x.StartsWith("Ls ", StringComparison.Ordinal));
            Assert.Empty(runs);
        }

        [Fact]
        public async Task PostFlipProducerFilesNeverBecomeCandidates()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var clock = new TimerClock(Now);
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(ProducerKey(table));
            var old = holder.Enter();
            var sweep = Sweep(table.Configuration, table.Owned, clock);
            var runs = Registered(sweep);

            var sweeping = RunAsync(sweep, table, OrphanContext(2, producers: producers));
            await BarrierWaitsAsync(clock, sweeping);
            // A producer of the new generation writes an output that looks old, then the old producer ends
            var fresh = holder.Enter();
            Plant(table, "postflip.parquet");
            old.Exit();
            var (result, seen) = await sweeping;
            fresh.Exit();

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(1, fresh.Generation);
            Assert.Equal("old.parquet", Assert.Single(seen!.Frozen).Relative);
            Assert.Equal(new[] { "old.parquet" }, TargetsOf(Assert.Single(runs), table));
            Assert.True(File.Exists(Path.Combine(table.Folder!, "postflip.parquet")));
        }

        [Fact]
        public async Task StuckDestinationGenerationDeclines()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var clock = new TimerClock(Now);
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(ProducerKey(table));
            // A producer left in generation 0 by a barrier that timed out on it
            var stuck = holder.Enter();
            Assert.Equal(BarrierOutcome.Timeout, await holder.Entry.FlipAndDrainAsync(TimeSpan.Zero, TimeProvider.System, default));
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, clock, counters);
            var runs = Registered(sweep);

            var (result, _) = await RunAsync(sweep, table, OrphanContext(2, producers: producers));
            stuck.Exit();

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.StuckProducer);
            Assert.Equal(1, counters.Barrier(BarrierOutcome.StuckProducer));
            Assert.Equal(0, counters.Barrier(BarrierOutcome.Timeout));
            // Declined before any wait
            await Assert.ThrowsAsync<TimeoutException>(() => clock.NextTimerAsync(TimeSpan.Zero));
            Assert.DoesNotContain(table.Owned.CallsAfterRecursive(), x => x.StartsWith("Ls ", StringComparison.Ordinal));
            Assert.Empty(runs);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task HostSuspendDoesNotDefeatBarrier(bool publishes)
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "inflight.parquet");
            var clock = new TimerClock(Now);
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(ProducerKey(table));
            // The writer of inflight.parquet, whose output already looks old
            var producer = holder.Enter();
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, clock, counters);
            var runs = Registered(sweep);

            var sweeping = RunAsync(sweep, table, OrphanContext(2, producers: producers));
            await BarrierWaitsAsync(clock, sweeping);
            if (publishes)
            {
                // It stages a commit naming its output, then ends
                await StageAsync(table, 3, "inflight.parquet");
                producer.Exit();
            }
            else
            {
                // The host sleeps for days while the writer still holds its generation
                clock.Fake.Advance(TimeSpan.FromDays(3));
            }
            var (result, seen) = await sweeping;
            if (!publishes)
            {
                producer.Exit();
            }

            Assert.Equal(new[] { "inflight.parquet", "old.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            if (publishes)
            {
                AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
                Assert.Equal(new[] { "old.parquet" }, TargetsOf(Assert.Single(runs), table));
            }
            else
            {
                AssertResult(result, OutcomeKind.Declined, MaintenanceReason.BarrierTimeout);
                Assert.Equal(1, counters.Barrier(BarrierOutcome.Timeout));
                Assert.Empty(runs);
            }
        }

        // The key a started sink's producers enter under, read without a request of the test's own
        private static async Task<string> SinkProducerKeyAsync(IFileStorage storage, string table, Func<bool> published)
        {
            DeltaLakeSink? sink = null;
            var name = table.Replace('/', '.');
            await using var stream = new DeltaLakeSinkStream(nameof(BarrierKeyIsTheSinkProducersKey) + ScratchOwners.ShortId(), storage, options =>
            {
                options.SinkCreatedForTests = created => sink = created;
                // Producers are held before the worker starts, which then fails and requests nothing
                options.MaintenanceInitFailsForTests = true;
            });
            stream.Generate(1);
            await stream.StartStream($"CREATE TABLE {name} (userkey INT); INSERT INTO {name} SELECT userKey AS userkey FROM users");
            // Fully started before the disposal, read from the store's side
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (!published())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The sink did not publish version 0");
                }
                await stream.SchedulerTick();
                await Task.Delay(50);
            }
            return sink!.ProducersForTests!.Key;
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        // Local disk
        [InlineData(null)]
        public async Task BarrierKeyIsTheSinkProducersKey(StoreFlavor? flavor)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            // A cloud key is the table path alone; the sink writes to a second store of the flavor
            await using var sinkServer = flavor is { } other ? new LoopbackObjectStore(other) : null;
            OrphanTable table;
            string sinkKey;
            if (server != null)
            {
                var sinkStorage = sinkServer!.CreateStorage();
                _disposables.Add(sinkStorage);
                sinkKey = await SinkProducerKeyAsync(sinkStorage, "c/tables/t", () => sinkServer!.Contains($"tables/t/_delta_log/{0:D20}.json"));
                server.Now = s_old;
                table = LoopbackTable(server);
                await RetainedTableAsync(table.Raw, table.Name, 2);
                server.Now = Now;
            }
            else
            {
                // The sink starts in the folder the table then replaces
                var root = Path.Combine(_directory, ScratchOwners.ShortId());
                Directory.CreateDirectory(root);
                var sinkStorage = Files.Of.LocalDisk(root);
                _disposables.Add(sinkStorage);
                sinkKey = await SinkProducerKeyAsync(sinkStorage, "t", () => File.Exists(Path.Combine(root, "t", "_delta_log", $"{0:D20}.json")));
                if (Directory.Exists(Path.Combine(root, "t")))
                {
                    Directory.Delete(Path.Combine(root, "t"), true);
                }
                table = LocalTable(root);
                await RetainedTableAsync(table.Raw, table.Name, 2);
            }
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(sinkKey);
            var producer = holder.Enter();
            var clock = new TimerClock(Now);

            var sweeping = RunAsync(Sweep(table.Configuration, table.Owned, clock), table, OrphanContext(2, producers: producers));
            await BarrierWaitsAsync(clock, sweeping);
            producer.Exit();
            var (result, _) = await sweeping;

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(1, holder.Entry.Generation);
        }

        [Fact]
        public async Task PostBarrierPublishedFloorIsReadAgain()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var published = 2L;
            var reads = new List<long>();
            var context = ClockedContext(new FakeTimeProvider(Now), new ProducerGenerations(), () =>
            {
                reads.Add(published);
                return published;
            });
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            var runs = Registered(sweep);
            sweep.StepForTests = step =>
            {
                if (step == OrphanSweepStep.Drained)
                {
                    // The published hand-off moves on while L2 still returns the old prefix
                    published = 3;
                }
                return Task.CompletedTask;
            };

            var (result, seen) = await RunAsync(sweep, table, context);

            Assert.Equal(2, seen!.VInventory);
            Assert.Equal(new long[] { 2, 3 }, reads);
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.RangeGap));
            Assert.Empty(runs);
        }
    }
}
