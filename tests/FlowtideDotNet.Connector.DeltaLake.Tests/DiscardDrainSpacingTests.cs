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
using System.Reflection;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DiscardDrainSpacingTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "dd-" + ScratchOwners.ShortId());

        public DiscardDrainSpacingTests()
        {
            Directory.CreateDirectory(_directory);
        }

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

        [Fact]
        public async Task DrainDownloadsTheNewestCheckpointTwicePerTurn()
        {
            await using var run = SinkRun.Start(nameof(DrainDownloadsTheNewestCheckpointTwicePerTurn), _directory, options => options.CheckpointInterval = 2);
            await run.Started();
            for (int version = 1; version <= 2; version++)
            {
                run.Stream.Generate(20);
                await WaitForVersion(run.Local, "test", run.Stream, version);
            }
            await run.Stream.Maintenance.TickAndSettleAsync(2);
            Assert.True(File.Exists(Path.Combine(run.TableDir, "_delta_log", $"{2:D20}.checkpoint.parquet")));
            // Version 3 is not due for a build, so only its discard drain runs
            run.Stream.Generate(20);
            await WaitForVersion(run.Local, "test", run.Stream, 3);
            run.Storage.ClearRequests();
            await run.Stream.Maintenance.TickAndSettleAsync(3);
            Assert.Equal(0, run.Sink.DiscardRouteForTests!.Entry.Hints);
            // Measured: the capability replay and the slice gate each download the whole newest checkpoint, once per turn
            var roots = run.Storage.Requests.Count(x => x.StartsWith("OpenRead") && x.EndsWith(".checkpoint.parquet"));
            Assert.Equal(2, roots);
        }

        [Fact]
        public async Task DrainTurnsDoNotRunPerCommitAtIntervalZero()
        {
            await using var run = SinkRun.Start(nameof(DrainTurnsDoNotRunPerCommitAtIntervalZero), _directory);
            run.Stream.Maintenance.Budget.Discards.DrainSpacing = DiscardHandOff.DefaultDrainSpacing;
            await run.Started();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            var entry = run.Sink.DiscardRouteForTests!.Entry;
            var reads = new List<int>();
            for (int version = 1; version <= 12; version++)
            {
                run.Stream.Generate(20);
                await WaitForVersion(run.Local, "test", run.Stream, version);
                run.Storage.ClearRequests();
                await run.Stream.Maintenance.TickAndSettleAsync(version);
                reads.Add(run.Storage.Requests.Count(x => x.StartsWith("OpenRead") && x.EndsWith(".json")));
            }
            // Within the spacing no commit starts a drain, so no commit pays a replay of the log before it
            Assert.True(reads[^1] <= reads[0] + 1, $"commit reads per drain: {string.Join(",", reads)}");
            Assert.All(reads, x => Assert.Equal(0, x));
            // Fewer hints than a turn takes, so only the spacing could have started a drain
            Assert.InRange(entry.Hints, 1, DeltaMaintenanceWorker.MaxHintsPerTurn - 1);
        }

        [Fact]
        public async Task CommitsWithinTheSpacingDrainInOneTurn()
        {
            await using var run = SinkRun.Start(nameof(CommitsWithinTheSpacingDrainInOneTurn), _directory);
            var handOff = run.Stream.Maintenance.Budget.Discards;
            handOff.DrainSpacing = DiscardHandOff.DefaultDrainSpacing;
            await run.Started();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            const int Head = 5;
            for (int version = 1; version <= Head; version++)
            {
                run.Stream.Generate(20);
                await WaitForVersion(run.Local, "test", run.Stream, version);
                await run.Stream.Maintenance.TickAndSettleAsync(version);
            }
            var entry = run.Sink.DiscardRouteForTests!.Entry;
            var discarded = run.ChangeData();
            Assert.Equal(discarded.Count, entry.Hints);
            Assert.InRange(entry.Hints, Head + 1, DeltaMaintenanceWorker.MaxHintsPerTurn);
            Assert.Equal(0, run.Sink.DiscardsForTests.Deleted);

            // Once the oldest waited the spacing, one turn drains every commit's hints from one checked listing, one replay and one slice
            run.Storage.ClearRequests();
            run.Clock.Advance(handOff.DrainSpacing);
            await run.Stream.Maintenance.TickAndSettleAsync(Head);
            Assert.Equal(0, entry.Hints);
            Assert.All(discarded, x => Assert.False(run.Exists(x)));
            Assert.Equal(discarded.Count, run.Sink.DiscardsForTests.Deleted);
            Assert.Equal(2, run.Storage.Requests.Count(x => x == "Stat " + CommitPath("test", Head + 1)));
            Assert.Equal(2, run.Storage.Requests.Count(x => x == "OpenRead " + CommitPath("test", 0)));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public void DueOnceDrainableHintsFillATurnWaitedTheSpacingOrPressTheAllowance()
        {
            using var storage = new ScriptedStorage();
            var handOff = new DiscardHandOff();
            using var producer = handOff.Acquire(new DiscardRoute(storage, "t"));
            var entry = producer.Entry;
            var spacing = handOff.DrainSpacing;
            Assert.Equal(TimeSpan.FromMinutes(5), spacing);

            // Version 1 queued now, version 2 a spacing ago with more hints than a turn takes
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", new[] { "a" }) { QueuedAt = s_now }));
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 2, "stage-2", Paths("b", 200)) { QueuedAt = s_now - spacing }));
            // Queued since the last drain, no hint counts before one is at or below the head, then every one does
            Assert.False(entry.IsDue(0, s_now + spacing, 128));
            Assert.True(entry.IsDue(1, s_now, 128));
            Assert.True(entry.IsDue(2, s_now, 128));
            // The drainer's view counts each group at its own version
            entry.Pending();
            Assert.False(entry.IsDue(1, s_now, 128));
            Assert.False(entry.IsDue(1, s_now + spacing - TimeSpan.FromTicks(1), 128));
            Assert.True(entry.IsDue(1, s_now + spacing, 128));
            // At its head the older group's batch is due at once
            Assert.True(entry.IsDue(2, s_now, 128));

            // A batch at the head is due before the spacing
            using var batched = handOff.Acquire(new DiscardRoute(storage, "batched"));
            Assert.Equal(0, batched.Entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", Paths("c", 127)) { QueuedAt = s_now }));
            Assert.False(batched.Entry.IsDue(1, s_now, 128));
            Assert.Equal(0, batched.Entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", Paths("d", 1)) { QueuedAt = s_now }));
            Assert.True(batched.Entry.IsDue(1, s_now, 128));

            // A failure waiting for its retry is not due before it, however long ago it was queued
            using var retried = handOff.Acquire(new DiscardRoute(storage, "retried"));
            var failing = new DiscardGroup("id", 0, 1, "stage-1", Paths("f", 2)) { QueuedAt = s_now - spacing };
            Assert.Equal(0, retried.Entry.Enqueue(failing));
            retried.Entry.Pending();
            Assert.Equal(1, retried.Entry.Conclude(failing, new[] { HintState.Failed, HintState.Settled }, s_now));
            retried.Entry.EndDrain();
            Assert.False(retried.Entry.IsDue(1, s_now, 128));
            Assert.True(retried.Entry.IsDue(1, s_now + DiscardHandOff.RetryDelay(1), 128));

            // With three quarters of the allowance charged, every drainable hint is due
            var pressed = new DiscardHandOff(4096);
            using var small = pressed.Acquire(new DiscardRoute(storage, "small"));
            using var large = pressed.Acquire(new DiscardRoute(storage, "large"));
            Assert.Equal(0, small.Entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", new[] { "e" }) { QueuedAt = s_now }));
            Assert.False(small.Entry.IsDue(1, s_now, 128));
            Assert.True(pressed.TryCharge(pressed.Capacity - pressed.Capacity / 4 - pressed.Charged));
            Assert.True(pressed.UnderPressure);
            Assert.True(small.Entry.IsDue(1, s_now, 128));
            Assert.False(small.Entry.IsDue(0, s_now, 128));
        }

        [Fact]
        public void DueCheckLeavesTheInboxToItsProducers()
        {
            using var storage = new ScriptedStorage();
            var handOff = new DiscardHandOff();
            using var producer = handOff.Acquire(new DiscardRoute(storage, "t"));
            var entry = producer.Entry;
            var paths = new[] { "_change_data/a.parquet" };
            // Warmed up, so only what a due check leaves behind allocates
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", paths) { QueuedAt = s_now }));
            Assert.False(entry.IsDue(1, s_now, 128));
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 2, "stage-2", paths) { QueuedAt = s_now }));
            for (long version = 3; version <= 5; version++)
            {
                var group = new DiscardGroup("id", 0, version, "stage", paths) { QueuedAt = s_now };
                Assert.True(entry.IsDue(version, s_now + handOff.DrainSpacing, 128));
                var before = GC.GetAllocatedBytesForCurrentThread();
                var dropped = entry.Enqueue(group);
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.Equal(0, dropped);
                Assert.Equal(0L, allocated);
            }
            Assert.Equal(5, entry.Hints);
        }

        [Fact]
        public void HintsFromAPublishThatFailedClosedAreDueAtOnce()
        {
            using var storage = new ScriptedStorage();
            var handOff = new DiscardHandOff();
            using var producer = handOff.Acquire(new DiscardRoute(storage, "t"));
            var entry = producer.Entry;
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", new[] { "a" }) { QueuedAt = s_now }));
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 2, "stage-2", new[] { "b" }) { QueuedAt = s_now }));

            // Within the spacing and above the head, until the publish of version 2 fails closed
            Assert.False(entry.IsDue(1, s_now, 128));
            var mark = new FailedClosedMark(2, "stage-2");
            entry.MarkFailedClosed(mark);
            Assert.True(entry.IsDue(1, s_now, 128));
            // Read from the drainer's view as from the inbox
            entry.Pending();
            Assert.True(entry.IsDue(1, s_now, 128));
            // Only the mark a turn read is cleared
            Assert.False(entry.ClearFailedClosed(new FailedClosedMark(2, "stage-2")));
            Assert.True(entry.ClearFailedClosed(mark));
            Assert.Null(entry.FailedClosed);
            Assert.False(entry.IsDue(1, s_now, 128));

            // A mark on a route without hints makes no turn due
            using var empty = handOff.Acquire(new DiscardRoute(storage, "empty"));
            empty.Entry.MarkFailedClosed(new FailedClosedMark(1, "stage-1"));
            Assert.False(empty.Entry.IsDue(1, s_now, 128));
        }

        [Fact]
        public void AStaleInboxSummaryMakesNoRouteWithoutHintsDue()
        {
            using var storage = new ScriptedStorage();
            var handOff = new DiscardHandOff();
            using var producer = handOff.Acquire(new DiscardRoute(storage, "t"));
            var entry = producer.Entry;
            var spacing = handOff.DrainSpacing;
            var group = new DiscardGroup("id", 0, 1, "stage-1", new[] { "a" }) { QueuedAt = s_now };
            Assert.Equal(0, entry.Enqueue(group));

            // The drainer took the group and settled it, then the summary of an Enqueue that raced the take landed
            Assert.True(entry.TryBeginDrain());
            entry.Pending();
            entry.Settle(group, 1);
            entry.Prune();
            entry.EndDrain();
            SetInboxSummary(entry, 1, group.Version, group.QueuedAt);
            Assert.Equal(0, entry.Hints);
            Assert.False(entry.IsDue(1, s_now + spacing, 128));

            // A route with hints still counts its inbox
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("id", 0, 1, "stage-1", new[] { "b" }) { QueuedAt = s_now }));
            Assert.True(entry.IsDue(1, s_now + spacing, 128));
        }

        // The inbox summary fields as a producer's Interlocked updates leave them
        private static void SetInboxSummary(DiscardEntry entry, long hints, long lowest, DateTimeOffset oldest)
        {
            var flags = BindingFlags.NonPublic | BindingFlags.Instance;
            typeof(DiscardEntry).GetField("_inboxHints", flags)!.SetValue(entry, hints);
            typeof(DiscardEntry).GetField("_inboxLowest", flags)!.SetValue(entry, lowest);
            typeof(DiscardEntry).GetField("_inboxOldest", flags)!.SetValue(entry, oldest.UtcTicks);
        }

        private static string[] Paths(string prefix, int count) => Enumerable.Range(0, count).Select(i => $"_change_data/{prefix}{i}.parquet").ToArray();
    }
}
