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
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // The maintenance delete switch is process-wide, so classes that set it never run beside another test
    [CollectionDefinition(nameof(MaintenanceSwitchCollection), DisableParallelization = true)]
    public class MaintenanceSwitchCollection
    {
    }

    [Collection(nameof(MaintenanceSwitchCollection))]
    public class DiscardKillSwitchTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ks-" + ScratchOwners.ShortId());

        public DiscardKillSwitchTests()
        {
            Directory.CreateDirectory(_directory);
            Switch(false);
        }

        public void Dispose()
        {
            Switch(false);
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

        private static void Switch(bool on) => AppContext.SetSwitch(MaintenanceSwitches.DisableMaintenanceDeletes, on);

        private static bool IsProbe(string request) => request.StartsWith("Stat /test/_delta_log/", StringComparison.Ordinal);

        [Fact]
        public async Task KillSwitchStopsBackgroundIssuance()
        {
            await using var run = SinkRun.Start(nameof(KillSwitchStopsBackgroundIssuance), _directory);
            await run.Started();
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var first = run.ChangeData();
            Assert.NotEmpty(first);
            Assert.Equal(first.Count, entry.Hints);
            var worker = Assert.Single(run.Stream.Maintenance.Workers);

            // Switched on during a queued run: the drain issues nothing, not even a checked listing, and keeps every hint
            Switch(true);
            run.Storage.ClearRequests();
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.NotNull(worker.SweepDueAt);
            Assert.DoesNotContain(run.Storage.Requests, x => IsProbe(x) || x.StartsWith("OpenRead ", StringComparison.Ordinal));
            Assert.Equal(first.Count, entry.Hints);
            Assert.All(first, x => Assert.True(run.Exists(x)));

            // The run keeps its mode: a new attempt's discards are queued, never deleted inline
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 1);
            var second = run.ChangeData();
            Assert.True(second.Count > first.Count);
            Assert.Equal(DiscardMode.Queued, sink.DiscardModeForTests);
            Assert.Equal(second.Count, entry.Hints);
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            Assert.All(second, x => Assert.True(run.Exists(x)));

            // Switched on after a slice passed its gate: issuance stops before the first delete
            Switch(false);
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            await run.WaitForTarget(1);
            // Only maintenance probes a successor, once for each checked listing: the establishment's, then the slice's
            var start = run.Storage.Requests.Count(IsProbe);
            run.Storage.Before = (verb, path) =>
            {
                if (verb == "Stat" && run.Storage.Requests.Count(IsProbe) == start + 2)
                {
                    Switch(true);
                }
                return Task.CompletedTask;
            };
            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.Equal(start + 2, run.Storage.Requests.Count(IsProbe));
            Assert.Equal(second.Count, entry.Hints);
            Assert.Equal(0, sink.DiscardsForTests.Deleted);
            Assert.All(second, x => Assert.True(run.Exists(x)));

            // Switched off again, the same run drains them
            run.Storage.Before = null;
            Switch(false);
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(second.Count, sink.DiscardsForTests.Deleted);
            Assert.All(second, x => Assert.False(run.Exists(x)));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task RestartWithKillSwitchUsesInlineFallback()
        {
            await using var run = SinkRun.Start(nameof(RestartWithKillSwitchUsesInlineFallback), _directory);
            await run.Started();
            var sink = run.Sink;
            var entry = sink.DiscardRouteForTests!.Entry;
            var first = run.ChangeData();
            Assert.Equal(first.Count, entry.Hints);

            // A handle standing in for a detached drain keeps the route through the restart
            Assert.True(run.Stream.Maintenance.Budget.Discards.TryAcquire(entry, out var detached));
            using (detached)
            {
                var worker = Assert.Single(run.Stream.Maintenance.Workers);
                Switch(true);
                await run.Stream.Crash();
                await SinkRun.WaitForNewWorker(run.Stream, worker);

                // The restarted run reads the switch: inline, and the hints it would inherit are dropped, never deleted inline
                Assert.Equal(DiscardMode.Inline, sink.DiscardModeForTests);
                Assert.Null(sink.DiscardRouteForTests);
                Assert.Equal(0, entry.Hints);
                Assert.Equal(first.Count, sink.DiscardsForTests.Dropped);
                Assert.All(first, x => Assert.True(run.Exists(x)));
            }

            // The attempt deletes its own discards before it stages
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 1);
            Assert.Equal(first, run.ChangeData());
            var deleted = sink.DiscardsForTests.Deleted;
            Assert.True(deleted > 0);
            Assert.Equal(0, run.Stream.Maintenance.Budget.Discards.Queued);

            // Background issuance stays off
            run.Storage.ClearRequests();
            await run.WaitForTarget(1);
            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.DoesNotContain(run.Storage.Requests, x => x.StartsWith("Stat ", StringComparison.Ordinal));

            // The inline fallback deletes through the storage it was made for; a replaced instance keeps the discards
            using var replaced = Files.Of.LocalDisk(run.Root);
            run.Options.StorageLocation = OwnedHookFileStorage.Over(replaced);
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 2);
            var kept = run.ChangeData().Except(first).ToList();
            Assert.NotEmpty(kept);
            Assert.Equal(kept.Count, sink.DiscardsForTests.Kept);
            Assert.Equal(deleted, sink.DiscardsForTests.Deleted);
            // Only the injected crash
            Assert.Equal(1, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task RestartRereadsKillSwitchOutsideConstructionSnapshot()
        {
            Switch(true);
            await using var run = SinkRun.Start(nameof(RestartRereadsKillSwitchOutsideConstructionSnapshot), _directory);
            await run.Started();
            var sink = run.Sink;
            var cleanup = sink.CleanupForTests;
            Assert.Equal(DiscardMode.Inline, sink.DiscardModeForTests);
            Assert.Null(sink.DiscardRouteForTests);
            Assert.Empty(run.ChangeData());
            Assert.True(sink.DiscardsForTests.Deleted > 0);

            // The reused vertex reads the switch again at its restart; its construction snapshot is unchanged
            Switch(false);
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            await run.Stream.Crash();
            worker = await SinkRun.WaitForNewWorker(run.Stream, worker);
            Assert.Equal(1, run.SinksConstructed);
            Assert.Same(cleanup, sink.CleanupForTests);
            Assert.Equal(DiscardMode.Queued, sink.DiscardModeForTests);
            run.Stream.Generate(100);
            await WaitForVersion(run.Local, "test", run.Stream, 1);
            var queued = run.ChangeData();
            Assert.NotEmpty(queued);
            Assert.Equal(queued.Count, sink.DiscardRouteForTests!.Entry.Hints);

            Switch(true);
            await run.Stream.Crash();
            await SinkRun.WaitForNewWorker(run.Stream, worker);
            Assert.Equal(1, run.SinksConstructed);
            Assert.Same(cleanup, sink.CleanupForTests);
            Assert.Equal(DiscardMode.Inline, sink.DiscardModeForTests);
            // Only the injected crashes
            Assert.Equal(2, run.Stream.FailureNotificationCount);
        }
    }
}
