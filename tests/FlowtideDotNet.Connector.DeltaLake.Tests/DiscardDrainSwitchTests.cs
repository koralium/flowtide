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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(MaintenanceSwitchCollection))]
    public class DiscardDrainSwitchTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ds-" + ScratchOwners.ShortId());

        public DiscardDrainSwitchTests()
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

        [Fact]
        public async Task KillSwitchedDrainDeclinesAndBacksOff()
        {
            await using var run = SinkRun.Start(nameof(KillSwitchedDrainDeclinesAndBacksOff), _directory);
            await run.Started();
            var entry = run.Sink.DiscardRouteForTests!.Entry;
            var handOff = run.Stream.Maintenance.Budget.Discards;
            var discarded = run.ChangeData();
            Assert.NotEmpty(discarded);
            var worker = Assert.Single(run.Stream.Maintenance.Workers);

            // With the switch on, each turn keeps the hints, holds them back from the pressure and declines, doubling the wait
            Switch(true);
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeletesDisabled), (worker.LastOutcomeForTests!.Kind, worker.LastOutcomeForTests.Reason));
            Assert.Equal(entry.Charged, handOff.Held);
            Assert.Equal(1, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(1), worker.CleanupRetryAt(JobKind.Deletion));
            run.Clock.Advance(TimeSpan.FromMinutes(1));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(2, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(2), worker.CleanupRetryAt(JobKind.Deletion));
            Assert.Equal(discarded.Count, entry.Hints);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));

            // Switched on after the slice passed its gate, the turn declines too
            Switch(false);
            run.Clock.Advance(TimeSpan.FromMinutes(2));
            var probes = 0;
            run.Storage.Before = (verb, path) =>
            {
                if (verb == "Stat" && path.Full.StartsWith("/test/_delta_log/", StringComparison.Ordinal) && ++probes == 2)
                {
                    Switch(true);
                }
                return Task.CompletedTask;
            };
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(2, probes);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeletesDisabled), (worker.LastOutcomeForTests!.Kind, worker.LastOutcomeForTests.Reason));
            Assert.Equal(3, worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(run.Clock.GetUtcNow() + TimeSpan.FromMinutes(4), worker.CleanupRetryAt(JobKind.Deletion));
            Assert.Equal(discarded.Count, entry.Hints);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Equal(entry.Charged, handOff.Held);

            // Switched off again, the run drains them once the backoff passed
            run.Storage.Before = null;
            Switch(false);
            run.Clock.Advance(TimeSpan.FromMinutes(4));
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal(0, entry.Hints);
            Assert.Equal(0, handOff.Held);
            Assert.Equal(0, worker.CleanupFailures(JobKind.Deletion));
            Assert.All(discarded, x => Assert.False(run.Exists(x)));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task KillSwitchedDrainStillDropsAFailedClosedCommitsHints()
        {
            await using var run = SinkRun.Start(nameof(KillSwitchedDrainStillDropsAFailedClosedCommitsHints), _directory);
            await run.Started();
            var entry = run.Sink.DiscardRouteForTests!.Entry;
            var handOff = run.Stream.Maintenance.Budget.Discards;
            var discarded = run.ChangeData();
            var group = Assert.Single(entry.QueuedForTests);
            Assert.Equal(discarded.Count, group.Remaining);
            // Another stage's hints of the same version beside the marked ones
            Assert.Equal(0, entry.Enqueue(new DiscardGroup("table-b", 0, group.Version, "stage-b", new[] { "_change_data/b.parquet" }) { QueuedAt = run.Clock.GetUtcNow() }));
            entry.MarkFailedClosed(new FailedClosedMark(group.Version, group.StageId));
            var worker = Assert.Single(run.Stream.Maintenance.Workers);

            // Drops delete nothing: with the switch on, the turn still drops the marked commit's hints, keeps the others and declines
            Switch(true);
            await run.Stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeletesDisabled), (worker.LastOutcomeForTests!.Kind, worker.LastOutcomeForTests.Reason));
            Assert.Equal(1, entry.Hints);
            Assert.Equal(discarded.Count, handOff.Dropped);
            Assert.Equal(discarded.Count, run.Sink.DiscardsForTests.Dropped);
            Assert.All(discarded, x => Assert.True(run.Exists(x)));
            Assert.Null(entry.FailedClosed);
            Assert.Equal(entry.Charged, handOff.Held);
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }
    }
}
