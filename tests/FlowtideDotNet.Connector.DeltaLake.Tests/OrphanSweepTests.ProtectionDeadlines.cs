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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // A long orphan admission on the test's clock and producers, with the published version it reads when given
        private CleanupContext ClockedContext(TimeProvider clock, ProducerGenerations producers, Func<long>? published = null)
        {
            return new CleanupContext(new JobRun(2, JobKind.Orphan) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Orphan,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(6),
                Head = 2,
                PublishedNow = published,
                TableId = null,
                Scratch = Scratch(DiskLedger.DefaultCap),
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default,
                Producers = producers
            };
        }

        [Fact]
        public async Task DeadlineAtTheFrozenHandOffLeavesTheProducersUnflipped()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var clock = new FakeTimeProvider(Now);
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(ProducerKey(table));
            var context = ClockedContext(clock, producers);
            var sweep = Sweep(table.Configuration, table.Owned, clock);
            var runs = Registered(sweep);
            var frozen = false;
            sweep.FrozenHookForTests = _ =>
            {
                frozen = true;
                clock.Advance(TimeSpan.FromHours(6));
            };

            CleanupResult result;
            try
            {
                result = await sweep.RunAsync(context);
            }
            finally
            {
                await context.SettleAsync();
            }

            Assert.True(frozen);
            AssertResult(result, OutcomeKind.Preempted, MaintenanceReason.JobDeadline);
            Assert.Equal(0, holder.Entry.Generation);
            Assert.Empty(runs);
            Assert.Equal(0, context.Memory.Managed);
            Assert.Empty(context.Scratch.Files);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public async Task DeadlineDuringAProtectionListingIssuesNoSuccessorProbe(int listing)
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var clock = new FakeTimeProvider(Now);
            var context = ClockedContext(clock, new ProducerGenerations());
            var sweep = Sweep(table.Configuration, table.Owned, clock);
            var runs = Registered(sweep);
            var callsAtDeadline = -1;
            // The provider returns the listing after the deadline stops new issuance
            AtListing(table, listing, entries =>
            {
                callsAtDeadline = table.Owned.Calls.Count;
                clock.Advance(TimeSpan.FromHours(6));
                return entries;
            });

            var (result, _) = await RunAsync(sweep, table, context);

            Assert.True(callsAtDeadline >= 0);
            AssertResult(result, OutcomeKind.Preempted, MaintenanceReason.JobDeadline);
            Assert.Empty(runs);
            Assert.Empty(table.Owned.Calls.Skip(callsAtDeadline));
        }
    }
}
