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
    public partial class OrphanSweepTests
    {
        [Theory]
        // The capability check replays the commits
        [InlineData(false)]
        // A root at the head leaves the anchor's commit to its own proof read
        [InlineData(true)]
        public async Task CapAtTheAnchorConcludesAsOversized(bool rootAtHead)
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            if (rootAtHead)
            {
                using var kit = new AdvanceExecutionKit();
                await kit.BuildAsync(table.Raw, 2);
            }
            var memory = new JobMemory();
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var armed = false;
            var capped = false;
            var built = false;
            sweep.StepForTests = step =>
            {
                // Without stages the anchor makes every reservation after L3
                armed |= step == OrphanSweepStep.ThirdListing;
                built |= step == OrphanSweepStep.Protected;
                return Task.CompletedTask;
            };
            memory.BeforeReserveForTests = bytes =>
            {
                if (armed && (!rootAtHead || Environment.StackTrace.Contains(nameof(CommitAnchor.ProveAsync), StringComparison.Ordinal)))
                {
                    capped = true;
                    throw new CheckpointDeclinedException(MaintenanceReason.ManagedMemoryCap, DeclineDependency.None, $"{bytes} bytes over the managed cap");
                }
            };

            var (result, _) = await RunAsync(sweep, table, OrphanContext(2, memory: memory));

            Assert.True(capped);
            Assert.False(built);
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ManagedMemoryCap);
            Assert.Contains("The anchor at", result.Detail);
            Assert.True(result.Oversized);
            Assert.Empty(runs);
        }

        [Fact]
        public async Task CapDecodingTheAnchorsRootConcludesAsOversized()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            using var kit = new AdvanceExecutionKit();
            await kit.BuildAsync(table.Raw, 2);
            // Rooted at checkpoint 2 alone, as after an advance to 2
            var log = Path.Combine(table.Folder!, "_delta_log");
            File.Delete(Path.Combine(log, $"{0:D20}.json"));
            File.Delete(Path.Combine(log, $"{1:D20}.json"));

            var memory = new JobMemory();
            var context = OrphanContext(2, memory: memory);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var frozen = false;
            var built = false;
            long held = 0;
            sweep.FrozenHookForTests = _ => frozen = true;
            sweep.StepForTests = step =>
            {
                built |= step == OrphanSweepStep.Protected;
                if (step == OrphanSweepStep.ThirdListing)
                {
                    // The download fits, its footer does not
                    held = memory.ManagedCap - memory.Managed - ProofReader.BufferSize;
                    memory.Reserve(held);
                }
                return Task.CompletedTask;
            };

            CleanupResult result;
            try
            {
                result = await sweep.RunAsync(context);
            }
            finally
            {
                memory.Release(held);
                await context.SettleAsync();
            }

            Assert.True(frozen);
            Assert.True(held > 0);
            Assert.False(built);
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ManagedMemoryCap);
            Assert.True(result.Oversized);
            Assert.Contains("The anchor at", result.Detail);
            Assert.Contains("managed cap", result.Detail);
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
            Assert.Empty(context.Scratch.Files);
            Assert.Empty(runs);
        }
    }
}
