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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepSwitchTests
    {
        [Fact]
        public async Task KillSwitchDeclinesTheSweepBeforeAnyRequest()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            using var raw = Files.Of.LocalDisk(root);
            await OrphanSweepTests.RetainedTableAsync(raw, "t", 2);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(OrphanSweepTests.Retention, OrphanSweepTests.Interval));
            var owned = new LocalTestStorage(raw);
            var sweep = OrphanSweepTests.Sweep(configuration, owned);
            var owners = new ScratchOwners();
            try
            {
                using var scratch = owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
                var clock = new FakeTimeProvider(OrphanSweepTests.Now);
                // A short admission, as the worker first dispatches the sweep
                var context = new CleanupContext(new JobRun(2, JobKind.Orphan) { Cancellation = new CancellationTokenSource() }, clock)
                {
                    Kind = JobKind.Orphan,
                    Long = false,
                    ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                    Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                    Head = 2,
                    TableId = null,
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Transfer = new TransferAllowance(),
                    Stop = default
                };

                Switch(true);
                var result = await sweep.RunAsync(context);

                Assert.True(result.Kind == OutcomeKind.Declined && result.Reason == MaintenanceReason.DeletesDisabled && !result.NeedsLongAdmission, $"{result.Kind} {result.Reason}: {result.Detail}");
                Assert.Empty(owned.Calls);
                Assert.False(File.Exists(Path.Combine(root, "t", ".flowtide-orphan-sweep")));

                // With the switch off the same admission promotes
                Switch(false);
                Assert.True((await sweep.RunAsync(context)).NeedsLongAdmission);
                Assert.Empty(owned.Calls);
            }
            finally
            {
                owners.Release();
            }
        }
    }
}
