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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(MaintenanceSwitchCollection))]
    public class DeletionRunSwitchTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "rs-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public DeletionRunSwitchTests()
        {
            Directory.CreateDirectory(_directory);
            Switch(false);
        }

        public void Dispose()
        {
            Switch(false);
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
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

        private CleanupContext Context()
        {
            var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(new JobRun(0, JobKind.Deletion) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Deletion,
                Long = false,
                ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                Head = -1,
                TableId = null,
                Scratch = scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default
            };
        }

        [Fact]
        public async Task RunReadsTheSwitchWithoutASettingsDelegate()
        {
            // Scripted objects classified as pinned Azure, so four deletes may be outstanding
            var objects = new ScriptedStorage();
            using var azure = StoreFixtures.Azure();
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, StoreDescriptor.Of(azure), "c/t", false), CleanupOptIns.Off);
            var owned = new TestStorage(objects);
            var metered = MeteredFileStorage.Wrap(owned, new MaintenanceBuckets(TimeProvider.System), ListingPaging.Azure);
            var executor = new DeletionExecutor(configuration, metered);
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            foreach (var name in names)
            {
                objects.Put("/c/t/" + name);
            }
            var gate = ChunkGate.ForStages(configuration, metered, (_, _) => Task.FromResult(new GateResult(GateVerdict.Passed)));
            DeletionRun NewRun() => new DeletionRun(executor, gate, new ListDeletionSource(names.Select(x => new DeletionTarget("/c/t/" + x, TargetKind.Payload)).ToList()), TimeProvider.System,
                new DeletionRunSettings() { MaxOutstanding = 1 });

            // On before the slice: nothing is listed or deleted
            Switch(true);
            var off = await NewRun().RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Disabled, off.Status);
            Assert.Equal(MaintenanceReason.DeletesDisabled, off.Reason);
            Assert.Empty(owned.Calls);

            // Turned on inside the first delete: issuance stops before the next one
            Switch(false);
            owned.BeforeRm = (_, _) =>
            {
                Switch(true);
                return Task.CompletedTask;
            };
            var stopped = await NewRun().RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Disabled, stopped.Status);
            Assert.Equal(1, stopped.Issued);
            Assert.Equal(1, stopped.Cursor);
            Assert.Null(await objects.Stat("/c/t/a.parquet"));
            Assert.NotNull(await objects.Stat("/c/t/b.parquet"));
            Assert.NotNull(await objects.Stat("/c/t/c.parquet"));
        }
    }
}
