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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class StagedSweepAliasTests
    {
        [Fact]
        public async Task StagedSweepKeepsForeignReferencedNativeReadAlias()
        {
            var directory = Path.Combine(Path.GetTempPath(), "sa-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(directory, "t", "_delta_log"));
            var owners = new ScratchOwners();
            try
            {
                using var storage = new AssertedAliasProtectionTests.OrderedFileStorage(Files.Of.LocalDisk(directory));
                var configuration = CleanupConfiguration.Validate(storage, "t", true, CleanupOptIns.Off);
                Assert.True(configuration.Store.Eligible);
                var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
                metadata.MetaData!.Id = "table-a";
                await DeltaTestKit.WriteCommit(storage, "t", 0, Own(0), DeltaTestKit.Protocol(), metadata);
                await DeltaTestKit.WriteCommit(storage, "t", 1, Own(1));
                await DeltaTestKit.WriteCommit(storage, "t", 2, Own(2));
                const string payloadName = "part-1.parquet";
                var nativePayload = Path.Combine(directory, "t", payloadName);
                File.WriteAllText(nativePayload, "published payload");
                // An own stage at 3 creates the file, another attempt publishes 3, and a foreign 4 references the file by an alias spelling
                var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 3, new List<DeltaAction>()
                {
                    Own(3, created: new[] { payloadName }),
                    DeltaLakeCheckpointBuildTests.Add(payloadName)
                });
                await DeltaTestKit.WriteCommit(storage, "t", 3, Own(3, id: "competing-attempt"));
                await DeltaTestKit.WriteCommit(storage, "t", 4,
                    new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Timestamp = 1004, Operation = "WRITE" } },
                    DeltaLakeCheckpointBuildTests.Add(payloadName + "."));
                await DeltaTestKit.WriteCommit(storage, "t", 5, Own(5, epoch: 5));

                if (OperatingSystem.IsWindows())
                {
                    // The referenced spelling reads the stage's output
                    using var published = await storage.OpenRead("/t/" + payloadName + ".");
                    Assert.NotNull(published);
                    using var reader = new StreamReader(published!);
                    Assert.Equal("published payload", await reader.ReadToEndAsync());
                }

                using var scratch = owners.Add(new ScratchDirectory(Path.Combine(directory, "scratch"))).CreateJob();
                using var cancellation = new CancellationTokenSource();
                var context = new CleanupContext(new JobRun(0, JobKind.StagedSweep) { Cancellation = cancellation }, TimeProvider.System)
                {
                    Kind = JobKind.StagedSweep,
                    Long = false,
                    ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                    Deadline = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(15),
                    Head = 5,
                    TableId = "table-a",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Transfer = new TransferAllowance(),
                    Stop = default
                };
                var metered = MeteredFileStorage.Wrap(storage, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
                var sweep = new StagedSweep(configuration, metered, TimeProvider.System, new DeletionRunSettings());
                var result = await sweep.RunAsync(context);

                Assert.True(File.Exists(nativePayload), $"The referenced payload was deleted; stage verdict {sweep.LastReport!.Verdicts[staged.FileName]}, deleted payloads {sweep.LastReport.PayloadDeleted}.");
                Assert.Equal(0, sweep.LastReport!.PayloadDeleted);
                // The alias spelling cannot protect by name, so the turn declines and the stage waits for a later one
                Assert.Equal((OutcomeKind.Declined, MaintenanceReason.PathRejected), (result.Kind, result.Reason));
                Assert.Equal(StageVerdict.Retry, sweep.LastReport.Verdicts[staged.FileName]);
                Assert.Equal(0, sweep.LastReport.LogFilesDeleted);
            }
            finally
            {
                owners.Release();
                try
                {
                    Directory.Delete(directory, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        private static DeltaAction Own(long version, long epoch = 0, string? id = null, IEnumerable<string>? created = null)
        {
            return new DeltaAction()
            {
                CommitInfo = new DeltaCommitInfoAction()
                {
                    StageId = id ?? "attempt-" + version,
                    AdoptedAt = epoch,
                    Timestamp = 1000 + version,
                    Operation = "WRITE",
                    CreatedFiles = created?.ToList() ?? new List<string>()
                }
            };
        }
    }
}
