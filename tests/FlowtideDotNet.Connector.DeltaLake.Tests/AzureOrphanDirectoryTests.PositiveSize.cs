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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AzureOrphanDirectoryTests
    {
        [Fact]
        public async Task HnsDirectoryListedWithAPositiveSizeIsNeverDeleted()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.AzureHns);
            var pinned = await PinnedOrphanRun.SeedAsync(_kit, server);
            pinned.PutOld("a-old.parquet", 3);
            pinned.PutOld("sub/x.bin", 5);
            await DeltaTestKit.WriteOwnCommit(pinned.Table.Raw, pinned.Table.Name, 3, 0, BoundarySelectorTests.Old(3), DeltaLakeCheckpointBuildTests.Add("sub/x.bin"));
            // The directory listed with a positive size
            pinned.Table.Owned.AfterRecursiveLs = (_, listed) =>
            {
                foreach (var entry in listed.Where(x => x.Path.Full == "/c/tables/t/sub"))
                {
                    entry.Size = 3;
                }
                return listed;
            };
            var kit = await OrphanInvariants.CaptureAsync(pinned.Table, server);
            var run = await _kit.RegisterPinnedAsync(pinned);
            var targets = run.Targets.ReadAllForTests();
            Assert.Equal(new[] { "/c/tables/t/a-old.parquet", "/c/tables/t/sub" }, targets.Select(x => x.Path));
            // Frozen as a file of three bytes
            Assert.Equal(3, targets[1].Size);
            kit.Record(run);
            var absent = pinned.Counters.CandidateProbes(ProbeResult.Absent);
            server.Requests.Clear();

            var turns = await _kit.DeleteTurnsAsync(run, 3);

            Assert.Equal(new[] { "DELETE /c/tables/t/a-old.parquet HTTP/1.1" }, Deletes(server));
            AssertCompleted(turns);
            Assert.True(run.Ended);
            Assert.Contains("HEAD /c/tables/t/sub HTTP/1.1", server.Requests);
            Assert.False(server.Contains("tables/t/a-old.parquet"));
            Assert.True(server.Contains("tables/t/sub/x.bin"));
            Assert.Equal(absent + 1, pinned.Counters.CandidateProbes(ProbeResult.Absent));
            await kit.AssertAsync(new[] { run }, 1, contexts: pinned.Context);
        }
    }
}
