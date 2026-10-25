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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class PathSetSealConstructionFailureTests
    {
        [Fact]
        public void RefusedSealWriterConstructionReleasesReducerMemory()
        {
            var directory = Path.Combine(Path.GetTempPath(), "sf-" + ScratchOwners.ShortId());
            var owners = new ScratchOwners();
            var memory = new JobMemory();
            try
            {
                using var scratch = owners.Add(new ScratchDirectory(directory)).CreateJob();
                var set = new PathSet(scratch, memory, PathRole.Candidate);
                try
                {
                    set.Add("part-1.parquet", "/c/t/part-1.parquet");
                    Assert.True(memory.Managed > 0);
                    memory.BeforeReserveForTests = bytes =>
                    {
                        if (bytes == RunWriter.BufferSize)
                        {
                            throw new CheckpointDeclinedException(MaintenanceReason.ManagedMemoryCap, DeclineDependency.None, "injected writer construction refusal");
                        }
                    };
                    Assert.Throws<CheckpointDeclinedException>(() => set.Seal());
                }
                finally
                {
                    memory.BeforeReserveForTests = null;
                    set.Dispose();
                }
                Assert.Equal(0, memory.Managed);
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
    }
}
