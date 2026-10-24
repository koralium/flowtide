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
    [Collection(nameof(PinnedClientCollection))]
    public partial class AzureOrphanDirectoryTests : IDisposable
    {
        // Its tables and orphan contexts
        private readonly OrphanSweepTests _kit = new OrphanSweepTests();

        public void Dispose()
        {
            _kit.Dispose();
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task HnsZeroSizeDirectoryAndUnknownSizeNeverCandidate(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var run = await PinnedOrphanRun.SeedAsync(_kit, server);
            run.PutOld("_change_data/cdc-1.parquet", 4);
            run.PutOld("sub/deep/x.bin", 5);
            run.PutOld("known.parquet", 3);
            run.PutOld("unknown.parquet", 3);
            if (flavor == StoreFlavor.Azure)
            {
                // A flat account's folder marker
                run.PutOld("empty/", 0);
            }

            var result = await run.SweepAsync(listed =>
            {
                foreach (var entry in listed.Where(x => x.Path.Full == "/c/tables/t/unknown.parquet"))
                {
                    entry.Size = null;
                }
                return listed;
            });

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            run.AssertFrozen(("_change_data/cdc-1.parquet", 4), ("known.parquet", 3), ("sub/deep/x.bin", 5));
            var zeroSize = flavor == StoreFlavor.AzureHns ? new[] { "_change_data", "sub", "sub/deep" } : new[] { "empty/" };
            foreach (var name in zeroSize)
            {
                var entry = run.Delivered!.Single(x => x.Path.Full == "/c/tables/t/" + name);
                Assert.Equal(0, entry.Size);
                Assert.True(entry.LastModificationTime <= PinnedOrphanRun.Old);
            }
            // Each zero-size entry and the file without a size
            Assert.Equal(zeroSize.Length + 1, run.Counts!.Value.UnknownSize);
            Assert.Equal(zeroSize.Length + 1, run.Counters.OrphanKept(KeepReason.UnknownSize));
        }

        [Fact]
        public async Task HnsDirectoryWithoutTrailingSlashIsKept()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.AzureHns);
            var run = await PinnedOrphanRun.SeedAsync(_kit, server);
            run.PutOld("sub/deep/x.bin", 5);
            run.PutOld("sub/y.bin", 4);

            var result = await run.SweepAsync();

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            run.AssertFrozen(("sub/deep/x.bin", 5), ("sub/y.bin", 4));
            foreach (var name in new[] { "sub", "sub/deep" })
            {
                // An old directory listed as an empty file
                var entry = run.Delivered!.Single(x => x.Path.Full == "/c/tables/t/" + name);
                Assert.False(entry.Path.IsFolder);
                Assert.Equal(0, entry.Size);
                Assert.True(entry.LastModificationTime <= PinnedOrphanRun.Old);
            }
            Assert.Equal(2, run.Counts!.Value.UnknownSize);
            Assert.Equal(2, run.Counters.OrphanKept(KeepReason.UnknownSize));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task AzureZeroByteBlobIsConservativelyKept(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var run = await PinnedOrphanRun.SeedAsync(_kit, server);
            run.PutOld("empty.bin", 0);
            run.PutOld("full.bin", 3);

            var result = await run.SweepAsync();

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.Equal(0, run.Delivered!.Single(x => x.Path.Full == "/c/tables/t/empty.bin").Size);
            if (flavor == StoreFlavor.Azure)
            {
                run.AssertFrozen(("full.bin", 3));
                Assert.Equal(1, run.Counts!.Value.UnknownSize);
                Assert.Equal(1, run.Counters.OrphanKept(KeepReason.UnknownSize));
            }
            else
            {
                // The size rule is Azure's alone
                run.AssertFrozen(("empty.bin", 0), ("full.bin", 3));
                Assert.Equal(0, run.Counts!.Value.UnknownSize);
                Assert.Equal(0, run.Counters.OrphanKept(KeepReason.UnknownSize));
            }
        }
    }
}
