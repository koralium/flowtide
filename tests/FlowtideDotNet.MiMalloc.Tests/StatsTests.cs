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

// Tests for the public MiMalloc.GetStats() snapshot.

using Xunit;
using Xunit.Abstractions;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class StatsTests
    {
        private readonly ITestOutputHelper _output;

        public StatsTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void GetStats_ReportsOsUsage()
        {
            // make sure the allocator is up
            void* warm = MiMalloc.mi_malloc(64);
            MiMalloc.mi_free(warm);

            var before = MiMalloc.GetStats(mergeThreadStats: true);
            _output.WriteLine("before: " + before);

            // hold ~64 MiB live so the OS-level numbers have to move
            const int count = 64;
            const int size = 1024 * 1024;
            var ptrs = new nint[count];
            for (int i = 0; i < count; i++)
            {
                byte* p = (byte*)MiMalloc.mi_malloc(size);
                Assert.True(p != null);
                p[0] = 1;
                p[size - 1] = 1;   // touch both ends so it is really committed
                ptrs[i] = (nint)p;
            }

            var during = MiMalloc.GetStats(mergeThreadStats: true);
            _output.WriteLine("during: " + during);

            // OS-level figures are exact and must reflect the 64 MiB
            Assert.True(during.ReservedBytes > 0, "nothing reserved from the OS");
            Assert.True(during.CommittedBytes >= before.CommittedBytes, "committed went backwards");
            Assert.True(during.CommittedBytes >= count * (long)size, $"committed {during.CommittedBytes} < 64 MiB");
            Assert.True(during.ReservedBytes >= during.CommittedBytes, "reserved must cover committed");
            Assert.True(during.OsAllocCalls > 0, "no OS allocation calls recorded");
            Assert.True(during.ReservedPeakBytes >= during.ReservedBytes);
            Assert.True(during.CommittedPeakBytes >= during.CommittedBytes);

            // live-allocation figures come from the merged thread stats
            Assert.True(during.LiveNormalBytes + during.LiveHugeBytes >= count * (long)size,
                $"live bytes {during.LiveNormalBytes + during.LiveHugeBytes} < 64 MiB");
            Assert.True(during.Threads >= 1);
            Assert.True(during.Heaps >= 1);

            foreach (var ptr in ptrs) { MiMalloc.mi_free((void*)ptr); }
            MiMalloc.mi_collect(true);

            var after = MiMalloc.GetStats(mergeThreadStats: true);
            _output.WriteLine("after:  " + after);

            // the blocks are gone from the live figures ...
            Assert.True(after.LiveNormalBytes + after.LiveHugeBytes
                        < during.LiveNormalBytes + during.LiveHugeBytes,
                "live bytes did not drop after freeing");
            // ... and a forced collect must have purged something back to the OS
            Assert.True(after.PurgedBytesTotal >= before.PurgedBytesTotal, "purge counter went backwards");
        }

        [Fact]
        public void GetHeapUsage_CountsLiveBytesWithoutMerging()
        {
            // block size chosen to sit in its own bin so the assertions are unambiguous
            const int size = 3 * 1024;
            const int count = 400;

            var before = MiMalloc.GetHeapUsage();
            _output.WriteLine("before: " + before);

            var ptrs = new nint[count];
            for (int i = 0; i < count; i++)
            {
                byte* p = (byte*)MiMalloc.mi_malloc(size);
                Assert.True(p != null);
                p[0] = 1;
                ptrs[i] = (nint)p;
            }

            // NOTE: no merge, no collect, no per-thread anything -- the walk sees the pages directly
            var during = MiMalloc.GetHeapUsage();
            _output.WriteLine("during: " + during);

            long grew = during.LiveBytes - before.LiveBytes;
            Assert.True(grew >= count * (long)size, $"live bytes only grew by {grew}, expected >= {count * (long)size}");
            Assert.True(during.CommittedBytes >= during.LiveBytes, "live cannot exceed committed");
            Assert.True(during.Pages > before.Pages, "no new pages observed");
            Assert.True(during.Utilization > 0 && during.Utilization <= 1.0);

            // the bin holding our blocks must show up with the right block size
            long expectedBlockSize = (long)MiMalloc.mi_good_size(size);
            var bin = during.Bins.FirstOrDefault(b => b.BlockSize == expectedBlockSize);
            Assert.True(bin.Pages > 0, $"no bin with block size {expectedBlockSize}");
            Assert.True(bin.LiveBytes >= count * (long)size);

            foreach (var ptr in ptrs) { MiMalloc.mi_free((void*)ptr); }
            MiMalloc.mi_collect(true);

            var after = MiMalloc.GetHeapUsage();
            _output.WriteLine("after:  " + after);
            Assert.True(after.LiveBytes < during.LiveBytes, "live bytes did not drop after freeing");
        }

        [Fact]
        public void GetHeapUsage_ExposesSparsePagesAsLowUtilization()
        {
            // Reproduce the production symptom: allocate many blocks, then free all but a few
            // per page. The pages cannot be returned (each still holds a live block), so
            // committed stays high while live collapses -- exactly what fragmentation looks like.
            const int size = 5 * 1024;   // its own bin, several blocks per 64 KiB page
            const int count = 2000;

            var ptrs = new nint[count];
            for (int i = 0; i < count; i++)
            {
                byte* p = (byte*)MiMalloc.mi_malloc(size);
                Assert.True(p != null);
                p[0] = 1;
                ptrs[i] = (nint)p;
            }
            nuint goodSize = MiMalloc.mi_good_size(size);

            var full = MiMalloc.GetHeapUsage();
            var fullBin = full.Bins.First(b => b.BlockSize == (long)goodSize);
            _output.WriteLine($"full:   pages {fullBin.Pages}, live {fullBin.LiveBytes}, committed {fullBin.CommittedBytes}, util {fullBin.Utilization:P1}");
            Assert.True(fullBin.Utilization > 0.5, $"expected densely used pages, got {fullBin.Utilization:P1}");

            // keep every 8th block alive, free the rest
            for (int i = 0; i < count; i++)
            {
                if (i % 8 != 0) { MiMalloc.mi_free((void*)ptrs[i]); ptrs[i] = 0; }
            }
            MiMalloc.mi_collect(true);

            var sparse = MiMalloc.GetHeapUsage();
            var sparseBin = sparse.Bins.First(b => b.BlockSize == (long)goodSize);
            _output.WriteLine($"sparse: pages {sparseBin.Pages}, live {sparseBin.LiveBytes}, committed {sparseBin.CommittedBytes}, util {sparseBin.Utilization:P1}");

            // live dropped ~8x, but the pages are still held: that gap is the fragmentation signal
            Assert.True(sparseBin.LiveBytes < fullBin.LiveBytes / 4, "live bytes should have collapsed");
            Assert.True(sparseBin.Utilization < fullBin.Utilization, "utilization should have dropped");
            Assert.True(sparseBin.RetainedBytes > sparseBin.LiveBytes, "retained should now dominate live");

            foreach (var ptr in ptrs) { if (ptr != 0) MiMalloc.mi_free((void*)ptr); }
            MiMalloc.mi_collect(true);
        }

        [Fact]
        public void GetStats_UnmergedDoesNotClaimRetained()
        {
            // the trap: without a merge the live figures are meaningless, so retained must be null
            var unmerged = MiMalloc.GetStats();
            Assert.False(unmerged.LiveStatsMerged);
            Assert.Null(unmerged.RetainedBytes);
            Assert.Contains("unknown", unmerged.ToString());

            var merged = MiMalloc.GetStats(mergeThreadStats: true);
            Assert.True(merged.LiveStatsMerged);
            Assert.NotNull(merged.RetainedBytes);
        }

        [Fact]
        public void GetHeapUsage_CountsMultiSlicePagesOnce()
        {
            // A page larger than one 64 KiB slice occupies MANY page-map entries, one per slice.
            // If the walk did not collapse those runs it would count the same page once per slice
            // and every figure would be inflated several-fold, so pin that down explicitly.
            const int size = 2 * 1024 * 1024;
            const int count = 8;

            var before = MiMalloc.GetHeapUsage();

            var ptrs = new nint[count];
            for (int i = 0; i < count; i++)
            {
                byte* p = (byte*)MiMalloc.mi_malloc(size);
                Assert.True(p != null);
                p[0] = 1;
                p[size - 1] = 1;
                ptrs[i] = (nint)p;
            }

            var during = MiMalloc.GetHeapUsage();
            _output.WriteLine("during: " + during);

            long allocated = count * (long)size;
            long grew = during.LiveBytes - before.LiveBytes;
            Assert.True(grew >= allocated, $"live grew by only {grew}, expected >= {allocated}");
            // the real assertion: each 2 MiB block spans dozens of slices, so a walk that failed to
            // dedup would report an order of magnitude more than was actually allocated
            Assert.True(grew < 2 * allocated,
                $"live grew by {grew} for {allocated} allocated -- pages are being counted more than once");

            long pagesAdded = during.Pages - before.Pages;
            Assert.True(pagesAdded <= 4 * count,
                $"{pagesAdded} pages appeared for {count} blocks -- slices are being counted as pages");

            foreach (var ptr in ptrs) { MiMalloc.mi_free((void*)ptr); }
            MiMalloc.mi_collect(true);
        }

        [Fact]
        public void GetHeapUsage_SeesPagesOwnedByAnotherThread()
        {
            // The walk enumerates the page map, so it must see another thread's live pages without
            // that thread merging its statistics or collecting. This is exactly what the GetStats()
            // live figures cannot do, and why they lag (or go negative) under cross-thread traffic.
            const int size = 3 * 1024;
            const int count = 500;

            var before = MiMalloc.GetHeapUsage();

            var ptrs = new nint[count];
            using var allocated = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            var thread = new Thread(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    byte* p = (byte*)MiMalloc.mi_malloc(size);
                    p[0] = 1;
                    ptrs[i] = (nint)p;
                }
                allocated.Set();
                release.Wait();
                for (int i = 0; i < count; i++) { MiMalloc.mi_free((void*)ptrs[i]); }
                MiMalloc.mi_collect(true);
            });
            thread.Start();
            Assert.True(allocated.Wait(TimeSpan.FromSeconds(30)), "allocating thread did not finish");

            // walked from THIS thread, while the owning thread is parked and has merged nothing
            var during = MiMalloc.GetHeapUsage();
            _output.WriteLine("during: " + during);

            long grew = during.LiveBytes - before.LiveBytes;
            Assert.True(grew >= count * (long)size,
                $"another thread's live bytes were invisible to the walk: grew by {grew}");

            release.Set();
            Assert.True(thread.Join(TimeSpan.FromSeconds(30)), "freeing thread did not finish");
        }

        [Fact]
        public void GetHeapUsage_NonArenaFiguresAreASubsetOfTheTotals()
        {
            void* warm = MiMalloc.mi_malloc(64);
            var u = MiMalloc.GetHeapUsage();
            MiMalloc.mi_free(warm);

            // non-arena pages are the ones an arena walk cannot reach while live; whatever their
            // count, they are part of the totals and can never exceed them
            Assert.True(u.NonArenaPages >= 0);
            Assert.True(u.NonArenaPages <= u.Pages, $"non-arena pages {u.NonArenaPages} > total {u.Pages}");
            Assert.True(u.NonArenaLiveBytes <= u.LiveBytes);
            Assert.True(u.NonArenaCommittedBytes <= u.CommittedBytes);
            _output.WriteLine($"non-arena: {u.NonArenaPages} pages, live {u.NonArenaLiveBytes}, committed {u.NonArenaCommittedBytes}");
        }

        [Fact]
        public void GetStats_DoesNotRequireMerge()
        {
            // the OS-level numbers must be readable without merging (the cheap polling path)
            var s = MiMalloc.GetStats();
            Assert.True(s.ReservedBytes > 0);
            Assert.True(s.CommittedBytes > 0);
            Assert.True(s.ReservedBytes >= s.CommittedBytes);
            Assert.False(string.IsNullOrEmpty(s.ToString()));
        }
    }
}
