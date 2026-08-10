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

using System.Text;

namespace FlowtideDotNet.MiMalloc
{
    /// <summary>Usage of one size-class bin, from <see cref="MiMalloc.GetHeapUsage"/>.</summary>
    public readonly struct MiHeapBinUsage
    {
        /// <summary>The size-class bin index.</summary>
        public int Bin { get; init; }
        /// <summary>Block size of this bin, in bytes.</summary>
        public long BlockSize { get; init; }
        /// <summary>Number of pages in this bin.</summary>
        public long Pages { get; init; }
        /// <summary>Bytes of these pages actually committed to the OS.</summary>
        public long CommittedBytes { get; init; }
        /// <summary>Bytes currently handed out as live blocks.</summary>
        public long LiveBytes { get; init; }
        /// <summary>Total capacity of these pages (all blocks they could ever hold).</summary>
        public long CapacityBytes { get; init; }

        /// <summary>Committed bytes not handed out (free blocks plus committed-but-uncarved page space).</summary>
        public long RetainedBytes => CommittedBytes - LiveBytes;
        /// <summary>Live bytes as a fraction of committed bytes (0..1). Low = sparse pages.</summary>
        public double Utilization => CommittedBytes == 0 ? 0.0 : (double)LiveBytes / CommittedBytes;
    }

    /// <summary>
    /// Exact committed-versus-live accounting obtained by walking a heap's pages.
    /// Unlike <see cref="MiMallocStats"/> this does not depend on any thread merging its
    /// statistics: the walk enumerates every page of the heap regardless of which thread
    /// currently owns it.
    /// </summary>
    public readonly struct MiHeapUsage
    {
        /// <summary>Total pages in the heap.</summary>
        public long Pages { get; init; }
        /// <summary>Total bytes of those pages committed to the OS.</summary>
        public long CommittedBytes { get; init; }
        /// <summary>Total bytes currently handed out as live blocks.</summary>
        public long LiveBytes { get; init; }
        /// <summary>Total capacity of all pages.</summary>
        public long CapacityBytes { get; init; }
        /// <summary>Per size-class breakdown, only bins that hold at least one page, largest retained first.</summary>
        public IReadOnlyList<MiHeapBinUsage> Bins { get; init; }

        /// <summary>
        /// Committed bytes not handed out. This is the fragmentation/retention number: memory the
        /// allocator holds from the OS but is not serving to the application.
        /// </summary>
        public long RetainedBytes => CommittedBytes - LiveBytes;
        /// <summary>Live bytes as a fraction of committed bytes (0..1).</summary>
        public double Utilization => CommittedBytes == 0 ? 0.0 : (double)LiveBytes / CommittedBytes;

        public override string ToString()
        {
            static string MB(long b) => $"{b / (1024.0 * 1024.0):N1} MiB";
            var sb = new StringBuilder();
            sb.Append($"heap: {Pages} pages, committed {MB(CommittedBytes)}, live {MB(LiveBytes)}, ")
              .Append($"retained {MB(RetainedBytes)}, utilization {Utilization:P1}");
            if (Bins != null)
            {
                foreach (var b in Bins)
                {
                    sb.AppendLine();
                    sb.Append($"  bin {b.Bin,2} block {b.BlockSize,8:N0} B: {b.Pages,6} pages, ")
                      .Append($"committed {MB(b.CommittedBytes),12}, live {MB(b.LiveBytes),12}, ")
                      .Append($"retained {MB(b.RetainedBytes),12}, util {b.Utilization,6:P1}");
                }
            }
            return sb.ToString();
        }
    }

    public static unsafe partial class MiMalloc
    {
        // per-bin accumulators for the page walk
        private struct MiHeapUsageAcc
        {
            public fixed long Pages[Mi.MI_BIN_COUNT];
            public fixed long Committed[Mi.MI_BIN_COUNT];
            public fixed long Live[Mi.MI_BIN_COUNT];
            public fixed long Capacity[Mi.MI_BIN_COUNT];
            public fixed long BlockSize[Mi.MI_BIN_COUNT];
        }

        private static bool HeapUsageVisitor(mi_heap_t* heap, mi_heap_area_t* area, void* block, nuint block_size, void* arg)
        {
            if (block != null) return true;   // area callbacks only (we pass visitBlocks: false)
            var acc = (MiHeapUsageAcc*)arg;
            mi_page_t* page = (mi_page_t*)area->reserved1;
            if (page == null) return true;

            nuint bsize = Mi.mi_page_block_size(page);
            if (bsize == 0) return true;
            int bin = (int)Mi._mi_page_stats_bin(page);
            if (bin < 0 || bin >= Mi.MI_BIN_COUNT) return true;

            acc->Pages[bin] += 1;
            acc->Committed[bin] += (long)Mi.mi_page_committed(page);
            acc->Live[bin] += (long)((nuint)page->used * bsize);
            acc->Capacity[bin] += (long)((nuint)page->capacity * bsize);
            acc->BlockSize[bin] = (long)bsize;
            return true;
        }

        /// <summary>
        /// Walk every page of the main heap and report committed-versus-live bytes, in total and
        /// per size-class bin. This is the measurement to use when diagnosing "the allocator holds
        /// far more memory than my data" — a low <see cref="MiHeapUsage.Utilization"/> in a
        /// particular bin points straight at the size class responsible.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Cost is O(pages), so call this at checkpoint cadence rather than in a tight polling
        /// loop. Use <see cref="GetStats"/> for cheap frequent sampling of OS-level totals.
        /// </para>
        /// <para>
        /// The walk reads page counters without synchronization (as the C does). Under concurrent
        /// allocation a page or two may be counted slightly stale, so treat the result as a gauge
        /// rather than an exact invariant. It never blocks and never allocates from the heap it
        /// is walking.
        /// </para>
        /// </remarks>
        public static MiHeapUsage GetHeapUsage()
        {
            MiHeapUsageAcc acc = default;
            mi_heap_t* heap = Mi.mi_heap_main();
            Mi._mi_heap_visit_blocks(heap, false /* all pages, not just abandoned */,
                                     false /* areas only, do not enumerate blocks */,
                                     &HeapUsageVisitor, &acc);

            long pages = 0, committed = 0, live = 0, capacity = 0;
            var bins = new List<MiHeapBinUsage>();
            for (int bin = 0; bin < Mi.MI_BIN_COUNT; bin++)
            {
                if (acc.Pages[bin] == 0) continue;
                pages += acc.Pages[bin];
                committed += acc.Committed[bin];
                live += acc.Live[bin];
                capacity += acc.Capacity[bin];
                bins.Add(new MiHeapBinUsage
                {
                    Bin = bin,
                    BlockSize = acc.BlockSize[bin],
                    Pages = acc.Pages[bin],
                    CommittedBytes = acc.Committed[bin],
                    LiveBytes = acc.Live[bin],
                    CapacityBytes = acc.Capacity[bin],
                });
            }
            // worst offenders first: most committed-but-unused bytes
            bins.Sort((a, b) => b.RetainedBytes.CompareTo(a.RetainedBytes));

            return new MiHeapUsage
            {
                Pages = pages,
                CommittedBytes = committed,
                LiveBytes = live,
                CapacityBytes = capacity,
                Bins = bins,
            };
        }
    }
}
