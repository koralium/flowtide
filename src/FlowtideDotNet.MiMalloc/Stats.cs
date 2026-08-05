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
//
// Port of mimalloc v3.4.4 `include/mimalloc-stats.h` (struct definitions).
// Original: Copyright (c) 2024-2026 Microsoft Research, Daan Leijen (MIT license).
//
// The update functions from `src/stats.c` are ported later (MI_STAT == 0 keeps
// them minimal); the struct itself is embedded in mi_theap_t/mi_heap_t/mi_subproc_t
// so it is defined up front, faithful to the C layout.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        public const int MI_STAT_VERSION = 5;   // increased on every backward incompatible change

        // Initialization (from mimalloc-stats.h)
        public static void mi_stats_header_init(mi_stats_t* stats)
        {
            stats->size = (nuint)sizeof(mi_stats_t);
            stats->version = MI_STAT_VERSION;
        }

        public static void mi_stats_init(mi_stats_t* stats)
        {
            NativeMemory.Clear(stats, (nuint)sizeof(mi_stats_t));
            mi_stats_header_init(stats);
        }

        /* -----------------------------------------------------------
          Statistics operations (from src/stats.c)
          The `_mt` variants are multi-thread safe (subproc/heap stats);
          the plain variants are for thread-local theap stats.
        ----------------------------------------------------------- */

        private static void mi_stat_update_mt(mi_stat_count_t* stat, long amount)
        {
            if (amount == 0) return;
            // add atomically
            long current = mi_atomic_addi64_relaxed(ref stat->current, amount);
            mi_atomic_maxi64_relaxed(ref stat->peak, current + amount);
            if (amount > 0)
            {
                mi_atomic_addi64_relaxed(ref stat->total, amount);
            }
        }

        private static void mi_stat_update(mi_stat_count_t* stat, long amount)
        {
            if (amount == 0) return;
            // add thread local
            stat->current += amount;
            if (stat->current > stat->peak) { stat->peak = stat->current; }
            if (amount > 0) { stat->total += amount; }
        }

        public static void __mi_stat_counter_increase_mt(mi_stat_counter_t* stat, nuint amount)
        {
            mi_atomic_addi64_relaxed(ref stat->total, (long)amount);
        }

        public static void __mi_stat_counter_increase(mi_stat_counter_t* stat, nuint amount)
        {
            stat->total += (long)amount;
        }

        public static void __mi_stat_increase_mt(mi_stat_count_t* stat, nuint amount) => mi_stat_update_mt(stat, (long)amount);
        public static void __mi_stat_increase(mi_stat_count_t* stat, nuint amount) => mi_stat_update(stat, (long)amount);

        public static void __mi_stat_decrease_mt(mi_stat_count_t* stat, nuint amount) => mi_stat_update_mt(stat, -(long)amount);
        public static void __mi_stat_decrease(mi_stat_count_t* stat, nuint amount) => mi_stat_update(stat, -(long)amount);

        // Adjust stats to compensate; for example before committing a range,
        // first adjust downwards with parts that were already committed so
        // we avoid double counting.
        private static void mi_stat_adjust_mt(mi_stat_count_t* stat, long amount)
        {
            if (amount == 0) return;
            // adjust atomically
            mi_atomic_addi64_relaxed(ref stat->current, amount);
            mi_atomic_addi64_relaxed(ref stat->total, amount);
        }

        private static void mi_stat_adjust(mi_stat_count_t* stat, long amount)
        {
            if (amount == 0) return;
            stat->current += amount;
            stat->total += amount;
        }

        public static void __mi_stat_adjust_increase_mt(mi_stat_count_t* stat, nuint amount) => mi_stat_adjust_mt(stat, (long)amount);
        public static void __mi_stat_adjust_increase(mi_stat_count_t* stat, nuint amount) => mi_stat_adjust(stat, (long)amount);
        public static void __mi_stat_adjust_decrease_mt(mi_stat_count_t* stat, nuint amount) => mi_stat_adjust_mt(stat, -(long)amount);
        public static void __mi_stat_adjust_decrease(mi_stat_count_t* stat, nuint amount) => mi_stat_adjust(stat, -(long)amount);

        // must be thread safe as it is called from stats_merge
        public static void mi_stat_count_add_mt(mi_stat_count_t* stat, mi_stat_count_t* src)
        {
            if (stat == src) return;
            mi_atomic_void_addi64_relaxed(ref stat->total, ref src->total);
            long src_peak = mi_atomic_loadi64_relaxed(ref src->peak);
            long src_current = mi_atomic_loadi64_relaxed(ref src->current);
            long prev_current = mi_atomic_addi64_relaxed(ref stat->current, src_current);
            // Global current plus thread peak approximates new global peak (see stats.c PR#1112)
            mi_atomic_maxi64_relaxed(ref stat->peak, prev_current + src_peak);
        }

        public static void mi_stat_counter_add_mt(mi_stat_counter_t* stat, mi_stat_counter_t* src)
        {
            if (stat == src) return;
            mi_atomic_void_addi64_relaxed(ref stat->total, ref src->total);
        }

        // must be thread safe as it is called from stats_merge
        // (MI_STAT <= 1: malloc_bins are not merged, matching the C release build)
        private static void mi_stats_add(mi_stats_t* stats, mi_stats_t* src)
        {
            if (stats == null || src == null || stats == src) return;

            // copy all fields (MI_STAT_FIELDS in declaration order)
            mi_stat_count_add_mt(&stats->pages, &src->pages);
            mi_stat_count_add_mt(&stats->reserved, &src->reserved);
            mi_stat_count_add_mt(&stats->committed, &src->committed);
            mi_stat_counter_add_mt(&stats->reset, &src->reset);
            mi_stat_counter_add_mt(&stats->purged, &src->purged);
            mi_stat_count_add_mt(&stats->page_committed, &src->page_committed);
            mi_stat_count_add_mt(&stats->pages_abandoned, &src->pages_abandoned);
            mi_stat_count_add_mt(&stats->threads, &src->threads);
            mi_stat_count_add_mt(&stats->malloc_normal, &src->malloc_normal);
            mi_stat_count_add_mt(&stats->malloc_huge, &src->malloc_huge);
            mi_stat_count_add_mt(&stats->malloc_requested, &src->malloc_requested);
            mi_stat_counter_add_mt(&stats->mmap_calls, &src->mmap_calls);
            mi_stat_counter_add_mt(&stats->commit_calls, &src->commit_calls);
            mi_stat_counter_add_mt(&stats->reset_calls, &src->reset_calls);
            mi_stat_counter_add_mt(&stats->purge_calls, &src->purge_calls);
            mi_stat_counter_add_mt(&stats->arena_count, &src->arena_count);
            mi_stat_counter_add_mt(&stats->malloc_normal_count, &src->malloc_normal_count);
            mi_stat_counter_add_mt(&stats->malloc_huge_count, &src->malloc_huge_count);
            mi_stat_counter_add_mt(&stats->malloc_guarded_count, &src->malloc_guarded_count);
            mi_stat_counter_add_mt(&stats->arena_rollback_count, &src->arena_rollback_count);
            mi_stat_counter_add_mt(&stats->arena_purges, &src->arena_purges);
            mi_stat_counter_add_mt(&stats->pages_extended, &src->pages_extended);
            mi_stat_counter_add_mt(&stats->pages_retire, &src->pages_retire);
            mi_stat_counter_add_mt(&stats->page_searches, &src->page_searches);
            mi_stat_counter_add_mt(&stats->page_searches_count, &src->page_searches_count);
            mi_stat_count_add_mt(&stats->segments, &src->segments);
            mi_stat_count_add_mt(&stats->segments_abandoned, &src->segments_abandoned);
            mi_stat_count_add_mt(&stats->segments_cache, &src->segments_cache);
            mi_stat_count_add_mt(&stats->_segments_reserved, &src->_segments_reserved);
            mi_stat_count_add_mt(&stats->heaps, &src->heaps);
            mi_stat_count_add_mt(&stats->theaps, &src->theaps);
            mi_stat_counter_add_mt(&stats->pages_reclaim_on_alloc, &src->pages_reclaim_on_alloc);
            mi_stat_counter_add_mt(&stats->pages_reclaim_on_free, &src->pages_reclaim_on_free);
            mi_stat_counter_add_mt(&stats->pages_reabandon_full, &src->pages_reabandon_full);
            mi_stat_counter_add_mt(&stats->pages_unabandon_busy_wait, &src->pages_unabandon_busy_wait);
            mi_stat_counter_add_mt(&stats->heaps_delete_wait, &src->heaps_delete_wait);

            // (MI_STAT <= 1: skip malloc_bins)
            for (int i = 0; i <= MI_BIN_HUGE; i++)
            {
                mi_stat_count_add_mt((mi_stat_count_t*)Unsafe.AsPointer(ref stats->page_bins[i]), (mi_stat_count_t*)Unsafe.AsPointer(ref src->page_bins[i]));
            }
            for (int i = 0; i < (int)mi_chunkbin_t.MI_CBIN_COUNT; i++)
            {
                mi_stat_count_add_mt((mi_stat_count_t*)Unsafe.AsPointer(ref stats->chunk_bins[i]), (mi_stat_count_t*)Unsafe.AsPointer(ref src->chunk_bins[i]));
            }
        }

        public static void _mi_stats_merge_into(mi_stats_t* to, mi_stats_t* from)
        {
            mi_assert_internal(to != null && from != null);
            if (to == from) return;
            mi_stats_add(to, from);
            mi_stats_init(from);   // zero fields and keep the header
        }
    }

    // count allocation over time
    [StructLayout(LayoutKind.Sequential)]
    internal struct mi_stat_count_t
    {
        public long total;      // total allocated
        public long peak;       // peak allocation
        public long current;    // current allocation
    }

    // counters only increase
    [StructLayout(LayoutKind.Sequential)]
    internal struct mi_stat_counter_t
    {
        public long total;      // total count
    }

    // Size bins for chunks
    internal enum mi_chunkbin_t : int
    {
        MI_CBIN_SMALL,    // slice_count == 1
        MI_CBIN_OTHER,    // slice_count: any other from the other bins, and 1 <= slice_count <= MI_BCHUNK_BITS
        MI_CBIN_MEDIUM,   // slice_count == 8
        MI_CBIN_LARGE,    // slice_count == MI_SIZE_BITS  (only used if MI_ENABLE_LARGE_PAGES is 1)
        MI_CBIN_HUGE,     // slice_count > MI_BCHUNK_BITS
        MI_CBIN_NONE,     // no bin assigned yet (the chunk is completely free)
        MI_CBIN_COUNT
    }

    [InlineArray(4)]
    internal struct mi_stat_count_reserved_t
    {
        private mi_stat_count_t _element0;
    }

    [InlineArray(4)]
    internal struct mi_stat_counter_reserved_t
    {
        private mi_stat_counter_t _element0;
    }

    [InlineArray(Mi.MI_BIN_HUGE + 1)]
    internal struct mi_stat_count_bins_t
    {
        private mi_stat_count_t _element0;
    }

    [InlineArray((int)mi_chunkbin_t.MI_CBIN_COUNT)]
    internal struct mi_stat_count_chunkbins_t
    {
        private mi_stat_count_t _element0;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct mi_stats_t
    {
        public nuint size;      // size of the mi_stats_t structure
        public nuint version;

        // MI_STAT_FIELDS()
        public mi_stat_count_t pages;                       // count of mimalloc pages
        public mi_stat_count_t reserved;                    // reserved memory bytes
        public mi_stat_count_t committed;                   // committed bytes
        public mi_stat_counter_t reset;                     // reset bytes
        public mi_stat_counter_t purged;                    // purged bytes
        public mi_stat_count_t page_committed;              // committed memory inside pages
        public mi_stat_count_t pages_abandoned;             // abandoned pages count
        public mi_stat_count_t threads;                     // number of threads
        public mi_stat_count_t malloc_normal;               // allocated bytes <= MI_LARGE_OBJ_SIZE_MAX
        public mi_stat_count_t malloc_huge;                 // allocated bytes in huge pages
        public mi_stat_count_t malloc_requested;            // malloc requested bytes

        public mi_stat_counter_t mmap_calls;
        public mi_stat_counter_t commit_calls;
        public mi_stat_counter_t reset_calls;
        public mi_stat_counter_t purge_calls;
        public mi_stat_counter_t arena_count;               // number of memory arena's
        public mi_stat_counter_t malloc_normal_count;       // number of blocks <= MI_LARGE_OBJ_SIZE_MAX
        public mi_stat_counter_t malloc_huge_count;         // number of huge blocks
        public mi_stat_counter_t malloc_guarded_count;      // number of allocations with guard pages

        // internal statistics
        public mi_stat_counter_t arena_rollback_count;
        public mi_stat_counter_t arena_purges;
        public mi_stat_counter_t pages_extended;            // number of page extensions
        public mi_stat_counter_t pages_retire;              // number of pages that are retired
        public mi_stat_counter_t page_searches;             // total pages searched for a fresh page
        public mi_stat_counter_t page_searches_count;       // searched count for a fresh page
        // only on v1 and v2
        public mi_stat_count_t segments;
        public mi_stat_count_t segments_abandoned;
        public mi_stat_count_t segments_cache;
        public mi_stat_count_t _segments_reserved;
        // only on v3
        public mi_stat_count_t heaps;
        public mi_stat_count_t theaps;
        public mi_stat_counter_t pages_reclaim_on_alloc;
        public mi_stat_counter_t pages_reclaim_on_free;
        public mi_stat_counter_t pages_reabandon_full;
        public mi_stat_counter_t pages_unabandon_busy_wait;
        public mi_stat_counter_t heaps_delete_wait;

        // future extension
        public mi_stat_count_reserved_t _stat_reserved;
        public mi_stat_counter_reserved_t _stat_counter_reserved;

        // size segregated statistics
        public mi_stat_count_bins_t malloc_bins;            // allocation per size bin
        public mi_stat_count_bins_t page_bins;              // pages allocated per size bin
        public mi_stat_count_chunkbins_t chunk_bins;        // chunks per page sizes
    }
}
