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
// Port of mimalloc v3.4.4 `src/init.c` -- IN PROGRESS (port task #8).
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Currently only the pieces needed by the page map and meta allocator are here:
// the main sub-process (C: `static mi_subproc_t subproc_main` in .bss; here a
// one-time native allocation with MI_MEM_STATIC provenance -- same lifetime).
// The main heap wiring, tld/theap bootstrapping and thread init/done land with task #8.

using System.Runtime.InteropServices;
using System.Threading;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        private static nint mi_subproc_main_field;   // holds the mi_subproc_t* once created

        // C: MI_MEMID_STATIC = { .., MI_MEM_STATIC, pinned=true, committed=true, zero=false }
        private static mi_memid_t mi_memid_static()
        {
            mi_memid_t memid = _mi_memid_create(mi_memkind_t.MI_MEM_STATIC);
            memid.is_pinned = true;
            memid.initially_committed = true;
            return memid;
        }

        // C: the relevant parts of mi_subproc_main_init() / the static heap initializers
        // that don't need theaps yet. (C: `static mi_subproc_t subproc_main` and
        // `static mi_heap_t mi_process_heap_main` in .bss.)
        private static mi_subproc_t* mi_subproc_main_create()
        {
            var subproc = (mi_subproc_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_subproc_t));
            subproc->memid = mi_memid_static();
            mi_stats_header_init(&subproc->stats);
            mi_lock_init(&subproc->arena_reserve_lock);
            mi_lock_init(&subproc->heaps_lock);

            // main heap (minimal wiring; theap slot / theaps list land with task #8)
            var heap = (mi_heap_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_heap_t));
            heap->subproc = subproc;
            heap->heap_seq = 0;
            heap->numa_node = 0;   // C: mi_process_heap_main is zero-initialized (dynamic heaps get -1)
            mi_lock_init(&heap->theaps_lock);
            mi_lock_init(&heap->os_abandoned_pages_lock);
            mi_lock_init(&heap->arena_pages_lock);
            mi_stats_header_init(&heap->stats);

            subproc->heaps = heap;
            subproc->heap_total_count = 1;
            subproc->heap_count = 1;
            mi_atomic_store_ptr_release(&subproc->heap_main, heap);
            __mi_stat_increase_mt(&subproc->stats.heaps, 1);
            return subproc;
        }

        public static mi_subproc_t* _mi_subproc_main()
        {
            nint p = Volatile.Read(ref mi_subproc_main_field);
            if (p == 0)
            {
                mi_process_init();
                p = Volatile.Read(ref mi_subproc_main_field);
            }
            return (mi_subproc_t*)p;
        }

        // C: `mi_process_init()` runs before any allocation (called from the loader/CRT);
        // the port runs it lazily on first use, one thread at a time. Order matches C:
        // options, OS config, main subproc/heap, then the page map (which allocates via
        // the now-published main subproc).
        private static readonly object mi_process_init_lock = new();

        private static void mi_process_init()
        {
            lock (mi_process_init_lock)
            {
                if (Volatile.Read(ref mi_subproc_main_field) != 0) return;
                _mi_options_init();
                _mi_os_init();
                mi_subproc_t* subproc = mi_subproc_main_create();
                Volatile.Write(ref mi_subproc_main_field, (nint)subproc);
                // now the page map can allocate through _mi_subproc_main()
                if (!_mi_page_map_init())
                {
                    _mi_error_message(ENOMEM, "unable to initialize the page map");
                }
            }
        }

        // The sub-process of the current thread. In C this reads the thread-local
        // tld->subproc (defaulting to the main subproc); custom sub-processes are out
        // of scope for the port so this is always the main one (revisit with task #8).
        public static mi_subproc_t* _mi_subproc() => _mi_subproc_main();

        // C: internal.h `_mi_is_heap_main`
        public static bool _mi_is_heap_main(mi_heap_t* heap)
            => heap == mi_atomic_load_ptr_relaxed(&heap->subproc->heap_main);

        /* -----------------------------------------------------------
          Static empty page / empty theaps (C: `_mi_page_empty`,
          `_mi_theap_empty`, `_mi_theap_empty_wrong`, `tld_empty` in .bss)
          Allocated once in native memory.
        ----------------------------------------------------------- */

        // The bin block sizes in machine words (C: the QNULL entries of MI_PAGE_QUEUES_EMPTY).
        // Index == bin; the queue block_size is `wsize * sizeof(void*)`.
        private static ReadOnlySpan<uint> mi_bin_wsizes => new uint[MI_BIN_COUNT]
        {
            1,
            1, 2, 3, 4, 5, 6, 7, 8,                                          /* 8 */
            10, 12, 14, 16, 20, 24, 28, 32,                                  /* 16 */
            40, 48, 56, 64, 80, 96, 112, 128,                                /* 24 */
            160, 192, 224, 256, 320, 384, 448, 512,                          /* 32 */
            640, 768, 896, 1024, 1280, 1536, 1792, 2048,                     /* 40 */
            2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192,                  /* 48 */
            10240, 12288, 14336, 16384, 20480, 24576, 28672, 32768,          /* 56 */
            40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072,        /* 64 */
            163840, 196608, 229376, 262144, 327680, 393216, 458752, 524288,  /* 72 */
            (uint)MI_LARGE_MAX_OBJ_WSIZE + 1,   /* Huge queue */
            (uint)MI_LARGE_MAX_OBJ_WSIZE + 2,   /* Full queue */
        };

        private static nint mi_page_empty_field;    // mi_page_t*
        private static nint mi_theap_empty_field;   // mi_theap_t*
        private static nint mi_theap_empty_wrong_field;
        private static nint mi_tld_empty_field;     // mi_tld_t*

        // initialize the page queues of a theap with the bin block sizes
        private static void mi_theap_queues_init(mi_theap_t* theap)
        {
            for (int bin = 0; bin < MI_BIN_COUNT; bin++)
            {
                theap->pages[bin].first = null;
                theap->pages[bin].last = null;
                theap->pages[bin].count = 0;
                theap->pages[bin].block_size = mi_bin_wsizes[bin] * (nuint)MI_INTPTR_SIZE;
            }
        }

        // initialize a theap to the "empty" value (C: _mi_theap_empty initializer)
        private static void mi_theap_empty_init(mi_theap_t* theap, nuint cookie)
        {
            theap->tld = mi_tld_empty();
            theap->heap = null;
            theap->subproc = null;
            theap->refcount = 1;
            theap->freed = 0;
            theap->heartbeat = 0;
            theap->cookie = cookie;
            theap->random.weak = true;
            theap->page_count = 0;
            theap->page_retired_min = MI_BIN_FULL;
            theap->page_retired_max = 0;
            theap->pages_full_size = 0;
            theap->generic_count = 0;
            theap->generic_collect_count = 0;
            theap->tnext = null; theap->tprev = null;
            theap->hnext = null; theap->hprev = null;
            theap->page_full_retain = 0;
            theap->allow_page_reclaim = false;
            theap->allow_page_abandon = true;
            mi_page_t* empty = _mi_page_empty();
            for (int i = 0; i < MI_PAGES_DIRECT; i++) { theap->pages_free_direct[i] = (ulong)empty; }
            mi_theap_queues_init(theap);
            theap->memid = mi_memid_static();
            mi_stats_header_init(&theap->stats);
        }

        private static T* mi_static_lazy<T>(ref nint field, delegate*<T*, void> init) where T : unmanaged
        {
            nint p = Volatile.Read(ref field);
            if (p == 0)
            {
                T* fresh = (T*)NativeMemory.AllocZeroed((nuint)sizeof(T));
                init(fresh);
                nint prev = Interlocked.CompareExchange(ref field, (nint)fresh, 0);
                if (prev != 0) { NativeMemory.Free(fresh); p = prev; }
                else { p = (nint)fresh; }
            }
            return (T*)p;
        }

        private static void mi_page_empty_ctor(mi_page_t* page)
        {
            // C: _mi_page_empty -- all zero except:
            page->slice_committed = (uint)MI_ARENA_SLICE_SIZE;
            page->memid = mi_memid_static();
        }

        public static mi_page_t* _mi_page_empty()
        {
            return mi_static_lazy<mi_page_t>(ref mi_page_empty_field, &mi_page_empty_ctor);
        }

        private static void mi_tld_empty_ctor(mi_tld_t* tld)
        {
            // C: tld_empty -- all zero except the subproc
            tld->subproc = _mi_subproc_main();
            tld->memid = mi_memid_static();
        }

        private static mi_tld_t* mi_tld_empty()
        {
            return mi_static_lazy<mi_tld_t>(ref mi_tld_empty_field, &mi_tld_empty_ctor);
        }

        private static void mi_theap_empty_ctor(mi_theap_t* theap) => mi_theap_empty_init(theap, 0);
        private static void mi_theap_empty_wrong_ctor(mi_theap_t* theap) => mi_theap_empty_init(theap, 1);   // cookie 1 (see issue #1343)

        public static mi_theap_t* _mi_theap_empty()
        {
            return mi_static_lazy<mi_theap_t>(ref mi_theap_empty_field, &mi_theap_empty_ctor);
        }

        public static mi_theap_t* _mi_theap_empty_wrong()
        {
            return mi_static_lazy<mi_theap_t>(ref mi_theap_empty_wrong_field, &mi_theap_empty_wrong_ctor);
        }

        // C: page-queue.c `_mi_bin_size` (reads the empty theap's queue table)
        public static nuint _mi_bin_size(nuint bin)
        {
            mi_assert_internal(bin <= MI_BIN_HUGE);
            return mi_bin_wsizes[(int)bin] * (nuint)MI_INTPTR_SIZE;
        }
    }
}
