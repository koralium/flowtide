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

        // C: the relevant parts of mi_subproc_main_init() / the static heap initializers
        // that don't need theaps yet. (C: `static mi_subproc_t subproc_main` and
        // `static mi_heap_t mi_process_heap_main` in .bss.)
        private static mi_subproc_t* mi_subproc_main_create()
        {
            var subproc = (mi_subproc_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_subproc_t));
            subproc->memid = _mi_memid_create(mi_memkind_t.MI_MEM_STATIC);
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
                mi_subproc_t* fresh = mi_subproc_main_create();
                nint prev = Interlocked.CompareExchange(ref mi_subproc_main_field, (nint)fresh, 0);
                if (prev != 0)
                {
                    // another thread won the race
                    NativeMemory.Free(fresh->heaps);
                    NativeMemory.Free(fresh);
                    p = prev;
                }
                else
                {
                    p = (nint)fresh;
                }
            }
            return (mi_subproc_t*)p;
        }

        // The sub-process of the current thread. In C this reads the thread-local
        // tld->subproc (defaulting to the main subproc); custom sub-processes are out
        // of scope for the port so this is always the main one (revisit with task #8).
        public static mi_subproc_t* _mi_subproc() => _mi_subproc_main();

        // C: internal.h `_mi_is_heap_main`
        public static bool _mi_is_heap_main(mi_heap_t* heap)
            => heap == mi_atomic_load_ptr_relaxed(&heap->subproc->heap_main);
    }
}
