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

        // C: the relevant parts of mi_subproc_main_init() that don't need the main heap yet.
        private static mi_subproc_t* mi_subproc_main_create()
        {
            var subproc = (mi_subproc_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_subproc_t));
            subproc->memid = _mi_memid_create(mi_memkind_t.MI_MEM_STATIC);
            mi_stats_header_init(&subproc->stats);
            mi_lock_init(&subproc->arena_reserve_lock);
            mi_lock_init(&subproc->heaps_lock);
            // note: heap_main / heaps list wiring happens with the init.c port (task #8)
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
    }
}
