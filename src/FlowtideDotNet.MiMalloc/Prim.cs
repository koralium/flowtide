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
// Port of the platform primitive layer of mimalloc v3.4.4 (`include/mimalloc/prim-tls.h`,
// `src/prim/*`). Original: Copyright (c) 2018-2024 Microsoft Research, Daan Leijen (MIT).
//
// This file will grow as the port proceeds (OS memory primitives land with Os.cs).

using System.Runtime.CompilerServices;
using System.Threading;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ---------------------------------------------------------------------------
        // Get a fast unique thread id.
        //
        // Contract (see prim-tls.h): unique per thread, unequal to zero, greater than
        // MI_THREADID_ABANDONED_MAPPED (4), and with the bottom 2 bits clear
        // (page flags are stored in the low bits of `mi_page_t.xthread_id`).
        //
        // The C code uses the TEB/TLS-block address. In .NET we must NOT use
        // Environment.CurrentManagedThreadId: managed thread ids are recycled after a
        // thread dies, and our `mi_thread_done` runs GC-delayed (finalizer based), so a
        // recycled id could alias a dead thread's still-unabandoned pages. Instead ids
        // come from a monotonically increasing counter (never reused), shifted left so
        // the flag bits are clear. The first id is 8 (> 4) as required.
        // ---------------------------------------------------------------------------

        private static ulong _threadIdCounter;   // incremented with Interlocked; id 0 is never handed out

        [ThreadStatic]
        private static nuint _threadId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_prim_thread_id()
        {
            nuint tid = _threadId;
            if (tid == 0)
            {
                tid = mi_prim_thread_id_slow();
            }
            mi_assert_internal(tid > 1);
            mi_assert_internal((tid & MI_PAGE_FLAG_MASK) == 0);  // bottom 2 bits are clear?
            return tid;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static nuint mi_prim_thread_id_slow()
        {
            nuint tid = (nuint)(Interlocked.Increment(ref _threadIdCounter) << 3);
            _threadId = tid;
            return tid;
        }

        // Regular code calls `_mi_thread_id()` (in C defined in init.c).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_thread_id() => _mi_prim_thread_id();

        // Swap the RAW thread id of the current thread (0 = not yet assigned) and return
        // the previous raw value. Port-only: used by the thread-exit sentinel to run the
        // dead thread's cleanup on the finalizer thread under the dead thread's identity,
        // so the same-thread checks in `_mi_thread_done` behave as in C. Not part of the
        // C API; must always be paired with a restoring swap.
        internal static nuint _mi_prim_thread_id_swap_raw(nuint tid)
        {
            nuint prev = _threadId;
            _threadId = tid;
            return prev;
        }
    }
}
