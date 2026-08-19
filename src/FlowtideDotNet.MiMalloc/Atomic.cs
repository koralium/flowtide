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
// Port of mimalloc v3.4.4 `include/mimalloc/atomic.h` (+ `_mi_atomic_once_*` from `src/libc.c`).
// Original: Copyright (c) 2018-2024 Microsoft Research, Daan Leijen (MIT license).
//
// Mapping rules (correctness-first; each .NET op is >= the C memory order):
//  - all read-modify-write ops (cas/exchange/fetch_add/and/or) -> Interlocked.*  (full fence)
//  - load_acquire / load_relaxed   -> Volatile.Read   (acquire; relaxed upgraded to acquire)
//  - store_release / store_relaxed -> Volatile.Write  (release; relaxed upgraded to release)
//  - C weak CAS -> strong CAS (spurious-failure loops remain correct)
// The `_Atomic(T)` struct fields are declared as plain `T` in the C# structs; every access
// to such a field MUST go through these helpers (same discipline as the C code).
//
// IMPORTANT: the `ref` overloads may only be used on locations in native memory (refs obtained
// by dereferencing a pointer) or on stack locals (e.g. `ref expected`). Never pass a ref to a
// managed static/field: the generic pointer helpers use Unsafe.AsPointer and a moving GC target
// would break them. All mimalloc state lives in native memory, so this holds by construction;
// C globals that need atomic access are placed in the native globals area (see Init.cs).

using System.Runtime.CompilerServices;
using System.Threading;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ---------------------------------------------------------------------------
        // loads/stores  (size_t)
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_load_acquire(ref nuint p) => Volatile.Read(ref p);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_load_relaxed(ref nuint p) => Volatile.Read(ref p);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_atomic_store_release(ref nuint p, nuint x) => Volatile.Write(ref p, x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_atomic_store_relaxed(ref nuint p, nuint x) => Volatile.Write(ref p, x);

        // ---------------------------------------------------------------------------
        // exchange / cas  (size_t)
        // C-style CAS: on failure `expected` is updated to the observed value.
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_exchange_relaxed(ref nuint p, nuint x) => Interlocked.Exchange(ref p, x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_exchange_release(ref nuint p, nuint x) => Interlocked.Exchange(ref p, x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_exchange_acq_rel(ref nuint p, nuint x) => Interlocked.Exchange(ref p, x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_atomic_cas(ref nuint p, ref nuint expected, nuint desired)
        {
            nuint read = Interlocked.CompareExchange(ref p, desired, expected);
            if (read == expected) return true;
            expected = read;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_weak_relaxed(ref nuint p, ref nuint expected, nuint desired) => mi_atomic_cas(ref p, ref expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_weak_release(ref nuint p, ref nuint expected, nuint desired) => mi_atomic_cas(ref p, ref expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_weak_acq_rel(ref nuint p, ref nuint expected, nuint desired) => mi_atomic_cas(ref p, ref expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_strong_relaxed(ref nuint p, ref nuint expected, nuint desired) => mi_atomic_cas(ref p, ref expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_strong_release(ref nuint p, ref nuint expected, nuint desired) => mi_atomic_cas(ref p, ref expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_strong_acq_rel(ref nuint p, ref nuint expected, nuint desired) => mi_atomic_cas(ref p, ref expected, desired);

        // ---------------------------------------------------------------------------
        // fetch add/sub/and/or  (size_t) -- return the PREVIOUS value (like C11 fetch_*)
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_add_relaxed(ref nuint p, nuint x) => (nuint)(Interlocked.Add(ref Unsafe.As<nuint, ulong>(ref p), x) - x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_add_acq_rel(ref nuint p, nuint x) => (nuint)(Interlocked.Add(ref Unsafe.As<nuint, ulong>(ref p), x) - x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_sub_relaxed(ref nuint p, nuint x) => (nuint)(Interlocked.Add(ref Unsafe.As<nuint, ulong>(ref p), unchecked(0UL - x)) + x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_sub_acq_rel(ref nuint p, nuint x) => (nuint)(Interlocked.Add(ref Unsafe.As<nuint, ulong>(ref p), unchecked(0UL - x)) + x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_and_relaxed(ref nuint p, nuint x) => (nuint)Interlocked.And(ref Unsafe.As<nuint, ulong>(ref p), x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_and_acq_rel(ref nuint p, nuint x) => (nuint)Interlocked.And(ref Unsafe.As<nuint, ulong>(ref p), x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_or_relaxed(ref nuint p, nuint x) => (nuint)Interlocked.Or(ref Unsafe.As<nuint, ulong>(ref p), x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_or_acq_rel(ref nuint p, nuint x) => (nuint)Interlocked.Or(ref Unsafe.As<nuint, ulong>(ref p), x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_increment_relaxed(ref nuint p) => mi_atomic_add_relaxed(ref p, 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_decrement_relaxed(ref nuint p) => mi_atomic_sub_relaxed(ref p, 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_increment_acq_rel(ref nuint p) => mi_atomic_add_acq_rel(ref p, 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_atomic_decrement_acq_rel(ref nuint p) => mi_atomic_sub_acq_rel(ref p, 1);

        // Atomically add a signed value; returns the previous value.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nint mi_atomic_addi(ref nint p, nint add) => (nint)(Interlocked.Add(ref Unsafe.As<nint, long>(ref p), add) - add);
        // Atomically subtract a signed value; returns the previous value.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nint mi_atomic_subi(ref nint p, nint sub) => mi_atomic_addi(ref p, -sub);

        // ---------------------------------------------------------------------------
        // int64 variants (used by statistics and timers)
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long mi_atomic_loadi64_acquire(ref long p) => Volatile.Read(ref p);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long mi_atomic_loadi64_relaxed(ref long p) => Volatile.Read(ref p);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_atomic_storei64_release(ref long p, long x) => Volatile.Write(ref p, x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_atomic_storei64_relaxed(ref long p, long x) => Volatile.Write(ref p, x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long mi_atomic_addi64_relaxed(ref long p, long add) => Interlocked.Add(ref p, add) - add;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long mi_atomic_addi64_acq_rel(ref long p, long add) => Interlocked.Add(ref p, add) - add;

        public static void mi_atomic_void_addi64_relaxed(ref long p, ref long padd)
        {
            long add = Volatile.Read(ref padd);
            if (add != 0)
            {
                Interlocked.Add(ref p, add);
            }
        }

        public static void mi_atomic_maxi64_relaxed(ref long p, long x)
        {
            long current = Volatile.Read(ref p);
            while (current < x && !mi_atomic_casi64_strong_acq_rel(ref p, ref current, x)) { /* nothing */ }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_casi64_strong_acq_rel(ref long p, ref long expected, long desired)
        {
            long read = Interlocked.CompareExchange(ref p, desired, expected);
            if (read == expected) return true;
            expected = read;
            return false;
        }

        // ---------------------------------------------------------------------------
        // pointer variants
        // C: mi_atomic_load_ptr_acquire(tp, p) where `p` is `&somestruct->field`.
        // These take `T**` so call sites mirror the C exactly: `mi_atomic_load_ptr_acquire(&pq->first)`.
        // `expected` is also a pointer (C passes `&expected`), updated on CAS failure.
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_atomic_load_ptr_acquire<T>(T** p) where T : unmanaged
            => (T*)Volatile.Read(ref *(nint*)p);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_atomic_load_ptr_relaxed<T>(T** p) where T : unmanaged
            => (T*)Volatile.Read(ref *(nint*)p);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_atomic_store_ptr_release<T>(T** p, T* x) where T : unmanaged
            => Volatile.Write(ref *(nint*)p, (nint)x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_atomic_store_ptr_relaxed<T>(T** p, T* x) where T : unmanaged
            => Volatile.Write(ref *(nint*)p, (nint)x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_atomic_cas_ptr<T>(T** p, T** expected, T* desired) where T : unmanaged
        {
            nint exp = (nint)(*expected);
            nint read = Interlocked.CompareExchange(ref *(nint*)p, (nint)desired, exp);
            if (read == exp) return true;
            *expected = (T*)read;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_ptr_weak_release<T>(T** p, T** expected, T* desired) where T : unmanaged
            => mi_atomic_cas_ptr(p, expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_ptr_weak_acq_rel<T>(T** p, T** expected, T* desired) where T : unmanaged
            => mi_atomic_cas_ptr(p, expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_ptr_strong_release<T>(T** p, T** expected, T* desired) where T : unmanaged
            => mi_atomic_cas_ptr(p, expected, desired);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_atomic_cas_ptr_strong_acq_rel<T>(T** p, T** expected, T* desired) where T : unmanaged
            => mi_atomic_cas_ptr(p, expected, desired);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_atomic_exchange_ptr_relaxed<T>(T** p, T* x) where T : unmanaged
            => (T*)Interlocked.Exchange(ref *(nint*)p, (nint)x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_atomic_exchange_ptr_release<T>(T** p, T* x) where T : unmanaged
            => (T*)Interlocked.Exchange(ref *(nint*)p, (nint)x);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_atomic_exchange_ptr_acq_rel<T>(T** p, T* x) where T : unmanaged
            => (T*)Interlocked.Exchange(ref *(nint*)p, (nint)x);

        // ---------------------------------------------------------------------------
        // Locks
        // These should be light-weight in-process only locks.
        // Only used for reserving arena's and to maintain the abandoned list.
        //
        // The C code uses SRWLOCK/pthread_mutex; those cannot be embedded in
        // native-memory structs from C#, so we use a spin-then-yield lock
        // (SpinWait escalates to Sleep(0)/Sleep(1)). These locks guard rare slow
        // paths only, so this is acceptable.
        // ---------------------------------------------------------------------------

        public static void mi_lock_init(mi_lock_t* @lock) => @lock->mutex = 0;

        public static bool mi_lock_try_acquire(mi_lock_t* @lock)
        {
            nuint expected = 0;
            return mi_atomic_cas_strong_acq_rel(ref @lock->mutex, ref expected, 1);
        }

        public static void mi_lock_acquire(mi_lock_t* @lock)
        {
            if (mi_lock_try_acquire(@lock)) return;
            SpinWait spin = default;
            while (true)
            {
                spin.SpinOnce();
                if (mi_lock_try_acquire(@lock)) return;
            }
        }

        public static void mi_lock_release(mi_lock_t* @lock)
        {
            mi_atomic_store_release(ref @lock->mutex, 0);
        }

        public static void mi_lock_done(mi_lock_t* @lock)
        {
            // nothing to do
        }

        // ---------------------------------------------------------------------------
        // Once  (from src/libc.c: _mi_atomic_once_enter/_mi_atomic_once_release)
        // Returns `true` only on the first invocation; the caller must call
        // _mi_atomic_once_release after performing the action. Other threads block
        // until release has been called.
        // ---------------------------------------------------------------------------

        public static bool _mi_atomic_once_enter(mi_atomic_once_t* once)
        {
            nuint once_tid = mi_atomic_load_acquire(ref once->tid);
            if (once_tid == 1)
            {
                return false; // already executed
            }
            nuint current_tid = _mi_thread_id();
            if (once_tid == current_tid)
            {
                return false; // recursive invocation; we need this for process_init for example
            }

            mi_lock_acquire(&once->@lock);
            nuint expected = 0;
            if (mi_atomic_cas_strong_acq_rel(ref once->tid, ref expected, current_tid))
            {
                return true;  // should execute and release
            }
            else
            {
                mi_lock_release(&once->@lock);
                return false; // already another thread entered and released
            }
        }

        public static void _mi_atomic_once_release(mi_atomic_once_t* once)
        {
            if (mi_atomic_load_acquire(ref once->tid) > 1)  // paranoia
            {
                mi_atomic_store_release(ref once->tid, 1);  // done executing
                mi_lock_release(&once->@lock);
            }
        }
    }

    // In-process lock embedded in native-memory structs (see Mi.mi_lock_* above).
    internal struct mi_lock_t
    {
        public nuint mutex;
    }

    internal struct mi_atomic_once_t
    {
        public nuint tid;
        public mi_lock_t @lock;
    }
}
