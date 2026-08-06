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
// Port of mimalloc v3.4.4 `src/threadlocal.c` -- dynamic thread-local variables
// for heaps (no limit on the number that can be allocated).
// Original: Copyright (c) 2019-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Port deviation (documented in README.md):
//  - The per-thread state (slots array pointer + fast slot) lives in the native
//    per-thread context (`mi_thread_ctx_t` in Init.cs) so the thread-exit sentinel
//    can free it from the finalizer thread (which adopts the dead thread's
//    identity, so the `mi_free` here behaves as the same-thread free in C).

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.MiMalloc
{
    // one slot: a value and a version (the version is used to safely reuse slots)
    internal unsafe struct mi_tls_slot_t
    {
        public nuint version;
        public void* value;
    }

    // per-thread dynamically expanding slots array (flexible tail; use mi_tls_slots())
    internal unsafe struct mi_thread_locals_t
    {
        public nuint count;
        // followed by count slots
    }

    internal static unsafe partial class Mi
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_tls_slot_t* mi_tls_slots(mi_thread_locals_t* tls)
            => (mi_tls_slot_t*)((byte*)tls + sizeof(mi_thread_locals_t));

        // the (native) empty thread-locals used before a thread allocates its own
        private static nint mi_thread_locals_empty_field;

        private static mi_thread_locals_t* mi_thread_locals_empty()
        {
            nint p = Volatile.Read(ref mi_thread_locals_empty_field);
            if (p == 0)
            {
                var fresh = (mi_thread_locals_t*)NativeMemory.AllocZeroed((nuint)(sizeof(mi_thread_locals_t) + sizeof(mi_tls_slot_t)));
                nint prev = Interlocked.CompareExchange(ref mi_thread_locals_empty_field, (nint)fresh, 0);
                if (prev != 0) { NativeMemory.Free(fresh); p = prev; }
                else { p = (nint)fresh; }
            }
            return (mi_thread_locals_t*)p;
        }

        // C: mi_thread_locals_get/set and mi_slot_fast_get/set (thread locals; here in the thread ctx)
        private static mi_thread_locals_t* mi_thread_locals_get()
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx_peek();
            if (ctx == null || ctx->thread_locals == null) return mi_thread_locals_empty();
            return ctx->thread_locals;
        }

        private static void mi_thread_locals_set(mi_thread_locals_t* tls)
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx();   // create on demand
            mi_assert_internal(ctx != null);
            if (ctx != null) { ctx->thread_locals = tls; }
        }

        private static void* mi_slot_fast_get()
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx_peek();
            return ctx == null ? null : ctx->slot_fast;
        }

        private static bool mi_slot_fast_set(void* val)
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx();   // create on demand
            if (ctx == null) return false;
            ctx->slot_fast = val;
            return true;
        }

        /* -----------------------------------------------------------
          Each key consists of the slot index in the lower bits,
          and its version in the top bits.
        ----------------------------------------------------------- */

        private const int MI_TLS_IDX_BITS = MI_SIZE_BITS / 4;   // 16 bits index, 48 bits version
        private const nuint MI_TLS_IDX_MASK = ((nuint)1 << MI_TLS_IDX_BITS) - 1;
        private const nuint MI_TLS_IDX_MAX = MI_TLS_IDX_MASK;
        private static readonly nuint MI_TLS_VERSION_MAX = (((nuint)1 << (MI_SIZE_BITS - MI_TLS_IDX_BITS)) - 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_key_index(nuint key) => key & MI_TLS_IDX_MASK;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_key_version(nuint key) => key >> MI_TLS_IDX_BITS;

        private static nuint mi_key_create(nuint index, nuint version)
        {
            mi_assert_internal(version != 0 && version <= MI_TLS_VERSION_MAX);
            mi_assert_internal(index <= MI_TLS_IDX_MAX);
            nuint key = (version << MI_TLS_IDX_BITS) | index;
            mi_assert_internal(key != 0);
            return key;
        }

        // dynamically reallocate the thread local slots when needed
        private static mi_thread_locals_t* mi_thread_locals_expand(nuint least_idx)
        {
            mi_thread_locals_t* tls_old = mi_thread_locals_get();
            nuint count_old = tls_old->count;
            nuint count;
            if (count_old == 0)
            {
                tls_old = null;   // so we allocate fresh (from mi_thread_locals_empty)
                count = 16;       // start with 16 slots
            }
            else if (count_old >= 1024)
            {
                count = count_old + 1024;   // at some point increase linearly
            }
            else
            {
                count = 2 * count_old;      // and double initially
            }
            if (count <= least_idx)
            {
                count = least_idx + 1;
            }
            if (count > MI_TLS_IDX_MAX) { return null; }   // too large
            mi_thread_locals_t* tls = (mi_thread_locals_t*)mi_rezalloc(tls_old, (nuint)sizeof(mi_thread_locals_t) + count * (nuint)sizeof(mi_tls_slot_t));
            if (tls == null) return null;
            tls->count = count;
            mi_thread_locals_set(tls);
            return tls;
        }

        private static bool mi_thread_local_set_expand(nuint key, void* val)
        {
            if (val == null) return true;
            nuint idx = mi_key_index(key);
            mi_thread_locals_t* tls = mi_thread_locals_expand(idx);
            if (tls == null)
            {
                _mi_error_message(EFAULT, "unable to allocate thread local variables");
                return false;
            }
            mi_assert_internal(tls == mi_thread_locals_get());
            mi_assert_internal(idx < tls->count);
            mi_tls_slots(tls)[idx].value = val;
            mi_tls_slots(tls)[idx].version = mi_key_version(key);
            return true;
        }

        // set a tls slot; returns `true` if successful.
        private static bool mi_thread_local_set_regular(nuint key, void* val)
        {
            mi_thread_locals_t* tls = mi_thread_locals_get();
            mi_assert_internal(tls != null);
            mi_assert_internal(key != 0);
            nuint idx = mi_key_index(key);
            if (idx < tls->count)
            {
                mi_tls_slots(tls)[idx].value = val;
                mi_tls_slots(tls)[idx].version = mi_key_version(key);
                return true;
            }
            else
            {
                return mi_thread_local_set_expand(key, val);
            }
        }

        public const nuint mi_thread_local_key_fast = 1;

        public static bool _mi_thread_local_set(nuint key, void* val)
        {
            mi_assert_internal(key != 0);
            if (key == mi_thread_local_key_fast)
            {
                return mi_slot_fast_set(val);
            }
            else
            {
                return mi_thread_local_set_regular(key, val);
            }
        }

        // get a tls slot value
        private static void* mi_thread_local_get_regular(nuint key)
        {
            mi_assert_internal(key != 0);
            mi_thread_locals_t* tls = mi_thread_locals_get();
            mi_assert_internal(tls != null);
            nuint idx = mi_key_index(key);
            if (idx < tls->count && mi_key_version(key) == mi_tls_slots(tls)[idx].version)
            {
                return mi_tls_slots(tls)[idx].value;
            }
            else
            {
                return null;
            }
        }

        // get a thread local value
        public static void* _mi_thread_local_get(nuint key)
        {
            mi_assert_internal(key != 0);
            if (key == mi_thread_local_key_fast)
            {
                return mi_slot_fast_get();
            }
            else
            {
                return mi_thread_local_get_regular(key);
            }
        }

        // release the given thread context's dynamic locals (C: _mi_thread_locals_thread_done;
        // ctx-based so the exit sentinel can run it from the finalizer thread under the
        // dead thread's adopted identity -- the mi_free below is then a same-thread free)
        public static void _mi_thread_locals_thread_done(mi_thread_ctx_t* ctx)
        {
            if (ctx == null) return;
            mi_thread_locals_t* tls = ctx->thread_locals;
            if (tls != null && tls->count > 0)
            {
                mi_free(tls);
                ctx->thread_locals = null;
            }
            if (ctx->slot_fast != null)
            {
                ctx->slot_fast = null;
            }
        }

        /* -----------------------------------------------------------
          Create and free fresh TLS key's
        ----------------------------------------------------------- */

        private static mi_lock_t mi_thread_locals_lock;      // we need a lock in order to re-allocate the slot bits
        private static nint mi_thread_locals_free_bitmap;    // mi_bitmap_t*: which slots are assigned (1=free, 0=in-use)
        private static mi_memid_t mi_thread_locals_memid;    // provenance of the bitmap
        private static nuint mi_thread_locals_version;       // version to be able to reuse slots safely

        public static void _mi_thread_locals_init()
        {
            fixed (mi_lock_t* lockPtr = &mi_thread_locals_lock)
            {
                mi_lock_init(lockPtr);
            }
        }

        // C: _mi_thread_locals_done frees the key bitmap at process exit; the port keeps
        // process-lifetime state alive (no mi_process_done), so it is intentionally omitted.

        // strange signature but allows us to reuse the arena code for claiming free pages
        private static bool mi_thread_local_claim_fun(nuint _slice_index, mi_arena_t* _arena, bool* keep_set)
        {
            *keep_set = false;
            return true;
        }

        // When we claim a free slot, we increase the global version counter
        // (so if we reuse a slot it will be returning NULL initially when a thread tries to get it)
        private static nuint mi_thread_local_claim()
        {
            nuint idx = 0;
            mi_bitmap_t* slots = (mi_bitmap_t*)mi_thread_locals_free_bitmap;
            if (slots != null && mi_bitmap_try_find_and_claim(slots, 0, &idx, &mi_thread_local_claim_fun, null))
            {
                mi_thread_locals_version++;
                if (mi_thread_locals_version >= MI_TLS_VERSION_MAX) { mi_thread_locals_version = 1; }   // wrap around the version
                return mi_key_create(idx, mi_thread_locals_version);
            }
            else
            {
                return 0;
            }
        }

        private static bool mi_thread_local_create_expand()
        {
            mi_bitmap_t* slots = (mi_bitmap_t*)mi_thread_locals_free_bitmap;
            // 1024 bits at a time
            nuint oldcount = (slots == null ? 0 : mi_bitmap_max_bits(slots));
            nuint newcount = 1024 + oldcount;
            if (newcount > MI_TLS_IDX_MAX) { return false; }
            nuint newsize = mi_bitmap_size(newcount, null);
            mi_memid_t memid;
            // always allocate thread locals in the main subprocess
            mi_bitmap_t* newslots = (mi_bitmap_t*)_mi_meta_zalloc(_mi_subproc_main(), newsize, &memid);
            mi_assert_internal(_mi_is_aligned(newslots, MI_BCHUNK_SIZE));
            if (newslots == null) { return false; }
            if (slots != null)
            {
                // copy over the previous bitmap
                nuint oldsize = mi_bitmap_size(oldcount, null);
                _mi_memcpy_aligned(newslots, slots, oldsize);
                _mi_meta_free(_mi_subproc_main(), slots, oldsize, mi_thread_locals_memid);
            }
            mi_bitmap_init(newslots, newcount, true /* pretend already zero'd so we do not zero out the copied old entries */);
            mi_bitmap_unsafe_setN(newslots, oldcount, newcount - oldcount);   // set the new expanded slots as available
            mi_thread_locals_free_bitmap = (nint)newslots;
            mi_thread_locals_memid = memid;
            return true;
        }

        // create a fresh key
        public static nuint _mi_thread_local_create()
        {
            nuint key = 0;
            fixed (mi_lock_t* lockPtr = &mi_thread_locals_lock)
            {
                mi_lock_acquire(lockPtr);
                try
                {
                    key = mi_thread_local_claim();
                    if (key == 0)
                    {
                        if (mi_thread_local_create_expand())
                        {
                            key = mi_thread_local_claim();
                        }
                    }
                }
                finally
                {
                    mi_lock_release(lockPtr);
                }
            }
            mi_assert_internal(key != 0);
            mi_assert_internal(key != mi_thread_local_key_fast);
            return key;
        }

        // free a key
        public static void _mi_thread_local_free(nuint key)
        {
            if (key == 0) return;
            nuint idx = mi_key_index(key);
            fixed (mi_lock_t* lockPtr = &mi_thread_locals_lock)
            {
                mi_lock_acquire(lockPtr);
                try
                {
                    mi_bitmap_t* slots = (mi_bitmap_t*)mi_thread_locals_free_bitmap;
                    if (slots != null && idx < mi_bitmap_max_bits(slots))
                    {
                        mi_bitmap_set(slots, idx);
                    }
                }
                finally
                {
                    mi_lock_release(lockPtr);
                }
            }
        }
    }
}
