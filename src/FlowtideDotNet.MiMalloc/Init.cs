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
// Port of mimalloc v3.4.4 `src/init.c` -- process and thread initialization:
// the main sub-process and heap (C: statics in .bss; here one-time native
// allocations with MI_MEM_STATIC provenance -- same lifetime), the empty
// page/theap templates, tld/theap bootstrapping, and thread init/done with
// the finalizer-sentinel thread-exit mechanism (see README.md).
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).

using System.Runtime.CompilerServices;
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

            // main heap (C: mi_heap_main_init)
            var heap = (mi_heap_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_heap_t));
            heap->subproc = subproc;
            heap->heap_seq = 0;
            heap->theap = mi_thread_local_key_fast;   // the main heap uses the fast slot
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

        private static nuint mi_process_main_thread_id;   // C: mi_process_tld_main.thread_id

        private static void mi_process_init()
        {
            // This port pins the 64-bit configuration (MI_INTPTR_SHIFT / MI_SIZE_SHIFT are the
            // constant 3, see README.md). On a 32-bit runtime every size and shift computation
            // would be silently wrong, so refuse to start rather than corrupt memory.
            if (IntPtr.Size != 8)
            {
                throw new PlatformNotSupportedException(
                    "FlowtideDotNet.MiMalloc requires a 64-bit process (the port is compiled for MI_INTPTR_SIZE == 8).");
            }

            lock (mi_process_init_lock)
            {
                if (Volatile.Read(ref mi_subproc_main_field) != 0) return;
                mi_process_main_thread_id = _mi_thread_id();   // C: mi_tld_main_init
                _mi_options_init();
                _mi_os_init();
                _mi_thread_locals_init();
                mi_subproc_t* subproc = mi_subproc_main_create();
                Volatile.Write(ref mi_subproc_main_field, (nint)subproc);
                // now the page map can allocate through _mi_subproc_main()
                if (!_mi_page_map_init())
                {
                    _mi_error_message(ENOMEM, "unable to initialize the page map");
                }

                // C: the tail of `mi_process_init_once`. (The `mi_option_reserve_huge_os_pages`
                // branch is out of scope -- 1 GiB huge OS pages are not supported by this port.)
                // `_mi_subproc()` falls back to the main subproc while there is no default theap,
                // and a re-entrant `mi_process_init` short-circuits on `mi_subproc_main_field`.
                if (mi_option_is_enabled(mi_option_t.mi_option_reserve_os_memory))
                {
                    long ksize = mi_option_get(mi_option_t.mi_option_reserve_os_memory);
                    if (ksize > 0)
                    {
                        mi_reserve_os_memory((nuint)ksize * MI_KiB, true, true);
                    }
                }

                // C: `mi_process_load` calls this right after `mi_process_init`; the port has
                // no process-load hook, so it runs at the tail of process init instead.
                _mi_options_post_init();
            }
        }

        // C: `_mi_is_main_thread`. The port has no static main tld; the "main" thread is
        // the one that ran mi_process_init (the first thread to touch the allocator).
        public static bool _mi_is_main_thread()
            => (mi_process_main_thread_id == 0 || mi_process_main_thread_id == _mi_thread_id());

        // The sub-process of the current thread (C: init.c `_mi_subproc`). Reads the
        // default theap's tld; safe to call from uninitialized threads.
        public static mi_subproc_t* _mi_subproc()
        {
            mi_theap_t* theap = _mi_theap_default();
            if (theap == null || theap->tld == null)   // see issue #1289
            {
                return _mi_subproc_main();
            }
            else
            {
                return theap->tld->subproc;   // avoid using thread local storage (`thread_tld`)
            }
        }

        // C: init.c `mi_heap_main` (`_mi_subproc_heap_main` is in Internal.cs)
        public static mi_heap_t* mi_heap_main() => _mi_subproc_heap_main(_mi_subproc());

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

        /* -----------------------------------------------------------
          Per-thread context and the thread-exit sentinel.

          C keeps the per-thread state in thread locals (or TLS slots) and
          runs `_mi_thread_done` from an OS thread-exit callback (FLS /
          pthread key destructor). .NET has no reliable native thread-exit
          hook, so the port keeps ALL per-thread native state in one
          meta-allocated `mi_thread_ctx_t` and arms a finalizable sentinel
          object in a [ThreadStatic]: when the thread dies the sentinel is
          collected and its finalizer runs the C thread-done logic on the
          finalizer thread while temporarily adopting the dead thread's
          identity (thread id + context), so all same-thread checks and
          cleanup behave exactly as in C.
        ----------------------------------------------------------- */

        public static readonly nuint MI_THREADID_INVALID = nuint.MaxValue;

        [ThreadStatic]
        private static mi_thread_ctx_t* mi_thread_ctx_current;

        [ThreadStatic]
        private static MiThreadExitSentinel? mi_thread_ctx_sentinel;

        private sealed class MiThreadExitSentinel
        {
            private mi_thread_ctx_t* ctx;
            internal MiThreadExitSentinel(mi_thread_ctx_t* ctx) { this.ctx = ctx; }
            ~MiThreadExitSentinel()
            {
                mi_thread_ctx_t* c = ctx;
                ctx = null;
                if (c == null) return;
                // A finalizer must NEVER let an exception escape
                try
                {
                    mi_thread_done_from_finalizer(c);
                }
                catch
                {
                    // ignored on purpose (see above)
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_thread_ctx_t* mi_thread_ctx_peek() => mi_thread_ctx_current;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_thread_ctx_t* mi_thread_ctx()
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx_current;
            if (ctx == null) { ctx = mi_thread_ctx_create(); }
            return ctx;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static mi_thread_ctx_t* mi_thread_ctx_create()
        {
            mi_memid_t memid;
            var ctx = (mi_thread_ctx_t*)_mi_meta_zalloc(_mi_subproc_main(), (nuint)sizeof(mi_thread_ctx_t), &memid);
            if (ctx == null)
            {
                _mi_error_message(ENOMEM, "unable to allocate memory for the thread context");
                return null;
            }
            ctx->memid = memid;
            ctx->thread_id = _mi_thread_id();
            mi_thread_ctx_current = ctx;
            mi_thread_ctx_sentinel = new MiThreadExitSentinel(ctx);
            return ctx;
        }

        /* -----------------------------------------------------------
          The default / cached theap thread locals (C: prim-tls.h with
          MI_THEAP_INITASNULL semantics -- null until first set)
        ----------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_theap_t* _mi_theap_default()
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx_current;
            return (ctx == null ? null : ctx->theap_default);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_theap_t* _mi_theap_cached()
        {
            mi_thread_ctx_t* ctx = mi_thread_ctx_current;
            return (ctx == null ? null : ctx->theap_cached);
        }

        // C: init.c `_mi_theap_cached_set`
        public static void _mi_theap_cached_set(mi_theap_t* theap)
        {
            mi_theap_t* prev = _mi_theap_cached();
            if (prev == theap) return;
            mi_thread_ctx_t* ctx = mi_thread_ctx();
            mi_assert_internal(ctx != null);   // ctx is created in mi_thread_init_ex before any theap exists
            if (ctx == null) return;           // defensive (out of memory; theap stays uncached)
            ctx->theap_cached = theap;
            // update refcounts (so cached theap memory keeps available until no longer cached)
            _mi_theap_incref(theap);
            _mi_theap_decref(prev);
        }

        // C: init.c `_mi_theap_default_set`
        public static void _mi_theap_default_set(mi_theap_t* theap)
        {
            mi_assert_internal(theap != null);
            mi_assert_internal(theap->tld != null);
            mi_assert_internal(theap->tld->thread_id == 0 || theap->tld->thread_id == _mi_thread_id());
            mi_thread_ctx_t* ctx = mi_thread_ctx();
            mi_assert_internal(ctx != null);   // ctx is created in mi_thread_init_ex before any theap exists,
                                               // so this store is infallible like the C TLS write
            if (ctx == null) return;           // defensive
            ctx->theap_default = theap;
            // C: _mi_prim_thread_associate_default_theap(theap) -- the port's exit
            // sentinel is armed at context creation, so nothing more is needed here.
        }

        // C: prim-tls.h `_mi_thread_is_initialized`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool _mi_thread_is_initialized()
            => mi_theap_is_initialized(_mi_theap_default());

        // C: init.c `_mi_is_empty_theap`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool _mi_is_empty_theap(mi_theap_t* theap) => theap == _mi_theap_empty();

        /* -----------------------------------------------------------
          Thread local data (C: init.c)
        ----------------------------------------------------------- */

        // C: init.c `_mi_theap_options_init`
        public static void _mi_theap_options_init(mi_theap_t* theap)
        {
            theap->allow_page_reclaim = (mi_option_get(mi_option_t.mi_option_page_reclaim_on_free) >= 0);
            theap->allow_page_abandon = (mi_option_get(mi_option_t.mi_option_page_full_retain) >= 0);
            theap->page_full_retain = mi_option_get_clamp(mi_option_t.mi_option_page_full_retain, -1, 32);
        }

        // Allocate fresh tld (C: init.c `mi_tld_alloc`; the port has no static tld_main --
        // every thread's tld is meta-allocated, see README.md)
        private static mi_tld_t* mi_tld_alloc(mi_subproc_t* subproc)
        {
            // note: we need to be careful to not access the tld from `_mi_meta_zalloc`
            // (and in turn from `_mi_arena_alloc_aligned` and `_mi_os_alloc_aligned`).
            mi_memid_t memid;
            mi_tld_t* tld = (mi_tld_t*)_mi_meta_zalloc(subproc, (nuint)sizeof(mi_tld_t), &memid);
            if (tld == null)
            {
                _mi_error_message(ENOMEM, "unable to allocate memory for thread local data");
                return null;
            }
            tld->memid = memid;
            tld->theaps = null;
            mi_lock_init(&tld->theaps_lock);
            tld->subproc = subproc;
            tld->numa_node = _mi_os_numa_node();
            tld->thread_id = _mi_prim_thread_id();
            tld->thread_seq = mi_atomic_increment_relaxed(ref tld->subproc->thread_total_count);
            tld->is_in_threadpool = _mi_prim_thread_is_in_threadpool();
            mi_atomic_increment_relaxed(ref tld->subproc->thread_count);
            return tld;
        }

        private static void mi_tld_free(mi_tld_t* tld)
        {
            if (tld == null) return;
            mi_atomic_decrement_relaxed(ref tld->subproc->thread_count);
            tld->thread_id = MI_THREADID_INVALID;   // note: not 0; the same thread-id can be
                                                    // reused by the OS after a thread has terminated (issue #1287)
                                                    // (cannot happen with the port's counter ids, kept for fidelity)
            mi_lock_done(&tld->theaps_lock);
            _mi_meta_free(tld->subproc, tld, (nuint)sizeof(mi_tld_t), tld->memid);
        }

        /* -----------------------------------------------------------
          Thread init / done (C: init.c)
        ----------------------------------------------------------- */

        // C: init.c `mi_heap_check_for_existing_theap`
        private static mi_theap_t* mi_heap_check_for_existing_theap(mi_heap_t* heap)
        {
            nuint tid = _mi_thread_id();
            mi_theap_t* thread_theap = null;
            mi_lock_acquire(&heap->theaps_lock);
            try
            {
                for (mi_theap_t* theap = heap->theaps; theap != null; theap = theap->hnext)
                {
                    if (theap->tld->thread_id == tid)
                    {
                        thread_theap = theap;
                        break;
                    }
                }
            }
            finally
            {
                mi_lock_release(&heap->theaps_lock);
            }
            return thread_theap;
        }

        // Initialize the thread local default theap, called from `mi_thread_init`.
        // Port deviation: C gives the main thread a statically allocated theap/tld so it can
        // run before any allocation is possible; .NET has no such constraint so EVERY thread
        // (including the first) allocates its tld + theap via the meta allocator.
        private static mi_theap_t* _mi_thread_init_theap_default(mi_heap_t* heap_main)
        {
            mi_theap_t* theap = _mi_theap_default();
            if (mi_theap_is_initialized(theap)) return theap;

            if (heap_main == null)
            {
                heap_main = mi_heap_main();
            }
            mi_assert_internal(heap_main != null);
            theap = mi_heap_check_for_existing_theap(heap_main);
            if (theap == null)
            {
                // allocated the tld
                mi_tld_t* tld = mi_tld_alloc(heap_main->subproc);
                if (tld == null) return null;   // out-of-memory on tld allocation

                // allocate and initialize the theap for the main heap
                theap = _mi_theap_create(heap_main, tld);
                if (theap == null)
                {
                    mi_tld_free(tld);
                    return null;   // out-of-memory on theap allocation
                }
            }
            // now initialize the thread
            _mi_theap_default_set(theap);

            // and only then set the heap_theap field as that accesses thread locals
            _mi_heap_theap_set(heap_main, theap);   // todo: can fail!

            mi_assert_internal(mi_theap_is_initialized(theap));
            mi_assert_internal((mi_theap_t*)_mi_thread_local_get(heap_main->theap) == theap);
            return theap;
        }

        // Free the thread local theaps (C: init.c `mi_thread_theaps_done`)
        private static void mi_thread_theaps_done(mi_tld_t* tld)
        {
            // abandon the pages of all theaps in this thread
            mi_lock_acquire(&tld->theaps_lock);
            try
            {
                mi_theap_t* theap = tld->theaps;
                while (theap != null)
                {
                    mi_theap_t* next = theap->tnext;
                    // never destroy theaps; there may still be delete/free calls after
                    // the thread-done callback ran. Issue #207
                    _mi_theap_collect_abandon(theap);
                    mi_assert_internal(theap->page_count == 0);
                    theap = next;
                }
            }
            finally
            {
                mi_lock_release(&tld->theaps_lock);
            }

            // reset the thread local theaps
            // note: do this after abandon as page->heap may be NULL and mi_heap_main should return the heap
            // belonging to the right subprocess
            _mi_theap_default_set(_mi_theap_empty());
            _mi_theap_cached_set(_mi_theap_empty());

            // free the theaps of this thread.
            // This can run concurrently with a `mi_heap_free_theaps` and we need to ensure we free theaps atomically.
            // We do this in a loop where we release the theaps_lock at every potential re-iteration to unblock
            // potential concurrent `mi_heap_free_theaps` which tries to remove the theap from our theaps list.
            bool all_freed;
            do
            {
                all_freed = true;
                mi_lock_acquire(&tld->theaps_lock);
                try
                {
                    mi_theap_t* theap = tld->theaps;
                    while (theap != null)
                    {
                        mi_theap_t* next = theap->tnext;
                        mi_assert_internal(theap->page_count == 0);
                        if (!_mi_theap_free(theap, true /* acquire heap->theaps_lock */, false /* dont re-acquire the tld->theaps_lock */))
                        {
                            all_freed = false;
                        }
                        theap = next;
                    }
                }
                finally
                {
                    mi_lock_release(&tld->theaps_lock);
                }
                if (!all_freed)
                {
                    __mi_stat_counter_increase_mt(&tld->subproc->stats.heaps_delete_wait, 1);
                    _mi_prim_thread_yield();
                }
                else
                {
                    mi_assert_internal(tld->theaps == null);
                }
            } while (!all_freed);

            mi_assert(_mi_theap_default() == _mi_theap_empty());   // careful to not re-initialize the default theap during theap_delete
            mi_assert(!_mi_thread_is_initialized());
        }

        // Initialize thread (C: init.c `mi_thread_init_ex`)
        private static mi_theap_t* mi_thread_init_ex(mi_heap_t* heap_main)
        {
            // ensure our process has started already (port: lazy via _mi_subproc_main)
            _mi_subproc_main();

            // if the theap_default is already set we have already initialized
            mi_theap_t* theap = _mi_theap_default();
            if (mi_theap_is_initialized(theap)) return theap;

            // port: create the thread context BEFORE any tld/theap allocation so the
            // TLS stores below cannot fail (in C the TLS write is infallible; an OOM
            // here must propagate before anything gets registered)
            if (mi_thread_ctx() == null) return null;

            // otherwise initialize the default theap
            theap = _mi_thread_init_theap_default(heap_main);
            if (theap == null) return null;   // out-of-memory on tld/theap allocation

            __mi_stat_increase_mt(&_mi_theap_subproc(theap)->stats.threads, 1);
            return theap;
        }

        public static mi_theap_t* _mi_thread_init()
        {
            return mi_thread_init_ex(null);
        }

        public static void mi_thread_init()
        {
            _mi_thread_init();
        }

        public static void mi_thread_done()
        {
            _mi_thread_done(null);
        }

        public static void _mi_thread_done(mi_theap_t* _theap_main)
        {
            // NULL can be passed on some platforms
            if (_theap_main == null)
            {
                _theap_main = _mi_theap_default();
            }

            // prevent re-entrancy through theap_done/theap_set_default_direct (issue #699)
            if (!mi_theap_is_initialized(_theap_main))
            {
                return;
            }

            // note: we store the tld as we should avoid reading the thread-local at this point
            mi_tld_t* tld = _theap_main->tld;

            // release dynamic thread_local's
            _mi_thread_locals_thread_done(mi_thread_ctx_peek());

            // adjust stats
            __mi_stat_decrease_mt(&tld->subproc->stats.threads, 1);

            // check thread-id as the cleanup may run on another thread (C: Windows FLS on
            // shutdown; port: the finalizer sentinel, which ADOPTS the dead thread's id
            // precisely so this check passes)
            if (tld->thread_id != _mi_prim_thread_id()) return;

            // delete the thread local theaps
            mi_thread_theaps_done(tld);

            // free thread local data
            mi_tld_free(tld);
        }

        // Runs on the finalizer thread once a thread that used the allocator has died:
        // temporarily adopt the dead thread's identity (thread id + context) so the C
        // same-thread cleanup logic runs unchanged, then release the context itself.
        private static void mi_thread_done_from_finalizer(mi_thread_ctx_t* ctx)
        {
            mi_assert_internal(ctx != null);
            mi_memid_t memid = ctx->memid;   // read before the context can be released
            nuint prev_id = _mi_prim_thread_id_swap_raw(ctx->thread_id);
            mi_thread_ctx_t* prev_ctx = mi_thread_ctx_current;
            mi_thread_ctx_current = ctx;
            try
            {
                _mi_thread_done(ctx->theap_default);
                // if the theap was never initialized `_mi_thread_done` returned early;
                // make sure the dynamic locals are released either way
                _mi_thread_locals_thread_done(ctx);
            }
            finally
            {
                mi_thread_ctx_current = prev_ctx;
                _mi_prim_thread_id_swap_raw(prev_id);
                // release the context even if the teardown above failed
                _mi_meta_free(_mi_subproc_main(), ctx, (nuint)sizeof(mi_thread_ctx_t), memid);
            }
        }

        // C: init.c `mi_thread_set_in_threadpool`
        public static void mi_thread_set_in_threadpool()
        {
            mi_theap_t* theap = mi_theap_get_default();
            theap->tld->is_in_threadpool = true;
        }
    }

    // Per-thread native state (port-only; see the sentinel comment in Init.cs).
    // C equivalents: `__mi_theap_default`/`__mi_theap_cached` (prim-tls.h),
    // `mi_thread_locals`/`mi_slot_fast` (threadlocal.c) -- all thread locals in C.
    internal unsafe struct mi_thread_ctx_t
    {
        public nuint thread_id;                    // the mi thread id of the owning thread (for finalizer adoption)
        public mi_theap_t* theap_default;          // default theap (null = not initialized)
        public mi_theap_t* theap_cached;           // last used theap via the heap api (refcounted)
        public mi_thread_locals_t* thread_locals;  // dynamic thread-local slots array (or null)
        public void* slot_fast;                    // the fast slot (the main heap's theap)
        public mi_memid_t memid;                   // provenance of this context
    }
}
