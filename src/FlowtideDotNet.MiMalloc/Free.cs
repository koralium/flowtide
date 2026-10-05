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
// Port of mimalloc v3.4.4 `src/free.c` -- local and multi-threaded free:
// the xthread_id dispatch in mi_free_ex, the lock-free thread-free push with
// ownership claim, the abandoned-page collect (free / reclaim / reabandon /
// unown), usable-size queries, and the free variants.
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Pinned-configuration notes:
//  - MI_ENCODE_FREELIST=0 => MI_CHECK_DOUBLE_FREE is off (as in the C release
//    build); mi_check_is_double_free is a constant false.
//  - MI_PADDING=0 => mi_check_padding/_mi_padding_shrink are no-ops and
//    mi_page_usable_size_of is just the page block size.
//  - MI_GUARDED=0 => the `was_guarded` parameters are always false and
//    _mi_page_unguard_all is a no-op.

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ------------------------------------------------------
        // Free
        // ------------------------------------------------------

        // regular free of a (thread local) block pointer
        // fast path written carefully to prevent spilling on the stack
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void mi_free_block_local(mi_page_t* page, mi_block_t* block, bool was_guarded, bool track_stats, bool check_full)
        {
            // checks
            if (mi_check_is_double_free(page, block)) return;
            if (!was_guarded) { mi_check_padding(page, block); }
            if (track_stats) { mi_stat_free(page, block); }
#if MI_DEBUG
            {
                nuint dbgsize = mi_page_block_size(page);
                if (dbgsize > 1 * MI_MiB) { dbgsize = 1 * MI_MiB; }
                _mi_memset_aligned(block, MI_DEBUG_FREED, dbgsize);
            }
#endif

            // actual free: push on the local free list
            mi_block_set_next(page, block, page->local_free);
            page->local_free = block;
            if (--page->used == 0)
            {
                if (page->retire_expire == 0)   // no need to re-retire retired pages (happens when we alloc/free one block repeatedly in an empty page)
                {
                    _mi_page_retire(page);
                }
            }
            else if (check_full && mi_page_is_in_full(page))
            {
                _mi_page_unfull(page);
            }
        }

        // Free a block multi-threaded
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void mi_free_block_mt(mi_page_t* page, mi_block_t* block, bool was_guarded, bool allow_collect)
        {
            // todo: we cannot safely check for double free in _mt -- should check when collecting the thread_free list
            if (!was_guarded) { mi_check_padding(page, block); }   // checking padding is safe for mt
            // adjust stats (after padding check)
            mi_stat_free(page, block);   // stat_free may access the padding
#if MI_DEBUG
            if (!was_guarded)
            {
                nuint dbgsize = mi_usable_size(block);
                if (dbgsize > 1 * MI_MiB) { dbgsize = 1 * MI_MiB; }
                _mi_memset_aligned(block, MI_DEBUG_FREED, dbgsize);
            }
#endif

            // push atomically on the page thread free list
            nuint tf_new;
            nuint tf_old = mi_atomic_load_relaxed(ref page->xthread_free);
            do
            {
                mi_block_set_next(page, block, mi_tf_block(tf_old));
                bool new_owned = (allow_collect ? true : mi_tf_is_owned(tf_old));   // if allow collection then always try to claim it if the page is abandoned
                tf_new = mi_tf_create(block, new_owned);
            } while (!mi_atomic_cas_weak_acq_rel(ref page->xthread_free, ref tf_old, tf_new));

            // and atomically try to collect the page if it was abandoned
            if (allow_collect)
            {
                bool is_owned_now = !mi_tf_is_owned(tf_old);
                if (is_owned_now)
                {
                    mi_assert_internal(mi_page_is_abandoned(page));
                    mi_free_try_collect_mt(page, block);
                }
            }
        }

        // Adjust a block that was allocated aligned, to the actual start of the block in the page.
        // note: this can be called from `mi_free_generic_mt` where a non-owning thread accesses the
        // `page_woffset` and `block_size` fields; however these are constant and the page won't be
        // deallocated (as the block we are freeing keeps it alive) and thus safe to read concurrently.
        public static mi_block_t* _mi_page_ptr_unalign(mi_page_t* page, void* p)
        {
            mi_assert_internal(page != null && p != null);

            nuint diff = (nuint)((byte*)p - mi_page_start(page));
            nuint block_size = mi_page_block_size(page);
            nuint adjust = (_mi_is_power_of_two(block_size) ? diff & (block_size - 1) : diff % block_size);
            return (mi_block_t*)((nuint)p - adjust);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_block_t* mi_validate_block_from_ptr(mi_page_t* page, void* p)
        {
            mi_assert(_mi_page_ptr_unalign(page, p) == (mi_block_t*)p);   // should never be an interior pointer
            return (mi_block_t*)p;
        }

        // C: MI_GUARDED only -- always false in the pinned configuration
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_block_check_unguard(mi_page_t* page, mi_block_t* block, void* p) => false;

        // free a local pointer  (page parameter comes first for better codegen)
        private static void mi_free_generic_local(mi_page_t* page, void* p)
        {
            mi_assert_internal(p != null && page != null);
            mi_block_t* block = (mi_page_has_interior_pointers(page) ? _mi_page_ptr_unalign(page, p) : mi_validate_block_from_ptr(page, p));
            bool was_guarded = mi_block_check_unguard(page, block, p);
            mi_free_block_local(page, block, was_guarded, true /* track stats */, true /* check for a full page */);
        }

        // free a pointer owned by another thread (page parameter comes first for better codegen)
        private static void mi_free_generic_mt(mi_page_t* page, void* p, bool allow_collect)
        {
            mi_assert_internal(p != null && page != null);
            mi_block_t* block = (mi_page_has_interior_pointers(page) ? _mi_page_ptr_unalign(page, p) : mi_validate_block_from_ptr(page, p));
            bool was_guarded = mi_block_check_unguard(page, block, p);
            mi_free_block_mt(page, block, was_guarded, allow_collect);
        }

        // generic free (for runtime integration)
        public static void _mi_free_generic(mi_page_t* page, bool is_local, void* p)
        {
            if (is_local) mi_free_generic_local(page, p);
            else mi_free_generic_mt(page, p, true);
        }

        // Get the page belonging to a pointer
        // Does further checks in debug mode to see if this was a valid pointer.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_page_t* mi_validate_ptr_page(void* p, string msg)
        {
#if MI_DEBUG
            if (((nuint)p & ((nuint)MI_INTPTR_SIZE - 1)) != 0)
            {
                _mi_error_message(EINVAL, $"{msg}: invalid (unaligned) pointer: 0x{(nuint)p:x}");
                return null;
            }
            mi_page_t* page = _mi_safe_ptr_page(p);
            if (p != null && page == null)
            {
                _mi_error_message(EINVAL, $"{msg}: invalid pointer: 0x{(nuint)p:x}");
            }
            return page;
#else
            return _mi_ptr_page(p);
#endif
        }

        // Free a block
        // Fast path written carefully to prevent register spilling on the stack
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void mi_free_ex(void* p, nuint* usable, mi_page_t* page, bool allow_collect)
        {
            if (page == null)   // page will be NULL if p==NULL
            {
                if (usable != null) { *usable = 0; }
                return;
            }
            mi_assert_internal(p != null && page != null);
            if (usable != null) { *usable = mi_page_usable_block_size(page); }

            nuint xtid = (_mi_prim_thread_id() ^ mi_page_xthread_id(page));
            if (xtid == 0)                                  // `tid == mi_page_thread_id(page) && mi_page_flags(page) == 0`
            {
                // thread-local, aligned, and not a full page
                mi_block_t* block = mi_validate_block_from_ptr(page, p);
                mi_free_block_local(page, block, false /* was guarded */, true /* track stats */, false /* no need to check if the page is full */);
            }
            else if (xtid <= MI_PAGE_FLAG_MASK)             // `tid == mi_page_thread_id(page) && mi_page_flags(page) != 0`
            {
                // page is local, but is full or contains (inner) aligned blocks; use generic path
                mi_free_generic_local(page, p);
            }
            // free-ing in a page owned by a theap in another thread, or an abandoned page (not belonging to a theap)
            else if ((xtid & MI_PAGE_FLAG_MASK) == 0)       // `tid != mi_page_thread_id(page) && mi_page_flags(page) == 0`
            {
                // blocks are aligned (and not a full page); push on the thread_free list
                mi_block_t* block = mi_validate_block_from_ptr(page, p);
                mi_free_block_mt(page, block, false /* was_guarded */, allow_collect);
            }
            else
            {
                // page is full or contains (inner) aligned blocks; use generic multi-thread path
                mi_free_generic_mt(page, p, allow_collect);
            }
        }

        public static void mi_free(void* p)
        {
            mi_page_t* page = mi_validate_ptr_page(p, "mi_free");
            mi_free_ex(p, null, page, true);
        }

        public static void mi_ufree(void* p, nuint* usable)
        {
            mi_page_t* page = mi_validate_ptr_page(p, "mi_ufree");
            mi_free_ex(p, usable, page, true);
        }

        public static void mi_free_small(void* p)
        {
            // C: MI_PAGE_META_ALIGNED_FREE_SMALL is 0 in the pinned configuration
            mi_free(p);
        }

        // Free a pointer that is potentially allocated in a different sub-process
        public static void _mi_free_subproc_safe(void* p)
        {
            mi_page_t* page = mi_validate_ptr_page(p, "_mi_free_subproc_safe");
            mi_free_ex(p, null, page, false);
        }

        // --------------------------------------------------------------------------------------------
        // `mi_free_try_collect_mt`: Potentially collect a page in a free in an abandoned page.
        // 1. if the page becomes empty, free it
        // 2. if it can be reclaimed, reclaim it in our theap
        // 3. if it went to < 7/8th used, re-abandon to be mapped (so it can be found by theaps looking for free pages)
        // --------------------------------------------------------------------------------------------

        // Helper for mi_free_try_collect_mt: free if the page has no more used blocks (this is updated by `_mi_page_free_collect(_partly)`)
        private static bool mi_abandoned_page_try_free(mi_page_t* page)
        {
            if (!mi_page_all_free(page)) return false;
            // first remove it from the abandoned pages in the arena (if mapped, this might wait for any readers to finish)
            _mi_arenas_page_unabandon(page, null);
            _mi_arenas_page_free(page, null);   // we can now free the page directly
            return true;
        }

        // Helper for mi_free_try_collect_mt: try if we can reabandon a previously abandoned mostly full page to be mapped
        private static bool mi_abandoned_page_try_reabandon_to_mapped(mi_page_t* page)
        {
            // if the page is unmapped, try to reabandon so it can possibly be mapped and found for allocations
            // We only reabandon if a full page starts to have enough blocks available to prevent immediate re-abandon of a full page
            if (mi_page_is_mostly_used(page)) return false;   // not too full
            if (page->memid.memkind != mi_memkind_t.MI_MEM_ARENA || mi_page_is_abandoned_mapped(page)) return false;   // and not already mapped (or unmappable)

            mi_assert(!mi_page_is_full(page));
            return _mi_arenas_page_try_reabandon_to_mapped(page);
        }

        // Release ownership of a page. This may free or reabandon the page if other blocks are concurrently
        // freed in the meantime.
        // By passing the captured `expected_thread_free`, we can often avoid calling `mi_page_free_collect`.
        private static void mi_abandoned_page_unown_from_free(mi_page_t* page, mi_block_t* expected_thread_free)
        {
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(!mi_page_all_free(page));
            // try to cas atomically the original free list (`mt_free`) back with the ownership cleared.
            nuint tf_expect = mi_tf_create(expected_thread_free, true);
            nuint tf_new = mi_tf_create(expected_thread_free, false);
            while (!mi_atomic_cas_weak_acq_rel(ref page->xthread_free, ref tf_expect, tf_new))
            {
                mi_assert_internal(mi_tf_is_owned(tf_expect));
                // while the xthread_free list is not empty..
                while (mi_tf_block(tf_expect) != null)
                {
                    // if there were concurrent updates to the thread-free list, we retry to free or reabandon to mapped (if it became !mostly_used).
                    _mi_page_free_collect(page, false);   // update used count
                    if (mi_abandoned_page_try_free(page)) return;
                    if (mi_abandoned_page_try_reabandon_to_mapped(page)) return;
                    // otherwise continue un-owning
                    tf_expect = mi_atomic_load_relaxed(ref page->xthread_free);
                }
                // and try again to release ownership
                mi_assert_internal(mi_tf_block(tf_expect) == null);
                tf_new = mi_tf_create(null, false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_page_queue_len_is_atmost(mi_theap_t* theap, nuint block_size, long atmost)
        {
            if (atmost < 0) return false;
            mi_page_queue_t* pq = mi_page_queue(theap, block_size);
            mi_assert_internal(pq != null);
            return (pq->count <= (nuint)atmost);
        }

        // Helper for mi_free_try_collect_mt:  try to reclaim the page for ourselves
        private static bool mi_abandoned_page_try_reclaim(mi_page_t* page, long reclaim_on_free)
        {
            // note: reclaiming can improve benchmarks like `larson` or `rbtree-ck` a lot even in the single-threaded case,
            // since free-ing from an owned page avoids atomic operations. However, if we reclaim too eagerly in
            // a multi-threaded scenario we may start to hold on to too much memory and reduce reuse among threads.
            // If the current theap is where the page originally came from, we reclaim much more eagerly while
            // 'cross-thread' reclaiming on free is by default off (and we only 'reclaim' these by finding the abandoned
            // pages when we allocate a fresh page).
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(!mi_page_all_free(page));
            mi_assert_internal(page->block_size <= MI_MEDIUM_MAX_OBJ_SIZE);
            mi_assert_internal(reclaim_on_free >= 0);

            // dont reclaim if we just have terminated this thread and we should
            // not reinitialize the theap for this thread. (can happen due to thread-local destructors for example -- issue #944)
            if (!_mi_thread_is_initialized()) return false;

            // get our theap
            mi_theap_t* theap = _mi_page_associated_theap_peek(page);
            if (theap == null || theap->tld == null || !theap->allow_page_reclaim) return false;   // see issue #1289

            // set max_reclaim limit
            long max_reclaim = 0;
            if (theap == page->theap)   // did this page originate from the current theap? (and thus allocated from this thread)
            {
                // originating theap
                max_reclaim = _mi_option_get_fast(theap->tld->is_in_threadpool ? mi_option_t.mi_option_page_cross_thread_max_reclaim : mi_option_t.mi_option_page_max_reclaim);
            }
            else if (reclaim_on_free == 1 &&                // if cross-thread is allowed
                     !theap->tld->is_in_threadpool &&       // and we are not part of a threadpool
                     !mi_page_is_mostly_used(page) &&       // and the page is not too full
                     _mi_arena_memid_is_suitable(page->memid, _mi_theap_heap(theap)->exclusive_arena))   // and it fits our memory
            {
                // across threads
                max_reclaim = _mi_option_get_fast(mi_option_t.mi_option_page_cross_thread_max_reclaim);
            }

            // are we within the reclaim limit?
            if (max_reclaim >= 0 && !mi_page_queue_len_is_atmost(theap, page->block_size, max_reclaim))
            {
                return false;
            }

            // reclaim the page into this theap
            // first remove it from the abandoned pages in the arena -- this might wait for any readers to finish
            _mi_arenas_page_unabandon(page, theap);
            _mi_theap_page_reclaim(theap, page);
            __mi_stat_counter_increase(&theap->stats.pages_reclaim_on_free, 1);
            return true;
        }

        // We freed a block in an abandoned page (that was not owned). Try to collect
        private static void mi_free_try_collect_mt(mi_page_t* page, mi_block_t* mt_free)
        {
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(mt_free != null);

            // we own the page now, and it is safe to collect the thread atomic free list
            if (page->block_size <= MI_SMALL_SIZE_MAX)
            {
                // use the `_partly` version to avoid atomic operations since we already have the `mt_free` pointing into the thread free list
                // (after this the `used` count might be too high (as some blocks may have been concurrently added to the thread free list and are yet uncounted).
                //  however, if the page became completely free, the used count is guaranteed to be 0.)
                mi_assert_internal(page->reserved >= 16);   // below this even one freed block goes from full to no longer mostly used.
                _mi_page_free_collect_partly(page, mt_free);
            }
            else
            {
                // for larger blocks we use the regular collect
                _mi_page_free_collect(page, false /* no force */);
                mt_free = null;   // expected page->xthread_free value after collection
            }
            long reclaim_on_free = _mi_option_get_fast(mi_option_t.mi_option_page_reclaim_on_free);
#if MI_DEBUG
            if (mi_page_is_singleton(page)) { mi_assert_internal(mi_page_all_free(page)); }
            if (mi_page_is_full(page)) { mi_assert(mi_page_is_mostly_used(page)); }
#endif

            // try to: 1. free it, 2. reclaim it, or 3. reabandon it to be mapped
            if (mi_abandoned_page_try_free(page)) return;
            if (page->block_size <= MI_MEDIUM_MAX_OBJ_SIZE && reclaim_on_free >= 0)   // early test for better codegen
            {
                if (mi_abandoned_page_try_reclaim(page, reclaim_on_free)) return;
            }
            if (mi_abandoned_page_try_reabandon_to_mapped(page)) return;

            // otherwise unown the page again
            mi_abandoned_page_unown_from_free(page, mt_free);
        }

        // ------------------------------------------------------
        // Usable size
        // ------------------------------------------------------

        // Bytes available in a block
        private static nuint mi_page_usable_aligned_size_of(mi_page_t* page, void* p)
        {
            mi_block_t* block = _mi_page_ptr_unalign(page, p);
            nuint size = mi_page_usable_size_of(page, block, false /* is guarded */);
            mi_assert_internal((void*)p >= (void*)block);
            nuint adjust = (nuint)((byte*)p - (byte*)block);
            mi_assert_internal(adjust <= size);
            nuint aligned_size = (adjust <= size ? size - adjust : 0);
            return aligned_size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint _mi_usable_size(void* p, mi_page_t* page)
        {
            if (page == null) return 0;
            if (!mi_page_has_interior_pointers(page))
            {
                mi_block_t* block = mi_validate_block_from_ptr(page, p);
                return mi_page_usable_size_of(page, block, false /* is guarded */);
            }
            else
            {
                // split out to separate routine for improved code generation
                return mi_page_usable_aligned_size_of(page, p);
            }
        }

        public static nuint mi_usable_size(void* p)
        {
            mi_page_t* page = mi_validate_ptr_page(p, "mi_usable_size");
            return _mi_usable_size(p, page);
        }

        // ------------------------------------------------------
        // Free variants
        // ------------------------------------------------------

        public static void mi_free_size(void* p, nuint size)
        {
#if MI_DEBUG
            mi_page_t* page = mi_validate_ptr_page(p, "mi_free_size");
            nuint available = _mi_usable_size(p, page);
            mi_assert(p == null || size <= available || available == 0 /* invalid pointer */);
#endif
            mi_free(p);
        }

        public static void mi_free_size_aligned(void* p, nuint size, nuint alignment)
        {
            mi_assert(((nuint)p % alignment) == 0);
            mi_free_size(p, size);
        }

        public static void mi_free_aligned(void* p, nuint alignment)
        {
            mi_assert(((nuint)p % alignment) == 0);
            mi_free(p);
        }

        // ------------------------------------------------------
        // Double-free / padding checks
        // (MI_CHECK_DOUBLE_FREE and MI_PADDING are 0 in the pinned configuration)
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_check_is_double_free(mi_page_t* page, mi_block_t* block) => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void mi_check_padding(mi_page_t* page, mi_block_t* block) { }

        // Return the exact usable size of a block (C: MI_PADDING==0 branch)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_page_usable_size_of(mi_page_t* page, mi_block_t* block, bool is_guarded)
        {
            mi_assert_internal(!is_guarded);
            return mi_page_usable_block_size(page);
        }

        public static void _mi_padding_shrink(mi_page_t* page, mi_block_t* block, nuint min_size) { }

        // C: MI_GUARDED only -- nothing to do
        public static void _mi_page_unguard_all(mi_page_t* page) { }

        // only maintain stats for smaller objects if requested (C: MI_STAT>0; the port pins MI_STAT=1)
        private static void mi_stat_free(mi_page_t* page, mi_block_t* block)
        {
            mi_theap_t* theap = _mi_theap_default();
            if (!mi_theap_is_initialized(theap)) return;   // (for now) skip statistics if free'd after thread_done was called (usually a thread cleanup call by the OS)

            nuint bsize = mi_page_usable_block_size(page);
            if (bsize <= MI_LARGE_MAX_OBJ_SIZE)
            {
                __mi_stat_decrease(&theap->stats.malloc_normal, bsize);
            }
            else
            {
                nuint bpsize = mi_page_block_size(page);   // match stat in page.c:mi_huge_page_alloc
                __mi_stat_decrease(&theap->stats.malloc_huge, bpsize);
            }
        }
    }
}
