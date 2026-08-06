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
// Port of mimalloc v3.4.4 `src/page.c` (page lifecycle: free-list collect, init,
// extend, retire, full-queue handling, page search).
// Original: Copyright (c) 2018-2024 Microsoft Research, Daan Leijen (MIT license).
//
// Deferred to the alloc.c port (task #9): `_mi_malloc_generic` and `mi_find_page`
// (they call `_mi_thread_init`, `mi_theap_collect` and `_mi_page_malloc_zero`).
// MI_SECURE < 2: the randomized `mi_page_free_list_extend_secure` path is compiled
// out in C and not ported (the plain sequential extend is always used).

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    // C: mi_deferred_free_fun (registered by runtimes to free more memory on demand)
    internal delegate void mi_deferred_free_fun(bool force, ulong heartbeat, object? arg);

    internal static unsafe partial class Mi
    {
        /* -----------------------------------------------------------
          Page helpers
        ----------------------------------------------------------- */

        // Index a block in a page
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_block_t* mi_page_block_at(mi_page_t* page, void* page_start, nuint block_size, nuint i)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(i <= page->reserved);
            return (mi_block_t*)((byte*)page_start + (i * block_size));
        }

        /* -----------------------------------------------------------
          Page collect the `local_free` and `thread_free` lists
        ----------------------------------------------------------- */

        private static void mi_page_thread_collect_to_local(mi_page_t* page, mi_block_t* head)
        {
            if (head == null) return;

            // find the last block in the list -- also to get a proper use count (without data races)
            nuint max_count = page->capacity;   // cannot collect more than capacity
            nuint count = 1;
            mi_block_t* last = head;
            mi_block_t* next;
            while ((next = mi_block_next(page, last)) != null && count <= max_count)
            {
                count++;
                last = next;
            }

            // if `count > max_count` there was a memory corruption (possibly infinite list due to double multi-threaded free)
            if (count > max_count)
            {
                _mi_error_message(EFAULT, "corrupted thread-free list (possibly due to a cross-thread double free)");
                return;   // the thread-free items cannot be freed
            }
            // if `count > page->used` there was another kind memory corruption (either in the page meta-data or in the linked list)
            else if (count > page->used)
            {
                _mi_error_message(EFAULT, "corrupted meta-data in thread-free list");
                return;   // the thread-free items cannot be freed
            }

            // and append the current local free list
            mi_block_set_next(page, last, page->local_free);
            page->local_free = head;

            // update counts now
            mi_assert_internal(count <= ushort.MaxValue);
            mi_assert_internal(page->used >= (ushort)count);
            page->used = (ushort)(page->used - (ushort)count);
        }

        // Collect the local `thread_free` list using an atomic exchange.
        private static void mi_page_thread_free_collect(mi_page_t* page)
        {
            // atomically capture the thread free list
            mi_block_t* head;
            nuint tfreex;
            nuint tfree = mi_atomic_load_relaxed(ref page->xthread_free);
            do
            {
                head = mi_tf_block(tfree);
                if (head == null) return;   // return if the list is empty
                tfreex = mi_tf_create(null, mi_tf_is_owned(tfree));   // set the thread free list to NULL
            } while (!mi_atomic_cas_weak_acq_rel(ref page->xthread_free, ref tfree, tfreex));
            mi_assert_internal(head != null);

            // and move it to the local list
            mi_page_thread_collect_to_local(page, head);
        }

        // returns `true` if after collection `mi_page_immediate_available` is true.
        private static bool mi_page_free_quick_collect(mi_page_t* page)
        {
            if (page->free != null) return true;
            if (page->local_free == null) return false;
            // move local_free to free
            page->free = page->local_free;
            page->local_free = null;
            page->free_is_zero = false;
            return true;
        }

        public static void _mi_page_free_collect(mi_page_t* page, bool force)
        {
            mi_assert_internal(page != null);

            // collect the thread free list
            mi_page_thread_free_collect(page);

            // and the local free list
            if (page->local_free != null)
            {
                if (page->free == null)
                {
                    // usual case
                    page->free = page->local_free;
                    page->local_free = null;
                    page->free_is_zero = false;
                }
                else if (force)
                {
                    // append -- only on shutdown (force) as this is a linear operation
                    mi_block_t* tail = page->local_free;
                    mi_block_t* next;
                    while ((next = mi_block_next(page, tail)) != null)
                    {
                        tail = next;
                    }
                    mi_block_set_next(page, tail, page->free);
                    page->free = page->local_free;
                    page->local_free = null;
                    page->free_is_zero = false;
                }
            }

            mi_assert_internal(!force || page->local_free == null);
        }

        // Collect elements in the thread-free list starting at `head`. This is an optimized
        // version of `_mi_page_free_collect` to be used from `free.c:_mi_free_collect_mt` that avoids atomic access to `xthread_free`.
        //
        // `head` must be in the `xthread_free` list. It will not collect `head` itself
        // so the `used` count is not fully updated in general. However, if the `head` is
        // the last remaining element, it will be collected and the used count will become `0` (so `mi_page_all_free` becomes true).
        public static void _mi_page_free_collect_partly(mi_page_t* page, mi_block_t* head)
        {
            if (head == null) return;
            mi_block_t* next = mi_block_next(page, head);   // we cannot collect the head element itself as `page->thread_free` may point to it (and we want to avoid atomic ops)
            if (next != null)
            {
                mi_block_set_next(page, head, null);
                mi_page_thread_collect_to_local(page, next);
                if (page->local_free != null && page->free == null)
                {
                    page->free = page->local_free;
                    page->local_free = null;
                    page->free_is_zero = false;
                }
            }
            if (page->used == 1)
            {
                // all elements are free'd since we skipped the `head` element itself
                mi_assert_internal(mi_tf_block(mi_atomic_load_relaxed(ref page->xthread_free)) == head);
                mi_assert_internal(mi_block_next(page, head) == null);
                _mi_page_free_collect(page, false);   // collect the final element
            }
        }

        /* -----------------------------------------------------------
          Page fresh and retire
        ----------------------------------------------------------- */

        // called from `mi_free` on a reclaim, and fresh_alloc if we get an abandoned page
        public static void _mi_theap_page_reclaim(mi_theap_t* theap, mi_page_t* page)
        {
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));

            mi_page_set_theap(page, theap);
            _mi_page_free_collect(page, false);   // ensure used count is up to date
            mi_page_queue_t* pq = mi_theap_page_queue_of(theap, page);
            mi_page_queue_push_at_end(theap, pq, page);
        }

        public static void _mi_page_abandon(mi_page_t* page, mi_page_queue_t* pq)
        {
            _mi_page_free_collect(page, false);   // ensure used count is up to date
            if (mi_page_all_free(page))
            {
                _mi_page_free(page, pq);
            }
            else
            {
                mi_page_queue_remove(pq, page);
                mi_theap_t* theap = page->theap;
                mi_page_set_theap(page, null);
                page->theap = theap;   // don't actually set theap to NULL so we can reclaim_on_free within the same theap
                _mi_arenas_page_abandon(page, theap);
                _mi_arenas_collect(false, false, theap->tld);   // allow purging
            }
        }

        // allocate a fresh page from an arena
        private static mi_page_t* mi_page_fresh_alloc(mi_theap_t* theap, mi_page_queue_t* pq, nuint block_size, nuint page_alignment)
        {
            // !MI_HUGE_PAGE_ABANDON
            mi_assert_internal(pq != null);
            mi_assert_internal(page_alignment > 0 || block_size > MI_LARGE_MAX_OBJ_SIZE || block_size == pq->block_size);

            mi_page_t* page = _mi_arenas_page_alloc(theap, block_size, page_alignment);
            if (page == null)
            {
                // out-of-memory
                return null;
            }
            if (mi_page_is_abandoned(page))
            {
                _mi_theap_page_reclaim(theap, page);
                if (!mi_page_immediate_available(page))
                {
                    if (mi_page_is_expandable(page))
                    {
                        if (!mi_page_extend_free(theap, page))
                        {
                            // cannot commit
                            _mi_page_abandon(page, pq);
                            return null;
                        }
                    }
                    else
                    {
                        mi_assert(false);   // should not happen?
                        return null;
                    }
                }
            }
            else if (pq != null)
            {
                mi_page_queue_push(theap, pq, page);
            }
            mi_assert_internal(pq != null || mi_page_block_size(page) >= block_size);
            return page;
        }

        // Get a fresh page to use
        private static mi_page_t* mi_page_fresh(mi_theap_t* theap, mi_page_queue_t* pq)
        {
            mi_page_t* page = mi_page_fresh_alloc(theap, pq, pq->block_size, 0);
            if (page == null) return null;
            mi_assert_internal(pq->block_size == mi_page_block_size(page));
            mi_assert_internal(pq == mi_theap_page_queue_of(theap, page));
            return page;
        }

        /* -----------------------------------------------------------
          Unfull, abandon, free and retire
        ----------------------------------------------------------- */

        // Move a page from the full list back to a regular list (called from thread-local mi_free)
        public static void _mi_page_unfull(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(mi_page_is_in_full(page));
            mi_assert_internal(!mi_page_theap(page)->allow_page_abandon);
            if (!mi_page_is_in_full(page)) return;

            mi_theap_t* theap = mi_page_theap(page);
            mi_page_queue_t* pqfull = &mi_theap_pages(theap)[MI_BIN_FULL];
            mi_page_set_in_full(page, false);   // to get the right queue
            mi_page_queue_t* pq = mi_theap_page_queue_of(theap, page);
            mi_page_set_in_full(page, true);
            mi_page_queue_enqueue_from_full(pq, pqfull, page);
        }

        private static void mi_page_to_full(mi_page_t* page, mi_page_queue_t* pq)
        {
            mi_assert_internal(pq == mi_page_queue_of(page));
            mi_assert_internal(!mi_page_immediate_available(page));
            mi_assert_internal(!mi_page_is_in_full(page));

            mi_theap_t* theap = mi_page_theap(page);
            if (theap->allow_page_abandon)
            {
                // abandon full pages (this is the usual case in order to allow for sharing of memory between theaps)
                _mi_page_abandon(page, pq);
            }
            else if (!mi_page_is_in_full(page))
            {
                // put full pages in a theap local queue (this is for theaps that cannot abandon, for example, if the theap can be destroyed)
                mi_page_queue_enqueue_from(&mi_theap_pages(mi_page_theap(page))[MI_BIN_FULL], pq, page);
                _mi_page_free_collect(page, false);   // try to collect right away in case another thread freed just before MI_USE_DELAYED_FREE was set
            }
        }

        // Free a page with no more free blocks
        public static void _mi_page_free(mi_page_t* page, mi_page_queue_t* pq)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(pq == mi_page_queue_of(page));
            mi_assert_internal(mi_page_all_free(page));

            // no more aligned blocks in here
            mi_page_set_has_interior_pointers(page, false);

            // remove from the page list
            // (no need to do _mi_theap_delayed_free first as all blocks are already free)
            mi_page_queue_remove(pq, page);

            // and free it
            mi_theap_t* theap = mi_page_theap(page);
            mi_assert_internal(theap != null);
            mi_page_set_theap(page, null);
            _mi_arenas_page_free(page, theap);
            _mi_arenas_collect(false, false, theap->tld);   // allow purging
        }

        private const byte MI_RETIRE_CYCLES = 16;

        // Retire a page with no more used blocks
        // Important to not retire too quickly though as new
        // allocations might coming.
        //
        // Note: called from `mi_free` and benchmarks often
        // trigger this due to freeing everything and then
        // allocating again so careful when changing this.
        public static void _mi_page_retire(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(mi_page_all_free(page));

            if (page->retire_expire != 0) return;   // already retired, just keep it retired
            mi_page_set_has_interior_pointers(page, false);

            // don't retire too often..
            // (or we end up retiring and re-allocating most of the time)
            // for now, we don't retire if it is the only page left of this size class.
            mi_page_queue_t* pq = mi_page_queue_of(page);
            // MI_RETIRE_CYCLES > 0
            nuint bsize = mi_page_block_size(page);
            if (!mi_page_queue_is_special(pq))   // not full or huge queue?
            {
                if (pq->last == page && pq->first == page)   // the only page in the queue?
                {
                    mi_theap_t* theap = mi_page_theap(page);
                    // MI_STAT > 0
                    __mi_stat_counter_increase(&theap->stats.pages_retire, 1);
                    page->retire_expire = (byte)(bsize <= MI_SMALL_MAX_OBJ_SIZE ? MI_RETIRE_CYCLES : MI_RETIRE_CYCLES / 4);
                    mi_page_queue_t* pages = mi_theap_pages(theap);
                    mi_assert_internal(pq >= pages);
                    nuint index = (nuint)(pq - pages);
                    mi_assert_internal(index < MI_BIN_FULL && index < MI_BIN_HUGE);
                    if (index < theap->page_retired_min) theap->page_retired_min = index;
                    if (index > theap->page_retired_max) theap->page_retired_max = index;
                    mi_assert_internal(mi_page_all_free(page));
                    return;   // don't free after all
                }
            }
            _mi_page_free(page, pq);
        }

        // free retired pages: we don't need to look at the entire queues
        // since we only retire pages that are at the head position in a queue.
        public static void _mi_theap_collect_retired(mi_theap_t* theap, bool force)
        {
            nuint min = MI_BIN_FULL;
            nuint max = 0;
            mi_page_queue_t* pages = mi_theap_pages(theap);
            for (nuint bin = theap->page_retired_min; bin <= theap->page_retired_max; bin++)
            {
                mi_page_queue_t* pq = &pages[bin];
                mi_page_t* page = pq->first;
                if (page != null && page->retire_expire != 0)
                {
                    if (mi_page_all_free(page))
                    {
                        page->retire_expire--;
                        if (page->retire_expire == 0 || force)
                        {
                            _mi_page_free(page, pq);
                        }
                        else
                        {
                            // keep retired, update min/max
                            if (bin < min) min = bin;
                            if (bin > max) max = bin;
                        }
                    }
                    else
                    {
                        page->retire_expire = 0;
                    }
                }
            }
            theap->page_retired_min = min;
            theap->page_retired_max = max;
        }

        /* -----------------------------------------------------------
          Initialize the initial free list in a page.
        ----------------------------------------------------------- */

        private const nuint MI_MIN_SLICES = 2;

        private static void mi_page_free_list_extend(mi_page_t* page, nuint bsize, nuint extend)
        {
            // MI_SECURE < 2
            mi_assert_internal(page->free == null);
            mi_assert_internal(page->local_free == null);
            mi_assert_internal(page->capacity + extend <= page->reserved);
            mi_assert_internal(bsize == mi_page_block_size(page));
            void* page_area = mi_page_start(page);

            mi_block_t* start = mi_page_block_at(page, page_area, bsize, page->capacity);

            // initialize a sequential free list
            mi_block_t* last = mi_page_block_at(page, page_area, bsize, page->capacity + extend - 1);
            mi_block_t* block = start;
            while (block <= last)
            {
                mi_block_t* next = (mi_block_t*)((byte*)block + bsize);
                mi_block_set_next(page, block, next);
                block = next;
            }
            // prepend to free list (usually `NULL`)
            mi_block_set_next(page, last, page->free);
            page->free = start;
        }

        /* -----------------------------------------------------------
          Page initialize and extend the capacity
        ----------------------------------------------------------- */

        private const nuint MI_MAX_EXTEND_SIZE = 4 * 1024;   // heuristic, one OS page seems to work well.
        private const nuint MI_MIN_EXTEND = 1;               // MI_SECURE < 2

        // Extend the capacity (up to reserved) by initializing a free list
        // We do at most `MI_MAX_EXTEND` to avoid touching too much memory
        private static bool mi_page_extend_free(mi_theap_t* theap, mi_page_t* page)
        {
            // MI_SECURE < 2
            mi_assert(page->free == null);
            mi_assert(page->local_free == null);
            if (page->free != null) return true;
            if (page->capacity >= page->reserved) return true;

            nuint page_size;
            mi_page_area(page, &page_size);
            // MI_STAT > 0
            __mi_stat_counter_increase(&theap->stats.pages_extended, 1);

            // calculate the extend count
            nuint bsize = mi_page_block_size(page);
            nuint extend = (nuint)page->reserved - page->capacity;
            mi_assert_internal(extend > 0);

            nuint max_extend = (bsize >= MI_MAX_EXTEND_SIZE ? MI_MIN_EXTEND : MI_MAX_EXTEND_SIZE / bsize);
            if (max_extend < MI_MIN_EXTEND) { max_extend = MI_MIN_EXTEND; }
            mi_assert_internal(max_extend > 0);

            if (extend > max_extend)
            {
                // ensure we don't touch memory beyond the page to reduce page commit.
                // the `lean` benchmark tests this. Going from 1 to 8 increases rss by 50%.
                extend = max_extend;
            }

            mi_assert_internal(extend > 0 && extend + page->capacity <= page->reserved);
            mi_assert_internal(extend < ((nuint)1 << 16));

            // commit on demand?
            if (page->slice_committed > 0)
            {
                // reduce extend if it commits more than an arena slice
                if (extend * bsize > MI_ARENA_SLICE_SIZE)
                {
                    extend = _mi_divide_up(MI_ARENA_SLICE_SIZE, bsize);
                }
                // commit required size
                nuint needed_size = (page->capacity + extend) * bsize;
                mi_assert_internal(needed_size <= page_size);
                nuint needed_commit = _mi_align_up(mi_page_slice_offset_of(page, needed_size), MI_PAGE_MIN_COMMIT_SIZE);
                if (needed_commit > page->slice_committed)
                {
                    mi_assert_internal(((needed_commit - page->slice_committed) % _mi_os_page_size()) == 0);
                    if (!_mi_os_commit(_mi_theap_subproc(theap), mi_page_slice_start(page) + page->slice_committed, needed_commit - page->slice_committed, null))
                    {
                        return false;
                    }
                    mi_assert_internal(needed_commit < uint.MaxValue);
                    page->slice_committed = (uint)needed_commit;
                }
            }

            // and append the extend the free list
            // (MI_SECURE < 2: always the sequential extend)
            mi_page_free_list_extend(page, bsize, extend);
            // enable the new free list
            page->capacity += (ushort)extend;
            // MI_STAT > 0
            __mi_stat_increase(&theap->stats.page_committed, extend * bsize);
            return true;
        }

        // Initialize a fresh page (that is already partially initialized)
        public static bool _mi_page_init(mi_theap_t* theap, mi_page_t* page)
        {
            mi_assert(page != null);
            mi_assert(theap != null);

            nuint page_size;
            mi_page_area(page, &page_size);
            mi_assert_internal(page_size / mi_page_block_size(page) < ((nuint)1 << 16));
            mi_assert_internal(page->reserved > 0);

            mi_assert_internal(page->heap != null);
            mi_assert_internal(page->heap == _mi_theap_heap(theap));
            mi_assert_internal(page->theap != null);
            mi_assert_internal(page->theap == mi_page_theap(page));
            mi_assert_internal(page->capacity == 0);
            mi_assert_internal(page->free == null);
            mi_assert_internal(page->used == 0);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(page->xthread_free == 1);
            mi_assert_internal(page->next == null);
            mi_assert_internal(page->prev == null);
            mi_assert_internal(page->retire_expire == 0);
            mi_assert_internal(!mi_page_has_interior_pointers(page));

            // initialize an initial free list
            if (!mi_page_extend_free(theap, page)) return false;
            mi_assert(mi_page_immediate_available(page));
            return true;
        }

        /* -----------------------------------------------------------
          Find pages with free blocks
        -------------------------------------------------------------*/

        // Find a page with free blocks of `page->block_size`.
        private static mi_page_t* mi_page_queue_find_free_ex(mi_theap_t* theap, mi_page_queue_t* pq, bool first_try)
        {
            // search through the pages in "next fit" order
            nuint count = 0;
            long candidate_limit = 0;          // we reset this on the first candidate to limit the search
            long page_full_retain = (pq->block_size > MI_SMALL_MAX_OBJ_SIZE ? 0 : theap->page_full_retain);   // only retain small pages
            mi_page_t* page_candidate = null;  // a page with free space
            mi_page_t* page = pq->first;

            while (page != null)
            {
                mi_page_t* next = page->next;   // remember next (as this page can move to another queue)
                count++;
                candidate_limit--;

                // search up to N pages for a best candidate

                // is the local free list non-empty?
                bool immediate_available = mi_page_immediate_available(page);
                if (!immediate_available)
                {
                    // collect freed blocks by us and other threads to we get a proper use count
                    _mi_page_free_collect(page, false);
                    immediate_available = mi_page_immediate_available(page);
                }

                // if the page is completely full, move it to the `mi_pages_full`
                // queue so we don't visit long-lived pages too often.
                if (!immediate_available && !mi_page_is_expandable(page))
                {
                    page_full_retain--;
                    if (page_full_retain < 0)
                    {
                        mi_assert_internal(!mi_page_is_in_full(page) && !mi_page_immediate_available(page));
                        mi_page_to_full(page, pq);
                    }
                }
                else
                {
                    // the page has free space, make it a candidate
                    // we prefer non-expandable pages with high usage as candidates (to reduce commit, and increase chances of free-ing up pages)
                    if (page_candidate == null)
                    {
                        page_candidate = page;
                        candidate_limit = _mi_option_get_fast(mi_option_t.mi_option_page_max_candidates);
                    }
                    else if (mi_page_all_free(page_candidate))
                    {
                        _mi_page_free(page_candidate, pq);
                        page_candidate = page;
                    }
                    // prefer to reuse fuller pages (in the hope the less used page gets freed)
                    else if (page->used >= page_candidate->used && !mi_page_is_mostly_used(page))
                    {
                        page_candidate = page;
                    }
                    // if we find a non-expandable candidate, or searched for N pages, return with the best candidate
                    if (immediate_available || candidate_limit <= 0)
                    {
                        mi_assert_internal(page_candidate != null);
                        break;
                    }
                }

                page = next;
            }   // for each page

            __mi_stat_counter_increase(&theap->stats.page_searches, count);
            __mi_stat_counter_increase(&theap->stats.page_searches_count, 1);

            // set the page to the best candidate
            if (page_candidate != null)
            {
                page = page_candidate;
            }
            if (page != null)
            {
                if (!mi_page_immediate_available(page))
                {
                    mi_assert_internal(mi_page_is_expandable(page));
                    if (!mi_page_extend_free(theap, page))
                    {
                        page = null;   // failed to extend
                    }
                }
                mi_assert_internal(page == null || mi_page_immediate_available(page));
            }

            if (page == null)
            {
                _mi_theap_collect_retired(theap, false);   // perhaps make a page available
                page = mi_page_fresh(theap, pq);
                mi_assert_internal(page == null || mi_page_immediate_available(page));
                if (page == null && first_try)
                {
                    // out-of-memory _or_ an abandoned page with free blocks was reclaimed, try once again
                    page = mi_page_queue_find_free_ex(theap, pq, false);
                    mi_assert_internal(page == null || mi_page_immediate_available(page));
                }
            }
            else
            {
                mi_assert_internal(page == null || mi_page_immediate_available(page));
                // move the page to the front of the queue
                mi_page_queue_move_to_front(theap, pq, page);
                page->retire_expire = 0;
            }
            mi_assert_internal(page == null || mi_page_immediate_available(page));

            return page;
        }

        // Find a page with free blocks of `size`.
        private static mi_page_t* mi_find_free_page(mi_theap_t* theap, mi_page_queue_t* pq)
        {
            mi_assert_internal(!mi_page_queue_is_huge(pq));

            // check the first page: we even do this with candidate search or otherwise we re-search every time
            mi_page_t* page = pq->first;
            if (page != null && mi_page_free_quick_collect(page))
            {
                // (MI_SECURE < 2: no random extend)
                page->retire_expire = 0;
                return page;   // fast path
            }
            else
            {
                return mi_page_queue_find_free_ex(theap, pq, true);
            }
        }

        /* -----------------------------------------------------------
          Users can register a deferred free function called
          when the `free` list is empty. Since the `local_free`
          is separate this is deterministically called after
          a certain number of allocations.
        ----------------------------------------------------------- */

        // C: _Atomic(void*) pair; the release-store of arg BEFORE fn paired with the
        // acquire-load of fn BEFORE arg guarantees a reader that sees a new handler
        // also sees its matching arg.
        private static mi_deferred_free_fun? mi_deferred_free_handler;
        private static object? mi_deferred_free_arg;

        public static void _mi_deferred_free(mi_theap_t* theap, bool force)
        {
            theap->heartbeat++;
            mi_deferred_free_fun? fun = Volatile.Read(ref mi_deferred_free_handler);
            if (fun != null && !theap->tld->recurse)
            {
                theap->tld->recurse = true;
                object? arg = Volatile.Read(ref mi_deferred_free_arg);
                fun(force, theap->heartbeat, arg);
                theap->tld->recurse = false;
            }
        }

        public static void mi_register_deferred_free(mi_deferred_free_fun? fn, object? arg)
        {
            Volatile.Write(ref mi_deferred_free_arg, arg);
            Volatile.Write(ref mi_deferred_free_handler, fn);
        }

        /* -----------------------------------------------------------
          General allocation
        ----------------------------------------------------------- */

        // Huge pages contain just one block, and the segment contains just that page.
        // Huge pages are also used if the requested alignment is very large (> MI_PAGE_MAX_OVERALLOC_ALIGN)
        // so their size is not always `> MI_LARGE_MAX_OBJ_SIZE`.
        private static mi_page_t* mi_huge_page_alloc(mi_theap_t* theap, nuint size, nuint page_alignment, mi_page_queue_t* pq)
        {
            nuint block_size = _mi_os_good_alloc_size(size);
            // !MI_HUGE_PAGE_ABANDON
            mi_assert_internal(mi_page_queue_is_huge(pq));
            mi_page_t* page = mi_page_fresh_alloc(theap, pq, block_size, page_alignment);
            if (page != null)
            {
                mi_assert_internal(mi_page_block_size(page) >= size);
                mi_assert_internal(mi_page_immediate_available(page));
                mi_assert_internal(mi_page_is_huge(page));
                mi_assert_internal(mi_page_is_singleton(page));
                __mi_stat_increase(&theap->stats.malloc_huge, mi_page_block_size(page));
                __mi_stat_counter_increase(&theap->stats.malloc_huge_count, 1);
            }
            return page;
        }

        // Allocate a page
        // Note: in debug mode the size includes MI_PADDING_SIZE and might have overflowed.
        private static mi_page_t* mi_find_page(mi_theap_t* theap, nuint size, nuint huge_alignment)
        {
            nuint req_size = size - MI_PADDING_SIZE;   // correct for padding_size in case of an overflow on `size`
            if (req_size > unchecked((nuint)MI_MAX_ALLOC_SIZE))
            {
                _mi_error_message(EOVERFLOW, $"allocation request is too large ({req_size} bytes)");
                return null;
            }
            mi_page_queue_t* pq = mi_page_queue(theap, (huge_alignment > 0 ? MI_LARGE_MAX_OBJ_SIZE + 1 : size));
            // huge allocation?
            if (mi_page_queue_is_huge(pq) || req_size > unchecked((nuint)MI_MAX_ALLOC_SIZE))
            {
                return mi_huge_page_alloc(theap, size, huge_alignment, pq);
            }
            else
            {
                // otherwise find a page with free blocks in our size segregated queues
                return mi_find_free_page(theap, pq);
            }
        }

        // Generic allocation routine if the fast path (`alloc.c:mi_page_malloc`) does not succeed.
        // Note: in debug mode the size includes MI_PADDING_SIZE and might have overflowed.
        // The `huge_alignment` is normally 0 but is set to a multiple of MI_SLICE_SIZE for
        // very large requested alignments in which case we use a huge singleton page.
        // Note: we put `bool zero, size_t huge_alignment` into one parameter (with zero in the low bit)
        // to use 4 parameters which compiles better on msvc for the malloc fast path.
        public static void* _mi_malloc_generic(mi_theap_t* theap, nuint size, nuint zero_huge_alignment, nuint* usable)
        {
            bool zero = ((zero_huge_alignment & 1) != 0);
            nuint huge_alignment = (zero_huge_alignment & ~(nuint)1);

            // initialize if necessary
            if (!mi_theap_is_initialized(theap))
            {
                if (theap == _mi_theap_empty_wrong())
                {
                    // we were unable to allocate a theap for a first-class heap
                    return null;
                }
                // otherwise we initialize the thread and its default theap
                theap = _mi_thread_init();
                if (!mi_theap_is_initialized(theap)) { return null; }
            }
            mi_assert_internal(mi_theap_is_initialized(theap));

            // do administrative tasks every N generic mallocs
            if (++theap->generic_count >= 1000)
            {
                theap->generic_collect_count += theap->generic_count;
                theap->generic_count = 0;
                // call potential deferred free routines
                _mi_deferred_free(theap, false);
                // free retired pages
                _mi_theap_collect_retired(theap, false);

                // collect every once in a while (10000 by default)
                long generic_collect = mi_option_get_clamp(mi_option_t.mi_option_generic_collect, 1, 1000000L);
                if (theap->generic_collect_count >= generic_collect)
                {
                    theap->generic_collect_count = 0;
                    mi_theap_collect(theap, false /* force? */);
                }
            }

            // find (or allocate) a page of the right size
            mi_page_t* page = mi_find_page(theap, size, huge_alignment);
            if (page == null)   // first time out of memory, try to collect and retry the allocation once more
            {
                mi_theap_collect(theap, true /* force? */);
                page = mi_find_page(theap, size, huge_alignment);
            }

            if (page == null)   // out of memory
            {
                nuint req_size = size - MI_PADDING_SIZE;   // correct for padding_size in case of an overflow on `size`
                _mi_error_message(ENOMEM, $"unable to allocate memory ({req_size} bytes)");
                return null;
            }

            mi_assert_internal(mi_page_immediate_available(page));
            mi_assert_internal(mi_page_block_size(page) >= size);
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);

            // and try again, this time succeeding! (i.e. this should never recurse through _mi_page_malloc)
            if (usable != null) { *usable = mi_page_usable_block_size(page); }
            void* p = _mi_page_malloc_zero(theap, page, size, zero);
            mi_assert_internal(p != null);

            // move full pages to the full queue
            if (mi_page_block_size(page) > MI_SMALL_MAX_OBJ_SIZE && mi_page_is_full(page))
            {
                mi_page_to_full(page, mi_page_queue_of(page));
            }
            return p;
        }
    }
}
