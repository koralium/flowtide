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
// Port of mimalloc v3.4.4 `src/theap.c` -- thread-local heaps (theaps):
// page visiting, collection (normal/force/abandon), theap init/create,
// reference counting and free-ing, and block/area visiting.
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    // public heap area (C: mimalloc.h `mi_heap_area_t`)
    internal unsafe struct mi_heap_area_t
    {
        public void* blocks;            // start of the area containing theap blocks
        public nuint reserved;          // bytes reserved for this area (virtual)
        public nuint committed;         // current available bytes for this area
        public nuint used;              // number of allocated blocks
        public nuint block_size;        // size in bytes of each block
        public nuint full_block_size;   // size in bytes of a full block including padding and metadata
        public void* reserved1;         // internal
    }

    internal static unsafe partial class Mi
    {
        /* -----------------------------------------------------------
          Helpers
        ----------------------------------------------------------- */

        // return `true` if ok, `false` to break
        // C: theap_page_visitor_fun

        // Visit all pages in a theap; returns `false` if break was called.
        private static bool mi_theap_visit_pages(mi_theap_t* theap, delegate*<mi_theap_t*, mi_page_queue_t*, mi_page_t*, void*, void*, bool> fn, bool include_full, void* arg1, void* arg2)
        {
            if (theap == null || theap->page_count == 0) return true;

            // visit all pages
#if MI_DEBUG
            nuint total = theap->page_count;
            nuint count = 0;
#endif

            nuint max_bin = (nuint)(include_full ? MI_BIN_FULL : MI_BIN_FULL - 1);
            for (nuint i = 0; i <= max_bin; i++)
            {
                mi_page_queue_t* pq = &mi_theap_pages(theap)[i];
                mi_page_t* page = pq->first;
                while (page != null)
                {
                    mi_page_t* next = page->next;   // save next in case the page gets removed from the queue
                    mi_assert_internal(mi_page_theap(page) == theap);
#if MI_DEBUG
                    count++;
#endif
                    if (!fn(theap, pq, page, arg1, arg2)) return false;
                    page = next;   // and continue
                }
            }
#if MI_DEBUG
            mi_assert_internal(!include_full || count == total);
#endif
            return true;
        }

        /* -----------------------------------------------------------
          "Collect" pages by migrating `local_free` and `thread_free`
          lists and freeing empty pages. This is done when a thread
          stops (and in that case abandons pages if there are still
          blocks alive)
        ----------------------------------------------------------- */

        private enum mi_collect_t
        {
            MI_NORMAL,
            MI_FORCE,
            MI_ABANDON
        }

        private static bool mi_theap_page_collect(mi_theap_t* theap, mi_page_queue_t* pq, mi_page_t* page, void* arg_collect, void* arg2)
        {
            mi_collect_t collect = *((mi_collect_t*)arg_collect);
            _mi_page_free_collect(page, collect >= mi_collect_t.MI_FORCE);
            if (mi_page_all_free(page))
            {
                // no more used blocks, possibly free the page.
                if (collect >= mi_collect_t.MI_FORCE || page->retire_expire == 0)   // either forced/abandon, or not already retired
                {
                    // note: this will potentially free retired pages as well.
                    _mi_page_free(page, pq);
                }
            }
            else if (collect == mi_collect_t.MI_ABANDON)
            {
                // still used blocks but the thread is done; abandon the page
                _mi_page_abandon(page, pq);
            }
            return true;   // don't break
        }

        private static void mi_theap_merge_stats(mi_theap_t* theap)
        {
            mi_assert_internal(mi_theap_is_initialized(theap));
            mi_heap_t* heap = _mi_theap_heap(theap);
            _mi_stats_merge_into(&heap->stats, &theap->stats);
        }

        private static void mi_theap_collect_ex(mi_theap_t* theap, mi_collect_t collect)
        {
            if (theap == null || !mi_theap_is_initialized(theap)) return;

            bool force = (collect >= mi_collect_t.MI_FORCE);
            _mi_deferred_free(theap, force);

            // collect retired pages
            _mi_theap_collect_retired(theap, force);

            // collect all pages owned by this thread (dont normally visit full pages, see issue #1220)
            mi_collect_t collectArg = collect;
            mi_theap_visit_pages(theap, &mi_theap_page_collect, (collect != mi_collect_t.MI_NORMAL), &collectArg, null);

            // collect arenas (this is program wide so don't force purges on abandonment of threads)
            _mi_arenas_collect(collect == mi_collect_t.MI_FORCE /* force purge? */, collect >= mi_collect_t.MI_FORCE /* visit all? */, theap->tld);

            // merge statistics
            mi_theap_merge_stats(theap);
        }

        public static void _mi_theap_collect_abandon(mi_theap_t* theap)
        {
            mi_theap_collect_ex(theap, mi_collect_t.MI_ABANDON);
        }

        public static void mi_theap_collect(mi_theap_t* theap, bool force)
        {
            mi_theap_collect_ex(theap, (force ? mi_collect_t.MI_FORCE : mi_collect_t.MI_NORMAL));
        }

        public static void mi_collect(bool force)
        {
            // cannot really collect process wide, just a theap..
            mi_theap_collect(_mi_theap_default(), force);
        }

        public static void mi_heap_collect(mi_heap_t* heap, bool force)
        {
            // cannot really collect a heap, just a theap..
            mi_theap_collect(mi_heap_theap(heap), force);
        }

        /* -----------------------------------------------------------
          Heap new
        ----------------------------------------------------------- */

        public static mi_theap_t* mi_theap_get_default()
        {
            mi_theap_t* theap = _mi_theap_default();
            if (!mi_theap_is_initialized(theap))
            {
                mi_thread_init();
                theap = _mi_theap_default();
                mi_assert_internal(mi_theap_is_initialized(theap));
            }
            return theap;
        }

        public static mi_theap_t* mi_theap_set_default(mi_theap_t* theap)
        {
            mi_theap_t* previous = mi_theap_get_default();
            if (mi_theap_is_initialized(theap))
            {
                _mi_theap_default_set(theap);
            }
            return previous;
        }

        // C: MI_GUARDED only -- no-op in the pinned configuration
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void _mi_theap_guarded_init(mi_theap_t* theap) { }

        public static void _mi_theap_init(mi_theap_t* theap, mi_heap_t* heap, mi_tld_t* tld)
        {
            mi_assert_internal(theap != null);
            mi_assert_internal(heap != null);
            mi_assert_internal(tld != null);
            mi_memid_t memid = theap->memid;
            _mi_memcpy_aligned(theap, _mi_theap_empty(), (nuint)sizeof(mi_theap_t));
            theap->memid = memid;
            theap->tld = tld;   // avoid reading the thread-local tld during initialization
            mi_atomic_store_release(ref theap->refcount, 1);
            mi_atomic_store_release(ref theap->freed, 0);
            mi_atomic_store_ptr_release(&theap->subproc, heap->subproc);
            mi_assert_internal(theap->stats.size == (nuint)sizeof(mi_stats_t));

            _mi_theap_options_init(theap);

            if (theap->tld->is_in_threadpool)
            {
                // if we run as part of a thread pool it is better to not arbitrarily reclaim abandoned pages into our theap.
                // this is checked in `free.c:mi_free_try_collect_mt`
                // .. but abandoning is good in this case: quarter the full page retain (possibly to 0)
                // (so blocked threads do not hold on to too much memory)
                if (theap->page_full_retain > 0)
                {
                    theap->page_full_retain = theap->page_full_retain / 4;
                }
            }

            // push on the thread local theaps list
            mi_theap_t* head = null;
            mi_random_ctx_t head_random = default;
            mi_lock_acquire(&theap->tld->theaps_lock);
            try
            {
                head = theap->tld->theaps;
                theap->tprev = null;
                theap->tnext = head;
                theap->tld->theaps = theap;
                if (head != null)
                {
                    head->tprev = theap;
                    head_random = head->random;
                }
            }
            finally
            {
                mi_lock_release(&theap->tld->theaps_lock);
            }

            // initialize random if this is the first theap of this thread
            if (head == null)
            {
                // (the C Windows static-link special case for thread_seq==0 -- weak random to avoid
                //  bcrypt loader issues, issue #1185 -- does not apply to the managed port)
                _mi_random_init(&theap->random);
            }
            else
            {
                _mi_random_split(&head_random, &theap->random);   // &theap->random is used as nonce so it is ok if threads capture the same head->random
            }
            theap->cookie = _mi_theap_random_next(theap) | 1;
            _mi_theap_guarded_init(theap);   // needs theap->random
            __mi_stat_increase_mt(&_mi_theap_subproc(theap)->stats.theaps, 1);   // on subproc to match theap_free_mem

            // only now set the heap member as it is used to determine if a theap is initialized
            mi_atomic_store_ptr_release(&theap->heap, heap);

            // push on the heap's theap list
            mi_lock_acquire(&heap->theaps_lock);
            try
            {
                head = heap->theaps;
                theap->hprev = null;
                theap->hnext = head;
                if (head != null) { head->hprev = theap; }
                heap->theaps = theap;
            }
            finally
            {
                mi_lock_release(&heap->theaps_lock);
            }
        }

        public static mi_theap_t* _mi_theap_create(mi_heap_t* heap, mi_tld_t* tld)
        {
            mi_assert_internal(tld != null);
            mi_assert_internal(heap != null);
            mi_assert_internal(_mi_thread_id() == tld->thread_id);

            // allocate and initialize a theap
            mi_memid_t memid;
            mi_theap_t* theap;

            if (heap->exclusive_arena == null)
            {
                theap = (mi_theap_t*)_mi_meta_zalloc(heap->subproc, (nuint)sizeof(mi_theap_t), &memid);
            }
            else
            {
                // theaps associated with a specific arena are allocated in that arena
                // note: takes up at least one slice which is quite wasteful...
                nuint size = _mi_align_up((nuint)sizeof(mi_theap_t), MI_ARENA_MIN_OBJ_SIZE);
                theap = (mi_theap_t*)_mi_arenas_alloc(heap, size, true, true, heap->exclusive_arena, tld->thread_seq, tld->numa_node, &memid);
            }
            if (theap == null)
            {
                _mi_error_message(ENOMEM, "unable to allocate theap meta-data");
                return null;
            }

            theap->memid = memid;
            _mi_theap_init(theap, heap, tld);
            return theap;
        }

        public static nuint _mi_theap_random_next(mi_theap_t* theap)
        {
            return _mi_random_next(&theap->random);
        }

        private static void mi_theap_free_mem(mi_theap_t* theap)
        {
            if (theap != null)
            {
                __mi_stat_decrease_mt(&_mi_theap_subproc(theap)->stats.theaps, 1);
                // free the used memory
                if (theap->memid.memkind == mi_memkind_t.MI_MEM_HEAP_MAIN)   // note: for now unused (as in C)
                {
                    mi_assert_internal(_mi_is_heap_main(mi_heap_of(theap)));
                    _mi_free_subproc_safe(theap);
                }
                else if (theap->memid.memkind == mi_memkind_t.MI_MEM_META)
                {
                    _mi_meta_free(_mi_theap_subproc(theap), theap, (nuint)sizeof(mi_theap_t), theap->memid);
                }
                else
                {
                    _mi_arenas_free(_mi_theap_subproc(theap), theap, _mi_align_up((nuint)sizeof(mi_theap_t), MI_ARENA_MIN_OBJ_SIZE), theap->memid);   // issue #1168, avoid assertion failure
                }
            }
        }

        // we need to reference count theaps due to the _mi_theap_cached thread locals
        public static void _mi_theap_incref(mi_theap_t* theap)
        {
            if (theap != null && !mi_memid_needs_no_free(theap->memid))
            {
                mi_atomic_increment_acq_rel(ref theap->refcount);
            }
        }

        public static void _mi_theap_decref(mi_theap_t* theap)
        {
            if (theap != null && !mi_memid_needs_no_free(theap->memid))
            {
                if (mi_atomic_decrement_acq_rel(ref theap->refcount) == 1)
                {
                    mi_theap_free_mem(theap);
                }
            }
        }

        // called from `mi_theap_delete` to free the internal theap resources.
        public static bool _mi_theap_free(mi_theap_t* theap, bool acquire_heap_theaps_lock, bool acquire_tld_theaps_lock)
        {
            mi_assert(theap != null);
            if (theap == null) return true;

            // ensure only one thread actually frees the theap
            nuint freed = mi_atomic_exchange_acq_rel(ref theap->freed, 1);
            if (freed != 0)
            {
                // concurrent interaction, retry in an outer loop (as the other thread may be blocked on our lock)
                return false;
            }
            else
            {
                // merge stats to the owning heap
                mi_heap_t* heap = _mi_theap_heap(theap);
                _mi_stats_merge_into(&heap->stats, &theap->stats);

                // remove ourselves from the heap theaps list (C: mi_lock_maybe)
                if (acquire_heap_theaps_lock) { mi_lock_acquire(&heap->theaps_lock); }
                try
                {
                    if (theap->hnext != null) { theap->hnext->hprev = theap->hprev; }
                    if (theap->hprev != null) { theap->hprev->hnext = theap->hnext; }
                    else { mi_assert_internal(heap->theaps == theap); heap->theaps = theap->hnext; }
                    theap->hnext = theap->hprev = null;
                }
                finally
                {
                    if (acquire_heap_theaps_lock) { mi_lock_release(&heap->theaps_lock); }
                }

                // remove ourselves from the thread local theaps list (C: mi_lock_maybe)
                if (acquire_tld_theaps_lock) { mi_lock_acquire(&theap->tld->theaps_lock); }
                try
                {
                    if (theap->tnext != null) { theap->tnext->tprev = theap->tprev; }
                    if (theap->tprev != null) { theap->tprev->tnext = theap->tnext; }
                    else { mi_assert_internal(theap->tld->theaps == theap); theap->tld->theaps = theap->tnext; }
                    theap->tnext = theap->tprev = null;
                }
                finally
                {
                    if (acquire_tld_theaps_lock) { mi_lock_release(&theap->tld->theaps_lock); }
                }

                // Set heap to NULL only after we are removed from the thread local theaps list since
                // we may concurrently traverse it to collect (in `init.c:mi_thread_theaps_done`)
                // (We need to set it to NULL to avoid an ABA problem where the _mi_theap_cached
                // has a heap address that is reused for a newly allocated heap.)
                mi_atomic_store_ptr_release(&theap->heap, (mi_heap_t*)null);
                theap->tld = null;
                // leave subproc field as is for free-ing
                _mi_theap_decref(theap);
                return true;
            }
        }

        /* -----------------------------------------------------------
          Visit all theap blocks and areas
        ----------------------------------------------------------- */

        public static void _mi_heap_area_init(mi_heap_area_t* area, mi_page_t* page)
        {
            nuint bsize = mi_page_block_size(page);
            nuint ubsize = mi_page_usable_block_size(page);
            area->reserved = page->reserved * bsize;
            area->committed = page->capacity * bsize;
            area->blocks = mi_page_start(page);
            area->used = page->used;   // number of blocks in use (#553)
            area->block_size = ubsize;
            area->full_block_size = bsize;
            area->reserved1 = page;
        }

        private static void mi_get_fast_divisor(nuint divisor, ulong* magic, nuint* shift)
        {
            mi_assert_internal(divisor > 0 && divisor <= uint.MaxValue);
            *shift = (nuint)MI_SIZE_BITS - mi_clz(divisor - 1);
            *magic = ((((ulong)1 << 32) * (((ulong)1 << (int)*shift) - divisor)) / divisor + 1);
        }

        private static nuint mi_fast_divide(nuint n, ulong magic, nuint shift)
        {
            mi_assert_internal(n <= uint.MaxValue);
            ulong hi = ((ulong)n * magic) >> 32;
            return (nuint)((hi + n) >> (int)shift);
        }

        public static bool _mi_theap_area_visit_blocks(mi_heap_area_t* area, mi_page_t* page, delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor, void* arg)
        {
            mi_assert(area != null);
            if (area == null) return true;
            mi_assert(page != null);
            if (page == null) return true;

            _mi_page_free_collect(page, true);   // collect both thread_delayed and local_free
            mi_assert_internal(page->local_free == null);
            if (page->used == 0) return true;

            nuint psize;
            byte* pstart = mi_page_area(page, &psize);
            mi_heap_t* heap = mi_page_heap(page);
            nuint bsize = mi_page_block_size(page);
            nuint ubsize = mi_page_usable_block_size(page);   // without padding

            // optimize page with one block
            if (page->capacity == 1)
            {
                mi_assert_internal(page->used == 1 && page->free == null);
                return visitor(heap, area, pstart, ubsize, arg);
            }
            mi_assert(bsize <= uint.MaxValue);

            // optimize full pages
            if (page->used == page->capacity)
            {
                byte* fullblock = pstart;
                for (nuint i = 0; i < page->capacity; i++)
                {
                    if (!visitor(heap, area, fullblock, ubsize, arg)) return false;
                    fullblock += bsize;
                }
                return true;
            }

            // create a bitmap of free blocks.
            const int MI_MAX_BLOCKS = (int)(MI_SMALL_PAGE_SIZE / (uint)MI_INTPTR_SIZE);
            mi_assert_internal(page->capacity <= MI_MAX_BLOCKS);
            nuint* free_map = stackalloc nuint[MI_MAX_BLOCKS / MI_INTPTR_BITS];
            nuint bmapsize = _mi_divide_up(page->capacity, (nuint)MI_INTPTR_BITS);
            _mi_memzero(free_map, bmapsize * (nuint)MI_INTPTR_SIZE);
            if (page->capacity % MI_INTPTR_BITS != 0)
            {
                // mark left-over bits at the end as free
                int shiftBits = (int)(page->capacity % MI_INTPTR_BITS);
                nuint mask = (nuint.MaxValue << shiftBits);
                free_map[bmapsize - 1] = mask;
            }

            // fast repeated division by the block size
            ulong magic;
            nuint shift;
            mi_get_fast_divisor(bsize, &magic, &shift);

#if MI_DEBUG
            nuint free_count = 0;
#endif
            for (mi_block_t* block = page->free; block != null; block = mi_block_next(page, block))
            {
#if MI_DEBUG
                free_count++;
#endif
                mi_assert_internal((byte*)block >= pstart && (byte*)block < (pstart + psize));
                nuint offset = (nuint)((byte*)block - pstart);
                mi_assert_internal(offset % bsize == 0);
                mi_assert_internal(offset <= uint.MaxValue);
                nuint blockidx = mi_fast_divide(offset, magic, shift);
                mi_assert_internal(blockidx == offset / bsize);
                mi_assert_internal(blockidx < MI_MAX_BLOCKS);
                nuint bitidx = (blockidx / (nuint)MI_INTPTR_BITS);
                nuint bit = blockidx - (bitidx * (nuint)MI_INTPTR_BITS);
                free_map[bitidx] |= ((nuint)1 << (int)bit);
            }
#if MI_DEBUG
            mi_assert_internal(page->capacity == (free_count + page->used));
#endif

            // walk through all blocks skipping the free ones
#if MI_DEBUG
            nuint used_count = 0;
#endif
            byte* walkblock = pstart;
            for (nuint i = 0; i < bmapsize; i++)
            {
                if (free_map[i] == 0)
                {
                    // every block is in use
                    for (nuint j = 0; j < MI_INTPTR_BITS; j++)
                    {
#if MI_DEBUG
                        used_count++;
#endif
                        if (!visitor(heap, area, walkblock, ubsize, arg)) return false;
                        walkblock += bsize;
                    }
                }
                else
                {
                    // visit the used blocks in the mask
                    nuint m = ~free_map[i];
                    while (m != 0)
                    {
#if MI_DEBUG
                        used_count++;
#endif
                        nuint bitidx = mi_ctz(m);
                        if (!visitor(heap, area, walkblock + (bitidx * bsize), ubsize, arg)) return false;
                        m &= m - 1;   // clear least significant bit
                    }
                    walkblock += bsize * (nuint)MI_INTPTR_BITS;
                }
            }
#if MI_DEBUG
            mi_assert_internal(page->used == used_count);
#endif
            return true;
        }

        // Separate struct to keep `mi_page_t` out of the public interface
        private struct mi_theap_area_ex_t
        {
            public mi_heap_area_t area;
            public mi_page_t* page;
        }

        private static bool mi_theap_visit_areas_page(mi_theap_t* theap, mi_page_queue_t* pq, mi_page_t* page, void* vfun, void* arg)
        {
            var fun = (delegate*<mi_theap_t*, mi_theap_area_ex_t*, void*, bool>)vfun;
            mi_theap_area_ex_t xarea;
            xarea.page = page;
            _mi_heap_area_init(&xarea.area, page);
            return fun(theap, &xarea, arg);
        }

        // Visit all theap pages as areas
        private static bool mi_theap_visit_areas(mi_theap_t* theap, delegate*<mi_theap_t*, mi_theap_area_ex_t*, void*, bool> visitor, void* arg)
        {
            if (visitor == null) return false;
            return mi_theap_visit_pages(theap, &mi_theap_visit_areas_page, true, (void*)visitor, arg);   // note: function pointer to void* :-{
        }

        // Just to pass arguments
        private struct mi_visit_blocks_args_t
        {
            public bool visit_blocks;
            public delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor;
            public void* arg;
        }

        private static bool mi_theap_area_visitor(mi_theap_t* theap, mi_theap_area_ex_t* xarea, void* arg)
        {
            mi_visit_blocks_args_t* args = (mi_visit_blocks_args_t*)arg;
            if (!args->visitor(_mi_theap_heap(theap), &xarea->area, null, xarea->area.block_size, args->arg)) return false;
            if (args->visit_blocks)
            {
                return _mi_theap_area_visit_blocks(&xarea->area, xarea->page, args->visitor, args->arg);
            }
            else
            {
                return true;
            }
        }

        // Visit all blocks in a theap
        public static bool mi_theap_visit_blocks(mi_theap_t* theap, bool visit_blocks, delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor, void* arg)
        {
            mi_visit_blocks_args_t args;
            args.visit_blocks = visit_blocks;
            args.visitor = visitor;
            args.arg = arg;
            return mi_theap_visit_areas(theap, &mi_theap_area_visitor, &args);
        }
    }
}
