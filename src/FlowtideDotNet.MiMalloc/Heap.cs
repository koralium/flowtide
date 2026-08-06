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
// Port of mimalloc v3.4.4 `src/heap.c` -- heaps (shared, thread-safe) and the
// heap<->theap association, plus the `_mi_heap_theap*` inlines of prim-tls.h.
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Includes the full heap lifecycle (new/delete/destroy) on top of the
// malloc/free API (Alloc.cs / Free.cs).

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        /* -----------------------------------------------------------
          The `_mi_heap_theap*` inlines (C: prim-tls.h; MI_THEAP_INITASNULL)
        ----------------------------------------------------------- */

        // Get (and possibly create) the theap belonging to a heap.
        // We cache the last accessed theap in `_mi_theap_cached` for better performance.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_theap_t* _mi_heap_theap(mi_heap_t* heap)
        {
            mi_theap_t* theap = _mi_theap_cached();
            if (theap != null && _mi_theap_heap_peek(theap) == heap) return theap;
            return _mi_heap_theap_get_or_init(heap);
        }

        // Get the theap belonging to a heap without creating it if it is not yet initialized.
        public static mi_theap_t* _mi_heap_theap_peek(mi_heap_t* heap)
        {
            mi_theap_t* theap = _mi_theap_cached();
            if (theap != null && _mi_theap_heap_peek(theap) == heap) return theap;
            theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);   // don't update the cache on a query
            mi_assert_internal(theap == null || (!_mi_is_empty_theap(theap) && _mi_theap_heap_peek(theap) == heap));
            return theap;
        }

        // Find the associated theap or NULL if it does not exist (during shutdown)
        // Should be fast as it is called in `free.c:mi_free_try_collect`.
        public static mi_theap_t* _mi_page_associated_theap_peek(mi_page_t* page)
        {
            mi_heap_t* heap = mi_page_heap(page);
            mi_theap_t* theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);
            if (theap == null) return null;
            if (_mi_theap_heap_peek(theap) != heap) return null;   // should never happen, but can happen for a free across subprocesses
            mi_assert_internal(!_mi_is_empty_theap(theap) && _mi_thread_id() == theap->tld->thread_id);
            return theap;
        }

        /* -----------------------------------------------------------
          Heap's
        ----------------------------------------------------------- */

        public static mi_theap_t* mi_heap_theap(mi_heap_t* heap)
        {
            return _mi_heap_theap(heap);
        }

        // (`mi_heap_get_heap_main` is in Internal.cs with the other internal.h inlines)

        public static void mi_heap_set_numa_affinity(mi_heap_t* heap, int numa_node)
        {
            if (heap == null) { heap = mi_heap_main(); }
            heap->numa_node = (numa_node < 0 ? -1 : numa_node % _mi_os_numa_node_count());
        }

        public static void mi_heap_stats_merge_to_subproc(mi_heap_t* heap)
        {
            if (heap == null) { heap = mi_heap_main(); }
            _mi_stats_merge_into(&heap->subproc->stats, &heap->stats);
        }

        public static void mi_heap_stats_merge_to_main(mi_heap_t* heap)
        {
            if (heap == null) return;
            _mi_stats_merge_into(&mi_heap_get_heap_main(heap)->stats, &heap->stats);
        }

        public static bool _mi_heap_theap_set(mi_heap_t* heap, mi_theap_t* theap)
        {
            mi_assert_internal((nuint)theap == 1 || _mi_theap_heap(theap) == heap);
            mi_assert_internal(!_mi_is_empty_theap(theap));
            mi_assert_internal(heap->theap != 0);
            return _mi_thread_local_set(heap->theap, theap);
        }

        private static mi_theap_t* mi_heap_init_theap(mi_heap_t* heap)
        {
            mi_assert_internal(heap != null);

            // initialize thread first in case this is the main heap
            // (which may allocate the default theap already for the main heap)
            if (!_mi_thread_is_initialized())
            {
                mi_thread_init();
            }

            // get the thread local theap
            mi_theap_t* theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);

            // create a fresh theap?
            if (theap == null)
            {
                // allocate a fresh theap
                theap = _mi_theap_create(heap, mi_theap_get_default()->tld);   // sets the theap thread local
                if (theap == null)
                {
                    _mi_error_message(EFAULT, "unable to allocate memory for a thread local heap");
                    return null;
                }
                _mi_heap_theap_set(heap, theap);
                mi_assert_internal(theap == (mi_theap_t*)_mi_thread_local_get(heap->theap));
            }
            return theap;
        }

        // get (and possibly create) the theap belonging to a heap
        public static mi_theap_t* _mi_heap_theap_get_or_init(mi_heap_t* heap)
        {
            mi_assert_internal(heap->theap != 0);
            mi_theap_t* theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);
            if (theap == null)
            {
                theap = mi_heap_init_theap(heap);
                if (theap == null) { return _mi_theap_empty_wrong(); }   // this will return NULL from page.c:_mi_malloc_generic
            }
            _mi_theap_cached_set(theap);
            return theap;
        }

        // C: heap.c `mi_heap_initialize` (used by _mi_heap_new_for_subproc in task #9)
        private static void mi_heap_initialize(mi_heap_t* heap, nuint theap_slot, mi_subproc_t* subproc, void* exclusive_arena_id)
        {
            // init fields
            heap->theap = theap_slot;
            heap->subproc = subproc;
            heap->heap_seq = mi_atomic_increment_relaxed(ref subproc->heap_total_count);
            heap->exclusive_arena = _mi_arena_from_id(exclusive_arena_id);
            heap->numa_node = -1;   // no initial affinity
            mi_stats_header_init(&heap->stats);
            mi_lock_init(&heap->theaps_lock);
            mi_lock_init(&heap->os_abandoned_pages_lock);
            mi_lock_init(&heap->arena_pages_lock);

            // push onto the subproc heaps
            mi_lock_acquire(&heap->subproc->heaps_lock);
            try
            {
                mi_heap_t* head = heap->subproc->heaps;
                heap->prev = null;
                heap->next = head;
                if (head != null) { head->prev = heap; }
                heap->subproc->heaps = heap;
            }
            finally
            {
                mi_lock_release(&heap->subproc->heaps_lock);
            }
            mi_atomic_increment_relaxed(ref subproc->heap_count);
            __mi_stat_increase_mt(&subproc->stats.heaps, 1);
            mi_assert_internal(_mi_is_heap_main(heap) ? heap->theap == mi_thread_local_key_fast : heap->theap != 0);
        }

        // free all theaps belonging to this heap (without deleting their pages as we do this arena wise for efficiency)
        private static void mi_heap_free_theaps(mi_heap_t* heap)
        {
            // This can run concurrently with a thread that terminates (see `init.c:mi_thread_theaps_done`),
            // and we need to ensure we free theaps atomically.
            // We do this in a loop where we release the theaps_lock at every potential re-iteration to unblock
            // potential concurrent thread termination which tries to remove the theap from our theaps list.
            bool all_freed;
            do
            {
                all_freed = true;
                mi_lock_acquire(&heap->theaps_lock);
                try
                {
                    mi_theap_t* theap = heap->theaps;
                    while (theap != null)
                    {
                        mi_theap_t* next = theap->hnext;
                        if (!_mi_theap_free(theap, false /* dont re-acquire the heap->theaps_lock */, true /* acquire the tld->theaps_lock though */))
                        {
                            all_freed = false;
                        }
                        theap = next;
                    }
                }
                finally
                {
                    mi_lock_release(&heap->theaps_lock);
                }
                if (!all_freed)
                {
                    __mi_stat_counter_increase_mt(&heap->stats.heaps_delete_wait, 1);
                    _mi_prim_thread_yield();
                }
                else
                {
                    mi_assert_internal(heap->theaps == null);
                }
            } while (!all_freed);
        }

        // C: heap.c `_mi_heap_new_for_subproc`
        public static mi_heap_t* _mi_heap_new_for_subproc(mi_subproc_t* subproc, void* exclusive_arena_id, bool is_main_heap)
        {
            mi_assert_internal(is_main_heap
                ? (mi_atomic_load_ptr_relaxed(&subproc->heap_main) == null && subproc->parent != null)
                : mi_atomic_load_ptr_relaxed(&subproc->heap_main) != null);
            // heap data is allocated in the current subproc
            mi_heap_t* heap_main = (is_main_heap ? mi_atomic_load_ptr_relaxed(&subproc->parent->heap_main) : mi_atomic_load_ptr_relaxed(&subproc->heap_main));
            mi_heap_t* heap = (mi_heap_t*)mi_heap_zalloc(heap_main, (nuint)sizeof(mi_heap_t));
            if (heap == null) return null;
            // reserve a thread local slot for this heap (see also issue #1230)
            nuint theap_slot = (is_main_heap ? mi_thread_local_key_fast : _mi_thread_local_create());
            if (theap_slot == 0)
            {
                _mi_error_message(EFAULT, "unable to dynamically create a thread local for a heap");
                mi_free(heap);
                return null;
            }
            if (is_main_heap)
            {
                mi_assert_internal(mi_atomic_load_ptr_relaxed(&subproc->heap_main) == null);
                mi_atomic_store_ptr_release(&subproc->heap_main, heap);
            }
            mi_heap_initialize(heap, theap_slot, subproc, exclusive_arena_id);
            return heap;
        }

        public static mi_heap_t* mi_heap_new_in_arena(void* exclusive_arena_id)
        {
            return _mi_heap_new_for_subproc(_mi_subproc(), exclusive_arena_id, false);
        }

        public static mi_heap_t* mi_heap_new()
        {
            return mi_heap_new_in_arena(null);
        }

        // free the heap resources (assuming the pages are already moved/destroyed, and all theaps have been freed)
        private static void mi_heap_free(mi_heap_t* heap, bool acquire_heaps_lock)
        {
            mi_assert_internal(heap != null && !_mi_is_heap_main(heap));

            // free all arena pages infos
            mi_lock_acquire(&heap->arena_pages_lock);
            try
            {
                for (int i = 0; i < MI_MAX_ARENAS; i++)
                {
                    mi_arena_pages_t* arena_pages = (mi_arena_pages_t*)mi_atomic_load_relaxed(ref *(nuint*)&heap->arena_pages[i]);
                    if (arena_pages != null)
                    {
                        mi_atomic_store_relaxed(ref *(nuint*)&heap->arena_pages[i], 0);
                        _mi_free_subproc_safe(arena_pages);
                    }
                }
            }
            finally
            {
                mi_lock_release(&heap->arena_pages_lock);
            }

            // remove the heap from the subproc
            mi_heap_stats_merge_to_main(heap);
            mi_atomic_decrement_relaxed(ref heap->subproc->heap_count);
            __mi_stat_decrease_mt(&heap->subproc->stats.heaps, 1);
            if (acquire_heaps_lock) { mi_lock_acquire(&heap->subproc->heaps_lock); }   // C: mi_lock_maybe
            try
            {
                if (heap->next != null) { heap->next->prev = heap->prev; }
                if (heap->prev != null) { heap->prev->next = heap->next; }
                else { heap->subproc->heaps = heap->next; }
            }
            finally
            {
                if (acquire_heaps_lock) { mi_lock_release(&heap->subproc->heaps_lock); }
            }

            _mi_thread_local_free(heap->theap);
            mi_lock_done(&heap->theaps_lock);
            mi_lock_done(&heap->os_abandoned_pages_lock);
            mi_lock_done(&heap->arena_pages_lock);
            _mi_free_subproc_safe(heap);
        }

        public static void mi_heap_delete(mi_heap_t* heap)
        {
            if (heap == null) return;
            mi_heap_t* heap_main = mi_heap_get_heap_main(heap);
            if (heap == heap_main)
            {
                _mi_warning_message("cannot delete the main heap");
                return;
            }
            mi_heap_free_theaps(heap);
            _mi_heap_move_pages(heap, heap_main);
            mi_heap_free(heap, true /* acquire subproc->heaps_lock */);
        }

        public static void _mi_heap_force_destroy(mi_heap_t* heap, bool acquire_heaps_lock)
        {
            if (heap == null) return;
            mi_heap_free_theaps(heap);
            _mi_heap_destroy_pages(heap);
            if (!_mi_is_heap_main(heap)) { mi_heap_free(heap, acquire_heaps_lock); }
        }

        public static void mi_heap_destroy(mi_heap_t* heap)
        {
            if (heap == null) return;
            if (_mi_is_heap_main(heap))
            {
                _mi_warning_message("cannot destroy the main heap");
                return;
            }
            _mi_heap_force_destroy(heap, true /* acquire subproc->heaps_lock */);
        }

        public static mi_heap_t* mi_heap_of(void* p)
        {
            mi_page_t* page = _mi_safe_ptr_page(p);
            if (page == null) return null;
            return mi_page_heap(page);
        }

        public static bool mi_any_heap_contains(void* p)
        {
            mi_page_t* page = _mi_safe_ptr_page(p);
            return (page != null);
        }

        public static bool mi_heap_contains(mi_heap_t* heap, void* p)
        {
            if (heap == null) { heap = mi_heap_main(); }
            return (heap == mi_heap_of(p));
        }

        // deprecated
        public static bool mi_check_owned(void* p)
        {
            return mi_any_heap_contains(p);
        }

        // unsafe heap utilization function for DragonFly (see issue #1258)
        // If the page of pointer `p` belongs to `heap` (or `heap==NULL`) and has less than
        // `perc_threshold` used blocks in its used area return `true`.
        // This function is unsafe in general as it assumes we are the only thread accessing the page of `p`.
        public static bool mi_unsafe_heap_page_is_under_utilized(mi_heap_t* heap, void* p, nuint perc_threshold)
        {
            if (p == null) return false;
            mi_page_t* page = _mi_safe_ptr_page(p);   // Get the page containing this pointer
            if (page == null || page->used == page->capacity || page->capacity < page->reserved) return false;
            // If the page is the head of the queue, it is currently being used for
            // allocations; we skip it to avoid immediate thrashing.
            if (page->prev == null) return false;

            // match heap?
            mi_heap_t* page_heap = mi_page_heap(page);
            if (page_heap == null) return false;
            if (heap != null && page_heap != heap) return false;

            // check utilization
            if (page->capacity == 0) return false;
            if (perc_threshold >= 100) return true;
            return (perc_threshold >= ((100 * (nuint)page->used) / page->capacity));
        }
    }
}
