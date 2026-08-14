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
// Port of mimalloc v3.4.4 `src/page-queue.c` (page queues per size class).
// Original: Copyright (c) 2018-2024 Microsoft Research, Daan Leijen (MIT license).
//
// Pinned config notes: MI_MAX_ALIGN_SIZE == 16 with 8-byte words gives MI_ALIGN2W
// (sizes are rounded to double-word bins). `_mi_bin_size` and the empty statics
// live in Init.cs.

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        /* -----------------------------------------------------------
          Queue query
        ----------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_page_queue_is_huge(mi_page_queue_t* pq)
            => pq->block_size == MI_LARGE_MAX_OBJ_SIZE + (nuint)MI_INTPTR_SIZE;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_page_queue_is_full(mi_page_queue_t* pq)
            => pq->block_size == MI_LARGE_MAX_OBJ_SIZE + 2 * (nuint)MI_INTPTR_SIZE;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_page_queue_is_special(mi_page_queue_t* pq)
            => pq->block_size > MI_LARGE_MAX_OBJ_SIZE;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_page_queue_count(mi_page_queue_t* pq) => pq->count;

        // base pointer of a theap's page queue array
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_page_queue_t* mi_theap_pages(mi_theap_t* theap)
            => (mi_page_queue_t*)Unsafe.AsPointer(ref theap->pages[0]);

        /* -----------------------------------------------------------
          Bins
        ----------------------------------------------------------- */

        // Return the bin for a given field size.
        // Returns MI_BIN_HUGE if the size is too large.
        // We use `wsize` for the size in "machine word sizes",
        // i.e. byte size == `wsize*sizeof(void*)`.
        private static nuint mi_bin(nuint size)
        {
            nuint wsize = _mi_wsize_from_size(size);
            // MI_ALIGN2W
            if (wsize <= 8)
            {
                return (wsize <= 1 ? 1 : (wsize + 1) & ~(nuint)1);   // round to double word sizes
            }
            else if (wsize > MI_LARGE_MAX_OBJ_WSIZE)
            {
                return MI_BIN_HUGE;
            }
            else
            {
                wsize--;
                // find the highest bit
                nuint b = (nuint)MI_SIZE_BITS - 1 - mi_clz(wsize);   // note: wsize != 0
                // and use the top 3 bits to determine the bin (~12.5% worst internal fragmentation).
                // - adjust with 3 because we use do not round the first 8 sizes
                //   which each get an exact bin
                nuint bin = ((b << 2) + ((wsize >> (int)(b - 2)) & 0x03)) - 3;
                mi_assert_internal(bin > 0 && bin < MI_BIN_HUGE);
                return bin;
            }
        }

        /* -----------------------------------------------------------
          Queue of pages with free blocks
        ----------------------------------------------------------- */

        public static nuint _mi_bin(nuint size) => mi_bin(size);

        // Good size for allocation (public mi_good_size)
        public static nuint mi_good_size(nuint size)
        {
            if (size <= MI_LARGE_MAX_OBJ_SIZE - MI_PADDING_SIZE)
            {
                return _mi_bin_size(mi_bin(size + MI_PADDING_SIZE));
            }
            else if (size <= unchecked((nuint)MI_MAX_ALLOC_SIZE) - MI_PADDING_SIZE)
            {
                return _mi_align_up(size + MI_PADDING_SIZE, _mi_os_page_size());
            }
            else
            {
                return size;
            }
        }

#if MI_DEBUG
        private static bool mi_page_queue_contains(mi_page_queue_t* queue, mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_page_t* list = queue->first;
            while (list != null)
            {
                mi_assert_internal(list->next == null || list->next->prev == list);
                mi_assert_internal(list->prev == null || list->prev->next == list);
                if (list == page) break;
                list = list->next;
            }
            return list == page;
        }

        private static bool mi_theap_contains_queue(mi_theap_t* theap, mi_page_queue_t* pq)
        {
            mi_page_queue_t* pages = mi_theap_pages(theap);
            return pq >= &pages[0] && pq <= &pages[MI_BIN_FULL];
        }
#endif

        public static bool _mi_page_queue_is_valid(mi_theap_t* theap, mi_page_queue_t* pq)
        {
            if (pq == null) return false;
            nuint count = 0;
            mi_page_t* prev = null;
            for (mi_page_t* page = pq->first; page != null; page = page->next)
            {
                mi_assert_internal(page->prev == prev);
                if (mi_page_is_in_full(page))
                {
                    mi_assert_internal(_mi_wsize_from_size(pq->block_size) == MI_LARGE_MAX_OBJ_WSIZE + 2);
                }
                else if (mi_page_is_huge(page))
                {
                    mi_assert_internal(_mi_wsize_from_size(pq->block_size) == MI_LARGE_MAX_OBJ_WSIZE + 1);
                }
                else
                {
                    mi_assert_internal(mi_page_block_size(page) == pq->block_size);
                }
                mi_assert_internal(page->theap == theap);
                if (page->next == null)
                {
                    mi_assert_internal(pq->last == page);
                }
                count++;
                prev = page;
            }
            mi_assert_internal(pq->count == count);
            return true;
        }

        private static nuint mi_page_bin(mi_page_t* page)
        {
            nuint bin = (mi_page_is_in_full(page) ? MI_BIN_FULL : (mi_page_is_huge(page) ? MI_BIN_HUGE : mi_bin(mi_page_block_size(page))));
            mi_assert_internal(bin <= MI_BIN_FULL);
            return bin;
        }

        // returns the page bin without using MI_BIN_FULL for statistics
        public static nuint _mi_page_stats_bin(mi_page_t* page)
        {
            nuint bin = (mi_page_is_huge(page) ? MI_BIN_HUGE : mi_bin(mi_page_block_size(page)));
            mi_assert_internal(bin <= MI_BIN_HUGE);
            return bin;
        }

        private static mi_page_queue_t* mi_theap_page_queue_of(mi_theap_t* theap, mi_page_t* page)
        {
            mi_assert_internal(theap != null);
            nuint bin = mi_page_bin(page);
            mi_page_queue_t* pq = &mi_theap_pages(theap)[bin];
            mi_assert_internal((mi_page_block_size(page) == pq->block_size) ||
                               (mi_page_is_huge(page) && mi_page_queue_is_huge(pq)) ||
                               (mi_page_is_in_full(page) && mi_page_queue_is_full(pq)));
            return pq;
        }

        private static mi_page_queue_t* mi_page_queue_of(mi_page_t* page)
        {
            mi_theap_t* theap = mi_page_theap(page);
            mi_page_queue_t* pq = mi_theap_page_queue_of(theap, page);
#if MI_DEBUG
            mi_assert_expensive(mi_page_queue_contains(pq, page));
#endif
            return pq;
        }

        // The current small page array is for efficiency and for each
        // small size (up to 256) it points directly to the page for that
        // size without having to compute the bin. This means when the
        // current free page queue is updated for a small bin, we need to update a
        // range of entries in `_mi_page_small_free`.
        private static void mi_theap_queue_first_update(mi_theap_t* theap, mi_page_queue_t* pq)
        {
#if MI_DEBUG
            mi_assert_internal(mi_theap_contains_queue(theap, pq));
#endif
            nuint size = pq->block_size;
            if (size > MI_SMALL_SIZE_MAX) return;

            mi_page_t* page = pq->first;
            if (pq->first == null) page = _mi_page_empty();

            // find index in the right direct page array
            nuint idx = _mi_wsize_from_size(size);
            if ((mi_page_t*)theap->pages_free_direct[idx] == page) return;   // already set

            // find start slot
            nuint start;
            if (idx <= 1)
            {
                start = 0;
            }
            else
            {
                // find previous size; due to minimal alignment upto 3 previous bins may need to be skipped
                nuint bin = mi_bin(size);
                mi_page_queue_t* prev = pq - 1;
                while (bin == mi_bin(prev->block_size) && prev > &mi_theap_pages(theap)[0])
                {
                    prev--;
                }
                start = 1 + _mi_wsize_from_size(prev->block_size);
                if (start > idx) start = idx;
            }

            // set size range to the right page
            mi_assert(start <= idx);
            for (nuint sz = start; sz <= idx; sz++)
            {
                theap->pages_free_direct[sz] = (ulong)page;
            }
        }

        private static void mi_page_queue_remove(mi_page_queue_t* queue, mi_page_t* page)
        {
            mi_assert_internal(page != null);
#if MI_DEBUG
            mi_assert_expensive(mi_page_queue_contains(queue, page));
#endif
            mi_assert_internal(queue->count >= 1);
            mi_assert_internal(mi_page_block_size(page) == queue->block_size ||
                               (mi_page_is_huge(page) && mi_page_queue_is_huge(queue)) ||
                               (mi_page_is_in_full(page) && mi_page_queue_is_full(queue)));
            mi_theap_t* theap = mi_page_theap(page);
            if (page->prev != null) page->prev->next = page->next;
            if (page->next != null) page->next->prev = page->prev;
            if (page == queue->last) queue->last = page->prev;
            if (page == queue->first)
            {
                queue->first = page->next;
                // update first
                mi_theap_queue_first_update(theap, queue);
            }
            theap->page_count--;
            queue->count--;
            page->next = null;
            page->prev = null;
            mi_page_set_in_full(page, false);
        }

        private static void mi_page_queue_push(mi_theap_t* theap, mi_page_queue_t* queue, mi_page_t* page)
        {
            mi_assert_internal(mi_page_theap(page) == theap);
#if MI_DEBUG
            mi_assert_internal(!mi_page_queue_contains(queue, page));
#endif
            mi_assert_internal(mi_page_block_size(page) == queue->block_size ||
                               (mi_page_is_huge(page) && mi_page_queue_is_huge(queue)) ||
                               (mi_page_is_in_full(page) && mi_page_queue_is_full(queue)));

            mi_page_set_in_full(page, mi_page_queue_is_full(queue));

            page->next = queue->first;
            page->prev = null;
            if (queue->first != null)
            {
                mi_assert_internal(queue->first->prev == null);
                queue->first->prev = page;
                queue->first = page;
            }
            else
            {
                queue->first = queue->last = page;
            }
            queue->count++;

            // update direct
            mi_theap_queue_first_update(theap, queue);
            theap->page_count++;
        }

        private static void mi_page_queue_push_at_end(mi_theap_t* theap, mi_page_queue_t* queue, mi_page_t* page)
        {
            mi_assert_internal(mi_page_theap(page) == theap);
#if MI_DEBUG
            mi_assert_internal(!mi_page_queue_contains(queue, page));
#endif
            mi_assert_internal(mi_page_block_size(page) == queue->block_size ||
                               (mi_page_is_huge(page) && mi_page_queue_is_huge(queue)) ||
                               (mi_page_is_in_full(page) && mi_page_queue_is_full(queue)));

            mi_page_set_in_full(page, mi_page_queue_is_full(queue));

            page->prev = queue->last;
            page->next = null;
            if (queue->last != null)
            {
                mi_assert_internal(queue->last->next == null);
                queue->last->next = page;
                queue->last = page;
            }
            else
            {
                queue->first = queue->last = page;
            }
            queue->count++;

            // update direct
            if (queue->first == page)
            {
                mi_theap_queue_first_update(theap, queue);
            }
            theap->page_count++;
        }

        private static void mi_page_queue_move_to_front(mi_theap_t* theap, mi_page_queue_t* queue, mi_page_t* page)
        {
            mi_assert_internal(mi_page_theap(page) == theap);
#if MI_DEBUG
            mi_assert_internal(mi_page_queue_contains(queue, page));
#endif
            if (queue->first == page) return;
            mi_page_queue_remove(queue, page);
            mi_page_queue_push(theap, queue, page);
            mi_assert_internal(queue->first == page);
        }

        private static void mi_page_queue_enqueue_from_ex(mi_page_queue_t* to, mi_page_queue_t* from, bool enqueue_at_end, mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(from->count >= 1);
#if MI_DEBUG
            mi_assert_expensive(mi_page_queue_contains(from, page));
            mi_assert_expensive(!mi_page_queue_contains(to, page));
#endif
            nuint bsize = mi_page_block_size(page);
            mi_assert_internal((bsize == to->block_size && bsize == from->block_size) ||
                               (bsize == to->block_size && mi_page_queue_is_full(from)) ||
                               (bsize == from->block_size && mi_page_queue_is_full(to)) ||
                               (mi_page_is_huge(page) && mi_page_queue_is_huge(to)) ||
                               (mi_page_is_huge(page) && mi_page_queue_is_full(to)));

            mi_theap_t* theap = mi_page_theap(page);

            // delete from `from`
            if (page->prev != null) page->prev->next = page->next;
            if (page->next != null) page->next->prev = page->prev;
            if (page == from->last) from->last = page->prev;
            if (page == from->first)
            {
                from->first = page->next;
                // update first
                mi_theap_queue_first_update(theap, from);
            }
            from->count--;

            // insert into `to`
            to->count++;
            if (enqueue_at_end)
            {
                // enqueue at the end
                page->prev = to->last;
                page->next = null;
                if (to->last != null)
                {
                    mi_assert_internal(theap == mi_page_theap(to->last));
                    to->last->next = page;
                    to->last = page;
                }
                else
                {
                    to->first = page;
                    to->last = page;
                    mi_theap_queue_first_update(theap, to);
                }
            }
            else
            {
                if (to->first != null)
                {
                    // enqueue at 2nd place
                    mi_assert_internal(theap == mi_page_theap(to->first));
                    mi_page_t* next = to->first->next;
                    page->prev = to->first;
                    page->next = next;
                    to->first->next = page;
                    if (next != null)
                    {
                        next->prev = page;
                    }
                    else
                    {
                        to->last = page;
                    }
                }
                else
                {
                    // enqueue at the head (singleton list)
                    page->prev = null;
                    page->next = null;
                    to->first = page;
                    to->last = page;
                    mi_theap_queue_first_update(theap, to);
                }
            }

            mi_page_set_in_full(page, mi_page_queue_is_full(to));
        }

        private static void mi_page_queue_enqueue_from(mi_page_queue_t* to, mi_page_queue_t* from, mi_page_t* page)
        {
            mi_page_queue_enqueue_from_ex(to, from, true /* enqueue at the end */, page);
        }

        private static void mi_page_queue_enqueue_from_full(mi_page_queue_t* to, mi_page_queue_t* from, mi_page_t* page)
        {
            // note: we could insert at the front to increase reuse, but it slows down certain benchmarks (like `alloc-test`)
            mi_page_queue_enqueue_from_ex(to, from, true /* enqueue at the end of the `to` queue? */, page);
        }
    }
}
