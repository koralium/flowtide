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
// Port of the PAGE half of mimalloc v3.4.4 `src/arena.c` (lines ~605-1310):
// arena page allocation (fresh/abandoned), free, abandon/unabandon and the
// ownership protocol around the `pages_abandoned` bitmaps.
// Original: Copyright (c) 2019-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Port notes:
//  - `_mi_page_associated_theap_peek` is implemented in Heap.cs (with the other
//    prim-tls.h inlines).
//  - The heap-wide page/block visiting and heap page move/destroy (C arena.c
//    lines ~2300-2506) live at the bottom of this file.

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        /* -----------------------------------------------------------
          Arena page allocation
        ----------------------------------------------------------- */

        // release ownership of a page. This may free the page if all blocks were concurrently
        // freed in the meantime. Returns true if the page was freed.
        private static bool mi_abandoned_page_unown(mi_page_t* page, mi_theap_t* current_theap)
        {
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(_mi_thread_id() == current_theap->tld->thread_id);
            nuint tf_new;
            nuint tf_old = mi_atomic_load_relaxed(ref page->xthread_free);
            do
            {
                mi_assert_internal(mi_tf_is_owned(tf_old));
                while (mi_tf_block(tf_old) != null)
                {
                    _mi_page_free_collect(page, false);   // update used
                    if (mi_page_all_free(page))           // it may become free just before unowning it
                    {
                        _mi_arenas_page_unabandon(page, current_theap);
                        _mi_arenas_page_free(page, current_theap);
                        return true;
                    }
                    tf_old = mi_atomic_load_relaxed(ref page->xthread_free);
                }
                mi_assert_internal(mi_tf_block(tf_old) == null);
                tf_new = mi_tf_create(null, false);
            } while (!mi_atomic_cas_weak_acq_rel(ref page->xthread_free, ref tf_old, tf_new));
            return false;
        }

        private static bool mi_arena_try_claim_abandoned(nuint slice_index, mi_arena_t* arena, bool* keep_abandoned)
        {
            // found an abandoned page of the right size
            mi_page_t* page = mi_arena_page_at_slice(arena, slice_index);
            // can we claim ownership?
            if (!mi_page_claim_ownership(page))
            {
                // there was a concurrent free that reclaims this page ..
                // we need to keep it in the abandoned map as the free will call `mi_arena_page_unabandon`,
                // and wait for readers (us!) to finish. This is why it is very important to set the abandoned
                // bit again (or otherwise the unabandon will never stop waiting).
                *keep_abandoned = true;
                return false;
            }
            else
            {
                // yes, we can reclaim it, keep the abandoned map entry clear
                *keep_abandoned = false;
                return true;
            }
        }

        // allocate initial arena_pages from the main heap (used for non-main heaps;
        // the main heap uses the pre-carved `arena->pages_main`)
        private static mi_arena_pages_t* mi_arena_pages_alloc(mi_arena_t* arena)
        {
            nuint slice_count = arena->slice_count;
            nuint bitmap_base = 0;
            nuint size = mi_arena_pages_size(slice_count, &bitmap_base);
            mi_arena_pages_t* arena_pages = (mi_arena_pages_t*)mi_heap_zalloc_aligned(_mi_subproc_heap_main(arena->subproc), size, MI_BCHUNK_SIZE);
            if (arena_pages == null) return null;
            byte* @base = (byte*)arena_pages + bitmap_base;
            mi_assert_internal(_mi_is_aligned(@base, MI_BCHUNK_SIZE));
            arena_pages->pages = mi_arena_bitmap_init(slice_count, &@base);
            for (int i = 0; i < MI_ARENA_BIN_COUNT; i++)
            {
                arena_pages->pages_abandoned[i] = (ulong)mi_arena_bitmap_init(slice_count, &@base);
            }
            return arena_pages;
        }

        private static mi_arena_pages_t* mi_heap_arena_pages(mi_heap_t* heap, mi_arena_t* arena)
        {
            mi_assert_internal(arena != null);
            mi_assert_internal(heap != null);
            mi_assert(arena->arena_idx < MI_MAX_ARENAS);
            return (mi_arena_pages_t*)mi_atomic_load_acquire(ref *(nuint*)&heap->arena_pages[(int)arena->arena_idx]);
        }

        private static mi_arena_t* mi_page_arena_pages(mi_page_t* page, nuint* slice_index, nuint* slice_count, mi_arena_pages_t** parena_pages)
        {
            mi_assert_internal(mi_page_is_owned(page));
            mi_arena_t* arena = mi_arena_from_memid(page->memid, slice_index, slice_count);
            mi_assert_internal(arena != null);
            if (parena_pages != null)
            {
                mi_heap_t* heap = mi_page_heap(page);
                mi_arena_pages_t* arena_pages = mi_heap_arena_pages(heap, arena);
                mi_assert_internal(arena_pages != null);
                mi_assert_internal(slice_index == null || mi_bitmap_is_set(arena_pages->pages, *slice_index));
                *parena_pages = arena_pages;
            }
            return arena;
        }

        private static mi_arena_pages_t* mi_heap_ensure_arena_pages(mi_heap_t* heap, mi_arena_t* arena)
        {
            mi_assert_internal(arena != null);
            mi_assert_internal(heap != null);
            mi_assert(arena->arena_idx < MI_MAX_ARENAS);
            mi_arena_pages_t* arena_pages = mi_heap_arena_pages(heap, arena);
            if (arena_pages == null)
            {
                mi_lock_acquire(&heap->arena_pages_lock);
                try
                {
                    arena_pages = (mi_arena_pages_t*)mi_atomic_load_acquire(ref *(nuint*)&heap->arena_pages[(int)arena->arena_idx]);
                    if (arena_pages == null)   // still NULL?
                    {
                        if (_mi_is_heap_main(heap))
                        {
                            // the page info for the main heap is always allocated as part of an arena
                            arena_pages = &arena->pages_main;
                        }
                        else
                        {
                            // always allocate the arena pages info from the main heap
                            arena_pages = mi_arena_pages_alloc(arena);
                        }
                        mi_atomic_store_release(ref *(nuint*)&heap->arena_pages[(int)arena->arena_idx], (nuint)arena_pages);
                    }
                }
                finally
                {
                    mi_lock_release(&heap->arena_pages_lock);
                }
            }
            if (_mi_is_heap_main(heap)) { mi_assert(arena_pages != null); }   // can never fail
            return arena_pages;
        }

        private static mi_page_t* mi_arenas_page_try_find_abandoned(mi_theap_t* theap, nuint slice_count, nuint block_size)
        {
            mi_heap_t* heap = _mi_theap_heap(theap);
            nuint tseq = theap->tld->thread_seq;
            mi_arena_t* req_arena = heap->exclusive_arena;

            nuint bin = _mi_bin(block_size);
            if (bin >= MI_ARENA_BIN_COUNT)
            {
                return null;   // singleton page size
            }

            // any abandoned in our size class?
            mi_assert_internal(heap != null);
            if (mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]) == 0)
            {
                return null;
            }

            // search arena's
            const bool allow_large = true;
            const int any_numa = -1;
            const bool match_numa = true;
            var iter = new mi_forall_arenas_iter(heap, req_arena, tseq);
            while (iter.TryNextSuitable(match_numa, any_numa, allow_large, out mi_arena_t* arena))
            {
                mi_arena_pages_t* arena_pages = mi_heap_arena_pages(heap, arena);
                if (arena_pages != null)
                {
                    nuint slice_index;
                    mi_bitmap_t* bitmap = (mi_bitmap_t*)arena_pages->pages_abandoned[(int)bin];

                    if (mi_bitmap_try_find_and_claim(bitmap, tseq, &slice_index, &mi_arena_try_claim_abandoned, arena))
                    {
                        // found an abandoned page of the right size
                        // and claimed ownership.
                        mi_page_t* page = mi_arena_page_at_slice(arena, slice_index);
                        mi_assert_internal(mi_page_is_owned(page));
                        mi_assert_internal(mi_page_is_abandoned(page));
                        mi_atomic_decrement_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]);
                        __mi_stat_decrease(&theap->stats.pages_abandoned, 1);
                        __mi_stat_counter_increase(&theap->stats.pages_reclaim_on_alloc, 1);

                        _mi_page_free_collect(page, false);   // update `used` count
                        mi_assert_internal(mi_bbitmap_is_clearN(arena->slices_free, slice_index, slice_count));
                        mi_assert_internal(page->slice_committed > 0 || mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count));
                        mi_assert_internal(mi_bitmap_is_setN(arena->slices_dirty, slice_index, slice_count));
                        mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
                        mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
                        mi_assert_internal(mi_page_block_size(page) == block_size);
                        mi_assert_internal(!mi_page_is_full(page));
                        return page;
                    }
                }
            }
            return null;
        }

        private static byte* mi_arenas_page_alloc_fresh_area(mi_theap_t* theap, nuint slice_count, nuint block_size, nuint block_alignment, bool os_align, bool commit, mi_memid_t* memid, mi_arena_pages_t** parena_pages)
        {
            mi_assert_internal(parena_pages != null);

            *parena_pages = null;
            const bool allow_large = true;   // MI_SECURE < 5
            nuint page_alignment = MI_ARENA_SLICE_ALIGN;

            mi_heap_t* heap = _mi_theap_heap(theap);
            mi_tld_t* tld = theap->tld;
            mi_arena_t* req_arena = heap->exclusive_arena;
            int numa_node = (heap->numa_node >= 0 ? heap->numa_node : tld->numa_node);

            // try to allocate from free space in arena's
            byte* start = null;
            *memid = _mi_memid_none();
            nuint alloc_size = mi_size_of_slices(slice_count);
            if (!mi_option_is_enabled(mi_option_t.mi_option_disallow_arena_alloc) &&           // allowed to allocate from arena's?
                !os_align &&                                                                   // not large alignment
                slice_count <= mi_arena_max_object_size() / MI_ARENA_SLICE_SIZE)               // and not too large
            {
                start = (byte*)mi_arenas_try_alloc(heap, slice_count, page_alignment, commit, allow_large, req_arena, tld->thread_seq, numa_node, memid);
                if (start != null)
                {
                    mi_arena_pages_t* arena_pages = mi_heap_ensure_arena_pages(heap, memid->mem.arena.arena);
                    *parena_pages = arena_pages;
                    if (arena_pages == null)
                    {
                        _mi_arenas_free(heap->subproc, start, mi_size_of_slices(slice_count), *memid);   // roll back
                        start = null;
                    }
                    else
                    {
                        mi_assert_internal(mi_bitmap_is_clear(arena_pages->pages, memid->mem.arena.slice_index));
                        // don't set yet: mi_bitmap_set(arena_pages->pages, memid->mem.arena.slice_index);
                    }
                }
            }

            // otherwise fall back to the OS
            if (start == null)
            {
                if (os_align)
                {
                    // note: slice_count already includes the page
                    mi_assert_internal(slice_count >= mi_slice_count_of_size(block_size) + mi_slice_count_of_size(page_alignment));
                    start = (byte*)mi_arena_os_alloc_aligned(heap->subproc, alloc_size, block_alignment, page_alignment /* align offset */, commit, allow_large, req_arena, memid);
                }
                else
                {
                    start = (byte*)mi_arena_os_alloc_aligned(heap->subproc, alloc_size, page_alignment, 0 /* align offset */, commit, allow_large, req_arena, memid);
                }
            }

            if (start == null) return null;
            mi_assert_internal(_mi_is_aligned(start, MI_PAGE_ALIGN));
            mi_assert_internal(!os_align || _mi_is_aligned(start + page_alignment, block_alignment));
            return start;
        }

        private static nuint mi_page_block_start(nuint block_size, bool os_align)
        {
            nuint offset;
            if (os_align)
            {
                offset = MI_PAGE_ALIGN;
            }
            else if (_mi_is_power_of_two(block_size) && block_size <= MI_PAGE_MAX_START_BLOCK_ALIGN2)
            {
                // naturally align power-of-2 blocks up to MI_PAGE_MAX_START_BLOCK_ALIGN2 size (4KiB)
                offset = _mi_align_up(mi_page_info_size(), block_size);
            }
            else if (block_size != 0 && (block_size % MI_PAGE_OSPAGE_BLOCK_ALIGN2) == 0)
            {
                // also align large pages that are a multiple of MI_PAGE_OSPAGE_BLOCK_ALIGN2 (4KiB)
                offset = _mi_align_up(mi_page_info_size(), MI_PAGE_OSPAGE_BLOCK_ALIGN2);
            }
            else
            {
                // otherwise start after the info
                offset = mi_page_info_size();
            }
            return _mi_align_up(offset, MI_MAX_ALIGN_SIZE);
        }

        private const nuint MI_PAGE_BLOCK_START_MAX_OFFSET = 8 * (nuint)MI_INTPTR_BITS;   // 512

        // Allocate a fresh page
        private static mi_page_t* mi_arenas_page_alloc_fresh(mi_theap_t* theap, nuint slice_count, nuint block_size, nuint block_alignment, bool commit)
        {
            bool os_align = block_alignment > MI_PAGE_MAX_OVERALLOC_ALIGN;
            nuint alloc_size = mi_size_of_slices(slice_count);
            mi_memid_t memid = _mi_memid_none();
            mi_arena_pages_t* arena_pages = null;
            byte* slice_start = mi_arenas_page_alloc_fresh_area(theap, slice_count, block_size, block_alignment, os_align, commit, &memid, &arena_pages);
            if (slice_start == null) return null;

            // (MI_SECURE < 5: no guard page at the end)
            nuint page_noguard_size = alloc_size;

            // allocate the page meta info
            mi_page_t* page = null;
            bool page_meta_is_separate = false;
            nuint block_start = 0;

            // allocate page meta info at the arena start?
            if (memid.memkind == mi_memkind_t.MI_MEM_ARENA)
            {
                mi_arena_t* arena = memid.mem.arena.arena;
                if (arena->pages_meta != null)
                {
                    // MI_PAGE_META_IS_SEPARATED == 1
                    mi_page_t* page_meta = &arena->pages_meta[memid.mem.arena.slice_index];
                    mi_assert_internal(page_meta->block_size == 0);
                    {
                        page = page_meta;
                        page_meta_is_separate = true;
                        block_start = 0;
                        if (block_size >= MI_INTPTR_SIZE && block_size <= MI_PAGE_BLOCK_START_MAX_OFFSET && _mi_is_power_of_two(block_size))
                        {
                            block_start += block_size;
                        }
                        mi_assert_internal(page->block_size == 0);
                        _mi_memzero_aligned(page, (nuint)sizeof(mi_page_t));
                    }
                }
            }
            if (page == null)
            {
                // put page meta info in front of the slice
                page = (mi_page_t*)slice_start;
                block_start = mi_page_block_start(block_size, os_align);
            }
            // note: C asserts `block_start % MI_MAX_ALIGN_SIZE == 0` -- with separated page
            // meta an 8-byte block size gives block_start == 8, but C debug builds never see
            // that (MI_PADDING>=1 forces block sizes >= 16 there); the port pins MI_PADDING=0
            // so the reachable invariant is alignment to min(block_size, MI_MAX_ALIGN_SIZE).
            mi_assert_internal(block_start % MI_MAX_ALIGN_SIZE == 0
                || (page_meta_is_separate && block_size < MI_MAX_ALIGN_SIZE && (block_start % block_size) == 0));

            // commit first block?
            nuint commit_size = 0;
            if (!memid.initially_committed)
            {
                commit_size = _mi_align_up(block_start + block_size, MI_PAGE_MIN_COMMIT_SIZE);
                if (commit_size > page_noguard_size) { commit_size = page_noguard_size; }
                bool is_zero = false;
                if (!mi_arena_commit(_mi_theap_subproc(theap), mi_memid_arena(memid), slice_start, commit_size, &is_zero, 0))
                {
                    _mi_arenas_free(_mi_theap_subproc(theap), slice_start, alloc_size, memid);
                    return null;
                }
            }
            // now we can finish initialization and use `mi_arenas_page_free_prim` on error

            // zero initialize the page meta data
            if (!memid.initially_zero && !page_meta_is_separate)
            {
                _mi_memzero_aligned(page, (nuint)sizeof(mi_page_t));
            }

#if MI_DEBUG
            if (memid.initially_zero && memid.initially_committed)
            {
                if (!mi_mem_is_zero(slice_start, page_noguard_size))
                {
                    _mi_error_message(EFAULT, "internal error: page memory was not zero initialized.");
                    memid.initially_zero = false;
                    if (block_start > 0) { _mi_memzero_aligned(page, (nuint)sizeof(mi_page_t)); }
                }
            }
#endif
            nuint reserved = (os_align ? 1 : (page_noguard_size - block_start) / block_size);
            mi_assert_internal(reserved > 0 && reserved <= ushort.MaxValue);

            // initialize the page start
            byte* start = slice_start + block_start;
            mi_assert_internal(start > (byte*)page);
            nuint offset = (nuint)(start - (byte*)page);
            // note: C asserts `offset % MI_MAX_ALIGN_SIZE == 0`; for block sizes < 16 with
            // separated page meta the offset can be ~8 mod 16 (see the block_start note above)
            // and the truncating division below matches the C release behavior exactly: the
            // effective page start snaps down within the slice, and ALL later pointer math
            // derives consistently from `mi_page_start` so the layout stays self-consistent.
            mi_assert_internal(((offset % MI_MAX_ALIGN_SIZE) == 0 || block_size < MI_MAX_ALIGN_SIZE) && (offset / MI_MAX_ALIGN_SIZE) <= uint.MaxValue);
            page->page_ma_offset = (uint)(offset / MI_MAX_ALIGN_SIZE);

            // initialize page meta-data
            page->reserved = (ushort)reserved;
            page->block_size = block_size;
            page->memid = memid;
            page->free_is_zero = memid.initially_zero;

            mi_assert_internal((commit && commit_size == 0) || (!commit && commit_size < uint.MaxValue));
            page->slice_committed = (uint)commit_size;

            page->heap = _mi_theap_heap(theap);
            mi_page_set_theap(page, theap);

            mi_assert_internal(page->free == null);
            mi_assert_internal(page_meta_is_separate == mi_page_meta_is_separated(page));
            mi_assert_internal(mi_page_slice_start(page) == slice_start);
            mi_assert_internal(mi_page_size(page) <= page_noguard_size);

            // now register in the arena_pages
            if (arena_pages != null)
            {
                mi_assert_internal(memid.memkind == mi_memkind_t.MI_MEM_ARENA);
                mi_bitmap_set(arena_pages->pages, memid.mem.arena.slice_index);
            }

            // and own it
            mi_page_claim_ownership(page);

            // register in the page map
            if (!_mi_page_map_register(page))
            {
                mi_arenas_page_free_prim(page);
                return null;
            }

            // stats
            __mi_stat_increase(&theap->stats.pages, 1);
            __mi_stat_increase((mi_stat_count_t*)Unsafe.AsPointer(ref theap->stats.page_bins[(int)_mi_page_stats_bin(page)]), 1);

            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_block_size(page) == block_size);
            mi_assert_internal(mi_page_is_owned(page));

            return page;
        }

        // Allocate a regular small/medium/large page.
        private static mi_page_t* mi_arenas_page_regular_alloc(mi_theap_t* theap, nuint slice_count, nuint block_size)
        {
            // 1. look for an abandoned page
            mi_page_t* page = mi_arenas_page_try_find_abandoned(theap, slice_count, block_size);
            if (page != null)
            {
                return page;   // return as abandoned
            }

            // 2. find a free block, potentially allocating a new arena
            long commit_on_demand = mi_option_get(mi_option_t.mi_option_page_commit_on_demand);
            bool commit = (slice_count <= mi_slice_count_of_size(MI_PAGE_MIN_COMMIT_SIZE) ||   // always commit small pages
                           (slice_count >= mi_slice_count_of_size(uint.MaxValue)) ||           // always commit pages too large to hold a 32-bit slice_committed
                           (commit_on_demand == 2 && _mi_os_has_overcommit()) || (commit_on_demand == 0));
            page = mi_arenas_page_alloc_fresh(theap, slice_count, block_size, 1, commit);
            if (page == null) return null;

            mi_assert_internal(page->memid.memkind != mi_memkind_t.MI_MEM_ARENA || page->memid.mem.arena.slice_count == slice_count);
            if (!_mi_page_init(theap, page))
            {
                _mi_arenas_page_free(page, theap);
                return null;
            }

            return page;
        }

        // Allocate a page containing one block (very large, or with large alignment)
        private static mi_page_t* mi_arenas_page_singleton_alloc(mi_theap_t* theap, nuint block_size, nuint block_alignment)
        {
            bool os_align = block_alignment > MI_PAGE_MAX_OVERALLOC_ALIGN;
            nuint info_size = (os_align ? MI_PAGE_ALIGN : mi_page_info_size());
            // MI_SECURE < 2
            nuint slice_count = mi_slice_count_of_size(info_size + block_size);

            mi_page_t* page = mi_arenas_page_alloc_fresh(theap, slice_count, block_size, block_alignment, true /* commit singletons always */);
            if (page == null) return null;

            mi_assert(page->reserved == 1);
            if (!_mi_page_init(theap, page))
            {
                _mi_arenas_page_free(page, theap);
                return null;
            }

            return page;
        }

        public static mi_page_t* _mi_arenas_page_alloc(mi_theap_t* theap, nuint block_size, nuint block_alignment)
        {
            mi_page_t* page;
            // semi static assert: ensure that all non-singleton block size bins are covered.
            mi_assert(_mi_bin(MI_LARGE_MAX_OBJ_SIZE) < MI_ARENA_BIN_COUNT);
            if (block_alignment > MI_PAGE_MAX_OVERALLOC_ALIGN)
            {
                mi_assert_internal(_mi_is_power_of_two(block_alignment));
                page = mi_arenas_page_singleton_alloc(theap, block_size, block_alignment);
            }
            else if (block_size <= MI_SMALL_MAX_OBJ_SIZE)
            {
                page = mi_arenas_page_regular_alloc(theap, mi_slice_count_of_size(MI_SMALL_PAGE_SIZE), block_size);
            }
            else if (block_size <= MI_MEDIUM_MAX_OBJ_SIZE)
            {
                page = mi_arenas_page_regular_alloc(theap, mi_slice_count_of_size(MI_MEDIUM_PAGE_SIZE), block_size);
            }
            // MI_ENABLE_LARGE_PAGES == 1
            else if (block_size <= MI_LARGE_MAX_OBJ_SIZE)
            {
                page = mi_arenas_page_regular_alloc(theap, mi_slice_count_of_size(MI_LARGE_PAGE_SIZE), block_size);
            }
            else
            {
                page = mi_arenas_page_singleton_alloc(theap, block_size, block_alignment);
            }
            if (page == null)
            {
                return null;
            }
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(block_alignment <= MI_PAGE_MAX_OVERALLOC_ALIGN || _mi_is_aligned(mi_page_start(page), block_alignment));

            return page;
        }

        // Free a page without modifying page_bin stats
        private static void mi_arenas_page_free_prim(mi_page_t* page)
        {
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_all_free(page));
            mi_assert_internal(page->next == null && page->prev == null);

            // unregister page
            _mi_page_map_unregister(page);

            // (MI_SECURE < 5: no guard page to recommit)

            // and free
            if (page->memid.memkind == mi_memkind_t.MI_MEM_ARENA)
            {
                mi_arena_pages_t* arena_pages;
                nuint slice_index;
                nuint slice_count;
                mi_arena_t* arena = mi_page_arena_pages(page, &slice_index, &slice_count, &arena_pages);
                mi_assert_internal(arena_pages != null);
                mi_assert_internal(arena->subproc == mi_page_subproc(page));
                mi_bitmap_clear(arena_pages->pages, slice_index);
                if (page->slice_committed > 0)
                {
                    // if committed on-demand, set the commit bits to account commit properly
                    mi_assert_internal(mi_page_full_size(page) >= page->slice_committed);
                    nuint total_slices = page->slice_committed / MI_ARENA_SLICE_SIZE;   // conservative
                    mi_assert_internal(slice_count >= total_slices);
                    if (total_slices > 0)
                    {
                        mi_bitmap_setN(arena->slices_committed, slice_index, total_slices, null);
                    }
                    // any left over?
                    nuint extra = page->slice_committed % MI_ARENA_SLICE_SIZE;
                    if (extra > 0)
                    {
                        // pretend it was decommitted already
                        __mi_stat_decrease_mt(&arena->subproc->stats.committed, extra);
                    }
                }
                else
                {
                    mi_assert_internal(mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count));
                }
            }
            if (mi_page_meta_is_separated(page)) { page->block_size = 0; }   // for assertion checking
            _mi_arenas_free(mi_page_subproc(page), mi_page_slice_start(page), mi_page_full_size(page), page->memid);
        }

        public static void _mi_arenas_page_free(mi_page_t* page, mi_theap_t* current_theapx)
        {
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_all_free(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(page->next == null && page->prev == null);
            mi_assert_internal(current_theapx == null || _mi_thread_id() == current_theapx->tld->thread_id);

            if (current_theapx != null)
            {
                __mi_stat_decrease((mi_stat_count_t*)Unsafe.AsPointer(ref current_theapx->stats.page_bins[(int)_mi_page_stats_bin(page)]), 1);
                __mi_stat_decrease(&current_theapx->stats.pages, 1);
            }
            else
            {
                mi_heap_t* heap = mi_page_heap(page);
                __mi_stat_decrease_mt((mi_stat_count_t*)Unsafe.AsPointer(ref heap->stats.page_bins[(int)_mi_page_stats_bin(page)]), 1);
                __mi_stat_decrease_mt(&heap->stats.pages, 1);
            }
            mi_arenas_page_free_prim(page);
        }

        /* -----------------------------------------------------------
          Arena abandon
        ----------------------------------------------------------- */

        public static void _mi_arenas_page_abandon(mi_page_t* page, mi_theap_t* current_theap)
        {
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(!mi_page_all_free(page));
            mi_assert_internal(page->next == null && page->prev == null);
            mi_assert_internal(_mi_thread_id() == current_theap->tld->thread_id);

            // add to abandoned?
            mi_heap_t* heap = mi_page_heap(page);
            mi_assert_internal(heap == _mi_theap_heap(current_theap));
            if (page->memid.memkind == mi_memkind_t.MI_MEM_ARENA && !mi_page_is_full(page))
            {
                // make available for allocations
                nuint bin = _mi_bin(mi_page_block_size(page));
                mi_assert_internal(bin < MI_ARENA_BIN_COUNT);
                if (bin < MI_ARENA_BIN_COUNT)   // paranoia
                {
                    nuint slice_index;
                    nuint slice_count;
                    mi_arena_pages_t* arena_pages = null;
                    mi_arena_t* arena = mi_page_arena_pages(page, &slice_index, &slice_count, &arena_pages);

                    mi_assert_internal(!mi_page_is_singleton(page));
                    mi_assert_internal(mi_bbitmap_is_clearN(arena->slices_free, slice_index, slice_count));
                    mi_assert_internal(page->slice_committed > 0 || mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count));
                    mi_assert_internal(mi_bitmap_is_setN(arena->slices_dirty, slice_index, slice_count));

                    mi_page_set_abandoned_mapped(page);
                    bool was_clear = mi_bitmap_set((mi_bitmap_t*)arena_pages->pages_abandoned[(int)bin], slice_index);
                    mi_assert_internal(was_clear);
                    mi_atomic_increment_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]);
                    __mi_stat_increase(&current_theap->stats.pages_abandoned, 1);
                    mi_abandoned_page_unown(page, current_theap);
                    return;
                }
            }
            // otherwise,
            // page is full (or a singleton), or the page is OS/externally allocated
            // leave as is; it will be reclaimed when an object is free'd in the page
            // but for non-arena pages, add to the subproc list so these can be visited
            if (page->memid.memkind != mi_memkind_t.MI_MEM_ARENA)
            {
                mi_lock_acquire(&heap->os_abandoned_pages_lock);
                try
                {
                    // push in front
                    page->prev = null;
                    page->next = heap->os_abandoned_pages;
                    if (page->next != null) { page->next->prev = page; }
                    heap->os_abandoned_pages = page;
                }
                finally
                {
                    mi_lock_release(&heap->os_abandoned_pages_lock);
                }
            }
            __mi_stat_increase(&current_theap->stats.pages_abandoned, 1);
            mi_abandoned_page_unown(page, current_theap);
        }

        // (`_mi_page_associated_theap_peek` lives in Heap.cs with the other prim-tls inlines)

        // this is called from `free.c:mi_free_try_collect_mt` only.
        public static bool _mi_arenas_page_try_reabandon_to_mapped(mi_page_t* page)
        {
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(!mi_page_is_abandoned_mapped(page));
            mi_assert_internal(!mi_page_is_full(page));
            mi_assert_internal(!mi_page_all_free(page));
            mi_assert_internal(!mi_page_is_singleton(page));
            if (mi_page_is_full(page) || mi_page_is_abandoned_mapped(page) || page->memid.memkind != mi_memkind_t.MI_MEM_ARENA)
            {
                return false;
            }
            else
            {
                // do not use _mi_heap_theap as we may call this during shutdown of threads and don't want to reinitialize the theap
                mi_theap_t* theap = _mi_page_associated_theap_peek(page);
                if (theap == null)
                {
                    return false;
                }
                else
                {
                    __mi_stat_counter_increase(&theap->stats.pages_reabandon_full, 1);
                    __mi_stat_adjust_decrease(&theap->stats.pages_abandoned, 1);   // adjust as we are not abandoning fresh
                    _mi_arenas_page_abandon(page, theap);
                    return true;
                }
            }
        }

        // called from `mi_free` if trying to unabandon an abandoned page
        public static void _mi_arenas_page_unabandon(mi_page_t* page, mi_theap_t* current_theapx)
        {
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            mi_assert_internal(mi_page_is_owned(page));
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(current_theapx == null || _mi_thread_id() == current_theapx->tld->thread_id);

            mi_heap_t* heap = mi_page_heap(page);
            if (mi_page_is_abandoned_mapped(page))
            {
                mi_assert_internal(page->memid.memkind == mi_memkind_t.MI_MEM_ARENA);
                // remove from the abandoned map
                nuint bin = _mi_bin(mi_page_block_size(page));
                mi_assert_internal(bin < MI_ARENA_BIN_COUNT);
                nuint slice_index;
                nuint slice_count;
                mi_arena_pages_t* arena_pages;
                mi_arena_t* arena = mi_page_arena_pages(page, &slice_index, &slice_count, &arena_pages);

                mi_assert_internal(mi_bbitmap_is_clearN(arena->slices_free, slice_index, slice_count));
                mi_assert_internal(page->slice_committed > 0 || mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count));

                // this busy waits until a concurrent reader (from alloc_abandoned) is done
                mi_bitmap_clear_once_set(arena->subproc, (mi_bitmap_t*)arena_pages->pages_abandoned[(int)bin], slice_index);
                mi_page_clear_abandoned_mapped(page);
                mi_atomic_decrement_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]);
            }
            else
            {
                // page is full (or a singleton), page is OS allocated
                // if not an arena page, remove from the subproc os pages list
                if (page->memid.memkind != mi_memkind_t.MI_MEM_ARENA)
                {
                    mi_lock_acquire(&heap->os_abandoned_pages_lock);
                    try
                    {
                        if (page->prev != null) { page->prev->next = page->next; }
                        if (page->next != null) { page->next->prev = page->prev; }
                        if (heap->os_abandoned_pages == page) { heap->os_abandoned_pages = page->next; }
                        page->next = null;
                        page->prev = null;
                    }
                    finally
                    {
                        mi_lock_release(&heap->os_abandoned_pages_lock);
                    }
                }
            }
            if (current_theapx != null)
            {
                __mi_stat_decrease(&current_theapx->stats.pages_abandoned, 1);
            }
            else
            {
                __mi_stat_decrease_mt(&heap->stats.pages_abandoned, 1);
            }
        }

        /* -----------------------------------------------------------
          Visit all pages and blocks in a heap (C: arena.c ~2300)
        ----------------------------------------------------------- */

        private struct mi_heap_visit_info_t
        {
            public mi_heap_t* heap;
            public delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor;
            public void* arg;
            public bool visit_blocks;
        }

        private static bool mi_heap_visit_page(mi_page_t* page, mi_heap_visit_info_t* vinfo)
        {
            mi_heap_area_t area;
            _mi_heap_area_init(&area, page);
            mi_assert_internal(vinfo->heap == mi_page_heap(page));
            if (!vinfo->visitor(vinfo->heap, &area, null, area.block_size, vinfo->arg))
            {
                return false;
            }
            if (vinfo->visit_blocks)
            {
                return _mi_theap_area_visit_blocks(&area, page, vinfo->visitor, vinfo->arg);
            }
            else
            {
                return true;
            }
        }

        private static bool mi_heap_visit_page_at(nuint slice_index, nuint slice_count, mi_arena_t* arena, void* arg)
        {
            mi_heap_visit_info_t* vinfo = (mi_heap_visit_info_t*)arg;
            mi_page_t* page = mi_arena_page_at_slice(arena, slice_index);
            return mi_heap_visit_page(page, vinfo);
        }

        public static bool _mi_heap_visit_blocks(mi_heap_t* heap, bool abandoned_only, bool visit_blocks, delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor, void* arg)
        {
            mi_assert(visitor != null);
            if (visitor == null) return false;
            if (heap == null) { heap = mi_heap_main(); }
            // visit all pages in a heap
            // we don't have to claim because we assume we are the only thread running (with this heap).
            // (but we could atomically claim as well by first doing abandoned_reclaim and afterwards reabandoning).
            mi_heap_visit_info_t visit_info;
            visit_info.heap = heap;
            visit_info.visitor = visitor;
            visit_info.arg = arg;
            visit_info.visit_blocks = visit_blocks;
            bool ok = true;
            var iter = new mi_forall_arenas_iter(heap, null, 0);
            while (iter.TryNext(out mi_arena_t* arena))
            {
                mi_arena_pages_t* arena_pages = mi_heap_arena_pages(heap, arena);
                if (ok && arena_pages != null)
                {
                    if (abandoned_only)
                    {
                        for (nuint bin = 0; ok && bin < MI_ARENA_BIN_COUNT; bin++)
                        {
                            // todo: if we had a single abandoned page map as well, this can be faster.
                            if (mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]) > 0)
                            {
                                ok = _mi_bitmap_forall_set((mi_bitmap_t*)arena_pages->pages_abandoned[(int)bin], &mi_heap_visit_page_at, arena, &visit_info);
                            }
                        }
                    }
                    else
                    {
                        ok = _mi_bitmap_forall_set(arena_pages->pages, &mi_heap_visit_page_at, arena, &visit_info);
                    }
                }
            }
            if (!ok) return false;

            // visit abandoned pages in OS allocated memory
            // (technically we don't need the initial lock as we assume we are the only thread running in this subproc)
            mi_page_t* page = null;
            mi_lock_acquire(&heap->os_abandoned_pages_lock);
            try
            {
                page = heap->os_abandoned_pages;
            }
            finally
            {
                mi_lock_release(&heap->os_abandoned_pages_lock);
            }
            while (ok && page != null)
            {
                mi_page_t* next = page->next;   // read upfront in case the visitor frees the page
                ok = mi_heap_visit_page(page, &visit_info);
                page = next;
            }

            return ok;
        }

        public static bool mi_heap_visit_blocks(mi_heap_t* heap, bool visit_blocks, delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor, void* arg)
        {
            return _mi_heap_visit_blocks(heap, false, visit_blocks, visitor, arg);
        }

        public static bool mi_heap_visit_abandoned_blocks(mi_heap_t* heap, bool visit_blocks, delegate*<mi_heap_t*, mi_heap_area_t*, void*, nuint, void*, bool> visitor, void* arg)
        {
            return _mi_heap_visit_blocks(heap, true, visit_blocks, visitor, arg);
        }

        /* -----------------------------------------------------------
          Move / destroy all pages of a heap (C: arena.c ~2386)
        ----------------------------------------------------------- */

        private struct mi_heap_delete_visit_info_t
        {
            public mi_heap_t* heap_target;
            public mi_theap_t* theap_target;
            public mi_theap_t* theap;
        }

        private static bool mi_heap_delete_page(mi_heap_t* heap, mi_heap_area_t* area, void* block, nuint block_size, void* arg)
        {
            mi_heap_delete_visit_info_t* info = (mi_heap_delete_visit_info_t*)arg;
            mi_heap_t* heap_target = info->heap_target;
            mi_theap_t* theap = null;   // C: info->theap is unused (NULL)
            mi_page_t* page = (mi_page_t*)area->reserved1;

            mi_page_claim_ownership(page);   // claim ownership
            if (mi_page_is_abandoned(page))
            {
                _mi_arenas_page_unabandon(page, theap);
            }
            else
            {
                page->next = page->prev = null;   // yikes.. better not to try to access this from a thread later on..
                mi_page_set_theap(page, null);    // set threadid to abandoned
            }
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_assert_internal(mi_page_is_owned(page));

            if (page->used == 0)
            {
                // free the page
                _mi_arenas_page_free(page, theap);
            }
            else if (heap_target == null)
            {
                // destroy the page
                page->used = 0;   // note: invariant `|local_free| + |free| == reserved - used` does not hold in this case
                _mi_arenas_page_free(page, theap);
            }
            else
            {
                // move the page to `heap_target` as an abandoned page
                // first remove it from the current heap
                nuint sbin = _mi_page_stats_bin(page);
                mi_arena_t* arena = null;
                nuint slice_index = 0;
                if (page->memid.memkind == mi_memkind_t.MI_MEM_ARENA)
                {
                    nuint slice_count;
                    mi_arena_pages_t* arena_pages = null;
                    arena = mi_page_arena_pages(page, &slice_index, &slice_count, &arena_pages);
                    mi_assert_internal(mi_bitmap_is_set(arena_pages->pages, slice_index));
                    mi_bitmap_clear(arena_pages->pages, slice_index);
                }
                else
                {
                    // os allocated
                    mi_assert_internal(mi_memkind_is_os(page->memid.memkind) && page->next == null);
                }
                // C: theap is NULL here so use the (atomic) heap stats
                __mi_stat_decrease_mt((mi_stat_count_t*)Unsafe.AsPointer(ref heap->stats.page_bins[(int)sbin]), 1);
                __mi_stat_decrease_mt(&heap->stats.pages, 1);
                mi_theap_t* theap_target = info->theap_target;

                // and then add it to the new target heap
                if (arena != null)
                {
                    mi_arena_pages_t* arena_pages_target = mi_heap_ensure_arena_pages(heap_target, arena);
                    if (arena_pages_target == null)
                    {
                        // if we cannot allocate this, we move it to the main heap instead (which does not require allocation)
                        heap_target = mi_arena_heap_main(arena);
                        theap_target = mi_heap_theap(heap_target);
                        arena_pages_target = mi_heap_ensure_arena_pages(heap_target, arena);
                        mi_assert_internal(arena_pages_target != null);
                    }
                    mi_assert_internal(mi_bitmap_is_clear(arena_pages_target->pages, slice_index));
                    mi_bitmap_set(arena_pages_target->pages, slice_index);
                }
                page->heap = heap_target;
                __mi_stat_increase((mi_stat_count_t*)Unsafe.AsPointer(ref theap_target->stats.page_bins[(int)sbin]), 1);
                __mi_stat_increase(&theap_target->stats.pages, 1);

                // and abandon in the new heap
                _mi_arenas_page_abandon(page, theap_target);
            }
            return true;
        }

        private static void mi_heap_delete_pages(mi_heap_t* heap, mi_heap_t* heap_target)
        {
            mi_theap_t* theap_target = (heap_target != null ? _mi_heap_theap(heap_target) : null);
            mi_heap_delete_visit_info_t info;
            info.heap_target = heap_target;
            info.theap_target = theap_target;
            info.theap = null;
            _mi_heap_visit_blocks(heap, false, false, &mi_heap_delete_page, &info);
#if MI_DEBUG
            // no more arena pages?
            for (int i = 0; i < MI_MAX_ARENAS; i++)
            {
                mi_arena_pages_t* arena_pages = (mi_arena_pages_t*)mi_atomic_load_relaxed(ref *(nuint*)&heap->arena_pages[i]);
                if (arena_pages != null)
                {
                    mi_assert_internal(mi_bitmap_is_all_clear(arena_pages->pages));
                }
            }
            // nor os abandoned pages?
            mi_lock_acquire(&heap->os_abandoned_pages_lock);
            try
            {
                mi_assert_internal(heap->os_abandoned_pages == null);
            }
            finally
            {
                mi_lock_release(&heap->os_abandoned_pages_lock);
            }
            // nor arena abandoned pages?
            for (nuint i = 0; i < MI_ARENA_BIN_COUNT; i++)
            {
                mi_assert_internal(mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)i]) == 0);
            }
#endif
        }

        public static void _mi_heap_move_pages(mi_heap_t* heap_from, mi_heap_t* heap_to)
        {
            if (_mi_is_heap_main(heap_from)) return;
            if (heap_to == null) { heap_to = _mi_subproc_heap_main(heap_from->subproc); }
            mi_heap_delete_pages(heap_from, heap_to);
        }

        public static void _mi_heap_destroy_pages(mi_heap_t* heap_from)
        {
            if (_mi_is_heap_main(heap_from)) return;
            mi_heap_delete_pages(heap_from, null);
        }
    }
}
