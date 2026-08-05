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
// Port of mimalloc v3.4.4 `include/mimalloc/internal.h` — the pure inline helpers.
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// The page/heap/theap accessors from internal.h are ported together with the C files
// that own their state (Page.cs, Theap.cs, PageMap.cs, ...); this file holds only the
// state-free arithmetic helpers used throughout the port.

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // Is `x` a power of two? (0 is considered a power of two)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool _mi_is_power_of_two(nuint x) => (x & (x - 1)) == 0;

        // valid alignment values are as posix memalign
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_alignment_is_valid(nuint alignment) => alignment != 0 && _mi_is_power_of_two(alignment);

        // Is a pointer aligned?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool _mi_is_aligned(void* p, nuint alignment) => alignment == 0 || ((nuint)p % alignment) == 0;

        // Align upwards
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_align_up(nuint sz, nuint alignment)
        {
            mi_assert_internal(alignment != 0);
            nuint mask = alignment - 1;
            if ((alignment & mask) == 0)  // power of two?
            {
                return (sz + mask) & ~mask;
            }
            else
            {
                return ((sz + mask) / alignment) * alignment;
            }
        }

        // Align a pointer upwards
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* _mi_align_up_ptr(void* p, nuint alignment) => (void*)_mi_align_up((nuint)p, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_align_down(nuint sz, nuint alignment)
        {
            mi_assert_internal(alignment != 0);
            nuint mask = alignment - 1;
            if ((alignment & mask) == 0)  // power of two?
            {
                return sz & ~mask;
            }
            else
            {
                return (sz / alignment) * alignment;
            }
        }

        // align a pointer downwards
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* _mi_align_down_ptr(void* p, nuint alignment) => (void*)_mi_align_down((nuint)p, alignment);

        // Divide upwards: `s <= _mi_divide_up(s,d)*d < s+d`.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_divide_up(nuint size, nuint divider)
        {
            mi_assert_internal(divider != 0);
            return divider == 0 ? size : (size + divider - 1) / divider;
        }

        // clamp an integer
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_clamp(nuint sz, nuint min, nuint max)
        {
            if (sz < min) return min;
            else if (sz > max) return max;
            else return sz;
        }

        // Is memory zero initialized?
        public static bool mi_mem_is_zero(void* p, nuint size)
        {
            for (nuint i = 0; i < size; i++)
            {
                if (((byte*)p)[i] != 0) return false;
            }
            return true;
        }

        // Align a byte size to a size in _machine words_,
        // i.e. byte size == `wsize*sizeof(void*)`.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_wsize_from_size(nuint size)
        {
            mi_assert_internal(size <= nuint.MaxValue - (nuint)MI_INTPTR_SIZE);
            return (size + (nuint)MI_INTPTR_SIZE - 1) / (nuint)MI_INTPTR_SIZE;
        }

        // Overflow detecting multiply
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_mul_overflow(nuint count, nuint size, out nuint total)
        {
            total = unchecked(count * size);
            if (((size | count) >> (4 * MI_SIZE_SIZE)) == 0)  // did size and count fit both in the lower half bits of a size_t?
            {
                return false;
            }
            else
            {
                return size != 0 && (nuint.MaxValue / size) < count;
            }
        }

        // Safe multiply `count*size` into `total`; return `true` on overflow.
        public static bool mi_count_size_overflow(nuint count, nuint size, out nuint total)
        {
            if (count == 1)  // quick check for the case where count is one (common for C++ allocators)
            {
                total = size;
                return false;
            }
            else if (!mi_mul_overflow(count, size, out total))
            {
                return false;
            }
            else
            {
#if MI_DEBUG
                // C: only under `#if MI_DEBUG > 0` (release builds are silent here)
                _mi_error_message(EOVERFLOW, $"allocation request is too large ({count} * {size} bytes)");
#endif
                total = nuint.MaxValue;
                return true;
            }
        }

        /* -----------------------------------------------------------
          arena blocks
        ----------------------------------------------------------- */

        // Blocks needed for a given byte size
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_slice_count_of_size(nuint size) => _mi_divide_up(size, MI_ARENA_SLICE_SIZE);

        // Byte size of a number of blocks
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_size_of_slices(nuint bcount) => bcount * MI_ARENA_SLICE_SIZE;

        /* -----------------------------------------------------------
          memory id's
        ----------------------------------------------------------- */

        public static mi_memid_t _mi_memid_create(mi_memkind_t memkind)
        {
            mi_memid_t memid = default;
            memid.memkind = memkind;
            return memid;
        }

        public static mi_memid_t _mi_memid_none() => _mi_memid_create(mi_memkind_t.MI_MEM_NONE);

        public static mi_memid_t _mi_memid_create_os(void* @base, nuint size, bool committed, bool is_zero, bool is_large)
        {
            mi_memid_t memid = _mi_memid_create(mi_memkind_t.MI_MEM_OS);
            memid.mem.os.@base = @base;
            memid.mem.os.size = size;
            memid.initially_committed = committed;
            memid.initially_zero = is_zero;
            memid.is_pinned = is_large;
            return memid;
        }

        public static mi_memid_t _mi_memid_create_meta(mi_meta_page_t* mpage, nuint block_idx, nuint block_count)
        {
            mi_memid_t memid = _mi_memid_create(mi_memkind_t.MI_MEM_META);
            memid.mem.meta.meta_page = mpage;
            memid.mem.meta.block_index = (uint)block_idx;
            memid.mem.meta.block_count = (uint)block_count;
            memid.initially_committed = true;
            memid.initially_zero = true;
            memid.is_pinned = true;
            return memid;
        }

        /* -----------------------------------------------------------
          Page accessors (pure; from internal.h)
        ----------------------------------------------------------- */

        // Get the block size of a page
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_block_size(mi_page_t* page)
        {
            mi_assert_internal(page->block_size > 0);
            return page->block_size;
        }

        // Page start
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* mi_page_start(mi_page_t* page)
        {
            return (byte*)page + ((nuint)page->page_ma_offset * MI_MAX_ALIGN_SIZE);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_size(mi_page_t* page) => mi_page_block_size(page) * page->reserved;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* mi_page_area(mi_page_t* page, nuint* size)
        {
            if (size != null) { *size = mi_page_size(page); }
            return mi_page_start(page);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_info_size() => _mi_align_up((nuint)sizeof(mi_page_t), MI_MAX_ALIGN_SIZE);

        public static bool mi_page_contains_address(mi_page_t* page, void* p)
        {
            nuint psize;
            byte* start = mi_page_area(page, &psize);
            return start <= (byte*)p && (byte*)p < start + psize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_in_arena(mi_page_t* page) => page->memid.memkind == mi_memkind_t.MI_MEM_ARENA;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_singleton(mi_page_t* page) => page->reserved == 1;

        // Get the usable block size of a page without fixed padding.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_usable_block_size(mi_page_t* page) => mi_page_block_size(page) - MI_PADDING_SIZE;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_meta_is_separated(mi_page_t* page)
        {
            // MI_PAGE_META_IS_SEPARATED == 1:
            // usually separated but can still be in front for direct OS allocations (due to size or alignment)
            return page->memid.memkind == mi_memkind_t.MI_MEM_ARENA
                && page != (mi_page_t*)_mi_align_down_ptr(mi_page_start(page), MI_ARENA_SLICE_ALIGN);
        }

        public static byte* mi_page_slice_start(mi_page_t* page)
        {
            if (mi_page_meta_is_separated(page))
            {
                // page meta info is at a separate location (at `arena->pages`)
                return (byte*)_mi_align_down_ptr(mi_page_start(page), MI_ARENA_SLICE_ALIGN);
            }
            else
            {
                // page meta info is at the start of the page slices
                return (byte*)page;
            }
        }

        // This gives the offset relative to the start slice of a page.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_slice_offset_of(mi_page_t* page, nuint offset_relative_to_page_start)
        {
            return (nuint)(mi_page_start(page) - mi_page_slice_start(page)) + offset_relative_to_page_start;
        }

        // Currently committed part of a page
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_committed(mi_page_t* page)
        {
            return page->slice_committed == 0 ? mi_page_size(page) : page->slice_committed - mi_page_slice_offset_of(page, 0);
        }

        // are all blocks in a page freed?
        // note: needs up-to-date used count, (as the `xthread_free` list may not be empty). see `_mi_page_collect_free`.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_all_free(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            return page->used == 0;
        }

        // are there immediately available blocks, i.e. blocks available on the free list.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_immediate_available(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            return page->free != null;
        }

        // is the page not yet used up to its reserved space?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_expandable(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(page->capacity <= page->reserved);
            return page->capacity < page->reserved;
        }

        /* -----------------------------------------------------------
          Theap accessors (from internal.h)
        ----------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_heap_t* _mi_theap_heap_peek(mi_theap_t* theap)
            => mi_atomic_load_ptr_relaxed(&theap->heap);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_heap_t* _mi_theap_heap(mi_theap_t* theap)
        {
            mi_heap_t* heap = _mi_theap_heap_peek(theap);
            mi_assert_internal(heap != null);
            return heap;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_theap_is_initialized(mi_theap_t* theap)
            => theap != null && _mi_theap_heap_peek(theap) != null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_subproc_t* _mi_theap_subproc(mi_theap_t* theap)
        {
            mi_subproc_t* subproc = mi_atomic_load_ptr_relaxed(&theap->subproc);
            mi_assert_internal(!mi_theap_is_initialized(theap) || _mi_theap_heap(theap)->subproc == subproc);
            return subproc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_page_t* _mi_theap_get_free_small_page(mi_theap_t* theap, nuint size)
        {
            mi_assert_internal(size <= MI_SMALL_SIZE_MAX + MI_PADDING_SIZE);
            nuint idx = _mi_wsize_from_size(size);
            mi_assert_internal(idx < MI_PAGES_DIRECT);
            return (mi_page_t*)theap->pages_free_direct[idx];
        }

        /* -----------------------------------------------------------
          Page fullness / huge / queue (from internal.h)
        ----------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_full(mi_page_t* page)
        {
            bool full = page->reserved == page->used;
            mi_assert_internal(!full || page->free == null);
            return full;
        }

        // is more than 7/8th of a page in use?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_mostly_used(mi_page_t* page)
        {
            if (page == null) return true;
            ushort frac = (ushort)(page->reserved / 8u);
            return page->reserved - page->used <= frac;
        }

        // is more than (n-1)/n'th of a page in use?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_used_at_frac(mi_page_t* page, ushort n)
        {
            if (page == null) return true;
            ushort frac = (ushort)(page->reserved / n);
            return page->reserved - page->used <= frac;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_huge(mi_page_t* page)
        {
            return mi_page_is_singleton(page) &&
                   (page->block_size > MI_LARGE_MAX_OBJ_SIZE ||
                    (mi_memkind_is_os(page->memid.memkind) && page->memid.mem.os.@base < (void*)page));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_page_queue_t* mi_page_queue(mi_theap_t* theap, nuint size)
        {
            mi_page_queue_t* pq = (mi_page_queue_t*)Unsafe.AsPointer(ref theap->pages[(int)_mi_bin(size)]);
            if (size <= MI_LARGE_MAX_OBJ_SIZE) { mi_assert_internal(pq->block_size <= MI_LARGE_MAX_OBJ_SIZE); }
            return pq;
        }

        /* -----------------------------------------------------------
          Page thread id and flags (from internal.h)
        ----------------------------------------------------------- */

        // Thread id of thread that owns this page (with flags in the bottom 2 bits)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_xthread_id(mi_page_t* page)
            => mi_atomic_load_relaxed(ref page->xthread_id);

        // Plain thread id of the thread that owns this page
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_thread_id(mi_page_t* page)
            => mi_page_xthread_id(page) & ~MI_PAGE_FLAG_MASK;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_page_flags(mi_page_t* page)
            => mi_page_xthread_id(page) & MI_PAGE_FLAG_MASK;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_page_flags_set(mi_page_t* page, bool set, nuint newflag)
        {
            nuint old;
            if (set) { old = mi_atomic_or_relaxed(ref page->xthread_id, newflag); }
            else { old = mi_atomic_and_relaxed(ref page->xthread_id, ~newflag); }
            return (old & newflag) == newflag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_in_full(mi_page_t* page)
            => (mi_page_flags(page) & MI_PAGE_IN_FULL_QUEUE) != 0;

        public static void mi_page_set_in_full(mi_page_t* page, bool in_full)
        {
            bool was_in_full = mi_page_flags_set(page, in_full, MI_PAGE_IN_FULL_QUEUE);
            if (was_in_full != in_full)
            {
                // optimize: maintain pages_full_size to avoid visiting the full queue (issue #1220)
                mi_theap_t* theap = page->theap;
                mi_assert_internal(theap != null);
                if (theap != null)
                {
                    mi_assert_internal(page->capacity == page->reserved);
                    nuint size = page->reserved * mi_page_block_size(page);
                    if (in_full) { theap->pages_full_size += size; }
                    else { mi_assert_internal(size <= theap->pages_full_size); theap->pages_full_size -= size; }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_has_interior_pointers(mi_page_t* page)
            => (mi_page_flags(page) & MI_PAGE_HAS_INTERIOR_POINTERS) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_page_set_has_interior_pointers(mi_page_t* page, bool has_aligned)
            => mi_page_flags_set(page, has_aligned, MI_PAGE_HAS_INTERIOR_POINTERS);

        public static void mi_page_set_theap(mi_page_t* page, mi_theap_t* theap)
        {
            page->theap = theap;
            nuint tid = (theap == null ? MI_THREADID_ABANDONED : theap->tld->thread_id);
            mi_assert_internal((tid & MI_PAGE_FLAG_MASK) == 0);

            // we need to use an atomic cas since a concurrent thread may still set the
            // MI_PAGE_HAS_INTERIOR_POINTERS flag (see `alloc_aligned.c`).
            nuint xtid_old = mi_page_xthread_id(page);
            nuint xtid;
            do
            {
                xtid = tid | (xtid_old & MI_PAGE_FLAG_MASK);
            } while (!mi_atomic_cas_weak_release(ref page->xthread_id, ref xtid_old, xtid));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_abandoned(mi_page_t* page)
        {
            // note: the xtheap field of an abandoned theap is set to the subproc (for fast reclaim-on-free)
            return mi_page_thread_id(page) <= MI_THREADID_ABANDONED_MAPPED;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_abandoned_mapped(mi_page_t* page)
            => mi_page_thread_id(page) == MI_THREADID_ABANDONED_MAPPED;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_page_set_abandoned_mapped(mi_page_t* page)
        {
            mi_assert_internal(mi_page_is_abandoned(page));
            mi_atomic_or_relaxed(ref page->xthread_id, MI_THREADID_ABANDONED_MAPPED);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_page_clear_abandoned_mapped(mi_page_t* page)
        {
            mi_assert_internal(mi_page_is_abandoned_mapped(page));
            mi_atomic_and_relaxed(ref page->xthread_id, MI_PAGE_FLAG_MASK);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_theap_t* mi_page_theap(mi_page_t* page)
        {
            mi_assert_internal(!mi_page_is_abandoned(page));
            mi_assert_internal(page->theap != null);
            return page->theap;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_tld_t* mi_page_tld(mi_page_t* page)
        {
            mi_assert_internal(!mi_page_is_abandoned(page));
            mi_assert_internal(page->theap != null);
            return page->theap->tld;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_heap_t* mi_page_heap(mi_page_t* page)
        {
            mi_heap_t* heap = page->heap;
            mi_assert_internal(heap != null);
            return heap;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_subproc_t* mi_page_subproc(mi_page_t* page)
            => mi_page_heap(page)->subproc;

        // C: init.c `_mi_subproc_heap_main` (the port creates the main heap eagerly in
        // mi_process_init so the lazy-init branch of the C version is not needed)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_heap_t* _mi_subproc_heap_main(mi_subproc_t* subproc)
        {
            mi_heap_t* heap = mi_atomic_load_ptr_acquire(&subproc->heap_main);
            mi_assert_internal(heap != null);
            return heap;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_heap_t* mi_arena_heap_main(mi_arena_t* arena)
            => _mi_subproc_heap_main(arena->subproc);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_heap_t* mi_heap_get_heap_main(mi_heap_t* heap)
            => _mi_subproc_heap_main(heap->subproc);

        /* -----------------------------------------------------------
          Thread free list and ownership (from internal.h)
        ----------------------------------------------------------- */

        // Thread free flag helpers
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_block_t* mi_tf_block(nuint tf) => (mi_block_t*)(tf & ~(nuint)1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_tf_is_owned(nuint tf) => (tf & 1) == 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_tf_create(mi_block_t* block, bool owned)
            => (nuint)block | (owned ? (nuint)1 : 0);

        // Thread free access
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_block_t* mi_page_thread_free(mi_page_t* page)
            => mi_tf_block(mi_atomic_load_relaxed(ref page->xthread_free));

        // are there any available blocks?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_has_any_available(mi_page_t* page)
        {
            mi_assert_internal(page != null && page->reserved > 0);
            return page->used < page->reserved || mi_page_thread_free(page) != null;
        }

        // Owned?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_is_owned(mi_page_t* page)
            => mi_tf_is_owned(mi_atomic_load_relaxed(ref page->xthread_free));

        // get ownership; returns true if the page was not owned before.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_page_claim_ownership(mi_page_t* page)
        {
            nuint old = mi_atomic_or_acq_rel(ref page->xthread_free, 1);
            return (old & 1) == 0;
        }

        /* -------------------------------------------------------------------
          Free list next pointers (MI_ENCODE_FREELIST == 0: plain pointers)
        ------------------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_is_in_same_page(void* p, void* q)
        {
            mi_page_t* page = _mi_ptr_page(p);
            return mi_page_contains_address(page, q);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_block_t* mi_block_next(mi_page_t* page, mi_block_t* block)
            => (mi_block_t*)block->next;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_block_set_next(mi_page_t* page, mi_block_t* block, mi_block_t* next)
            => block->next = (nuint)next;
    }
}
