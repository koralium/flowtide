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
    }
}
