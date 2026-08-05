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
// Port of mimalloc v3.4.4 `src/alloc-aligned.c` -- aligned allocation:
// natural-alignment fast path, over-allocation fallback with interior-pointer
// marking, huge-singleton path for very large alignments, and the aligned
// realloc family. MI_GUARDED paths are omitted (pinned to 0).
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ------------------------------------------------------
        // Aligned Allocation
        // ------------------------------------------------------

        private static bool mi_malloc_is_naturally_aligned(nuint size, nuint alignment)
        {
            // certain blocks are always allocated at a certain natural alignment.
            // (see also `arena.c:mi_arenas_page_alloc_fresh`).
            mi_assert_internal(mi_alignment_is_valid(alignment));
            if (alignment > size) return false;
            nuint bsize = mi_good_size(size);
            bool ok = (bsize <= MI_PAGE_MAX_START_BLOCK_ALIGN2 && _mi_is_power_of_two(bsize)) ||                    // power-of-two under N
                      (alignment == MI_PAGE_OSPAGE_BLOCK_ALIGN2 && (bsize % MI_PAGE_OSPAGE_BLOCK_ALIGN2) == 0);     // or multiple of N
            if (ok) { mi_assert_internal((bsize & (alignment - 1)) == 0); }   // since both power of 2 and alignment <= size
            return ok;
        }

        // C: mi_theap_malloc_zero_no_guarded -- no guarded sampling in the pinned config
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_theap_malloc_zero_no_guarded(mi_theap_t* theap, nuint size, bool zero, nuint* usable)
        {
            return _mi_theap_malloc_zero(theap, size, zero, usable);
        }

        // Fallback aligned allocation that over-allocates -- split out for better codegen
        private static void* mi_theap_malloc_zero_aligned_at_overalloc(mi_theap_t* theap, nuint size, nuint alignment, nuint offset, bool zero, nuint* usable)
        {
            mi_assert_internal(size <= (unchecked((nuint)MI_MAX_ALLOC_SIZE) - MI_PADDING_SIZE));
            mi_assert_internal(mi_alignment_is_valid(alignment));

            void* p;
            nuint oversize;
            if (alignment > MI_PAGE_MAX_OVERALLOC_ALIGN)
            {
                // use OS allocation for large alignments and allocate inside a singleton page (not in an arena)
                // This can support alignments >= MI_PAGE_ALIGN by ensuring the object can be aligned
                // in the first (and single) page such that the page info is `MI_PAGE_ALIGN` bytes before it (and can be found in the _mi_page_map).
                if (offset != 0)
                {
                    // todo: cannot support offset alignment for very large alignments yet
                    _mi_error_message(EOVERFLOW, $"aligned allocation with a large alignment cannot be used with an alignment offset (size {size}, alignment {alignment}, offset {offset})");
                    return null;
                }
                oversize = (size <= MI_SMALL_SIZE_MAX ? MI_SMALL_SIZE_MAX + 1 /* ensure we use generic malloc path */ : size);
                // note: no guarded as alignment > 0
                p = _mi_theap_malloc_zero_ex(theap, oversize, zero, alignment, usable);   // the page block size should be large enough to align in the single huge page block
                if (p == null) return null;
            }
            else
            {
                // otherwise over-allocate
                mi_assert_internal(size <= (unchecked((nuint)MI_MAX_ALLOC_SIZE) - MI_PADDING_SIZE) && alignment <= MI_PAGE_MAX_OVERALLOC_ALIGN);
                mi_assert_internal(size < nuint.MaxValue - alignment);   // `oversize` cannot overflow
                oversize = (size < MI_MAX_ALIGN_SIZE ? MI_MAX_ALIGN_SIZE : size) + alignment - 1;   // adjust for size <= 16; with size 0 and alignment 64k, we would allocate a 64k block and pointing just beyond that.
                p = mi_theap_malloc_zero_no_guarded(theap, oversize, zero, usable);
                if (p == null) return null;
            }

            // .. and align within the allocation
            nuint align_mask = alignment - 1;   // for any x, `(x & align_mask) == (x % alignment)`
            nuint poffset = ((nuint)p + offset) & align_mask;
            nuint adjust = (poffset == 0 ? 0 : alignment - poffset);
            mi_assert_internal(adjust < alignment);
            void* aligned_p = (void*)((nuint)p + adjust);

            // note: after the above allocation, the page may be abandoned now (as it became full, see `page.c:_mi_malloc_generic`)
            // and we no longer own it. We should be careful to only read constant fields in the page,
            // or use safe atomic access as in `mi_page_set_has_interior_pointers`.
            // (we can access the page though since the just allocated pointer keeps it alive)
            mi_page_t* page = _mi_ptr_page(p);
            if (aligned_p != p)
            {
                mi_page_set_has_interior_pointers(page, true);
                if (usable != null)
                {
                    mi_assert_internal(*usable > adjust);
                    if (*usable > adjust) { *usable = *usable - adjust; }
                    mi_assert_internal(*usable >= size);
                }
                _mi_padding_shrink(page, (mi_block_t*)p, adjust + size);
            }

            mi_assert_internal(mi_page_usable_block_size(page) >= adjust + size);
            mi_assert_internal(((nuint)aligned_p + offset) % alignment == 0);
            mi_assert_internal(mi_usable_size(aligned_p) >= size);
            mi_assert_internal(mi_usable_size(p) == mi_usable_size(aligned_p) + adjust);
#if MI_DEBUG
            {
                mi_page_t* apage = _mi_ptr_page(aligned_p);
                void* unalign_p = _mi_page_ptr_unalign(apage, aligned_p);
                mi_assert_internal(p == unalign_p);
            }
#endif
            return aligned_p;
        }

        // Generic primitive aligned allocation -- split out for better codegen
        private static void* mi_theap_malloc_zero_aligned_at_generic(mi_theap_t* theap, nuint size, nuint alignment, nuint offset, bool zero, nuint* usable)
        {
            mi_assert_internal(mi_alignment_is_valid(alignment));
            // we don't allocate more than MI_MAX_ALLOC_SIZE
            if (size > (unchecked((nuint)MI_MAX_ALLOC_SIZE) - MI_PADDING_SIZE))
            {
                _mi_error_message(EINVAL, $"aligned allocation request is too large (size {size}, alignment {alignment})");
                return null;
            }

            // use regular allocation if it is guaranteed to fit the alignment constraints.
            // this is important to try as the fast path in `mi_theap_malloc_zero_aligned` only works when there exist
            // a page with the right block size, and if we always use the over-alloc fallback that would never happen.
            if (offset == 0 && mi_malloc_is_naturally_aligned(size, alignment))
            {
                void* p = mi_theap_malloc_zero_no_guarded(theap, size, zero, usable);
                mi_assert_internal(p == null || ((nuint)p % alignment) == 0);
                bool is_aligned_or_null = (((nuint)p) & (alignment - 1)) == 0;
                if (is_aligned_or_null)
                {
                    return p;
                }
                else
                {
                    // this should never happen if the `mi_malloc_is_naturally_aligned` check is correct..
                    mi_assert(false);
                    mi_free(p);
                }
            }

            // fall back to over-allocation
            return mi_theap_malloc_zero_aligned_at_overalloc(theap, size, alignment, offset, zero, usable);
        }

        private static void* mi_error_bad_alignment(nuint size, nuint alignment, nuint offset)
        {
            _mi_error_message(EINVAL, $"aligned allocation requires the alignment to be a power-of-two (size {size}, alignment {alignment}, offset {offset})");
            return null;
        }

        // Primitive aligned allocation
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_theap_malloc_zero_aligned_at(mi_theap_t* theap, nuint size, nuint alignment, nuint offset, bool zero, nuint* usable)
        {
            // note: we don't require `size > offset`, we just guarantee that the address at offset is aligned regardless of the allocated size.
            if (!mi_alignment_is_valid(alignment))   // require power-of-two (see <https://en.cppreference.com/w/c/memory/aligned_alloc#Notes>)
            {
                return mi_error_bad_alignment(size, alignment, offset);
            }

            // try first if there happens to be a small block available with just the right alignment
            // since most small power-of-2 blocks (under MI_PAGE_MAX_BLOCK_START_ALIGN2) are already
            // naturally aligned this can be often the case.
            if (theap != null)   // C: MI_THEAP_INITASNULL
            {
                if (size <= MI_SMALL_SIZE_MAX && alignment <= size)
                {
                    nuint align_mask = alignment - 1;   // for any x, `(x & align_mask) == (x % alignment)`
                    nuint padsize = size + MI_PADDING_SIZE;
                    mi_page_t* page = _mi_theap_get_free_small_page(theap, padsize);
                    if (page->free != null)
                    {
                        bool is_aligned = (((nuint)page->free + offset) & align_mask) == 0;
                        if (is_aligned)
                        {
                            if (usable != null) { *usable = mi_page_usable_block_size(page); }
                            void* p = _mi_page_malloc_zero(theap, page, padsize, zero);
                            mi_assert_internal(p != null);
                            mi_assert_internal(((nuint)p + offset) % alignment == 0);
                            return p;
                        }
                    }
                }
            }

            // fallback to generic aligned allocation
            return mi_theap_malloc_zero_aligned_at_generic(theap, size, alignment, offset, zero, usable);
        }

        // ------------------------------------------------------
        // Internal mi_theap_malloc_aligned / mi_malloc_aligned
        // ------------------------------------------------------

        private static void* mi_theap_malloc_aligned_at(mi_theap_t* theap, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_malloc_zero_aligned_at(theap, size, alignment, offset, false, null);
        }

        public static void* mi_theap_malloc_aligned(mi_theap_t* theap, nuint size, nuint alignment)
        {
            return mi_theap_malloc_aligned_at(theap, size, alignment, 0);
        }

        private static void* mi_theap_zalloc_aligned_at(mi_theap_t* theap, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_malloc_zero_aligned_at(theap, size, alignment, offset, true, null);
        }

        private static void* mi_theap_zalloc_aligned(mi_theap_t* theap, nuint size, nuint alignment)
        {
            return mi_theap_zalloc_aligned_at(theap, size, alignment, 0);
        }

        private static void* mi_theap_calloc_aligned_at(mi_theap_t* theap, nuint count, nuint size, nuint alignment, nuint offset)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_theap_zalloc_aligned_at(theap, total, alignment, offset);
        }

        private static void* mi_theap_calloc_aligned(mi_theap_t* theap, nuint count, nuint size, nuint alignment)
        {
            return mi_theap_calloc_aligned_at(theap, count, size, alignment, 0);
        }

        // ------------------------------------------------------
        // Aligned Allocation
        // ------------------------------------------------------

        public static void* mi_malloc_aligned_at(nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_malloc_aligned_at(_mi_theap_default(), size, alignment, offset);
        }

        public static void* mi_malloc_aligned(nuint size, nuint alignment)
        {
            return mi_theap_malloc_aligned(_mi_theap_default(), size, alignment);
        }

        public static void* mi_umalloc_aligned(nuint size, nuint alignment, nuint* block_size)
        {
            return mi_theap_malloc_zero_aligned_at(_mi_theap_default(), size, alignment, 0, false, block_size);
        }

        public static void* mi_zalloc_aligned_at(nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_zalloc_aligned_at(_mi_theap_default(), size, alignment, offset);
        }

        public static void* mi_zalloc_aligned(nuint size, nuint alignment)
        {
            return mi_theap_zalloc_aligned(_mi_theap_default(), size, alignment);
        }

        public static void* mi_uzalloc_aligned(nuint size, nuint alignment, nuint* block_size)
        {
            return mi_theap_malloc_zero_aligned_at(_mi_theap_default(), size, alignment, 0, true, block_size);
        }

        public static void* mi_calloc_aligned_at(nuint count, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_calloc_aligned_at(_mi_theap_default(), count, size, alignment, offset);
        }

        public static void* mi_calloc_aligned(nuint count, nuint size, nuint alignment)
        {
            return mi_theap_calloc_aligned(_mi_theap_default(), count, size, alignment);
        }

        public static void* mi_heap_malloc_aligned_at(mi_heap_t* heap, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_malloc_aligned_at(_mi_heap_theap(heap), size, alignment, offset);
        }

        public static void* mi_heap_malloc_aligned(mi_heap_t* heap, nuint size, nuint alignment)
        {
            return mi_theap_malloc_aligned(_mi_heap_theap(heap), size, alignment);
        }

        public static void* mi_heap_zalloc_aligned_at(mi_heap_t* heap, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_zalloc_aligned_at(_mi_heap_theap(heap), size, alignment, offset);
        }

        public static void* mi_heap_zalloc_aligned(mi_heap_t* heap, nuint size, nuint alignment)
        {
            return mi_theap_zalloc_aligned(_mi_heap_theap(heap), size, alignment);
        }

        public static void* mi_heap_calloc_aligned_at(mi_heap_t* heap, nuint count, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_calloc_aligned_at(_mi_heap_theap(heap), count, size, alignment, offset);
        }

        public static void* mi_heap_calloc_aligned(mi_heap_t* heap, nuint count, nuint size, nuint alignment)
        {
            return mi_theap_calloc_aligned(_mi_heap_theap(heap), count, size, alignment);
        }

        // ------------------------------------------------------
        // Aligned re-allocation
        // ------------------------------------------------------

        private static void* mi_theap_realloc_zero_aligned_at(mi_theap_t* theap, void* p, nuint newsize, nuint alignment, nuint offset, bool zero)
        {
            mi_assert(mi_alignment_is_valid(alignment));
            if (!mi_alignment_is_valid(alignment))   // require power-of-two (see <https://en.cppreference.com/w/c/memory/aligned_alloc>)
            {
                return mi_error_bad_alignment(newsize, alignment, offset);
            }
            if (alignment <= (nuint)MI_INTPTR_SIZE && offset == 0) return _mi_theap_realloc_zero(theap, p, newsize, zero, null, null);
            if (p == null) return mi_theap_malloc_zero_aligned_at(theap, newsize, alignment, offset, zero, null);
            nuint size = mi_usable_size(p);
            if (newsize <= size && newsize >= (size - (size / 2)) && (((nuint)p + offset) & (alignment - 1)) == 0)
            {
                return p;   // reallocation still fits, is aligned and not more than 50% waste
            }
            else
            {
                // note: we don't zero allocate upfront so we only zero initialize the expanded part
                void* newp = mi_theap_malloc_aligned_at(theap, newsize, alignment, offset);
                if (newp != null)
                {
                    if (zero && newsize > size)
                    {
                        // also set last word in the previous allocation to zero to ensure any padding is zero-initialized
                        nuint start = (size >= (nuint)MI_INTPTR_SIZE ? size - (nuint)MI_INTPTR_SIZE : 0);
                        _mi_memzero((byte*)newp + start, newsize - start);
                    }
                    _mi_memcpy(newp, p, (newsize > size ? size : newsize));   // cannot be aligned due to arbitrary offset
                    mi_free(p);   // only free if successful
                }
                return newp;
            }
        }

        private static void* mi_theap_realloc_zero_aligned(mi_theap_t* theap, void* p, nuint newsize, nuint alignment, bool zero)
        {
            mi_assert(alignment > 0);
            if (alignment <= (nuint)MI_INTPTR_SIZE) return _mi_theap_realloc_zero(theap, p, newsize, zero, null, null);
            return mi_theap_realloc_zero_aligned_at(theap, p, newsize, alignment, 0, zero);
        }

        private static void* mi_theap_realloc_aligned_at(mi_theap_t* theap, void* p, nuint newsize, nuint alignment, nuint offset)
        {
            return mi_theap_realloc_zero_aligned_at(theap, p, newsize, alignment, offset, false);
        }

        private static void* mi_theap_realloc_aligned(mi_theap_t* theap, void* p, nuint newsize, nuint alignment)
        {
            return mi_theap_realloc_zero_aligned(theap, p, newsize, alignment, false);
        }

        private static void* mi_theap_rezalloc_aligned_at(mi_theap_t* theap, void* p, nuint newsize, nuint alignment, nuint offset)
        {
            return mi_theap_realloc_zero_aligned_at(theap, p, newsize, alignment, offset, true);
        }

        private static void* mi_theap_rezalloc_aligned(mi_theap_t* theap, void* p, nuint newsize, nuint alignment)
        {
            return mi_theap_realloc_zero_aligned(theap, p, newsize, alignment, true);
        }

        private static void* mi_theap_recalloc_aligned_at(mi_theap_t* theap, void* p, nuint newcount, nuint size, nuint alignment, nuint offset)
        {
            nuint total;
            if (mi_count_size_overflow(newcount, size, out total)) return null;
            return mi_theap_rezalloc_aligned_at(theap, p, total, alignment, offset);
        }

        private static void* mi_theap_recalloc_aligned(mi_theap_t* theap, void* p, nuint newcount, nuint size, nuint alignment)
        {
            nuint total;
            if (mi_count_size_overflow(newcount, size, out total)) return null;
            return mi_theap_rezalloc_aligned(theap, p, total, alignment);
        }

        public static void* mi_realloc_aligned_at(void* p, nuint newsize, nuint alignment, nuint offset)
        {
            return mi_theap_realloc_aligned_at(_mi_theap_default(), p, newsize, alignment, offset);
        }

        public static void* mi_realloc_aligned(void* p, nuint newsize, nuint alignment)
        {
            return mi_theap_realloc_aligned(_mi_theap_default(), p, newsize, alignment);
        }

        public static void* mi_rezalloc_aligned_at(void* p, nuint newsize, nuint alignment, nuint offset)
        {
            return mi_theap_rezalloc_aligned_at(_mi_theap_default(), p, newsize, alignment, offset);
        }

        public static void* mi_rezalloc_aligned(void* p, nuint newsize, nuint alignment)
        {
            return mi_theap_rezalloc_aligned(_mi_theap_default(), p, newsize, alignment);
        }

        public static void* mi_recalloc_aligned_at(void* p, nuint newcount, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_recalloc_aligned_at(_mi_theap_default(), p, newcount, size, alignment, offset);
        }

        public static void* mi_recalloc_aligned(void* p, nuint newcount, nuint size, nuint alignment)
        {
            return mi_theap_recalloc_aligned(_mi_theap_default(), p, newcount, size, alignment);
        }

        public static void* mi_heap_realloc_aligned_at(mi_heap_t* heap, void* p, nuint newsize, nuint alignment, nuint offset)
        {
            return mi_theap_realloc_aligned_at(_mi_heap_theap(heap), p, newsize, alignment, offset);
        }

        public static void* mi_heap_realloc_aligned(mi_heap_t* heap, void* p, nuint newsize, nuint alignment)
        {
            return mi_theap_realloc_aligned(_mi_heap_theap(heap), p, newsize, alignment);
        }

        public static void* mi_heap_rezalloc_aligned_at(mi_heap_t* heap, void* p, nuint newsize, nuint alignment, nuint offset)
        {
            return mi_theap_rezalloc_aligned_at(_mi_heap_theap(heap), p, newsize, alignment, offset);
        }

        public static void* mi_heap_rezalloc_aligned(mi_heap_t* heap, void* p, nuint newsize, nuint alignment)
        {
            return mi_theap_rezalloc_aligned(_mi_heap_theap(heap), p, newsize, alignment);
        }

        public static void* mi_heap_recalloc_aligned_at(mi_heap_t* heap, void* p, nuint newcount, nuint size, nuint alignment, nuint offset)
        {
            return mi_theap_recalloc_aligned_at(_mi_heap_theap(heap), p, newcount, size, alignment, offset);
        }

        public static void* mi_heap_recalloc_aligned(mi_heap_t* heap, void* p, nuint newcount, nuint size, nuint alignment)
        {
            return mi_theap_recalloc_aligned(_mi_heap_theap(heap), p, newcount, size, alignment);
        }
    }
}
