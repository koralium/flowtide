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
// Port of mimalloc v3.4.4 `src/alloc.c` -- the malloc fast paths:
// per-page free-list pop, small-size direct page lookup, zalloc/calloc/mallocn,
// realloc family, and strdup/strndup.
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Not ported (out of scope for the pinned configuration / .NET):
//  - mi_realpath (OS path API; .NET callers use Path.GetFullPath)
//  - the C++ `mi_new_*` family (new-handler + bad_alloc semantics do not map to C#)
//  - MI_GUARDED paths (pinned to 0)

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // Fast allocation in a page: just pop from the free list.
        // Fall back to generic allocation only if the list is empty.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_page_malloc_zero(mi_theap_t* theap, mi_page_t* page, nuint size, bool zero, nuint* usable)
        {
            if (page->block_size != 0)   // not the empty theap
            {
                mi_assert_internal(mi_page_block_size(page) >= size);
                mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
                mi_assert_internal(_mi_ptr_page(mi_page_start(page)) == page);
            }

            // check the free list
            mi_block_t* block = page->free;
            if (block == null)
            {
                return _mi_malloc_generic(theap, size, (zero ? (nuint)1 : 0), usable);
            }
            mi_assert_internal(block != null && _mi_ptr_page(block) == page);
            if (usable != null) { *usable = mi_page_usable_block_size(page); }

            // pop from the free list
            page->free = mi_block_next(page, block);
            page->used++;
            mi_assert_internal(page->free == null || _mi_ptr_page(page->free) == page);
            mi_assert_internal(page->block_size < MI_MAX_ALIGN_SIZE || _mi_is_aligned(block, MI_MAX_ALIGN_SIZE));

            nuint bsize = mi_page_usable_block_size(page);

            // stats (C: MI_STAT>0; the port pins MI_STAT=1)
            if (bsize <= MI_LARGE_MAX_OBJ_SIZE)
            {
                __mi_stat_increase(&theap->stats.malloc_normal, bsize);
            }

            // zero the block? note: we need to zero the full block size (issue #63)
            if (!zero)
            {
                block->next = 0;   // don't leak internal data
#if MI_DEBUG
                if (!mi_page_is_huge(page)) { _mi_memset(block, MI_DEBUG_UNINIT, bsize); }
#endif
            }
            else
            {
                if (!page->free_is_zero)
                {
                    _mi_memzero_aligned(block, bsize);
                }
                else
                {
                    block->next = 0;
                }
            }

            return block;
        }

        // extra entry for improved efficiency in `alloc-aligned.c` (and in `page.c:_mi_malloc_generic`)
        public static void* _mi_page_malloc_zero(mi_theap_t* theap, mi_page_t* page, nuint size, bool zero)
        {
            return mi_page_malloc_zero(theap, page, size, zero, null);
        }

        // main allocation primitives for small and generic allocation

        // internal small size allocation
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_theap_malloc_small_zero_nonnull(mi_theap_t* theap, nuint size, bool zero, nuint* usable)
        {
            mi_assert(theap != null);
            mi_assert(size <= MI_SMALL_SIZE_MAX);
            mi_assert(theap->tld->thread_id == 0 || theap->tld->thread_id == _mi_thread_id());   // theaps are thread local

            // get page in constant time, and allocate from it
            mi_page_t* page = _mi_theap_get_free_small_page(theap, size + MI_PADDING_SIZE);
            void* p = mi_page_malloc_zero(theap, page, size + MI_PADDING_SIZE, zero, usable);
            return p;
        }

        // internal generic allocation
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_theap_malloc_generic(mi_theap_t* theap, nuint size, bool zero, nuint huge_alignment, nuint* usable)
        {
            mi_assert(theap == null || theap->tld->thread_id == 0 || theap->tld->thread_id == _mi_thread_id());   // theaps are thread local
            mi_assert((huge_alignment & 1) == 0);
            void* p = _mi_malloc_generic(theap, size + MI_PADDING_SIZE, (zero ? (nuint)1 : 0) | huge_alignment, usable);   // note: size can overflow but it is detected in malloc_generic
            return p;
        }

        // internal small allocation (C: MI_THEAP_INITASNULL variant)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_theap_malloc_small_zero(mi_theap_t* theap, nuint size, bool zero, nuint* usable)
        {
            if (theap != null)
            {
                return mi_theap_malloc_small_zero_nonnull(theap, size, zero, usable);
            }
            else
            {
                return mi_theap_malloc_generic(theap, size, zero, 0, usable);   // tailcall
            }
        }

        // allocate a small block
        public static void* mi_theap_malloc_small(mi_theap_t* theap, nuint size)
        {
            return mi_theap_malloc_small_zero(theap, size, false, null);
        }

        public static void* mi_malloc_small(nuint size)
        {
            return mi_theap_malloc_small(_mi_theap_default(), size);
        }

        public static void* mi_heap_malloc_small(mi_heap_t* heap, nuint size)
        {
            return mi_theap_malloc_small_zero_nonnull(_mi_heap_theap(heap), size, false, null);
        }

        // The main internal allocation functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* mi_theap_malloc_zero_nonnull(mi_theap_t* theap, nuint size, bool zero, nuint huge_alignment, nuint* usable)
        {
            // fast path for small objects
            if (size <= MI_SMALL_SIZE_MAX)
            {
                mi_assert_internal(huge_alignment == 0);
                return mi_theap_malloc_small_zero_nonnull(theap, size, zero, usable);
            }
            else
            {
                return mi_theap_malloc_generic(theap, size, zero, huge_alignment, usable);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* _mi_theap_malloc_zero_ex(mi_theap_t* theap, nuint size, bool zero, nuint huge_alignment, nuint* usable)
        {
            // fast path for small objects (C: MI_THEAP_INITASNULL null check)
            if (theap != null && size <= MI_SMALL_SIZE_MAX)
            {
                mi_assert_internal(huge_alignment == 0);
                return mi_theap_malloc_small_zero_nonnull(theap, size, zero, usable);
            }
            else
            {
                return mi_theap_malloc_generic(theap, size, zero, huge_alignment, usable);
            }
        }

        public static void* _mi_theap_malloc_zero(mi_theap_t* theap, nuint size, bool zero, nuint* usable)
        {
            return _mi_theap_malloc_zero_ex(theap, size, zero, 0, usable);
        }

        // Main allocation functions

        public static void* mi_theap_malloc(mi_theap_t* theap, nuint size)
        {
            return _mi_theap_malloc_zero(theap, size, false, null);
        }

        public static void* mi_malloc(nuint size)
        {
            return mi_theap_malloc(_mi_theap_default(), size);
        }

        public static void* mi_heap_malloc(mi_heap_t* heap, nuint size)
        {
            return mi_theap_malloc_zero_nonnull(_mi_heap_theap(heap), size, false, 0, null);
        }

        // zero initialized small block
        public static void* mi_zalloc_small(nuint size)
        {
            return mi_theap_malloc_small_zero(_mi_theap_default(), size, true, null);
        }

        public static void* mi_theap_zalloc_small(mi_theap_t* theap, nuint size)
        {
            return mi_theap_malloc_small_zero(theap, size, true, null);
        }

        public static void* mi_heap_zalloc_small(mi_heap_t* heap, nuint size)
        {
            return mi_theap_malloc_small_zero_nonnull(_mi_heap_theap(heap), size, true, null);
        }

        public static void* mi_theap_zalloc(mi_theap_t* theap, nuint size)
        {
            return _mi_theap_malloc_zero(theap, size, true, null);
        }

        public static void* mi_zalloc(nuint size)
        {
            return _mi_theap_malloc_zero(_mi_theap_default(), size, true, null);
        }

        public static void* mi_heap_zalloc(mi_heap_t* heap, nuint size)
        {
            return mi_theap_malloc_zero_nonnull(_mi_heap_theap(heap), size, true, 0, null);
        }

        public static void* mi_theap_calloc(mi_theap_t* theap, nuint count, nuint size)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_theap_zalloc(theap, total);
        }

        public static void* mi_calloc(nuint count, nuint size)
        {
            return mi_theap_calloc(_mi_theap_default(), count, size);
        }

        public static void* mi_heap_calloc(mi_heap_t* heap, nuint count, nuint size)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_heap_zalloc(heap, total);
        }

        // Return usable size
        public static void* mi_umalloc_small(nuint size, nuint* usable)
        {
            return mi_theap_malloc_small_zero(_mi_theap_default(), size, false, usable);
        }

        public static void* mi_uzalloc_small(nuint size, nuint* usable)
        {
            return mi_theap_malloc_small_zero(_mi_theap_default(), size, true, usable);
        }

        public static void* mi_theap_umalloc(mi_theap_t* theap, nuint size, nuint* usable)
        {
            return _mi_theap_malloc_zero_ex(theap, size, false, 0, usable);
        }

        public static void* mi_umalloc(nuint size, nuint* usable)
        {
            return mi_theap_umalloc(_mi_theap_default(), size, usable);
        }

        public static void* mi_uzalloc(nuint size, nuint* usable)
        {
            return _mi_theap_malloc_zero_ex(_mi_theap_default(), size, true, 0, usable);
        }

        public static void* mi_ucalloc(nuint count, nuint size, nuint* usable)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_uzalloc(total, usable);
        }

        // Uninitialized `calloc`
        private static void* mi_theap_mallocn(mi_theap_t* theap, nuint count, nuint size)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_theap_malloc(theap, total);
        }

        public static void* mi_mallocn(nuint count, nuint size)
        {
            return mi_theap_mallocn(_mi_theap_default(), count, size);
        }

        public static void* mi_heap_mallocn(mi_heap_t* heap, nuint count, nuint size)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_heap_malloc(heap, total);
        }

        // Expand (or shrink) in place (or fail)  (C: MI_PADDING==0 branch)
        public static void* mi_expand(void* p, nuint newsize)
        {
            if (p == null) return null;
            mi_page_t* page = mi_validate_ptr_page(p, "mi_expand");
            nuint size = _mi_usable_size(p, page);
            if (newsize > size) return null;
            return p;   // it fits
        }

        public static void* _mi_theap_realloc_zero(mi_theap_t* theap, void* p, nuint newsize, bool zero, nuint* usable_pre, nuint* usable_post)
        {
            // if p == NULL then behave as malloc.
            // else if size == 0 then reallocate to a zero-sized block (and don't return NULL, just as mi_malloc(0)).
            // (this means that returning NULL always indicates an error, and `p` will not have been freed in that case.)
            mi_page_t* page;
            nuint size;
            if (p == null)
            {
                page = null;
                size = 0;
                if (usable_pre != null) { *usable_pre = 0; }
            }
            else
            {
                page = mi_validate_ptr_page(p, "mi_realloc");
                if (page == null)   // invalid pointer
                {
                    if (usable_pre != null) { *usable_pre = 0; }
                    if (usable_post != null) { *usable_post = 0; }
                    return null;
                }
                size = _mi_usable_size(p, page);
                if (usable_pre != null) { *usable_pre = mi_page_usable_block_size(page); }
            }
            // check if we can reuse the existing block
            if (newsize <= size && newsize >= (size / 2) && newsize > 0)   // note: newsize must be > 0 or otherwise we return NULL for realloc(NULL,0)
            {
                mi_assert_internal(page != null);   // note: page!=NULL (since if p==NULL, we have size=0 and size>=newsize>0
                if (theap != null)   // C: MI_THEAP_INITASNULL
                {
                    if (mi_page_heap(page) == _mi_theap_heap(theap))   // and within the same heap
                    {
                        mi_assert_internal(p != null);
                        if (usable_post != null) { *usable_post = mi_page_usable_block_size(page); }
                        return p;   // reallocation still fits and not more than 50% waste
                    }
                }
            }
            // otherwise allocate a fresh block
            void* newp = mi_theap_umalloc(theap, newsize, usable_post);
            if (newp != null)
            {
                if (zero && newsize > size)
                {
                    // also set last word in the previous allocation to zero to ensure any padding is zero-initialized
                    nuint start = (size >= (nuint)MI_INTPTR_SIZE ? size - (nuint)MI_INTPTR_SIZE : 0);
                    _mi_memzero((byte*)newp + start, newsize - start);
                }
                else if (newsize == 0)
                {
                    ((byte*)newp)[0] = 0;   // work around for applications that expect zero-reallocation to be zero initialized (issue #725)
                }
                if (p != null)
                {
                    nuint copysize = (newsize > size ? size : newsize);
                    _mi_memcpy(newp, p, copysize);
                    mi_free(p);   // only free the original pointer if successful
                }
            }
            return newp;
        }

        public static void* mi_theap_realloc(mi_theap_t* theap, void* p, nuint newsize)
        {
            return _mi_theap_realloc_zero(theap, p, newsize, false, null, null);
        }

        private static void* mi_theap_reallocn(mi_theap_t* theap, void* p, nuint count, nuint size)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_theap_realloc(theap, p, total);
        }

        // Reallocate but free `p` on errors
        private static void* mi_theap_reallocf(mi_theap_t* theap, void* p, nuint newsize)
        {
            void* newp = mi_theap_realloc(theap, p, newsize);
            if (newp == null && p != null) mi_free(p);
            return newp;
        }

        private static void* mi_theap_rezalloc(mi_theap_t* theap, void* p, nuint newsize)
        {
            return _mi_theap_realloc_zero(theap, p, newsize, true, null, null);
        }

        private static void* mi_theap_recalloc(mi_theap_t* theap, void* p, nuint count, nuint size)
        {
            nuint total;
            if (mi_count_size_overflow(count, size, out total)) return null;
            return mi_theap_rezalloc(theap, p, total);
        }

        public static void* mi_realloc(void* p, nuint newsize)
        {
            return mi_theap_realloc(_mi_theap_default(), p, newsize);
        }

        public static void* mi_reallocn(void* p, nuint count, nuint size)
        {
            return mi_theap_reallocn(_mi_theap_default(), p, count, size);
        }

        public static void* mi_urealloc(void* p, nuint newsize, nuint* usable_pre, nuint* usable_post)
        {
            return _mi_theap_realloc_zero(_mi_theap_default(), p, newsize, false, usable_pre, usable_post);
        }

        // Reallocate but free `p` on errors
        public static void* mi_reallocf(void* p, nuint newsize)
        {
            return mi_theap_reallocf(_mi_theap_default(), p, newsize);
        }

        public static void* mi_rezalloc(void* p, nuint newsize)
        {
            return mi_theap_rezalloc(_mi_theap_default(), p, newsize);
        }

        public static void* mi_recalloc(void* p, nuint count, nuint size)
        {
            return mi_theap_recalloc(_mi_theap_default(), p, count, size);
        }

        public static void* mi_heap_realloc(mi_heap_t* heap, void* p, nuint newsize)
        {
            return mi_theap_realloc(_mi_heap_theap(heap), p, newsize);
        }

        public static void* mi_heap_reallocn(mi_heap_t* heap, void* p, nuint count, nuint size)
        {
            return mi_theap_reallocn(_mi_heap_theap(heap), p, count, size);
        }

        // Reallocate but free `p` on errors
        public static void* mi_heap_reallocf(mi_heap_t* heap, void* p, nuint newsize)
        {
            return mi_theap_reallocf(_mi_heap_theap(heap), p, newsize);
        }

        public static void* mi_heap_rezalloc(mi_heap_t* heap, void* p, nuint newsize)
        {
            return mi_theap_rezalloc(_mi_heap_theap(heap), p, newsize);
        }

        public static void* mi_heap_recalloc(mi_heap_t* heap, void* p, nuint count, nuint size)
        {
            return mi_theap_recalloc(_mi_heap_theap(heap), p, count, size);
        }

        // ------------------------------------------------------
        // strdup and strndup (C strings as byte*)
        // ------------------------------------------------------

        private static byte* mi_theap_strdup(mi_theap_t* theap, byte* s)
        {
            if (s == null) return null;
            nuint len = _mi_strlen(s);
            if (len > unchecked((nuint)MI_MAX_ALLOC_SIZE) - 1) return null;   // prevent overflow on len+1
            byte* t = (byte*)mi_theap_malloc(theap, len + 1);
            if (t == null) return null;
            _mi_memcpy(t, s, len);
            t[len] = 0;
            return t;
        }

        public static byte* mi_strdup(byte* s)
        {
            return mi_theap_strdup(_mi_theap_default(), s);
        }

        public static byte* mi_heap_strdup(mi_heap_t* heap, byte* s)
        {
            return mi_theap_strdup(_mi_heap_theap(heap), s);
        }

        private static byte* mi_theap_strndup(mi_theap_t* theap, byte* s, nuint n)
        {
            if (s == null) return null;
            nuint len = _mi_strnlen(s, n);   // len <= n
            if (len > unchecked((nuint)MI_MAX_ALLOC_SIZE) - 1) return null;   // prevent overflow on len+1
            byte* t = (byte*)mi_theap_malloc(theap, len + 1);
            if (t == null) return null;
            _mi_memcpy(t, s, len);
            t[len] = 0;
            return t;
        }

        public static byte* mi_strndup(byte* s, nuint n)
        {
            return mi_theap_strndup(_mi_theap_default(), s, n);
        }

        public static byte* mi_heap_strndup(mi_heap_t* heap, byte* s, nuint n)
        {
            return mi_theap_strndup(_mi_heap_theap(heap), s, n);
        }
    }
}
