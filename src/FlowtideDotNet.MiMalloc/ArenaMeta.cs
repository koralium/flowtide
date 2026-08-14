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
// Port of mimalloc v3.4.4 `src/arena-meta.c` -- the "mini" allocator for meta-data
// (theaps, tlds) that reuses 64b-block bitmaps inside a 64 KiB arena slice.
// Original: Copyright (c) 2019-2024 Microsoft Research, Daan Leijen (MIT license).
//
// MI_SECURE == 0: `_mi_os_secure_guard_page_size()` is 0, so all guard-page offsets
// vanish (kept in the code to mirror the C).

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    // a meta-data page (never freed once allocated)
    internal unsafe struct mi_meta_page_t
    {
        public mi_meta_page_t* next;      // atomic; a linked list of meta-data pages (never released)
        public mi_memid_t memid;          // provenance of the meta-page memory itself
        public mi_bbitmap_t blocks_free;  // a small bitmap with 1 bit per block (sized for MI_META_BLOCKS_PER_PAGE bits)
    }

    internal static unsafe partial class Mi
    {
        public const nuint MI_META_PAGE_SIZE = MI_ARENA_SLICE_SIZE;
        public const nuint MI_META_PAGE_ALIGN = MI_ARENA_SLICE_ALIGN;

        // large enough such that META_MAX_SIZE > 4k
        public const nuint MI_META_BLOCK_SIZE = 1 << (16 - MI_BCHUNK_BITS_SHIFT);        // 128 on 64-bit
        public const nuint MI_META_BLOCK_ALIGN = MI_META_BLOCK_SIZE;
        public const nuint MI_META_BLOCKS_PER_PAGE = MI_META_PAGE_SIZE / MI_META_BLOCK_SIZE;   // 512
        public const nuint MI_META_MAX_SIZE = MI_BCHUNK_SIZE * MI_META_BLOCK_SIZE;       // 8 KiB

        private static mi_meta_page_t* mi_meta_page_next(mi_meta_page_t* mpage)
        {
            return mi_atomic_load_ptr_acquire(&mpage->next);
        }

        private static void* mi_meta_block_start(mi_meta_page_t* mpage, nuint block_idx)
        {
            mi_assert_internal(_mi_is_aligned((byte*)mpage - _mi_os_secure_guard_page_size(), MI_META_PAGE_ALIGN));
            mi_assert_internal(block_idx < MI_META_BLOCKS_PER_PAGE);
            void* p = (byte*)mpage - _mi_os_secure_guard_page_size() + (block_idx * MI_META_BLOCK_SIZE);
            return p;
        }

        // allocate a fresh meta page and add it to the global list.
        private static mi_meta_page_t* mi_meta_page_zalloc(mi_subproc_t* subproc)
        {
            mi_assert_internal(subproc != null);
            // note: C asserts subproc->heap_main != NULL; the heap wiring lands with the
            // init.c port (task #8) -- until then the arena alloc below returns null and
            // callers fall back to OS allocation.
            // allocate a fresh arena slice
            mi_memid_t memid;
            byte* @base = (byte*)_mi_arenas_alloc_aligned(subproc->heap_main, MI_META_PAGE_SIZE, MI_META_PAGE_ALIGN, 0,
                                                          true /* commit*/, true /* allow large? (MI_SECURE==0) */,
                                                          null /* req arena */, 0 /* thread_seq */, -1 /* numa node */, &memid);
            if (@base == null) return null;
            mi_assert_internal(_mi_is_aligned(@base, MI_META_PAGE_ALIGN));
            if (!memid.initially_zero)
            {
                _mi_memzero_aligned(@base, MI_ARENA_SLICE_SIZE);
            }

            // (guard pages: MI_SECURE == 0, nothing to do)

            // initialize the page and free block bitmap
            mi_meta_page_t* mpage = (mi_meta_page_t*)(@base + _mi_os_secure_guard_page_size());
            mpage->memid = memid;
            mi_bbitmap_init(subproc, &mpage->blocks_free, MI_META_BLOCKS_PER_PAGE, true /* already_zero */);
            nuint blocks_free_offset = (nuint)((byte*)&mpage->blocks_free - (byte*)mpage);   // C: offsetof(mi_meta_page_t, blocks_free)
            nuint mpage_size = blocks_free_offset + mi_bbitmap_size(MI_META_BLOCKS_PER_PAGE, null);
            nuint info_blocks = _mi_divide_up(mpage_size, MI_META_BLOCK_SIZE);
            nuint guard_blocks = _mi_divide_up(_mi_os_secure_guard_page_size(), MI_META_BLOCK_SIZE);
            mi_assert_internal(info_blocks + 2 * guard_blocks < MI_META_BLOCKS_PER_PAGE);
            mi_bbitmap_unsafe_setN(&mpage->blocks_free, info_blocks + guard_blocks, MI_META_BLOCKS_PER_PAGE - info_blocks - 2 * guard_blocks);

            // push atomically in front of the meta page list
            // (note: there is no ABA issue since we never free meta-pages)
            mi_meta_page_t* old = mi_atomic_load_ptr_acquire(&subproc->meta_pages);
            do
            {
                mi_atomic_store_ptr_release(&mpage->next, old);
            } while (!mi_atomic_cas_ptr_weak_acq_rel(&subproc->meta_pages, &old, mpage));
            return mpage;
        }

        // allocate meta-data
        public static void* _mi_meta_zalloc(mi_subproc_t* subproc, nuint size, mi_memid_t* pmemid)
        {
            mi_assert_internal(pmemid != null);
            mi_assert_internal(subproc != null);
            size = _mi_align_up(size, MI_META_BLOCK_SIZE);
            if (size == 0 || size > MI_META_MAX_SIZE) return null;
            nuint block_count = _mi_divide_up(size, MI_META_BLOCK_SIZE);
            mi_assert_internal(block_count > 0 && block_count < MI_BCHUNK_BITS);
            mi_meta_page_t* mpage0 = mi_atomic_load_ptr_acquire(&subproc->meta_pages);
            mi_meta_page_t* mpage = mpage0;
            while (mpage != null)
            {
                nuint block_idx;
                if (mi_bbitmap_try_find_and_clearN(&mpage->blocks_free, 0, block_count, &block_idx))
                {
                    // found and claimed `block_count` blocks
                    *pmemid = _mi_memid_create_meta(mpage, block_idx, block_count);
                    return mi_meta_block_start(mpage, block_idx);
                }
                else
                {
                    mpage = mi_meta_page_next(mpage);
                }
            }
            // failed to find space in existing pages
            if (mi_atomic_load_ptr_acquire(&subproc->meta_pages) != mpage0)
            {
                // the page list was updated by another thread in the meantime, retry
                return _mi_meta_zalloc(subproc, size, pmemid);
            }
            // otherwise, allocate a fresh metapage and try once more
            mpage = mi_meta_page_zalloc(subproc);
            if (mpage != null)
            {
                nuint block_idx;
                if (mi_bbitmap_try_find_and_clearN(&mpage->blocks_free, 0, block_count, &block_idx))
                {
                    // found and claimed `block_count` blocks
                    *pmemid = _mi_memid_create_meta(mpage, block_idx, block_count);
                    return mi_meta_block_start(mpage, block_idx);
                }
            }
            // if all this failed, allocate from the OS
            return _mi_os_zalloc(subproc, size, pmemid);
        }

        // free meta-data
        public static void _mi_meta_free(mi_subproc_t* subproc, void* p, nuint size, mi_memid_t memid)
        {
            if (p == null) return;
            if (memid.memkind == mi_memkind_t.MI_MEM_META)
            {
                mi_assert_internal(_mi_divide_up(size, MI_META_BLOCK_SIZE) == memid.mem.meta.block_count);
                nuint block_count = memid.mem.meta.block_count;
                nuint block_idx = memid.mem.meta.block_index;
                mi_meta_page_t* mpage = memid.mem.meta.meta_page;
                mi_assert_internal(block_idx + block_count <= MI_META_BLOCKS_PER_PAGE);
                mi_assert_internal(mi_bbitmap_is_clearN(&mpage->blocks_free, block_idx, block_count));
                // we zero on free (and on the initial page allocation) so we don't need a "dirty" map
                _mi_memzero_aligned(mi_meta_block_start(mpage, block_idx), block_count * MI_META_BLOCK_SIZE);
                mi_bbitmap_setN(&mpage->blocks_free, block_idx, block_count);
            }
            else
            {
                _mi_arenas_free(subproc, p, size, memid);
            }
        }

        // used for debug output
        public static bool _mi_meta_is_meta_page(mi_subproc_t* subproc, void* p)
        {
            mi_meta_page_t* mpage = mi_atomic_load_ptr_acquire(&subproc->meta_pages);
            while (mpage != null)
            {
                if ((void*)mpage == p) return true;
                mpage = mi_meta_page_next(mpage);
            }
            return false;
        }

        // note: `_mi_arenas_alloc_aligned` / `_mi_arenas_free` live in Arena.cs
    }
}
