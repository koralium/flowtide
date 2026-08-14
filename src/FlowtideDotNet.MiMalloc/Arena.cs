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
// Port of mimalloc v3.4.4 `src/arena.c` -- the memory-management half:
// arena ids, slice allocation (mi_arena_try_alloc_at / _mi_arenas_alloc_aligned),
// arena reserve/creation (mi_manage_os_memory / mi_reserve_os_memory), free,
// and delayed purging.
// Original: Copyright (c) 2019-2026 Microsoft Research, Daan Leijen (MIT license).
//
// The PAGE half of arena.c (_mi_arenas_page_alloc/free/abandon/unabandon and heap
// visiting) is ported together with page.c (task #7) as it forms one concurrency
// unit with the page ownership protocol.
//
// Notes:
//  - `mi_arena_id_t` is `void*` in C; the port uses `mi_arena_t*` style ids via the
//    same conversion helpers (`_mi_arena_id_none()` == null).
//  - custom commit functions (`mi_commit_fun_t`) are not supported: the fields are
//    carried as `void*` and asserted null (Flowtide never sets them).
//  - `errno = ENOMEM` side effects are dropped (no errno in .NET).

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        /* -----------------------------------------------------------
          Arena id's
        ----------------------------------------------------------- */

        public static void* _mi_arena_id_none() => null;

        public static mi_arena_t* _mi_arena_from_id(void* id)
        {
            mi_arena_t* arena = (mi_arena_t*)id;
            mi_assert_internal(arena == null || arena->parent == null);   // id's should never point to sub-arena's
            return arena;
        }

        public static void* mi_arena_id_from_arena(mi_arena_t* arena)
        {
            mi_assert_internal(arena == null || arena->parent == null);
            return arena == null ? _mi_arena_id_none() : (void*)arena;
        }

        private static bool mi_arena_is_suitable(mi_arena_t* arena, mi_arena_t* req_arena)
        {
            if (arena == req_arena) return true;                          // they match
            if (arena == null) return false;
            if (req_arena == null && !arena->is_exclusive) return true;   // or the arena is not exclusive, and we didn't request a specific one
            if (arena->parent != null && arena->parent == req_arena) return true;   // sub-arena? (note that req_arena is never a sub arena)
            return false;
        }

        public static bool _mi_arena_memid_is_suitable(mi_memid_t memid, mi_arena_t* request_arena)
        {
            if (memid.memkind == mi_memkind_t.MI_MEM_ARENA)
            {
                return mi_arena_is_suitable(memid.mem.arena.arena, request_arena);
            }
            else
            {
                return mi_arena_is_suitable(null, request_arena);
            }
        }

        public static nuint mi_arenas_get_count(mi_subproc_t* subproc)
            => mi_atomic_load_relaxed(ref subproc->arena_count);

        public static mi_arena_t* mi_arena_from_index(mi_subproc_t* subproc, nuint idx)
        {
            mi_assert_internal(idx < mi_arenas_get_count(subproc));
            // (arenas[] is a fixed ulong buffer holding atomic mi_arena_t* entries)
            return (mi_arena_t*)mi_atomic_load_acquire(ref *(nuint*)&subproc->arenas[(int)idx]);
        }

        private static nuint mi_arena_info_slices(mi_arena_t* arena) => arena->info_slices;

        public static nuint mi_arena_min_alignment() => MI_ARENA_SLICE_ALIGN;

        public static nuint mi_arena_min_size() => (nuint)MI_ARENA_MIN_SIZE;

        private static nuint mi_arena_max_object_size()
        {
            nuint max_size = mi_option_get_size(mi_option_t.mi_option_arena_max_object_size);
            max_size = _mi_align_up(max_size, MI_ARENA_SLICE_SIZE);
            if (max_size <= MI_ARENA_MIN_OBJ_SIZE)
            {
                return MI_ARENA_MIN_OBJ_SIZE;
            }
            else if (max_size >= unchecked((nuint)(MI_ARENA_MAX_SIZE - (ulong)(MI_BCHUNK_BITS * MI_ARENA_SLICE_SIZE))))   // minus an initial chunk to accommodate meta info
            {
                return unchecked((nuint)(MI_ARENA_MAX_SIZE - (ulong)(MI_BCHUNK_BITS * MI_ARENA_SLICE_SIZE)));
            }
            else
            {
                return max_size;
            }
        }

        private static bool mi_arena_commit(mi_subproc_t* subproc, mi_arena_t* arena, void* start, nuint size, bool* is_zero, nuint already_committed)
        {
            mi_assert_internal(subproc != null);
            // custom commit functions are not supported in the port
            mi_assert_internal(arena == null || arena->commit_fun == null);
            if (already_committed > 0)
            {
                return _mi_os_commit_ex(subproc, start, size, is_zero, already_committed);
            }
            else
            {
                return _mi_os_commit(subproc, start, size, is_zero);
            }
        }

        /* -----------------------------------------------------------
          Util
        ----------------------------------------------------------- */

        // Size of an arena
        private static nuint mi_arena_size(mi_arena_t* arena) => mi_size_of_slices(arena->slice_count);

        // Start of the arena memory area
        private static byte* mi_arena_start(mi_arena_t* arena) => (byte*)arena;

        // Start of a slice
        public static byte* mi_arena_slice_start(mi_arena_t* arena, nuint slice_index)
        {
            mi_assert_internal(slice_index < arena->slice_count);
            return mi_arena_start(arena) + mi_size_of_slices(slice_index);
        }

        public static mi_page_t* mi_arena_page_at_slice(mi_arena_t* arena, nuint slice_index)
        {
            mi_assert_internal(slice_index < arena->slice_count);
            if (arena->pages_meta != null)
            {
                // MI_PAGE_META_ALIGNED_FREE_SMALL == 0: pages_meta always holds the page
                return &arena->pages_meta[slice_index];
            }
            return (mi_page_t*)mi_arena_slice_start(arena, slice_index);
        }

        // Arena area
        public static void* mi_arena_area(void* arena_id, nuint* size)
        {
            if (size != null) *size = 0;
            mi_arena_t* arena = _mi_arena_from_id(arena_id);
            if (arena == null) return null;
            if (size != null)
            {
                mi_assert_internal(mi_size_of_slices(arena->slice_count) <= arena->total_size);
                *size = arena->total_size;
            }
            return mi_arena_start(arena);
        }

        // Create an arena memid
        private static mi_memid_t mi_memid_create_arena(mi_arena_t* arena, nuint slice_index, nuint slice_count)
        {
            mi_assert_internal(slice_index < uint.MaxValue);
            mi_assert_internal(slice_count < uint.MaxValue);
            mi_assert_internal(slice_count > 0);
            mi_assert_internal(slice_index < arena->slice_count);
            mi_memid_t memid = _mi_memid_create(mi_memkind_t.MI_MEM_ARENA);
            memid.mem.arena.arena = arena;
            memid.mem.arena.slice_index = (uint)slice_index;
            memid.mem.arena.slice_count = (uint)slice_count;
            return memid;
        }

        // get the arena and slice span
        private static mi_arena_t* mi_arena_from_memid(mi_memid_t memid, nuint* slice_index, nuint* slice_count)
        {
            mi_assert_internal(memid.memkind == mi_memkind_t.MI_MEM_ARENA);
            mi_arena_t* arena = memid.mem.arena.arena;
            if (slice_index != null) { *slice_index = memid.mem.arena.slice_index; }
            if (slice_count != null) { *slice_count = memid.mem.arena.slice_count; }
            return arena;
        }

        public static nuint mi_page_full_size(mi_page_t* page)
        {
            if (page->memid.memkind == mi_memkind_t.MI_MEM_ARENA)
            {
                return page->memid.mem.arena.slice_count * MI_ARENA_SLICE_SIZE;
            }
            else if (mi_memid_is_os(page->memid) || page->memid.memkind == mi_memkind_t.MI_MEM_EXTERNAL)
            {
                mi_assert_internal((byte*)page->memid.mem.os.@base <= (byte*)page);
                nint presize = (nint)((byte*)page - (byte*)page->memid.mem.os.@base);
                mi_assert_internal((nint)page->memid.mem.os.size >= presize);
                return presize > (nint)page->memid.mem.os.size ? 0 : page->memid.mem.os.size - (nuint)presize;
            }
            else
            {
                return 0;
            }
        }

        /* -----------------------------------------------------------
          Arena Allocation
        ----------------------------------------------------------- */

        private static void* mi_arena_try_alloc_at(mi_arena_t* arena, nuint slice_count, bool commit, nuint tseq, mi_memid_t* memid)
        {
            mi_assert_internal(arena != null);
            mi_assert_internal(slice_count > 0);
            nuint slice_index;
            if (!mi_bbitmap_try_find_and_clearN(arena->slices_free, tseq, slice_count, &slice_index)) return null;

            // claimed it!
            void* p = mi_arena_slice_start(arena, slice_index);
            *memid = mi_memid_create_arena(arena, slice_index, slice_count);
            memid->is_pinned = arena->memid.is_pinned;

            // set the dirty bits and track which slices become accessible
            nuint touched_slices = slice_count;
            if (arena->memid.initially_zero)
            {
                nuint already_dirty = 0;
                memid->initially_zero = mi_bitmap_setN(arena->slices_dirty, slice_index, slice_count, &already_dirty);
                mi_assert_internal(already_dirty <= touched_slices);
                touched_slices -= already_dirty;
            }
            else
            {
                // todo: properly count touched pages with a separate bitmap?
                touched_slices = 0;
            }

            // set commit state
            if (commit)
            {
                // commit requested, but the range may not be committed as a whole: ensure it is committed now
                nuint already_committed = mi_bitmap_popcountN(arena->slices_committed, slice_index, slice_count);
                if (already_committed < slice_count)
                {
                    // not all committed, try to commit now
                    bool commit_zero = false;
                    if (!mi_arena_commit(arena->subproc, arena, p, mi_size_of_slices(slice_count), &commit_zero, mi_size_of_slices(slice_count - already_committed)))
                    {
                        // if the commit fails, release ownership, and return NULL;
                        // note: this does not roll back dirty bits but that is ok.
                        mi_bbitmap_setN(arena->slices_free, slice_index, slice_count);
                        return null;
                    }
                    if (commit_zero)
                    {
                        memid->initially_zero = true;
                    }

                    // set the commit bits
                    mi_bitmap_setN(arena->slices_committed, slice_index, slice_count, null);

#if MI_DEBUG
                    if (memid->initially_zero)
                    {
                        if (!mi_mem_is_zero(p, mi_size_of_slices(slice_count)))
                        {
                            _mi_error_message(EFAULT, "internal error: arena allocation was not zero-initialized!");
                            memid->initially_zero = false;
                        }
                    }
#endif
                }
                else
                {
                    // already fully committed.
                    _mi_os_reuse(arena->subproc, p, mi_size_of_slices(slice_count));
                    // if the OS has overcommit, and this is the first time we access these pages, then
                    // count the commit now (as at arena reserve we didn't count those commits as these are on-demand)
                    if (_mi_os_has_overcommit() && touched_slices > 0 && !arena->memid.is_pinned /* huge pages, issue #1236 */)
                    {
                        __mi_stat_increase_mt(&arena->subproc->stats.committed, mi_size_of_slices(touched_slices));
                    }
                }

                mi_assert_internal(mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count));
                memid->initially_committed = true;
            }
            else
            {
                // no need to commit, but check if it is already fully committed
                memid->initially_committed = mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count);
                if (!memid->initially_committed)
                {
                    // partly committed.. adjust stats
                    nuint already_committed_count = 0;
                    mi_bitmap_setN(arena->slices_committed, slice_index, slice_count, &already_committed_count);
                    mi_bitmap_clearN(arena->slices_committed, slice_index, slice_count);
                    __mi_stat_decrease_mt(&arena->subproc->stats.committed, mi_size_of_slices(already_committed_count));
                }
            }

            mi_assert_internal(mi_bbitmap_is_clearN(arena->slices_free, slice_index, slice_count));
            if (commit) { mi_assert_internal(mi_bitmap_is_setN(arena->slices_committed, slice_index, slice_count)); }
            if (commit) { mi_assert_internal(memid->initially_committed); }
            mi_assert_internal(mi_bitmap_is_setN(arena->slices_dirty, slice_index, slice_count));

            return p;
        }

        // try to reserve a fresh arena space
        private static bool mi_arena_reserve(mi_subproc_t* subproc, nuint req_size, bool allow_large, void** arena_id)
        {
            nuint arena_count = mi_arenas_get_count(subproc);
            if (arena_count > MI_MAX_ARENAS - 4) return false;

            // calc reserve
            nuint arena_reserve = mi_option_get_size(mi_option_t.mi_option_arena_reserve);
            if (arena_reserve == 0) return false;

            if (!_mi_os_has_virtual_reserve())
            {
                arena_reserve = arena_reserve / 4;   // be conservative if virtual reserve is not supported
            }
            arena_reserve = _mi_align_up(arena_reserve, MI_ARENA_SLICE_SIZE);

            if (arena_count >= 1 && arena_count <= 128)
            {
                // scale up the arena sizes exponentially every 8 entries
                nuint multiplier = (nuint)1 << (int)_mi_clamp(arena_count / 8, 0, 16);
                if (!mi_mul_overflow(multiplier, arena_reserve, out nuint reserve))
                {
                    arena_reserve = reserve;
                }
            }

            // try to accommodate the requested size for huge allocations
            req_size = _mi_align_up(req_size + (nuint)MI_ARENA_MAX_CHUNK_OBJ_SIZE, (nuint)MI_ARENA_MAX_CHUNK_OBJ_SIZE);   // over-reserve for meta-info
            if (arena_reserve < req_size)
            {
                arena_reserve = req_size;
            }

            // check arena bounds
            nuint min_reserve = (nuint)MI_ARENA_MIN_SIZE;
            nuint max_reserve = unchecked((nuint)MI_ARENA_MAX_SIZE);   // 16 GiB
            if (arena_reserve < min_reserve)
            {
                arena_reserve = min_reserve;
            }
            else if (arena_reserve > max_reserve)
            {
                arena_reserve = max_reserve;
            }

            // should be able to at least handle the current allocation size
            if (arena_reserve < req_size) return false;

            // commit eagerly?
            bool arena_commit = false;
            bool overcommit = _mi_os_has_overcommit();
            if (mi_option_get(mi_option_t.mi_option_arena_eager_commit) == 2) { arena_commit = overcommit || mi_option_is_enabled(mi_option_t.mi_option_allow_large_os_pages); }
            else if (mi_option_get(mi_option_t.mi_option_arena_eager_commit) == 1) { arena_commit = true; }

            // on an OS with overcommit (Linux) we don't count the commit yet as it is on-demand. Once a slice
            // is actually allocated for the first time it will be counted.
            bool adjust = overcommit && arena_commit;
            if (adjust) { __mi_stat_adjust_decrease_mt(&subproc->stats.committed, arena_reserve); }
            // and try to reserve the arena
            int err = mi_reserve_os_memory_ex2(subproc, arena_reserve, arena_commit, allow_large, false /* exclusive? */, arena_id);
            if (err != 0)
            {
                if (adjust) { __mi_stat_adjust_increase_mt(&subproc->stats.committed, arena_reserve); }   // roll back
                // failed to allocate: try a smaller size arena as fallback?
                nuint small_arena_reserve = 4 * (nuint)MI_ARENA_MIN_SIZE;   // 128 MiB
                if (arena_reserve > small_arena_reserve && small_arena_reserve > req_size)
                {
                    // try again
                    if (adjust) { __mi_stat_adjust_decrease_mt(&subproc->stats.committed, small_arena_reserve); }
                    err = mi_reserve_os_memory_ex2(subproc, small_arena_reserve, arena_commit, allow_large, false /* exclusive? */, arena_id);
                    if (err != 0 && adjust) { __mi_stat_adjust_increase_mt(&subproc->stats.committed, small_arena_reserve); }   // roll back
                }
            }
            return err == 0;
        }

        /* -----------------------------------------------------------
          Arena iteration
        ----------------------------------------------------------- */

        private static bool mi_arena_is_suitable_ex(mi_arena_t* arena, mi_arena_t* req_arena, bool match_numa, int numa_node, bool allow_pinned)
        {
            if (!allow_pinned && arena->memid.is_pinned) return false;
            if (!mi_arena_is_suitable(arena, req_arena)) return false;
            if (req_arena == null)   // if not specific, check numa affinity
            {
                bool numa_suitable = numa_node < 0 || arena->numa_node < 0 || arena->numa_node == numa_node;
                if (match_numa) { if (!numa_suitable) return false; }
                else { if (numa_suitable) return false; }
            }
            return true;
        }

        // determine the start of search; important to keep heaps and threads
        // into their own memory regions to reduce contention.
        private static nuint mi_arena_start_idx(mi_heap_t* heap, nuint tseq, nuint arena_cycle)
        {
            nuint hseq = heap->heap_seq;
            nuint hcount = mi_atomic_load_relaxed(ref heap->subproc->heap_count);
            if (arena_cycle <= 1) return 0;
            if (hseq == 0 || hcount <= 1 || arena_cycle > 0x8FF) return tseq % arena_cycle;   // common for single heap programs

            // spread heaps evenly among arena's, and then evenly for threads in their fraction
            nuint start;
            mi_assert_internal(arena_cycle <= 0x8FF);
            nuint frac = (arena_cycle * 256) / hcount;   // fraction in the arena_cycle; at most: arena_cycle * 0x100
            if (frac == 0)
            {
                // many heaps (> 256 per arena)
                start = hseq % arena_cycle;
            }
            else
            {
                nuint hspot = hseq % hcount;
                start = (frac * hspot) / 256;   // (arena_cycle * (hseq % hcount)) / hcount
                if (frac >= 512)   // at least 2 arena's per heap?
                {
                    start = start + (tseq % (frac / 256));
                }
            }
            mi_assert_internal(start < arena_cycle);
            return start;
        }

        // C: the mi_forall_arenas / mi_forall_suitable_arenas macros as an iterator struct.
        // Search order: if `req_arena` is given, visit only that one; otherwise rotate
        // through the arenas below the last one starting at `mi_arena_start_idx`, then
        // the remaining arenas in order.
        internal struct mi_forall_arenas_iter
        {
            private mi_subproc_t* _subproc;
            private mi_arena_t* _req_arena;
            private nuint _arena_count;
            private nuint _arena_cycle;
            private nuint _start;
            private nuint _i;

            public mi_forall_arenas_iter(mi_heap_t* heap, mi_arena_t* req_arena, nuint tseq)
            {
                _subproc = heap->subproc;
                _req_arena = req_arena;
                _arena_count = mi_arenas_get_count(heap->subproc);
                _arena_cycle = _arena_count == 0 ? 0 : _arena_count - 1;   // first search the arenas below the last one
                _start = mi_arena_start_idx(heap, tseq, _arena_cycle);
                _i = 0;
            }

            public bool TryNext(out mi_arena_t* arena)
            {
                while (_i < _arena_count)
                {
                    if (_req_arena != null)
                    {
                        if (_i > 0) break;   // only once
                        _i++;
                        arena = _req_arena;   // if there is a specific req_arena, only search that one
                        return true;          // (req_arena is never null here)
                    }
                    nuint idx;
                    if (_i < _arena_cycle)
                    {
                        idx = _i + _start;
                        if (idx >= _arena_cycle) { idx -= _arena_cycle; }   // adjust so we rotate through the cycle
                    }
                    else
                    {
                        idx = _i;   // remaining arena's after the cycle
                    }
                    _i++;
                    arena = mi_arena_from_index(_subproc, idx);
                    if (arena != null) return true;
                }
                arena = null;
                return false;
            }

            public bool TryNextSuitable(bool match_numa, int numa_node, bool allow_large, out mi_arena_t* arena)
            {
                while (TryNext(out arena))
                {
                    if (mi_arena_is_suitable_ex(arena, _req_arena, match_numa, numa_node, allow_large)) return true;
                }
                return false;
            }
        }

        /* -----------------------------------------------------------
          Arena allocation
        ----------------------------------------------------------- */

        // allocate slices from the arenas
        private static void* mi_arenas_try_find_free(
            mi_heap_t* heap, nuint slice_count, nuint alignment,
            bool commit, bool allow_large, mi_arena_t* req_arena, nuint tseq, int numa_node, mi_memid_t* memid)
        {
            mi_assert(alignment <= MI_ARENA_SLICE_ALIGN);
            if (alignment > MI_ARENA_SLICE_ALIGN) return null;

            // search arena's
            var iter = new mi_forall_arenas_iter(heap, req_arena, tseq);
            while (iter.TryNextSuitable(true /* only numa matching */, numa_node, allow_large, out mi_arena_t* arena))
            {
                void* p = mi_arena_try_alloc_at(arena, slice_count, commit, tseq, memid);
                if (p != null) return p;
            }
            if (numa_node < 0) return null;

            // search again but now regardless of preferred numa affinity
            iter = new mi_forall_arenas_iter(heap, req_arena, tseq);
            while (iter.TryNextSuitable(false /* numa non-matching now */, numa_node, allow_large, out mi_arena_t* arena))
            {
                void* p = mi_arena_try_alloc_at(arena, slice_count, commit, tseq, memid);
                if (p != null) return p;
            }
            return null;
        }

        // Allocate slices from the arena's -- potentially allocating a fresh arena
        private static void* mi_arenas_try_alloc(
            mi_heap_t* heap,
            nuint slice_count, nuint alignment,
            bool commit, bool allow_large,
            mi_arena_t* req_arena, nuint tseq, int numa_node, mi_memid_t* memid)
        {
            mi_assert(alignment <= MI_ARENA_SLICE_ALIGN);
            void* p;

            // not too large?
            if ((ulong)slice_count * MI_ARENA_SLICE_SIZE > MI_ARENA_MAX_SIZE) return null;

            // try to find free slices in the arena's
            p = mi_arenas_try_find_free(heap, slice_count, alignment, commit, allow_large, req_arena, tseq, numa_node, memid);
            if (p != null) return p;

            // did we need a specific arena?
            if (req_arena != null) return null;

            // don't create arena's while preloading
            if (_mi_preloading()) return null;

            // don't create arena's if OS allocation is disallowed
            if (mi_option_is_enabled(mi_option_t.mi_option_disallow_os_alloc)) return null;

            // otherwise, try to reserve a new arena -- but one thread at a time..
            mi_subproc_t* subproc = heap->subproc;
            nuint arena_count = mi_arenas_get_count(subproc);
            mi_lock_acquire(&subproc->arena_reserve_lock);
            try
            {
                if (arena_count == mi_arenas_get_count(subproc))
                {
                    // we are the first to enter the lock, reserve a fresh arena
                    void* arena_id = _mi_arena_id_none();
                    mi_arena_reserve(subproc, mi_size_of_slices(slice_count), allow_large, &arena_id);
                }
                else
                {
                    // another thread already reserved a new arena
                }
            }
            finally
            {
                mi_lock_release(&subproc->arena_reserve_lock);
            }
            // try once more to allocate in the new arena
            mi_assert_internal(req_arena == null);
            p = mi_arenas_try_find_free(heap, slice_count, alignment, commit, allow_large, req_arena, tseq, numa_node, memid);
            if (p != null) return p;

            return null;
        }

        // Allocate from the OS (if allowed)
        private static void* mi_arena_os_alloc_aligned(
            mi_subproc_t* subproc,
            nuint size, nuint alignment, nuint align_offset,
            bool commit, bool allow_large,
            mi_arena_t* req_arena, mi_memid_t* memid)
        {
            // if we cannot use OS allocation, return NULL
            if (mi_option_is_enabled(mi_option_t.mi_option_disallow_os_alloc) || req_arena != null)
            {
                return null;
            }

            if (align_offset > 0)
            {
                return _mi_os_alloc_aligned_at_offset(subproc, size, alignment, align_offset, commit, allow_large, memid);
            }
            else
            {
                return _mi_os_alloc_aligned(subproc, size, alignment, commit, allow_large, memid);
            }
        }

        // Allocate large sized memory
        public static void* _mi_arenas_alloc_aligned(mi_heap_t* heap,
            nuint size, nuint alignment, nuint align_offset,
            bool commit, bool allow_large,
            mi_arena_t* req_arena, nuint tseq, int numa_node, mi_memid_t* memid)
        {
            mi_assert_internal(memid != null);
            mi_assert_internal(size > 0);

            // try to allocate in an arena if the alignment is small enough and the object is not too small (as for theap meta data)
            if (!mi_option_is_enabled(mi_option_t.mi_option_disallow_arena_alloc) &&                 // is arena allocation allowed?
                size >= MI_ARENA_MIN_OBJ_SIZE && size <= mi_arena_max_object_size() &&               // and not too small or too large
                alignment <= MI_ARENA_SLICE_ALIGN && align_offset == 0)                              // and good alignment
            {
                nuint slice_count = mi_slice_count_of_size(size);
                void* ap = mi_arenas_try_alloc(heap, slice_count, alignment, commit, allow_large, req_arena, tseq, numa_node, memid);
                if (ap != null) return ap;
            }

            // fall back to the OS
            void* p = mi_arena_os_alloc_aligned(heap->subproc, size, alignment, align_offset, commit, allow_large, req_arena, memid);
            return p;
        }

        public static void* _mi_arenas_alloc(mi_heap_t* heap, nuint size, bool commit, bool allow_large, mi_arena_t* req_arena, nuint tseq, int numa_node, mi_memid_t* memid)
        {
            return _mi_arenas_alloc_aligned(heap, size, MI_ARENA_SLICE_SIZE, 0, commit, allow_large, req_arena, tseq, numa_node, memid);
        }

        /* -----------------------------------------------------------
          Arena free
        ----------------------------------------------------------- */

        public static void _mi_arenas_free(mi_subproc_t* subproc, void* p, nuint size, mi_memid_t memid)
        {
            if (p == null) return;
            if (size == 0) return;

            if (mi_memkind_is_os(memid.memkind))
            {
                // was a direct OS allocation, pass through
                _mi_os_free(subproc, p, size, memid);
            }
            else if (memid.memkind == mi_memkind_t.MI_MEM_ARENA)
            {
                // allocated in an arena
                nuint slice_count;
                nuint slice_index;
                mi_arena_t* arena = mi_arena_from_memid(memid, &slice_index, &slice_count);
                mi_assert_internal(arena != null);
                mi_assert_internal(arena->subproc == subproc);
                mi_assert_internal((size % MI_ARENA_SLICE_SIZE) == 0);
                mi_assert_internal(slice_count * MI_ARENA_SLICE_SIZE == size);
                mi_assert_internal(mi_arena_slice_start(arena, slice_index) <= (byte*)p);
                mi_assert_internal(mi_arena_slice_start(arena, slice_index) + mi_size_of_slices(slice_count) > (byte*)p);
                // checks
                if (arena == null)
                {
                    _mi_error_message(EINVAL, $"trying to free from an invalid arena: 0x{(nuint)p:x}, size {size}, memkind: 0x{(int)memid.memkind:x}");
                    return;
                }
                mi_assert_internal(slice_index < arena->slice_count);
                mi_assert_internal(slice_index >= mi_arena_info_slices(arena));
                if (slice_index < mi_arena_info_slices(arena) || slice_index >= arena->slice_count)
                {
                    _mi_error_message(EINVAL, $"trying to free from an invalid arena block: 0x{(nuint)p:x}, size {size}, memkind: 0x{(int)memid.memkind:x}");
                    return;
                }

                // potentially decommit
                if (!arena->memid.is_pinned)
                {
                    // (delay) purge the page
                    mi_arena_schedule_purge(arena, slice_index, slice_count);
                }

                // and make it available to others again
                bool all_inuse = mi_bbitmap_setN(arena->slices_free, slice_index, slice_count);
                if (!all_inuse)
                {
                    _mi_error_message(EAGAIN, $"trying to free an already freed arena block: 0x{(nuint)mi_arena_slice_start(arena, slice_index):x}, size {mi_size_of_slices(slice_count)}");
                    return;
                }
            }
            else if (memid.memkind == mi_memkind_t.MI_MEM_META)
            {
                _mi_meta_free(subproc, p, size, memid);
            }
            else
            {
                // arena was none, external, or static; nothing to do
                mi_assert_internal(mi_memid_needs_no_free(memid));
            }
        }

        // Purge the arenas; if `force_purge` is true, amenable parts are purged even if not yet expired
        public static void _mi_arenas_collect(bool force_purge, bool visit_all, mi_tld_t* tld)
        {
            mi_arenas_try_purge(force_purge, visit_all, tld->subproc, tld->thread_seq);
        }

        // Is a pointer contained in the given arena area?
        private static bool mi_arena_strictly_contains(mi_arena_t* arena, void* p)
        {
            return arena != null &&
                   mi_arena_start(arena) <= (byte*)p &&
                   mi_arena_start(arena) + mi_size_of_slices(arena->slice_count) > (byte*)p;
        }

        // Is a pointer inside any of our arenas?
        private static bool mi_arenas_contain_ex(void* p, mi_arena_t* parent)
        {
            mi_subproc_t* subproc = _mi_subproc();
            nuint max_arena = mi_arenas_get_count(subproc);
            for (nuint i = 0; i < max_arena; i++)
            {
                mi_arena_t* arena = (mi_arena_t*)mi_atomic_load_acquire(ref *(nuint*)&subproc->arenas[(int)i]);
                if (arena != null)
                {
                    if (parent == null || arena == parent || arena->parent == parent)
                    {
                        if (mi_arena_strictly_contains(arena, p))
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        // Is a pointer contained in the given arena area?
        public static bool mi_arena_contains(void* arena_id, void* p)
        {
            mi_arena_t* arena = _mi_arena_from_id(arena_id);
            if (arena == null) return false;
            else if (mi_arena_strictly_contains(arena, p)) return true;
            else return mi_arenas_contain_ex(p, arena);   // maybe a subarena?
        }

        /* -----------------------------------------------------------
          Remove an arena.
        ----------------------------------------------------------- */

        // destroy owned arenas; this is unsafe and should only be done using `mi_option_destroy_on_exit`
        private static void mi_arenas_unsafe_destroy(mi_subproc_t* subproc)
        {
            mi_assert_internal(subproc != null);
            nuint arena_count = mi_arenas_get_count(subproc);
            for (nuint i = 0; i < arena_count; i++)
            {
                mi_arena_t* arena = (mi_arena_t*)mi_atomic_load_acquire(ref *(nuint*)&subproc->arenas[(int)i]);
                if (arena != null)
                {
                    mi_atomic_store_release(ref *(nuint*)&subproc->arenas[(int)i], 0);
                    if (mi_memkind_is_os(arena->memid.memkind))
                    {
                        _mi_os_free_ex(subproc, mi_arena_start(arena), mi_arena_size(arena), true, arena->memid);
                    }
                }
            }
            // try to lower the max arena.
            nuint expected = arena_count;
            mi_atomic_cas_strong_acq_rel(ref subproc->arena_count, ref expected, 0);
        }

        // destroy owned arenas; this is unsafe and should only be done using `mi_option_destroy_on_exit`
        public static void _mi_arenas_unsafe_destroy_all(mi_subproc_t* subproc)
        {
            mi_arenas_unsafe_destroy(subproc);
        }

        /* -----------------------------------------------------------
          Add an arena.
        ----------------------------------------------------------- */

        private static bool mi_arenas_add(mi_subproc_t* subproc, mi_arena_t* arena, void** arena_id)
        {
            mi_assert_internal(arena != null);
            mi_assert_internal(arena->slice_count > 0);
            if (arena_id != null) { *arena_id = _mi_arena_id_none(); }

            // try to find a NULL entry
            nuint count = mi_arenas_get_count(subproc);
            for (nuint i = 0; i < count; i++)
            {
                if (mi_arena_from_index(subproc, i) == null)
                {
                    arena->arena_idx = i;
                    nuint expected = 0;
                    if (mi_atomic_cas_strong_release(ref *(nuint*)&subproc->arenas[(int)i], ref expected, (nuint)arena))
                    {
                        // success
                        if (arena_id != null) { *arena_id = mi_arena_id_from_arena(arena); }
                        return true;
                    }
                }
            }

            // otherwise, try to allocate a fresh slot
            while (count < MI_MAX_ARENAS)
            {
                if (mi_atomic_cas_strong_release(ref subproc->arena_count, ref count, count + 1))
                {
                    arena->arena_idx = count;
                    nuint expected = 0;
                    if (mi_atomic_cas_strong_release(ref *(nuint*)&subproc->arenas[(int)count], ref expected, (nuint)arena))
                    {
                        __mi_stat_counter_increase_mt(&arena->subproc->stats.arena_count, 1);
                        if (arena_id != null) { *arena_id = mi_arena_id_from_arena(arena); }
                        return true;
                    }
                }
            }

            // failed
            arena->arena_idx = 0;
            arena->subproc = null;
            return false;
        }

        public static nuint mi_arena_pages_size(nuint slice_count, nuint* bitmap_base)
        {
            if (slice_count == 0) slice_count = MI_BCHUNK_BITS;
            mi_assert_internal((slice_count % MI_BCHUNK_BITS) == 0);
            nuint base_size = _mi_align_up((nuint)sizeof(mi_arena_pages_t), MI_BCHUNK_SIZE);
            nuint bitmaps_count = 1 + MI_ARENA_BIN_COUNT;   // pages, and abandoned
            nuint bitmaps_size = bitmaps_count * mi_bitmap_size(slice_count, null);
            nuint size = base_size + bitmaps_size;
            if (bitmap_base != null) *bitmap_base = base_size;
            return size;
        }

        private static nuint mi_arena_info_slices_needed(nuint slice_count, nuint* bitmap_base)
        {
            if (slice_count == 0) slice_count = MI_BCHUNK_BITS;
            mi_assert_internal((slice_count % MI_BCHUNK_BITS) == 0);
            nuint base_size = _mi_align_up((nuint)sizeof(mi_arena_t), MI_BCHUNK_SIZE);
            nuint bitmaps_count = 4 + MI_ARENA_BIN_COUNT;   // commit, dirty, purge, pages, and abandoned
            nuint bitmaps_size = bitmaps_count * mi_bitmap_size(slice_count, null) + mi_bbitmap_size(slice_count, null);   // + free
            // MI_PAGE_META_IS_SEPARATED == 1
            nuint pages_size = slice_count * (nuint)sizeof(mi_page_t);
            nuint size = base_size + bitmaps_size + pages_size;

            nuint os_page_size = _mi_os_page_size();
            nuint info_size = _mi_align_up(size, os_page_size) + _mi_os_secure_guard_page_size();
            nuint info_slices = mi_slice_count_of_size(info_size);

            if (bitmap_base != null) *bitmap_base = base_size;
            return info_slices;
        }

        private static mi_bitmap_t* mi_arena_bitmap_init(nuint slice_count, byte** @base)
        {
            mi_bitmap_t* bitmap = (mi_bitmap_t*)(*@base);
            *@base = *@base + mi_bitmap_init(bitmap, slice_count, true /* already zero */);
            return bitmap;
        }

        private static mi_bbitmap_t* mi_arena_bbitmap_init(mi_subproc_t* subproc, nuint slice_count, byte** @base)
        {
            mi_bbitmap_t* bbitmap = (mi_bbitmap_t*)(*@base);
            *@base = *@base + mi_bbitmap_init(subproc, bbitmap, slice_count, true /* already zero */);
            return bbitmap;
        }

        private static mi_arena_t* mi_arena_initialize(mi_subproc_t* subproc, void* start,
            nuint slice_count, mi_arena_t* parent, nuint total_size,
            int numa_node, bool exclusive,
            mi_memid_t memid, void* commit_fun, void* commit_fun_arg, void** arena_id)
        {
            mi_assert_internal(_mi_is_aligned(start, MI_ARENA_SLICE_ALIGN));
            mi_assert_internal(mi_size_of_slices(slice_count) >= (nuint)MI_ARENA_MIN_SIZE);
            mi_assert_internal(commit_fun == null);   // custom commit functions unsupported in the port

            if (slice_count > MI_BITMAP_MAX_BIT_COUNT)   // 16 GiB for now
            {
                // note: this should never happen if called from `mi_manage_os_memory` (as that allocates sub-arenas when needed)
                _mi_warning_message($"cannot use OS memory since it is too large (size {mi_size_of_slices(slice_count) / MI_MiB} MiB, maximum is {mi_size_of_slices(MI_BITMAP_MAX_BIT_COUNT) / MI_MiB} MiB)");
                return null;
            }

            nuint bitmap_base;
            nuint info_slices = mi_arena_info_slices_needed(slice_count, &bitmap_base);
            if (slice_count < info_slices + 1)
            {
                _mi_warning_message($"cannot use OS memory since it is not large enough (size {mi_size_of_slices(slice_count) / MI_KiB} KiB, minimum required is {mi_size_of_slices(info_slices + 1) / MI_KiB} KiB)");
                return null;
            }

            mi_arena_t* arena = (mi_arena_t*)start;

            // commit & zero if needed
            if (!memid.initially_committed)
            {
                nuint commit_size = mi_size_of_slices(info_slices);
                // leave a guard OS page decommitted at the end? (MI_SECURE == 0: guard size is 0)
                if (!memid.is_pinned) { commit_size -= _mi_os_secure_guard_page_size(); }
                if (!_mi_os_commit(subproc, arena, commit_size, null))
                {
                    _mi_warning_message("unable to commit meta-data for OS memory");
                    return null;
                }
            }
            else if (!memid.is_pinned)
            {
                // if MI_SECURE, set a guard page at the end of the arena info (no-op here)
                _mi_os_secure_guard_page_set_before(subproc, (byte*)arena + mi_size_of_slices(info_slices), memid);
            }
            if (!memid.initially_zero)
            {
                _mi_memzero(arena, mi_size_of_slices(info_slices) - _mi_os_secure_guard_page_size());
            }

            // init
            arena->subproc = subproc;
            arena->memid = memid;
            arena->is_exclusive = exclusive;
            arena->slice_count = slice_count;
            arena->info_slices = info_slices;
            if (numa_node < 0 && mi_option_is_enabled(mi_option_t.mi_option_arena_is_numa_local))
            {
                arena->numa_node = _mi_os_numa_node();
            }
            else
            {
                arena->numa_node = numa_node;
            }
            arena->purge_expire = 0;
            arena->commit_fun = commit_fun;
            arena->commit_fun_arg = commit_fun_arg;
            arena->parent = parent;
            arena->total_size = total_size;

            // init bitmaps
            byte* @base = mi_arena_start(arena) + bitmap_base;
            arena->slices_free = mi_arena_bbitmap_init(subproc, slice_count, &@base);
            arena->slices_committed = mi_arena_bitmap_init(slice_count, &@base);
            arena->slices_dirty = mi_arena_bitmap_init(slice_count, &@base);
            arena->slices_purge = mi_arena_bitmap_init(slice_count, &@base);
            arena->pages_main.pages = mi_arena_bitmap_init(slice_count, &@base);
            for (int i = 0; i < MI_ARENA_BIN_COUNT; i++)
            {
                // pages_abandoned is a fixed ulong buffer of mi_bitmap_t* entries
                arena->pages_main.pages_abandoned[i] = (ulong)mi_arena_bitmap_init(slice_count, &@base);
            }
            // MI_PAGE_META_IS_SEPARATED == 1
            arena->pages_meta = (mi_page_t*)@base;
            @base += slice_count * (nuint)sizeof(mi_page_t);
            mi_assert_internal(mi_size_of_slices(info_slices) >= (nuint)(@base - mi_arena_start(arena)));

            // reserve our meta info (and reserve slices outside the memory area)
            mi_bbitmap_unsafe_setN(arena->slices_free, info_slices /* start */, arena->slice_count - info_slices);
            if (memid.initially_committed)
            {
                mi_bitmap_unsafe_setN(arena->slices_committed, 0, arena->slice_count);
            }
            if (!memid.initially_zero)
            {
                mi_bitmap_unsafe_setN(arena->slices_dirty, 0, arena->slice_count);
            }

            if (!mi_arenas_add(subproc, arena, arena_id)) { return null; }
            return arena;
        }

        private static bool mi_manage_os_memory_ex2(mi_subproc_t* subproc, void* start, nuint size, int numa_node, bool exclusive,
            mi_memid_t memid, void* commit_fun, void* commit_fun_arg, void** arena_id)
        {
            // checks
            mi_assert(start != null);
            if (arena_id != null) { *arena_id = _mi_arena_id_none(); }
            if (start == null) return false;
            if (!_mi_is_aligned(start, MI_ARENA_SLICE_SIZE))
            {
                // we can align the start since the memid tracks the real base of the memory.
                void* aligned_start = _mi_align_up_ptr(start, MI_ARENA_SLICE_SIZE);
                nuint diff = (nuint)((byte*)aligned_start - (byte*)start);
                if (diff >= size || (size - diff) < MI_ARENA_SLICE_SIZE)
                {
                    _mi_warning_message($"after alignment, the size of the arena becomes too small (memory at 0x{(nuint)start:x} with size {size})");
                    return false;
                }
                start = aligned_start;
                size = size - diff;
            }

            // allocate enough arena's to span the full memory area
            // the first arena is the owner, the rest are "sub-arena" (with `parent` pointing to the first one)
            nuint total_slice_count = _mi_align_down(size / MI_ARENA_SLICE_SIZE, MI_BCHUNK_BITS);
            nuint total_size = mi_size_of_slices(total_slice_count);
            if (total_size < (nuint)MI_ARENA_MIN_SIZE)
            {
                _mi_warning_message($"cannot use OS memory since it is not large enough (size {size / MI_KiB} KiB, minimum required is {MI_ARENA_MIN_SIZE / 1024} KiB)");
                return false;
            }

            mi_arena_t* parent = null;
            do
            {
                // counting down on the total_slice_count
                nuint slice_count = total_slice_count;
                if (slice_count > MI_BITMAP_MAX_BIT_COUNT)   // 16 GiB for now (with 64KiB slices)
                {
                    slice_count = MI_BITMAP_MAX_BIT_COUNT;
                }

                // initialize
                mi_arena_t* arena = mi_arena_initialize(subproc, start, slice_count, parent,
                                                        parent == null ? total_size : 0, numa_node, exclusive,
                                                        memid, commit_fun, commit_fun_arg,
                                                        parent == null ? arena_id : null);
                if (arena == null)
                {
                    // failed to initialize due to failing commit or too many arena's
                    if (parent == null)
                    {
                        return false;
                    }
                    else
                    {
                        // partial success, but failed to use the full area..
                        mi_assert(mi_size_of_slices(total_slice_count) <= parent->total_size);
                        parent->total_size -= mi_size_of_slices(total_slice_count);
                        return true;
                    }
                }

                // success
                if (parent == null)
                {
                    parent = arena;
                    memid.memkind = mi_memkind_t.MI_MEM_NONE;
                }
                mi_assert(slice_count <= total_slice_count);
                total_slice_count -= slice_count;
                start = (byte*)start + mi_size_of_slices(slice_count);
            }
            while (total_slice_count > 0);

            return true;
        }

        public static bool mi_manage_os_memory_ex(void* start, nuint size, bool is_committed, bool is_pinned, bool is_zero, int numa_node, bool exclusive, void** arena_id)
        {
            mi_memid_t memid = _mi_memid_create(mi_memkind_t.MI_MEM_EXTERNAL);
            memid.mem.os.@base = start;
            memid.mem.os.size = size;
            memid.initially_committed = is_committed;
            memid.initially_zero = is_zero;
            memid.is_pinned = is_pinned;
            return mi_manage_os_memory_ex2(_mi_subproc(), start, size, numa_node, exclusive, memid, null, null, arena_id);
        }

        // Reserve a range of regular OS memory
        private static int mi_reserve_os_memory_ex2(mi_subproc_t* subproc, nuint size, bool commit, bool allow_large, bool exclusive, void** arena_id)
        {
            if (arena_id != null) *arena_id = _mi_arena_id_none();
            if (size <= unchecked((nuint)MI_MAX_ALLOC_SIZE))
            {
                size = _mi_align_up(size, MI_ARENA_SLICE_SIZE);   // at least one slice
            }
            if (size > unchecked((nuint)MI_MAX_ALLOC_SIZE))
            {
                _mi_error_message(EOVERFLOW, $"memory reservation request is too large (size {size})");
                return ENOMEM;
            }
            mi_memid_t memid;
            void* start = _mi_os_alloc_aligned(subproc, size, MI_ARENA_SLICE_ALIGN, commit, allow_large, &memid);
            if (start == null) return ENOMEM;
            if (!mi_manage_os_memory_ex2(subproc, start, size, -1 /* numa node */, exclusive, memid, null, null, arena_id))
            {
                _mi_os_free_ex(subproc, start, size, commit, memid);
                _mi_verbose_message($"failed to reserve {_mi_divide_up(size, 1024)} KiB memory");
                return ENOMEM;
            }
            _mi_verbose_message($"reserved {_mi_divide_up(size, 1024)} KiB memory{(memid.is_pinned ? " (in large os pages)" : "")}");

            return 0;
        }

        // Reserve a range of regular OS memory
        public static int mi_reserve_os_memory_ex(nuint size, bool commit, bool allow_large, bool exclusive, void** arena_id)
        {
            return mi_reserve_os_memory_ex2(_mi_subproc(), size, commit, allow_large, exclusive, arena_id);
        }

        // Manage a range of regular OS memory
        public static bool mi_manage_os_memory(void* start, nuint size, bool is_committed, bool is_large, bool is_zero, int numa_node)
        {
            return mi_manage_os_memory_ex(start, size, is_committed, is_large, is_zero, numa_node, false /* exclusive? */, null);
        }

        // Reserve a range of regular OS memory
        public static int mi_reserve_os_memory(nuint size, bool commit, bool allow_large)
        {
            return mi_reserve_os_memory_ex(size, commit, allow_large, false, null);
        }

        /* -----------------------------------------------------------
          Reserve a huge page arena. (huge pages unsupported: fails with ENOMEM)
        ----------------------------------------------------------- */

        public static int mi_reserve_huge_os_pages_at_ex(nuint pages, int numa_node, nuint timeout_msecs, bool exclusive, void** arena_id)
        {
            if (arena_id != null) *arena_id = null;
            if (pages == 0) return 0;
            if (numa_node < -1) numa_node = -1;
            if (numa_node >= 0) numa_node = numa_node % _mi_os_numa_node_count();
            mi_subproc_t* subproc = _mi_subproc();
            nuint hsize = 0;
            nuint pages_reserved = 0;
            mi_memid_t memid;
            void* p = _mi_os_alloc_huge_os_pages(subproc, pages, numa_node, (long)timeout_msecs, &pages_reserved, &hsize, &memid);
            if (p == null || pages_reserved == 0)
            {
                _mi_warning_message($"failed to reserve {pages} GiB huge pages");
                return ENOMEM;
            }
            _mi_verbose_message($"numa node {numa_node}: reserved {pages_reserved} GiB huge pages (of the {pages} GiB requested)");

            if (!mi_manage_os_memory_ex2(subproc, p, hsize, numa_node, exclusive, memid, null, null, arena_id))
            {
                _mi_os_free(subproc, p, hsize, memid);
                return ENOMEM;
            }
            return 0;
        }

        public static int mi_reserve_huge_os_pages_at(nuint pages, int numa_node, nuint timeout_msecs)
        {
            return mi_reserve_huge_os_pages_at_ex(pages, numa_node, timeout_msecs, false, null);
        }

        /* -----------------------------------------------------------
          Arena purge
        ----------------------------------------------------------- */

        private static long mi_arena_purge_delay()
        {
            // <0 = no purging allowed, 0=immediate purging, >0=milli-second delay
            long delay = mi_option_get(mi_option_t.mi_option_purge_delay);
            long mult = mi_option_get(mi_option_t.mi_option_arena_purge_mult);
            if (delay < 0 || mult < 0) { return -1; }
            if (delay == 0 || mult == 0) { return 0; }
            if (mi_mul_overflow((nuint)delay, (nuint)mult, out nuint total)) { return delay; }
            if (total > long.MaxValue) { return delay; }
            return (long)total;
        }

        // reset or decommit in an arena and update the commit bitmap
        // assumes we own the area (i.e. slices_free is claimed by us)
        // returns if the memory is no longer committed (versus reset which keeps the commit)
        private static bool mi_arena_purge(mi_arena_t* arena, nuint slice_index, nuint slice_count)
        {
            mi_assert_internal(!arena->memid.is_pinned);
            mi_assert_internal(mi_bbitmap_is_clearN(arena->slices_free, slice_index, slice_count));

            nuint size = mi_size_of_slices(slice_count);
            void* p = mi_arena_slice_start(arena, slice_index);
            nuint already_committed;
            mi_bitmap_setN(arena->slices_committed, slice_index, slice_count, &already_committed);   // pretend all committed..
            bool all_committed = already_committed == slice_count;
            bool needs_recommit = _mi_os_purge_ex(arena->subproc, p, size, all_committed /* allow reset? */, mi_size_of_slices(already_committed), arena->commit_fun, arena->commit_fun_arg);

            if (needs_recommit)
            {
                // no longer committed
                mi_bitmap_clearN(arena->slices_committed, slice_index, slice_count);
            }
            else if (!all_committed)
            {
                // we cannot assume any of these are committed any longer (even with reset since we did setN and may have marked uncommitted slices as committed)
                mi_bitmap_clearN(arena->slices_committed, slice_index, slice_count);
            }

            return needs_recommit;
        }

        // Schedule a purge. This is usually delayed to avoid repeated decommit/commit calls.
        // Note: assumes we (still) own the area as we may purge immediately
        private static void mi_arena_schedule_purge(mi_arena_t* arena, nuint slice_index, nuint slice_count)
        {
            long delay = mi_arena_purge_delay();
            if (arena->memid.is_pinned || delay < 0 || _mi_preloading()) return;   // is purging allowed at all?

            mi_assert_internal(mi_bbitmap_is_clearN(arena->slices_free, slice_index, slice_count));
            if (delay == 0)
            {
                // purge directly
                mi_arena_purge(arena, slice_index, slice_count);
            }
            else
            {
                // schedule purge
                long expire = _mi_clock_now() + delay;
                long expire0 = 0;
                if (mi_atomic_casi64_strong_acq_rel(ref arena->purge_expire, ref expire0, expire))
                {
                    // expiration was not yet set
                    // maybe set the global arenas expire as well (if it wasn't set already)
                    mi_assert_internal(expire0 == 0);
                    mi_atomic_casi64_strong_acq_rel(ref arena->subproc->purge_expire, ref expire0, expire);
                }
                else
                {
                    // already an expiration was set
                }
                mi_bitmap_setN(arena->slices_purge, slice_index, slice_count, null);
            }
        }

        private struct mi_purge_visit_info_t
        {
            public long now;
            public long delay;
            public bool all_purged;
            public bool any_purged;
        }

        private static bool mi_arena_try_purge_range(mi_arena_t* arena, nuint slice_index, nuint slice_count)
        {
            mi_assert(slice_count < MI_BCHUNK_BITS);
            if (mi_bbitmap_try_clearNC(arena->slices_free, slice_index, slice_count))
            {
                // purge
                bool decommitted = mi_arena_purge(arena, slice_index, slice_count);
                mi_assert_internal(!decommitted || mi_bitmap_is_clearN(arena->slices_committed, slice_index, slice_count));
                // and reset the free range
                mi_bbitmap_setN(arena->slices_free, slice_index, slice_count);
                return true;
            }
            else
            {
                // was allocated again already
                return false;
            }
        }

        private static bool mi_arena_try_purge_visitor(nuint slice_index, nuint slice_count, mi_arena_t* arena, void* arg)
        {
            mi_purge_visit_info_t* vinfo = (mi_purge_visit_info_t*)arg;
            // try to purge: first claim the free blocks
            if (mi_arena_try_purge_range(arena, slice_index, slice_count))
            {
                vinfo->any_purged = true;
                vinfo->all_purged = true;
            }
            else if (slice_count > 1)
            {
                // failed to claim the full range, try per slice instead
                for (nuint i = 0; i < slice_count; i++)
                {
                    bool purged = mi_arena_try_purge_range(arena, slice_index + i, 1);
                    vinfo->any_purged = vinfo->any_purged || purged;
                    vinfo->all_purged = vinfo->all_purged && purged;
                }
            }
            // don't clear the purge bits as that is done atomically by the _bitmap_forall_setc_ranges
            return true;   // continue
        }

        // returns
        // -1 = nothing was purged
        // 0  = nothing was purged yet because have not yet reached the expire time
        // 1  = some pages in the arena were purged
        private static int mi_arena_try_purge(mi_arena_t* arena, long now, bool force)
        {
            // check pre-conditions
            if (arena->memid.is_pinned) return -1;

            // expired yet?
            long expire = mi_atomic_loadi64_relaxed(ref arena->purge_expire);
            if (!force)
            {
                if (expire == 0) return -1;
                if (expire > now) return 0;
            }

            // reset expire
            mi_atomic_storei64_release(ref arena->purge_expire, 0);
            __mi_stat_counter_increase_mt(&arena->subproc->stats.arena_purges, 1);

            // go through all purge info's (with max MI_BFIELD_BITS ranges at a time)
            // this also clears those ranges atomically (so any newly freed blocks will get purged next time around)
            mi_purge_visit_info_t vinfo;
            vinfo.now = now;
            vinfo.delay = mi_arena_purge_delay();
            vinfo.all_purged = true;
            vinfo.any_purged = false;

            // we purge by at least `minslices` to not fragment transparent huge pages for example
            nuint minslices = mi_slice_count_of_size(_mi_os_minimal_purge_size());
            _mi_bitmap_forall_setc_rangesn(arena->slices_purge, minslices, &mi_arena_try_purge_visitor, arena, &vinfo);

            return vinfo.any_purged ? 1 : -1;
        }

        private static nuint mi_arenas_purge_guard;   // C: static mi_atomic_guard_t purge_guard

        public static void mi_arenas_try_purge(bool force, bool visit_all, mi_subproc_t* subproc, nuint tseq)
        {
            // try purge can be called often so try to only run when needed
            long delay = mi_arena_purge_delay();
            if (_mi_preloading() || delay <= 0) return;   // nothing will be scheduled

            // check if any arena needs purging?
            long now = _mi_clock_now();
            long arenas_expire = mi_atomic_loadi64_acquire(ref subproc->purge_expire);
            if (!visit_all && !force && (arenas_expire == 0 || arenas_expire > now)) return;

            nuint max_arena = mi_arenas_get_count(subproc);
            if (max_arena == 0) return;

            // allow only one thread to purge at a time (C: the mi_atomic_guard macro)
            nuint guard_expected = 0;
            if (mi_atomic_cas_strong_acq_rel(ref mi_arenas_purge_guard, ref guard_expected, 1))
            {
                try
                {
                    // increase global expire: at most one purge per delay cycle
                    if (arenas_expire > now) { mi_atomic_storei64_release(ref subproc->purge_expire, now + (delay / 10)); }
                    nuint arena_start = tseq % max_arena;
                    nuint max_purge_count = (visit_all ? max_arena : (max_arena / 4) + 1);
                    bool all_visited = true;
                    bool any_purged = false;
                    for (nuint _i = 0; _i < max_arena; _i++)
                    {
                        nuint i = _i + arena_start;
                        if (i >= max_arena) { i -= max_arena; }
                        mi_arena_t* arena = mi_arena_from_index(subproc, i);
                        if (arena != null)
                        {
                            int purged = mi_arena_try_purge(arena, now, force);
                            if (purged >= 0)   // purged, or arena expire is not yet reached
                            {
                                any_purged = true;
                                if (purged >= 1)   // purged
                                {
                                    if (max_purge_count <= 1)
                                    {
                                        all_visited = false;
                                        break;
                                    }
                                    max_purge_count--;
                                }
                            }
                        }
                    }
                    if (all_visited && !any_purged)
                    {
                        mi_atomic_storei64_release(ref subproc->purge_expire, 0);
                    }
                }
                finally
                {
                    mi_atomic_store_release(ref mi_arenas_purge_guard, 0);
                }
            }
        }
    }
}
