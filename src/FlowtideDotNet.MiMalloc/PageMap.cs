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
// Port of mimalloc v3.4.4 `src/page-map.c` (two-level page map; MI_PAGE_MAP_FLAT == 0)
// plus the page-map inlines from `include/mimalloc/internal.h`.
// Original: Copyright (c) 2023-2026 Microsoft Research, Daan Leijen (MIT license).
//
// The page-map maps any address to its owning `mi_page_t*`:
//   level 1: `_mi_page_map[idx]` -- 2^19 submap pointers (4 MiB, committed on demand
//            in 64 parts tracked by the `mi_page_map_commit` bfield)
//   level 2: submap of 2^13 `mi_page_t*` entries; each entry covers one 64 KiB slice
//            (one submap covers 512 MiB of address space)
//
// C globals: `_mi_page_map` is a PLAIN pointer (not atomic) to an array of atomic
// submap pointers; the C# equivalents are static fields (Interlocked/Volatile on
// static fields is safe; the pointed-to arrays are native memory).

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // A 2-level page map
        public const int MI_PAGE_MAP_SUB_SHIFT = 13;
        public const nuint MI_PAGE_MAP_SUB_COUNT = (nuint)1 << MI_PAGE_MAP_SUB_SHIFT;                 // 8192 entries
        public const int MI_PAGE_MAP_SHIFT = MI_MAX_VABITS - MI_PAGE_MAP_SUB_SHIFT - MI_ARENA_SLICE_SHIFT;   // 19
        public const nuint MI_PAGE_MAP_COUNT = (nuint)1 << MI_PAGE_MAP_SHIFT;                          // 2^19 submaps

        private const nuint MI_PAGE_MAP_SUB_SIZE = MI_PAGE_MAP_SUB_COUNT * 8;                          // bytes (sizeof(mi_page_t*) == 8)
        private const nuint MI_PAGE_MAP_ENTRIES_PER_CBIT = MI_PAGE_MAP_COUNT < MI_BFIELD_BITS ? 1 : MI_PAGE_MAP_COUNT / MI_BFIELD_BITS;

        // Use an initial empty page map so `free(NULL)` works even if mimalloc is not yet
        // initialized (issue #1341). In C these are static arrays; here a tiny one-time
        // native allocation: one submap entry (NULL) + one map entry pointing at it.
        private static mi_page_t*** mi_page_map_empty_create()
        {
            // [0] = the map entry (points to the empty submap), [1] = the empty submap entry (null)
            var mem = (void**)NativeMemory.AllocZeroed(2 * (nuint)sizeof(void*));
            mem[0] = &mem[1];   // map[0] -> empty submap
            mem[1] = null;      // submap[0] == NULL so _mi_ptr_page(NULL) == NULL
            return (mi_page_t***)mem;
        }

        // C: _mi_page_map (plain pointer to atomic submap entries); stored as nint because
        // Volatile.Read/Write generics reject pointer types
        private static nint _mi_page_map_field = (nint)mi_page_map_empty_create();
        private static readonly nint mi_page_map_empty_field = _mi_page_map_field;    // remember the empty map
        private static nuint _mi_page_map_max_address;    // atomic void* (as nuint)
        private static nuint mi_page_map_count;
        private static mi_memid_t mi_page_map_memid;
        private static mi_lock_t mi_page_map_lock;

        // divide the main map in 64 (`MI_BFIELD_BITS`) parts and commit those parts on demand
        private static nuint mi_page_map_commit;   // atomic mi_bfield_t

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_page_t*** mi_page_map_get() => (mi_page_t***)Volatile.Read(ref _mi_page_map_field);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static mi_page_t*** mi_page_map_empty_get() => (mi_page_t***)mi_page_map_empty_field;

        private static void mi_page_map_cannot_commit()
        {
            _mi_warning_message("unable to commit the allocation page-map on-demand");
        }

        // ---------------------------------------------------------------------------
        // inlines from internal.h
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint _mi_page_map_index(void* p, nuint* sub_idx)
        {
            nuint u = (nuint)p / MI_ARENA_SLICE_SIZE;
            if (sub_idx != null) { *sub_idx = u % MI_PAGE_MAP_SUB_COUNT; }
            return u / MI_PAGE_MAP_SUB_COUNT;
        }

        // note: the submap entries are pointer-to-POINTER atomics; C# generics cannot take
        // `mi_page_t*` as a type argument, so these cells go through the nuint atomics.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_page_t** _mi_page_map_at(nuint idx)
        {
            return (mi_page_t**)mi_atomic_load_relaxed(ref *(nuint*)&mi_page_map_get()[idx]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_page_t* _mi_unchecked_ptr_page(void* p)
        {
            nuint sub_idx;
            nuint idx = _mi_page_map_index(p, &sub_idx);
            return _mi_page_map_at(idx)[sub_idx];   // NULL if p==NULL
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_page_t* _mi_checked_ptr_page(void* p)
        {
            // MI_MIN_VABITS (43) < MI_INTPTR_BITS
            if (((nuint)p >> MI_MIN_VABITS) != 0)
            {
                if ((nuint)p > mi_atomic_load_relaxed(ref _mi_page_map_max_address)) return null;
            }
            nuint sub_idx;
            nuint idx = _mi_page_map_index(p, &sub_idx);
            mi_page_t** sub = _mi_page_map_at(idx);
            if (sub == null) return null;
            return sub[sub_idx];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_page_t* _mi_ptr_page(void* p)
        {
#if MI_DEBUG
            return _mi_checked_ptr_page(p);
#else
            return _mi_unchecked_ptr_page(p);
#endif
        }

        // ---------------------------------------------------------------------------
        // page-map.c
        // ---------------------------------------------------------------------------

        private static bool mi_page_map_is_committed(nuint idx, nuint* pbit_idx)
        {
            nuint commit = mi_atomic_load_relaxed(ref mi_page_map_commit);
            nuint bit_idx = idx / MI_PAGE_MAP_ENTRIES_PER_CBIT;
            mi_assert_internal(bit_idx < MI_BFIELD_BITS);
            if (pbit_idx != null) { *pbit_idx = bit_idx; }
            return (commit & ((nuint)1 << (int)bit_idx)) != 0;
        }

        private static bool mi_page_map_ensure_committed(nuint idx, mi_page_t*** submap)
        {
            mi_assert_internal(submap != null && *submap == null);
            nuint bit_idx;
            if (!mi_page_map_is_committed(idx, &bit_idx))
            {
                byte* start = (byte*)&mi_page_map_get()[bit_idx * MI_PAGE_MAP_ENTRIES_PER_CBIT];
                if (!_mi_os_commit(_mi_subproc_main(), start, MI_PAGE_MAP_ENTRIES_PER_CBIT * (nuint)sizeof(void*), null))
                {
                    mi_page_map_cannot_commit();
                    return false;
                }
                mi_atomic_or_acq_rel(ref mi_page_map_commit, (nuint)1 << (int)bit_idx);
            }
            *submap = (mi_page_t**)mi_atomic_load_acquire(ref *(nuint*)&mi_page_map_get()[idx]);
            return true;
        }

        // initialize the page map
        // (port note: idempotent -- C calls this exactly once from process init, but the
        // port also allows explicit calls, e.g. from tests, without resetting a live map)
        public static bool _mi_page_map_init()
        {
            if (mi_page_map_get() != mi_page_map_empty_get()) return true;   // already initialized
            nuint vbits = (nuint)mi_option_get_clamp(mi_option_t.mi_option_max_vabits, 0, MI_MAX_VABITS);
            if (vbits == 0)
            {
                vbits = _mi_os_virtual_address_bits();
                // x64: canonical address is limited to the first 128 TiB
                if (System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture == Architecture.X64 && vbits >= 48) { vbits = 47; }
            }
            if (vbits < MI_PAGE_MAP_SUB_SHIFT + MI_ARENA_SLICE_SHIFT)
            {
                vbits = MI_PAGE_MAP_SUB_SHIFT + MI_ARENA_SLICE_SHIFT;
            }
            if (vbits < MI_MIN_VABITS)     // cover at least this much for a faster _mi_checked_ptr
            {
                vbits = MI_MIN_VABITS;
            }
            if (vbits > MI_MAX_VABITS)     // limit page map size even if more virtual addresses are available
            {
                vbits = MI_MAX_VABITS;
            }

            // Allocate the page map and commit bits
            mi_assert(MI_MAX_VABITS >= (int)vbits);
            mi_atomic_store_relaxed(ref _mi_page_map_max_address, (int)vbits >= MI_SIZE_BITS ? (nuint.MaxValue - MI_ARENA_SLICE_SIZE + 1) : ((nuint)1 << (int)vbits));
            mi_page_map_count = (nuint)1 << (int)(vbits - MI_PAGE_MAP_SUB_SHIFT - MI_ARENA_SLICE_SHIFT);
            mi_assert(mi_page_map_count <= MI_PAGE_MAP_COUNT);
            nuint os_page_size = _mi_os_page_size();
            nuint page_map_size = _mi_align_up(mi_page_map_count * (nuint)sizeof(void*), os_page_size);
            nuint submap_size = MI_PAGE_MAP_SUB_SIZE;
            nuint reserve_size = page_map_size + submap_size;
            bool commit = page_map_size <= 64 * MI_KiB ||
                          mi_option_is_enabled(mi_option_t.mi_option_pagemap_commit) || _mi_os_has_overcommit();
            mi_subproc_t* subproc = _mi_subproc_main();
            mi_memid_t memid;
            var page_map = (mi_page_t***)_mi_os_alloc_aligned(subproc, reserve_size, 1, commit, true /* allow large */, &memid);
            if (page_map == null)
            {
                _mi_error_message(ENOMEM, $"unable to reserve virtual memory for the page map ({page_map_size / MI_KiB} KiB)");
                Volatile.Write(ref _mi_page_map_field, mi_page_map_empty_field);
                return false;
            }
            mi_page_map_memid = memid;
            if (mi_page_map_memid.initially_committed && !mi_page_map_memid.initially_zero)
            {
                _mi_warning_message("internal: the page map was committed but not zero initialized!");
                _mi_memzero_aligned(page_map, page_map_size);
            }
            Volatile.Write(ref _mi_page_map_field, (nint)page_map);
            mi_atomic_store_release(ref mi_page_map_commit, mi_page_map_memid.initially_committed ? ~(nuint)0 : 0);

            // ensure there is a submap for the NULL address
            mi_page_t** sub0 = (mi_page_t**)((byte*)page_map + page_map_size);   // we reserved a submap part at the end already
            if (!mi_page_map_memid.initially_committed)
            {
                if (!_mi_os_commit(subproc, sub0, submap_size, null))   // commit full submap (issue #1087)
                {
                    mi_page_map_cannot_commit();
                    return false;
                }
            }
            if (!mi_page_map_memid.initially_zero)     // initialize low addresses with NULL
            {
                _mi_memzero_aligned(sub0, submap_size);
            }
            mi_page_t** nullsub = null;
            if (!mi_page_map_ensure_committed(0, &nullsub))
            {
                mi_page_map_cannot_commit();
                return false;
            }
            mi_atomic_store_release(ref *(nuint*)&mi_page_map_get()[0], (nuint)sub0);
            fixed (mi_lock_t* lockPtr = &mi_page_map_lock)
            {
                mi_lock_init(lockPtr);   // initialize late in case the lock init causes allocation
            }

            mi_assert_internal(_mi_ptr_page(null) == null);
            return true;
        }

        public static void _mi_page_map_unsafe_destroy()
        {
            mi_page_t*** page_map = mi_page_map_get();
            mi_assert_internal(page_map != null && page_map != mi_page_map_empty_get());
            if (page_map == null || page_map == mi_page_map_empty_get()) return;
            mi_subproc_t* subproc = _mi_subproc_main();
            for (nuint idx = 1; idx < mi_page_map_count; idx++)   // skip entry 0 (as we allocate that submap at the end of the page_map)
            {
                // free all sub-maps
                if (mi_page_map_is_committed(idx, null))
                {
                    mi_page_t** sub = _mi_page_map_at(idx);
                    if (sub != null)
                    {
                        mi_memid_t memid = _mi_memid_create_os(sub, MI_PAGE_MAP_SUB_SIZE, true, false, false);
                        _mi_os_free_ex(subproc, memid.mem.os.@base, memid.mem.os.size, true, memid);
                        mi_atomic_store_release(ref *(nuint*)&page_map[idx], 0);
                    }
                }
            }
            _mi_os_free_ex(subproc, page_map, mi_page_map_memid.mem.os.size, true, mi_page_map_memid);
            Volatile.Write(ref _mi_page_map_field, mi_page_map_empty_field);
            mi_page_map_count = 0;
            mi_page_map_memid = _mi_memid_none();
            mi_atomic_store_relaxed(ref _mi_page_map_max_address, 0);
            mi_atomic_store_release(ref mi_page_map_commit, 0);
        }

        private static bool mi_page_map_ensure_submap_at(nuint idx, mi_page_t*** submap)
        {
            mi_assert_internal(submap != null && *submap == null);
            mi_page_t** sub = null;
            if (!mi_page_map_ensure_committed(idx, &sub))
            {
                return false;
            }
            if (sub == null)
            {
                // sub map not yet allocated, alloc now
                fixed (mi_lock_t* lockPtr = &mi_page_map_lock)
                {
                    mi_lock_acquire(lockPtr);
                    try
                    {
                        sub = (mi_page_t**)mi_atomic_load_acquire(ref *(nuint*)&mi_page_map_get()[idx]);   // reload
                        if (sub == null)   // not yet allocated by another thread?
                        {
                            mi_subproc_t* subproc = _mi_subproc_main();
                            mi_memid_t memid;
                            nuint submap_size = MI_PAGE_MAP_SUB_SIZE;
                            sub = (mi_page_t**)_mi_os_zalloc(subproc, submap_size, &memid);
                            if (sub == null)
                            {
                                _mi_warning_message("internal error: unable to extend the page map");
                            }
                            else
                            {
                                nuint expect = 0;
                                if (!mi_atomic_cas_strong_acq_rel(ref *(nuint*)&mi_page_map_get()[idx], ref expect, (nuint)sub))
                                {
                                    // another thread already allocated it.. free and continue
                                    _mi_os_free(subproc, sub, submap_size, memid);
                                    sub = (mi_page_t**)expect;
                                }
                            }
                        }
                    }
                    finally
                    {
                        mi_lock_release(lockPtr);
                    }
                }
                if (sub == null) return false;   // unable to allocate the submap..
            }
            mi_assert_internal(sub != null);
            *submap = sub;
            return true;
        }

        private static bool mi_page_map_set_range_prim(mi_page_t* page, nuint idx, nuint sub_idx, nuint slice_count)
        {
            // is the page map area that contains the page address committed?
            while (slice_count > 0)
            {
                mi_page_t** sub = null;
                if (!mi_page_map_ensure_submap_at(idx, &sub))
                {
                    return false;
                }
                mi_assert_internal(sub != null);
                // set the offsets for the page
                while (slice_count > 0 && sub_idx < MI_PAGE_MAP_SUB_COUNT)
                {
                    sub[sub_idx] = page;
                    slice_count--;
                    sub_idx++;
                }
                idx++;   // potentially wrap around to the next idx
                sub_idx = 0;
            }
            return true;
        }

        private static bool mi_page_map_set_range(mi_page_t* page, nuint idx, nuint sub_idx, nuint slice_count)
        {
            if (!mi_page_map_set_range_prim(page, idx, sub_idx, slice_count))
            {
                // failed to commit, call again to reset the page pointer if needed
                if (page != null)
                {
                    mi_page_map_set_range_prim(null, idx, sub_idx, slice_count);
                }
                return false;
            }
            return true;
        }

        private static nuint mi_page_map_get_idx(mi_page_t* page, nuint* sub_idx, nuint* slice_count)
        {
            nuint page_size;
            byte* page_start = mi_page_area(page, &page_size);
            if (page_size > MI_LARGE_PAGE_SIZE) { page_size = MI_LARGE_PAGE_SIZE - MI_ARENA_SLICE_SIZE; }   // furthest interior pointer
            *slice_count = mi_slice_count_of_size(page_size) + (nuint)(page_start - mi_page_slice_start(page)) / MI_ARENA_SLICE_SIZE;   // add for large aligned blocks
            return _mi_page_map_index(page_start, sub_idx);
        }

        public static bool _mi_page_map_register(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            mi_assert_internal(mi_page_map_get() != mi_page_map_empty_get());   // should be initialized before multi-thread access!
            if (mi_page_map_get() == mi_page_map_empty_get())
            {
                if (!_mi_page_map_init()) return false;
            }
            nuint slice_count;
            nuint sub_idx;
            nuint idx = mi_page_map_get_idx(page, &sub_idx, &slice_count);
            return mi_page_map_set_range(page, idx, sub_idx, slice_count);
        }

        public static void _mi_page_map_unregister(mi_page_t* page)
        {
            mi_assert_internal(page != null);
            mi_assert_internal(_mi_is_aligned(mi_page_slice_start(page), MI_PAGE_ALIGN));
            // note: should proceed even if the page was not registered yet (for failure paths in page allocation in `arena.c`)
            if (mi_page_map_get() == mi_page_map_empty_get()) return;
            // get index and count
            nuint slice_count;
            nuint sub_idx;
            nuint idx = mi_page_map_get_idx(page, &sub_idx, &slice_count);
            // unset the offsets
            mi_page_map_set_range(null, idx, sub_idx, slice_count);
        }

        public static void _mi_page_map_unregister_range(void* start, nuint size)
        {
            if (mi_page_map_get() == mi_page_map_empty_get()) return;
            nuint slice_count = _mi_divide_up(size, MI_ARENA_SLICE_SIZE);
            nuint sub_idx;
            nuint idx = _mi_page_map_index(start, &sub_idx);
            mi_page_map_set_range(null, idx, sub_idx, slice_count);   // todo: avoid committing if not already committed?
        }

        // Visit every distinct page currently registered in the page map.
        //
        // Port note: this has no counterpart in the C -- it exists so diagnostics can enumerate pages
        // independently of where they came from. `_mi_heap_visit_blocks` can only reach pages that are
        // either in an arena's `pages` bitmap or on the `os_abandoned_pages` list, so a live page that
        // was allocated outside an arena is invisible to it. The page map has no such gap: every page
        // is registered here at creation regardless of its memkind.
        //
        // `mi_page_map_set_range` writes one entry per slice covered by the page, and those entries are
        // always a contiguous ascending run, so remembering the previous entry is enough to visit each
        // page exactly once. Reads are unsynchronized (as the C does elsewhere) -- a page being created
        // or freed concurrently may be seen or missed, so treat the result as a gauge.
        // Returns false if the visitor asked to stop.
        public static bool _mi_page_map_forall_pages(delegate*<mi_page_t*, void*, bool> visitor, void* arg)
        {
            mi_assert(visitor != null);
            if (visitor == null) return false;
            mi_page_t*** page_map = mi_page_map_get();
            if (page_map == null || page_map == mi_page_map_empty_get()) return true;   // not initialized yet

            mi_page_t* prev = null;
            for (nuint idx = 0; idx < mi_page_map_count; idx++)
            {
                // never dereference an uncommitted part of the map
                if (!mi_page_map_is_committed(idx, null)) { prev = null; continue; }
                mi_page_t** sub = _mi_page_map_at(idx);
                if (sub == null) { prev = null; continue; }
                for (nuint sub_idx = 0; sub_idx < MI_PAGE_MAP_SUB_COUNT; sub_idx++)
                {
                    mi_page_t* page = sub[sub_idx];
                    if (page == null) { prev = null; continue; }
                    if (page == prev) continue;   // still inside the run of the page we just visited
                    prev = page;
                    if (!visitor(page, arg)) return false;
                }
            }
            return true;
        }

        // Return NULL for invalid pointers
        public static mi_page_t* _mi_safe_ptr_page(void* p)
        {
            if (p == null) return null;
            if ((nuint)p >= mi_atomic_load_relaxed(ref _mi_page_map_max_address)) return null;
            nuint sub_idx;
            nuint idx = _mi_page_map_index(p, &sub_idx);
            if (!mi_page_map_is_committed(idx, null)) return null;
            mi_page_t** sub = _mi_page_map_at(idx);
            if (sub == null) return null;
            return sub[sub_idx];
        }

        public static bool mi_is_in_heap_region(void* p)
        {
            return _mi_safe_ptr_page(p) != null;
        }
    }
}
