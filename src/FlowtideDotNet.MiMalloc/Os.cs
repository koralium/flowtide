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
// Port of mimalloc v3.4.4 `src/os.c`.
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Deviations (see README.md):
//  - MI_SECURE == 0: the secure guard-page helpers are the no-op variants.
//  - Huge OS pages (1 GiB) are not supported: _mi_os_alloc_huge_os_pages returns null
//    (the arena layer treats that as "no huge pages available", same as when the OS
//    refuses them). NUMA is single-node.
//  - The aligned-hint start address is randomized via the theap random context in
//    Release builds only (C: MI_SECURE>=1 || NDEBUG), matching the C debug/release split.
//  - Custom commit functions (`mi_commit_fun_t`) are not supported (never set by Flowtide).

namespace FlowtideDotNet.MiMalloc
{
    // OS memory configuration (from include/mimalloc/prim.h)
    internal struct mi_os_mem_config_t
    {
        public nuint page_size;              // default to 4KiB
        public nuint large_page_size;        // 0 if not supported, usually 2MiB (4MiB on Windows)
        public nuint alloc_granularity;      // smallest allocation size (usually 4KiB, on Windows 64KiB)
        public nuint physical_memory_in_kib; // physical memory size in KiB
        public nuint virtual_address_bits;   // usually 48 or 56 bits on 64-bit systems
        public bool has_overcommit;          // can we reserve more memory than can be actually committed?
        public bool has_partial_free;        // can allocated blocks be freed partially? (true for mmap, false for VirtualAlloc)
        public bool has_virtual_reserve;     // supports virtual address space reservation?
        public bool has_transparent_huge_pages; // true if transparent huge pages are enabled (on Linux)
    }

    // Return process information (only for statistics)
    internal struct mi_process_info_t
    {
        public long elapsed;
        public long utime;
        public long stime;
        public nuint current_rss;
        public nuint peak_rss;
        public nuint current_commit;
        public nuint peak_commit;
        public nuint page_faults;
    }

    internal static unsafe partial class Mi
    {
        private const nuint MI_DEFAULT_PHYSICAL_MEMORY_IN_KIB = 32 * MI_MiB;   // 32 GiB (in KiB)

        private static mi_os_mem_config_t mi_os_mem_config = new mi_os_mem_config_t
        {
            page_size = 4096,
            large_page_size = 0,
            alloc_granularity = 4096,
            physical_memory_in_kib = MI_DEFAULT_PHYSICAL_MEMORY_IN_KIB,
            virtual_address_bits = MI_MAX_VABITS,
            has_overcommit = true,
            has_partial_free = false,
            has_virtual_reserve = true,
            has_transparent_huge_pages = false,
        };

        public static bool _mi_os_has_overcommit() => mi_os_mem_config.has_overcommit;

        public static bool _mi_os_has_virtual_reserve() => mi_os_mem_config.has_virtual_reserve;

        // OS (small) page size
        public static nuint _mi_os_page_size() => mi_os_mem_config.page_size;

        // if large OS pages are supported (2 or 4MiB), then return the size, otherwise return the small page size (4KiB)
        public static nuint _mi_os_large_page_size()
            => mi_os_mem_config.large_page_size != 0 ? mi_os_mem_config.large_page_size : _mi_os_page_size();

        // minimal purge size. Can be larger than the page size if transparent huge pages are enabled.
        public static nuint _mi_os_minimal_purge_size()
        {
            nuint minsize = mi_option_get_size(mi_option_t.mi_option_minimal_purge_size);
            if (minsize != 0)
            {
                return _mi_align_up(minsize, _mi_os_page_size());
            }
            else if (mi_os_mem_config.has_transparent_huge_pages && mi_option_get(mi_option_t.mi_option_allow_thp) == 2)
            {
                return _mi_os_large_page_size();
            }
            else
            {
                return _mi_os_page_size();
            }
        }

        public static nuint _mi_os_guard_page_size()
        {
            nuint gsize = _mi_os_page_size();
            mi_assert(gsize <= (MI_ARENA_SLICE_SIZE / 4)); // issue #1166
            return gsize;
        }

        public static nuint _mi_os_virtual_address_bits()
        {
            nuint vbits = mi_os_mem_config.virtual_address_bits;
            mi_assert(vbits <= MI_MAX_VABITS);
            return vbits;
        }

        public static bool _mi_os_canuse_large_page(nuint size, nuint alignment)
        {
            // if we have access, check the size and alignment requirements
            if (mi_os_mem_config.large_page_size == 0) return false;
            return (size % mi_os_mem_config.large_page_size) == 0 && (alignment % mi_os_mem_config.large_page_size) == 0;
        }

        // round to a good OS allocation size (bounded by max 12.5% waste)
        public static nuint _mi_os_good_alloc_size(nuint size)
        {
            nuint align_size;
            if (size < 512 * MI_KiB) align_size = _mi_os_page_size();
            else if (size < 2 * MI_MiB) align_size = 64 * MI_KiB;
            else if (size < 8 * MI_MiB) align_size = 256 * MI_KiB;
            else if (size < 32 * MI_MiB) align_size = 1 * MI_MiB;
            else align_size = 4 * MI_MiB;
            if (size >= nuint.MaxValue - align_size) return size; // possible overflow?
            return _mi_align_up(size, align_size);
        }

        public static void _mi_os_init()
        {
            _mi_prim_mem_init(ref mi_os_mem_config);
        }

        /* -----------------------------------------------------------
          Util
        -------------------------------------------------------------- */

        // On systems with enough virtual address bits, we can do efficient aligned allocation by using
        // the 2TiB to 30TiB area to allocate those. (see os.c for background)
        private const nuint MI_HINT_ALIGN = (nuint)4 << 20;   // 4MiB alignment
        private const ulong MI_HINT_BASE = (ulong)2 << 40;    // 2TiB start
        private const ulong MI_HINT_AREA = (ulong)4 << 40;    // upto (2+4) 6TiB
        private const ulong MI_HINT_MAX = (ulong)30 << 40;    // wrap after 30TiB

        private static nuint mi_os_aligned_base;   // atomic; = 0

        public static void* _mi_os_get_aligned_hint(nuint try_alignment, nuint size)
        {
            if (try_alignment <= mi_os_mem_config.alloc_granularity || try_alignment > MI_HINT_ALIGN) return null;
            if (mi_os_mem_config.virtual_address_bits < 46) return null;  // < 64TiB virtual address space
            size = _mi_align_up(size, MI_HINT_ALIGN);
            size += MI_HINT_ALIGN;   // put in virtual gaps between hinted blocks

            nuint hint = mi_atomic_add_acq_rel(ref mi_os_aligned_base, size);
            if (hint == 0 || hint > unchecked((nuint)MI_HINT_MAX))   // wrap or initialize
            {
                nuint init = unchecked((nuint)MI_HINT_BASE);
#if !MI_DEBUG
                // security: randomize start of aligned allocations unless in debug mode (C: MI_SECURE>=1 || NDEBUG)
                mi_theap_t* theap = _mi_theap_default();   // don't use `mi_theap_get_default()` as that can cause allocation recursively (issue #1267)
                if (!mi_theap_is_initialized(theap)) return null;   // no hint as we lack randomness at this point
                nuint r = _mi_theap_random_next(theap);
                init = init + ((MI_HINT_ALIGN * ((r >> 17) & 0xFFFFF)) % unchecked((nuint)MI_HINT_AREA));   // (randomly 20 bits)*4MiB == 0 to 4TiB
#endif
                nuint expected = hint + size;
                mi_atomic_cas_strong_acq_rel(ref mi_os_aligned_base, ref expected, init);
                hint = mi_atomic_add_acq_rel(ref mi_os_aligned_base, size); // this may still give 0 or > MI_HINT_MAX but that is ok, it is a hint after all
            }
            mi_assert_internal(hint % MI_HINT_ALIGN == 0);
            if (hint % try_alignment != 0) return null;
            return (void*)hint;
        }

        /* -----------------------------------------------------------
          Guard page allocation (MI_SECURE == 0: no-ops)
        ----------------------------------------------------------- */

        // In secure mode, return the size of a guard page, otherwise 0
        public static nuint _mi_os_secure_guard_page_size() => 0;

        public static bool _mi_os_secure_guard_page_set_at(mi_subproc_t* subproc, void* addr, mi_memid_t memid) => true;

        public static bool _mi_os_secure_guard_page_set_before(mi_subproc_t* subproc, void* addr, mi_memid_t memid)
            => _mi_os_secure_guard_page_set_at(subproc, (byte*)addr - _mi_os_secure_guard_page_size(), memid);

        public static bool _mi_os_secure_guard_page_reset_at(mi_subproc_t* subproc, void* addr, mi_memid_t memid) => true;

        public static bool _mi_os_secure_guard_page_reset_before(mi_subproc_t* subproc, void* addr, mi_memid_t memid)
            => _mi_os_secure_guard_page_reset_at(subproc, (byte*)addr - _mi_os_secure_guard_page_size(), memid);

        /* -----------------------------------------------------------
          Free memory
        -------------------------------------------------------------- */

        private static void mi_os_prim_free(mi_subproc_t* subproc, void* addr, nuint size, nuint commit_size)
        {
            mi_assert_internal(subproc != null);
            mi_assert_internal((size % _mi_os_page_size()) == 0);
            if (addr == null) return;
            int err = _mi_prim_free(addr, size);  // allow size==0 (issue #1041)
            if (err != 0)
            {
                _mi_warning_message($"unable to free OS memory (error: {err} (0x{err:x}), size: 0x{size:x} bytes, address: 0x{(nuint)addr:x})");
            }
            if (commit_size > 0)
            {
                __mi_stat_decrease_mt(&subproc->stats.committed, commit_size);
            }
            __mi_stat_decrease_mt(&subproc->stats.reserved, size);
        }

        public static void _mi_os_free_ex(mi_subproc_t* subproc, void* addr, nuint size, bool still_committed, mi_memid_t memid)
        {
            if (mi_memkind_is_os(memid.memkind))
            {
                nuint csize = memid.mem.os.size;
                if (csize == 0) { csize = _mi_os_good_alloc_size(size); }
                mi_assert_internal(csize >= size);
                nuint commit_size = still_committed ? csize : 0;
                void* @base = addr;
                // different base? (due to alignment)
                if (memid.mem.os.@base != @base)
                {
                    mi_assert(memid.mem.os.@base <= addr);
                    @base = memid.mem.os.@base;
                    nuint diff = (nuint)((byte*)addr - (byte*)memid.mem.os.@base);
                    if (memid.mem.os.size == 0)
                    {
                        csize += diff;
                    }
                    if (still_committed)
                    {
                        commit_size -= diff;  // the (addr-base) part was already un-committed
                    }
                }
                // free it
                if (memid.memkind == mi_memkind_t.MI_MEM_OS_HUGE)
                {
                    mi_assert(memid.is_pinned);
                    mi_os_free_huge_os_pages(subproc, @base, csize);
                }
                else
                {
                    mi_os_prim_free(subproc, @base, csize, still_committed ? commit_size : 0);
                }
            }
            else
            {
                // nothing to do
                mi_assert(memid.memkind < mi_memkind_t.MI_MEM_OS);
            }
        }

        public static void _mi_os_free(mi_subproc_t* subproc, void* p, nuint size, mi_memid_t memid)
        {
            _mi_os_free_ex(subproc, p, size, true, memid);
        }

        /* -----------------------------------------------------------
           Primitive allocation from the OS.
        -------------------------------------------------------------- */

        // Note: the `try_alignment` is just a hint and the returned pointer is not guaranteed to be aligned.
        // Also `hint_addr` is a hint and may be ignored.
        private static void* mi_os_prim_alloc_at(mi_subproc_t* subproc, void* hint_addr, nuint size, nuint try_alignment, bool commit, bool allow_large, ref bool is_large, ref bool is_zero)
        {
            mi_assert_internal(size > 0 && (size % _mi_os_page_size()) == 0);
            if (size == 0) return null;
            if (!commit) { allow_large = false; }
            if (try_alignment == 0) { try_alignment = 1; } // avoid 0 to ensure there will be no divide by zero when aligning

            // try to align along large OS page size for larger allocations
            nuint large_page_size = mi_os_mem_config.large_page_size;
            if (large_page_size > 0 && hint_addr == null && size >= 8 * large_page_size && _mi_is_power_of_two(try_alignment) && try_alignment < large_page_size)
            {
                try_alignment = large_page_size;
            }

            is_zero = false;
            void* p = null;
            int err = _mi_prim_alloc(hint_addr, size, try_alignment, commit, allow_large, ref is_large, ref is_zero, &p);
            if (err != 0)
            {
                _mi_warning_message($"unable to allocate OS memory (error: {err} (0x{err:x}), addr: 0x{(nuint)hint_addr:x}, size: 0x{size:x} bytes, align: 0x{try_alignment:x}, commit: {(commit ? 1 : 0)}, allow large: {(allow_large ? 1 : 0)})");
            }

            __mi_stat_counter_increase_mt(&subproc->stats.mmap_calls, 1);
            if (p != null)
            {
                __mi_stat_increase_mt(&subproc->stats.reserved, size);
                if (commit)
                {
                    __mi_stat_increase_mt(&subproc->stats.committed, size);
                }
            }
            return p;
        }

        private static void* mi_os_prim_alloc(mi_subproc_t* subproc, nuint size, nuint try_alignment, bool commit, bool allow_large, ref bool is_large, ref bool is_zero)
        {
            return mi_os_prim_alloc_at(subproc, null /* hint addr */, size, try_alignment, commit, allow_large, ref is_large, ref is_zero);
        }

        // Primitive aligned allocation from the OS.
        // This function guarantees the allocated memory is aligned.
        private static void* mi_os_prim_alloc_aligned(mi_subproc_t* subproc, nuint size, nuint alignment, bool commit, bool allow_large, mi_memid_t* memid)
        {
            mi_assert_internal(memid != null);
            mi_assert_internal(alignment >= _mi_os_page_size() && ((alignment & (alignment - 1)) == 0));
            mi_assert_internal(size > 0 && (size % _mi_os_page_size()) == 0);
            *memid = _mi_memid_none();
            if (!commit) allow_large = false;
            if (!(alignment >= _mi_os_page_size() && ((alignment & (alignment - 1)) == 0))) return null;
            size = _mi_align_up(size, _mi_os_page_size());

            // try a direct allocation if the alignment is below the default, or less than or equal to 1/4 fraction of the size.
            bool try_direct_alloc = alignment <= mi_os_mem_config.alloc_granularity || alignment <= size / 4;

            bool os_is_large = false;
            bool os_is_zero = false;
            void* os_base = null;
            nuint os_size = size;
            void* p = null;
            if (try_direct_alloc)
            {
                p = mi_os_prim_alloc(subproc, size, alignment, commit, allow_large, ref os_is_large, ref os_is_zero);
            }

            // aligned already?
            if (p != null && _mi_is_aligned(p, alignment))
            {
                os_base = p;
            }
            else
            {
                // if not aligned, free it, overallocate, and unmap around it
                if (try_direct_alloc)
                {
                    _mi_warning_message($"unable to allocate aligned OS memory directly, fall back to over-allocation (size: 0x{size:x} bytes, address: 0x{(nuint)p:x}, alignment: 0x{alignment:x}, commit: {(commit ? 1 : 0)})");
                }
                if (p != null) { mi_os_prim_free(subproc, p, size, commit ? size : 0); }
                if (size >= nuint.MaxValue - alignment) return null; // overflow
                nuint over_size = size + alignment;

                if (!mi_os_mem_config.has_partial_free)
                {
                    // win32 virtualAlloc cannot free parts of an allocated block
                    // over-allocate uncommitted (virtual) memory
                    p = mi_os_prim_alloc(subproc, over_size, 1 /*alignment*/, false /* commit? */, false /* allow_large */, ref os_is_large, ref os_is_zero);
                    if (p == null) return null;

                    // set p to the aligned part in the full region
                    // note: Windows VirtualFree needs the actual base pointer
                    // this is handled though by having the `base` field in the memid
                    os_base = p; // remember the base
                    os_size = over_size;
                    p = _mi_align_up_ptr(p, alignment);

                    // explicitly commit only the aligned part
                    if (commit)
                    {
                        if (!_mi_os_commit(subproc, p, size, null))
                        {
                            mi_os_prim_free(subproc, os_base, over_size, 0);
                            return null;
                        }
                    }
                }
                else
                {
                    // mmap can free inside an allocation
                    // overallocate...
                    p = mi_os_prim_alloc(subproc, over_size, 1, commit, false, ref os_is_large, ref os_is_zero);
                    if (p == null) return null;

                    // and selectively unmap parts around the over-allocated area.
                    void* aligned_p = _mi_align_up_ptr(p, alignment);
                    nuint pre_size = (nuint)((byte*)aligned_p - (byte*)p);
                    nuint mid_size = _mi_align_up(size, _mi_os_page_size());
                    nuint post_size = over_size - pre_size - mid_size;
                    mi_assert_internal(pre_size < over_size && post_size < over_size && mid_size >= size);
                    if (pre_size > 0) { mi_os_prim_free(subproc, p, pre_size, commit ? pre_size : 0); }
                    if (post_size > 0) { mi_os_prim_free(subproc, (byte*)aligned_p + mid_size, post_size, commit ? post_size : 0); }
                    // we can return the aligned pointer on `mmap` systems
                    p = aligned_p;
                    os_base = aligned_p; // since we freed the pre part, `*base == p`.
                    os_size = mid_size;
                }
            }

            mi_assert_internal(p != null && os_base != null && _mi_is_aligned(p, alignment));
            mi_assert_internal(os_base <= p && size <= os_size);
            *memid = _mi_memid_create_os(os_base, os_size, commit, os_is_zero, os_is_large);
            return p;
        }

        /* -----------------------------------------------------------
          OS API: alloc and alloc_aligned
        ----------------------------------------------------------- */

        public static void* _mi_os_alloc(mi_subproc_t* subproc, nuint size, mi_memid_t* memid)
        {
            *memid = _mi_memid_none();
            if (size == 0) return null;
            size = _mi_os_good_alloc_size(size);
            bool os_is_large = false;
            bool os_is_zero = false;
            void* p = mi_os_prim_alloc(subproc, size, 0, true, false, ref os_is_large, ref os_is_zero);
            if (p == null) return null;

            *memid = _mi_memid_create_os(p, size, true, os_is_zero, os_is_large);
            mi_assert_internal(memid->mem.os.size >= size);
            mi_assert_internal(memid->initially_committed);
            return p;
        }

        public static void* _mi_os_alloc_aligned(mi_subproc_t* subproc, nuint size, nuint alignment, bool commit, bool allow_large, mi_memid_t* memid)
        {
            *memid = _mi_memid_none();
            if (size == 0) return null;
            size = _mi_os_good_alloc_size(size);
            alignment = _mi_align_up(alignment, _mi_os_page_size());

            void* p = mi_os_prim_alloc_aligned(subproc, size, alignment, commit, allow_large, memid);
            if (p == null) return null;

            mi_assert_internal(memid->mem.os.size >= size);
            mi_assert_internal(_mi_is_aligned(p, alignment));
            if (commit) { mi_assert_internal(memid->initially_committed); }
            return p;
        }

        private static void* mi_os_ensure_zero(mi_subproc_t* subproc, void* p, nuint size, mi_memid_t* memid)
        {
            if (p == null || size == 0) return p;
            // ensure committed
            if (!memid->initially_committed)
            {
                if (!_mi_os_commit(subproc, p, size, null))
                {
                    _mi_os_free(subproc, p, size, *memid);
                    return null;
                }
                memid->initially_committed = true;
            }
            // ensure zero'd
            if (memid->initially_zero) return p;
            _mi_memzero_aligned(p, size);
            memid->initially_zero = true;
            return p;
        }

        public static void* _mi_os_zalloc(mi_subproc_t* subproc, nuint size, mi_memid_t* memid)
        {
            void* p = _mi_os_alloc(subproc, size, memid);
            return mi_os_ensure_zero(subproc, p, size, memid);
        }

        /* -----------------------------------------------------------
          OS aligned allocation with an offset. (used for very large alignments)
        ----------------------------------------------------------- */

        public static void* _mi_os_alloc_aligned_at_offset(mi_subproc_t* subproc, nuint size, nuint alignment, nuint offset, bool commit, bool allow_large, mi_memid_t* memid)
        {
            mi_assert(offset <= size);
            mi_assert((alignment % _mi_os_page_size()) == 0);
            *memid = _mi_memid_none();
            if (offset > size) return null;
            if (offset == 0)
            {
                // regular aligned allocation
                return _mi_os_alloc_aligned(subproc, size, alignment, commit, allow_large, memid);
            }
            else
            {
                // overallocate to align at an offset
                nuint extra = _mi_align_up(offset, alignment) - offset;
                if (size >= nuint.MaxValue - extra) return null;  // too large
                nuint oversize = size + extra;
                void* start = _mi_os_alloc_aligned(subproc, oversize, alignment, commit, allow_large, memid);
                if (start == null) return null;

                void* p = (byte*)start + extra;
                mi_assert(_mi_is_aligned((byte*)p + offset, alignment));
                // decommit the overallocation at the start
                if (commit && extra >= _mi_os_page_size())
                {
                    _mi_os_decommit(subproc, start, extra);
                }
                return p;
            }
        }

        /* -----------------------------------------------------------
          OS memory API: reset, commit, decommit, protect, unprotect.
        ----------------------------------------------------------- */

        // OS page align within a given area, either conservative (pages inside the area only),
        // or not (straddling pages outside the area is possible)
        private static void* mi_os_page_align_areax(bool conservative, void* addr, nuint size, out nuint newsize)
        {
            mi_assert(addr != null && size > 0);
            newsize = 0;
            if (size == 0 || addr == null) return null;

            // page align conservatively within the range, or liberally straddling pages outside the range
            void* start = conservative ? _mi_align_up_ptr(addr, _mi_os_page_size())
                                       : _mi_align_down_ptr(addr, _mi_os_page_size());
            void* end = conservative ? _mi_align_down_ptr((byte*)addr + size, _mi_os_page_size())
                                     : _mi_align_up_ptr((byte*)addr + size, _mi_os_page_size());
            nint diff = (nint)((byte*)end - (byte*)start);
            if (diff <= 0) return null;

            mi_assert_internal((conservative && (nuint)diff <= size) || (!conservative && (nuint)diff >= size));
            newsize = (nuint)diff;
            return start;
        }

        private static void* mi_os_page_align_area_conservative(void* addr, nuint size, out nuint newsize)
        {
            return mi_os_page_align_areax(true, addr, size, out newsize);
        }

        public static bool _mi_os_commit_ex(mi_subproc_t* subproc, void* addr, nuint size, bool* is_zero, nuint stat_size)
        {
            if (is_zero != null) { *is_zero = false; }
            __mi_stat_counter_increase_mt(&subproc->stats.commit_calls, 1);

            // page align range
            void* start = mi_os_page_align_areax(false /* conservative? */, addr, size, out nuint csize);
            if (csize == 0) return true;

            // commit
            bool os_is_zero = false;
            int err = _mi_prim_commit(start, csize, ref os_is_zero);
            if (err != 0)
            {
                _mi_warning_message($"cannot commit OS memory (error: {err} (0x{err:x}), address: 0x{(nuint)start:x}, size: 0x{csize:x} bytes)");
                return false;
            }
            if (os_is_zero && is_zero != null)
            {
                *is_zero = true;
                mi_assert_expensive(mi_mem_is_zero(start, csize));
            }
            __mi_stat_increase_mt(&subproc->stats.committed, stat_size);  // use size for precise commit vs. decommit
            return true;
        }

        public static bool _mi_os_commit(mi_subproc_t* subproc, void* addr, nuint size, bool* is_zero)
        {
            return _mi_os_commit_ex(subproc, addr, size, is_zero, size);
        }

        private static bool mi_os_decommit_ex(mi_subproc_t* subproc, void* addr, nuint size, ref bool needs_recommit, nuint stat_size)
        {
            // page align
            void* start = mi_os_page_align_area_conservative(addr, size, out nuint csize);
            if (csize == 0) return true;

            // decommit
            needs_recommit = true;
            int err = _mi_prim_decommit(start, csize, ref needs_recommit);
            if (err != 0)
            {
                _mi_warning_message($"cannot decommit OS memory (error: {err} (0x{err:x}), address: 0x{(nuint)start:x}, size: 0x{csize:x} bytes)");
            }
            else if (needs_recommit)
            {
                __mi_stat_decrease_mt(&subproc->stats.committed, stat_size);
            }
            mi_assert_internal(err == 0);
            return err == 0;
        }

        public static bool _mi_os_decommit(mi_subproc_t* subproc, void* addr, nuint size)
        {
            bool needs_recommit = false;
            return mi_os_decommit_ex(subproc, addr, size, ref needs_recommit, size);
        }

        // Signal to the OS that the address range is no longer in use
        // but may be used later again. This will release physical memory
        // pages and reduce swapping while keeping the memory committed.
        // We page align to a conservative area inside the range to reset.
        public static bool _mi_os_reset(mi_subproc_t* subproc, void* addr, nuint size)
        {
            // page align conservatively within the range
            void* start = mi_os_page_align_area_conservative(addr, size, out nuint csize);
            if (csize == 0) return true;
            __mi_stat_counter_increase_mt(&subproc->stats.reset, csize);
            __mi_stat_counter_increase_mt(&subproc->stats.reset_calls, 1);

            int err = _mi_prim_reset(start, csize);
            if (err != 0)
            {
                _mi_warning_message($"cannot reset OS memory (error: {err} (0x{err:x}), address: 0x{(nuint)start:x}, size: 0x{csize:x} bytes)");
            }
            return err == 0;
        }

        public static void _mi_os_reuse(mi_subproc_t* subproc, void* addr, nuint size)
        {
            // page align conservatively within the range
            void* start = mi_os_page_align_area_conservative(addr, size, out nuint csize);
            if (csize == 0) return;
            int err = _mi_prim_reuse(start, csize);
            if (err != 0)
            {
                _mi_warning_message($"cannot reuse OS memory (error: {err} (0x{err:x}), address: 0x{(nuint)start:x}, size: 0x{csize:x} bytes)");
            }
        }

        // either resets or decommits memory, returns true if the memory needs
        // to be recommitted if it is to be re-used later on.
        public static bool _mi_os_purge_ex(mi_subproc_t* subproc, void* p, nuint size, bool allow_reset, nuint stat_size, void* commit_fun, void* commit_fun_arg)
        {
            if (mi_option_get(mi_option_t.mi_option_purge_delay) < 0) return false;  // is purging allowed?
            __mi_stat_counter_increase_mt(&subproc->stats.purge_calls, 1);
            __mi_stat_counter_increase_mt(&subproc->stats.purged, size);

            // custom commit functions are not supported in the port
            mi_assert_internal(commit_fun == null);

            if (mi_option_is_enabled(mi_option_t.mi_option_purge_decommits) &&   // should decommit?
                !_mi_preloading())                                               // don't decommit during preloading (unsafe)
            {
                bool needs_recommit = true;
                mi_os_decommit_ex(subproc, p, size, ref needs_recommit, stat_size);
                return needs_recommit;
            }
            else
            {
                if (allow_reset)
                {
                    // this can sometimes be not allowed if the range is not fully committed
                    _mi_os_reset(subproc, p, size);
                }
                return false;  // needs no recommit
            }
        }

        // either resets or decommits memory, returns true if the memory needs
        // to be recommitted if it is to be re-used later on.
        public static bool _mi_os_purge(mi_subproc_t* subproc, void* p, nuint size)
        {
            return _mi_os_purge_ex(subproc, p, size, true, size, null, null);
        }

        // Protect a region in memory to be not accessible.
        private static bool mi_os_protectx(void* addr, nuint size, bool protect)
        {
            // page align conservatively within the range
            void* start = mi_os_page_align_area_conservative(addr, size, out nuint csize);
            if (csize == 0) return false;

            int err = _mi_prim_protect(start, csize, protect);
            if (err != 0)
            {
                _mi_warning_message($"cannot {(protect ? "protect" : "unprotect")} OS memory (error: {err} (0x{err:x}), address: 0x{(nuint)start:x}, size: 0x{csize:x} bytes)");
            }
            return err == 0;
        }

        public static bool _mi_os_protect(void* addr, nuint size) => mi_os_protectx(addr, size, true);

        public static bool _mi_os_unprotect(void* addr, nuint size) => mi_os_protectx(addr, size, false);

        /* ----------------------------------------------------------------------------
        Huge OS page (1 GiB) support: not ported. The arena layer treats a null result
        as "huge pages unavailable" which is a supported configuration in C as well.
        -----------------------------------------------------------------------------*/

        public static void* _mi_os_alloc_huge_os_pages(mi_subproc_t* subproc, nuint pages, int numa_node, long max_msecs, nuint* pages_reserved, nuint* psize, mi_memid_t* memid)
        {
            *memid = _mi_memid_none();
            if (psize != null) *psize = 0;
            if (pages_reserved != null) *pages_reserved = 0;
            _mi_warning_message("huge OS page (1GiB) reservation is not supported by the managed mimalloc port");
            return null;
        }

        private static void mi_os_free_huge_os_pages(mi_subproc_t* subproc, void* p, nuint size)
        {
            // never reached: huge page allocation always fails in the port
            mi_assert_internal(p == null);
        }

        /* ----------------------------------------------------------------------------
        Support NUMA aware allocation (single-node in the port)
        -----------------------------------------------------------------------------*/

        private static nuint mi_numa_node_count;   // atomic; = 0   // cache the node count

        public static int _mi_os_numa_node_count()
        {
            nuint count = mi_atomic_load_acquire(ref mi_numa_node_count);
            if (count == 0)
            {
                long ncount = mi_option_get(mi_option_t.mi_option_use_numa_nodes); // given explicitly?
                if (ncount > 0 && ncount < int.MaxValue)
                {
                    count = (nuint)ncount;
                }
                else
                {
                    nuint n = _mi_prim_numa_node_count(); // or detect dynamically
                    if (n == 0 || n > int.MaxValue) { count = 1; }
                    else { count = n; }
                }
                mi_atomic_store_release(ref mi_numa_node_count, count); // save it
                if (count > 1) { _mi_verbose_message($"using {count} numa regions"); }
            }
            mi_assert_internal(count > 0 && count <= int.MaxValue);
            return (int)count;
        }

        private static int mi_os_numa_node_get()
        {
            int numa_count = _mi_os_numa_node_count();
            if (numa_count <= 1) return 0; // optimize on single numa node systems: always node 0
            // never more than the node count and >= 0
            nuint n = _mi_prim_numa_node();
            int numa_node = n < int.MaxValue ? (int)n : 0;
            if (numa_node >= numa_count) { numa_node = numa_node % numa_count; }
            return numa_node;
        }

        public static int _mi_os_numa_node()
        {
            if (mi_atomic_load_relaxed(ref mi_numa_node_count) == 1)
            {
                return 0;
            }
            else
            {
                return mi_os_numa_node_get();
            }
        }
    }
}
