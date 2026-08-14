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
// Port of mimalloc v3.4.4 `src/bitmap.h` + `src/bitmap.c` (concurrent bitmaps).
// Original: Copyright (c) 2019-2024 Microsoft Research, Daan Leijen (MIT license).
//
// Port notes:
//  - Only the portable (non-SIMD) paths are ported (C default is MI_OPT_SIMD=0 too).
//  - `mi_bitmap_t`/`mi_bbitmap_t` are variable-length in C (`chunks[]` is sized at
//    runtime, the declared MI_BITMAP_DEFAULT_CHUNK_COUNT=64 chunks only determine
//    `sizeof`). The C# structs declare the same 64 chunks so `sizeof()` matches, but
//    `chunks` MUST be accessed via the `mi_bitmap_chunk()`/`mi_bbitmap_chunk()`
//    pointer helpers (InlineArray indexers are bounds-checked at the declared size).
//  - The C `mi_bfield_cycle_iterate` macros become the `mi_bfield_cycle_iter` struct.
//  - The C function-pointer callbacks (`mi_claim_fun_t` etc.) are C# `delegate*`s.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.MiMalloc
{
    // A bitmap chunk contains 512 bits on 64-bit
    [StructLayout(LayoutKind.Sequential)]
    internal struct mi_bchunk_t
    {
        public mi_bfields_t bfields;   // atomic mi_bfield_t[MI_BCHUNK_FIELDS]
    }

    [InlineArray(Mi.MI_BCHUNK_FIELDS)]
    internal struct mi_bfields_t
    {
        private nuint _element0;
    }

    // An atomic bitmap.
    // Layout (matching C): chunk_count + padding to MI_BCHUNK_SIZE (64 bytes), then the
    // chunkmap (one chunk), then the chunks (declared 64 = MI_BITMAP_DEFAULT_CHUNK_COUNT;
    // real count is dynamic, up to 512).
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_bitmap_t
    {
        public nuint chunk_count;                    // atomic; total count of chunks (0 < N <= MI_BCHUNKMAP_BITS)
        private fixed ulong _padding[Mi.MI_BCHUNK_SIZE / Mi.MI_SIZE_SIZE - 1];
        public mi_bchunk_t chunkmap;
        public mi_bitmap_chunks_t chunks;            // access via Mi.mi_bitmap_chunk() only!
    }

    [InlineArray(Mi.MI_BITMAP_DEFAULT_CHUNK_COUNT)]
    internal struct mi_bitmap_chunks_t
    {
        private mi_bchunk_t _element0;
    }

    // An atomic "binned" bitmap for the free slices where we keep chunks reserved for particular size classes
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_bbitmap_t
    {
        public nuint chunk_count;                    // atomic; total count of chunks (0 < N <= MI_BCHUNKMAP_BITS)
        public nuint chunk_max_accessed;             // atomic; max chunk index that was once cleared or set
        public mi_subproc_t* subproc;                // constant, for stats
        private fixed ulong _padding[Mi.MI_BCHUNK_SIZE / Mi.MI_SIZE_SIZE - 3];
        public mi_bchunk_t chunkmap;
        public mi_bbitmap_chunkmap_bins_t chunkmap_bins;   // chunkmaps with bit set if the chunk is in that size class (excluding MI_CBIN_NONE)
        public mi_bitmap_chunks_t chunks;            // access via Mi.mi_bbitmap_chunk() only!
    }

    [InlineArray((int)mi_chunkbin_t.MI_CBIN_COUNT - 1)]
    internal struct mi_bbitmap_chunkmap_bins_t
    {
        private mi_bchunk_t _element0;
    }

    internal static unsafe partial class Mi
    {
        public const int MI_BFIELD_BITS_SHIFT = MI_SIZE_SHIFT + 3;             // 6
        public const int MI_BFIELD_BITS = 1 << MI_BFIELD_BITS_SHIFT;           // 64
        public const int MI_BFIELD_SIZE = MI_BFIELD_BITS / 8;                  // 8
        public const ulong MI_BFIELD_LO_BIT8 = ~0UL / 0xFF;                    // 0x01010101...
        public const ulong MI_BFIELD_HI_BIT8 = MI_BFIELD_LO_BIT8 << 7;         // 0x80808080...

        public const int MI_BCHUNK_SIZE = MI_BCHUNK_BITS / 8;                  // 64 bytes
        public const int MI_BCHUNK_FIELDS = MI_BCHUNK_BITS / MI_BFIELD_BITS;   // 8

        public const int MI_BCHUNKMAP_BITS = MI_BCHUNK_BITS;                   // 512
        public const int MI_BITMAP_MAX_CHUNK_COUNT = MI_BCHUNKMAP_BITS;        // 512
        public const int MI_BITMAP_MIN_CHUNK_COUNT = 1;
        public const int MI_BITMAP_DEFAULT_CHUNK_COUNT = 64;                   // 2 GiB on 64-bit -- this is for the page map
        public const int MI_BITMAP_MAX_BIT_COUNT = MI_BITMAP_MAX_CHUNK_COUNT * MI_BCHUNK_BITS;   // 16 GiB arena
        public const int MI_BITMAP_MIN_BIT_COUNT = MI_BITMAP_MIN_CHUNK_COUNT * MI_BCHUNK_BITS;   // 32 MiB arena
        public const int MI_BITMAP_DEFAULT_BIT_COUNT = MI_BITMAP_DEFAULT_CHUNK_COUNT * MI_BCHUNK_BITS;   // 2 GiB arena

        // Many operations are generic over setting or clearing a bit (mi_xset_t in C)
        public const bool MI_BIT_SET = true;
        public const bool MI_BIT_CLEAR = false;

        // byte offsets of the flexible `chunks` member (verified by tests against the layout)
        public const int MI_BITMAP_CHUNKS_OFFSET = 2 * MI_BCHUNK_SIZE;                                   // 128
        public const int MI_BBITMAP_CHUNKS_OFFSET = (2 + ((int)mi_chunkbin_t.MI_CBIN_COUNT - 1)) * MI_BCHUNK_SIZE;   // 448

        // C: `&bitmap->chunks[i]` -- the chunks array is dynamically sized so use pointer arithmetic
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_bchunk_t* mi_bitmap_chunk(mi_bitmap_t* bitmap, nuint chunk_idx)
            => (mi_bchunk_t*)((byte*)bitmap + MI_BITMAP_CHUNKS_OFFSET) + chunk_idx;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_bchunk_t* mi_bbitmap_chunk(mi_bbitmap_t* bbitmap, nuint chunk_idx)
            => (mi_bchunk_t*)((byte*)bbitmap + MI_BBITMAP_CHUNKS_OFFSET) + chunk_idx;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_bitmap_chunk_count(mi_bitmap_t* bitmap) => mi_atomic_load_relaxed(ref bitmap->chunk_count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_bitmap_max_bits(mi_bitmap_t* bitmap) => mi_bitmap_chunk_count(bitmap) * MI_BCHUNK_BITS;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_bbitmap_chunk_count(mi_bbitmap_t* bbitmap) => mi_atomic_load_relaxed(ref bbitmap->chunk_count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_bbitmap_max_bits(mi_bbitmap_t* bbitmap) => mi_bbitmap_chunk_count(bbitmap) * MI_BCHUNK_BITS;

        /* --------------------------------------------------------------------------------
          chunk bins (from bitmap.h)
        -------------------------------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_chunkbin_t mi_chunkbin_inc(mi_chunkbin_t bbin)
        {
            mi_assert_internal(bbin < mi_chunkbin_t.MI_CBIN_COUNT);
            return (mi_chunkbin_t)((int)bbin + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_chunkbin_t mi_chunkbin_dec(mi_chunkbin_t bbin)
        {
            mi_assert_internal(bbin > mi_chunkbin_t.MI_CBIN_NONE);
            return (mi_chunkbin_t)((int)bbin - 1);
        }

        public static mi_chunkbin_t mi_chunkbin_of(nuint slice_count)
        {
            if (slice_count == 1) return mi_chunkbin_t.MI_CBIN_SMALL;
            if (slice_count == 8) return mi_chunkbin_t.MI_CBIN_MEDIUM;
            // MI_ENABLE_LARGE_PAGES == 1
            if (slice_count == MI_BFIELD_BITS) return mi_chunkbin_t.MI_CBIN_LARGE;
            if (slice_count > MI_BCHUNK_BITS) return mi_chunkbin_t.MI_CBIN_HUGE;
            return mi_chunkbin_t.MI_CBIN_OTHER;
        }

        /* --------------------------------------------------------------------------------
          bfields
        -------------------------------------------------------------------------------- */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_ctz(nuint x) => mi_ctz(x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_clz(nuint x) => mi_clz(x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_popcount(nuint x) => mi_popcount(x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_clear_least_bit(nuint x) => x & (x - 1);

        // find the least significant bit that is set; false if `x==0`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_bfield_find_least_bit(nuint x, out nuint idx) => mi_bsf(x, out idx);

        // find the most significant bit that is set; false if `x==0`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_bfield_find_highest_bit(nuint x, out nuint idx) => mi_bsr(x, out idx);

        // find each set bit in a bit field `x` and clear it, until it becomes zero.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_bfield_foreach_bit(ref nuint x, out nuint idx)
        {
            bool found = mi_bfield_find_least_bit(x, out idx);
            x = mi_bfield_clear_least_bit(x);
            return found;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_zero() => 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_one() => 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_all_set() => ~(nuint)0;

        // mask of `bit_count` bits set shifted to the left by `shiftl`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint mi_bfield_mask(nuint bit_count, nuint shiftl)
        {
            mi_assert_internal(bit_count > 0);
            mi_assert_internal(bit_count + shiftl <= MI_BFIELD_BITS);
            mi_assert_internal(shiftl < MI_BFIELD_BITS);
            nuint mask0 = (bit_count < MI_BFIELD_BITS ? (mi_bfield_one() << (int)bit_count) - 1 : mi_bfield_all_set());
            return mask0 << (int)shiftl;
        }

        // ------- mi_bfield_atomic_set ---------------------------------------
        // the `_set` functions return also the count of bits that were already set (for commit statistics)
        // the `_clear` functions return also whether the new bfield is all clear or not (for the chunk_map)

        // Set a bit atomically. Returns `true` if the bit transitioned from 0 to 1
        private static bool mi_bfield_atomic_set(ref nuint b, nuint idx)
        {
            mi_assert_internal(idx < MI_BFIELD_BITS);
            nuint mask = mi_bfield_mask(1, idx);
            nuint old = mi_atomic_or_acq_rel(ref b, mask);
            return (old & mask) == 0;
        }

        // Clear a bit atomically. Returns `true` if the bit transitioned from 1 to 0.
        // `all_clear` is set if the new bfield is zero.
        private static bool mi_bfield_atomic_clear(ref nuint b, nuint idx, bool* all_clear)
        {
            mi_assert_internal(idx < MI_BFIELD_BITS);
            nuint mask = mi_bfield_mask(1, idx);
            nuint old = mi_atomic_and_acq_rel(ref b, ~mask);
            if (all_clear != null) { *all_clear = (old & ~mask) == 0; }
            return (old & mask) == mask;
        }

        // Clear a bit but only when/once it is set. This is used by concurrent free's while
        // the page is abandoned and mapped. This can incur a busy wait :-( but it should
        // happen almost never (and is accounted for in the stats)
        private static void mi_bfield_atomic_clear_once_set(mi_subproc_t* subproc, ref nuint b, nuint idx)
        {
            mi_assert_internal(idx < MI_BFIELD_BITS);
            nuint mask = mi_bfield_mask(1, idx);
            nuint old = mi_atomic_load_relaxed(ref b);
            do
            {
                if ((old & mask) == 0)
                {
                    old = mi_atomic_load_acquire(ref b);
                    if ((old & mask) == 0)
                    {
                        __mi_stat_counter_increase_mt(&subproc->stats.pages_unabandon_busy_wait, 1);
                    }
                    while ((old & mask) == 0)   // busy wait
                    {
                        _mi_prim_thread_yield();
                        old = mi_atomic_load_acquire(ref b);
                    }
                }
            } while (!mi_atomic_cas_weak_acq_rel(ref b, ref old, old & ~mask));
            mi_assert_internal((old & mask) == mask);  // we should only clear when it was set
        }

        // Set a mask set of bits atomically, and return true if the mask bits transitioned from all 0's to 1's.
        // `already_set` contains the count of bits that were already set.
        private static bool mi_bfield_atomic_set_mask(ref nuint b, nuint mask, nuint* already_set)
        {
            mi_assert_internal(mask != 0);
            nuint old = mi_atomic_load_relaxed(ref b);
            while (!mi_atomic_cas_weak_acq_rel(ref b, ref old, old | mask)) { }   // try to atomically set the mask bits until success
            if (already_set != null) { *already_set = mi_bfield_popcount(old & mask); }
            return (old & mask) == 0;
        }

        // Clear a mask set of bits atomically, and return true if the mask bits transitioned from all 1's to 0's
        // `all_clear` is set to `true` if the new bfield became zero.
        private static bool mi_bfield_atomic_clear_mask(ref nuint b, nuint mask, bool* all_clear)
        {
            mi_assert_internal(mask != 0);
            nuint old = mi_atomic_load_relaxed(ref b);
            while (!mi_atomic_cas_weak_acq_rel(ref b, ref old, old & ~mask)) { }  // try to atomically clear the mask bits until success
            if (all_clear != null) { *all_clear = (old & ~mask) == 0; }
            return (old & mask) == mask;
        }

        private static bool mi_bfield_atomic_setX(ref nuint b, nuint* already_set)
        {
            nuint old = mi_atomic_exchange_release(ref b, mi_bfield_all_set());
            if (already_set != null) { *already_set = mi_bfield_popcount(old); }
            return old == 0;
        }

        // ------- mi_bfield_atomic_try_clear ---------------------------------------

        // Tries to clear a mask atomically, and returns true if the mask bits atomically transitioned from mask to 0
        // and false otherwise (leaving the bit field as is).
        // `all_clear` is set to `true` if the new bfield became zero.
        private static bool mi_bfield_atomic_try_clear_mask_of(ref nuint b, nuint mask, nuint expect, bool* all_clear)
        {
            mi_assert_internal(mask != 0);
            // try to atomically clear the mask bits
            do
            {
                if ((expect & mask) != mask)   // are all bits still set?
                {
                    if (all_clear != null) { *all_clear = expect == 0; }
                    return false;
                }
            } while (!mi_atomic_cas_weak_acq_rel(ref b, ref expect, expect & ~mask));
            if (all_clear != null) { *all_clear = (expect & ~mask) == 0; }
            return true;
        }

        private static bool mi_bfield_atomic_try_clear_mask(ref nuint b, nuint mask, bool* all_clear)
        {
            mi_assert_internal(mask != 0);
            nuint expect = mi_atomic_load_relaxed(ref b);
            return mi_bfield_atomic_try_clear_mask_of(ref b, mask, expect, all_clear);
        }

        // Tries to clear a byte atomically, and returns true if the byte atomically transitioned from 0xFF to 0
        private static bool mi_bfield_atomic_try_clear8(ref nuint b, nuint idx, bool* all_clear)
        {
            mi_assert_internal(idx < MI_BFIELD_BITS);
            mi_assert_internal((idx % 8) == 0);
            nuint mask = (nuint)0xFF << (int)idx;
            return mi_bfield_atomic_try_clear_mask(ref b, mask, all_clear);
        }

        // Try to clear a full field of bits atomically, and return true if all bits transitioned from all 1's to 0's.
        private static bool mi_bfield_atomic_try_clearX(ref nuint b, bool* all_clear)
        {
            nuint old = mi_bfield_all_set();
            if (mi_atomic_cas_strong_acq_rel(ref b, ref old, mi_bfield_zero()))
            {
                if (all_clear != null) { *all_clear = true; }
                return true;
            }
            else return false;
        }

        // ------- mi_bfield_atomic_is_set ---------------------------------------

        private static bool mi_bfield_atomic_is_set(ref nuint b, nuint idx)
        {
            nuint x = mi_atomic_load_acquire(ref b);
            return (x & mi_bfield_mask(1, idx)) != 0;
        }

        private static bool mi_bfield_atomic_is_clear(ref nuint b, nuint idx)
        {
            nuint x = mi_atomic_load_acquire(ref b);
            return (x & mi_bfield_mask(1, idx)) == 0;
        }

        private static bool mi_bfield_atomic_is_xset(bool set, ref nuint b, nuint idx)
        {
            if (set) return mi_bfield_atomic_is_set(ref b, idx);
            else return mi_bfield_atomic_is_clear(ref b, idx);
        }

        private static bool mi_bfield_atomic_is_set_mask(ref nuint b, nuint mask)
        {
            mi_assert_internal(mask != 0);
            nuint x = mi_atomic_load_acquire(ref b);
            return (x & mask) == mask;
        }

        private static bool mi_bfield_atomic_is_clear_mask(ref nuint b, nuint mask)
        {
            mi_assert_internal(mask != 0);
            nuint x = mi_atomic_load_acquire(ref b);
            return (x & mask) == 0;
        }

        private static bool mi_bfield_atomic_is_xset_mask(bool set, ref nuint b, nuint mask)
        {
            mi_assert_internal(mask != 0);
            if (set) return mi_bfield_atomic_is_set_mask(ref b, mask);
            else return mi_bfield_atomic_is_clear_mask(ref b, mask);
        }

        // Count bits in a mask
        private static nuint mi_bfield_atomic_popcount_mask(ref nuint b, nuint mask)
        {
            nuint x = mi_atomic_load_acquire(ref b);
            return mi_bfield_popcount(x & mask);
        }

        /* --------------------------------------------------------------------------------
         bitmap chunks
        -------------------------------------------------------------------------------- */

        // ------- mi_bchunk_set ---------------------------------------

        // Set a single bit
        private static bool mi_bchunk_set(mi_bchunk_t* chunk, nuint cidx, nuint* already_set)
        {
            mi_assert_internal(cidx < MI_BCHUNK_BITS);
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            bool was_clear = mi_bfield_atomic_set(ref chunk->bfields[(int)i], idx);
            if (already_set != null) { *already_set = was_clear ? 0 : (nuint)1; }
            return was_clear;
        }

        // Set `0 < n <= MI_BFIELD_BITS`, and return true if the mask bits transitioned from all 0's to 1's.
        // Can cross over two bfields.
        private static bool mi_bchunk_setNX(mi_bchunk_t* chunk, nuint cidx, nuint n, nuint* already_set)
        {
            mi_assert_internal(cidx < MI_BCHUNK_BITS);
            mi_assert_internal(n > 0 && n <= MI_BFIELD_BITS);
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            if (idx + n <= MI_BFIELD_BITS)
            {
                // within one field
                return mi_bfield_atomic_set_mask(ref chunk->bfields[(int)i], mi_bfield_mask(n, idx), already_set);
            }
            else
            {
                // spanning two fields
                nuint m = MI_BFIELD_BITS - idx;  // bits to set in the first field
                mi_assert_internal(m < n);
                mi_assert_internal(i < MI_BCHUNK_FIELDS - 1);
                mi_assert_internal(idx + m <= MI_BFIELD_BITS);
                nuint already_set1;
                bool all_set1 = mi_bfield_atomic_set_mask(ref chunk->bfields[(int)i], mi_bfield_mask(m, idx), &already_set1);
                mi_assert_internal(n - m > 0);
                mi_assert_internal(n - m < MI_BFIELD_BITS);
                nuint already_set2;
                bool all_set2 = mi_bfield_atomic_set_mask(ref chunk->bfields[(int)i + 1], mi_bfield_mask(n - m, 0), &already_set2);
                if (already_set != null) { *already_set = already_set1 + already_set2; }
                return all_set1 && all_set2;
            }
        }

        // Set/clear a sequence of `n` bits within a chunk.
        // Returns true if all bits transitioned from 0 to 1 (or 1 to 0).
        private static bool mi_bchunk_xsetNC(bool set, mi_bchunk_t* chunk, nuint cidx, nuint n, nuint* palready_set, bool* pmaybe_all_clear)
        {
            mi_assert_internal(cidx + n <= MI_BCHUNK_BITS);
            mi_assert_internal(n > 0);
            bool all_transition = true;
            bool maybe_all_clear = true;
            nuint total_already_set = 0;
            nuint idx = cidx % MI_BFIELD_BITS;
            nuint field = cidx / MI_BFIELD_BITS;
            while (n > 0)
            {
                nuint m = MI_BFIELD_BITS - idx;   // m is the bits to xset in this field
                if (m > n) { m = n; }
                mi_assert_internal(idx + m <= MI_BFIELD_BITS);
                mi_assert_internal(field < MI_BCHUNK_FIELDS);
                nuint mask = mi_bfield_mask(m, idx);
                nuint already_set = 0;
                bool all_clear = false;
                bool transition = (set ? mi_bfield_atomic_set_mask(ref chunk->bfields[(int)field], mask, &already_set)
                                       : mi_bfield_atomic_clear_mask(ref chunk->bfields[(int)field], mask, &all_clear));
                mi_assert_internal(!set || ((transition && already_set == 0) || (!transition && already_set > 0)));
                all_transition = all_transition && transition;
                total_already_set += already_set;
                maybe_all_clear = maybe_all_clear && all_clear;
                // next field
                field++;
                idx = 0;
                mi_assert_internal(m <= n);
                n -= m;
            }
            if (palready_set != null) { *palready_set = total_already_set; }
            if (pmaybe_all_clear != null) { *pmaybe_all_clear = maybe_all_clear; }
            return all_transition;
        }

        private static bool mi_bchunk_setN(mi_bchunk_t* chunk, nuint cidx, nuint n, nuint* already_set)
        {
            mi_assert_internal(n > 0 && n <= MI_BCHUNK_BITS);
            if (n == 1) return mi_bchunk_set(chunk, cidx, already_set);
            if (n <= MI_BFIELD_BITS) return mi_bchunk_setNX(chunk, cidx, n, already_set);
            return mi_bchunk_xsetNC(MI_BIT_SET, chunk, cidx, n, already_set, null);
        }

        // ------- mi_bchunk_clear ---------------------------------------

        private static bool mi_bchunk_clear(mi_bchunk_t* chunk, nuint cidx, bool* all_clear)
        {
            mi_assert_internal(cidx < MI_BCHUNK_BITS);
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            return mi_bfield_atomic_clear(ref chunk->bfields[(int)i], idx, all_clear);
        }

        private static bool mi_bchunk_clearN(mi_bchunk_t* chunk, nuint cidx, nuint n, bool* maybe_all_clear)
        {
            mi_assert_internal(n > 0 && n <= MI_BCHUNK_BITS);
            if (n == 1) return mi_bchunk_clear(chunk, cidx, maybe_all_clear);
            return mi_bchunk_xsetNC(MI_BIT_CLEAR, chunk, cidx, n, null, maybe_all_clear);
        }

        // Count set bits in a sequence of `n` bits (crossing bfields).
        private static nuint mi_bchunk_popcountNC(mi_bchunk_t* chunk, nuint field_idx, nuint idx, nuint n)
        {
            mi_assert_internal((field_idx * MI_BFIELD_BITS) + idx + n <= MI_BCHUNK_BITS);
            nuint count = 0;
            while (n > 0)
            {
                nuint m = MI_BFIELD_BITS - idx;   // m is the bits to count in this field
                if (m > n) { m = n; }
                mi_assert_internal(idx + m <= MI_BFIELD_BITS);
                mi_assert_internal(field_idx < MI_BCHUNK_FIELDS);
                nuint mask = mi_bfield_mask(m, idx);
                count += mi_bfield_atomic_popcount_mask(ref chunk->bfields[(int)field_idx], mask);
                // next field
                field_idx++;
                idx = 0;
                n -= m;
            }
            return count;
        }

        // Count set bits in a sequence of `n` bits.
        private static nuint mi_bchunk_popcountN(mi_bchunk_t* chunk, nuint cidx, nuint n)
        {
            mi_assert_internal(cidx + n <= MI_BCHUNK_BITS);
            mi_assert_internal(n > 0);
            if (n == 0) return 0;
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            if (n == 1) { return mi_bfield_atomic_is_set(ref chunk->bfields[(int)i], idx) ? (nuint)1 : 0; }
            if (idx + n <= MI_BFIELD_BITS) { return mi_bfield_atomic_popcount_mask(ref chunk->bfields[(int)i], mi_bfield_mask(n, idx)); }
            return mi_bchunk_popcountNC(chunk, i, idx, n);
        }

        // ------- mi_bchunk_is_xset ---------------------------------------

        // Check if a sequence of `n` bits within a chunk are all set/cleared (crossing bfields)
        private static bool mi_bchunk_is_xsetNC(bool set, mi_bchunk_t* chunk, nuint field_idx, nuint idx, nuint n)
        {
            mi_assert_internal((field_idx * MI_BFIELD_BITS) + idx + n <= MI_BCHUNK_BITS);
            while (n > 0)
            {
                nuint m = MI_BFIELD_BITS - idx;   // m is the bits to test in this field
                if (m > n) { m = n; }
                mi_assert_internal(idx + m <= MI_BFIELD_BITS);
                mi_assert_internal(field_idx < MI_BCHUNK_FIELDS);
                nuint mask = mi_bfield_mask(m, idx);
                if (!mi_bfield_atomic_is_xset_mask(set, ref chunk->bfields[(int)field_idx], mask))
                {
                    return false;
                }
                // next field
                field_idx++;
                idx = 0;
                n -= m;
            }
            return true;
        }

        // Check if a sequence of `n` bits within a chunk are all set/cleared.
        private static bool mi_bchunk_is_xsetN(bool set, mi_bchunk_t* chunk, nuint cidx, nuint n)
        {
            mi_assert_internal(cidx + n <= MI_BCHUNK_BITS);
            mi_assert_internal(n > 0);
            if (n == 0) return true;
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            if (n == 1) { return mi_bfield_atomic_is_xset(set, ref chunk->bfields[(int)i], idx); }
            if (idx + n <= MI_BFIELD_BITS) { return mi_bfield_atomic_is_xset_mask(set, ref chunk->bfields[(int)i], mi_bfield_mask(n, idx)); }
            return mi_bchunk_is_xsetNC(set, chunk, i, idx, n);
        }

        // ------- mi_bchunk_try_clear  ---------------------------------------

        // Clear `0 < n <= MI_BFIELD_BITS`. Can cross over a bfield boundary.
        private static bool mi_bchunk_try_clearNX(mi_bchunk_t* chunk, nuint cidx, nuint n, bool* pmaybe_all_clear, bool* did_temp_clear_bits)
        {
            mi_assert_internal(cidx < MI_BCHUNK_BITS);
            mi_assert_internal(n <= MI_BFIELD_BITS);
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            if (idx + n <= MI_BFIELD_BITS)
            {
                // within one field
                return mi_bfield_atomic_try_clear_mask(ref chunk->bfields[(int)i], mi_bfield_mask(n, idx), pmaybe_all_clear);
            }
            else
            {
                // spanning two fields
                nuint m = MI_BFIELD_BITS - idx;  // bits to clear in the first field
                mi_assert_internal(m < n);
                mi_assert_internal(i < MI_BCHUNK_FIELDS - 1);
                bool field1_is_clear;
                if (!mi_bfield_atomic_try_clear_mask(ref chunk->bfields[(int)i], mi_bfield_mask(m, idx), &field1_is_clear)) return false;
                // try the second field as well
                mi_assert_internal(n - m > 0);
                mi_assert_internal(n - m < MI_BFIELD_BITS);
                bool field2_is_clear;
                if (!mi_bfield_atomic_try_clear_mask(ref chunk->bfields[(int)i + 1], mi_bfield_mask(n - m, 0), &field2_is_clear))
                {
                    // we failed to clear the second field, restore the first one
                    mi_bfield_atomic_set_mask(ref chunk->bfields[(int)i], mi_bfield_mask(m, idx), null);
                    if (did_temp_clear_bits != null) { *did_temp_clear_bits = true; }
                    return false;
                }
                if (pmaybe_all_clear != null) { *pmaybe_all_clear = field1_is_clear && field2_is_clear; }
                return true;
            }
        }

        // Try to atomically clear a sequence of `n` bits within a chunk.
        // Returns true if all bits transitioned from 1 to 0, and false otherwise leaving all bit fields as is.
        // Note: this is the complex one as we need to unwind partial atomic operations if we fail halfway..
        // `maybe_all_clear` is set to `true` if all the bfields involved become zero.
        private static bool mi_bchunk_try_clearNC(mi_bchunk_t* chunk, nuint cidx, nuint n, bool* pmaybe_all_clear, bool* did_temp_clear_bits)
        {
            mi_assert_internal(cidx + n <= MI_BCHUNK_BITS);
            mi_assert_internal(n > 0);
            if (pmaybe_all_clear != null) { *pmaybe_all_clear = true; }
            if (n == 0) return true;

            // first field
            nuint start_idx = cidx % MI_BFIELD_BITS;
            nuint start_field = cidx / MI_BFIELD_BITS;
            nuint field = start_field;
            nuint m = MI_BFIELD_BITS - start_idx;   // m are the bits to clear in this field
            if (m > n) { m = n; }
            mi_assert_internal(start_idx + m <= MI_BFIELD_BITS);
            mi_assert_internal(start_field < MI_BCHUNK_FIELDS);
            nuint mask_start = mi_bfield_mask(m, start_idx);
            bool maybe_all_clear;
            if (!mi_bfield_atomic_try_clear_mask(ref chunk->bfields[(int)field], mask_start, &maybe_all_clear)) return false;

            // done?
            mi_assert_internal(m <= n);
            n -= m;

            // continue with mid fields and last field: if these fail we need to recover by unsetting previous fields
            // mid fields?
            while (n >= MI_BFIELD_BITS)
            {
                field++;
                mi_assert_internal(field < MI_BCHUNK_FIELDS);
                bool field_is_clear;
                if (!mi_bfield_atomic_try_clearX(ref chunk->bfields[(int)field], &field_is_clear)) goto restore;
                maybe_all_clear = maybe_all_clear && field_is_clear;
                n -= MI_BFIELD_BITS;
            }

            // last field?
            if (n > 0)
            {
                mi_assert_internal(n < MI_BFIELD_BITS);
                field++;
                mi_assert_internal(field < MI_BCHUNK_FIELDS);
                nuint mask_end = mi_bfield_mask(n, 0);
                bool field_is_clear;
                if (!mi_bfield_atomic_try_clear_mask(ref chunk->bfields[(int)field], mask_end, &field_is_clear)) goto restore;
                maybe_all_clear = maybe_all_clear && field_is_clear;
            }

            if (pmaybe_all_clear != null) { *pmaybe_all_clear = maybe_all_clear; }
            return true;

        restore:
            // `field` is the index of the field that failed to clear atomically; we need to restore all previous fields
            mi_assert_internal(field > start_field);
            if (did_temp_clear_bits != null) { *did_temp_clear_bits = true; }
            while (field > start_field)
            {
                field--;
                if (field == start_field)
                {
                    mi_bfield_atomic_set_mask(ref chunk->bfields[(int)field], mask_start, null);
                }
                else
                {
                    mi_bfield_atomic_setX(ref chunk->bfields[(int)field], null);  // mid-field: set all bits again
                }
            }
            return false;
        }

        private static bool mi_bchunk_try_clearN(mi_bchunk_t* chunk, nuint cidx, nuint n, bool* maybe_all_clear, bool* did_temp_clear_bits)
        {
            mi_assert_internal(n > 0);
            if (n <= MI_BFIELD_BITS) return mi_bchunk_try_clearNX(chunk, cidx, n, maybe_all_clear, did_temp_clear_bits);
            return mi_bchunk_try_clearNC(chunk, cidx, n, maybe_all_clear, did_temp_clear_bits);
        }

        // ------- mi_bchunk_try_find_and_clear ---------------------------------------

        private static bool mi_bchunk_try_find_and_clear_at(mi_bchunk_t* chunk, nuint chunk_idx, nuint* pidx)
        {
            mi_assert_internal(chunk_idx < MI_BCHUNK_FIELDS);
            // note: this must be acquire (and not relaxed) -- see the C source
            nuint b = mi_atomic_load_acquire(ref chunk->bfields[(int)chunk_idx]);
            if (mi_bfield_find_least_bit(b, out nuint idx))            // find the least bit
            {
                if (mi_bfield_atomic_try_clear_mask_of(ref chunk->bfields[(int)chunk_idx], mi_bfield_mask(1, idx), b, null))   // clear it atomically
                {
                    *pidx = (chunk_idx * MI_BFIELD_BITS) + idx;
                    mi_assert_internal(*pidx < MI_BCHUNK_BITS);
                    return true;
                }
            }
            return false;
        }

        // Find least 1-bit in a chunk and try to clear it atomically
        // set `*pidx` to the bit index (0 <= *pidx < MI_BCHUNK_BITS) on success.
        // This is used to find free slices and abandoned pages and should be efficient.
        private static bool mi_bchunk_try_find_and_clear(mi_bchunk_t* chunk, nuint* pidx)
        {
            for (int i = 0; i < MI_BCHUNK_FIELDS; i++)
            {
                if (mi_bchunk_try_find_and_clear_at(chunk, (nuint)i, pidx)) return true;
            }
            return false;
        }

        private static bool mi_bchunk_try_find_and_clear_1(mi_bchunk_t* chunk, nuint n, nuint* pidx, bool* did_temp_clear_bits)
        {
            mi_assert_internal(n == 1);
            return mi_bchunk_try_find_and_clear(chunk, pidx);
        }

        private static bool mi_bchunk_try_find_and_clear8_at(mi_bchunk_t* chunk, nuint chunk_idx, nuint* pidx)
        {
            nuint b = mi_atomic_load_relaxed(ref chunk->bfields[(int)chunk_idx]);
            // has_set8 has low bit in each byte set if the byte in x == 0xFF
            nuint has_set8 =
                ((~b - unchecked((nuint)MI_BFIELD_LO_BIT8)) &     // high bit set if byte in x is 0xFF or < 0x7F
                 (b & unchecked((nuint)MI_BFIELD_HI_BIT8)))       // high bit set if byte in x is >= 0x80
                 >> 7;                                            // shift high bit to low bit
            if (mi_bfield_find_least_bit(has_set8, out nuint idx))   // find least 1-bit
            {
                mi_assert_internal(idx <= MI_BFIELD_BITS - 8);
                mi_assert_internal((idx % 8) == 0);
                if (mi_bfield_atomic_try_clear_mask_of(ref chunk->bfields[(int)chunk_idx], (nuint)0xFF << (int)idx, b, null))   // unset the byte atomically
                {
                    *pidx = (chunk_idx * MI_BFIELD_BITS) + idx;
                    mi_assert_internal(*pidx + 8 <= MI_BCHUNK_BITS);
                    return true;
                }
            }
            return false;
        }

        // find least aligned byte in a chunk with all bits set, and try unset it atomically
        // Used to find medium size pages in the free blocks.
        private static bool mi_bchunk_try_find_and_clear8(mi_bchunk_t* chunk, nuint* pidx)
        {
            for (int i = 0; i < MI_BCHUNK_FIELDS; i++)
            {
                if (mi_bchunk_try_find_and_clear8_at(chunk, (nuint)i, pidx)) return true;
            }
            return false;
        }

        private static bool mi_bchunk_try_find_and_clear_8(mi_bchunk_t* chunk, nuint n, nuint* pidx, bool* did_temp_clear_bits)
        {
            mi_assert_internal(n == 8);
            return mi_bchunk_try_find_and_clear8(chunk, pidx);
        }

        // find a sequence of `n` bits in a chunk with `0 < n <= MI_BFIELD_BITS` with all bits set,
        // and try to clear them atomically. Will cross bfield boundaries.
        private static bool mi_bchunk_try_find_and_clearNX(mi_bchunk_t* chunk, nuint n, nuint* pidx, bool* did_temp_clear_bits)
        {
            if (n == 0 || n > MI_BFIELD_BITS) return false;
            nuint mask = mi_bfield_mask(n, 0);
            // for all fields in the chunk
            for (int i = 0; i < MI_BCHUNK_FIELDS; i++)
            {
                nuint b0 = mi_atomic_load_relaxed(ref chunk->bfields[i]);
                nuint b = b0;
                nuint idx;

                // is there a range inside the field?
                while (mi_bfield_find_least_bit(b, out idx))   // find least 1-bit
                {
                    if (idx + n > MI_BFIELD_BITS) break;       // too short: maybe cross over, or continue with the next field

                    nuint bmask = mask << (int)idx;
                    mi_assert_internal(bmask >> (int)idx == mask);
                    if ((b & bmask) == bmask)   // found a match with all bits set, try clearing atomically
                    {
                        if (mi_bfield_atomic_try_clear_mask_of(ref chunk->bfields[i], bmask, b0, null))
                        {
                            *pidx = ((nuint)i * MI_BFIELD_BITS) + idx;
                            mi_assert_internal(*pidx < MI_BCHUNK_BITS);
                            mi_assert_internal(*pidx + n <= MI_BCHUNK_BITS);
                            return true;
                        }
                        else
                        {
                            // if we failed to atomically commit, reload b and try again from the start
                            b = b0 = mi_atomic_load_acquire(ref chunk->bfields[i]);
                        }
                    }
                    else
                    {
                        // advance by clearing the least run of ones, for example, with n>=4, idx=2:
                        // b             = 1111 1101 1010 1100
                        // .. + (1<<idx) = 1111 1101 1011 0000
                        // .. & b        = 1111 1101 1010 0000
                        b = b & (b + (mi_bfield_one() << (int)idx));
                    }
                }

                // check if we can cross into the next bfield
                if (b != 0 && i < MI_BCHUNK_FIELDS - 1)
                {
                    nuint post = mi_bfield_clz(~b);
                    if (post > 0)
                    {
                        nuint pre = mi_bfield_ctz(~mi_atomic_load_relaxed(ref chunk->bfields[i + 1]));
                        if (post + pre >= n)
                        {
                            // it fits -- try to claim it atomically
                            nuint cidx = ((nuint)i * MI_BFIELD_BITS) + (MI_BFIELD_BITS - post);
                            if (mi_bchunk_try_clearNX(chunk, cidx, n, null, did_temp_clear_bits))
                            {
                                // we cleared all atomically
                                *pidx = cidx;
                                mi_assert_internal(*pidx < MI_BCHUNK_BITS);
                                mi_assert_internal(*pidx + n <= MI_BCHUNK_BITS);
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        // find a sequence of `n` bits in a chunk with `n <= MI_BCHUNK_BITS` with all bits set,
        // and try to clear them atomically. This can cross bfield boundaries.
        private static bool mi_bchunk_try_find_and_clearNC(mi_bchunk_t* chunk, nuint n, nuint* pidx, bool* did_temp_clear_bits)
        {
            if (n == 0 || n > MI_BCHUNK_BITS) return false;  // cannot be more than a chunk

            // we first scan ahead to see if there is a range of `n` set bits, and only then try to clear atomically
            mi_assert_internal(n > 0);
            nuint skip_count = (n - 1) / MI_BFIELD_BITS;
            nuint cidx;
            for (nuint i = 0; i < MI_BCHUNK_FIELDS - skip_count; i++)
            {
                nuint m = n;   // bits to go

                // first field
                nuint b = mi_atomic_load_relaxed(ref chunk->bfields[(int)i]);
                nuint ones = mi_bfield_clz(~b);

                cidx = (i * MI_BFIELD_BITS) + (MI_BFIELD_BITS - ones);  // start index
                if (ones >= m)
                {
                    // we found enough bits already!
                    m = 0;
                }
                else if (ones > 0)
                {
                    // keep scanning further fields until we have enough bits
                    m -= ones;
                    nuint j = 1;   // field count from i
                    while (i + j < MI_BCHUNK_FIELDS)
                    {
                        mi_assert_internal(m > 0);
                        b = mi_atomic_load_relaxed(ref chunk->bfields[(int)(i + j)]);
                        ones = mi_bfield_ctz(~b);
                        if (ones >= m)
                        {
                            // we found enough bits
                            m = 0;
                            break;
                        }
                        else if (ones == MI_BFIELD_BITS)
                        {
                            // not enough yet, proceed to the next field
                            j++;
                            m -= MI_BFIELD_BITS;
                        }
                        else
                        {
                            // the range was not enough, start from scratch
                            i = i + j - 1;  // no need to re-scan previous fields, except the last one (with clz this time)
                            mi_assert_internal(m > 0);
                            break;
                        }
                    }
                }

                // did we find a range?
                if (m == 0)
                {
                    if (mi_bchunk_try_clearN(chunk, cidx, n, null, did_temp_clear_bits))
                    {
                        // we cleared all atomically
                        *pidx = cidx;
                        mi_assert_internal(*pidx < MI_BCHUNK_BITS);
                        mi_assert_internal(*pidx + n <= MI_BCHUNK_BITS);
                        return true;
                    }
                    // note: if we fail for a small `n` on the first field, we don't rescan that field (as `i` is incremented)
                }
                // otherwise continue searching
            }
            return false;
        }

        // ------- mi_bchunk_clear_once_set ---------------------------------------

        private static void mi_bchunk_clear_once_set(mi_subproc_t* subproc, mi_bchunk_t* chunk, nuint cidx)
        {
            mi_assert_internal(cidx < MI_BCHUNK_BITS);
            nuint i = cidx / MI_BFIELD_BITS;
            nuint idx = cidx % MI_BFIELD_BITS;
            mi_bfield_atomic_clear_once_set(subproc, ref chunk->bfields[(int)i], idx);
        }

        // ------- mi_bitmap_all_are_clear ---------------------------------------

        // are all bits in a bitmap chunk clear?
        private static bool mi_bchunk_all_are_clear_relaxed(mi_bchunk_t* chunk)
        {
            for (int i = 0; i < MI_BCHUNK_FIELDS; i++)
            {
                if (mi_atomic_load_relaxed(ref chunk->bfields[i]) != 0) return false;
            }
            return true;
        }

        // are all bits in a bitmap chunk set?
        private static bool mi_bchunk_all_are_set_relaxed(mi_bchunk_t* chunk)
        {
            for (int i = 0; i < MI_BCHUNK_FIELDS; i++)
            {
                if (~mi_atomic_load_relaxed(ref chunk->bfields[i]) != 0) return false;
            }
            return true;
        }

        private static bool mi_bchunk_bsr(mi_bchunk_t* chunk, nuint* pidx)
        {
            for (nuint i = MI_BCHUNK_FIELDS; i > 0;)
            {
                i--;
                nuint b = mi_atomic_load_relaxed(ref chunk->bfields[(int)i]);
                if (mi_bsr(b, out nuint idx))
                {
                    *pidx = (i * MI_BFIELD_BITS) + idx;
                    return true;
                }
            }
            return false;
        }

        private static bool mi_bchunk_bsr_inv(mi_bchunk_t* chunk, nuint* pidx)
        {
            for (nuint i = MI_BCHUNK_FIELDS; i > 0;)
            {
                i--;
                nuint b = mi_atomic_load_relaxed(ref chunk->bfields[(int)i]);
                if (mi_bsr(~b, out nuint idx))
                {
                    *pidx = (i * MI_BFIELD_BITS) + idx;
                    return true;
                }
            }
            return false;
        }

        private static nuint mi_bchunk_popcount(mi_bchunk_t* chunk)
        {
            nuint popcount = 0;
            for (int i = 0; i < MI_BCHUNK_FIELDS; i++)
            {
                nuint b = mi_atomic_load_relaxed(ref chunk->bfields[i]);
                popcount += mi_bfield_popcount(b);
            }
            return popcount;
        }

        /* --------------------------------------------------------------------------------
         bitmap chunkmap
        -------------------------------------------------------------------------------- */

        private static void mi_bitmap_chunkmap_set(mi_bitmap_t* bitmap, nuint chunk_idx)
        {
            mi_assert(chunk_idx < mi_bitmap_chunk_count(bitmap));
            mi_bchunk_set(&bitmap->chunkmap, chunk_idx, null);
        }

        private static bool mi_bitmap_chunkmap_try_clear(mi_bitmap_t* bitmap, nuint chunk_idx)
        {
            mi_assert(chunk_idx < mi_bitmap_chunk_count(bitmap));
            // check if the corresponding chunk is all clear
            if (!mi_bchunk_all_are_clear_relaxed(mi_bitmap_chunk(bitmap, chunk_idx))) return false;
            // clear the chunkmap bit
            mi_bchunk_clear(&bitmap->chunkmap, chunk_idx, null);
            // .. but a concurrent set may have happened in between our all-clear test and the clearing of the
            // bit in the mask. We check again to catch this situation.
            if (!mi_bchunk_all_are_clear_relaxed(mi_bitmap_chunk(bitmap, chunk_idx)))
            {
                mi_bchunk_set(&bitmap->chunkmap, chunk_idx, null);
                return false;
            }
            return true;
        }

        /* --------------------------------------------------------------------------------
          bitmap
        -------------------------------------------------------------------------------- */

        public static nuint mi_bitmap_size(nuint bit_count, nuint* pchunk_count)
        {
            mi_assert_internal((bit_count % MI_BCHUNK_BITS) == 0);
            bit_count = _mi_align_up(bit_count, MI_BCHUNK_BITS);
            mi_assert_internal(bit_count <= MI_BITMAP_MAX_BIT_COUNT);
            mi_assert_internal(bit_count > 0);
            nuint chunk_count = bit_count / MI_BCHUNK_BITS;
            mi_assert_internal(chunk_count >= 1);
            nuint size = MI_BITMAP_CHUNKS_OFFSET + (chunk_count * MI_BCHUNK_SIZE);
            mi_assert_internal((size % MI_BCHUNK_SIZE) == 0);
            if (pchunk_count != null) { *pchunk_count = chunk_count; }
            return size;
        }

        // initialize a bitmap to all unset; avoid a mem_zero if `already_zero` is true
        // returns the size of the bitmap
        public static nuint mi_bitmap_init(mi_bitmap_t* bitmap, nuint bit_count, bool already_zero)
        {
            nuint chunk_count;
            nuint size = mi_bitmap_size(bit_count, &chunk_count);
            if (!already_zero)
            {
                _mi_memzero_aligned(bitmap, size);
            }
            mi_atomic_store_release(ref bitmap->chunk_count, chunk_count);
            mi_assert_internal(mi_atomic_load_relaxed(ref bitmap->chunk_count) <= MI_BITMAP_MAX_CHUNK_COUNT);
            return size;
        }

        // Set a sequence of `n` bits in the bitmap (and can cross chunks). Not atomic so only use if local to a thread.
        private static void mi_bchunks_unsafe_setN(mi_bchunk_t* chunks, mi_bchunk_t* cmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);

            // start chunk and index
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            nuint ccount = _mi_divide_up(n, MI_BCHUNK_BITS);

            // first update the chunkmap
            mi_bchunk_setN(cmap, chunk_idx, ccount, null);

            // first chunk
            nuint m = MI_BCHUNK_BITS - cidx;
            if (m > n) { m = n; }
            mi_bchunk_setN(&chunks[chunk_idx], cidx, m, null);

            // n can be large so use memset for efficiency for all in-between chunks
            chunk_idx++;
            n -= m;
            nuint mid_chunks = n / MI_BCHUNK_BITS;
            if (mid_chunks > 0)
            {
                _mi_memset(&chunks[chunk_idx], 0xFF, mid_chunks * MI_BCHUNK_SIZE);
                chunk_idx += mid_chunks;
                n -= mid_chunks * MI_BCHUNK_BITS;
            }

            // last chunk
            if (n > 0)
            {
                mi_assert_internal(n < MI_BCHUNK_BITS);
                mi_bchunk_setN(&chunks[chunk_idx], 0, n, null);
            }
        }

        // Set a sequence of `n` bits in the bitmap (and can cross chunks). Not atomic so only use if local to a thread.
        public static void mi_bitmap_unsafe_setN(mi_bitmap_t* bitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            mi_assert_internal(idx + n <= mi_bitmap_max_bits(bitmap));
            mi_bchunks_unsafe_setN(mi_bitmap_chunk(bitmap, 0), &bitmap->chunkmap, idx, n);
        }

        // ------- mi_bitmap_xset ---------------------------------------

        // Set a sequence of `n` bits in the bitmap; returns `true` if atomically transitioned from 0's to 1's
        public static bool mi_bitmap_setN(mi_bitmap_t* bitmap, nuint idx, nuint n, nuint* palready_set)
        {
            mi_assert_internal(n > 0);
            nuint maxbits = mi_bitmap_max_bits(bitmap);
            mi_assert_internal(idx + n <= maxbits);
            if (idx + n > maxbits)   // paranoia
            {
                if (idx >= maxbits) return false;
                n = maxbits - idx;
            }

            // iterate through the chunks
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            bool were_allclear = true;
            nuint already_set = 0;
            while (n > 0)
            {
                nuint m = (cidx + n > MI_BCHUNK_BITS ? MI_BCHUNK_BITS - cidx : n);
                nuint _already_set = 0;
                were_allclear = mi_bchunk_setN(mi_bitmap_chunk(bitmap, chunk_idx), cidx, m, &_already_set) && were_allclear;
                already_set += _already_set;
                mi_bitmap_chunkmap_set(bitmap, chunk_idx); // set afterwards
                mi_assert_internal(m <= n);
                n -= m;
                cidx = 0;
                chunk_idx++;
            }
            if (palready_set != null) { *palready_set = already_set; }
            return were_allclear;
        }

        // Clear a sequence of `n` bits in the bitmap; returns `true` if atomically transitioned from 1's to 0's.
        public static bool mi_bitmap_clearN(mi_bitmap_t* bitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            nuint maxbits = mi_bitmap_max_bits(bitmap);
            mi_assert_internal(idx + n <= maxbits);
            if (idx + n > maxbits)   // paranoia
            {
                if (idx >= maxbits) return false;
                n = maxbits - idx;
            }

            // iterate through the chunks
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            bool were_allset = true;
            while (n > 0)
            {
                nuint m = (cidx + n > MI_BCHUNK_BITS ? MI_BCHUNK_BITS - cidx : n);
                bool maybe_all_clear = false;
                were_allset = mi_bchunk_clearN(mi_bitmap_chunk(bitmap, chunk_idx), cidx, m, &maybe_all_clear) && were_allset;
                if (maybe_all_clear) { mi_bitmap_chunkmap_try_clear(bitmap, chunk_idx); }
                mi_assert_internal(m <= n);
                n -= m;
                cidx = 0;
                chunk_idx++;
            }
            return were_allset;
        }

        // Count bits set in a range of `n` bits.
        public static nuint mi_bitmap_popcountN(mi_bitmap_t* bitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            nuint maxbits = mi_bitmap_max_bits(bitmap);
            mi_assert_internal(idx + n <= maxbits);
            if (idx + n > maxbits)   // paranoia
            {
                if (idx >= maxbits) return 0;
                n = maxbits - idx;
            }

            // iterate through the chunks
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            nuint popcount = 0;
            while (n > 0)
            {
                nuint m = (cidx + n > MI_BCHUNK_BITS ? MI_BCHUNK_BITS - cidx : n);
                popcount += mi_bchunk_popcountN(mi_bitmap_chunk(bitmap, chunk_idx), cidx, m);
                mi_assert_internal(m <= n);
                n -= m;
                cidx = 0;
                chunk_idx++;
            }
            return popcount;
        }

        // Set/clear a bit in the bitmap; returns `true` if atomically transitioned from 0 to 1 (or 1 to 0)
        public static bool mi_bitmap_set(mi_bitmap_t* bitmap, nuint idx) => mi_bitmap_setN(bitmap, idx, 1, null);

        public static bool mi_bitmap_clear(mi_bitmap_t* bitmap, nuint idx) => mi_bitmap_clearN(bitmap, idx, 1);

        // ------- mi_bitmap_is_xset ---------------------------------------

        // Is a sequence of n bits already all set/cleared?
        public static bool mi_bitmap_is_xsetN(bool set, mi_bitmap_t* bitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            nuint maxbits = mi_bitmap_max_bits(bitmap);
            mi_assert_internal(idx + n <= maxbits);
            if (idx + n > maxbits)   // paranoia
            {
                if (idx >= maxbits) return false;
                n = maxbits - idx;
            }

            // iterate through the chunks
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            bool xset = true;
            while (n > 0 && xset)
            {
                nuint m = (cidx + n > MI_BCHUNK_BITS ? MI_BCHUNK_BITS - cidx : n);
                xset = mi_bchunk_is_xsetN(set, mi_bitmap_chunk(bitmap, chunk_idx), cidx, m) && xset;
                mi_assert_internal(m <= n);
                n -= m;
                cidx = 0;
                chunk_idx++;
            }
            return xset;
        }

        public static bool mi_bitmap_is_all_clear(mi_bitmap_t* bitmap)
            => mi_bitmap_is_xsetN(MI_BIT_CLEAR, bitmap, 0, mi_bitmap_max_bits(bitmap));

        public static bool mi_bitmap_is_setN(mi_bitmap_t* bitmap, nuint idx, nuint n) => mi_bitmap_is_xsetN(MI_BIT_SET, bitmap, idx, n);

        public static bool mi_bitmap_is_clearN(mi_bitmap_t* bitmap, nuint idx, nuint n) => mi_bitmap_is_xsetN(MI_BIT_CLEAR, bitmap, idx, n);

        public static bool mi_bitmap_is_set(mi_bitmap_t* bitmap, nuint idx) => mi_bitmap_is_setN(bitmap, idx, 1);

        public static bool mi_bitmap_is_clear(mi_bitmap_t* bitmap, nuint idx) => mi_bitmap_is_clearN(bitmap, idx, 1);

        /* --------------------------------------------------------------------------------
          Iterate through a bfield (C: the mi_bfield_cycle_iterate macros)
        -------------------------------------------------------------------------------- */

        // Cycle iteration through a bitfield. This is used to space out threads
        // so there is less chance of contention.
        // We iterate through the bitfield as:
        // first: [start, cycle>
        // then : [0, start>
        // then : [cycle, MI_BFIELD_BITS>
        // where `start == tseq % cycle`.
        private struct mi_bfield_cycle_iter
        {
            private readonly nuint _bfield;
            private readonly nuint _cycle_mask;
            private nuint _bcount;
            private nuint _b;

            public mi_bfield_cycle_iter(nuint bfield, nuint tseq, nuint cycle)
            {
                nuint start = (nuint)((uint)tseq % (uint)cycle);
                mi_assert_internal(start <= cycle);
                mi_assert_internal(start < MI_BFIELD_BITS);
                mi_assert_internal(cycle <= MI_BFIELD_BITS);
                _bfield = bfield;
                _cycle_mask = mi_bfield_mask(cycle - start, start);
                _bcount = mi_bfield_popcount(bfield);
                _b = bfield & _cycle_mask;   // process [start, cycle> first
            }

            public bool TryNext(out nuint idx)
            {
                if (_bcount == 0) { idx = 0; return false; }
                _bcount--;
                if (_b == 0) { _b = _bfield & ~_cycle_mask; }   // process [0,start> + [cycle, MI_BFIELD_BITS> next
                bool found = mi_bfield_find_least_bit(_b, out idx);
                _b = mi_bfield_clear_least_bit(_b);   // clear early so `continue` works
                mi_assert_internal(found);
                return true;
            }
        }

        /* --------------------------------------------------------------------------------
          mi_bitmap_find  (used to find free pages)
        -------------------------------------------------------------------------------- */

        // Go through the bitmap and for every sequence of `n` set bits, call the visitor function.
        // If it returns `true` stop the search.
        private static bool mi_bitmap_find(mi_bitmap_t* bitmap, nuint tseq, nuint n, nuint* pidx,
            delegate*<mi_bitmap_t*, nuint, nuint, nuint*, void*, void*, bool> on_find, void* arg1, void* arg2)
        {
            nuint chunkmap_max = _mi_divide_up(mi_bitmap_chunk_count(bitmap), MI_BFIELD_BITS);
            for (nuint i = 0; i < chunkmap_max; i++)
            {
                // and for each chunkmap entry we iterate over its bits to find the chunks
                nuint cmap_entry = mi_atomic_load_relaxed(ref bitmap->chunkmap.bfields[(int)i]);
                if (mi_bfield_find_highest_bit(cmap_entry, out nuint hi))
                {
                    // reduce the tseq to 8 bins to reduce using extra memory (see `mstress`)
                    var iterY = new mi_bfield_cycle_iter(cmap_entry, tseq % 8, hi + 1);
                    while (iterY.TryNext(out nuint eidx))
                    {
                        mi_assert_internal(eidx <= MI_BFIELD_BITS);
                        nuint chunk_idx = i * MI_BFIELD_BITS + eidx;
                        mi_assert_internal(chunk_idx < mi_bitmap_chunk_count(bitmap));
                        if (on_find(bitmap, chunk_idx, n, pidx, arg1, arg2))
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /* --------------------------------------------------------------------------------
          Bitmap: try_find_and_claim  -- used to allocate abandoned pages
        -------------------------------------------------------------------------------- */

        private struct mi_claim_fun_data_t
        {
            public mi_arena_t* arena;
        }

        private static bool mi_bitmap_try_find_and_claim_visit(mi_bitmap_t* bitmap, nuint chunk_idx, nuint n, nuint* pidx, void* arg1, void* arg2)
        {
            mi_assert_internal(n == 1);
            var claim_fun = (delegate*<nuint, mi_arena_t*, bool*, bool>)arg1;
            mi_claim_fun_data_t* claim_data = (mi_claim_fun_data_t*)arg2;
            nuint cidx;
            if (mi_bchunk_try_find_and_clear(mi_bitmap_chunk(bitmap, chunk_idx), &cidx))
            {
                nuint slice_index = (chunk_idx * MI_BCHUNK_BITS) + cidx;
                mi_assert_internal(slice_index < mi_bitmap_max_bits(bitmap));
                bool keep_set = true;
                if (claim_fun(slice_index, claim_data->arena, &keep_set))
                {
                    // success!
                    mi_assert_internal(!keep_set);
                    *pidx = slice_index;
                    return true;
                }
                else
                {
                    // failed to claim it, set abandoned mapping again (unless the page was freed and keep_set will be false)
                    if (keep_set)
                    {
                        bool wasclear = mi_bchunk_set(mi_bitmap_chunk(bitmap, chunk_idx), cidx, null);
                        mi_bitmap_chunkmap_set(bitmap, chunk_idx);
                        mi_assert_internal(wasclear);
                    }
                }
            }
            else
            {
                // we may find that all are cleared only on a second iteration but that is ok as
                // the chunkmap is a conservative approximation.
                mi_bitmap_chunkmap_try_clear(bitmap, chunk_idx);
            }
            return false;
        }

        // Find a set bit in the bitmap and try to atomically clear it and claim it.
        // (Used to find pages in the pages_abandoned bitmaps.)
        public static bool mi_bitmap_try_find_and_claim(mi_bitmap_t* bitmap, nuint tseq, nuint* pidx,
            delegate*<nuint, mi_arena_t*, bool*, bool> claim, mi_arena_t* arena)
        {
            mi_claim_fun_data_t claim_data;
            claim_data.arena = arena;
            return mi_bitmap_find(bitmap, tseq, 1, pidx, &mi_bitmap_try_find_and_claim_visit, (void*)claim, &claim_data);
        }

        public static bool mi_bitmap_bsr(mi_bitmap_t* bitmap, nuint* idx)
        {
            nuint chunkmap_max = _mi_divide_up(mi_bitmap_chunk_count(bitmap), MI_BFIELD_BITS);
            for (nuint i = chunkmap_max; i > 0;)
            {
                i--;
                nuint cmap = mi_atomic_load_relaxed(ref bitmap->chunkmap.bfields[(int)i]);
                if (mi_bsr(cmap, out nuint cmap_idx))
                {
                    // from highest chunk to lowest (scan all in case the cmap entry was stale)
                    for (nuint j = cmap_idx + 1; j > 0;)
                    {
                        j--;
                        nuint chunk_idx = (i * MI_BFIELD_BITS) + j;
                        nuint cidx;
                        if (mi_bchunk_bsr(mi_bitmap_chunk(bitmap, chunk_idx), &cidx))
                        {
                            *idx = (chunk_idx * MI_BCHUNK_BITS) + cidx;
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        // Return count of all set bits in a bitmap.
        public static nuint mi_bitmap_popcount(mi_bitmap_t* bitmap)
        {
            // for all chunkmap entries
            nuint popcount = 0;
            nuint chunkmap_max = _mi_divide_up(mi_bitmap_chunk_count(bitmap), MI_BFIELD_BITS);
            for (nuint i = 0; i < chunkmap_max; i++)
            {
                nuint cmap_entry = mi_atomic_load_relaxed(ref bitmap->chunkmap.bfields[(int)i]);
                // for each chunk (corresponding to a set bit in a chunkmap entry)
                while (mi_bfield_foreach_bit(ref cmap_entry, out nuint cmap_idx))
                {
                    nuint chunk_idx = i * MI_BFIELD_BITS + cmap_idx;
                    // count bits in a chunk
                    popcount += mi_bchunk_popcount(mi_bitmap_chunk(bitmap, chunk_idx));
                }
            }
            return popcount;
        }

        // Clear a bit once it is set.
        public static void mi_bitmap_clear_once_set(mi_subproc_t* subproc, mi_bitmap_t* bitmap, nuint idx)
        {
            mi_assert_internal(idx < mi_bitmap_max_bits(bitmap));
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            mi_assert_internal(chunk_idx < mi_bitmap_chunk_count(bitmap));
            mi_bchunk_clear_once_set(subproc, mi_bitmap_chunk(bitmap, chunk_idx), cidx);
        }

        // Visit all set bits in a bitmap (`slice_count == 1`)
        public static bool _mi_bitmap_forall_set(mi_bitmap_t* bitmap, delegate*<nuint, nuint, mi_arena_t*, void*, bool> visit, mi_arena_t* arena, void* arg)
        {
            // for all chunkmap entries
            nuint chunkmap_max = _mi_divide_up(mi_bitmap_chunk_count(bitmap), MI_BFIELD_BITS);
            for (nuint i = 0; i < chunkmap_max; i++)
            {
                nuint cmap_entry = mi_atomic_load_relaxed(ref bitmap->chunkmap.bfields[(int)i]);
                // for each chunk (corresponding to a set bit in a chunkmap entry)
                while (mi_bfield_foreach_bit(ref cmap_entry, out nuint cmap_idx))
                {
                    nuint chunk_idx = i * MI_BFIELD_BITS + cmap_idx;
                    // for each chunk field
                    mi_bchunk_t* chunk = mi_bitmap_chunk(bitmap, chunk_idx);
                    for (int j = 0; j < MI_BCHUNK_FIELDS; j++)
                    {
                        nuint base_idx = (chunk_idx * MI_BCHUNK_BITS) + ((nuint)j * MI_BFIELD_BITS);
                        nuint b = mi_atomic_load_relaxed(ref chunk->bfields[j]);
                        while (mi_bfield_foreach_bit(ref b, out nuint bidx))
                        {
                            nuint idx = base_idx + bidx;
                            if (!visit(idx, 1, arena, arg)) return false;
                        }
                    }
                }
            }
            return true;
        }

        // Visit all set bits in a bitmap but try to return ranges (within bfields) if possible.
        // Also clear those ranges atomically. Used by purging to purge larger ranges when possible.
        public static bool _mi_bitmap_forall_setc_ranges(mi_bitmap_t* bitmap, delegate*<nuint, nuint, mi_arena_t*, void*, bool> visit, mi_arena_t* arena, void* arg)
        {
            // for all chunkmap entries
            nuint chunkmap_max = _mi_divide_up(mi_bitmap_chunk_count(bitmap), MI_BFIELD_BITS);
            for (nuint i = 0; i < chunkmap_max; i++)
            {
                nuint cmap_entry = mi_atomic_load_relaxed(ref bitmap->chunkmap.bfields[(int)i]);
                // for each chunk (corresponding to a set bit in a chunkmap entry)
                while (mi_bfield_foreach_bit(ref cmap_entry, out nuint cmap_idx))
                {
                    nuint chunk_idx = i * MI_BFIELD_BITS + cmap_idx;
                    // for each chunk field
                    mi_bchunk_t* chunk = mi_bitmap_chunk(bitmap, chunk_idx);
                    for (int j = 0; j < MI_BCHUNK_FIELDS; j++)
                    {
                        nuint base_idx = (chunk_idx * MI_BCHUNK_BITS) + ((nuint)j * MI_BFIELD_BITS);
                        nuint b = mi_atomic_exchange_relaxed(ref chunk->bfields[j], 0);
                        while (mi_bfield_find_least_bit(b, out nuint bidx))
                        {
                            nuint rng = mi_ctz(~(b >> (int)bidx));   // all the set bits from bidx
                            nuint idx = base_idx + bidx;
                            mi_assert_internal(rng >= 1 && rng <= MI_BFIELD_BITS);
                            mi_assert_internal((idx % MI_BFIELD_BITS) + rng <= MI_BFIELD_BITS);
                            mi_assert_internal((idx / MI_BCHUNK_BITS) < mi_bitmap_chunk_count(bitmap));
                            // clear rng bits in b
                            b = b & ~mi_bfield_mask(rng, bidx);
                            if (!visit(idx, rng, arena, arg))
                            {
                                // break early: reset the non-visited bits
                                if (b != 0)
                                {
                                    mi_atomic_or_relaxed(ref chunk->bfields[j], b);
                                }
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }

        // Visit all set bits in a bitmap in ranges of `rngslices` aligned slices and clear them atomically.
        // Used by purging with transparent huge pages to only purge whole huge pages at a time.
        public static bool _mi_bitmap_forall_setc_rangesn(mi_bitmap_t* bitmap, nuint rngslices, delegate*<nuint, nuint, mi_arena_t*, void*, bool> visit, mi_arena_t* arena, void* arg)
        {
            // use the generic routine for `rngslices<=1` (as that one finds longest ranges at a time)
            if (rngslices <= 1)
            {
                return _mi_bitmap_forall_setc_ranges(bitmap, visit, arena, arg);
            }
            if (rngslices > MI_BFIELD_BITS) { rngslices = MI_BFIELD_BITS; }   // cap at MI_BFIELD_BITS at most

            // for all chunkmap entries
            nuint chunkmap_max = _mi_divide_up(mi_bitmap_chunk_count(bitmap), MI_BFIELD_BITS);
            for (nuint i = 0; i < chunkmap_max; i++)
            {
                nuint cmap_entry = mi_atomic_load_relaxed(ref bitmap->chunkmap.bfields[(int)i]);
                // for each chunk (corresponding to a set bit in a chunkmap entry)
                while (mi_bfield_foreach_bit(ref cmap_entry, out nuint cmap_idx))
                {
                    nuint chunk_idx = i * MI_BFIELD_BITS + cmap_idx;
                    // for each chunk field
                    mi_bchunk_t* chunk = mi_bitmap_chunk(bitmap, chunk_idx);
                    for (int j = 0; j < MI_BCHUNK_FIELDS; j++)
                    {
                        nuint base_idx = (chunk_idx * MI_BCHUNK_BITS) + ((nuint)j * MI_BFIELD_BITS);
                        nuint b = mi_atomic_exchange_relaxed(ref chunk->bfields[j], 0);   // atomic clear
                        nuint skipped = 0;                                                // but track which bits we skip so we can restore them
                        nuint shift;
                        for (shift = 0; rngslices + shift <= MI_BFIELD_BITS; shift += rngslices)   // per `rngslices` to keep alignment
                        {
                            nuint rngmask = mi_bfield_mask(rngslices, shift);
                            if ((b & rngmask) == rngmask)
                            {
                                nuint idx = base_idx + shift;
                                if (!visit(idx, rngslices, arena, arg))
                                {
                                    // break early: restore non-visited entries
                                    nuint notyet_visited = 0;
                                    if (rngslices + shift < MI_BFIELD_BITS)
                                    {
                                        notyet_visited = b & (~(nuint)0 << (int)(shift + rngslices));
                                    }
                                    mi_assert_internal((notyet_visited & skipped) == 0);
                                    if ((notyet_visited | skipped) != 0)
                                    {
                                        mi_atomic_or_relaxed(ref chunk->bfields[j], notyet_visited | skipped);
                                    }
                                    return false;
                                }
                            }
                            else
                            {
                                skipped = skipped | (b & rngmask);
                            }
                        }
                        if (shift < MI_BFIELD_BITS)
                        {
                            // there are some non-visited top bits when `MI_BFIELD_BITS % rngslices != 0`.
                            mi_assert_internal(MI_BFIELD_BITS % rngslices != 0);
                            skipped = skipped | (b & (~(nuint)0 << (int)shift));
                        }
                        if (skipped != 0)
                        {
                            // restore non-visited entries
                            mi_atomic_or_relaxed(ref chunk->bfields[j], skipped);
                        }
                    }
                }
            }
            return true;
        }

        /* --------------------------------------------------------------------------------
          binned bitmap's
        -------------------------------------------------------------------------------- */

        public static nuint mi_bbitmap_size(nuint bit_count, nuint* pchunk_count)
        {
            bit_count = _mi_align_up(bit_count, MI_BCHUNK_BITS);
            mi_assert_internal(bit_count <= MI_BITMAP_MAX_BIT_COUNT);
            mi_assert_internal(bit_count > 0);
            nuint chunk_count = bit_count / MI_BCHUNK_BITS;
            mi_assert_internal(chunk_count >= 1);
            nuint size = MI_BBITMAP_CHUNKS_OFFSET + (chunk_count * MI_BCHUNK_SIZE);
            mi_assert_internal((size % MI_BCHUNK_SIZE) == 0);
            if (pchunk_count != null) { *pchunk_count = chunk_count; }
            return size;
        }

        // initialize a bitmap to all unset; avoid a mem_zero if `already_zero` is true
        // returns the size of the bitmap
        public static nuint mi_bbitmap_init(mi_subproc_t* subproc, mi_bbitmap_t* bbitmap, nuint bit_count, bool already_zero)
        {
            nuint chunk_count;
            nuint size = mi_bbitmap_size(bit_count, &chunk_count);
            if (!already_zero)
            {
                _mi_memzero_aligned(bbitmap, size);
            }
            mi_atomic_store_release(ref bbitmap->chunk_count, chunk_count);
            mi_assert_internal(mi_atomic_load_relaxed(ref bbitmap->chunk_count) <= MI_BITMAP_MAX_CHUNK_COUNT);
            bbitmap->subproc = subproc;
            return size;
        }

        public static void mi_bbitmap_unsafe_setN(mi_bbitmap_t* bbitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            mi_assert_internal(idx + n <= mi_bbitmap_max_bits(bbitmap));
            mi_bchunks_unsafe_setN(mi_bbitmap_chunk(bbitmap, 0), &bbitmap->chunkmap, idx, n);
        }

        public static bool mi_bbitmap_bsr_inv(mi_bbitmap_t* bbitmap, nuint* idx)
        {
            // scan for highest zero bit in the bitmap
            // note: we cannot use the chunkmap since that only conservatively denotes if there might be a set bit in a chunk
            nuint chunk_count = mi_bbitmap_chunk_count(bbitmap);
            for (nuint i = chunk_count; i > 0;)
            {
                i--;
                nuint cidx;
                if (mi_bchunk_bsr_inv(mi_bbitmap_chunk(bbitmap, i), &cidx))
                {
                    *idx = (i * MI_BCHUNK_BITS) + cidx;
                    return true;
                }
            }
            return false;
        }

        /* --------------------------------------------------------------------------------
         binned bitmap used to track free slices
        -------------------------------------------------------------------------------- */

        // Assign a specific size bin to a chunk
        private static void mi_bbitmap_set_chunk_bin(mi_bbitmap_t* bbitmap, nuint chunk_idx, mi_chunkbin_t bin)
        {
            mi_assert_internal(chunk_idx < mi_bbitmap_chunk_count(bbitmap));
            for (mi_chunkbin_t ibin = mi_chunkbin_t.MI_CBIN_SMALL; ibin < mi_chunkbin_t.MI_CBIN_NONE; ibin = mi_chunkbin_inc(ibin))
            {
                if (ibin == bin)
                {
                    bool was_clear = mi_bchunk_set((mi_bchunk_t*)Unsafe.AsPointer(ref bbitmap->chunkmap_bins[(int)ibin]), chunk_idx, null);
                    if (was_clear) { __mi_stat_increase_mt((mi_stat_count_t*)Unsafe.AsPointer(ref bbitmap->subproc->stats.chunk_bins[(int)ibin]), 1); }
                }
                else
                {
                    bool was_set = mi_bchunk_clear((mi_bchunk_t*)Unsafe.AsPointer(ref bbitmap->chunkmap_bins[(int)ibin]), chunk_idx, null);
                    if (was_set) { __mi_stat_decrease_mt((mi_stat_count_t*)Unsafe.AsPointer(ref bbitmap->subproc->stats.chunk_bins[(int)ibin]), 1); }
                }
            }
        }

        public static mi_chunkbin_t mi_bbitmap_debug_get_bin(mi_bchunk_t* chunkmap_bins, nuint chunk_idx)
        {
            for (mi_chunkbin_t ibin = mi_chunkbin_t.MI_CBIN_SMALL; ibin < mi_chunkbin_t.MI_CBIN_NONE; ibin = mi_chunkbin_inc(ibin))
            {
                if (mi_bchunk_is_xsetN(MI_BIT_SET, &chunkmap_bins[(int)ibin], chunk_idx, 1))
                {
                    return ibin;
                }
            }
            return mi_chunkbin_t.MI_CBIN_NONE;
        }

        // Track the index of the highest chunk that is accessed.
        private static void mi_bbitmap_chunkmap_set_max(mi_bbitmap_t* bbitmap, nuint chunk_idx)
        {
            nuint oldmax = mi_atomic_load_relaxed(ref bbitmap->chunk_max_accessed);
            if (chunk_idx > oldmax)
            {
                mi_atomic_cas_strong_relaxed(ref bbitmap->chunk_max_accessed, ref oldmax, chunk_idx);
            }
        }

        // Set a bit in the chunkmap
        private static void mi_bbitmap_chunkmap_set(mi_bbitmap_t* bbitmap, nuint chunk_idx, bool check_all_set)
        {
            mi_assert(chunk_idx < mi_bbitmap_chunk_count(bbitmap));
            if (check_all_set)
            {
                if (mi_bchunk_all_are_set_relaxed(mi_bbitmap_chunk(bbitmap, chunk_idx)))
                {
                    // all slices are free in this chunk: return back to the NONE bin
                    mi_bbitmap_set_chunk_bin(bbitmap, chunk_idx, mi_chunkbin_t.MI_CBIN_NONE);
                }
            }
            mi_bchunk_set(&bbitmap->chunkmap, chunk_idx, null);
            mi_bbitmap_chunkmap_set_max(bbitmap, chunk_idx);
        }

        private static bool mi_bbitmap_chunkmap_try_clear(mi_bbitmap_t* bbitmap, nuint chunk_idx)
        {
            mi_assert(chunk_idx < mi_bbitmap_chunk_count(bbitmap));
            // check if the corresponding chunk is all clear
            if (!mi_bchunk_all_are_clear_relaxed(mi_bbitmap_chunk(bbitmap, chunk_idx))) return false;
            // clear the chunkmap bit
            mi_bchunk_clear(&bbitmap->chunkmap, chunk_idx, null);
            // .. but a concurrent set may have happened in between our all-clear test and the clearing of the
            // bit in the mask. We check again to catch this situation.
            if (!mi_bchunk_all_are_clear_relaxed(mi_bbitmap_chunk(bbitmap, chunk_idx)))
            {
                mi_bchunk_set(&bbitmap->chunkmap, chunk_idx, null);
                return false;
            }
            mi_bbitmap_chunkmap_set_max(bbitmap, chunk_idx);
            return true;
        }

        /* --------------------------------------------------------------------------------
          mi_bbitmap_setN, try_clearN, and is_xsetN  (used to find free pages)
        -------------------------------------------------------------------------------- */

        // Set a sequence of `n` bits in the bitmap; returns `true` if atomically transitioned from 0's to 1's
        public static bool mi_bbitmap_setN(mi_bbitmap_t* bbitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            nuint maxbits = mi_bbitmap_max_bits(bbitmap);
            mi_assert_internal(idx + n <= maxbits);
            if (idx + n > maxbits)   // paranoia
            {
                if (idx >= maxbits) return false;
                n = maxbits - idx;
            }

            // iterate through the chunks
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            bool were_allclear = true;
            while (n > 0)
            {
                nuint m = (cidx + n > MI_BCHUNK_BITS ? MI_BCHUNK_BITS - cidx : n);
                were_allclear = mi_bchunk_setN(mi_bbitmap_chunk(bbitmap, chunk_idx), cidx, m, null) && were_allclear;
                mi_bbitmap_chunkmap_set(bbitmap, chunk_idx, true); // set afterwards
                mi_assert_internal(m <= n);
                n -= m;
                cidx = 0;
                chunk_idx++;
            }
            return were_allclear;
        }

        // ------- mi_bbitmap_try_clearNC ---------------------------------------

        // Try to clear `n` bits at `idx` where `n <= MI_BCHUNK_BITS` (must not cross chunks).
        public static bool mi_bbitmap_try_clearNC(mi_bbitmap_t* bbitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            mi_assert_internal(n <= MI_BCHUNK_BITS);
            mi_assert_internal(idx + n <= mi_bbitmap_max_bits(bbitmap));

            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            mi_assert_internal(cidx + n <= MI_BCHUNK_BITS);  // don't cross chunks (for now)
            mi_assert_internal(chunk_idx < mi_bbitmap_chunk_count(bbitmap));
            if (cidx + n > MI_BCHUNK_BITS) return false;
            bool maybe_all_clear = false;
            bool did_temp_clear_bits = false;
            bool cleared = mi_bchunk_try_clearN(mi_bbitmap_chunk(bbitmap, chunk_idx), cidx, n, &maybe_all_clear, &did_temp_clear_bits);
            if (cleared && maybe_all_clear)
            {
                mi_assert_internal(!did_temp_clear_bits);
                mi_bbitmap_chunkmap_try_clear(bbitmap, chunk_idx);
            }
            else if (did_temp_clear_bits)
            {
                // may have raced with a clearer (in on_find) so set the chunkmap bit conservatively
                mi_bbitmap_chunkmap_set(bbitmap, chunk_idx, false);
            }
            // note: we don't set the size class for an explicit try_clearN (only used by purging)
            return cleared;
        }

        // ------- mi_bbitmap_is_xset ---------------------------------------

        // Is a sequence of n bits already all set/cleared?
        public static bool mi_bbitmap_is_xsetN(bool set, mi_bbitmap_t* bbitmap, nuint idx, nuint n)
        {
            mi_assert_internal(n > 0);
            nuint maxbits = mi_bbitmap_max_bits(bbitmap);
            mi_assert_internal(idx + n <= maxbits);
            if (idx + n > maxbits)   // paranoia
            {
                if (idx >= maxbits) return false;
                n = maxbits - idx;
            }

            // iterate through the chunks
            nuint chunk_idx = idx / MI_BCHUNK_BITS;
            nuint cidx = idx % MI_BCHUNK_BITS;
            bool xset = true;
            while (n > 0 && xset)
            {
                nuint m = (cidx + n > MI_BCHUNK_BITS ? MI_BCHUNK_BITS - cidx : n);
                xset = mi_bchunk_is_xsetN(set, mi_bbitmap_chunk(bbitmap, chunk_idx), cidx, m) && xset;
                mi_assert_internal(m <= n);
                n -= m;
                cidx = 0;
                chunk_idx++;
            }
            return xset;
        }

        public static bool mi_bbitmap_is_setN(mi_bbitmap_t* bbitmap, nuint idx, nuint n) => mi_bbitmap_is_xsetN(MI_BIT_SET, bbitmap, idx, n);

        public static bool mi_bbitmap_is_clearN(mi_bbitmap_t* bbitmap, nuint idx, nuint n) => mi_bbitmap_is_xsetN(MI_BIT_CLEAR, bbitmap, idx, n);

        /* --------------------------------------------------------------------------------
          mi_bbitmap_find  (used to find free pages)
        -------------------------------------------------------------------------------- */

        // Go through the bbitmap and for every sequence of `n` set bits, call the visitor function.
        // If it returns `true` stop the search.
        //
        // This is used for finding free blocks and it is important to be efficient (with 2-level bitscan)
        // but also reduce fragmentation (through size bins).
        private static bool mi_bbitmap_try_find_and_clear_generic(mi_bbitmap_t* bbitmap, nuint tseq, nuint n, nuint* pidx,
            delegate*<mi_bchunk_t*, nuint, nuint*, bool*, bool> on_find)
        {
            // we space out threads to reduce contention
            nuint cmap_max_count = _mi_divide_up(mi_bbitmap_chunk_count(bbitmap), MI_BFIELD_BITS);
            nuint chunk_acc = mi_atomic_load_relaxed(ref bbitmap->chunk_max_accessed);
            nuint cmap_acc = chunk_acc / MI_BFIELD_BITS;
            nuint cmap_acc_bits = 1 + (chunk_acc % MI_BFIELD_BITS);

            // create a mask over the chunkmap entries to iterate over them efficiently
            mi_assert_internal(MI_BFIELD_BITS >= MI_BCHUNK_FIELDS);
            nuint cmap_mask = mi_bfield_mask(cmap_max_count, 0);
            nuint cmap_cycle = cmap_acc + 1;
            mi_chunkbin_t bbin = mi_chunkbin_of(n);
            // size bin masks scratch space (C declares this inside the loop; hoisted here as
            // C# stackalloc inside a loop keeps growing the stack)
            nuint* cmap_bins = stackalloc nuint[(int)mi_chunkbin_t.MI_CBIN_COUNT];
            // visit each cmap entry
            var iterX = new mi_bfield_cycle_iter(cmap_mask, tseq, cmap_cycle);
            while (iterX.TryNext(out nuint cmap_idx))
            {
                // and for each chunkmap entry we iterate over its bits to find the chunks
                nuint cmap_entry = mi_atomic_load_relaxed(ref bbitmap->chunkmap.bfields[(int)cmap_idx]);
                nuint cmap_entry_cycle = (cmap_idx != cmap_acc ? MI_BFIELD_BITS : cmap_acc_bits);
                if (cmap_entry == 0)
                {
                    continue;
                }

                // get size bin masks
                for (int k = 0; k < (int)mi_chunkbin_t.MI_CBIN_COUNT; k++) { cmap_bins[k] = 0; }
                cmap_bins[(int)mi_chunkbin_t.MI_CBIN_NONE] = cmap_entry;
                for (mi_chunkbin_t ibin0 = mi_chunkbin_t.MI_CBIN_SMALL; ibin0 < mi_chunkbin_t.MI_CBIN_NONE; ibin0 = mi_chunkbin_inc(ibin0))
                {
                    ref mi_bchunk_t bin_chunk = ref bbitmap->chunkmap_bins[(int)ibin0];
                    nuint cmap_bin = mi_atomic_load_relaxed(ref bin_chunk.bfields[(int)cmap_idx]);
                    cmap_bins[(int)ibin0] = cmap_bin & cmap_entry;
                    cmap_bins[(int)mi_chunkbin_t.MI_CBIN_NONE] &= ~cmap_bin;   // clear bits that are in an assigned size bin
                }

                // consider only chunks for a particular size bin at a time
                mi_assert_internal(bbin < mi_chunkbin_t.MI_CBIN_NONE);
                for (mi_chunkbin_t ibin = mi_chunkbin_t.MI_CBIN_SMALL; ibin <= mi_chunkbin_t.MI_CBIN_NONE;
                     // skip from bbin to NONE (so, say, a SMALL will never be placed in a OTHER, MEDIUM, or LARGE chunk to reduce fragmentation)
                     ibin = (ibin == bbin ? mi_chunkbin_t.MI_CBIN_NONE : mi_chunkbin_inc(ibin)))
                {
                    mi_assert_internal(ibin < mi_chunkbin_t.MI_CBIN_COUNT);
                    nuint cmap_bin = cmap_bins[(int)ibin];
                    var iterY = new mi_bfield_cycle_iter(cmap_bin, tseq, cmap_entry_cycle);
                    while (iterY.TryNext(out nuint eidx))
                    {
                        // get the chunk
                        nuint chunk_idx = cmap_idx * MI_BFIELD_BITS + eidx;
                        mi_bchunk_t* chunk = mi_bbitmap_chunk(bbitmap, chunk_idx);

                        nuint cidx;
                        bool did_temp_clear_bits = false;
                        if (on_find(chunk, n, &cidx, &did_temp_clear_bits))
                        {
                            if (cidx == 0 && ibin == mi_chunkbin_t.MI_CBIN_NONE)   // only the first block determines the size bin
                            {
                                // this chunk is now reserved for the `bbin` size class
                                mi_bbitmap_set_chunk_bin(bbitmap, chunk_idx, bbin);
                            }
                            *pidx = (chunk_idx * MI_BCHUNK_BITS) + cidx;
                            mi_assert_internal(*pidx + n <= mi_bbitmap_max_bits(bbitmap));
                            return true;
                        }
                        else
                        {
                            // we may find that all are cleared only on a second iteration but that is ok as the chunkmap is a conservative approximation.
                            if (did_temp_clear_bits)
                            {
                                // a concurrent find_and_claim may have cleared the chunkmap bit, restore it now
                                mi_bbitmap_chunkmap_set(bbitmap, chunk_idx, false);
                            }
                            else
                            {
                                mi_bbitmap_chunkmap_try_clear(bbitmap, chunk_idx);
                            }
                        }
                    }
                }
            }
            return false;
        }

        /* --------------------------------------------------------------------------------
          mi_bbitmap_try_find_and_clear -- used to find free pages
        -------------------------------------------------------------------------------- */

        public static bool mi_bbitmap_try_find_and_clear(mi_bbitmap_t* bbitmap, nuint tseq, nuint* pidx)
            => mi_bbitmap_try_find_and_clear_generic(bbitmap, tseq, 1, pidx, &mi_bchunk_try_find_and_clear_1);

        public static bool mi_bbitmap_try_find_and_clear8(mi_bbitmap_t* bbitmap, nuint tseq, nuint* pidx)
            => mi_bbitmap_try_find_and_clear_generic(bbitmap, tseq, 8, pidx, &mi_bchunk_try_find_and_clear_8);

        public static bool mi_bbitmap_try_find_and_clearNX(mi_bbitmap_t* bbitmap, nuint tseq, nuint n, nuint* pidx)
        {
            mi_assert_internal(n <= MI_BFIELD_BITS);
            return mi_bbitmap_try_find_and_clear_generic(bbitmap, tseq, n, pidx, &mi_bchunk_try_find_and_clearNX);
        }

        public static bool mi_bbitmap_try_find_and_clearNC(mi_bbitmap_t* bbitmap, nuint tseq, nuint n, nuint* pidx)
        {
            mi_assert_internal(n <= MI_BCHUNK_BITS);
            return mi_bbitmap_try_find_and_clear_generic(bbitmap, tseq, n, pidx, &mi_bchunk_try_find_and_clearNC);
        }

        // Find a sequence of `n` bits in the bbitmap with all bits set, and try to atomically clear all.
        public static bool mi_bbitmap_try_find_and_clearN(mi_bbitmap_t* bbitmap, nuint tseq, nuint n, nuint* pidx)
        {
            if (n == 1) return mi_bbitmap_try_find_and_clear(bbitmap, tseq, pidx);    // small pages
            if (n == 8) return mi_bbitmap_try_find_and_clear8(bbitmap, tseq, pidx);   // medium pages
            if (n == 0) return false;
            if (n <= MI_BFIELD_BITS) return mi_bbitmap_try_find_and_clearNX(bbitmap, tseq, n, pidx);
            if (n <= MI_BCHUNK_BITS) return mi_bbitmap_try_find_and_clearNC(bbitmap, tseq, n, pidx);
            return mi_bbitmap_try_find_and_clearN_(bbitmap, tseq, n, pidx);
        }

        /* --------------------------------------------------------------------------------
          mi_bbitmap_try_find_and_clear for huge objects spanning multiple chunks
        -------------------------------------------------------------------------------- */

        // Try to atomically clear `n` bits starting at `chunk_idx` where `n` can span over multiple chunks
        private static bool mi_bchunk_try_clearN_(mi_bbitmap_t* bbitmap, nuint chunk_idx, nuint n)
        {
            mi_assert_internal((chunk_idx * MI_BCHUNK_BITS) + n <= mi_bbitmap_max_bits(bbitmap));
            nuint m = n;       // bits to go
            nuint count = 0;   // chunk count
            while (m > 0)
            {
                mi_bchunk_t* chunk = mi_bbitmap_chunk(bbitmap, chunk_idx + count);
                if (!mi_bchunk_try_clearN(chunk, 0, (m > MI_BCHUNK_BITS ? MI_BCHUNK_BITS : m), null, null))
                {
                    goto rollback;
                }
                m = (m <= MI_BCHUNK_BITS ? 0 : m - MI_BCHUNK_BITS);
                count++;
            }
            return true;

        rollback:
            // we only need to reset chunks the we just fully cleared
            while (count > 0)
            {
                count--;
                mi_bchunk_t* chunk = mi_bbitmap_chunk(bbitmap, chunk_idx + count);
                mi_bchunk_setN(chunk, 0, MI_BCHUNK_BITS, null);
                // since we may race with clearing, we need to set the chunkmap conservatively
                mi_bbitmap_chunkmap_set(bbitmap, chunk_idx + count, false);
            }
            return false;
        }

        // Go through the bbitmap to find a sequence of `n` bits and clear them atomically where `n > MI_BCHUNK_BITS`.
        // Since these are very large object allocations we always search from the start and only consider starting at
        // the start of a chunk (for fragmentation and efficiency).
        public static bool mi_bbitmap_try_find_and_clearN_(mi_bbitmap_t* bbitmap, nuint tseq, nuint n, nuint* pidx)
        {
            mi_assert(n > 0); if (n == 0) { return false; }

            nuint chunk_max = mi_bbitmap_chunk_count(bbitmap);
            nuint chunk_req = _mi_divide_up(n, MI_BCHUNK_BITS);   // minimal number of chunks needed
            if (chunk_max < chunk_req) { return false; }

            // iterate through the chunks
            nuint chunk_idx = 0;
            while (chunk_idx <= chunk_max - chunk_req)
            {
                nuint count = 0;   // chunk count
                do
                {
                    mi_assert_internal(chunk_idx + count < chunk_max);
                    mi_bchunk_t* chunk = mi_bbitmap_chunk(bbitmap, chunk_idx + count);
                    if (!mi_bchunk_all_are_set_relaxed(chunk))
                    {
                        break;
                    }
                    else
                    {
                        count++;
                    }
                }
                while (count < chunk_req);

                // did we find a suitable range?
                if (count == chunk_req)
                {
                    // now try to claim it!
                    if (mi_bchunk_try_clearN_(bbitmap, chunk_idx, n))
                    {
                        *pidx = chunk_idx * MI_BCHUNK_BITS;
                        for (nuint i = 0; i < count; i++)
                        {
                            mi_bbitmap_set_chunk_bin(bbitmap, chunk_idx + i, mi_chunkbin_t.MI_CBIN_HUGE);
                        }
                        mi_assert_internal(*pidx + n <= mi_bbitmap_max_bits(bbitmap));
                        return true;
                    }
                    else
                    {
                        // contended: we reset count to retry from the first
                        // (we still skip the first chunk to guarantee progress)
                        count = 0;
                    }
                }

                // keep searching but skip the scanned range
                chunk_idx += count + 1;
            }
            return false;
        }
    }
}
