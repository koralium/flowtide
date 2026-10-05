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

// Sanity checks for Types.cs: the derived constants must match the values the C
// code computes for the pinned 64-bit configuration, and the structs must be
// unmanaged with the expected union/layout behavior.

using System.Runtime.CompilerServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class TypesTests
    {
        [Fact]
        public void SizeConstants_MatchCConfiguration()
        {
            Assert.Equal(16, MI_ARENA_SLICE_SHIFT);
            Assert.Equal((nuint)(64 * 1024), MI_ARENA_SLICE_SIZE);
            Assert.Equal(512, MI_BCHUNK_BITS);
            Assert.Equal((nuint)(64 * 1024), MI_SMALL_PAGE_SIZE);
            Assert.Equal((nuint)(512 * 1024), MI_MEDIUM_PAGE_SIZE);
            Assert.Equal((nuint)(4 * 1024 * 1024), MI_LARGE_PAGE_SIZE);
            Assert.Equal((nuint)(10 * 1024), MI_SMALL_MAX_OBJ_SIZE);
            Assert.Equal((nuint)((512 * 1024 - 4096) / 6), MI_MEDIUM_MAX_OBJ_SIZE);   // ~84 KiB
            Assert.Equal((nuint)(512 * 1024), MI_LARGE_MAX_OBJ_SIZE);
            Assert.Equal((nuint)65536, MI_LARGE_MAX_OBJ_WSIZE);
            Assert.Equal(75, MI_BIN_COUNT);
            Assert.Equal(129, MI_PAGES_DIRECT);
            Assert.Equal(61, MI_ARENA_BIN_COUNT);
            Assert.Equal(32UL * 1024 * 1024, MI_ARENA_MIN_SIZE);
            Assert.Equal(16UL * 1024 * 1024 * 1024, MI_ARENA_MAX_SIZE);
            Assert.Equal((nuint)1024, MI_SMALL_SIZE_MAX);
            // types.h invariant: max object size must fit the singleton bin check
            Assert.True(MI_LARGE_MAX_OBJ_WSIZE <= 65536);
            // types.h: static check that the max chunk object size can hold thread data
            Assert.True(MI_ARENA_MAX_CHUNK_OBJ_SIZE >= (ulong)MI_SIZE_SIZE * 1024);
        }

        [Fact]
        public void ThreadIdFlags_Values()
        {
            Assert.Equal((nuint)0, MI_THREADID_ABANDONED);
            Assert.Equal((nuint)4, MI_THREADID_ABANDONED_MAPPED);
            Assert.Equal((nuint)3, MI_PAGE_FLAG_MASK);
        }

        [Fact]
        public void Memid_UnionOverlaps()
        {
            mi_memid_t memid = default;
            memid.mem.os.@base = (void*)0x1234;
            memid.mem.os.size = 42;
            // arena view aliases the same memory
            Assert.True((void*)memid.mem.arena.arena == (void*)0x1234);
            Assert.Equal(42u, memid.mem.arena.slice_index);
            Assert.Equal(0u, memid.mem.arena.slice_count);
        }

        [Fact]
        public void MemkindHelpers_MatchC()
        {
            Assert.True(mi_memkind_is_os(mi_memkind_t.MI_MEM_OS));
            Assert.True(mi_memkind_is_os(mi_memkind_t.MI_MEM_OS_HUGE));
            Assert.True(mi_memkind_is_os(mi_memkind_t.MI_MEM_OS_REMAP));
            Assert.False(mi_memkind_is_os(mi_memkind_t.MI_MEM_ARENA));
            Assert.False(mi_memkind_is_os(mi_memkind_t.MI_MEM_META));

            Assert.True(mi_memkind_needs_no_free(mi_memkind_t.MI_MEM_NONE));
            Assert.True(mi_memkind_needs_no_free(mi_memkind_t.MI_MEM_EXTERNAL));
            Assert.True(mi_memkind_needs_no_free(mi_memkind_t.MI_MEM_STATIC));
            Assert.False(mi_memkind_needs_no_free(mi_memkind_t.MI_MEM_META));
        }

        [Fact]
        public void StructSizes_AreReasonable()
        {
            // no binary compat requirement with C, but catch accidental layout explosions
            // (C mi_page_t is ~112 bytes in this configuration)
            Assert.True(sizeof(mi_page_t) <= 128, $"mi_page_t is {sizeof(mi_page_t)} bytes");
            Assert.Equal(16, sizeof(mi_memid_mem_t));   // union of three 16-byte structs
            // theap embeds pages_free_direct[129] + 75 page queues + stats
            Assert.True(sizeof(mi_theap_t) > 129 * 8 + 75 * 32);
            // page queue: two pointers + two size_t
            Assert.Equal(32, sizeof(mi_page_queue_t));
        }

        [Fact]
        public void Stats_InitSetsHeader()
        {
            mi_stats_t stats;
            mi_stats_init(&stats);
            Assert.Equal((nuint)sizeof(mi_stats_t), stats.size);
            Assert.Equal((nuint)MI_STAT_VERSION, stats.version);
            Assert.Equal(0, stats.pages.total);
        }

        [Fact]
        public void InlineArrays_IndexIndependently()
        {
            mi_theap_t theap = default;
            theap.pages[0].block_size = 8;
            theap.pages[MI_BIN_COUNT - 1].block_size = 999;
            Assert.Equal((nuint)8, theap.pages[0].block_size);
            Assert.Equal((nuint)999, theap.pages[MI_BIN_COUNT - 1].block_size);
            Assert.Equal((nuint)0, theap.pages[1].block_size);
        }

        [Fact]
        public void AlignHelpers_MatchC()
        {
            Assert.True(_mi_is_power_of_two(0));   // C: 0 is considered a power of two
            Assert.True(_mi_is_power_of_two(1));
            Assert.True(_mi_is_power_of_two(4096));
            Assert.False(_mi_is_power_of_two(48));

            Assert.Equal((nuint)0, _mi_align_up(0, 16));
            Assert.Equal((nuint)16, _mi_align_up(1, 16));
            Assert.Equal((nuint)16, _mi_align_up(16, 16));
            Assert.Equal((nuint)32, _mi_align_up(17, 16));
            Assert.Equal((nuint)24, _mi_align_up(17, 12));   // non power-of-two divider

            Assert.Equal((nuint)16, _mi_align_down(17, 16));
            Assert.Equal((nuint)16, _mi_align_down(31, 16));
            Assert.Equal((nuint)12, _mi_align_down(17, 12));

            Assert.Equal((nuint)3, _mi_divide_up(17, 8));
            Assert.Equal((nuint)2, _mi_divide_up(16, 8));

            Assert.Equal((nuint)5, _mi_clamp(1, 5, 10));
            Assert.Equal((nuint)10, _mi_clamp(100, 5, 10));
            Assert.Equal((nuint)7, _mi_clamp(7, 5, 10));

            Assert.Equal((nuint)0, _mi_wsize_from_size(0));
            Assert.Equal((nuint)1, _mi_wsize_from_size(1));
            Assert.Equal((nuint)1, _mi_wsize_from_size(8));
            Assert.Equal((nuint)2, _mi_wsize_from_size(9));
        }

        [Fact]
        public void MulOverflow_Detects()
        {
            Assert.False(mi_mul_overflow(1000, 1000, out nuint total));
            Assert.Equal((nuint)1_000_000, total);
            Assert.True(mi_mul_overflow(nuint.MaxValue / 2, 3, out _));
            Assert.False(mi_count_size_overflow(1, nuint.MaxValue, out total));   // count==1 shortcut
            Assert.Equal(nuint.MaxValue, total);
        }

        [Fact]
        public void SliceHelpers_MatchC()
        {
            Assert.Equal((nuint)0, mi_slice_count_of_size(0));
            Assert.Equal((nuint)1, mi_slice_count_of_size(1));
            Assert.Equal((nuint)1, mi_slice_count_of_size(MI_ARENA_SLICE_SIZE));
            Assert.Equal((nuint)2, mi_slice_count_of_size(MI_ARENA_SLICE_SIZE + 1));
            Assert.Equal(MI_ARENA_SLICE_SIZE * 3, mi_size_of_slices(3));
        }
    }
}
