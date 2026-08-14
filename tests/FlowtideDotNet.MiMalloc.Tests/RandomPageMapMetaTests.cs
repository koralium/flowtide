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

// Tests for Random.cs (chacha20 vs the RFC 8439 vectors quoted in mimalloc's random.c),
// PageMap.cs (register/lookup/unregister roundtrips), and ArenaMeta.cs (meta allocator
// with the temporary OS fallback).

using System.Runtime.InteropServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class RandomTests
    {
        [Fact]
        public void QRound_MatchesRfc8439Vector()
        {
            // RFC 8439 2.1.1 test vector (as quoted in mimalloc random.c)
            uint* x = stackalloc uint[4] { 0x11111111, 0x01020304, 0x9b8d6f43, 0x01234567 };
            uint[] expected = { 0xea2a92f4, 0xcb1cf8ce, 0x4581472e, 0x5881c4bb };
            qround(x, 0, 1, 2, 3);
            for (int i = 0; i < 4; i++) Assert.Equal(expected[i], x[i]);
        }

        [Fact]
        public void QRound_MatchesRfc8439StateVector()
        {
            // RFC 8439 2.2.1: QUARTERROUND(2, 7, 8, 13) on a full state
            uint* y = stackalloc uint[16] {
                0x879531e0, 0xc5ecf37d, 0x516461b1, 0xc9a62f8a,
                0x44c20ef3, 0x3390af7f, 0xd9fc690b, 0x2a5f714c,
                0x53372767, 0xb00a5631, 0x974c541a, 0x359e9963,
                0x5c971061, 0x3d631689, 0x2098d9d6, 0x91dbd320 };
            uint[] expected = {
                0x879531e0, 0xc5ecf37d, 0xbdb886dc, 0xc9a62f8a,
                0x44c20ef3, 0x3390af7f, 0xd9fc690b, 0xcfacafd2,
                0xe46bea80, 0xb00a5631, 0x974c541a, 0x359e9963,
                0x5c971061, 0xccc07c79, 0x2098d9d6, 0x91dbd320 };
            qround(y, 2, 7, 8, 13);
            for (int i = 0; i < 16; i++) Assert.Equal(expected[i], y[i]);
        }

        [Fact]
        public void ChachaBlock_MatchesRfc8439BlockVector()
        {
            // RFC 8439 2.3.2 block function test vector (as quoted in mimalloc random.c)
            mi_random_ctx_t ctx = default;
            uint[] input = {
                0x61707865, 0x3320646e, 0x79622d32, 0x6b206574,
                0x03020100, 0x07060504, 0x0b0a0908, 0x0f0e0d0c,
                0x13121110, 0x17161514, 0x1b1a1918, 0x1f1e1d1c,
                0x00000001, 0x09000000, 0x4a000000, 0x00000000 };
            for (int i = 0; i < 16; i++) ctx.input[i] = input[i];
            uint[] expected = {
                0xe4e7f110, 0x15593bd1, 0x1fdd0f50, 0xc47120a3,
                0xc7f4d1c7, 0x0368c033, 0x9aaa2204, 0x4e6cd4c3,
                0x466482d2, 0x09aa9f07, 0x05d7c214, 0xa2028bd9,
                0xd19c12b5, 0xb94e16de, 0xe883d0cb, 0x4e3c50a2 };
            chacha_block(&ctx);
            for (int i = 0; i < 16; i++) Assert.Equal(expected[i], ctx.output[i]);
            // the counter is incremented afterwards
            Assert.Equal(2u, ctx.input[12]);
        }

        [Fact]
        public void RandomNext_NonZeroAndVarying()
        {
            mi_random_ctx_t ctx = default;
            _mi_random_init(&ctx);
            Assert.False(ctx.weak);   // OS randomness must be available in .NET
            var seen = new HashSet<nuint>();
            for (int i = 0; i < 1000; i++)
            {
                nuint r = _mi_random_next(&ctx);
                Assert.NotEqual((nuint)0, r);
                seen.Add(r);
            }
            Assert.True(seen.Count > 990, "random values should essentially never repeat");
        }

        [Fact]
        public void RandomSplit_ProducesDistinctStream()
        {
            mi_random_ctx_t ctx = default;
            _mi_random_init(&ctx);
            mi_random_ctx_t ctx2 = default;
            _mi_random_split(&ctx, &ctx2);
            // streams must differ
            bool anyDifferent = false;
            for (int i = 0; i < 16 && !anyDifferent; i++)
            {
                anyDifferent = _mi_random_next(&ctx) != _mi_random_next(&ctx2);
            }
            Assert.True(anyDifferent);
        }

        [Fact]
        public void Shuffle_NeverReturnsZero()
        {
            Assert.NotEqual((nuint)0, _mi_random_shuffle(0));
            // splitmix64 FINALIZER of 1 (mimalloc's shuffle is the mix step only, no gamma)
            Assert.Equal(unchecked((nuint)0x5692161d100b05e5ul), _mi_random_shuffle(1));
        }

        [Fact]
        public void OsRandomWeak_NonZero()
        {
            Assert.NotEqual((nuint)0, _mi_os_random_weak(0));
            Assert.NotEqual((nuint)0, _mi_os_random_weak(12345));
        }
    }

    public unsafe class PageMapTests
    {
        public PageMapTests()
        {
            _mi_os_init();
        }

        [Fact]
        public void PtrPage_NullIsNull_BeforeAndAfterInit()
        {
            // works via the empty map even before init
            Assert.True(_mi_ptr_page(null) == null);
            Assert.True(_mi_page_map_init());
            Assert.True(_mi_ptr_page(null) == null);
            Assert.True(_mi_safe_ptr_page(null) == null);
        }

        [Fact]
        public void RegisterLookupUnregister_Roundtrip()
        {
            Assert.True(_mi_page_map_init());

            // build a fake page: 64KiB aligned area, mi_page_t at the start (like a
            // non-separated page), blocks following the page info
            nuint area_size = MI_ARENA_SLICE_SIZE;
            byte* area = (byte*)NativeMemory.AlignedAlloc(area_size, MI_ARENA_SLICE_SIZE);
            try
            {
                NativeMemory.Clear(area, area_size);
                mi_page_t* page = (mi_page_t*)area;
                nuint info = mi_page_info_size();
                page->page_ma_offset = (uint)(info / MI_MAX_ALIGN_SIZE);
                page->block_size = 64;
                page->reserved = (ushort)((area_size - info) / 64);
                page->memid = _mi_memid_create_os(area, area_size, true, true, false);

                Assert.True(mi_page_start(page) == area + info);
                Assert.True(mi_page_slice_start(page) == area);   // OS memid: not separated

                Assert.True(_mi_page_map_register(page));

                // every address in the slice resolves to the page
                Assert.True(_mi_ptr_page(area) == page);
                Assert.True(_mi_ptr_page(area + info) == page);
                Assert.True(_mi_ptr_page(area + area_size - 1) == page);
                Assert.True(_mi_safe_ptr_page(area + 100) == page);
                Assert.True(mi_is_in_heap_region(area + 500));

                // neighboring slices do not
                Assert.True(_mi_safe_ptr_page(area + area_size) != page);

                _mi_page_map_unregister(page);
                Assert.True(_mi_safe_ptr_page(area + 100) == null);
                Assert.False(mi_is_in_heap_region(area + 500));
            }
            finally
            {
                NativeMemory.AlignedFree(area);
            }
        }

        [Fact]
        public void SafePtrPage_WildPointers()
        {
            Assert.True(_mi_page_map_init());
            // beyond max address
            Assert.True(_mi_safe_ptr_page((void*)nuint.MaxValue) == null);
            // random uncommitted region
            Assert.True(_mi_safe_ptr_page((void*)0x1234_5678_9000) == null);
        }
    }

    public unsafe class ArenaMetaTests
    {
        public ArenaMetaTests()
        {
            _mi_os_init();
        }

        [Fact]
        public void MetaZalloc_ZeroedAndAligned()
        {
            mi_subproc_t* subproc = _mi_subproc_main();
            mi_memid_t memid;
            void* p = _mi_meta_zalloc(subproc, 300, &memid);
            Assert.True(p != null);
            // note: until the Arena.cs port (task #6) meta allocations fall back to the OS
            Assert.True(memid.memkind == mi_memkind_t.MI_MEM_OS || memid.memkind == mi_memkind_t.MI_MEM_META);
            Assert.True(mi_mem_is_zero(p, 300));
            _mi_meta_free(subproc, p, 300, memid);
        }

        [Fact]
        public void MetaZalloc_SizeLimits()
        {
            mi_subproc_t* subproc = _mi_subproc_main();
            mi_memid_t memid;
            Assert.True(_mi_meta_zalloc(subproc, 0, &memid) == null);
            Assert.True(_mi_meta_zalloc(subproc, MI_META_MAX_SIZE + 1, &memid) == null);
            void* p = _mi_meta_zalloc(subproc, MI_META_MAX_SIZE, &memid);
            Assert.True(p != null);
            _mi_meta_free(subproc, p, MI_META_MAX_SIZE, memid);
        }

        [Fact]
        public void MetaZalloc_ManyDistinct()
        {
            mi_subproc_t* subproc = _mi_subproc_main();
            var list = new List<(nuint, mi_memid_t)>();
            for (int i = 0; i < 100; i++)
            {
                mi_memid_t memid;
                void* p = _mi_meta_zalloc(subproc, 128, &memid);
                Assert.True(p != null);
                ((byte*)p)[0] = 0xEE;   // touch
                list.Add(((nuint)p, memid));
            }
            Assert.Equal(100, list.Select(t => t.Item1).Distinct().Count());
            foreach (var (p, memid) in list)
            {
                _mi_meta_free(subproc, (void*)p, 128, memid);
            }
        }
    }
}
