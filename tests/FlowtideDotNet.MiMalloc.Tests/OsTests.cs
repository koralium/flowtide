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

// Tests for Os.cs/PrimOs.cs: real OS memory roundtrips (reserve, commit, touch,
// decommit, free), aligned allocation through the direct and fallback paths, and
// the good-alloc-size table from os.c.

using System.Runtime.InteropServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class OsTests : IDisposable
    {
        private readonly mi_subproc_t* _subproc;

        public OsTests()
        {
            _mi_os_init();
            // the os layer only touches subproc->stats; a zeroed struct suffices here
            _subproc = (mi_subproc_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_subproc_t));
        }

        public void Dispose()
        {
            NativeMemory.Free(_subproc);
        }

        [Fact]
        public void PageSize_IsSane()
        {
            nuint ps = _mi_os_page_size();
            Assert.True(ps >= 4096 && ps <= 65536);
            Assert.True(_mi_is_power_of_two(ps));
        }

        [Fact]
        public void GoodAllocSize_MatchesCTable()
        {
            nuint ps = _mi_os_page_size();
            Assert.Equal(ps, _mi_os_good_alloc_size(1));
            // < 512KiB rounds to the page size
            Assert.Equal(_mi_align_up(512 * 1024 - 1, ps), _mi_os_good_alloc_size(512 * 1024 - 1));
            // < 2MiB rounds to 64KiB
            Assert.Equal((nuint)(576 * 1024), _mi_os_good_alloc_size(520 * 1024));
            // < 8MiB rounds to 256KiB
            Assert.Equal((nuint)(4 * 1024 * 1024), _mi_os_good_alloc_size(4 * 1024 * 1024 - 1000));
            // < 32MiB rounds to 1MiB
            Assert.Equal((nuint)(17 * 1024 * 1024), _mi_os_good_alloc_size(16 * 1024 * 1024 + 1));
            // >= 32MiB rounds to 4MiB
            Assert.Equal((nuint)(36 * 1024 * 1024), _mi_os_good_alloc_size(33 * 1024 * 1024));
        }

        [Fact]
        public void Alloc_CommittedMemory_IsWritable()
        {
            mi_memid_t memid;
            nuint size = 2 * MI_ARENA_SLICE_SIZE;
            byte* p = (byte*)_mi_os_alloc(_subproc, size, &memid);
            Assert.True(p != null);
            Assert.True(memid.initially_committed);
            Assert.Equal(mi_memkind_t.MI_MEM_OS, memid.memkind);

            // touch every page
            for (nuint i = 0; i < size; i += _mi_os_page_size())
            {
                p[i] = 0xAB;
            }
            Assert.Equal(0xAB, p[0]);

            _mi_os_free(_subproc, p, size, memid);
        }

        [Fact]
        public void Zalloc_IsZeroed()
        {
            mi_memid_t memid;
            nuint size = MI_ARENA_SLICE_SIZE;
            byte* p = (byte*)_mi_os_zalloc(_subproc, size, &memid);
            Assert.True(p != null);
            Assert.True(memid.initially_zero);
            Assert.True(mi_mem_is_zero(p, size));
            _mi_os_free(_subproc, p, size, memid);
        }

        [Fact]
        public void AllocAligned_SliceAlignment()
        {
            // the arena layer always uses MI_ARENA_SLICE_ALIGN (64KiB)
            mi_memid_t memid;
            nuint size = 4 * MI_ARENA_SLICE_SIZE;
            void* p = _mi_os_alloc_aligned(_subproc, size, MI_ARENA_SLICE_ALIGN, true, false, &memid);
            Assert.True(p != null);
            Assert.True(_mi_is_aligned(p, MI_ARENA_SLICE_ALIGN));
            Assert.True(memid.initially_committed);
            // memory is writable
            ((byte*)p)[0] = 1;
            ((byte*)p)[size - 1] = 2;
            _mi_os_free(_subproc, p, size, memid);
        }

        [Theory]
        [InlineData(1 * 1024 * 1024)]     // 1 MiB alignment: > granularity, forces the aligned paths
        [InlineData(4 * 1024 * 1024)]     // 4 MiB alignment (mimalloc large page alignment)
        public void AllocAligned_LargeAlignment(int alignmentInt)
        {
            nuint alignment = (nuint)alignmentInt;
            mi_memid_t memid;
            nuint size = alignment;   // size == alignment: uses hint/VirtualAlloc2/overallocate paths
            void* p = _mi_os_alloc_aligned(_subproc, size, alignment, true, false, &memid);
            Assert.True(p != null);
            Assert.True(_mi_is_aligned(p, alignment));
            // base may differ from p (over-allocation fallback); free must handle that via memid
            ((byte*)p)[0] = 42;
            ((byte*)p)[size - 1] = 43;
            _mi_os_free(_subproc, p, size, memid);
        }

        [Fact]
        public void AllocAligned_ReserveOnly_CommitDecommitRoundtrip()
        {
            mi_memid_t memid;
            nuint size = 4 * MI_ARENA_SLICE_SIZE;
            void* p = _mi_os_alloc_aligned(_subproc, size, MI_ARENA_SLICE_ALIGN, false /* commit */, false, &memid);
            Assert.True(p != null);
            Assert.False(memid.initially_committed);

            // commit the first two slices and write into them
            Assert.True(_mi_os_commit(_subproc, p, 2 * MI_ARENA_SLICE_SIZE, null));
            byte* b = (byte*)p;
            b[0] = 1;
            b[2 * MI_ARENA_SLICE_SIZE - 1] = 2;

            // decommit them again
            Assert.True(_mi_os_decommit(_subproc, p, 2 * MI_ARENA_SLICE_SIZE));

            // recommit and verify the memory is zero (fresh pages on all our platforms after
            // decommit: MEM_DECOMMIT on Windows guarantees it; MADV_DONTNEED gives zero pages
            // on next touch on Linux)
            bool is_zero = false;
            Assert.True(_mi_os_commit(_subproc, p, 2 * MI_ARENA_SLICE_SIZE, &is_zero));
            Assert.Equal(0, b[0]);
            Assert.Equal(0, b[2 * MI_ARENA_SLICE_SIZE - 1]);

            _mi_os_free_ex(_subproc, p, size, true, memid);
        }

        [Fact]
        public void Purge_DecommitsWhenOptionEnabled()
        {
            // purge_decommits defaults to 1
            Assert.True(mi_option_is_enabled(mi_option_t.mi_option_purge_decommits));

            mi_memid_t memid;
            nuint size = 2 * MI_ARENA_SLICE_SIZE;
            byte* p = (byte*)_mi_os_alloc(_subproc, size, &memid);
            Assert.True(p != null);
            p[0] = 99;

            bool needs_recommit = _mi_os_purge(_subproc, p, size);
            if (OperatingSystem.IsWindows())
            {
                // windows decommit always needs recommit
                Assert.True(needs_recommit);
                Assert.True(_mi_os_commit(_subproc, p, size, null));
            }
            else
            {
                // unix MADV_DONTNEED path: reusable without recommit
                Assert.False(needs_recommit);
            }
            if (!OperatingSystem.IsMacOS())
            {
                // after purge + (re)commit the content is gone (zero); on macOS
                // MADV_FREE_REUSABLE may retain contents until reclaim, so skip there
                Assert.Equal(0, p[0]);
            }

            _mi_os_free(_subproc, p, size, memid);
        }

        [Fact]
        public void FreeEx_UncommittedMemory()
        {
            mi_memid_t memid;
            nuint size = MI_ARENA_SLICE_SIZE;
            void* p = _mi_os_alloc_aligned(_subproc, size, MI_ARENA_SLICE_ALIGN, false, false, &memid);
            Assert.True(p != null);
            _mi_os_free_ex(_subproc, p, size, false /* still_committed */, memid);
        }

        [Fact]
        public void Stats_TrackReservedAndCommitted()
        {
            mi_subproc_t* sp = (mi_subproc_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_subproc_t));
            try
            {
                mi_memid_t memid;
                nuint size = 2 * MI_ARENA_SLICE_SIZE;
                void* p = _mi_os_alloc(sp, size, &memid);
                Assert.True(p != null);
                Assert.True(sp->stats.reserved.current >= (long)size);
                Assert.True(sp->stats.committed.current >= (long)size);

                _mi_os_free(sp, p, size, memid);
                Assert.Equal(0, sp->stats.reserved.current);
                Assert.Equal(0, sp->stats.committed.current);
                Assert.True(sp->stats.reserved.peak >= (long)size);
            }
            finally
            {
                NativeMemory.Free(sp);
            }
        }

        [Fact]
        public void AlignedAtOffset_AlignsAtOffset()
        {
            mi_memid_t memid;
            nuint size = 4 * MI_ARENA_SLICE_SIZE;
            nuint alignment = MI_ARENA_SLICE_ALIGN;
            nuint offset = _mi_os_page_size();
            byte* p = (byte*)_mi_os_alloc_aligned_at_offset(_subproc, size, alignment, offset, true, false, &memid);
            Assert.True(p != null);
            Assert.True(_mi_is_aligned(p + offset, alignment));
            p[0] = 7;
            _mi_os_free_ex(_subproc, p, size, true, memid);
        }
    }
}
