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

// Tests for Arena.cs: arena reserve, slice allocation/free roundtrips, purging,
// and concurrent allocation stress. Arena state is process-global, so assertions
// are delta-based where needed.

using System.Runtime.InteropServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class ArenaTests
    {
        public ArenaTests()
        {
            _mi_os_init();
        }

        private static mi_heap_t* MainHeap()
        {
            mi_subproc_t* subproc = _mi_subproc_main();
            mi_heap_t* heap = subproc->heaps;
            Assert.True(heap != null);
            return heap;
        }

        [Fact]
        public void ArenasAlloc_SliceSizes_Roundtrip()
        {
            mi_heap_t* heap = MainHeap();
            mi_subproc_t* subproc = heap->subproc;

            // 1 slice (64 KiB), 8 slices (512 KiB), 64 slices (4 MiB)
            foreach (nuint size in new nuint[] { MI_ARENA_SLICE_SIZE, 8 * MI_ARENA_SLICE_SIZE, 64 * MI_ARENA_SLICE_SIZE })
            {
                mi_memid_t memid;
                byte* p = (byte*)_mi_arenas_alloc(heap, size, true /* commit */, true, null, 0, -1, &memid);
                Assert.True(p != null);
                Assert.Equal(mi_memkind_t.MI_MEM_ARENA, memid.memkind);
                Assert.True(_mi_is_aligned(p, MI_ARENA_SLICE_ALIGN));
                Assert.Equal(size / MI_ARENA_SLICE_SIZE, (nuint)memid.mem.arena.slice_count);
                // the arena contains the pointer
                Assert.True(mi_arena_contains(mi_arena_id_from_arena(memid.mem.arena.arena), p));
                // memory is usable
                p[0] = 0x11;
                p[size - 1] = 0x22;
                // free list bits are clear while allocated
                mi_arena_t* arena = memid.mem.arena.arena;
                Assert.True(mi_bbitmap_is_clearN(arena->slices_free, memid.mem.arena.slice_index, memid.mem.arena.slice_count));

                _mi_arenas_free(subproc, p, size, memid);
                // free again available
                Assert.True(mi_bbitmap_is_setN(arena->slices_free, memid.mem.arena.slice_index, memid.mem.arena.slice_count));
            }
        }

        [Fact]
        public void ArenasAlloc_ZeroInitialized_WhenClaimed()
        {
            mi_heap_t* heap = MainHeap();
            mi_memid_t memid;
            nuint size = MI_ARENA_SLICE_SIZE;
            byte* p = (byte*)_mi_arenas_alloc(heap, size, true, true, null, 0, -1, &memid);
            Assert.True(p != null);
            if (memid.initially_zero)
            {
                Assert.True(mi_mem_is_zero(p, size));
            }
            _mi_arenas_free(heap->subproc, p, size, memid);
        }

        [Fact]
        public void ArenasAlloc_TooSmall_FallsBackToOs()
        {
            mi_heap_t* heap = MainHeap();
            mi_memid_t memid;
            // below MI_ARENA_MIN_OBJ_SIZE the arena path is skipped -> OS
            void* p = _mi_arenas_alloc_aligned(heap, 4096, 4096, 0, true, false, null, 0, -1, &memid);
            Assert.True(p != null);
            Assert.True(mi_memkind_is_os(memid.memkind));
            _mi_arenas_free(heap->subproc, p, 4096, memid);
        }

        [Fact]
        public void ArenasAlloc_DistinctRanges()
        {
            mi_heap_t* heap = MainHeap();
            mi_subproc_t* subproc = heap->subproc;
            var allocs = new List<(nuint ptr, nuint size, mi_memid_t memid)>();
            for (int i = 0; i < 50; i++)
            {
                nuint size = ((nuint)(i % 4) + 1) * MI_ARENA_SLICE_SIZE;
                mi_memid_t memid;
                byte* p = (byte*)_mi_arenas_alloc(heap, size, true, true, null, (nuint)i, -1, &memid);
                Assert.True(p != null);
                allocs.Add(((nuint)p, size, memid));
            }
            // no two allocations overlap
            var sorted = allocs.OrderBy(a => a.ptr).ToList();
            for (int i = 1; i < sorted.Count; i++)
            {
                Assert.True(sorted[i - 1].ptr + sorted[i - 1].size <= sorted[i].ptr, "allocations overlap");
            }
            foreach (var (ptr, size, memid) in allocs)
            {
                _mi_arenas_free(subproc, (void*)ptr, size, memid);
            }
        }

        [Fact]
        public void ArenaReuse_FreedSlicesAreReused()
        {
            mi_heap_t* heap = MainHeap();
            mi_subproc_t* subproc = heap->subproc;
            nuint size = 2 * MI_ARENA_SLICE_SIZE;
            mi_memid_t memid1;
            void* p1 = _mi_arenas_alloc(heap, size, true, true, null, 0, -1, &memid1);
            Assert.True(p1 != null && memid1.memkind == mi_memkind_t.MI_MEM_ARENA);
            var arena = memid1.mem.arena.arena;
            var index = memid1.mem.arena.slice_index;
            _mi_arenas_free(subproc, p1, size, memid1);

            // with tseq 0 and the slot free again, the same range should be found
            mi_memid_t memid2;
            void* p2 = _mi_arenas_alloc(heap, size, true, true, null, 0, -1, &memid2);
            Assert.True(p2 != null && memid2.memkind == mi_memkind_t.MI_MEM_ARENA);
            Assert.True(memid2.mem.arena.arena == arena && memid2.mem.arena.slice_index == index,
                "freed slices should be found again by the next search");
            _mi_arenas_free(subproc, p2, size, memid2);
        }

        [Fact]
        public void Purge_Immediate_DecommitsFreedSlices()
        {
            long saved = mi_option_get(mi_option_t.mi_option_purge_delay);
            try
            {
                mi_option_set(mi_option_t.mi_option_purge_delay, 0);   // purge immediately on free
                mi_heap_t* heap = MainHeap();
                mi_subproc_t* subproc = heap->subproc;
                nuint size = 4 * MI_ARENA_SLICE_SIZE;
                mi_memid_t memid;
                byte* p = (byte*)_mi_arenas_alloc(heap, size, true, true, null, 0, -1, &memid);
                Assert.True(p != null && memid.memkind == mi_memkind_t.MI_MEM_ARENA);
                p[0] = 42;
                mi_arena_t* arena = memid.mem.arena.arena;
                nuint slice_index = memid.mem.arena.slice_index;

                _mi_arenas_free(subproc, p, size, memid);
                // immediate purge with purge_decommits=1: slices are decommitted
                Assert.True(mi_bitmap_is_clearN(arena->slices_committed, slice_index, 4));
                Assert.True(mi_bbitmap_is_setN(arena->slices_free, slice_index, 4));

                // allocating again recommits and (on decommit paths) the memory reads zero
                mi_memid_t memid2;
                byte* p2 = (byte*)_mi_arenas_alloc(heap, size, true, true, null, 0, -1, &memid2);
                Assert.True(p2 != null);
                Assert.True(memid2.initially_committed);
                p2[0] = 43;   // must not fault
                _mi_arenas_free(subproc, p2, size, memid2);
            }
            finally
            {
                mi_option_set(mi_option_t.mi_option_purge_delay, saved);
            }
        }

        [Fact]
        public void Purge_Delayed_RunsViaTryPurge()
        {
            long saved = mi_option_get(mi_option_t.mi_option_purge_delay);
            try
            {
                // long delay so concurrently running tests cannot purge our slices via a
                // non-forced collect before we assert; an EXCLUSIVE arena keeps other
                // tests from allocating/freeing in it at all.
                mi_option_set(mi_option_t.mi_option_purge_delay, 10_000);
                mi_heap_t* heap = MainHeap();
                mi_subproc_t* subproc = heap->subproc;

                void* arena_id = null;
                Assert.Equal(0, mi_reserve_os_memory_ex(64 * 1024 * 1024, true /* commit */, false, true /* exclusive */, &arena_id));
                mi_arena_t* req_arena = _mi_arena_from_id(arena_id);

                nuint size = 4 * MI_ARENA_SLICE_SIZE;
                mi_memid_t memid;
                byte* p = (byte*)_mi_arenas_alloc(heap, size, true, true, req_arena, 0, -1, &memid);
                Assert.True(p != null && memid.memkind == mi_memkind_t.MI_MEM_ARENA);
                mi_arena_t* arena = memid.mem.arena.arena;
                Assert.True(arena == req_arena);
                nuint slice_index = memid.mem.arena.slice_index;

                _mi_arenas_free(subproc, p, size, memid);
                // scheduled: purge bits set, still committed
                Assert.True(mi_bitmap_is_setN(arena->slices_purge, slice_index, 4));
                Assert.True(mi_atomic_loadi64_relaxed(ref arena->purge_expire) != 0);

                // force the purge now
                mi_arenas_try_purge(true /* force */, true /* visit all */, subproc, 0);
                Assert.True(mi_bitmap_is_clearN(arena->slices_purge, slice_index, 4));
                Assert.True(mi_bitmap_is_clearN(arena->slices_committed, slice_index, 4));
                Assert.True(mi_bbitmap_is_setN(arena->slices_free, slice_index, 4));
            }
            finally
            {
                mi_option_set(mi_option_t.mi_option_purge_delay, saved);
            }
        }

        [Fact]
        public void MetaZalloc_NowArenaBacked()
        {
            // with Arena.cs in place the meta allocator should place blocks in meta pages
            mi_subproc_t* subproc = _mi_subproc_main();
            mi_memid_t memid;
            void* p = _mi_meta_zalloc(subproc, 256, &memid);
            Assert.True(p != null);
            Assert.Equal(mi_memkind_t.MI_MEM_META, memid.memkind);
            Assert.True(mi_mem_is_zero(p, 256));
            ((byte*)p)[0] = 1;
            _mi_meta_free(subproc, p, 256, memid);
            // freed meta memory is zeroed on free
            mi_memid_t memid2;
            void* p2 = _mi_meta_zalloc(subproc, 256, &memid2);
            Assert.True(p2 != null);
            Assert.True(mi_mem_is_zero(p2, 256));
            _mi_meta_free(subproc, p2, 256, memid2);
        }

        [Fact]
        public void ReserveOsMemory_CreatesArena()
        {
            mi_subproc_t* subproc = _mi_subproc_main();
            nuint before = mi_arenas_get_count(subproc);
            void* arena_id = null;
            int err = mi_reserve_os_memory_ex(64 * 1024 * 1024, false /* commit */, false, false, &arena_id);
            Assert.Equal(0, err);
            Assert.True(arena_id != null);
            nuint after = mi_arenas_get_count(subproc);
            Assert.True(after > before);
            mi_arena_t* arena = _mi_arena_from_id(arena_id);
            Assert.True(arena->slice_count >= 64UL * 1024 * 1024 / MI_ARENA_SLICE_SIZE);
            Assert.True(arena->info_slices > 0);
            // the info slices are not free for allocation
            Assert.True(mi_bbitmap_is_clearN(arena->slices_free, 0, arena->info_slices));
        }

        [Fact]
        public void HugePages_ReserveFailsGracefully()
        {
            Assert.Equal(ENOMEM, mi_reserve_huge_os_pages_at(1, -1, 100));
        }

        [Fact]
        public void Concurrent_ArenaAllocFree_NoOverlap()
        {
            mi_heap_t* heap = MainHeap();
            mi_subproc_t* subproc = heap->subproc;
            const int threads = 8;
            const int iterations = 2_000;
            uint[] sizesInSlices = { 1, 2, 8, 3, 1, 64, 4, 1 };

            // shadow map: slice ownership marks per (arena, slice) via a dictionary is too slow;
            // instead each thread writes a unique pattern and verifies it before free.
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                byte pattern = (byte)(0xA0 + t);
                nuint slices = sizesInSlices[t];
                nuint tseq = (nuint)t;
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        nuint size = slices * MI_ARENA_SLICE_SIZE;
                        mi_memid_t memid;
                        byte* p = (byte*)_mi_arenas_alloc(heap, size, true, true, null, tseq, -1, &memid);
                        Assert.True(p != null);
                        // write our pattern at the borders and middle
                        p[0] = pattern;
                        p[size / 2] = pattern;
                        p[size - 1] = pattern;
                        if ((i & 7) == 0) Thread.Yield();
                        // verify no other thread scribbled into our range
                        Assert.Equal(pattern, p[0]);
                        Assert.Equal(pattern, p[size / 2]);
                        Assert.Equal(pattern, p[size - 1]);
                        _mi_arenas_free(subproc, p, size, memid);
                    }
                });
            }
            Task.WaitAll(tasks);
        }
    }
}
