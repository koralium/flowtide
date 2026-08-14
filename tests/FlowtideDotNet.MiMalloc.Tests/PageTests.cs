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

// Tests for PageQueue.cs / Page.cs / ArenaPages.cs: the size-class bin function
// against invariants, and full page lifecycles (fresh alloc, block pop/free,
// collect, queue, retire, abandon/reclaim) using a hand-built theap (mirroring
// what theap-init will do in task #8: copy the empty template, wire tld/heap).

using System.Runtime.CompilerServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class BinTests
    {
        [Fact]
        public void Bin_SmallSizes_Align2W()
        {
            // MI_ALIGN2W: wsize <= 8 -> round to double word sizes
            Assert.Equal((nuint)1, _mi_bin(1));      // wsize 1
            Assert.Equal((nuint)1, _mi_bin(8));      // wsize 1
            Assert.Equal((nuint)2, _mi_bin(16));     // wsize 2
            Assert.Equal((nuint)4, _mi_bin(24));     // wsize 3 -> 4
            Assert.Equal((nuint)4, _mi_bin(32));     // wsize 4
            Assert.Equal((nuint)6, _mi_bin(40));     // wsize 5 -> 6
            Assert.Equal((nuint)6, _mi_bin(48));     // wsize 6
            Assert.Equal((nuint)8, _mi_bin(64));     // wsize 8
        }

        [Fact]
        public void Bin_HugeThreshold()
        {
            Assert.Equal((nuint)MI_BIN_HUGE, _mi_bin(MI_LARGE_MAX_OBJ_SIZE + 1));
            Assert.True(_mi_bin(MI_LARGE_MAX_OBJ_SIZE) < MI_BIN_HUGE);
            // types.h invariant: the largest non-huge bin must fit the arena bins
            Assert.True(_mi_bin(MI_LARGE_MAX_OBJ_SIZE) < MI_ARENA_BIN_COUNT);
            Assert.True(_mi_bin(MI_LARGE_MAX_OBJ_SIZE) <= MI_MAX_SINGLETON_BIN);
        }

        [Fact]
        public void Bin_RoundtripWithBinSize()
        {
            // for every REACHABLE bin, the bin of its size must be the bin itself.
            // Unreachable table entries: odd small bins 3,5,7 (MI_ALIGN2W rounds to double
            // words) and bins > MI_MAX_SINGLETON_BIN (mi_bin caps at 60 for sizes up to
            // MI_LARGE_MAX_OBJ_SIZE; larger sizes go to MI_BIN_HUGE).
            for (nuint bin = 1; bin <= MI_MAX_SINGLETON_BIN; bin++)
            {
                if (bin < 8 && (bin % 2) == 1 && bin != 1) continue;   // skip unreachable odd bins 3,5,7
                nuint size = _mi_bin_size(bin);
                Assert.Equal(bin, _mi_bin(size));
            }
            // bin sizes are strictly increasing (except the initial duplicate wsize-1 entry)
            for (nuint bin = 2; bin < MI_BIN_HUGE; bin++)
            {
                Assert.True(_mi_bin_size(bin) > _mi_bin_size(bin - 1));
            }
        }

        [Fact]
        public void Bin_MonotoneAndBounded()
        {
            // _mi_bin must be monotone in size and always <= its bin size (12.5% waste bound)
            nuint prevBin = 0;
            for (nuint size = 8; size <= MI_LARGE_MAX_OBJ_SIZE; size += (size < 1024 ? 8 : size / 8))
            {
                nuint bin = _mi_bin(size);
                Assert.True(bin >= prevBin, $"bin not monotone at size {size}");
                nuint binSize = _mi_bin_size(bin);
                Assert.True(binSize >= size, $"bin size {binSize} < size {size}");
                // 12.5% waste bound (bin sizes are spaced in 12.5% increments after the first 8)
                Assert.True(binSize <= size + size / 4 + 2 * (nuint)MI_INTPTR_SIZE, $"bin size {binSize} too large for {size}");
                prevBin = bin;
            }
        }

        [Fact]
        public void GoodSize_MatchesBins()
        {
            Assert.Equal((nuint)8, mi_good_size(1));
            Assert.Equal((nuint)16, mi_good_size(9));
            Assert.Equal((nuint)1024, mi_good_size(1000));
            Assert.True(mi_good_size(100_000) >= 100_000);
            // above LARGE_MAX: page-aligned
            nuint big = MI_LARGE_MAX_OBJ_SIZE + 1;
            Assert.Equal(_mi_align_up(big, _mi_os_page_size()), mi_good_size(big));
        }
    }

    public unsafe class PageTests : IDisposable
    {
        private readonly mi_tld_t* _tld;
        private readonly mi_theap_t* _theap;
        private readonly mi_memid_t _tldMemid;
        private readonly mi_memid_t _theapMemid;

        public PageTests()
        {
            _mi_os_init();
            mi_subproc_t* subproc = _mi_subproc_main();
            mi_heap_t* heap = subproc->heaps;

            // hand-build a tld + theap for the current test thread (task #8 does this in theap.c)
            fixed (mi_memid_t* tldMemid = &_tldMemid, theapMemid = &_theapMemid)
            {
                _tld = (mi_tld_t*)_mi_meta_zalloc(subproc, (nuint)sizeof(mi_tld_t), tldMemid);
                _theap = (mi_theap_t*)_mi_meta_zalloc(subproc, (nuint)sizeof(mi_theap_t), theapMemid);
            }
            Assert.True(_tld != null && _theap != null);
            _tld->thread_id = _mi_thread_id();
            _tld->thread_seq = 0;
            _tld->numa_node = -1;
            _tld->subproc = subproc;

            *_theap = *_mi_theap_empty();   // copy the empty template (queues + free-direct initialized)
            _theap->tld = _tld;
            _theap->heap = heap;
            _theap->subproc = subproc;
            _theap->page_full_retain = 2;
            _theap->allow_page_reclaim = true;
            _theap->allow_page_abandon = true;
        }

        public void Dispose()
        {
            mi_subproc_t* subproc = _mi_subproc_main();
            _mi_meta_free(subproc, _theap, (nuint)sizeof(mi_theap_t), _theapMemid);
            _mi_meta_free(subproc, _tld, (nuint)sizeof(mi_tld_t), _tldMemid);
        }

        // pop one block from the page free list (the alloc.c fast-path, done manually)
        private static void* PopBlock(mi_page_t* page)
        {
            mi_block_t* block = page->free;
            Assert.True(block != null);
            page->free = mi_block_next(page, block);
            page->used++;
            return block;
        }

        // free one block to the local free list (the free.c local path, done manually)
        private static void FreeBlockLocal(mi_page_t* page, void* p)
        {
            mi_block_t* block = (mi_block_t*)p;
            mi_block_set_next(page, block, page->local_free);
            page->local_free = block;
            page->used--;
        }

        [Theory]
        [InlineData(64u)]       // small page (64 KiB)
        [InlineData(2048u)]     // small page, larger blocks
        [InlineData(16u * 1024)]   // medium page (512 KiB)
        [InlineData(100u * 1024)]  // large page (4 MiB)
        public void PageAlloc_FreshPage_Initialized(uint blockSizeInt)
        {
            nuint block_size = blockSizeInt;
            mi_page_t* page = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(page != null);
            try
            {
                Assert.True(mi_page_is_owned(page));
                Assert.False(mi_page_is_abandoned(page));
                Assert.Equal(block_size, mi_page_block_size(page));
                Assert.True(page->reserved > 1);
                Assert.True(mi_page_immediate_available(page));
                // page map: every block resolves to the page
                Assert.True(_mi_ptr_page(mi_page_start(page)) == page);
                Assert.True(_mi_safe_ptr_page(mi_page_start(page) + block_size) == page);

                // pop blocks (respect the initial capacity: pages with blocks >= 4 KiB
                // only extend one block at a time -- MI_MAX_EXTEND_SIZE heuristic)
                int popCount = page->capacity >= 2 ? 2 : 1;
                void* b1 = PopBlock(page);
                void* b2 = popCount == 2 ? PopBlock(page) : null;
                Assert.True(b2 == null || b1 != b2);
                Assert.Equal(popCount, (int)page->used);

                // free them back and collect
                FreeBlockLocal(page, b1);
                if (b2 != null) FreeBlockLocal(page, b2);
                _mi_page_free_collect(page, false);
                Assert.True(mi_page_all_free(page));

                // free the page (not in a queue: abandon then arena-free)
                mi_page_set_theap(page, null);
                _mi_arenas_page_free(page, _theap);
                page = null;
            }
            finally
            {
                if (page != null)
                {
                    mi_page_set_theap(page, null);
                    _mi_arenas_page_free(page, _theap);
                }
            }
        }

        [Fact]
        public void PageAlloc_Singleton_HugeBlock()
        {
            nuint block_size = 1024 * 1024;   // 1 MiB > MI_LARGE_MAX_OBJ_SIZE
            mi_page_t* page = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(page != null);
            Assert.True(mi_page_is_singleton(page));
            Assert.True(mi_page_is_huge(page));
            Assert.Equal(1, (int)page->reserved);
            Assert.True(mi_page_immediate_available(page));

            void* b = PopBlock(page);
            Assert.True(mi_page_is_full(page));
            // write the whole block
            _mi_memset(b, 0x5A, block_size);
            FreeBlockLocal(page, b);
            _mi_page_free_collect(page, false);
            Assert.True(mi_page_all_free(page));

            mi_page_set_theap(page, null);
            _mi_arenas_page_free(page, _theap);
        }

        [Fact]
        public void PageQueue_ReclaimRetireCollect()
        {
            nuint block_size = 128;
            mi_page_t* page = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(page != null);

            // put the page in the theap queue (reclaim path: abandon first, then reclaim)
            mi_page_set_theap(page, null);   // abandoned (but still owned)
            Assert.True(mi_page_is_abandoned(page));
            _mi_theap_page_reclaim(_theap, page);
            Assert.False(mi_page_is_abandoned(page));
            Assert.Equal(1, (int)_theap->page_count);

            mi_page_queue_t* pq = mi_page_queue(_theap, block_size);
            Assert.True(pq->first == page && pq->last == page);
            Assert.Equal(block_size, pq->block_size);
            Assert.True(_mi_page_queue_is_valid(_theap, pq));

            // the pages_free_direct entry for this size points at our page
            Assert.True(_mi_theap_get_free_small_page(_theap, block_size) == page);

            // use a block, then free it and retire the page
            void* b = PopBlock(page);
            FreeBlockLocal(page, b);
            _mi_page_free_collect(page, false);
            Assert.True(mi_page_all_free(page));

            _mi_page_retire(page);
            // only page in its queue: stays retired instead of freed
            Assert.True(page->retire_expire > 0);
            Assert.True(pq->first == page);

            // forced retired-collect frees it
            _mi_theap_collect_retired(_theap, true);
            Assert.True(pq->first == null);
            Assert.Equal(0, (int)_theap->page_count);
        }

        [Fact]
        public void PageAbandon_ReclaimViaAbandonedMap()
        {
            nuint block_size = 320;
            mi_heap_t* heap = _mi_theap_heap(_theap);
            nuint bin = _mi_bin(block_size);

            mi_page_t* page = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(page != null);
            mi_page_set_theap(page, null);
            _mi_theap_page_reclaim(_theap, page);
            mi_page_queue_t* pq = mi_page_queue(_theap, block_size);

            // keep one block used so the page is partially used (cannot be freed)
            void* b = PopBlock(page);

            nuint abandonedBefore = mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]);

            // abandon: page is partially used arena page -> goes to the abandoned map
            _mi_page_abandon(page, pq);
            Assert.True(mi_page_is_abandoned(page));
            Assert.True(mi_page_is_abandoned_mapped(page));
            Assert.False(mi_page_is_owned(page));   // unowned after abandon
            Assert.Equal(abandonedBefore + 1, mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]));

            // allocating a page of the same size class must find the abandoned page
            mi_page_t* found = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(found == page, "abandoned page should be reclaimed by the next allocation");
            Assert.True(mi_page_is_owned(page));
            Assert.True(mi_page_is_abandoned(page));   // returned as abandoned
            // note: the thread-id still reads abandoned-mapped (4) until reclaimed --
            // only the bitmap entry was cleared by the claim (C protocol)
            Assert.Equal(abandonedBefore, mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]));

            // reclaim it into our theap: set_theap clears the abandoned/mapped state
            _mi_theap_page_reclaim(_theap, page);
            Assert.False(mi_page_is_abandoned(page));
            Assert.False(mi_page_is_abandoned_mapped(page));
            FreeBlockLocal(page, b);
            _mi_page_free_collect(page, false);
            Assert.True(mi_page_all_free(page));
            _mi_page_free(page, mi_page_queue(_theap, block_size));
            Assert.Equal(0, (int)_theap->page_count);
        }

        [Fact]
        public void PageAbandon_UnabandonRoundtrip()
        {
            nuint block_size = 640;
            mi_heap_t* heap = _mi_theap_heap(_theap);
            nuint bin = _mi_bin(block_size);

            mi_page_t* page = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(page != null);
            mi_page_set_theap(page, null);
            _mi_theap_page_reclaim(_theap, page);
            void* b = PopBlock(page);

            // `abandoned_count` is per-BIN on the process-global main heap, so assert the DELTA
            // this test causes -- never an absolute value (see PageAbandon_ReclaimViaAbandonedMap,
            // and the note in AssemblyInfo.cs).
            nuint abandonedBefore = mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]);

            _mi_page_abandon(page, mi_page_queue(_theap, block_size));
            Assert.True(mi_page_is_abandoned_mapped(page));
            Assert.Equal(abandonedBefore + 1, mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]));

            // simulate the free.c multi-threaded free path: claim ownership, then unabandon
            Assert.True(mi_page_claim_ownership(page));
            _mi_arenas_page_unabandon(page, _theap);
            Assert.False(mi_page_is_abandoned_mapped(page));
            Assert.Equal(abandonedBefore, mi_atomic_load_relaxed(ref *(nuint*)&heap->abandoned_count[(int)bin]));

            // now free the block and the page
            _mi_theap_page_reclaim(_theap, page);
            FreeBlockLocal(page, b);
            _mi_page_free_collect(page, false);
            _mi_page_free(page, mi_page_queue(_theap, block_size));
        }

        [Fact]
        public void ThreadFreeCollect_CrossThread()
        {
            nuint block_size = 256;
            mi_page_t* page = _mi_arenas_page_alloc(_theap, block_size, 1);
            Assert.True(page != null);

            // pop as many blocks as the initial extend gave us (multiple of 4 for the tasks)
            int count = (int)page->capacity & ~3;
            Assert.True(count >= 4);
            var blocks = new nuint[count];
            for (int i = 0; i < blocks.Length; i++) { blocks[i] = (nuint)PopBlock(page); }
            Assert.Equal(count, (int)page->used);

            // free them from OTHER threads via the atomic xthread_free push (free.c mt path)
            mi_page_t* p = page;
            int per = count / 4;
            var tasks = new Task[4];
            for (int t = 0; t < 4; t++)
            {
                int start = t * per;
                tasks[t] = Task.Run(() =>
                {
                    for (int i = start; i < start + per; i++)
                    {
                        mi_block_t* block = (mi_block_t*)blocks[i];
                        // mi_free_block_mt push loop
                        nuint tf = mi_atomic_load_relaxed(ref p->xthread_free);
                        nuint tfNew;
                        do
                        {
                            mi_block_set_next(p, block, mi_tf_block(tf));
                            tfNew = mi_tf_create(block, mi_tf_is_owned(tf));
                        } while (!mi_atomic_cas_weak_release(ref p->xthread_free, ref tf, tfNew));
                    }
                });
            }
            Task.WaitAll(tasks);

            // collect on the owning thread: all blocks come back
            _mi_page_free_collect(page, false);
            Assert.Equal(0, (int)page->used);
            Assert.True(mi_page_all_free(page));
            // ownership was never lost
            Assert.True(mi_page_is_owned(page));

            mi_page_set_theap(page, null);
            _mi_arenas_page_free(page, _theap);
        }
    }
}
