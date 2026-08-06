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

// Tests for task #9: the full malloc/zalloc/calloc/realloc/free API (Alloc.cs,
// Free.cs, AllocAligned.cs, _mi_malloc_generic), cross-thread frees, heap
// new/delete/destroy, and allocator stress.

using System.Collections.Concurrent;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class AllocTests
    {
        private static void RunOnThread(Action body)
        {
            Exception? error = null;
            var t = new Thread(() => { try { body(); } catch (Exception e) { error = e; } });
            t.IsBackground = true;
            t.Start();
            Assert.True(t.Join(TimeSpan.FromSeconds(120)), "worker thread did not finish");
            if (error != null) throw error;
        }

        [Theory]
        [InlineData(1u)]
        [InlineData(8u)]
        [InlineData(16u)]
        [InlineData(100u)]        // odd size
        [InlineData(1024u)]       // == MI_SMALL_SIZE_MAX
        [InlineData(1025u)]       // just above small max -> generic path
        [InlineData(8192u)]
        [InlineData(100_000u)]    // large page
        [InlineData(600_000u)]    // near the large object max
        [InlineData(5_000_000u)]  // huge singleton page
        public void Malloc_WriteReadFree_Roundtrip(uint sizeInt)
        {
            RunOnThread(() =>
            {
                nuint size = sizeInt;
                byte* p = (byte*)mi_malloc(size);
                Assert.True(p != null);
                nuint usable = mi_usable_size(p);
                Assert.True(usable >= size);

                // whole usable area must be writable and stable
                for (nuint i = 0; i < usable; i += 251) { p[i] = (byte)(i & 0xFF); }
                p[usable - 1] = 0xAB;
                for (nuint i = 0; i < usable - 1; i += 251) { Assert.Equal((byte)(i & 0xFF), p[i]); }
                Assert.Equal(0xAB, p[usable - 1]);

                mi_free(p);
                mi_collect(true);
                mi_thread_done();
            });
        }

        [Fact]
        public void Malloc_Zero_ReturnsValidPointer()
        {
            void* p = mi_malloc(0);
            Assert.True(p != null);
            mi_free(p);
        }

        [Fact]
        public void Free_Null_IsNoop()
        {
            mi_free(null);
            Assert.Equal((nuint)0, mi_usable_size(null));
        }

        [Fact]
        public void Malloc_FreeList_ReusesBlocks()
        {
            RunOnThread(() =>
            {
                // repeated alloc/free of the same size hits the thread-local fast path
                void* first = mi_malloc(48);
                mi_free(first);
                for (int i = 0; i < 1000; i++)
                {
                    void* p = mi_malloc(48);
                    Assert.True(p != null);
                    mi_free(p);
                }
                // the theap-local stat must balance to zero live bytes
                mi_theap_t* theap = _mi_theap_default();
                Assert.Equal(0, theap->stats.malloc_normal.current);
                mi_thread_done();
            });
        }

        [Theory]
        [InlineData(24u)]
        [InlineData(2000u)]
        [InlineData(70_000u)]
        public void Zalloc_ReturnsZeroedMemory(uint sizeInt)
        {
            nuint size = sizeInt;
            byte* p = (byte*)mi_zalloc(size);
            Assert.True(p != null);
            for (nuint i = 0; i < size; i++) { Assert.Equal(0, p[i]); }
            // dirty it, free, re-zalloc (likely reuses the block) and verify zeroing again
            _mi_memset(p, 0xCC, size);
            mi_free(p);
            byte* q = (byte*)mi_zalloc(size);
            Assert.True(q != null);
            for (nuint i = 0; i < size; i++) { Assert.Equal(0, q[i]); }
            mi_free(q);
        }

        [Fact]
        public void Calloc_OverflowReturnsNull()
        {
            Assert.True(mi_calloc(nuint.MaxValue / 2, 16) == null);
            Assert.True(mi_mallocn(nuint.MaxValue / 4, 8) == null);
        }

        [Fact]
        public void Realloc_PreservesContent()
        {
            byte* p = (byte*)mi_malloc(100);
            Assert.True(p != null);
            for (int i = 0; i < 100; i++) { p[i] = (byte)i; }

            // grow
            byte* q = (byte*)mi_realloc(p, 5000);
            Assert.True(q != null);
            for (int i = 0; i < 100; i++) { Assert.Equal((byte)i, q[i]); }

            // shrink within 50% -> same pointer
            byte* r = (byte*)mi_realloc(q, mi_usable_size(q) - 8);
            Assert.True(r == q);

            // realloc(p, 0) does not return null
            byte* s = (byte*)mi_realloc(r, 0);
            Assert.True(s != null);
            mi_free(s);

            // realloc(NULL, n) behaves as malloc
            byte* t = (byte*)mi_realloc(null, 64);
            Assert.True(t != null);
            mi_free(t);
        }

        [Fact]
        public void Rezalloc_ZeroesExtension()
        {
            byte* p = (byte*)mi_zalloc(64);
            _mi_memset(p, 0xEE, 64);
            byte* q = (byte*)mi_rezalloc(p, 4096);
            Assert.True(q != null);
            for (int i = 0; i < 64; i++) { Assert.Equal(0xEE, q[i]); }
            nuint usable = mi_usable_size(q);
            for (nuint i = 64; i < 4096; i++) { Assert.True(q[i] == 0, $"byte {i} not zero"); }
            mi_free(q);
        }

        [Theory]
        [InlineData(32u, 32u)]         // natural alignment fast path
        [InlineData(100u, 64u)]        // over-alloc path (alignment > size after rounding? no -> natural for 128? size 100 -> bin 112, not pow2 -> overalloc)
        [InlineData(51u, 256u)]        // alignment > size -> over-alloc
        [InlineData(1000u, 1024u)]
        [InlineData(70_000u, 4096u)]   // multiple-of-4KiB natural path
        [InlineData(100u, 65536u)]     // large alignment over-alloc
        [InlineData(1000u, 4u * 1024 * 1024)]   // > MI_PAGE_MAX_OVERALLOC_ALIGN -> huge singleton page
        public void MallocAligned_IsAlignedAndUsable(uint sizeInt, uint alignInt)
        {
            RunOnThread(() =>
            {
                nuint size = sizeInt, alignment = alignInt;
                byte* p = (byte*)mi_malloc_aligned(size, alignment);
                Assert.True(p != null);
                Assert.True(((nuint)p % alignment) == 0, "pointer not aligned");
                Assert.True(mi_usable_size(p) >= size);
                for (nuint i = 0; i < size; i++) { p[i] = 0x5A; }
                mi_free(p);   // must handle interior/aligned pointers
                mi_collect(true);
                mi_thread_done();
            });
        }

        [Fact]
        public void MallocAligned_BadAlignment_ReturnsNull()
        {
            Assert.True(mi_malloc_aligned(64, 3) == null);    // not a power of two
            Assert.True(mi_malloc_aligned(64, 0) == null);
        }

        [Fact]
        public void ZallocAligned_IsZeroed()
        {
            byte* p = (byte*)mi_zalloc_aligned(300, 128);
            Assert.True(p != null && ((nuint)p % 128) == 0);
            for (int i = 0; i < 300; i++) { Assert.Equal(0, p[i]); }
            mi_free(p);
        }

        [Fact]
        public void ReallocAligned_KeepsAlignment()
        {
            byte* p = (byte*)mi_malloc_aligned(200, 512);
            for (int i = 0; i < 200; i++) { p[i] = (byte)(i ^ 0x33); }
            byte* q = (byte*)mi_realloc_aligned(p, 20_000, 512);
            Assert.True(q != null && ((nuint)q % 512) == 0);
            for (int i = 0; i < 200; i++) { Assert.Equal((byte)(i ^ 0x33), q[i]); }
            mi_free(q);
        }

        [Fact]
        public void UsableSize_MatchesGoodSize()
        {
            for (nuint size = 8; size <= 100_000; size += (size < 1024 ? 8 : size / 3))
            {
                void* p = mi_malloc(size);
                Assert.True(p != null);
                Assert.Equal(mi_good_size(size), mi_usable_size(p));
                mi_free(p);
            }
        }

        [Fact]
        public void Version_MatchesMimallocHeaderEncoding()
        {
            // mimalloc.h: major + 2 digits minor + 2 digits patch => v3.4.4 == 30404
            Assert.Equal(30404, mi_version());
        }

        [Fact]
        public void Strnlen_BoundIsRespected()
        {
            // no terminator within max_len: must return max_len WITHOUT reading s[max_len]
            byte* s = stackalloc byte[4] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' };
            Assert.Equal((nuint)4, _mi_strnlen(s, 4));
            Assert.Equal((nuint)2, _mi_strnlen(s, 2));
            Assert.Equal((nuint)0, _mi_strnlen(s, 0));
            byte* z = stackalloc byte[3] { (byte)'a', 0, (byte)'c' };
            Assert.Equal((nuint)1, _mi_strnlen(z, 3));   // terminator before the bound
        }

        [Fact]
        public void Strdup_Roundtrip()
        {
            byte* src = stackalloc byte[6] { (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o', 0 };
            byte* d = mi_strdup(src);
            Assert.True(d != null && d != src);
            Assert.Equal(0, d[5]);
            for (int i = 0; i < 5; i++) { Assert.Equal(src[i], d[i]); }
            byte* n = mi_strndup(src, 3);
            Assert.True(_mi_strlen(n) == 3);
            mi_free(d);
            mi_free(n);
        }

        [Fact]
        public void CrossThread_Free_CollectsViaThreadFreeList()
        {
            const int N = 200;
            var blocks = new nint[N];
            RunOnThread(() =>
            {
                for (int i = 0; i < N; i++)
                {
                    void* p = mi_malloc(112);   // block size unique to this test
                    Assert.True(p != null);
                    blocks[i] = (nint)p;
                }
                // free them all from ANOTHER thread (multi-threaded free path)
                RunOnThread(() =>
                {
                    foreach (var b in blocks) { mi_free((void*)b); }
                });
                // collect on the owning thread: the xthread_free lists get merged and pages freed
                mi_collect(true);
                mi_theap_t* theap = _mi_theap_default();
                Assert.Equal(0, (int)theap->page_count);
                mi_thread_done();
            });
        }

        [Fact]
        public void ThreadExit_AbandonedPages_FreedByCrossThreadFrees()
        {
            const int N = 100;
            var blocks = new nint[N];
            RunOnThread(() =>
            {
                for (int i = 0; i < N; i++)
                {
                    void* p = mi_malloc(944);   // block size unique to this test
                    Assert.True(p != null);
                    blocks[i] = (nint)p;
                }
                mi_thread_done();   // abandons the (still used) pages
            });

            // now free every block from this thread: frees into abandoned pages take the
            // mi_free_try_collect_mt path (claim ownership -> free/reclaim/reabandon)
            foreach (var b in blocks) { mi_free((void*)b); }
            // pages should be gone; verify a fresh allocation still works
            void* q = mi_malloc(944);
            Assert.True(q != null);
            mi_free(q);
        }

        [Fact]
        public void SingleThread_ChurnStress()
        {
            RunOnThread(() =>
            {
                var rng = new Random(42);
                var live = new List<nint>();
                for (int round = 0; round < 20_000; round++)
                {
                    if (live.Count > 0 && (rng.Next(2) == 0 || live.Count > 500))
                    {
                        int idx = rng.Next(live.Count);
                        mi_free((void*)live[idx]);
                        live[idx] = live[^1];
                        live.RemoveAt(live.Count - 1);
                    }
                    else
                    {
                        nuint size = (nuint)(1 << rng.Next(4, 15)) + (nuint)rng.Next(64);
                        byte* p = (byte*)mi_malloc(size);
                        Assert.True(p != null);
                        p[0] = 1; p[size - 1] = 2;   // touch both ends
                        live.Add((nint)p);
                    }
                }
                foreach (var b in live) { mi_free((void*)b); }
                mi_collect(true);
                mi_theap_t* theap = _mi_theap_default();
                Assert.Equal(0, theap->stats.malloc_normal.current);
                mi_thread_done();
            });
        }

        [Fact]
        public void MultiThread_ProducerConsumer_Stress()
        {
            const int Threads = 6;
            const int PerThread = 3000;
            var queue = new ConcurrentQueue<nint>();
            using var done = new CountdownEvent(Threads);
            Exception? error = null;

            var workers = new Thread[Threads];
            for (int t = 0; t < Threads; t++)
            {
                int seed = 1234 + t;
                workers[t] = new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(seed);
                        for (int i = 0; i < PerThread; i++)
                        {
                            // allocate a block and either free it ourselves, or hand it to another thread
                            nuint size = (nuint)(8 + rng.Next(2000));
                            byte* p = (byte*)mi_malloc(size);
                            Assert.True(p != null);
                            p[0] = (byte)i;
                            if ((i & 3) == 0)
                            {
                                queue.Enqueue((nint)p);
                            }
                            else
                            {
                                mi_free(p);
                            }
                            // drain some blocks freed by other threads
                            if ((i & 7) == 0 && queue.TryDequeue(out nint other))
                            {
                                mi_free((void*)other);
                            }
                        }
                        mi_collect(false);
                        mi_thread_done();   // abandons any pages that still have foreign-owned blocks
                    }
                    catch (Exception e) { Interlocked.CompareExchange(ref error, e, null); }
                    finally { done.Signal(); }
                });
                workers[t].IsBackground = true;
                workers[t].Start();
            }
            foreach (var w in workers) { Assert.True(w.Join(TimeSpan.FromSeconds(120))); }
            Assert.True(done.Wait(TimeSpan.FromSeconds(10)));
            if (error != null) throw error;

            // free the leftovers (cross-thread frees into abandoned pages)
            while (queue.TryDequeue(out nint b)) { mi_free((void*)b); }
        }

        [Fact]
        public void Heap_NewDeleteDestroy()
        {
            RunOnThread(() =>
            {
                // fresh heap: allocating from it exercises the dynamic TLS key + a fresh theap
                mi_heap_t* heap = mi_heap_new();
                Assert.True(heap != null);
                byte* p = (byte*)mi_heap_malloc(heap, 128);
                byte* z = (byte*)mi_heap_zalloc(heap, 4000);
                Assert.True(p != null && z != null);
                Assert.Equal(0, z[3999]);
                Assert.True(mi_heap_contains(heap, p));
                Assert.True(mi_heap_of(p) == heap);
                Assert.False(mi_heap_contains(heap, null));

                // free one block, keep one live, then delete: live pages move to the main heap
                mi_free(p);
                mi_heap_delete(heap);
                Assert.True(mi_heap_of(z) == mi_heap_main());
                mi_free(z);   // still freeable after the move

                // destroy: blocks are wiped without freeing
                mi_heap_t* heap2 = mi_heap_new();
                Assert.True(heap2 != null);
                void* d = mi_heap_malloc(heap2, 256);
                Assert.True(d != null);
                mi_heap_destroy(heap2);
                // d is gone -- do not touch it; the page map may already unmap it

                mi_collect(true);
                mi_thread_done();
            });
        }

        [Fact]
        public void Heap_VisitBlocks_SeesLiveBlocks()
        {
            RunOnThread(() =>
            {
                mi_heap_t* heap = mi_heap_new();
                Assert.True(heap != null);
                void* a = mi_heap_malloc(heap, 320);
                void* b = mi_heap_malloc(heap, 320);
                Assert.True(a != null && b != null);

                nuint found = 0;
                nuint* foundPtr = &found;
                Assert.True(mi_heap_visit_blocks(heap, true, &CountBlockVisitor, foundPtr));
                Assert.Equal((nuint)2, found);

                mi_free(a);
                mi_free(b);
                mi_heap_delete(heap);
                mi_thread_done();
            });
        }

        private static bool CountBlockVisitor(mi_heap_t* heap, mi_heap_area_t* area, void* block, nuint block_size, void* arg)
        {
            if (block != null) { (*(nuint*)arg)++; }
            return true;
        }
    }
}
