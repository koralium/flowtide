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

// Tests for first-class heaps coexisting: separation between live heaps, delete/destroy
// with other heaps alive, the dynamic TLS key lifecycle across heap delete/create, one
// heap shared by multiple threads (per-thread theaps), and realloc across heaps.

using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class HeapTests
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

        [Fact]
        public void MultipleHeaps_AreSeparate()
        {
            RunOnThread(() =>
            {
                mi_heap_t* a = mi_heap_new();
                mi_heap_t* b = mi_heap_new();
                Assert.True(a != null && b != null && a != b);

                // same size class from both heaps: blocks land in DIFFERENT pages
                byte* pa = (byte*)mi_heap_malloc(a, 200);
                byte* pb = (byte*)mi_heap_malloc(b, 200);
                byte* pm = (byte*)mi_malloc(200);   // and one from the main heap
                Assert.True(pa != null && pb != null && pm != null);
                Assert.True(_mi_ptr_page(pa) != _mi_ptr_page(pb));
                Assert.True(_mi_ptr_page(pa) != _mi_ptr_page(pm));

                // ownership resolves to the right heap
                Assert.True(mi_heap_of(pa) == a);
                Assert.True(mi_heap_of(pb) == b);
                Assert.True(mi_heap_of(pm) == mi_heap_main());
                Assert.True(mi_heap_contains(a, pa));
                Assert.False(mi_heap_contains(a, pb));
                Assert.False(mi_heap_contains(b, pa));

                // writes to one heap's block never touch another's
                _mi_memset(pa, 0xAA, 200);
                _mi_memset(pb, 0xBB, 200);
                Assert.Equal(0xAA, pa[199]);
                Assert.Equal(0xBB, pb[199]);

                // mi_free routes by page, no heap argument needed
                mi_free(pa);
                mi_free(pb);
                mi_free(pm);
                mi_heap_delete(a);
                mi_heap_delete(b);
                mi_thread_done();
            });
        }

        [Fact]
        public void HeapDelete_LeavesOtherHeapsUntouched()
        {
            RunOnThread(() =>
            {
                mi_subproc_t* subproc = _mi_subproc_main();
                nuint heapsBefore = mi_atomic_load_relaxed(ref subproc->heap_count);

                mi_heap_t* a = mi_heap_new();
                mi_heap_t* b = mi_heap_new();
                Assert.Equal(heapsBefore + 2, mi_atomic_load_relaxed(ref subproc->heap_count));

                byte* pa = (byte*)mi_heap_zalloc(a, 512);
                byte* pb = (byte*)mi_heap_zalloc(b, 512);
                _mi_memset(pb, 0x5C, 512);

                // delete A while it still has a live block: the block survives, now owned
                // by the main heap; B is untouched
                mi_heap_delete(a);
                Assert.Equal(heapsBefore + 1, mi_atomic_load_relaxed(ref subproc->heap_count));
                Assert.True(mi_heap_of(pa) == mi_heap_main());
                Assert.True(mi_heap_of(pb) == b);
                Assert.Equal(0x5C, pb[511]);

                // both blocks still freeable, B still usable
                mi_free(pa);
                void* pb2 = mi_heap_malloc(b, 64);
                Assert.True(pb2 != null && mi_heap_of(pb2) == b);
                mi_free(pb);
                mi_free(pb2);
                mi_heap_delete(b);
                Assert.Equal(heapsBefore, mi_atomic_load_relaxed(ref subproc->heap_count));
                mi_thread_done();
            });
        }

        [Fact]
        public void HeapDestroy_LeavesOtherHeapsUntouched()
        {
            RunOnThread(() =>
            {
                mi_heap_t* a = mi_heap_new();
                mi_heap_t* b = mi_heap_new();

                // several live blocks across size classes in A
                for (int i = 0; i < 50; i++)
                {
                    Assert.True(mi_heap_malloc(a, (nuint)(32 + i * 97)) != null);
                }
                byte* pb = (byte*)mi_heap_malloc(b, 4096);
                _mi_memset(pb, 0x7E, 4096);

                // destroy wipes all of A's blocks without freeing them individually
                mi_heap_destroy(a);

                // B is intact and usable
                Assert.Equal(0x7E, pb[0]);
                Assert.Equal(0x7E, pb[4095]);
                Assert.True(mi_heap_of(pb) == b);
                void* pb2 = mi_heap_malloc(b, 128);
                Assert.True(pb2 != null);
                mi_free(pb);
                mi_free(pb2);
                mi_heap_delete(b);
                mi_collect(true);
                mi_thread_done();
            });
        }

        [Fact]
        public void HeapTlsKey_NoAliasingAfterDeleteAndRecreate()
        {
            RunOnThread(() =>
            {
                // initialize a theap for heap A (binds A's dynamic TLS slot on this thread)
                mi_heap_t* a = mi_heap_new();
                void* pa = mi_heap_malloc(a, 96);
                mi_theap_t* theapA = _mi_heap_theap_peek(a);
                Assert.True(theapA != null && _mi_theap_heap_peek(theapA) == a);

                nuint keyA = a->theap;
                mi_free(pa);
                mi_heap_delete(a);

                // a fresh heap likely reuses the freed key's slot index, but with a bumped
                // version: it must NOT see A's stale theap through the recycled slot
                mi_heap_t* b = mi_heap_new();
                Assert.True(keyA != b->theap);   // version differs even if the index is reused
                Assert.True(_mi_thread_local_get(b->theap) == null);   // starts empty

                void* pb = mi_heap_malloc(b, 96);
                mi_theap_t* theapB = _mi_heap_theap_peek(b);
                Assert.True(theapB != null);
                Assert.True(_mi_theap_heap_peek(theapB) == b);   // wired to B, not a stale A theap
                Assert.True(mi_heap_of(pb) == b);

                mi_free(pb);
                mi_heap_delete(b);
                mi_thread_done();
            });
        }

        [Fact]
        public void OneHeap_UsedFromMultipleThreads_GetsPerThreadTheaps()
        {
            const int N = 4;
            mi_heap_t* heap = null;
            var theaps = new nint[N];
            var blocks = new nint[N];
            RunOnThread(() =>
            {
                heap = mi_heap_new();
                Assert.True(heap != null);
            });

            using var start = new Barrier(N);
            using var sampled = new Barrier(N);   // keep theaps alive until all are sampled
            Exception? error = null;
            var threads = new Thread[N];
            for (int i = 0; i < N; i++)
            {
                int idx = i;
                threads[i] = new Thread(() =>
                {
                    try
                    {
                        start.SignalAndWait();
                        byte* p = (byte*)mi_heap_malloc(heap, 700);
                        Assert.True(p != null);
                        p[0] = (byte)idx;
                        blocks[idx] = (nint)p;
                        theaps[idx] = (nint)_mi_heap_theap_peek(heap);
                        Assert.True((mi_theap_t*)theaps[idx] != null);
                    }
                    catch (Exception e) { Interlocked.CompareExchange(ref error, e, null); }
                    finally
                    {
                        try { sampled.SignalAndWait(TimeSpan.FromSeconds(30)); } catch { }
                        mi_thread_done();
                    }
                });
                threads[i].IsBackground = true;
                threads[i].Start();
            }
            foreach (var t in threads) Assert.True(t.Join(TimeSpan.FromSeconds(60)));
            if (error != null) throw error;

            // every thread got its OWN theap on the shared heap
            Assert.Equal(N, theaps.Distinct().Count());

            // all blocks belong to the shared heap and are freeable from this thread
            for (int i = 0; i < N; i++)
            {
                byte* p = (byte*)blocks[i];
                Assert.True(mi_heap_of(p) == heap);
                Assert.Equal((byte)i, p[0]);
                mi_free(p);
            }
            RunOnThread(() =>
            {
                mi_heap_delete(heap);
                mi_thread_done();
            });
        }

        [Fact]
        public void ReallocAcrossHeaps_MovesToTargetHeap()
        {
            RunOnThread(() =>
            {
                mi_heap_t* a = mi_heap_new();
                mi_heap_t* b = mi_heap_new();

                byte* p = (byte*)mi_heap_malloc(a, 100);
                for (int i = 0; i < 100; i++) { p[i] = (byte)i; }

                // same size would be reused in place, but only within the SAME heap:
                // realloc via B must move the block into B and preserve the content
                byte* q = (byte*)mi_heap_realloc(b, p, 100);
                Assert.True(q != null && q != p);
                Assert.True(mi_heap_of(q) == b);
                for (int i = 0; i < 100; i++) { Assert.Equal((byte)i, q[i]); }

                mi_free(q);
                mi_heap_delete(a);
                mi_heap_delete(b);
                mi_thread_done();
            });
        }

        [Fact]
        public void HeapCollect_FreesEmptyPages()
        {
            RunOnThread(() =>
            {
                mi_heap_t* heap = mi_heap_new();
                var ptrs = new nint[64];
                for (int i = 0; i < 64; i++)
                {
                    ptrs[i] = (nint)mi_heap_malloc(heap, 1500);
                    Assert.True(ptrs[i] != 0);
                }
                foreach (var ptr in ptrs) { mi_free((void*)ptr); }

                mi_heap_collect(heap, true);
                mi_theap_t* theap = _mi_heap_theap_peek(heap);
                Assert.True(theap != null);
                Assert.Equal(0, (int)theap->page_count);

                mi_heap_delete(heap);
                mi_thread_done();
            });
        }
    }
}
