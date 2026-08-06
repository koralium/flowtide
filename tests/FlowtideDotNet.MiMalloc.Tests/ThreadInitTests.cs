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

// Tests for the task #8 machinery: thread init/done (Init.cs), theaps (Theap.cs),
// the heap<->theap association (Heap.cs), dynamic thread locals (ThreadLocalMi.cs)
// and the GC-finalizer thread-exit sentinel.

using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class ThreadInitTests
    {
        // run `body` on a dedicated (non-threadpool) thread and propagate failures
        private static void RunOnThread(Action body)
        {
            Exception? error = null;
            var t = new Thread(() =>
            {
                try { body(); }
                catch (Exception e) { error = e; }
            });
            t.IsBackground = true;
            t.Start();
            Assert.True(t.Join(TimeSpan.FromSeconds(60)), "worker thread did not finish");
            if (error != null) throw error;
        }

        [Fact]
        public void ThreadInit_InitializesDefaultTheap()
        {
            RunOnThread(() =>
            {
                Assert.False(_mi_thread_is_initialized());
                mi_theap_t* theap = _mi_thread_init();
                Assert.True(theap != null);
                Assert.True(_mi_thread_is_initialized());
                Assert.True(_mi_theap_default() == theap);
                Assert.True(mi_theap_is_initialized(theap));

                // wired to the main heap and this thread's tld
                mi_heap_t* heap_main = mi_heap_main();
                Assert.True(_mi_theap_heap(theap) == heap_main);
                Assert.True(theap->tld != null);
                Assert.Equal(_mi_thread_id(), theap->tld->thread_id);
                Assert.True(theap->tld->subproc == _mi_subproc_main());
                Assert.True(_mi_subproc() == _mi_subproc_main());
                Assert.True((theap->cookie & 1) == 1);   // cookie = random | 1

                // the fast slot holds the theap (main heap uses mi_thread_local_key_fast)
                Assert.Equal(mi_thread_local_key_fast, heap_main->theap);
                Assert.True((mi_theap_t*)_mi_thread_local_get(heap_main->theap) == theap);

                // idempotent
                Assert.True(_mi_thread_init() == theap);
                Assert.True(mi_theap_get_default() == theap);

                // on the tld and heap theap lists
                Assert.True(theap->tld->theaps == theap);

                mi_thread_done();
                Assert.False(_mi_thread_is_initialized());
                Assert.True(_mi_is_empty_theap(_mi_theap_default()));
            });
        }

        [Fact]
        public void HeapTheap_CachesAndRefcounts()
        {
            RunOnThread(() =>
            {
                mi_heap_t* heap_main = mi_heap_main();
                // _mi_heap_theap initializes the thread on demand and caches the theap
                mi_theap_t* theap = _mi_heap_theap(heap_main);
                Assert.True(mi_theap_is_initialized(theap));
                Assert.True(theap == _mi_theap_default());
                Assert.True(_mi_theap_cached() == theap);
                // refcount: 1 (initial) + 1 (cached)
                Assert.Equal((nuint)2, theap->refcount);
                // peek does not re-initialize
                Assert.True(_mi_heap_theap_peek(heap_main) == theap);
                // the page->theap association peek resolves through the fast slot
                mi_thread_done();
            });
        }

        [Fact]
        public void ThreadDone_AbandonsUsedPages()
        {
            mi_page_t* page = null;
            void* block = null;
            RunOnThread(() =>
            {
                mi_theap_t* theap = _mi_thread_init();
                Assert.True(theap != null);

                // allocate a real page into the theap's queues and keep one block "allocated"
                page = _mi_arenas_page_alloc(theap, 896, 1);
                Assert.True(page != null);
                mi_page_set_theap(page, null);
                _mi_theap_page_reclaim(theap, page);
                Assert.Equal(1, (int)theap->page_count);
                block = page->free;
                Assert.True(block != null);
                page->free = mi_block_next(page, page->free);
                page->used++;

                mi_thread_done();
            });

            // the still-used page was abandoned (mapped in the arena bitmaps for reclaim)
            Assert.True(mi_page_is_abandoned(page));
            Assert.True(mi_page_is_abandoned_mapped(page));
            Assert.False(mi_page_is_owned(page));

            // clean up: reclaim the page into a fresh theap on this thread and free it
            mi_theap_t* mine = mi_theap_get_default();
            mi_page_t* found = _mi_arenas_page_alloc(mine, 896, 1);
            Assert.True(found == page);   // the abandoned page is found before fresh allocation
            mi_block_t* b = (mi_block_t*)block;
            mi_block_set_next(page, b, page->local_free);
            page->local_free = b;
            page->used--;
            _mi_page_free_collect(page, false);
            Assert.True(mi_page_all_free(page));
            mi_page_set_theap(page, null);
            _mi_arenas_page_free(page, mine);
        }

        [Fact]
        public void ThreadExit_WithoutThreadDone_FinalizerAbandonsPages()
        {
            mi_page_t* page = null;
            void* block = null;
            RunOnThread(() =>
            {
                mi_theap_t* theap = _mi_thread_init();
                Assert.True(theap != null);
                page = _mi_arenas_page_alloc(theap, 1792, 1);
                Assert.True(page != null);
                mi_page_set_theap(page, null);
                _mi_theap_page_reclaim(theap, page);
                block = page->free;
                Assert.True(block != null);
                page->free = mi_block_next(page, page->free);
                page->used++;
                // exit WITHOUT calling mi_thread_done -- the sentinel finalizer must clean up
            });

            // the page is still owned by the dead thread until the finalizer runs
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!mi_page_is_abandoned_mapped(page) && DateTime.UtcNow < deadline)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                Thread.Sleep(10);
            }
            Assert.True(mi_page_is_abandoned_mapped(page), "sentinel finalizer did not abandon the dead thread's pages");
            Assert.False(mi_page_is_owned(page));

            // clean up: adopt the abandoned page and free it
            mi_theap_t* mine = mi_theap_get_default();
            mi_page_t* found = _mi_arenas_page_alloc(mine, 1792, 1);
            Assert.True(found == page);
            mi_block_t* b = (mi_block_t*)block;
            mi_block_set_next(page, b, page->local_free);
            page->local_free = b;
            page->used--;
            _mi_page_free_collect(page, false);
            mi_page_set_theap(page, null);
            _mi_arenas_page_free(page, mine);
        }

        [Fact]
        public void TheapCollect_ForcedFreesEmptyPages()
        {
            RunOnThread(() =>
            {
                mi_theap_t* theap = _mi_thread_init();
                mi_page_t* page = _mi_arenas_page_alloc(theap, 3584, 1);
                Assert.True(page != null);
                mi_page_set_theap(page, null);
                _mi_theap_page_reclaim(theap, page);

                // allocate and free one block locally
                void* b = page->free;
                page->free = mi_block_next(page, page->free);
                page->used++;
                mi_block_set_next(page, (mi_block_t*)b, page->local_free);
                page->local_free = (mi_block_t*)b;
                page->used--;

                mi_theap_collect(theap, true /* force */);
                Assert.Equal(0, (int)theap->page_count);

                mi_collect(true);   // smoke: process-wide entry point
                mi_thread_done();
            });
        }

        [Fact]
        public void ConcurrentThreadInit_DistinctTheapsAndIds()
        {
            const int N = 8;
            var theaps = new nint[N];
            var tids = new nuint[N];
            var threads = new Thread[N];
            using var barrier = new Barrier(N);
            using var sampled = new Barrier(N);
            Exception? error = null;
            for (int i = 0; i < N; i++)
            {
                int idx = i;
                threads[i] = new Thread(() =>
                {
                    try
                    {
                        barrier.SignalAndWait();
                        mi_theap_t* theap = _mi_thread_init();
                        Assert.True(theap != null);
                        theaps[idx] = (nint)theap;
                        tids[idx] = theap->tld->thread_id;
                        // every thread must see itself on its own tld list
                        Assert.True(theap->tld->theaps == theap);
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

            Assert.Equal(N, theaps.Distinct().Count());
            Assert.Equal(N, tids.Distinct().Count());
            foreach (var tid in tids) { Assert.True(tid > 4 && (tid & 3) == 0); }
        }
    }

    public unsafe class ThreadLocalTests
    {
        [Fact]
        public void Key_CreateFreeReuse_VersionsProtect()
        {
            nuint key1 = _mi_thread_local_create();
            Assert.True(key1 != 0 && key1 != mi_thread_local_key_fast);

            int value = 42;
            Assert.True(_mi_thread_local_set(key1, &value));
            Assert.True(_mi_thread_local_get(key1) == &value);

            _mi_thread_local_free(key1);
            nuint key2 = _mi_thread_local_create();
            Assert.True(key2 != 0 && key2 != key1);   // same slot may be reused but the version differs
            // the version protects the NEW key: even if it reuses key1's slot (which still
            // holds key1's version+value on this thread) it must read null until set.
            // (Reading a freed key is not protected -- same as C.)
            Assert.True(_mi_thread_local_get(key2) == null);
            int value2 = 43;
            Assert.True(_mi_thread_local_set(key2, &value2));
            Assert.True(_mi_thread_local_get(key2) == &value2);
            _mi_thread_local_free(key2);
        }

        [Fact]
        public void Slots_ExpandPreservesValues()
        {
            const int N = 40;   // forces the per-thread slots array to grow 16 -> 32 -> 64
            var keys = new nuint[N];
            var values = new int[N];
            for (int i = 0; i < N; i++)
            {
                keys[i] = _mi_thread_local_create();
                Assert.True(keys[i] != 0);
            }
            try
            {
                fixed (int* vals = values)
                {
                    for (int i = 0; i < N; i++)
                    {
                        vals[i] = i;
                        Assert.True(_mi_thread_local_set(keys[i], &vals[i]));
                    }
                    for (int i = 0; i < N; i++)
                    {
                        int* p = (int*)_mi_thread_local_get(keys[i]);
                        Assert.True(p == &vals[i]);
                        Assert.Equal(i, *p);
                    }
                }
            }
            finally
            {
                foreach (var k in keys) _mi_thread_local_free(k);
            }
        }

        [Fact]
        public void Values_AreThreadLocal()
        {
            nuint key = _mi_thread_local_create();
            int value = 7;
            Assert.True(_mi_thread_local_set(key, &value));
            try
            {
                void* seenOnOtherThread = (void*)1;
                Exception? error = null;
                var t = new Thread(() =>
                {
                    try { seenOnOtherThread = _mi_thread_local_get(key); }
                    catch (Exception e) { error = e; }
                });
                t.IsBackground = true;
                t.Start();
                Assert.True(t.Join(TimeSpan.FromSeconds(30)));
                if (error != null) throw error;
                Assert.True(seenOnOtherThread == null);       // other thread never set it
                Assert.True(_mi_thread_local_get(key) == &value);   // ours is untouched
            }
            finally
            {
                _mi_thread_local_free(key);
            }
        }

        [Fact]
        public void FastKey_UsesSlotFast()
        {
            RunOnFreshThread(() =>
            {
                // the fast key works before any theap exists
                int value = 11;
                Assert.True(_mi_thread_local_set(mi_thread_local_key_fast, &value));
                Assert.True(_mi_thread_local_get(mi_thread_local_key_fast) == &value);
                Assert.True(_mi_thread_local_set(mi_thread_local_key_fast, null));
                Assert.True(_mi_thread_local_get(mi_thread_local_key_fast) == null);
            });
        }

        private static void RunOnFreshThread(Action body)
        {
            Exception? error = null;
            var t = new Thread(() => { try { body(); } catch (Exception e) { error = e; } });
            t.IsBackground = true;
            t.Start();
            Assert.True(t.Join(TimeSpan.FromSeconds(30)));
            if (error != null) throw error;
        }
    }
}
