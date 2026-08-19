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

// Tests for Atomic.cs: C11-style fetch_* return values (previous value), CAS
// expected-update-on-failure semantics, pointer variants, the embedded lock,
// and multi-threaded stress for the primitives the allocator's lock-free paths rely on.

using System.Runtime.InteropServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class AtomicTests
    {
        [Fact]
        public void FetchAdd_ReturnsPreviousValue()
        {
            nuint v = 10;
            Assert.Equal((nuint)10, mi_atomic_add_relaxed(ref v, 5));
            Assert.Equal((nuint)15, v);
            Assert.Equal((nuint)15, mi_atomic_sub_relaxed(ref v, 3));
            Assert.Equal((nuint)12, v);
        }

        [Fact]
        public void FetchAnd_Or_ReturnPreviousValue()
        {
            nuint v = 0b1111;
            Assert.Equal((nuint)0b1111, mi_atomic_and_acq_rel(ref v, 0b1010));
            Assert.Equal((nuint)0b1010, v);
            Assert.Equal((nuint)0b1010, mi_atomic_or_acq_rel(ref v, 0b0101));
            Assert.Equal((nuint)0b1111, v);
        }

        [Fact]
        public void Cas_UpdatesExpectedOnFailure()
        {
            nuint v = 42;
            nuint expected = 41;
            Assert.False(mi_atomic_cas_strong_acq_rel(ref v, ref expected, 100));
            Assert.Equal((nuint)42, expected);   // expected updated to observed value
            Assert.Equal((nuint)42, v);          // value unchanged
            Assert.True(mi_atomic_cas_strong_acq_rel(ref v, ref expected, 100));
            Assert.Equal((nuint)100, v);
        }

        [Fact]
        public void SubWithOverflowWrap_MatchesC()
        {
            // C: fetch_sub on unsigned wraps; the port emulates sub via add of two's complement
            nuint v = 2;
            Assert.Equal((nuint)2, mi_atomic_sub_relaxed(ref v, 5));
            Assert.Equal(unchecked((nuint)0 - 3), v);
        }

        [Fact]
        public void AddI_SubI_SignedPreviousValue()
        {
            nint v = -5;
            Assert.Equal((nint)(-5), mi_atomic_addi(ref v, 10));
            Assert.Equal((nint)5, v);
            Assert.Equal((nint)5, mi_atomic_subi(ref v, 7));
            Assert.Equal((nint)(-2), v);
        }

        [Fact]
        public void MaxI64_OnlyIncreases()
        {
            long v = 10;
            mi_atomic_maxi64_relaxed(ref v, 5);
            Assert.Equal(10, v);
            mi_atomic_maxi64_relaxed(ref v, 20);
            Assert.Equal(20, v);
        }

        [Fact]
        public void PointerOps_LoadStoreExchangeCas()
        {
            int a = 1, b = 2;
            int* pa = &a, pb = &b;
            int* slot = pa;

            Assert.True(slot == mi_atomic_load_ptr_acquire(&slot));
            mi_atomic_store_ptr_release(&slot, pb);
            Assert.True(pb == slot);

            int* prev = mi_atomic_exchange_ptr_acq_rel(&slot, pa);
            Assert.True(pb == prev);
            Assert.True(pa == slot);

            int* expected = pb;
            Assert.False(mi_atomic_cas_ptr_strong_acq_rel(&slot, &expected, pb));
            Assert.True(pa == expected);   // expected updated to observed
            Assert.True(mi_atomic_cas_ptr_strong_acq_rel(&slot, &expected, pb));
            Assert.True(pb == slot);
        }

        [Fact]
        public void ConcurrentIncrement_NoLostUpdates()
        {
            const int threads = 8;
            const int iterations = 200_000;
            nuint* counter = (nuint*)NativeMemory.AllocZeroed((nuint)sizeof(nuint));
            try
            {
                var tasks = new Task[threads];
                for (int t = 0; t < threads; t++)
                {
                    tasks[t] = Task.Run(() =>
                    {
                        for (int i = 0; i < iterations; i++)
                        {
                            mi_atomic_increment_relaxed(ref *counter);
                        }
                    });
                }
                Task.WaitAll(tasks);
                Assert.Equal((nuint)(threads * iterations), *counter);
            }
            finally
            {
                NativeMemory.Free(counter);
            }
        }

        [Fact]
        public void ConcurrentCasPush_LockFreeStackIsConsistent()
        {
            // models mimalloc's xthread_free push: CAS a singly-linked list head
            const int threads = 8;
            const int perThread = 50_000;
            int total = threads * perThread;

            var nodes = (StackNode*)NativeMemory.AllocZeroed((nuint)(total * sizeof(StackNode)));
            // the head lives in native memory: lambdas cannot take the address of a captured local
            StackNode** head = (StackNode**)NativeMemory.AllocZeroed((nuint)sizeof(StackNode*));
            try
            {
                var tasks = new Task[threads];
                for (int t = 0; t < threads; t++)
                {
                    int start = t * perThread;
                    tasks[t] = Task.Run(() =>
                    {
                        for (int i = 0; i < perThread; i++)
                        {
                            StackNode* node = &nodes[start + i];
                            node->value = start + i;
                            StackNode* expected = mi_atomic_load_ptr_relaxed(head);
                            do
                            {
                                node->next = expected;
                            } while (!mi_atomic_cas_ptr_weak_release(head, &expected, node));
                        }
                    });
                }
                Task.WaitAll(tasks);

                // walk the list: every pushed node must be present exactly once
                var seen = new bool[total];
                int count = 0;
                for (StackNode* p = *head; p != null; p = p->next)
                {
                    Assert.False(seen[p->value], "node seen twice");
                    seen[p->value] = true;
                    count++;
                }
                Assert.Equal(total, count);
            }
            finally
            {
                NativeMemory.Free(nodes);
                NativeMemory.Free(head);
            }
        }

        [Fact]
        public void Lock_MutualExclusion()
        {
            mi_lock_t lk = default;
            mi_lock_t* plk = &lk;
            mi_lock_init(plk);

            long counter = 0;
            long* pcounter = &counter;
            const int threads = 8;
            const int iterations = 20_000;

            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        mi_lock_acquire(plk);
                        // non-atomic increment under the lock
                        *pcounter = *pcounter + 1;
                        mi_lock_release(plk);
                    }
                });
            }
            Task.WaitAll(tasks);
            Assert.Equal((long)threads * iterations, counter);
            mi_lock_done(plk);
        }

        [Fact]
        public void Lock_TryAcquire()
        {
            mi_lock_t lk = default;
            mi_lock_t* plk = &lk;
            mi_lock_init(plk);
            Assert.True(mi_lock_try_acquire(plk));
            Assert.False(mi_lock_try_acquire(plk));
            mi_lock_release(plk);
            Assert.True(mi_lock_try_acquire(plk));
            mi_lock_release(plk);
        }

        [Fact]
        public void Once_RunsExactlyOnceAcrossThreads()
        {
            mi_atomic_once_t once = default;
            mi_atomic_once_t* ponce = &once;
            int runs = 0;
            int* pruns = &runs;
            const int threads = 8;

            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                tasks[t] = Task.Run(() =>
                {
                    if (_mi_atomic_once_enter(ponce))
                    {
                        Interlocked.Increment(ref *pruns);
                        _mi_atomic_once_release(ponce);
                    }
                });
            }
            Task.WaitAll(tasks);
            Assert.Equal(1, runs);
        }

        [Fact]
        public void ThreadIds_UniqueAndFlagBitsClear()
        {
            const int threads = 16;
            var ids = new nuint[threads];
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int slot = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    ids[slot] = _mi_thread_id();
                    // stable within a thread
                    Assert.Equal(ids[slot], _mi_thread_id());
                }, TaskCreationOptions.LongRunning);
            }
            Task.WaitAll(tasks);
            var set = new HashSet<nuint>();
            foreach (var id in ids)
            {
                Assert.True(id > 4);                        // > MI_THREADID_ABANDONED_MAPPED
                Assert.Equal((nuint)0, id & MI_PAGE_FLAG_MASK);  // bottom bits clear
                Assert.True(set.Add(id), "thread id not unique");
            }
        }

        private struct StackNode
        {
            public StackNode* next;
            public int value;
        }
    }
}
