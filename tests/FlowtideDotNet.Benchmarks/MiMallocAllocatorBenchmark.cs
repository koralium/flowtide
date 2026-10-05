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

using BenchmarkDotNet.Attributes;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Benchmarks
{
    // alias inside the namespace: the FlowtideDotNet.MiMalloc NAMESPACE shadows the
    // simple type name `MiMalloc` when resolving from FlowtideDotNet.* namespaces
    using MiMalloc = FlowtideDotNet.MiMalloc.MiMalloc;

    /// <summary>
    /// Compares the managed mimalloc port against NativeMemory for the allocation
    /// patterns Flowtide uses (aligned allocation, free, realloc-grow, mixed-size churn).
    ///
    /// Run with:
    ///   dotnet run -c Release -f net10.0 --project tests/FlowtideDotNet.Benchmarks -- --filter *MiMallocAllocator* --job short --inProcess
    /// </summary>
    public unsafe class MiMallocAllocatorBenchmark
    {
        private const int OpsPerInvoke = 1000;
        private const int Alignment = 64;
        private const int ChurnSlots = 256;

        [Params(128, 2048, 16384, 262144)]
        public int Size;

        private void*[] _churnMimalloc = null!;
        private void*[] _churnNativeMemory = null!;
        private int[] _churnSizes = null!;

        [GlobalSetup]
        public void Setup()
        {
            // fault in the allocator
            void* p = MiMalloc.mi_malloc_aligned((nuint)Size, Alignment);
            MiMalloc.mi_free(p);

            _churnMimalloc = new void*[ChurnSlots];
            _churnNativeMemory = new void*[ChurnSlots];
            // deterministic mixed sizes around the parameter size (1/4x .. 2x)
            _churnSizes = new int[ChurnSlots];
            var rng = new Random(12345);
            for (int i = 0; i < ChurnSlots; i++)
            {
                _churnSizes[i] = Math.Max(16, Size / 4 + rng.Next(Size * 2 - Size / 4));
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            for (int i = 0; i < ChurnSlots; i++)
            {
                if (_churnMimalloc[i] != null) { MiMalloc.mi_free(_churnMimalloc[i]); _churnMimalloc[i] = null; }
                if (_churnNativeMemory[i] != null) { NativeMemory.AlignedFree(_churnNativeMemory[i]); _churnNativeMemory[i] = null; }
            }
            MiMalloc.mi_collect(true);
        }

        // ---------------- alloc + touch + free ----------------

        [Benchmark(OperationsPerInvoke = OpsPerInvoke, Baseline = true)]
        public void NativeMemory_AllocFree()
        {
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                byte* p = (byte*)NativeMemory.AlignedAlloc((nuint)Size, Alignment);
                p[0] = 1;
                p[Size - 1] = 1;
                NativeMemory.AlignedFree(p);
            }
        }

        [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
        public void Mimalloc_AllocFree()
        {
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                byte* p = (byte*)MiMalloc.mi_malloc_aligned((nuint)Size, Alignment);
                p[0] = 1;
                p[Size - 1] = 1;
                MiMalloc.mi_free(p);
            }
        }

        // ---------------- mixed-size churn (slot ring) ----------------

        [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
        public void NativeMemory_Churn()
        {
            var slots = _churnNativeMemory;
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                int s = i & (ChurnSlots - 1);
                if (slots[s] != null) { NativeMemory.AlignedFree(slots[s]); }
                byte* p = (byte*)NativeMemory.AlignedAlloc((nuint)_churnSizes[s], Alignment);
                p[0] = (byte)i;
                slots[s] = p;
            }
        }

        [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
        public void Mimalloc_Churn()
        {
            var slots = _churnMimalloc;
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                int s = i & (ChurnSlots - 1);
                if (slots[s] != null) { MiMalloc.mi_free(slots[s]); }
                byte* p = (byte*)MiMalloc.mi_malloc_aligned((nuint)_churnSizes[s], Alignment);
                p[0] = (byte)i;
                slots[s] = p;
            }
        }

        // ---------------- realloc grow chain ----------------

        [Benchmark(OperationsPerInvoke = 100)]
        public void Mimalloc_ReallocGrow()
        {
            for (int i = 0; i < 100; i++)
            {
                void* p = MiMalloc.mi_malloc_aligned(64, Alignment);
                for (nuint sz = 128; sz <= (nuint)Size; sz *= 2)
                {
                    p = MiMalloc.mi_realloc_aligned(p, sz, Alignment);
                }
                MiMalloc.mi_free(p);
            }
        }
    }

    /// <summary>
    /// Multi-threaded allocator throughput: every thread allocates and frees its own
    /// blocks concurrently (the common Flowtide pattern: each stream operator works
    /// on its own thread with thread-local pages).
    /// </summary>
    public unsafe class MiMallocAllocatorMtBenchmark
    {
        private const int Threads = 4;
        private const int OpsPerThread = 20_000;

        [Params(2048, 65536)]
        public int Size;

        [GlobalSetup]
        public void Setup()
        {
            void* p = FlowtideDotNet.MiMalloc.MiMalloc.mi_malloc((nuint)Size);
            FlowtideDotNet.MiMalloc.MiMalloc.mi_free(p);
        }

        private static void RunOnThreads(Action body)
        {
            var threads = new Thread[Threads];
            for (int t = 0; t < Threads; t++)
            {
                threads[t] = new Thread(() => body());
                threads[t].IsBackground = true;
            }
            foreach (var t in threads) t.Start();
            foreach (var t in threads) t.Join();
        }

        [Benchmark(Baseline = true)]
        public void NativeMemory_Mt()
        {
            int size = Size;
            RunOnThreads(() =>
            {
                for (int i = 0; i < OpsPerThread; i++)
                {
                    byte* p = (byte*)NativeMemory.AlignedAlloc((nuint)size, 64);
                    p[0] = 1;
                    NativeMemory.AlignedFree(p);
                }
            });
        }

        [Benchmark]
        public void Mimalloc_Mt()
        {
            int size = Size;
            RunOnThreads(() =>
            {
                for (int i = 0; i < OpsPerThread; i++)
                {
                    byte* p = (byte*)FlowtideDotNet.MiMalloc.MiMalloc.mi_malloc_aligned((nuint)size, 64);
                    p[0] = 1;
                    FlowtideDotNet.MiMalloc.MiMalloc.mi_free(p);
                }
                FlowtideDotNet.MiMalloc.MiMalloc.mi_thread_done();
            });
        }
    }
}
