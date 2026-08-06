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
    // aliases inside the namespace: the FlowtideDotNet.MiMalloc NAMESPACE shadows the
    // simple type name `MiMalloc` when resolving from FlowtideDotNet.* namespaces
    using ManagedMiMalloc = FlowtideDotNet.MiMalloc.MiMalloc;
    using NativeMiMalloc = FlowtideDotNet.Storage.Mimalloc.MiMalloc;

    /// <summary>
    /// Compares the managed mimalloc port against the native mimalloc library and
    /// NativeMemory for the allocation patterns Flowtide uses (aligned allocation,
    /// free, realloc-grow, and mixed-size churn).
    ///
    /// Run with:
    ///   dotnet run -c Release -f net10.0 --project tests/FlowtideDotNet.Benchmarks -- --filter *MiMallocAllocator* --job short
    /// </summary>
    public unsafe class MiMallocAllocatorBenchmark
    {
        private const int OpsPerInvoke = 1000;
        private const int Alignment = 64;
        private const int ChurnSlots = 256;

        [Params(128, 2048, 16384, 262144)]
        public int Size;

        private void*[] _churnManaged = null!;
        private void*[] _churnNative = null!;
        private void*[] _churnNativeMemory = null!;
        private int[] _churnSizes = null!;

        [GlobalSetup]
        public void Setup()
        {
            // fault in both allocators and make sure the native library is present
            void* p = ManagedMiMalloc.mi_malloc_aligned((nuint)Size, Alignment);
            ManagedMiMalloc.mi_free(p);
            void* q = NativeMiMalloc.mi_malloc_aligned((nuint)Size, Alignment);
            NativeMiMalloc.mi_free(q);

            _churnManaged = new void*[ChurnSlots];
            _churnNative = new void*[ChurnSlots];
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
                if (_churnManaged[i] != null) { ManagedMiMalloc.mi_free(_churnManaged[i]); _churnManaged[i] = null; }
                if (_churnNative[i] != null) { NativeMiMalloc.mi_free(_churnNative[i]); _churnNative[i] = null; }
                if (_churnNativeMemory[i] != null) { NativeMemory.AlignedFree(_churnNativeMemory[i]); _churnNativeMemory[i] = null; }
            }
            ManagedMiMalloc.mi_collect(true);
            NativeMiMalloc.mi_collect(true);
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
        public void NativeMimalloc_AllocFree()
        {
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                byte* p = (byte*)NativeMiMalloc.mi_malloc_aligned((nuint)Size, Alignment);
                p[0] = 1;
                p[Size - 1] = 1;
                NativeMiMalloc.mi_free(p);
            }
        }

        [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
        public void ManagedMimalloc_AllocFree()
        {
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                byte* p = (byte*)ManagedMiMalloc.mi_malloc_aligned((nuint)Size, Alignment);
                p[0] = 1;
                p[Size - 1] = 1;
                ManagedMiMalloc.mi_free(p);
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
        public void NativeMimalloc_Churn()
        {
            var slots = _churnNative;
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                int s = i & (ChurnSlots - 1);
                if (slots[s] != null) { NativeMiMalloc.mi_free(slots[s]); }
                byte* p = (byte*)NativeMiMalloc.mi_malloc_aligned((nuint)_churnSizes[s], Alignment);
                p[0] = (byte)i;
                slots[s] = p;
            }
        }

        [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
        public void ManagedMimalloc_Churn()
        {
            var slots = _churnManaged;
            for (int i = 0; i < OpsPerInvoke; i++)
            {
                int s = i & (ChurnSlots - 1);
                if (slots[s] != null) { ManagedMiMalloc.mi_free(slots[s]); }
                byte* p = (byte*)ManagedMiMalloc.mi_malloc_aligned((nuint)_churnSizes[s], Alignment);
                p[0] = (byte)i;
                slots[s] = p;
            }
        }

        // ---------------- realloc grow chain ----------------

        [Benchmark(OperationsPerInvoke = 100)]
        public void NativeMimalloc_ReallocGrow()
        {
            for (int i = 0; i < 100; i++)
            {
                void* p = NativeMiMalloc.mi_malloc_aligned(64, Alignment);
                for (nuint sz = 128; sz <= (nuint)Size; sz *= 2)
                {
                    p = NativeMiMalloc.mi_realloc_aligned(p, sz, Alignment);
                }
                NativeMiMalloc.mi_free(p);
            }
        }

        [Benchmark(OperationsPerInvoke = 100)]
        public void ManagedMimalloc_ReallocGrow()
        {
            for (int i = 0; i < 100; i++)
            {
                void* p = ManagedMiMalloc.mi_malloc_aligned(64, Alignment);
                for (nuint sz = 128; sz <= (nuint)Size; sz *= 2)
                {
                    p = ManagedMiMalloc.mi_realloc_aligned(p, sz, Alignment);
                }
                ManagedMiMalloc.mi_free(p);
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
            void* q = FlowtideDotNet.Storage.Mimalloc.MiMalloc.mi_malloc((nuint)Size);
            FlowtideDotNet.Storage.Mimalloc.MiMalloc.mi_free(q);
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
        public void NativeMimalloc_Mt()
        {
            int size = Size;
            RunOnThreads(() =>
            {
                for (int i = 0; i < OpsPerThread; i++)
                {
                    byte* p = (byte*)FlowtideDotNet.Storage.Mimalloc.MiMalloc.mi_malloc_aligned((nuint)size, 64);
                    p[0] = 1;
                    FlowtideDotNet.Storage.Mimalloc.MiMalloc.mi_free(p);
                }
                FlowtideDotNet.Storage.Mimalloc.MiMalloc.mi_thread_done();
            });
        }

        [Benchmark]
        public void ManagedMimalloc_Mt()
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
