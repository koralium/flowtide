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
using FlowtideDotNet.Core.ColumnStore.Utils;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Benchmarks.ColumnStore.Utils
{
    public unsafe class MemCopyWithAddBenchmark
    {
        private int[]? source;
        private int[]? destination;
        private void* alignedMemory;
        private void* alignedMemoryDest;
        [GlobalSetup]
        public void GlobalSetup()
        {
            int elementCount = 1_000_000;
            source = new int[elementCount];
            destination = new int[elementCount];

            alignedMemory = (void*)NativeMemory.AlignedAlloc((nuint)(sizeof(int) * elementCount + 64), 64);
            alignedMemoryDest = (void*)NativeMemory.AlignedAlloc((nuint)(sizeof(int) * elementCount + 64), 64);

            var alignedSpan = new Span<int>(alignedMemory, elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                source[i] = i;
                alignedSpan[i] = i;
            }
        }

        [Benchmark]
        public void MemCopyWithAdd_AignedMemory()
        {
            var sourceSpan = new Span<int>(alignedMemory, 1_000_000);
            var destSpan = new Span<int>(alignedMemoryDest, 1_000_000);
            AvxUtils.MemCpyWithAdd(sourceSpan, destSpan, 5);
        }

        [Benchmark]
        public void CopyByIteration()
        {
            var sourceSpan = source.AsSpan();
            var destSpan = destination.AsSpan();

            for (int i = 0; i < sourceSpan.Length; i++)
            {
                destSpan[i] = sourceSpan[i] + 5;
            }
        }

        [Benchmark]
        public void MemCpy()
        {
            var sourceSpan = source.AsSpan();
            var destSpan = destination.AsSpan();
            sourceSpan.CopyTo(destSpan);
        }

        [Benchmark]
        public void MemCopyWithAdd()
        {
            var sourceSpan = source.AsSpan();
            var destSpan = destination.AsSpan();
            AvxUtils.MemCpyWithAdd(sourceSpan, destSpan, 5);
        }
    }
}
