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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using Xunit;
using Xunit.Abstractions;

namespace FlowtideDotNet.Storage.Tests.Memory
{
    /// <summary>
    /// zstd's free callback receives only a pointer, so the size reported on free has to match the size
    /// reported on allocation or the operator's memory accounting drifts permanently. mimalloc can
    /// recover the size from the pointer; the NativeMemory fallback cannot, which is why the compressor
    /// remembers sizes when the allocator cannot answer for itself. These tests pin the invariant that
    /// matters either way: the accounting returns to where it started.
    /// </summary>
    public unsafe class ZstdCompressorMetricsTests
    {
        private readonly ITestOutputHelper _output;

        public ZstdCompressorMetricsTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void ContextAllocations_AreFullyUnregisteredOnDispose()
        {
            using var stream = new StreamMemoryManager("zstdmetrics-" + Guid.NewGuid().ToString("N"));
            var op = stream.CreateOperatorMemoryManager("compressor");

            long before = stream.GetAllocatedMemory();

            var src = new byte[4096];
            for (int i = 0; i < src.Length; i++) { src[i] = (byte)(i % 251); }
            var dest = new byte[8192];

            long during;
            using (var compressor = new FlowtideZstdCompressor(op, 3))
            {
                // forces CreateContexts, and the first compress lazily allocates the match-finder
                // workspace -- the large block this whole change is about
                int written = compressor.Wrap(src, dest);
                Assert.True(written > 0);

                during = stream.GetAllocatedMemory();
                _output.WriteLine($"before {before:N0}, during {during:N0}");
                Assert.True(during > before, "zstd context memory was never registered");
            }

            long after = stream.GetAllocatedMemory();
            _output.WriteLine($"after {after:N0}");

            // the exact invariant: every byte registered on the way in is unregistered on the way out
            Assert.Equal(before, after);
        }

        [Fact]
        public void ResetContexts_ReturnsAccountingToBaseline()
        {
            using var stream = new StreamMemoryManager("zstdreset-" + Guid.NewGuid().ToString("N"));
            var op = stream.CreateOperatorMemoryManager("compressor");

            var src = new byte[4096];
            var dest = new byte[8192];

            using var compressor = new FlowtideZstdCompressor(op, 3);
            compressor.Wrap(src, dest);
            long afterFirst = stream.GetAllocatedMemory();

            // contexts are recreated lazily, so a reset/use cycle must not accumulate
            for (int i = 0; i < 3; i++)
            {
                compressor.ResetContexts();
                Assert.Equal(0, stream.GetAllocatedMemory());
                compressor.Wrap(src, dest);
                Assert.Equal(afterFirst, stream.GetAllocatedMemory());
            }
        }

        [Fact]
        public void RoundTrip_StillProducesTheOriginalBytes()
        {
            // the accounting changes touch the allocator zstd runs on, so prove it still works
            using var stream = new StreamMemoryManager("zstdroundtrip-" + Guid.NewGuid().ToString("N"));
            var op = stream.CreateOperatorMemoryManager("compressor");

            var src = new byte[16 * 1024];
            new Random(42).NextBytes(src);
            var compressed = new byte[32 * 1024];
            var restored = new byte[16 * 1024];

            using var compressor = new FlowtideZstdCompressor(op, 3);
            int written = compressor.Wrap(src, compressed);
            int read = compressor.Unwrap(compressed.AsSpan(0, written), restored);

            Assert.Equal(src.Length, read);
            Assert.Equal(src, restored);
        }

        [Fact]
        public void GetAllocationSize_AgreesWithWhatWasHandedOut()
        {
            if (!FlowtideMemoryAllocation.CanQueryAllocationSize)
            {
                // the fallback allocator genuinely cannot answer this; that is why the compressor
                // keeps its own size map, and asking anyway is what the review caught
                return;
            }

            var mem = FlowtideMemoryAllocation.AllocateAligned(1000, 16);
            try
            {
                nuint size = FlowtideMemoryAllocation.GetAllocationSize(mem.ptr);
                Assert.True(size >= 1000, $"reported {size} for a 1000 byte request");
                Assert.True((int)size >= mem.length, $"reported {size} but handed out {mem.length}");
            }
            finally
            {
                FlowtideMemoryAllocation.FreeAligned(mem.ptr, 16);
            }
        }
    }
}
