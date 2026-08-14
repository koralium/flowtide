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

// Tests for Bitmap.cs (the port of mimalloc's concurrent bitmaps): struct layout,
// single-threaded semantics of every operation family, and multi-threaded stress
// (concurrent claim/release with a shadow array to catch double-claims or lost bits).

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public unsafe class BitmapTests : IDisposable
    {
        // (ptr, isAligned) -- AlignedAlloc'd memory must be freed with AlignedFree
        private readonly List<(IntPtr, bool)> _kinds = new();

        public void Dispose()
        {
            foreach (var (ptr, aligned) in _kinds)
            {
                if (aligned) NativeMemory.AlignedFree((void*)ptr);
                else NativeMemory.Free((void*)ptr);
            }
            _kinds.Clear();
        }

        // -- layout ---------------------------------------------------------------

        [Fact]
        public void Layout_MatchesCOffsets()
        {
            Assert.Equal(64, sizeof(mi_bchunk_t));                     // MI_BCHUNK_SIZE
            Assert.Equal(8, MI_BCHUNK_FIELDS);
            Assert.Equal(512, MI_BCHUNK_BITS);

            mi_bitmap_t bm = default;
            byte* basePtr = (byte*)&bm;
            byte* chunksPtr = (byte*)Unsafe.AsPointer(ref bm.chunks);
            Assert.Equal(MI_BITMAP_CHUNKS_OFFSET, (int)(chunksPtr - basePtr));
            byte* chunkmapPtr = (byte*)Unsafe.AsPointer(ref bm.chunkmap);
            Assert.Equal(MI_BCHUNK_SIZE, (int)(chunkmapPtr - basePtr));
            Assert.Equal(MI_BITMAP_CHUNKS_OFFSET + 64 * MI_BCHUNK_SIZE, sizeof(mi_bitmap_t));

            mi_bbitmap_t bbm = default;
            byte* bbase = (byte*)&bbm;
            byte* bchunks = (byte*)Unsafe.AsPointer(ref bbm.chunks);
            Assert.Equal(MI_BBITMAP_CHUNKS_OFFSET, (int)(bchunks - bbase));
            byte* bchunkmap = (byte*)Unsafe.AsPointer(ref bbm.chunkmap);
            Assert.Equal(MI_BCHUNK_SIZE, (int)(bchunkmap - bbase));
        }

        [Fact]
        public void Size_MatchesChunkCount()
        {
            nuint chunkCount;
            Assert.Equal((nuint)(MI_BITMAP_CHUNKS_OFFSET + MI_BCHUNK_SIZE), mi_bitmap_size(512, &chunkCount));
            Assert.Equal((nuint)1, chunkCount);
            mi_bitmap_size(512 * 512, &chunkCount);
            Assert.Equal((nuint)512, chunkCount);
            mi_bbitmap_size(1024, &chunkCount);
            Assert.Equal((nuint)2, chunkCount);
        }

        // -- single-threaded semantics -------------------------------------------

        [Fact]
        public void SetClear_SingleBits()
        {
            var bitmap = AllocBitmapTracked(2 * 512);
            Assert.True(mi_bitmap_is_all_clear(bitmap));

            Assert.True(mi_bitmap_set(bitmap, 0));      // 0 -> 1 transition
            Assert.False(mi_bitmap_set(bitmap, 0));     // already set
            Assert.True(mi_bitmap_is_set(bitmap, 0));
            Assert.False(mi_bitmap_is_clear(bitmap, 0));

            Assert.True(mi_bitmap_set(bitmap, 700));    // second chunk
            Assert.True(mi_bitmap_is_set(bitmap, 700));

            Assert.True(mi_bitmap_clear(bitmap, 0));    // 1 -> 0
            Assert.False(mi_bitmap_clear(bitmap, 0));   // already clear
            Assert.True(mi_bitmap_clear(bitmap, 700));
            Assert.True(mi_bitmap_is_all_clear(bitmap));
        }

        [Theory]
        [InlineData(0u, 1u)]
        [InlineData(5u, 3u)]        // within one bfield
        [InlineData(60u, 8u)]       // crossing a bfield boundary
        [InlineData(0u, 64u)]       // exactly one bfield
        [InlineData(32u, 64u)]      // bfield-sized crossing
        [InlineData(100u, 300u)]    // many bfields within a chunk
        [InlineData(500u, 24u)]     // crossing a chunk boundary
        [InlineData(0u, 1024u)]     // two full chunks
        [InlineData(300u, 700u)]    // partial + full + partial chunks
        public void SetN_ClearN_Roundtrip(uint idxInt, uint nInt)
        {
            nuint idx = idxInt, n = nInt;
            var bitmap = AllocBitmapTracked(4 * 512);

            nuint alreadySet = 999;
            Assert.True(mi_bitmap_setN(bitmap, idx, n, &alreadySet));  // all transitioned
            Assert.Equal((nuint)0, alreadySet);
            Assert.True(mi_bitmap_is_setN(bitmap, idx, n));
            Assert.Equal(n, mi_bitmap_popcount(bitmap));
            Assert.Equal(n, mi_bitmap_popcountN(bitmap, idx, n));

            // neighbors untouched
            if (idx > 0) Assert.True(mi_bitmap_is_clear(bitmap, idx - 1));
            if (idx + n < mi_bitmap_max_bits(bitmap)) Assert.True(mi_bitmap_is_clear(bitmap, idx + n));

            // setting again reports already-set count and no transition
            Assert.False(mi_bitmap_setN(bitmap, idx, n, &alreadySet));
            Assert.Equal(n, alreadySet);

            Assert.True(mi_bitmap_clearN(bitmap, idx, n));   // all transitioned back
            Assert.False(mi_bitmap_clearN(bitmap, idx, n));  // already clear
            Assert.True(mi_bitmap_is_all_clear(bitmap));
        }

        [Fact]
        public void UnsafeSetN_LargeRange()
        {
            var bitmap = AllocBitmapTracked(8 * 512);
            mi_bitmap_unsafe_setN(bitmap, 10, 3000);
            Assert.Equal((nuint)3000, mi_bitmap_popcount(bitmap));
            Assert.True(mi_bitmap_is_setN(bitmap, 10, 3000));
            Assert.True(mi_bitmap_is_clearN(bitmap, 0, 10));
            Assert.True(mi_bitmap_is_clear(bitmap, 3010));
        }

        [Fact]
        public void Bsr_FindsHighestSetBit()
        {
            var bitmap = AllocBitmapTracked(4 * 512);
            nuint idx;
            Assert.False(mi_bitmap_bsr(bitmap, &idx));
            mi_bitmap_set(bitmap, 5);
            mi_bitmap_set(bitmap, 1000);
            mi_bitmap_set(bitmap, 1500);
            Assert.True(mi_bitmap_bsr(bitmap, &idx));
            Assert.Equal((nuint)1500, idx);
            mi_bitmap_clear(bitmap, 1500);
            Assert.True(mi_bitmap_bsr(bitmap, &idx));
            Assert.Equal((nuint)1000, idx);
        }

        [Fact]
        public void ClearOnceSet_WaitsForSet()
        {
            var bitmap = AllocBitmapTracked(512);
            var subproc = AllocSubprocTracked();
            mi_bitmap_set(bitmap, 42);
            // already set: clears immediately
            mi_bitmap_clear_once_set(subproc, bitmap, 42);
            Assert.True(mi_bitmap_is_clear(bitmap, 42));

            // not set: busy-waits until another thread sets it
            mi_bitmap_t* bm = bitmap;
            mi_subproc_t* sp = subproc;
            var setter = Task.Run(() =>
            {
                Thread.Sleep(50);
                mi_bitmap_set(bm, 99);
            });
            mi_bitmap_clear_once_set(sp, bm, 99);   // blocks until set, then clears
            Assert.True(mi_bitmap_is_clear(bm, 99));
            setter.Wait();
        }

        [Fact]
        public void ForallSet_VisitsEverySetBit()
        {
            var bitmap = AllocBitmapTracked(4 * 512);
            var expected = new nuint[] { 0, 3, 63, 64, 511, 512, 1000, 2047 };
            foreach (var i in expected) mi_bitmap_set(bitmap, i);

            var visited = new List<nuint>();
            var handle = GCHandle.Alloc(visited);
            try
            {
                _mi_bitmap_forall_set(bitmap, &VisitCollect, null, (void*)GCHandle.ToIntPtr(handle));
            }
            finally
            {
                handle.Free();
            }
            Assert.Equal(expected.OrderBy(x => x), visited.OrderBy(x => x));
        }

        private static bool VisitCollect(nuint idx, nuint count, mi_arena_t* arena, void* arg)
        {
            var list = (List<nuint>)GCHandle.FromIntPtr((IntPtr)arg).Target!;
            for (nuint i = 0; i < count; i++) { list.Add(idx + i); }
            return true;
        }

        [Fact]
        public void ForallSetcRanges_VisitsAndClearsRanges()
        {
            var bitmap = AllocBitmapTracked(2 * 512);
            // a run of 10, a run of 64 crossing nothing, an isolated bit
            mi_bitmap_setN(bitmap, 5, 10, null);
            mi_bitmap_setN(bitmap, 128, 64, null);
            mi_bitmap_set(bitmap, 600);

            var visited = new List<(nuint, nuint)>();
            var handle = GCHandle.Alloc(visited);
            try
            {
                Assert.True(_mi_bitmap_forall_setc_ranges(bitmap, &VisitRangeCollect, null, (void*)GCHandle.ToIntPtr(handle)));
            }
            finally
            {
                handle.Free();
            }
            // all bits are cleared afterwards (ranges are atomically claimed)
            Assert.Equal((nuint)0, mi_bitmap_popcount(bitmap));
            // total visited count matches
            nuint total = 0;
            foreach (var (_, cnt) in visited) total += cnt;
            Assert.Equal((nuint)(10 + 64 + 1), total);
            // ranges never cross bfield boundaries
            foreach (var (idx, cnt) in visited)
            {
                Assert.True((idx % 64) + cnt <= 64);
            }
        }

        private static bool VisitRangeCollect(nuint idx, nuint count, mi_arena_t* arena, void* arg)
        {
            var list = (List<(nuint, nuint)>)GCHandle.FromIntPtr((IntPtr)arg).Target!;
            list.Add((idx, count));
            return true;
        }

        // -- bbitmap: find-and-clear ----------------------------------------------

        [Theory]
        [InlineData(1u)]
        [InlineData(8u)]
        [InlineData(3u)]      // NX path
        [InlineData(64u)]     // NX path (== MI_BFIELD_BITS)
        [InlineData(100u)]    // NC path
        [InlineData(512u)]    // NC path (== MI_BCHUNK_BITS)
        [InlineData(1000u)]   // N_ path (spans chunks)
        public void BBitmap_TryFindAndClearN_ClaimsExactRange(uint nInt)
        {
            nuint n = nInt;
            var subproc = AllocSubprocTracked();
            var bbitmap = AllocBBitmapTracked(4 * 512, subproc);
            mi_bbitmap_unsafe_setN(bbitmap, 0, 4 * 512);   // all free

            nuint idx;
            Assert.True(mi_bbitmap_try_find_and_clearN(bbitmap, 0, n, &idx));
            Assert.True(idx + n <= mi_bbitmap_max_bits(bbitmap));
            // claimed range is now clear, everything else still set
            Assert.True(mi_bbitmap_is_clearN(bbitmap, idx, n));
            nuint remaining = 4 * 512 - n;
            nuint popcount = 0;
            for (nuint i = 0; i < mi_bbitmap_max_bits(bbitmap); i++)
            {
                if (mi_bbitmap_is_setN(bbitmap, i, 1)) popcount++;
            }
            Assert.Equal(remaining, popcount);

            // release it back
            Assert.True(mi_bbitmap_setN(bbitmap, idx, n));
        }

        [Fact]
        public void BBitmap_FindAndClear_ExhaustsExactly()
        {
            var subproc = AllocSubprocTracked();
            var bbitmap = AllocBBitmapTracked(512, subproc);
            mi_bbitmap_unsafe_setN(bbitmap, 0, 512);

            // claim all 512 bits one at a time; each claim must be unique
            var seen = new HashSet<nuint>();
            nuint idx;
            for (int i = 0; i < 512; i++)
            {
                Assert.True(mi_bbitmap_try_find_and_clear(bbitmap, (nuint)i, &idx));
                Assert.True(seen.Add(idx), $"bit {idx} claimed twice");
            }
            // exhausted now
            Assert.False(mi_bbitmap_try_find_and_clear(bbitmap, 0, &idx));
        }

        [Fact]
        public void BBitmap_TryClearNC_FailsOnPartialRange()
        {
            var subproc = AllocSubprocTracked();
            var bbitmap = AllocBBitmapTracked(512, subproc);
            mi_bbitmap_unsafe_setN(bbitmap, 0, 100);

            // range [50, 150) is only partially set: try_clear must fail and leave all as-is
            Assert.False(mi_bbitmap_try_clearNC(bbitmap, 50, 100));
            Assert.True(mi_bbitmap_is_setN(bbitmap, 0, 100));
            // fully set range succeeds
            Assert.True(mi_bbitmap_try_clearNC(bbitmap, 0, 100));
            Assert.True(mi_bbitmap_is_clearN(bbitmap, 0, 100));
        }

        [Fact]
        public void BBitmap_BsrInv_FindsHighestClearBit()
        {
            var subproc = AllocSubprocTracked();
            var bbitmap = AllocBBitmapTracked(512, subproc);
            mi_bbitmap_unsafe_setN(bbitmap, 0, 512);
            nuint idx;
            Assert.False(mi_bbitmap_bsr_inv(bbitmap, &idx));   // no clear bit
            mi_bbitmap_try_clearNC(bbitmap, 100, 1);
            mi_bbitmap_try_clearNC(bbitmap, 400, 1);
            Assert.True(mi_bbitmap_bsr_inv(bbitmap, &idx));
            Assert.Equal((nuint)400, idx);
        }

        // -- multi-threaded stress -----------------------------------------------

        [Fact]
        public void Bitmap_ConcurrentSetClear_NoLostUpdates()
        {
            // each thread owns a distinct bit and toggles it; other bits must be unaffected
            var bitmap = AllocBitmapTracked(2 * 512);
            mi_bitmap_t* bm = bitmap;
            const int threads = 8;
            const int iterations = 50_000;

            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                nuint bit = (nuint)(t * 97 + 13);   // scattered over both chunks
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        Assert.True(mi_bitmap_set(bm, bit));
                        Assert.True(mi_bitmap_is_set(bm, bit));
                        Assert.True(mi_bitmap_clear(bm, bit));
                    }
                });
            }
            Task.WaitAll(tasks);
            Assert.True(mi_bitmap_is_all_clear(bitmap));
        }

        [Fact]
        public void BBitmap_ConcurrentClaimRelease_NoDoubleClaims()
        {
            // models arena slice allocation: threads concurrently claim ranges (find+clear)
            // and release them (setN). A shadow array catches double-claims.
            var subproc = AllocSubprocTracked();
            var bbitmap = AllocBBitmapTracked(4 * 512, subproc);
            nuint totalBits = 4 * 512;
            mi_bbitmap_unsafe_setN(bbitmap, 0, totalBits);
            mi_bbitmap_t* bb = bbitmap;

            int[] shadow = new int[(int)totalBits];   // 0 = free, 1 = claimed
            const int threads = 8;
            const int iterations = 20_000;
            uint[] sizes = { 1, 8, 3, 64, 100, 5, 8, 1 };

            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                nuint n = sizes[t];
                nuint tseq = (nuint)t;
                tasks[t] = Task.Run(() =>
                {
                    var rnd = new Random((int)n * 7919 + (int)tseq);
                    for (int i = 0; i < iterations; i++)
                    {
                        nuint idx;
                        if (mi_bbitmap_try_find_and_clearN(bb, tseq, n, &idx))
                        {
                            // claim the shadow: every bit must have been free
                            for (nuint b = 0; b < n; b++)
                            {
                                int prev = Interlocked.Exchange(ref shadow[(int)(idx + b)], 1);
                                Assert.Equal(0, prev);   // double-claim detection!
                            }
                            // hold it briefly sometimes to increase overlap
                            if (rnd.Next(16) == 0) Thread.Yield();
                            // release the shadow BEFORE the bitmap release (mirror of claim order)
                            for (nuint b = 0; b < n; b++)
                            {
                                Interlocked.Exchange(ref shadow[(int)(idx + b)], 0);
                            }
                            bool wereAllClear = mi_bbitmap_setN(bb, idx, n);
                            Assert.True(wereAllClear);   // we owned them exclusively
                        }
                    }
                });
            }
            Task.WaitAll(tasks);

            // conservation: everything was released, so all bits are set again
            Assert.True(mi_bbitmap_is_setN(bbitmap, 0, totalBits));
            foreach (var s in shadow) Assert.Equal(0, s);
        }

        [Fact]
        public void Bitmap_ConcurrentSetN_ClearN_RangesRemainConsistent()
        {
            // threads work on disjoint 64-bit lanes but stress the shared chunkmap logic
            var bitmap = AllocBitmapTracked(8 * 512);
            mi_bitmap_t* bm = bitmap;
            const int threads = 8;
            const int iterations = 25_000;

            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                nuint baseIdx = (nuint)t * 512;   // one chunk per thread
                tasks[t] = Task.Run(() =>
                {
                    var rnd = new Random((int)baseIdx + 1);
                    for (int i = 0; i < iterations; i++)
                    {
                        nuint off = (nuint)rnd.Next(0, 512 - 65);
                        nuint n = (nuint)rnd.Next(1, 65);
                        Assert.True(mi_bitmap_setN(bm, baseIdx + off, n, null));
                        Assert.True(mi_bitmap_is_setN(bm, baseIdx + off, n));
                        Assert.True(mi_bitmap_clearN(bm, baseIdx + off, n));
                    }
                });
            }
            Task.WaitAll(tasks);
            Assert.True(mi_bitmap_is_all_clear(bitmap));
        }

        // -- tracked allocation helpers ------------------------------------------

        private mi_bitmap_t* AllocBitmapTracked(nuint bitCount)
        {
            nuint size = mi_bitmap_size(bitCount, null);
            void* p = NativeMemory.AlignedAlloc(size, 64);
            _kinds.Add(((IntPtr)p, true));
            mi_bitmap_t* bitmap = (mi_bitmap_t*)p;
            mi_bitmap_init(bitmap, bitCount, false);
            return bitmap;
        }

        private mi_bbitmap_t* AllocBBitmapTracked(nuint bitCount, mi_subproc_t* subproc)
        {
            nuint size = mi_bbitmap_size(bitCount, null);
            void* p = NativeMemory.AlignedAlloc(size, 64);
            _kinds.Add(((IntPtr)p, true));
            mi_bbitmap_t* bbitmap = (mi_bbitmap_t*)p;
            mi_bbitmap_init(subproc, bbitmap, bitCount, false);
            return bbitmap;
        }

        private mi_subproc_t* AllocSubprocTracked()
        {
            var p = (mi_subproc_t*)NativeMemory.AllocZeroed((nuint)sizeof(mi_subproc_t));
            _kinds.Add(((IntPtr)p, false));
            return p;
        }
    }
}
