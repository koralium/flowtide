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

using FlowtideDotNet.Core.Operators.LateMaterialization;
using System.Diagnostics;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    [CollectionDefinition(nameof(RowHeapDirectoryReadCollection), DisableParallelization = true)]
    public class RowHeapDirectoryReadCollection
    {
    }

    [Collection(nameof(RowHeapDirectoryReadCollection))]
    public class RowHeapDirectoryReadTests
    {
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        [Fact]
        public void TheDirectoryIsReadWhileItGrows()
        {
            var directory = new RowHeapDirectory();
            const long last = 300_000;
            long published = 0;
            long wrong = 0;
            bool stop = false;
            var checks = new long[3];
            var readers = new Thread[checks.Length];
            using var spinning = new CountdownEvent(readers.Length);
            for (int r = 0; r < readers.Length; r++)
            {
                int reader = r;
                readers[r] = new Thread(() =>
                {
                    long stride = 7 + reader * 4;
                    long sequence = 1;
                    spinning.Signal();
                    while (!Volatile.Read(ref stop))
                    {
                        long upTo = Volatile.Read(ref published);
                        if (upTo == 0)
                        {
                            continue;
                        }
                        sequence = (sequence + stride) % upTo + 1;
                        if (directory.Get(sequence) != sequence + 1000 || directory.Get(upTo) != upTo + 1000)
                        {
                            Interlocked.Increment(ref wrong);
                        }
                        if (upTo < last)
                        {
                            Interlocked.Increment(ref checks[reader]);
                        }
                    }
                });
                readers[r].IsBackground = true;
                readers[r].Start();
            }

            int newChunks = 0;
            try
            {
                Assert.True(spinning.Wait(Limit));
                for (long sequence = 1; sequence <= last; sequence++)
                {
                    directory.Set(sequence, sequence + 1000);
                    Volatile.Write(ref published, sequence);
                    if ((sequence & (RowHeapDirectory.ChunkSize - 1)) == 0)
                    {
                        WaitForEveryReader(checks);
                        newChunks++;
                    }
                }
            }
            finally
            {
                Volatile.Write(ref stop, true);
            }
            foreach (var reader in readers)
            {
                Assert.True(reader.Join(Limit));
            }

            Assert.Equal(0, Interlocked.Read(ref wrong));
            Assert.Equal(last >> RowHeapDirectory.ChunkBits, newChunks);
            foreach (long made in checks)
            {
                Assert.True(made >= 2 * newChunks, $"A reader made {made} checks while the directory grew.");
            }
        }

        [Fact]
        public void TheDirectoryIsReadWhileItsChunksAreMadeShortAndFreed()
        {
            var directory = new RowHeapDirectory();
            const int chunks = 40;
            const int chunk = RowHeapDirectory.ChunkSize;
            const long last = (long)chunks * chunk;
            const long emptied = 20L * chunk - 1;
            for (long sequence = 1; sequence <= last; sequence++)
            {
                directory.Set(sequence, sequence + 1000);
            }
            long full = directory.ByteSize;
            long cleared = 0;
            long clearedWhole = 0;
            long wrong = 0;
            bool lastCleared = false;
            bool stop = false;
            var checks = new long[3];
            var readers = new Thread[checks.Length];
            using var spinning = new CountdownEvent(readers.Length);
            for (int r = 0; r < readers.Length; r++)
            {
                int reader = r;
                readers[r] = new Thread(() =>
                {
                    long stride = 7 + reader * 4;
                    long sequence = 1;
                    spinning.Signal();
                    while (!Volatile.Read(ref stop))
                    {
                        sequence = (sequence + stride) % last + 1;
                        long value = directory.Get(sequence);
                        if (value != sequence + 1000)
                        {
                            bool mayBeGone = sequence <= Volatile.Read(ref clearedWhole) || (sequence % 500 != 0 && sequence <= Volatile.Read(ref cleared)) || (sequence == last && Volatile.Read(ref lastCleared));
                            if (value != 0 || !mayBeGone)
                            {
                                Interlocked.Increment(ref wrong);
                            }
                        }
                        Interlocked.Increment(ref checks[reader]);
                    }
                });
                readers[r].IsBackground = true;
                readers[r].Start();
            }

            int shortened = 0;
            int freed = 0;
            long shortBytes;
            try
            {
                Assert.True(spinning.Wait(Limit));
                for (long sequence = 1; sequence <= last - chunk; sequence++)
                {
                    Volatile.Write(ref cleared, sequence);
                    if (sequence % 500 != 0)
                    {
                        directory.Set(sequence, 0);
                    }
                    if ((sequence & (chunk - 1)) == 0)
                    {
                        WaitForEveryReader(checks);
                        shortened++;
                    }
                }
                shortBytes = directory.ByteSize;
                for (long sequence = 500; sequence <= emptied; sequence += 500)
                {
                    Volatile.Write(ref clearedWhole, sequence);
                    directory.Set(sequence, 0);
                    if (sequence / chunk != (sequence + 500) / chunk)
                    {
                        WaitForEveryReader(checks);
                        freed++;
                    }
                }
                Volatile.Write(ref clearedWhole, emptied);
                Volatile.Write(ref lastCleared, true);
                directory.Set(last, 0);
                WaitForEveryReader(checks);
            }
            finally
            {
                Volatile.Write(ref stop, true);
            }
            foreach (var reader in readers)
            {
                Assert.True(reader.Join(Limit));
            }
            long roomInShortChunks = 0;
            long roomInTheChunksLeft = 0;
            for (long first = 0; first < last - chunk; first += chunk)
            {
                long kept = (first + chunk - 1) / 500 - Math.Max(0, first - 1) / 500;
                long room = (long)System.Numerics.BitOperations.RoundUpToPowerOf2((ulong)kept);
                roomInShortChunks += room;
                roomInTheChunksLeft += first >= 20L * chunk ? room : 0;
            }

            Assert.Equal(0, Interlocked.Read(ref wrong));
            Assert.Equal(chunks - 1, shortened);
            Assert.Equal(20, freed);
            Assert.Equal(64 * 12 + (chunks + 1L) * chunk * 8, full);
            Assert.Equal(64 * 12 + 2 * chunk * 8 + roomInShortChunks * 10 + (chunks - 1) * 64, shortBytes);
            Assert.Equal(64 * 12 + chunk * 8 + roomInTheChunksLeft * 10 + (chunks - 1 - 20) * 64, directory.ByteSize);
            for (long sequence = 500; sequence < last; sequence += 500)
            {
                Assert.Equal(sequence <= emptied ? 0 : sequence + 1000, directory.Get(sequence));
            }
            Assert.Equal(0, directory.Get(last));
        }

        [Fact]
        public async Task RowsAreReadWhileTheirPagesAreMergedAndDeleted()
        {
            var kinds = new string[] { "int", "string" };
            var both = new int[] { 0, 1 };
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, kinds, 0, 3000);
            var kept = Enumerable.Range(0, 3000).Where(x => x % 10 == 0).ToArray();
            var keptReferences = kept.Select(x => (ulong?)references[x]).ToArray();
            var keptRows = kept.Select(x => (long?)x).ToArray();
            var others = new Queue<ulong>(Enumerable.Range(0, 3000).Where(x => x % 10 != 0).Select(x => references[x]));
            using var barrier = new SemaphoreSlim(1, 1);
            using var reader = heap.CreateReader();
            bool stop = false;
            long reads = 0;
            Exception? failure = null;
            var thread = new Thread(() =>
            {
                try
                {
                    while (!Volatile.Read(ref stop))
                    {
                        barrier.Wait();
                        try
                        {
                            var fetched = Gather(reader, keptReferences, both).GetAwaiter().GetResult();
                            AssertRows(kinds, both, keptRows, fetched);
                        }
                        finally
                        {
                            barrier.Release();
                        }
                        Interlocked.Increment(ref reads);
                    }
                }
                catch (Exception e)
                {
                    Volatile.Write(ref failure, e);
                }
            });
            thread.IsBackground = true;
            thread.Start();

            long nextRow = 3000;
            long readsDuringMerges = 0;
            try
            {
                for (int round = 0; round < 30 && Volatile.Read(ref failure) == null; round++)
                {
                    long before = Interlocked.Read(ref reads);
                    var appended = await Append(heap, kinds, nextRow, 300);
                    nextRow += 300;
                    for (int i = 0; i < 300; i++)
                    {
                        heap.Release(others.Dequeue());
                        others.Enqueue(appended[i]);
                    }
                    await heap.CompactAsync();
                    var watch = Stopwatch.StartNew();
                    while (Interlocked.Read(ref reads) < before + 2 && Volatile.Read(ref failure) == null && watch.Elapsed < Limit)
                    {
                        Thread.Yield();
                    }
                    readsDuringMerges += Interlocked.Read(ref reads) - before;
                    await barrier.WaitAsync();
                    try
                    {
                        await heap.CheckpointAsync();
                        await stream.Manager.CheckpointAsync();
                    }
                    finally
                    {
                        barrier.Release();
                    }
                }
            }
            finally
            {
                Volatile.Write(ref stop, true);
            }
            Assert.True(thread.Join(Limit));

            Assert.Null(failure);
            Assert.True(readsDuringMerges >= 60, $"The reader read {readsDuringMerges} times.");
            Assert.True(heap.PagesMerged > 20, $"{heap.PagesMerged} pages were merged.");
            Assert.True(heap.PagesDeleted > 100, $"{heap.PagesDeleted} pages were deleted.");
            Assert.Equal(3000, heap.RowCount);
        }

        // Two checks, the first may have begun before the new chunk
        private static void WaitForEveryReader(long[] checks)
        {
            var timer = Stopwatch.StartNew();
            var spin = new SpinWait();
            for (int r = 0; r < checks.Length; r++)
            {
                long seen = Volatile.Read(ref checks[r]);
                while (Volatile.Read(ref checks[r]) < seen + 2)
                {
                    if (timer.Elapsed > Limit)
                    {
                        throw new TimeoutException("A reader made no check while the directory grew.");
                    }
                    spin.SpinOnce();
                }
            }
        }
    }
}
