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
