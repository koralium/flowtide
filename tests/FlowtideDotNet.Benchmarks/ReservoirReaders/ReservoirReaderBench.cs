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

using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Benchmarks.ReservoirReaders
{
    /// <summary>
    /// Many readers reading stored pages, run with --reservoir-readers.
    /// </summary>
    /// <remarks>
    /// Options: --provider disk|memory --mode session|shared --readers --pages --page-bytes --reads --warm-up --state-dir
    /// </remarks>
    internal static class ReservoirReaderBench
    {
        private const long FirstPage = 1000;

        private sealed class Page : ICacheObject
        {
            private int _rentCount = 1;

            public Page(byte[] bytes)
            {
                Bytes = bytes;
            }

            public byte[] Bytes { get; }

            public bool RemovedFromCache { get; set; }

            public int RentCount => Volatile.Read(ref _rentCount);

            public bool TryRent()
            {
                Interlocked.Increment(ref _rentCount);
                return true;
            }

            public void Return()
            {
                Interlocked.Decrement(ref _rentCount);
            }

            public bool TryReclaimForEviction()
            {
                return Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1;
            }

            public void EnterWriteLock()
            {
                Monitor.Enter(this);
            }

            public void ExitWriteLock()
            {
                Monitor.Exit(this);
            }
        }

        private sealed class PageSerializer : IStateSerializer<Page>
        {
            public Page Deserialize(ReadOnlySequence<byte> bytes, int length)
            {
                return new Page(bytes.ToArray());
            }

            public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
            {
                return Deserialize(bytes, length);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in Page value)
            {
                bufferWriter.Write(value.Bytes);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
            {
                Serialize(bufferWriter, (Page)value);
            }

            public void ClearTemporaryAllocations()
            {
            }

            public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
            {
                return Task.CompletedTask;
            }

            public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
            {
                return Task.CompletedTask;
            }

            public void Dispose()
            {
            }
        }

        private static string Option(string[] args, string name, string fallback)
        {
            int index = Array.IndexOf(args, "--" + name);
            return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback;
        }

        // Wordlike text, compresses about as state pages do
        private static byte[] PageBytes(long page, int size)
        {
            var random = new Random((int)page);
            var bytes = new byte[size];
            int position = 0;
            while (position < size)
            {
                int word = random.Next(400);
                int length = 3 + word % 9;
                for (int i = 0; i < length && position < size; i++)
                {
                    bytes[position++] = (byte)('a' + (word * 7 + i * 3) % 26);
                }
                if (position < size)
                {
                    bytes[position++] = (byte)' ';
                }
            }
            return bytes;
        }

        private static CompressedStateSerializer<Page> NewSerializer()
        {
            return new CompressedStateSerializer<Page>(new PageSerializer(), 3, GlobalMemoryManager.Instance);
        }

        public static async Task<int> Run(string[] args)
        {
            string providerName = Option(args, "provider", "disk");
            string mode = Option(args, "mode", "session");
            int readers = int.Parse(Option(args, "readers", "10"), CultureInfo.InvariantCulture);
            int pages = int.Parse(Option(args, "pages", "4000"), CultureInfo.InvariantCulture);
            int pageBytes = int.Parse(Option(args, "page-bytes", "32768"), CultureInfo.InvariantCulture);
            int reads = int.Parse(Option(args, "reads", "2000"), CultureInfo.InvariantCulture);
            int warmUp = int.Parse(Option(args, "warm-up", "300"), CultureInfo.InvariantCulture);
            string directory = Path.Join(Option(args, "state-dir", Path.GetTempPath()), $"reservoir-readers-{Guid.NewGuid():N}");
            try
            {
                IReservoirStorageProvider provider = providerName == "memory" ? new MemoryFileProvider() : new LocalDiskProvider(directory);
                using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider });
                await storage.InitializeAsync(new StorageInitializationMetadata("readers", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
                using (var writer = storage.CreateSession())
                {
                    using var serializer = NewSerializer();
                    for (int i = 0; i < pages; i++)
                    {
                        await writer.Write(FirstPage + i, new SerializableObject(new Page(PageBytes(FirstPage + i, pageBytes)), serializer));
                    }
                    await writer.Commit();
                    await storage.CheckpointAsync(new byte[] { 1 }, false);
                }

                long wrong = 0;
                using var start = new ManualResetEventSlim(false);
                using var warm = new CountdownEvent(readers);
                var threads = new List<Thread>();
                var sessions = new List<IPersistentStorageSession>();
                for (int r = 0; r < readers; r++)
                {
                    int seed = r;
                    var session = storage.CreateSession();
                    sessions.Add(session);
                    var thread = new Thread(() =>
                    {
                        using var serializer = NewSerializer();
                        var random = new Random(seed);
                        // The first reads warm up and are not timed
                        for (int i = 0; i < warmUp + reads; i++)
                        {
                            if (i == warmUp)
                            {
                                warm.Signal();
                                start.Wait();
                            }
                            long page = FirstPage + random.Next(pages);
                            var read = mode == "shared"
                                ? storage.ReadConcurrentAsync(page, serializer)
                                : session.Read(page, serializer);
                            var result = read.IsCompletedSuccessfully ? read.Result : read.AsTask().GetAwaiter().GetResult();
                            // A cheap check, the read is what is timed
                            if (result.Bytes.Length != pageBytes || result.Bytes[0] != PageBytes(page, 1)[0])
                            {
                                Interlocked.Increment(ref wrong);
                            }
                        }
                    });
                    thread.Start();
                    threads.Add(thread);
                }
                warm.Wait();
                var stopwatch = Stopwatch.StartNew();
                start.Set();
                for (int i = 0; i < threads.Count; i++)
                {
                    threads[i].Join();
                }
                stopwatch.Stop();
                for (int i = 0; i < sessions.Count; i++)
                {
                    sessions[i].Dispose();
                }
                double seconds = stopwatch.Elapsed.TotalSeconds;
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Provider = providerName,
                    Directory = providerName == "memory" ? string.Empty : directory,
                    Mode = mode,
                    Readers = readers,
                    Pages = pages,
                    PageBytes = pageBytes,
                    Reads = (long)readers * reads,
                    Wrong = wrong,
                    Seconds = seconds,
                    PagesPerSecond = (double)readers * reads / seconds
                }));
                return wrong == 0 ? 0 : 1;
            }
            finally
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, true);
                }
            }
        }
    }
}
