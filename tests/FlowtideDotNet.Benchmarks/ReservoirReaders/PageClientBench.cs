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
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text.Json;
using Page = FlowtideDotNet.Benchmarks.ReservoirReaders.ReservoirReaderBench.Page;

namespace FlowtideDotNet.Benchmarks.ReservoirReaders
{
    /// <summary>
    /// Readers and a publisher on the immutable page client, run with --page-client.
    /// </summary>
    /// <remarks>
    /// Options: --provider disk|memory --readers --pages --page-bytes --reads --warm-up --cache --publisher 0|1 --spinners --state-dir
    /// </remarks>
    internal static class PageClientBench
    {
        // A handler for the baseline, which only fills the cache
        private sealed class DropHandler : ICacheEvictHandler
        {
            public Task<int> Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
            {
                return Task.FromResult(valuesToEvict.Count);
            }
        }

        // Times the shared table's Add for new keys
        private static int RunTableAdd(string[] args)
        {
            int count = int.Parse(ReservoirReaderBench.Option(args, "pages", "1000000"), CultureInfo.InvariantCulture);
            int rounds = int.Parse(ReservoirReaderBench.Option(args, "rounds", "5"), CultureInfo.InvariantCulture);
            var handler = new DropHandler();
            var pages = new Page[count];
            var empty = Array.Empty<byte>();
            double best = double.MaxValue;
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < count; i++)
                {
                    pages[i] = new Page(empty);
                }
                using var meter = new Meter("tableAdd");
                using var table = new S3FifoTableSync(new CacheTableOptions("tableAdd", NullLogger.Instance, meter, GlobalMemoryManager.Instance) { MaxSize = count * 2, MinSize = count * 2 });
                var watch = Stopwatch.StartNew();
                for (int i = 0; i < count; i++)
                {
                    table.Add(i + 1, pages[i], handler);
                }
                best = Math.Min(best, watch.Elapsed.TotalSeconds);
            }
            Console.WriteLine(JsonSerializer.Serialize(new { Adds = count, BestSeconds = best, NanosecondsPerAdd = best * 1e9 / count }));
            return 0;
        }

        // Times each step of one page write
        private static async Task<int> RunWriteCost(string[] args)
        {
            int pages = int.Parse(ReservoirReaderBench.Option(args, "pages", "2000"), CultureInfo.InvariantCulture);
            int pageBytes = int.Parse(ReservoirReaderBench.Option(args, "page-bytes", "32768"), CultureInfo.InvariantCulture);
            var contents = new Page[pages];
            for (int i = 0; i < pages; i++)
            {
                contents[i] = new Page(ReservoirReaderBench.PageBytes(i, pageBytes));
            }
            var plain = new ReservoirReaderBench.PageSerializer();
            var compressed = ReservoirReaderBench.NewSerializer();
            var buffer = new System.Buffers.ArrayBufferWriter<byte>(pageBytes * 2);
            double Time(Action<int> step)
            {
                // Once through to warm up, then timed
                for (int i = 0; i < pages; i++)
                {
                    step(i);
                }
                var watch = Stopwatch.StartNew();
                for (int i = 0; i < pages; i++)
                {
                    step(i);
                }
                return watch.Elapsed.TotalMilliseconds * 1000 / pages;
            }
            double copyOnly = Time(i =>
            {
                buffer.Clear();
                plain.Serialize(buffer, contents[i]);
            });
            long compressedBytes = 0;
            double compress = Time(i =>
            {
                buffer.Clear();
                compressed.Serialize(buffer, contents[i]);
                compressedBytes += buffer.WrittenCount;
            });
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            await storage.InitializeAsync(new StorageInitializationMetadata("writeCost", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            using var session = storage.CreateSession();
            long key = 1000;
            for (int i = 0; i < pages; i++)
            {
                await session.Write(key++, new FlowtideDotNet.Storage.SerializableObject(contents[i], compressed));
            }
            var sessionWatch = Stopwatch.StartNew();
            for (int i = 0; i < pages; i++)
            {
                await session.Write(key++, new FlowtideDotNet.Storage.SerializableObject(contents[i], compressed));
            }
            double sessionWrite = sessionWatch.Elapsed.TotalMilliseconds * 1000 / pages;
            var plainWatch = Stopwatch.StartNew();
            for (int i = 0; i < pages; i++)
            {
                await session.Write(key++, new FlowtideDotNet.Storage.SerializableObject(contents[i], plain));
            }
            double sessionWritePlain = plainWatch.Elapsed.TotalMilliseconds * 1000 / pages;
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Pages = pages,
                PageBytes = pageBytes,
                CompressedBytes = compressedBytes / (2 * pages),
                CopyMicroseconds = copyOnly,
                CompressMicroseconds = compress,
                SessionWriteCompressedMicroseconds = sessionWrite,
                SessionWriteUncompressedMicroseconds = sessionWritePlain
            }));
            return 0;
        }

        public static async Task<int> Run(string[] args)
        {
            if (ReservoirReaderBench.Option(args, "table-add", "0") == "1")
            {
                return RunTableAdd(args);
            }
            if (ReservoirReaderBench.Option(args, "write-cost", "0") == "1")
            {
                return await RunWriteCost(args);
            }
            string providerName = ReservoirReaderBench.Option(args, "provider", "memory");
            int readers = int.Parse(ReservoirReaderBench.Option(args, "readers", "10"), CultureInfo.InvariantCulture);
            int pages = int.Parse(ReservoirReaderBench.Option(args, "pages", "4000"), CultureInfo.InvariantCulture);
            int pageBytes = int.Parse(ReservoirReaderBench.Option(args, "page-bytes", "32768"), CultureInfo.InvariantCulture);
            int reads = int.Parse(ReservoirReaderBench.Option(args, "reads", "2000"), CultureInfo.InvariantCulture);
            int warmUp = int.Parse(ReservoirReaderBench.Option(args, "warm-up", "300"), CultureInfo.InvariantCulture);
            int cache = int.Parse(ReservoirReaderBench.Option(args, "cache", "10000"), CultureInfo.InvariantCulture);
            bool publisher = ReservoirReaderBench.Option(args, "publisher", "0") == "1";
            int spinners = int.Parse(ReservoirReaderBench.Option(args, "spinners", "0"), CultureInfo.InvariantCulture);
            string directory = Path.Join(ReservoirReaderBench.Option(args, "state-dir", Path.GetTempPath()), $"page-client-{Guid.NewGuid():N}");
            try
            {
                IReservoirStorageProvider provider = providerName == "memory" ? new MemoryFileProvider() : new LocalDiskProvider(Path.Join(directory, "store"));
                var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider });
                using var manager = new StateManagerSync<object>(
                    new StateManagerOptions()
                    {
                        PersistentStorage = storage,
                        CachePageCount = cache,
                        MinCachePageCount = Math.Min(1000, cache),
                        TemporaryStorageOptions = new FlowtideDotNet.Storage.FileCacheOptions() { DirectoryPath = Path.Join(directory, "tmp") }
                    },
                    NullLoggerFactory.Instance,
                    new Meter("pageClientBench"),
                    "pageClientBench",
                    GlobalMemoryManager.Instance);
                await manager.InitializeAsync();
                var client = manager.CreateImmutablePageClient("heap", new ImmutablePageClientOptions<Page>(ReservoirReaderBench.NewSerializer));

                // The same pages cache only, a publish's baseline
                var baselinePages = new Page[pages];
                for (int i = 0; i < pages; i++)
                {
                    baselinePages[i] = new Page(ReservoirReaderBench.PageBytes(i, pageBytes));
                }
                var handler = new DropHandler();
                var baselineWatch = Stopwatch.StartNew();
                for (int i = 0; i < pages; i++)
                {
                    manager.AddOrUpdate(manager.GetNewPageId(), baselinePages[i], handler);
                }
                double baselineSeconds = baselineWatch.Elapsed.TotalSeconds;

                var contents = new Page[pages];
                for (int i = 0; i < pages; i++)
                {
                    contents[i] = new Page(ReservoirReaderBench.PageBytes(i, pageBytes));
                }
                // The first publishes compile the write path and are not timed
                for (int i = 0; i < 500; i++)
                {
                    await client.PublishAsync(client.NewPageId(), new Page(ReservoirReaderBench.PageBytes(pages + i, pageBytes)));
                }
                await client.DrainAsync();
                long waitsBefore = client.QueueFullWaits;
                var ids = new long[pages];
                int gen0 = GC.CollectionCount(0);
                int gen1 = GC.CollectionCount(1);
                int gen2 = GC.CollectionCount(2);
                var steps = new List<double>();
                var publishWatch = Stopwatch.StartNew();
                for (int i = 0; i < pages; i++)
                {
                    ids[i] = client.NewPageId();
                    await client.PublishAsync(ids[i], contents[i]);
                    if ((i + 1) % 500 == 0)
                    {
                        steps.Add(Math.Round(publishWatch.Elapsed.TotalMilliseconds));
                    }
                }
                double publishSeconds = publishWatch.Elapsed.TotalSeconds;
                await client.CommitAsync();
                double writtenSeconds = publishWatch.Elapsed.TotalSeconds;
                gen0 = GC.CollectionCount(0) - gen0;
                gen1 = GC.CollectionCount(1) - gen1;
                gen2 = GC.CollectionCount(2) - gen2;
                await manager.CheckpointAsync();

                using var stop = new CancellationTokenSource();
                long published = 0;
                Task publishing = Task.CompletedTask;
                if (publisher)
                {
                    publishing = Task.Run(async () =>
                    {
                        var bytes = ReservoirReaderBench.PageBytes(7, pageBytes);
                        while (!stop.IsCancellationRequested)
                        {
                            await client.PublishAsync(client.NewPageId(), new Page(bytes));
                            Interlocked.Increment(ref published);
                        }
                    });
                }

                // Threads on their own memory, to tell load from locks
                var spinning = new Task[spinners];
                for (int i = 0; i < spinners; i++)
                {
                    int seed = i;
                    // Own threads, the pool stays free for the writer
                    spinning[i] = Task.Factory.StartNew(() =>
                    {
                        var own = new byte[64 * 1024 * 1024];
                        var random = new Random(seed + 100);
                        long sum = 0;
                        while (!stop.IsCancellationRequested)
                        {
                            sum += own[random.Next(own.Length)];
                        }
                        return sum;
                    }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                }

                long wrong = 0;
                long cold = 0;
                using var start = new ManualResetEventSlim(false);
                using var warm = new CountdownEvent(Math.Max(1, readers));
                var tasks = new Task<double>[readers];
                for (int r = 0; r < readers; r++)
                {
                    int reader = r;
                    tasks[r] = Task.Factory.StartNew(async () =>
                    {
                        using var window = client.CreateWindow();
                        var random = new Random(reader + 1);
                        // The first reads warm up and are not timed
                        for (int i = 0; i < warmUp; i++)
                        {
                            var page = await client.GetAsync(ids[random.Next(pages)], window);
                            window.Release(page);
                        }
                        warm.Signal();
                        start.Wait();
                        var watch = Stopwatch.StartNew();
                        for (int i = 0; i < reads; i++)
                        {
                            int index = random.Next(pages);
                            if (!client.TryGet(ids[index], window, out var page))
                            {
                                Interlocked.Increment(ref cold);
                                page = await client.GetAsync(ids[index], window);
                            }
                            // A cheap check, the read is what is timed
                            var expected = contents[index].Bytes;
                            if (page.Bytes.Length != expected.Length || page.Bytes[0] != expected[0] || page.Bytes[expected.Length - 1] != expected[expected.Length - 1])
                            {
                                Interlocked.Increment(ref wrong);
                            }
                            window.Release(page);
                        }
                        return watch.Elapsed.TotalSeconds;
                    }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
                }
                if (readers > 0)
                {
                    warm.Wait();
                }
                long publishedBefore = Interlocked.Read(ref published);
                var readWatch = Stopwatch.StartNew();
                start.Set();
                var times = await Task.WhenAll(tasks);
                double readSeconds = readWatch.Elapsed.TotalSeconds;
                long publishedDuring = Interlocked.Read(ref published) - publishedBefore;
                if (readers == 0 && publisher)
                {
                    // Alone, the publisher is timed for a fixed while
                    await Task.Delay(3000);
                    readSeconds = readWatch.Elapsed.TotalSeconds;
                    publishedDuring = Interlocked.Read(ref published) - publishedBefore;
                }
                stop.Cancel();
                await publishing;
                await Task.WhenAll(spinning);

                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Provider = providerName,
                    Readers = readers,
                    Pages = pages,
                    PageBytes = pageBytes,
                    Cache = cache,
                    Reads = (long)readers * reads,
                    Cold = cold,
                    Wrong = wrong,
                    Seconds = readSeconds,
                    PagesPerSecond = readers == 0 ? 0 : (double)readers * reads / readSeconds,
                    BaselineAddsPerSecond = pages / baselineSeconds,
                    PublishesPerSecond = pages / publishSeconds,
                    WrittenPerSecond = pages / writtenSeconds,
                    PublishStepsMs = steps,
                    PublishGen0 = gen0,
                    PublishGen1 = gen1,
                    PublishGen2 = gen2,
                    Publisher = publisher,
                    PublishedPerSecondDuring = publishedDuring / readSeconds,
                    QueueFullWaits = client.QueueFullWaits - waitsBefore
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
