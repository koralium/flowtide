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
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;

namespace FlowtideDotNet.Core.Tests.Reservoir
{
    [Collection(nameof(ReservoirManyReadersCollection))]
    public class ImmutablePageClientTests
    {
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        // Gone when its last rent is returned, like a tree page
        private sealed class Page : ICacheObject
        {
            private readonly byte[] _bytes;
            private int _rentCount = 1;

            public Page(byte[] bytes)
            {
                _bytes = bytes;
            }

            public byte[] Bytes
            {
                get
                {
                    ObjectDisposedException.ThrowIf(RentCount <= 0, this);
                    return _bytes;
                }
            }

            public bool RemovedFromCache { get; set; }

            public Action? BeforeReturn { get; set; }

            public Action? BeforeRent { get; set; }

            public int RentCount => Volatile.Read(ref _rentCount);

            public bool Disposed => RentCount <= 0;

            public bool TryRent()
            {
                BeforeRent?.Invoke();
                int current = Volatile.Read(ref _rentCount);
                while (current > 0)
                {
                    int old = Interlocked.CompareExchange(ref _rentCount, current + 1, current);
                    if (old == current)
                    {
                        return true;
                    }
                    current = old;
                }
                return false;
            }

            public void Return()
            {
                BeforeReturn?.Invoke();
                if (Interlocked.Decrement(ref _rentCount) < 0)
                {
                    throw new InvalidOperationException("A page was returned more often than it was rented.");
                }
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

        private sealed class TreeMetadata : IStorageMetadata
        {
            public bool Updated { get; set; }
        }

        private sealed class FixedMemory : IMemoryAllocationStats
        {
            public long Allocated { get; set; }

            public long GetAllocatedMemory()
            {
                return Allocated;
            }
        }

        private sealed class DropHandler : ICacheEvictHandler
        {
            public Task<int> Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
            {
                return Task.FromResult(valuesToEvict.Count);
            }
        }

        // Hooks shared by every serializer of one test
        private sealed class Hooks
        {
            private int _decodes;
            private int _created;
            private int _disposed;

            public Action? Decode { get; set; }

            public Action? DecodedPageRent { get; set; }

            public Action? Encode { get; set; }

            public Exception? FailEncode { get; set; }

            public Exception? FailCreate { get; set; }

            public Exception? FailDispose { get; set; }

            public int Decodes => Volatile.Read(ref _decodes);

            public int Created => Volatile.Read(ref _created);

            public int DisposedSerializers => Volatile.Read(ref _disposed);

            public System.Collections.Concurrent.ConcurrentQueue<Page> DecodedPages { get; } = new System.Collections.Concurrent.ConcurrentQueue<Page>();

            public void Decoded(Page page)
            {
                Interlocked.Increment(ref _decodes);
                DecodedPages.Enqueue(page);
            }

            public PageSerializer Create()
            {
                if (FailCreate != null)
                {
                    throw FailCreate;
                }
                Interlocked.Increment(ref _created);
                return new PageSerializer(this);
            }

            public void SerializerDisposed()
            {
                Interlocked.Increment(ref _disposed);
                if (FailDispose != null)
                {
                    throw FailDispose;
                }
            }
        }

        private sealed class PageSerializer : IStateSerializer<Page>
        {
            private readonly Hooks _hooks;

            public PageSerializer(Hooks hooks)
            {
                _hooks = hooks;
            }

            public Page Deserialize(ReadOnlySequence<byte> bytes, int length)
            {
                _hooks.Decode?.Invoke();
                var page = new Page(bytes.ToArray()) { BeforeRent = _hooks.DecodedPageRent };
                _hooks.Decoded(page);
                return page;
            }

            public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
            {
                return Deserialize(bytes, length);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in Page value)
            {
                _hooks.Encode?.Invoke();
                if (_hooks.FailEncode != null)
                {
                    throw _hooks.FailEncode;
                }
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
                _hooks.SerializerDisposed();
            }
        }

        // Holds the first caller until released
        private sealed class Gate
        {
            private int _armed = 1;

            public ManualResetEventSlim Entered { get; } = new ManualResetEventSlim(false);

            public ManualResetEventSlim Release { get; } = new ManualResetEventSlim(false);

            public void Pass()
            {
                if (Interlocked.Exchange(ref _armed, 0) == 1)
                {
                    Entered.Set();
                    Release.Wait(TimeSpan.FromMinutes(2));
                }
            }
        }

        // Counts page reads from files, and can hold or fail one
        private sealed class CountingProvider : MemoryFileProvider
        {
            private int _reads;

            public Action? BeforeRead { get; set; }

            public Exception? FailRead { get; set; }

            public Action? BeforeFileWrite { get; set; }

            // Runs once, at the first file read of a recovery
            public Action? BeforeRecoveryRead { get; set; }

            public int Reads => Volatile.Read(ref _reads);

            private void RecoveryRead()
            {
                var hook = BeforeRecoveryRead;
                if (hook != null)
                {
                    BeforeRecoveryRead = null;
                    hook();
                }
            }

            public override Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
            {
                RecoveryRead();
                return base.ReadCheckpointFileAsync(checkpointVersion, cancellationToken);
            }

            public override Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
            {
                RecoveryRead();
                return base.ReadDataFileAsync(fileId, fileSize, cancellationToken);
            }

            public override ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref _reads);
                BeforeRead?.Invoke();
                if (FailRead != null)
                {
                    throw FailRead;
                }
                return base.ReadAsync(fileId, offset, length, crc32, stateSerializer, cancellationToken);
            }

            public override async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
            {
                var hook = BeforeFileWrite;
                if (hook != null)
                {
                    await Task.Run(hook, cancellationToken);
                }
                await base.WriteDataFileAsync(fileId, crc64, size, isBundle, data, cancellationToken);
            }
        }

        private sealed class Stream : IDisposable
        {
            public Stream(StateManagerSync<object> manager, ReservoirPersistentStorage storage, CountingProvider provider, Hooks hooks)
            {
                Manager = manager;
                Storage = storage;
                Provider = provider;
                Hooks = hooks;
            }

            public StateManagerSync<object> Manager { get; }

            public ReservoirPersistentStorage Storage { get; }

            public CountingProvider Provider { get; }

            public Hooks Hooks { get; }

            public ImmutablePageClient<Page> Client(string name = "heap", int queue = 16, int window = 4)
            {
                return Manager.CreateImmutablePageClient(name, new ImmutablePageClientOptions<Page>(Hooks.Create) { WriteQueueCapacity = queue, WindowSize = window });
            }

            public void Dispose()
            {
                Manager.Dispose();
            }
        }

        private static async Task<Stream> Open(int cachePages = 10000, int restoreWaitSeconds = 10, CountingProvider? provider = null, Hooks? hooks = null)
        {
            provider ??= new CountingProvider();
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider });
            var manager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    CachePageCount = cachePages,
                    MinCachePageCount = Math.Min(1000, cachePages),
                    RecoveryCommitWaitTimeout = TimeSpan.FromSeconds(restoreWaitSeconds),
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/immutablePages/{Guid.NewGuid():N}" }
                },
                NullLoggerFactory.Instance,
                new Meter("immutablePages"),
                "immutablePages",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            return new Stream(manager, storage, provider, hooks ?? new Hooks());
        }

        private static byte[] Bytes(long page)
        {
            var bytes = new byte[500 + (int)(page % 7) * 100];
            new Random((int)page).NextBytes(bytes);
            return bytes;
        }

        private static async Task<long> Publish(ImmutablePageClient<Page> client)
        {
            long id = client.NewPageId();
            await client.PublishAsync(id, new Page(Bytes(id)));
            return id;
        }

        // Pages stored in a file and dropped from the cache
        private static async Task<long[]> PublishStoredPages(Stream stream, ImmutablePageClient<Page> client, int count)
        {
            var ids = new long[count];
            for (int i = 0; i < count; i++)
            {
                ids[i] = await Publish(client);
            }
            await client.CommitAsync();
            await stream.Manager.CheckpointAsync();
            foreach (var id in ids)
            {
                stream.Manager.DeleteFromCache(id);
            }
            return ids;
        }

        private static Task<T> Run<T>(Func<Task<T>> work)
        {
            return Task.Run(work).WaitAsync(Limit);
        }

        private static Task Run(Func<Task> work)
        {
            return Task.Run(work).WaitAsync(Limit);
        }

        private static async Task WaitUntil(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + Limit;
            while (!condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The awaited state was not reached.");
                }
                await Task.Delay(1);
            }
        }

        [Fact]
        public async Task APublishedPageIsReadFromTheCacheAndLaterFromStorage()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();

            long id = await Publish(client);
            bool cached = client.TryGet(id, window, out var first);
            var firstBytes = first!.Bytes;
            window.ReleaseAll();
            await client.CommitAsync();
            await stream.Manager.CheckpointAsync();
            stream.Manager.DeleteFromCache(id);
            bool cachedAfter = client.TryGet(id, window, out _);
            var loaded = await client.GetAsync(id, window);
            bool cachedAgain = client.TryGet(id, window, out var again);

            Assert.True(cached);
            Assert.Equal(Bytes(id), firstBytes);
            Assert.True(first.Disposed);
            Assert.False(cachedAfter);
            Assert.Equal(Bytes(id), loaded.Bytes);
            Assert.NotSame(first, loaded);
            Assert.True(cachedAgain);
            Assert.Same(loaded, again);
            Assert.Equal(1, stream.Provider.Reads);
        }

        [Fact]
        public async Task ManyReadersOfOneColdPageReadItOnce()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 1);
            var gate = new Gate();
            stream.Provider.BeforeRead = gate.Pass;
            int joined = 0;
            client.LoadJoinedHookForTests = () => Interlocked.Increment(ref joined);
            try
            {
                var windows = new List<PageWindow<Page>>();
                var reads = new List<Task<Page>>();

                for (int i = 0; i < 8; i++)
                {
                    var window = client.CreateWindow();
                    windows.Add(window);
                    reads.Add(Task.Run(async () => await client.GetAsync(ids[0], window)));
                }
                Assert.True(gate.Entered.Wait(Limit));
                await WaitUntil(() => Volatile.Read(ref joined) == 7);
                int inFlight = client.LoadsInFlight;
                gate.Release.Set();
                var pages = await Task.WhenAll(reads).WaitAsync(Limit);

                Assert.Equal(1, inFlight);
                Assert.Equal(1, stream.Provider.Reads);
                Assert.Equal(1, stream.Hooks.Decodes);
                foreach (var page in pages)
                {
                    Assert.Same(pages[0], page);
                    Assert.Equal(Bytes(ids[0]), page.Bytes);
                }
                Assert.Equal(8, client.HeldPages);
                Assert.Equal(9, pages[0].RentCount);
                Assert.Equal(0, client.LoadsInFlight);
                foreach (var window in windows)
                {
                    window.Dispose();
                }
                Assert.Equal(1, pages[0].RentCount);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task AFailedLoadFailsEveryReaderThatJoinedItAndTheNextReaderLoadsAgain()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 1);
            var fault = new IOException("The page cannot be read in this test");
            stream.Provider.FailRead = fault;
            using var first = client.CreateWindow();
            using var second = client.CreateWindow();
            await Assert.ThrowsAsync<IOException>(() => Run(async () => await client.GetAsync(ids[0], first)));
            int readsOfOneLoad = stream.Provider.Reads;
            var gate = new Gate();
            stream.Provider.BeforeRead = gate.Pass;
            int joined = 0;
            client.LoadJoinedHookForTests = () => Interlocked.Increment(ref joined);
            try
            {
                var loader = Task.Run(async () => await client.GetAsync(ids[0], first));
                Assert.True(gate.Entered.Wait(Limit));
                var joiner = Task.Run(async () => await client.GetAsync(ids[0], second));
                await WaitUntil(() => Volatile.Read(ref joined) == 1);
                gate.Release.Set();
                var loaderError = await Assert.ThrowsAsync<IOException>(() => loader.WaitAsync(Limit));
                var joinerError = await Assert.ThrowsAsync<IOException>(() => joiner.WaitAsync(Limit));
                int readsOfBoth = stream.Provider.Reads - readsOfOneLoad;
                stream.Provider.FailRead = null;
                var page = await Run(async () => await client.GetAsync(ids[0], first));

                Assert.Same(fault, loaderError);
                Assert.Same(fault, joinerError);
                Assert.Equal(readsOfOneLoad, readsOfBoth);
                Assert.Equal(Bytes(ids[0]), page.Bytes);
                Assert.Equal(0, client.LoadsInFlight);
                Assert.Equal(1, client.HeldPages);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ALoadWhoseSerializerCannotBeMadeFailsAndLeavesNothingBehind()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 1);
            using var window = client.CreateWindow();
            var fault = new InvalidOperationException("No serializer in this test");
            stream.Hooks.FailCreate = fault;

            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.GetAsync(ids[0], window));
            int inFlight = client.LoadsInFlight;
            stream.Hooks.FailCreate = null;
            var page = await Run(async () => await client.GetAsync(ids[0], window));

            Assert.Same(fault, error);
            Assert.Equal(0, inFlight);
            Assert.Equal(Bytes(ids[0]), page.Bytes);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AReaderPausedInALoadHoldsUpNoOtherReaderAndNoPublish(bool pausedInTheRead)
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 2);
            long resident = await Publish(client);
            var gate = new Gate();
            if (pausedInTheRead)
            {
                stream.Provider.BeforeRead = gate.Pass;
            }
            else
            {
                stream.Hooks.Decode = gate.Pass;
            }
            using var pausedWindow = client.CreateWindow();
            using var window = client.CreateWindow();
            try
            {
                var paused = Task.Run(async () => await client.GetAsync(ids[0], pausedWindow));
                Assert.True(gate.Entered.Wait(Limit));
                var other = await Run(async () => await client.GetAsync(ids[1], window));
                bool cached = client.TryGet(resident, window, out var residentPage);
                long published = await Run(() => Publish(client));
                await client.DrainAsync().WaitAsync(Limit);
                bool pausedStill = !paused.IsCompleted;
                gate.Release.Set();
                var first = await paused.WaitAsync(Limit);

                Assert.True(pausedStill);
                Assert.Equal(Bytes(ids[1]), other.Bytes);
                Assert.True(cached);
                Assert.Equal(Bytes(resident), residentPage!.Bytes);
                Assert.True(client.TryGet(published, window, out _));
                Assert.Equal(Bytes(ids[0]), first.Bytes);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task AReaderWithAFullWindowHoldsUpNoOne()
        {
            using var stream = await Open();
            var client = stream.Client(window: 2);
            var ids = await PublishStoredPages(stream, client, 3);
            using var full = client.CreateWindow();
            using var window = client.CreateWindow();

            await client.GetAsync(ids[0], full);
            await client.GetAsync(ids[1], full);
            var same = await Run(async () => await client.GetAsync(ids[0], window));
            var third = await Run(async () => await client.GetAsync(ids[2], window));
            long published = await Run(() => Publish(client));
            await client.DrainAsync().WaitAsync(Limit);

            Assert.Equal(2, full.Count);
            Assert.Equal(Bytes(ids[0]), same.Bytes);
            Assert.Equal(Bytes(ids[2]), third.Bytes);
            Assert.Throws<InvalidOperationException>(() => client.TryGet(published, full, out _));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.GetAsync(ids[2], full));
            Assert.Equal(4, client.HeldPages);
        }

        [Fact]
        public async Task AFullWriteQueueMakesOnlyItsOwnPublisherWait()
        {
            using var stream = await Open();
            var slow = stream.Client("slow", queue: 2);
            var other = stream.Client("other", queue: 2);
            var stored = await PublishStoredPages(stream, slow, 1);
            using var window = slow.CreateWindow();
            using var otherWindow = other.CreateWindow();
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            Task<long>? third = null;
            try
            {
                long first = await Publish(slow);
                Assert.True(gate.Entered.Wait(Limit));
                long second = await Publish(slow);
                third = Task.Run(() => Publish(slow));
                await WaitUntil(() => slow.QueueFullWaits == 1);
                long otherPage = await Run(() => Publish(other));
                await other.DrainAsync().WaitAsync(Limit);
                var cold = await Run(async () => await slow.GetAsync(stored[0], window));
                bool cached = slow.TryGet(second, window, out _);
                bool waiting = !third.IsCompleted;
                int heldWhileFull = slow.HeldPages;
                gate.Release.Set();
                long thirdPage = await third.WaitAsync(Limit);
                await slow.DrainAsync().WaitAsync(Limit);

                Assert.True(waiting);
                Assert.Equal(1, slow.QueueFullWaits);
                Assert.Equal(0, other.QueueFullWaits);
                Assert.True(other.TryGet(otherPage, otherWindow, out _));
                Assert.Equal(Bytes(stored[0]), cold.Bytes);
                Assert.True(cached);
                Assert.Equal(4, heldWhileFull);
                Assert.True(slow.TryGet(first, window, out _));
                Assert.True(slow.TryGet(thirdPage, window, out _));
            }
            finally
            {
                gate.Release.Set();
                if (third != null)
                {
                    await third.WaitAsync(Limit);
                }
            }
        }

        [Fact]
        public async Task APageThatWaitsToBeWrittenIsNotEvicted()
        {
            using var stream = await Open(cachePages: 400);
            await stream.Manager.CacheTable.StopCleanupTask();
            var slow = stream.Client("slow", queue: 8);
            var filler = stream.Client("filler", queue: 8);
            using var window = slow.CreateWindow();
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            var waiting = new long[8];
            var pages = new Page[8];
            try
            {
                for (int i = 0; i < waiting.Length; i++)
                {
                    waiting[i] = slow.NewPageId();
                    pages[i] = new Page(Bytes(waiting[i]));
                    await slow.PublishAsync(waiting[i], pages[i]);
                }
                Assert.True(gate.Entered.Wait(Limit));
                for (int i = 0; i < 600; i++)
                {
                    await Publish(filler);
                }
                await filler.DrainAsync().WaitAsync(Limit);
                await stream.Manager.CacheTable.ForceCleanup();
                int entriesAfterCleanup = stream.Manager.CacheTable.Count;
                int stillThere = 0;
                foreach (var page in pages)
                {
                    if (!page.Disposed)
                    {
                        stillThere++;
                    }
                }
                gate.Release.Set();
                await slow.CommitAsync().WaitAsync(Limit);
                await filler.CommitAsync().WaitAsync(Limit);
                await stream.Manager.CheckpointAsync();
                foreach (var id in waiting)
                {
                    stream.Manager.DeleteFromCache(id);
                }

                Assert.True(entriesAfterCleanup < 500);
                Assert.Equal(8, stillThere);
                foreach (var id in waiting)
                {
                    var page = await slow.GetAsync(id, window);
                    Assert.Equal(Bytes(id), page.Bytes);
                    window.Release(page);
                }
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task AWrittenPageThatIsNotCommittedYetIsReadBackAfterEviction()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();

            long id = await Publish(client);
            await client.DrainAsync().WaitAsync(Limit);
            stream.Manager.DeleteFromCache(id);
            var page = await Run(async () => await client.GetAsync(id, window));

            Assert.Equal(Bytes(id), page.Bytes);
            Assert.Equal(1, stream.Hooks.Decodes);
        }

        [Fact]
        public async Task APublishedPageReplacesAStaleEntryUnderItsId()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();
            long id = client.NewPageId();
            var stale = new Page(Bytes(id + 1000));
            stream.Manager.AddIfAbsent(id, stale, client, out _);

            await client.PublishAsync(id, new Page(Bytes(id)));
            bool cached = client.TryGet(id, window, out var page);

            Assert.True(cached);
            Assert.Equal(Bytes(id), page!.Bytes);
            Assert.True(stale.Disposed);
        }

        [Fact]
        public async Task ALoadThatEndsAfterAResetFailsItsReaderAndIsNotKept()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 1);
            var gate = new Gate();
            stream.Hooks.Decode = gate.Pass;
            using var window = client.CreateWindow();
            try
            {
                var read = Task.Run(async () => await client.GetAsync(ids[0], window));
                Assert.True(gate.Entered.Wait(Limit));
                await client.Reset(false);
                gate.Release.Set();
                await Assert.ThrowsAsync<InvalidOperationException>(() => read.WaitAsync(Limit));
                bool cached = stream.Manager.TryPeekCacheEntry(ids[0], out _);
                var page = await Run(async () => await client.GetAsync(ids[0], window));

                Assert.False(cached);
                Assert.Equal(Bytes(ids[0]), page.Bytes);
                Assert.Equal(2, stream.Hooks.Decodes);
                Assert.True(stream.Hooks.DecodedPages.TryPeek(out var dropped));
                Assert.True(dropped.Disposed);
                Assert.Equal(0, client.LoadsInFlight);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ALoadThatEndsDuringARestoreAddsNothingToTheNewCache()
        {
            using var stream = await Open(restoreWaitSeconds: 1);
            var client = stream.Client();
            using var window = client.CreateWindow();
            await PublishStoredPages(stream, client, 1);
            long uncommitted = await Publish(client);
            await client.DrainAsync().WaitAsync(Limit);
            stream.Manager.DeleteFromCache(uncommitted);
            var gate = new Gate();
            stream.Hooks.Decode = gate.Pass;
            try
            {
                var read = Task.Run(async () => await client.GetAsync(uncommitted, window));
                Assert.True(gate.Entered.Wait(Limit));
                await Run(() => stream.Manager.InitializeAsync());
                bool pausedStill = !read.IsCompleted;
                gate.Release.Set();
                await Assert.ThrowsAsync<InvalidOperationException>(() => read.WaitAsync(Limit));
                bool cached = stream.Manager.TryPeekCacheEntry(uncommitted, out _);
                int entries = stream.Manager.CacheTable.Count;
                long again = client.NewPageId();
                await client.PublishAsync(again, new Page(Bytes(again + 5)));
                bool published = client.TryGet(again, window, out var page);

                Assert.True(pausedStill);
                Assert.False(cached);
                Assert.Equal(0, entries);
                Assert.Equal(uncommitted, again);
                Assert.True(published);
                Assert.Equal(Bytes(again + 5), page!.Bytes);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ALoadThatEndsWhileARestoreReadsStorageAddsNothing()
        {
            using var stream = await Open(restoreWaitSeconds: 1);
            var client = stream.Client();
            using var window = client.CreateWindow();
            await PublishStoredPages(stream, client, 1);
            long uncommitted = await Publish(client);
            await client.DrainAsync().WaitAsync(Limit);
            stream.Manager.DeleteFromCache(uncommitted);
            var gate = new Gate();
            stream.Hooks.Decode = gate.Pass;
            bool endedInside = false;
            int entriesInside = -1;
            try
            {
                var read = Task.Run(async () => await client.GetAsync(uncommitted, window));
                Assert.True(gate.Entered.Wait(Limit));
                stream.Provider.BeforeRecoveryRead = () =>
                {
                    gate.Release.Set();
                    endedInside = Task.WhenAny(read).Wait(Limit);
                    entriesInside = stream.Manager.CacheTable.Count;
                };

                await Run(() => stream.Manager.InitializeAsync());
                gate.Release.Set();
                await Assert.ThrowsAsync<InvalidOperationException>(() => read.WaitAsync(Limit));
                bool cached = stream.Manager.TryPeekCacheEntry(uncommitted, out _);

                Assert.True(endedInside);
                Assert.Equal(0, entriesInside);
                Assert.False(cached);
                Assert.Equal(0, stream.Manager.CacheTable.Count);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ALoadOfADisposedClientAddsNothingToARestartedManager()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();
            var ids = await PublishStoredPages(stream, client, 1);
            var gate = new Gate();
            stream.Hooks.Decode = gate.Pass;
            try
            {
                var read = Task.Run(async () => await client.GetAsync(ids[0], window));
                Assert.True(gate.Entered.Wait(Limit));
                stream.Manager.Dispose();
                await Run(() => stream.Manager.InitializeAsync());
                gate.Release.Set();
                await Assert.ThrowsAsync<ObjectDisposedException>(() => read.WaitAsync(Limit));
                await client.DisposalTask.WaitAsync(Limit);

                Assert.False(stream.Manager.TryPeekCacheEntry(ids[0], out _));
                Assert.Equal(0, stream.Manager.CacheTable.Count);
                Assert.Equal(stream.Hooks.Created, stream.Hooks.DisposedSerializers);
                var late = new Page(Bytes(1));
                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PublishAsync(1_000_000, late));
                Assert.Equal(1, late.RentCount);
                Assert.Equal(0, client.HeldPages);
                Assert.Throws<ObjectDisposedException>(() => client.CreateWindow());
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ALoadPausedBeforeAdmissionCannotRepopulateRestoredCache()
        {
            using var stream = await Open(restoreWaitSeconds: 1);
            var client = stream.Client();
            await PublishStoredPages(stream, client, 1);
            long oldId = await Publish(client);
            await client.DrainAsync();
            stream.Manager.DeleteFromCache(oldId);
            var gate = new Gate();
            stream.Hooks.DecodedPageRent = gate.Pass;
            using var window = client.CreateWindow();
            var read = Task.Run(() => Record.ExceptionAsync(async () => await client.GetAsync(oldId, window)));
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                await stream.Manager.InitializeAsync().WaitAsync(Limit);
                gate.Release.Set();
                var failure = await read.WaitAsync(Limit);

                Assert.IsType<InvalidOperationException>(failure);
                Assert.False(stream.Manager.TryPeekCacheEntry(oldId, out _));
                Assert.Equal(0, client.LoadsInFlight);
                Assert.True(stream.Hooks.DecodedPages.Single().Disposed);
            }
            finally
            {
                gate.Release.Set();
                await read.WaitAsync(Limit);
            }
        }

        [Fact]
        public async Task ARestoredReaderDoesNotJoinAnAbandonedGeneration()
        {
            using var stream = await Open(restoreWaitSeconds: 1);
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 1);
            var gate = new Gate();
            stream.Hooks.Decode = gate.Pass;
            using var oldWindow = client.CreateWindow();
            using var newWindow = client.CreateWindow();
            var oldRead = Task.Run(() => Record.ExceptionAsync(async () => await client.GetAsync(ids[0], oldWindow)));
            Task<Page>? newRead = null;
            Exception? failure = null;
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                await stream.Manager.InitializeAsync().WaitAsync(Limit);
                newRead = client.GetAsync(ids[0], newWindow).AsTask();
                failure = await Record.ExceptionAsync(async () => await newRead.WaitAsync(TimeSpan.FromSeconds(2)));
            }
            finally
            {
                gate.Release.Set();
                await oldRead.WaitAsync(Limit);
                if (newRead != null)
                {
                    await newRead.WaitAsync(Limit);
                }
            }

            Assert.Null(failure);
            Assert.Equal(Bytes(ids[0]), newRead!.Result.Bytes);
        }

        [Fact]
        public async Task APublishThatWaitedForASlotAcrossARestoreIsRefused()
        {
            using var stream = await Open();
            var client = stream.Client(queue: 1);
            await PublishStoredPages(stream, client, 1);
            var encodeGate = new Gate();
            stream.Hooks.Encode = encodeGate.Pass;
            var rentGate = new Gate();
            Task? restore = null;
            try
            {
                long waits = client.QueueFullWaits;
                await Publish(client);
                Assert.True(encodeGate.Entered.Wait(Limit));
                long firstId = client.NewPageId();
                long secondId = client.NewPageId();
                var first = new Page(Bytes(firstId)) { BeforeRent = rentGate.Pass };
                var second = new Page(Bytes(secondId)) { BeforeRent = rentGate.Pass };
                var firstPublish = Task.Run(() => Record.ExceptionAsync(async () => await client.PublishAsync(firstId, first)));
                var secondPublish = Task.Run(() => Record.ExceptionAsync(async () => await client.PublishAsync(secondId, second)));
                await WaitUntil(() => client.QueueFullWaits == waits + 2);

                restore = Task.Run(() => stream.Manager.InitializeAsync());
                await WaitUntil(() => client.AdmissionClosed);
                encodeGate.Release.Set();
                await restore.WaitAsync(Limit);
                Assert.True(rentGate.Entered.Wait(Limit));
                bool waitedAcross = !firstPublish.IsCompleted && !secondPublish.IsCompleted;
                rentGate.Release.Set();
                var firstFailure = await firstPublish.WaitAsync(Limit);
                var secondFailure = await secondPublish.WaitAsync(Limit);

                Assert.True(waitedAcross);
                Assert.IsType<InvalidOperationException>(firstFailure);
                Assert.IsType<InvalidOperationException>(secondFailure);
                Assert.False(stream.Manager.TryPeekCacheEntry(firstId, out _));
                Assert.False(stream.Manager.TryPeekCacheEntry(secondId, out _));
                Assert.Equal(1, first.RentCount);
                Assert.Equal(1, second.RentCount);
                Assert.Equal(0, client.HeldPages);
            }
            finally
            {
                encodeGate.Release.Set();
                rentGate.Release.Set();
                if (restore != null)
                {
                    await restore.WaitAsync(Limit);
                }
            }
        }

        [Fact]
        public async Task APublishStartedBeforeRestoreCannotOverwriteAReusedId()
        {
            using var stream = await Open();
            var client = stream.Client();
            await PublishStoredPages(stream, client, 1);
            long oldId = client.NewPageId();
            var gate = new Gate();
            var stale = new Page(Bytes(oldId)) { BeforeRent = gate.Pass };
            Task<Exception?> publish = Task.Run(() => Record.ExceptionAsync(async () => await client.PublishAsync(oldId, stale)));
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                await stream.Manager.InitializeAsync().WaitAsync(Limit);
                long reusedId = client.NewPageId();
                Assert.Equal(oldId, reusedId);
                var current = new Page(Bytes(oldId + 1000));
                await client.PublishAsync(reusedId, current);
                await client.DrainAsync().WaitAsync(Limit);
                gate.Release.Set();
                var failure = await publish.WaitAsync(Limit);
                using var window = client.CreateWindow();
                var cached = await client.GetAsync(reusedId, window);

                Assert.Equal(Bytes(oldId + 1000), cached.Bytes);
                Assert.IsType<InvalidOperationException>(failure);
                Assert.Equal(1, stale.RentCount);

                window.ReleaseAll();
                await client.CommitAsync().WaitAsync(Limit);
                await stream.Manager.CheckpointAsync();
                stream.Manager.DeleteFromCache(reusedId);
                var loaded = await client.GetAsync(reusedId, window);
                Assert.Equal(Bytes(oldId + 1000), loaded.Bytes);
            }
            finally
            {
                gate.Release.Set();
                await publish.WaitAsync(Limit);
            }
        }

        [Fact]
        public async Task ARestoreDropsWhatWasQueuedAndTheClientGoesOn()
        {
            using var stream = await Open();
            var client = stream.Client(queue: 4);
            using var window = client.CreateWindow();
            var stored = await PublishStoredPages(stream, client, 2);
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            Task? restore = null;
            try
            {
                long inFlight = client.NewPageId();
                var inFlightPage = new Page(Bytes(inFlight));
                await client.PublishAsync(inFlight, inFlightPage);
                Assert.True(gate.Entered.Wait(Limit));
                long queued = client.NewPageId();
                var queuedPage = new Page(Bytes(queued));
                await client.PublishAsync(queued, queuedPage);

                restore = Task.Run(() => stream.Manager.InitializeAsync());
                await WaitUntil(() => client.AdmissionClosed);
                bool waitsForTheWriter = !restore.IsCompleted;
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.PublishAsync(client.NewPageId(), new Page(Bytes(1))));
                gate.Release.Set();
                await restore.WaitAsync(Limit);
                stream.Hooks.Encode = null;
                var old = await client.GetAsync(stored[0], window);
                var oldBytes = old.Bytes;
                window.ReleaseAll();
                long after = client.NewPageId();
                await client.PublishAsync(after, new Page(Bytes(after + 5)));
                await client.CommitAsync().WaitAsync(Limit);
                await stream.Manager.CheckpointAsync();
                stream.Manager.DeleteFromCache(after);
                var afterPage = await client.GetAsync(after, window);

                Assert.True(waitsForTheWriter);
                Assert.Equal(Bytes(stored[0]), oldBytes);
                Assert.True(inFlightPage.Disposed);
                Assert.True(queuedPage.Disposed);
                Assert.Equal(inFlight, after);
                Assert.Equal(Bytes(after + 5), afterPage.Bytes);
                Assert.Equal(1, client.HeldPages);
            }
            finally
            {
                gate.Release.Set();
                if (restore != null)
                {
                    await restore.WaitAsync(Limit);
                }
            }
        }

        [Fact]
        public async Task ARestoreWaitsForAWriterThatIsBetweenTwoPages()
        {
            using var stream = await Open();
            var client = stream.Client(queue: 4);
            var gate = new Gate();
            int returns = 0;
            long written = client.NewPageId();
            var writtenPage = new Page(Bytes(written));
            writtenPage.BeforeReturn = () =>
            {
                if (Interlocked.Increment(ref returns) == 1)
                {
                    gate.Pass();
                }
            };
            Task? restore = null;
            try
            {
                await client.PublishAsync(written, writtenPage);
                Assert.True(gate.Entered.Wait(Limit));
                long queued = client.NewPageId();
                var queuedPage = new Page(Bytes(queued));
                await client.PublishAsync(queued, queuedPage);

                restore = Task.Run(() => stream.Manager.InitializeAsync());
                await WaitUntil(() => client.AdmissionClosed);
                var first = await Task.WhenAny(restore, Task.Delay(1000));
                bool waited = first != restore;
                gate.Release.Set();
                await restore.WaitAsync(Limit);

                Assert.True(waited);
                Assert.True(queuedPage.Disposed);
                Assert.Equal(0, client.HeldPages);
            }
            finally
            {
                gate.Release.Set();
                if (restore != null)
                {
                    await restore.WaitAsync(Limit);
                }
            }
        }

        [Fact]
        public async Task ARestoreThatStopsHalfwayLeavesNoCommitWithoutItsPages()
        {
            using var stream = await Open();
            var client = stream.Client(queue: 4);
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            try
            {
                await Publish(client);
                Assert.True(gate.Entered.Wait(Limit));
                await Publish(client);
                var pause = client.PauseCommitsAsync(Limit);
                gate.Release.Set();
                await pause.WaitAsync(Limit);
                stream.Hooks.Encode = null;
                client.ResumeCommits();
                await Publish(client);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.CommitAsync().WaitAsync(Limit));
                await client.Reset(false);
                await Publish(client);
                await client.CommitAsync().WaitAsync(Limit);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ARestoreThatCannotStopTheWriterFailsAndKeepsItsCommit()
        {
            using var stream = await Open();
            var client = stream.Client();
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            try
            {
                await Publish(client);
                Assert.True(gate.Entered.Wait(Limit));
                var commit = client.CommitAsync();

                await Assert.ThrowsAsync<TimeoutException>(() => client.PauseCommitsAsync(TimeSpan.FromMilliseconds(200)));
                bool inFlight = client.HasCommitInFlight;
                gate.Release.Set();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => commit.WaitAsync(Limit));

                Assert.True(inFlight);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ACommitAfterAStopRequestIsCancelledNotDone()
        {
            using var stream = await Open();
            var client = stream.Client();

            await Publish(client);
            client.RequestStopCommits();
            await Publish(client);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.CommitAsync().WaitAsync(Limit));
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await stream.Manager.CheckpointAsync());
        }

        [Fact]
        public async Task AStopRequestStillLetsAReaderLoadAStoredPage()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 1);
            using var window = client.CreateWindow();

            stream.Manager.RequestStopCommits();
            var page = await Run(async () => await client.GetAsync(ids[0], window));

            Assert.Equal(Bytes(ids[0]), page.Bytes);
        }

        [Fact]
        public async Task APagePublishedAfterAStopRequestIsStillWritten()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();

            stream.Manager.RequestStopCommits();
            long id = await Publish(client);
            await client.DrainAsync().WaitAsync(Limit);
            stream.Manager.DeleteFromCache(id);
            var page = await Run(async () => await client.GetAsync(id, window));

            Assert.Equal(Bytes(id), page.Bytes);
        }

        [Fact]
        public async Task ALoadThatEndsWhileTheManagerDisposesItsCacheAddsNothing()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();
            var ids = await PublishStoredPages(stream, client, 1);
            long resident = client.NewPageId();
            var residentPage = new Page(Bytes(resident));
            await client.PublishAsync(resident, residentPage);
            await client.DrainAsync().WaitAsync(Limit);
            var gate = new Gate();
            stream.Hooks.Decode = gate.Pass;
            bool endedInside = false;
            try
            {
                var read = Task.Run(() => Record.ExceptionAsync(async () => await client.GetAsync(ids[0], window)));
                Assert.True(gate.Entered.Wait(Limit));
                residentPage.BeforeReturn = () =>
                {
                    gate.Release.Set();
                    endedInside = Task.WhenAny(read).Wait(Limit);
                };

                stream.Manager.Dispose();
                var failure = await read.WaitAsync(Limit);
                await client.DisposalTask.WaitAsync(Limit);

                Assert.True(endedInside);
                Assert.IsType<InvalidOperationException>(failure);
                Assert.True(stream.Hooks.DecodedPages.TryPeek(out var decoded));
                Assert.True(decoded.Disposed);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task AWindowThatCannotReturnAPageStillGivesItsReservationBack()
        {
            using var stream = await Open();
            var client = stream.Client();
            int before = stream.Manager.CacheTable.ReservedHeldPages;
            var window = client.CreateWindow();
            long id = client.NewPageId();
            var page = new Page(Bytes(id));
            await client.PublishAsync(id, page);
            await client.DrainAsync().WaitAsync(Limit);
            bool held = client.TryGet(id, window, out _);
            var fault = new InvalidOperationException("The page cannot be returned in this test");
            page.BeforeReturn = () => throw fault;

            var error = Assert.Throws<InvalidOperationException>(() => window.Dispose());
            page.BeforeReturn = null;

            Assert.True(held);
            Assert.Same(fault, error);
            Assert.Equal(before, stream.Manager.CacheTable.ReservedHeldPages);
        }

        [Fact]
        public async Task AWindowDisposalFailureDoesNotSkipOtherPages()
        {
            using var stream = await Open();
            var client = stream.Client(window: 2);
            var window = client.CreateWindow();
            long firstId = client.NewPageId();
            long secondId = client.NewPageId();
            var first = new Page(Bytes(firstId));
            var second = new Page(Bytes(secondId));
            await client.PublishAsync(firstId, first);
            await client.PublishAsync(secondId, second);
            await client.DrainAsync().WaitAsync(Limit);
            Assert.True(client.TryGet(firstId, window, out _));
            Assert.True(client.TryGet(secondId, window, out _));
            var fault = new InvalidOperationException("The first page cannot be returned in this test");
            first.BeforeReturn = () => throw fault;
            try
            {
                var error = Assert.Throws<InvalidOperationException>(() => window.Dispose());
                window.Dispose();

                Assert.Same(fault, error);
                Assert.Equal(1, second.RentCount);
            }
            finally
            {
                first.BeforeReturn = null;
                if (first.RentCount > 1)
                {
                    first.Return();
                }
                if (second.RentCount > 1)
                {
                    second.Return();
                }
            }
        }

        [Fact]
        public async Task AFailedWriteFailsTheCommitAndTheCheckpoint()
        {
            using var stream = await Open();
            var client = stream.Client();
            using var window = client.CreateWindow();
            var fault = new IOException("The page cannot be written in this test");
            stream.Hooks.FailEncode = fault;
            var refused = new Page(Bytes(1));

            await Publish(client);
            var drain = await Assert.ThrowsAsync<IOException>(() => client.DrainAsync().WaitAsync(Limit));
            var commit = await Assert.ThrowsAsync<IOException>(() => client.CommitAsync().WaitAsync(Limit));
            var publish = await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.PublishAsync(client.NewPageId(), refused));
            var checkpoint = await Assert.ThrowsAsync<IOException>(async () => await stream.Manager.CheckpointAsync());
            bool faulted = client.HasCommitFault;
            stream.Hooks.FailEncode = null;
            client.ClearCommitFault();
            long id = await Publish(client);
            await client.CommitAsync().WaitAsync(Limit);

            Assert.Same(fault, drain);
            Assert.Same(fault, commit);
            Assert.Same(fault, publish.InnerException);
            Assert.Same(fault, checkpoint);
            Assert.True(faulted);
            Assert.Equal(1, refused.RentCount);
            Assert.False(client.HasCommitFault);
            Assert.Equal(0, client.HeldPages);
            Assert.True(client.TryGet(id, window, out _));
        }

        [Fact]
        public async Task EveryClientReservesItsPagesFromOneBound()
        {
            using var stream = await Open(cachePages: 400);
            var table = stream.Manager.CacheTable;
            var first = stream.Client("first", queue: 8, window: 4);
            var second = stream.Client("second", queue: 4, window: 4);
            var one = first.CreateWindow();
            var two = second.CreateWindow();
            int reserved = table.ReservedHeldPages;
            int sessions = stream.Storage.SessionCount;
            int openSerializers = stream.Hooks.Created - stream.Hooks.DisposedSerializers;

            Assert.Throws<InvalidOperationException>(() => first.CreateWindow());
            Assert.Throws<InvalidOperationException>(() => second.CreateWindow());
            Assert.Throws<InvalidOperationException>(() => stream.Client("third", queue: 1));
            bool thirdExists = stream.Manager.StateExists("third");
            int sessionsAfterThird = stream.Storage.SessionCount;
            int openSerializersAfterThird = stream.Hooks.Created - stream.Hooks.DisposedSerializers;
            one.Dispose();
            using var again = second.CreateWindow();
            one.Dispose();
            int afterReuse = table.ReservedHeldPages;
            second.Dispose();
            await second.DisposalTask.WaitAsync(Limit);
            int afterDispose = table.ReservedHeldPages;
            two.Dispose();
            again.Dispose();

            Assert.Equal(20, table.HeldPagesBound);
            Assert.Equal(20, reserved);
            Assert.False(thirdExists);
            Assert.Equal(sessions, sessionsAfterThird);
            Assert.Equal(openSerializers, openSerializersAfterThird);
            Assert.Equal(20, afterReuse);
            Assert.Equal(16, afterDispose);
            Assert.Equal(8, table.ReservedHeldPages);
        }

        [Fact]
        public async Task AQueueSlotStaysReservedUntilItsPageRentIsReturned()
        {
            using var stream = await Open(cachePages: 20);
            var client = stream.Client(queue: 1);
            var gate = new Gate();
            var first = new Page(Bytes(1)) { BeforeReturn = gate.Pass };
            var second = new Page(Bytes(2));
            Task? secondPublish = null;
            try
            {
                await client.PublishAsync(client.NewPageId(), first);
                Assert.True(gate.Entered.Wait(Limit));
                secondPublish = client.PublishAsync(client.NewPageId(), second).AsTask();

                int pinned = (first.RentCount > 1 ? 1 : 0) + (second.RentCount > 1 ? 1 : 0);
                int bound = stream.Manager.CacheTable.HeldPagesBound;
                Assert.True(pinned <= bound, $"The queue pins {pinned} distinct cached pages against a bound of {bound}.");
            }
            finally
            {
                gate.Release.Set();
                if (secondPublish != null)
                {
                    await secondPublish.WaitAsync(Limit);
                }
                await client.DrainAsync().WaitAsync(Limit);
            }
        }

        [Fact]
        public async Task ADisposedClientKeepsItsQueueReservationUntilTheWriterReturnsItsPage()
        {
            using var stream = await Open(cachePages: 20);
            var client = stream.Client(queue: 1);
            var gate = new Gate();
            var page = new Page(Bytes(1)) { BeforeReturn = gate.Pass };
            try
            {
                await client.PublishAsync(client.NewPageId(), page);
                Assert.True(gate.Entered.Wait(Limit));
                client.Dispose();

                Assert.Equal(2, page.RentCount);
                Assert.Equal(1, stream.Manager.CacheTable.ReservedHeldPages);
            }
            finally
            {
                gate.Release.Set();
                client.Dispose();
                await client.DisposalTask.WaitAsync(Limit);
            }

            Assert.Equal(0, stream.Manager.CacheTable.ReservedHeldPages);
        }

        [Fact]
        public async Task ADisposedClientsHeldPagesStillCountAgainstTheSharedBound()
        {
            using var stream = await Open(cachePages: 400);
            var first = stream.Client("first", queue: 4, window: 4);
            var windows = new List<PageWindow<Page>>();
            try
            {
                for (int i = 0; i < 4; i++)
                {
                    var window = first.CreateWindow();
                    windows.Add(window);
                    for (int j = 0; j < 4; j++)
                    {
                        long id = await Publish(first);
                        await first.GetAsync(id, window);
                    }
                }
                await first.DrainAsync().WaitAsync(Limit);
                first.Dispose();
                await first.DisposalTask.WaitAsync(Limit);

                var second = stream.Client("second", queue: 4, window: 4);
                for (int i = 0; i < 4; i++)
                {
                    PageWindow<Page> window;
                    try
                    {
                        window = second.CreateWindow();
                    }
                    catch (InvalidOperationException)
                    {
                        break;
                    }
                    windows.Add(window);
                    for (int j = 0; j < 4; j++)
                    {
                        long id = await Publish(second);
                        await second.GetAsync(id, window);
                    }
                }
                await second.DrainAsync().WaitAsync(Limit);

                int held = 0;
                foreach (var window in windows)
                {
                    held += window.Count;
                }
                int bound = stream.Manager.CacheTable.HeldPagesBound;
                Assert.True(held <= bound, $"The windows hold {held} distinct pages against a shared bound of {bound}.");
            }
            finally
            {
                foreach (var window in windows)
                {
                    window.Dispose();
                }
            }
        }

        [Fact]
        public async Task AShrinkOfTheCacheKeepsRoomForReservedPages()
        {
            var memory = new FixedMemory();
            using var meter = new Meter("resize");
            using var table = new S3FifoTableSync(new CacheTableOptions("resize", NullLogger.Instance, meter, memory) { MaxSize = 2000, MinSize = 100, MaxMemoryUsageInBytes = 1_000_000 });
            await table.StopCleanupTask();
            var handler = new DropHandler();
            for (int i = 1; i <= 50; i++)
            {
                table.Add(i, new Page(Bytes(i)), handler);
            }

            bool reserved = table.TryReserveHeldPages(30);
            memory.Allocated = 1_000_000_000;
            Hit(table, 1);
            await table.ForceCleanup();
            int boundAfterShrink = table.HeldPagesBound;
            bool more = table.TryReserveHeldPages(1);
            table.ReleaseHeldPages(30);
            Hit(table, 2);
            await table.ForceCleanup();
            int boundWithoutReservations = table.HeldPagesBound;

            Assert.True(reserved);
            Assert.Equal(30, boundAfterShrink);
            Assert.False(more);
            Assert.Equal(5, boundWithoutReservations);
        }

        [Fact]
        public async Task AResizeLeavesASmallCacheWithoutReservationsAlone()
        {
            var memory = new FixedMemory();
            using var meter = new Meter("small");
            using var table = new S3FifoTableSync(new CacheTableOptions("small", NullLogger.Instance, meter, memory) { MaxSize = 90, MinSize = 10, MaxMemoryUsageInBytes = 1_000_000 });
            await table.StopCleanupTask();
            var handler = new DropHandler();
            for (int i = 1; i <= 50; i++)
            {
                table.Add(i, new Page(Bytes(i)), handler);
            }

            memory.Allocated = 1_000_000_000;
            Hit(table, 1);
            await table.ForceCleanup();

            Assert.Equal(4, table.HeldPagesBound);
        }

        // An idle table skips the resize
        private static void Hit(S3FifoTableSync table, long key)
        {
            Assert.True(table.TryGetCacheValue(key, out var entry));
            entry.Value.Return();
        }

        [Fact]
        public async Task ReadersAndTheQueueHoldNoMoreThanTheyReserved()
        {
            using var stream = await Open(cachePages: 400);
            var client = stream.Client(queue: 8, window: 4);
            var ids = await PublishStoredPages(stream, client, 4);
            var windows = new List<PageWindow<Page>>();
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    windows.Add(client.CreateWindow());
                }
                foreach (var window in windows)
                {
                    foreach (var id in ids)
                    {
                        await client.GetAsync(id, window);
                    }
                }
                for (int i = 0; i < 8; i++)
                {
                    await Publish(client);
                }
                int held = client.HeldPages;
                gate.Release.Set();
                await client.DrainAsync().WaitAsync(Limit);

                Assert.Equal(20, stream.Manager.CacheTable.ReservedHeldPages);
                Assert.Equal(20, held);
                Assert.Equal(12, client.HeldPages);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task AClientThatCannotBeCreatedLeavesNothingBehind()
        {
            using var stream = await Open();
            var fault = new InvalidOperationException("No serializer in this test");
            int sessionsBefore = stream.Storage.SessionCount;

            Assert.Throws<ArgumentOutOfRangeException>(() => stream.Client("pages", queue: 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => stream.Client("pages", window: 0));
            bool boundAfterBadOptions = stream.Manager.StateExists("pages");
            stream.Hooks.FailCreate = fault;
            var error = Assert.Throws<InvalidOperationException>(() => stream.Client("pages"));
            bool boundAfterFailure = stream.Manager.StateExists("pages");
            int sessionsAfterFailure = stream.Storage.SessionCount;
            stream.Hooks.FailCreate = null;
            var client = stream.Client("pages");

            Assert.False(boundAfterBadOptions);
            Assert.Same(fault, error);
            Assert.False(boundAfterFailure);
            Assert.Equal(sessionsBefore, sessionsAfterFailure);
            Assert.Same(client, stream.Client("pages"));
        }

        [Fact]
        public async Task ASerializerDisposalFailureStillDisposesEveryPooledSerializer()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 2);
            using var firstWindow = client.CreateWindow();
            using var secondWindow = client.CreateWindow();
            using var entered = new CountdownEvent(2);
            using var release = new ManualResetEventSlim(false);
            stream.Hooks.Decode = () =>
            {
                entered.Signal();
                if (!release.Wait(Limit))
                {
                    throw new TimeoutException("The decode gate was not released.");
                }
            };
            var first = Task.Run(async () => await client.GetAsync(ids[0], firstWindow));
            var second = Task.Run(async () => await client.GetAsync(ids[1], secondWindow));
            try
            {
                Assert.True(entered.Wait(Limit));
                release.Set();
                await Task.WhenAll(first, second).WaitAsync(Limit);
                firstWindow.ReleaseAll();
                secondWindow.ReleaseAll();
                stream.Hooks.FailDispose = new IOException("A serializer cannot be disposed in this test");

                stream.Manager.Dispose();
                await client.DisposalTask.WaitAsync(Limit);

                Assert.Equal(3, stream.Hooks.Created);
                Assert.Equal(stream.Hooks.Created, stream.Hooks.DisposedSerializers);
            }
            finally
            {
                release.Set();
                await Task.WhenAll(first, second).WaitAsync(Limit);
            }
        }

        [Fact]
        public async Task ADisposeThatFailsInsideStillEndsAndTheManagerStartsAgain()
        {
            using var stream = await Open();
            var client = stream.Client();
            var ids = await PublishStoredPages(stream, client, 2);
            using var window = client.CreateWindow();
            await client.GetAsync(ids[0], window);
            window.ReleaseAll();
            stream.Hooks.FailDispose = new IOException("A serializer cannot be disposed in this test");

            stream.Manager.Dispose();
            await client.DisposalTask.WaitAsync(Limit);
            await Run(() => stream.Manager.InitializeAsync());

            Assert.True(client.DisposalTask.IsCompletedSuccessfully);
            Assert.Equal(stream.Hooks.Created, stream.Hooks.DisposedSerializers);
        }

        [Fact]
        public async Task ATreeWriterGoesOnWhileTenReadersHoldFullWindows()
        {
            using var stream = await Open(cachePages: 1200);
            var client = stream.Client(queue: 4, window: 4);
            var other = stream.Client("other", queue: 2);
            var ids = await PublishStoredPages(stream, client, 40);
            var windows = new List<PageWindow<Page>>();
            for (int reader = 0; reader < 10; reader++)
            {
                var window = client.CreateWindow();
                windows.Add(window);
                for (int i = 0; i < 4; i++)
                {
                    await client.GetAsync(ids[reader * 4 + i], window);
                }
            }
            var gate = new Gate();
            stream.Hooks.Encode = gate.Pass;
            Task<long>? waiting = null;
            try
            {
                await Publish(other);
                Assert.True(gate.Entered.Wait(Limit));
                await Publish(other);
                waiting = Task.Run(() => Publish(other));
                var allocator = GlobalMemoryManager.Instance;
                var tree = await stream.Manager.GetOrCreateClient("writer").GetOrCreateTree("tree",
                    new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                    {
                        Comparer = new PrimitiveListComparer<long>(),
                        KeySerializer = new PrimitiveListKeyContainerSerializer<long>(allocator),
                        ValueSerializer = new PrimitiveListValueContainerSerializer<long>(allocator),
                        UseByteBasedPageSizes = true,
                        PageSizeBytes = 256,
                        MemoryAllocator = allocator
                    });

                await Run(async () =>
                {
                    for (long key = 0; key < 40_000; key++)
                    {
                        await tree.Upsert(key, key * 3);
                        if (key % 1000 == 0)
                        {
                            await stream.Manager.WaitForNotFullAsync();
                        }
                    }
                    await tree.Commit();
                });
                var found = await Run(async () => await tree.GetValue(39_999));
                int held = client.HeldPages;
                bool otherWaits = !waiting.IsCompleted;
                gate.Release.Set();
                await waiting.WaitAsync(Limit);

                Assert.True(found.found);
                Assert.Equal(39_999 * 3, found.value);
                Assert.Equal(40, held);
                Assert.True(otherWaits);
                Assert.True(stream.Manager.CacheTable.Count < 1200 + 100);
                foreach (var window in windows)
                {
                    Assert.Equal(4, window.Count);
                    window.Dispose();
                }
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ACommitStoresEveryPagePublishedBeforeIt()
        {
            var provider = new CountingProvider();
            var hooks = new Hooks();
            long[] ids;
            using (var first = await Open(provider: provider, hooks: hooks))
            {
                var client = first.Client();
                ids = new long[40];
                for (int i = 0; i < ids.Length; i++)
                {
                    ids[i] = await Publish(client);
                }
                await client.CommitAsync().WaitAsync(Limit);
                await first.Manager.CheckpointAsync();
                await Publish(client);
            }

            using var second = await Open(provider: provider, hooks: hooks);
            var restarted = second.Client();
            using var window = restarted.CreateWindow();
            foreach (var id in ids)
            {
                var page = await restarted.GetAsync(id, window);
                Assert.Equal(Bytes(id), page.Bytes);
                window.Release(page);
            }
            long next = restarted.NewPageId();
            Assert.True(next > ids[ids.Length - 1]);
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await restarted.GetAsync(next, window));
        }

        [Fact]
        public async Task AFileThatFillsDuringACheckpointGoesIntoTheNextOne()
        {
            var provider = new CountingProvider();
            var hooks = new Hooks();
            long during;
            long before;
            using (var stream = await Open(provider: provider, hooks: hooks))
            {
                using var session = (ReservoirPersistentSession)stream.Storage.CreateSession();
                before = stream.Manager.GetNewPageId();
                await session.Write(before, new SerializableObject(Bytes(before)));
                await session.Commit();
                var gate = new Gate();
                provider.BeforeFileWrite = gate.Pass;
                try
                {
                    var checkpoint = Task.Run(async () => await stream.Manager.CheckpointAsync());
                    Assert.True(gate.Entered.Wait(Limit));
                    during = stream.Manager.GetNewPageId();
                    await session.Write(during, new SerializableObject(Bytes(during)));
                    var handover = session.SendBlobFile_Testing();
                    bool waited = !handover.IsCompleted;
                    gate.Release.Set();
                    await checkpoint.WaitAsync(Limit);
                    await handover.WaitAsync(Limit);
                    provider.BeforeFileWrite = null;
                    await session.Commit();
                    await stream.Manager.CheckpointAsync();

                    Assert.True(waited);
                }
                finally
                {
                    gate.Release.Set();
                }
            }

            using var restarted = await Open(provider: provider, hooks: hooks);
            using var reader = restarted.Storage.CreateSession();
            Assert.Equal(Bytes(before), (await reader.Read(before)).ToArray());
            Assert.Equal(Bytes(during), (await reader.Read(during)).ToArray());
        }

        [Fact]
        public async Task TheTableKeepsTheFirstValueOfAKey()
        {
            using var stream = await Open();
            var client = stream.Client();
            var first = new Page(Bytes(1));
            var second = new Page(Bytes(2));
            long key = client.NewPageId();

            bool added = stream.Manager.AddIfAbsent(key, first, client, out var firstEntry);
            bool addedAgain = stream.Manager.AddIfAbsent(key, second, client, out var secondEntry);
            stream.Manager.DeleteFromCache(key);
            bool addedAfterDelete = stream.Manager.AddIfAbsent(key, second, client, out var thirdEntry);

            Assert.True(added);
            Assert.False(addedAgain);
            Assert.Same(firstEntry, secondEntry);
            Assert.Same(first, secondEntry.Value);
            Assert.True(addedAfterDelete);
            Assert.Same(second, thirdEntry.Value);
        }

        [Fact]
        public async Task ANameCannotBeBothATreeAndPages()
        {
            using var stream = await Open();
            stream.Client("pages");

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.Manager.CreateClientAsync<Page, TreeMetadata>("pages", new StateClientOptions<Page>() { ValueSerializer = new PageSerializer(stream.Hooks) }, GlobalMemoryManager.Instance));
        }
    }
}
