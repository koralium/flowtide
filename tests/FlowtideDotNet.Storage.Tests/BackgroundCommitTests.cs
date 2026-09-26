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

using FlowtideDotNet.Storage.AppendTree;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.AppendTree.Internal;
using FlowtideDotNet.Storage.Queue.Internal;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Reflection;
using Xunit;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// Commit hands its pages to a background walk. A page fetched before the walk reaches it
    /// is written by the fetch, never twice, and the checkpoint joins the walk.
    /// </summary>
    public class BackgroundCommitTests
    {
        /// <summary>
        /// Records every page write with the value it carried, and can fault the writes of chosen pages.
        /// </summary>
        internal class RecordingSession : IPersistentStorageSession
        {
            private readonly IPersistentStorageSession _inner;

            public RecordingSession(IPersistentStorageSession inner)
            {
                _inner = inner;
            }

            public IPersistentStorageSession Inner => _inner;

            /// <summary>
            /// Time each write holds whatever lock the writer took, to make a walk long.
            /// </summary>
            public TimeSpan WriteDelay { get; set; }

            public ConcurrentDictionary<long, List<int>> WrittenValues { get; } = new ConcurrentDictionary<long, List<int>>();

            public ConcurrentDictionary<long, byte> FaultingKeys { get; } = new ConcurrentDictionary<long, byte>();

            private readonly TaskCompletionSource _writerBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private ManualResetEventSlim? _writeGate;
            private IReadOnlySet<long>? _gatedKeys;
            private int _writesInFlight;

            /// <summary>
            /// True once a read ran while a write was in progress on this session.
            /// </summary>
            public volatile bool ReadDuringWrite;

            public bool? ConcurrentReadsOverride { get; set; }

            public bool SupportsConcurrentReads => ConcurrentReadsOverride ?? _inner.SupportsConcurrentReads;

            public int WriteCount(long key)
            {
                return WrittenValues.TryGetValue(key, out var values) ? values.Count : 0;
            }

            public ConcurrentDictionary<long, int> TotalWrites { get; } = new ConcurrentDictionary<long, int>();

            public int TotalWriteCount(long key)
            {
                return TotalWrites.TryGetValue(key, out var count) ? count : 0;
            }

            /// <summary>
            /// Blocks the first write of one of the keys inside Write, the writer keeps whatever lock it holds.
            /// </summary>
            public void ArmWriteGate(ManualResetEventSlim gate, IReadOnlySet<long> keys)
            {
                _gatedKeys = keys;
                Volatile.Write(ref _writeGate, gate);
            }

            public Task WriterBlocked => _writerBlocked.Task;

            public Task Write(long key, SerializableObject value)
            {
                if (FaultingKeys.ContainsKey(key))
                {
                    throw new IOException($"Injected write failure for page {key}");
                }
                Interlocked.Increment(ref _writesInFlight);
                try
                {
                    if (WriteDelay > TimeSpan.Zero)
                    {
                        Thread.Sleep(WriteDelay);
                    }
                    if (_gatedKeys != null && _gatedKeys.Contains(key))
                    {
                        var gate = Interlocked.Exchange(ref _writeGate, null);
                        if (gate != null)
                        {
                            _writerBlocked.TrySetResult();
                            gate.Wait();
                        }
                    }
                    return WriteCore(key, value);
                }
                finally
                {
                    Interlocked.Decrement(ref _writesInFlight);
                }
            }

            private Task WriteCore(long key, SerializableObject value)
            {
                TotalWrites.AddOrUpdate(key, 1, (_, c) => c + 1);
                var writer = new ArrayBufferWriter<byte>();
                value.Serialize(writer);
                if (writer.WrittenCount == 4)
                {
                    var list = WrittenValues.GetOrAdd(key, _ => new List<int>());
                    lock (list)
                    {
                        list.Add(BinaryPrimitives.ReadInt32LittleEndian(writer.WrittenSpan));
                    }
                }
                return _inner.Write(key, new SerializableObject(writer.WrittenMemory));
            }

            public ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer)
                where T : ICacheObject
            {
                if (Volatile.Read(ref _writesInFlight) > 0)
                {
                    ReadDuringWrite = true;
                }
                return _inner.Read(key, stateSerializer);
            }

            public ValueTask<ReadOnlyMemory<byte>> Read(long key)
            {
                if (Volatile.Read(ref _writesInFlight) > 0)
                {
                    ReadDuringWrite = true;
                }
                return _inner.Read(key);
            }

            public Task Delete(long key) => _inner.Delete(key);

            public volatile bool FaultCommit;

            public Task Commit()
            {
                if (FaultCommit)
                {
                    throw new IOException("Injected commit failure");
                }
                return _inner.Commit();
            }

            public volatile bool Disposed;

            public void Dispose()
            {
                Disposed = true;
                _inner.Dispose();
            }
        }

        /// <summary>
        /// A file cache whose Free can be held, to park a commit write inside its spill cleanup.
        /// </summary>
        private class RecordingFileCache : FlowtideDotNet.Storage.FileCache.IFileCache
        {
            private readonly FlowtideDotNet.Storage.FileCache.IFileCache _inner;

            public RecordingFileCache(FlowtideDotNet.Storage.FileCache.IFileCache inner)
            {
                _inner = inner;
            }

            private readonly TaskCompletionSource _freeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private ManualResetEventSlim? _freeGate;
            private TaskCompletionSource? _freeGateEntered;
            private long _gatedFreeKey;

            /// <summary>
            /// Blocks the first Free of the key inside the call, the caller keeps whatever it holds.
            /// </summary>
            public void ArmFreeGate(ManualResetEventSlim gate, long key, TaskCompletionSource? entered = null)
            {
                _gatedFreeKey = key;
                _freeGateEntered = entered;
                Volatile.Write(ref _freeGate, gate);
            }

            public Task FreeEntered => _freeEntered.Task;

            private readonly object _writeGateLock = new object();
            private readonly Dictionary<int, (ManualResetEventSlim Gate, TaskCompletionSource Entered)> _writeGates = new();
            private readonly List<long> _writtenKeys = new List<long>();
            private int _writes;

            /// <summary>
            /// Blocks the nth Write from now inside the call, the caller keeps whatever it holds.
            /// </summary>
            public Task ArmWriteGate(int nth, ManualResetEventSlim gate)
            {
                var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                lock (_writeGateLock)
                {
                    _writeGates[_writes + nth] = (gate, entered);
                }
                return entered.Task;
            }

            public List<long> WrittenKeys
            {
                get
                {
                    lock (_writeGateLock)
                    {
                        return _writtenKeys.ToList();
                    }
                }
            }

            public void Write(long id, SerializableObject serializableObject)
            {
                (ManualResetEventSlim Gate, TaskCompletionSource Entered) gated;
                lock (_writeGateLock)
                {
                    _writes++;
                    _writtenKeys.Add(id);
                    _writeGates.Remove(_writes, out gated);
                }
                if (gated.Gate != null)
                {
                    gated.Entered.TrySetResult();
                    if (!gated.Gate.Wait(Timeout))
                    {
                        throw new TimeoutException("The spill write was not released.");
                    }
                }
                _inner.Write(id, serializableObject);
            }

            public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => _inner.Read(pageKey);

            public ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer)
                where T : ICacheObject => _inner.Read(pageKey, serializer);

            public void Free(in long pageKey)
            {
                if (pageKey == _gatedFreeKey)
                {
                    var gate = Interlocked.Exchange(ref _freeGate, null);
                    if (gate != null)
                    {
                        var entered = _freeGateEntered;
                        _freeEntered.TrySetResult();
                        entered?.TrySetResult();
                        gate.Wait();
                    }
                }
                _inner.Free(pageKey);
            }

            public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);

            private readonly TaskCompletionSource _flushEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private ManualResetEventSlim? _flushGate;

            /// <summary>
            /// Blocks the first Flush inside the call, the caller keeps whatever it holds.
            /// </summary>
            public void ArmFlushGate(ManualResetEventSlim gate)
            {
                Volatile.Write(ref _flushGate, gate);
            }

            public Task FlushEntered => _flushEntered.Task;

            public void Flush()
            {
                var gate = Interlocked.Exchange(ref _flushGate, null);
                if (gate != null)
                {
                    _flushEntered.TrySetResult();
                    gate.Wait();
                }
                _inner.Flush();
            }

            public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();

            public void Dispose() => _inner.Dispose();
        }

        private class RecordingFileCacheFactory : IFileCacheFactory
        {
            private readonly IFileCacheFactory _inner;

            public RecordingFileCacheFactory(IFileCacheFactory inner)
            {
                _inner = inner;
            }

            public List<RecordingFileCache> Created { get; } = new List<RecordingFileCache>();

            public FlowtideDotNet.Storage.FileCache.IFileCache Create(string name, IMemoryAllocator memoryAllocator)
            {
                var cache = new RecordingFileCache(_inner.Create(name, memoryAllocator));
                lock (Created)
                {
                    Created.Add(cache);
                }
                return cache;
            }
        }

        internal class RecordingStorage : IPersistentStorage
        {
            private readonly IPersistentStorage _inner;

            public RecordingStorage(IPersistentStorage inner)
            {
                _inner = inner;
            }

            public List<RecordingSession> Sessions { get; } = new List<RecordingSession>();

            public bool? ThreadSafeSessions { get; set; }

            public IPersistentStorageSession CreateSession()
            {
                var session = new RecordingSession(_inner.CreateSession()) { ConcurrentReadsOverride = ThreadSafeSessions };
                lock (Sessions)
                {
                    Sessions.Add(session);
                }
                return session;
            }

            public long CurrentVersion => _inner.CurrentVersion;

            public Task InitializeAsync(StorageInitializationMetadata metadata) => _inner.InitializeAsync(metadata);

            public ValueTask CheckpointAsync(byte[] metadata, bool includeIndex) => _inner.CheckpointAsync(metadata, includeIndex);

            public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount) => _inner.CompactAsync(changesSinceLastCompact, pageCount);

            public ValueTask ResetAsync() => _inner.ResetAsync();

            public ValueTask RecoverAsync(long checkpointVersion) => _inner.RecoverAsync(checkpointVersion);

            public bool TryGetValue(long key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out ReadOnlyMemory<byte>? value) => _inner.TryGetValue(key, out value);

            public ValueTask Write(long key, byte[] value) => _inner.Write(key, value);

            public void ClearForRestore() => _inner.ClearForRestore();

            public void Dispose() => _inner.Dispose();
        }

        /// <summary>
        /// Holds the manager's next background walk before it claims its first page.
        /// </summary>
        internal sealed class WalkGate : IDisposable
        {
            private readonly StateManagerSync _manager;
            private readonly TaskCompletionSource _release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _armed = 1;

            public WalkGate(StateManagerSync manager)
            {
                _manager = manager;
                _manager.PageWriteHookForTests = Hook;
            }

            public Task Blocked => _blocked.Task;

            private Task Hook(string name, long key)
            {
                if (Interlocked.Exchange(ref _armed, 0) == 0)
                {
                    return Task.CompletedTask;
                }
                _blocked.TrySetResult();
                return _release.Task;
            }

            public void Release()
            {
                _release.TrySetResult();
            }

            public void Dispose()
            {
                Release();
                _manager.PageWriteHookForTests = null;
            }
        }

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        internal static async Task<(StateManagerSync<StateManagerMetadata> manager, RecordingStorage storage)> CreateManager(string name, int cachePageCount = 1000, bool useReadCache = false, bool backgroundCommit = true, bool? threadSafeSession = null, bool reservoir = true, TimeSpan? recoveryCommitWaitTimeout = null, IFileCacheFactory? fileCacheFactory = null, IReservoirStorageProvider? fileProvider = null)
        {
            IPersistentStorage inner = reservoir
                ? new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = fileProvider ?? new MemoryFileProvider() })
                : new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"./data/bgcommit_{name}/persist" });
            var storage = new RecordingStorage(inner)
            {
                ThreadSafeSessions = threadSafeSession
            };
            var options = new StateManagerOptions()
            {
                PersistentStorage = storage,
                CachePageCount = cachePageCount,
                MinCachePageCount = cachePageCount,
                UseReadCache = useReadCache,
                BackgroundCommit = backgroundCommit,
                RecoveryCommitWaitTimeout = recoveryCommitWaitTimeout ?? TimeSpan.FromSeconds(10),
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/bgcommit_{name}/temp" },
                FileCacheFactory = fileCacheFactory
            };
            var manager = new StateManagerSync<StateManagerMetadata>(options, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter($"bgcommit_{name}"), name, GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            await manager.CacheTable.StopCleanupTask();
            return (manager, storage);
        }

        private static async Task<(IStateClient<TestPage, TestMetadata> client, RecordingSession session, List<long> keys)> CreateClientWithPages(StateManagerSync<StateManagerMetadata> manager, RecordingStorage storage, string clientName, int pageCount, TestPageSerializer? serializer = null)
        {
            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                clientName,
                new StateClientOptions<TestPage>() { ValueSerializer = serializer ?? new TestPageSerializer() },
                GlobalMemoryManager.Instance);
            var session = storage.Sessions.Last();
            var keys = new List<long>();
            for (int i = 0; i < pageCount; i++)
            {
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(i));
                keys.Add(key);
            }
            return (client, session, keys);
        }

        private static async Task WaitUntil(Func<bool> condition, string what)
        {
            var deadline = DateTime.UtcNow + Timeout;
            while (!condition())
            {
                Assert.True(DateTime.UtcNow < deadline, $"timed out waiting for {what}");
                await Task.Delay(10);
            }
        }

        private static int ReadPersisted(RecordingStorage storage, long key)
        {
            Assert.True(storage.TryGetValue(key, out var bytes), $"page {key} is not in persistent storage");
            return BinaryPrimitives.ReadInt32LittleEndian(bytes.Value.Span);
        }

        private static void AssertCausedBy<T>(Exception? error)
            where T : Exception
        {
            for (var current = error; current != null; current = current.InnerException)
            {
                if (current is T)
                {
                    return;
                }
            }
            Assert.Fail($"expected a {typeof(T).Name} in the exception chain, got {error?.ToString() ?? "no exception"}");
        }

        [Fact]
        public async Task CachedPageProbesRejectPagesDeletedDuringBackgroundWriteCompletion()
        {
            var factory = new RecordingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions
            {
                DirectoryPath = "./data/bgcommit_deleted_cache_probe/temp"
            }));
            var (manager, storage) = await CreateManager("deleted_cache_probe", fileCacheFactory: factory);
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "pages", 1);
            var key = keys[0];
            Assert.True(manager.TryPeekCacheEntry(key, out var entry));
            var cache = factory.Created.Single();
            using var beforeWrite = new ManualResetEventSlim(false);
            using var afterWrite = new ManualResetEventSlim(false);
            using var beforeRemoval = new ManualResetEventSlim(false);
            var removalEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            session.ArmWriteGate(beforeWrite, new HashSet<long> { key });
            cache.ArmFreeGate(afterWrite, key);
            Task<bool>? probe = null;
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await session.WriterBlocked.WaitAsync(Timeout);
                client.Delete(key);
                beforeWrite.Set();
                await cache.FreeEntered.WaitAsync(Timeout);
                cache.ArmFreeGate(beforeRemoval, key, removalEntered);
                afterWrite.Set();
                await removalEntered.Task.WaitAsync(Timeout);
                var originalRents = entry.Value.RentCount;
                probe = Task.Run(() =>
                {
                    var found = client.TryGetCachedValue(key, out var page);
                    page?.Return();
                    return found;
                });
                await WaitUntil(() => probe.IsCompleted || entry.Value.RentCount > originalRents, "the cache probe to rent or reject the deleted page");
            }
            finally
            {
                beforeWrite.Set();
                afterWrite.Set();
                beforeRemoval.Set();
                if (probe != null) await probe.WaitAsync(Timeout);
                await ((StateClient)client).WaitForCommitAsync().WaitAsync(Timeout);
            }

            // Completed deletions must remain invisible to cache probes.
            Assert.NotNull(probe);
            Assert.False(await probe);
        }

        [Fact]
        public async Task SmallCommitsDoNotScanSlotsFromEarlierLargeCommits()
        {
            var (manager, storage) = await CreateManager("reused_dictionary_scan", cachePageCount: 4096);
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "pages", 4096);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var page = (await client.GetValue(keys[0]))!;
            page.Value = 42;
            client.AddOrUpdate(keys[0], page);
            page.Return();
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            page = (await client.GetValue(keys[0]))!;
            page.Value = 43;
            client.AddOrUpdate(keys[0], page);
            page.Return();

            using var gate = new WalkGate(manager);
            int pendingPages;
            int scannedSlots;
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await gate.Blocked.WaitAsync(Timeout);
                var generation = client.GetType().GetField("m_commit", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
                var pending = generation.GetType().GetField("Pending")!.GetValue(generation)!;
                pendingPages = ((System.Collections.IDictionary)pending).Count;
                // Dictionary enumeration scans removed slots below this count.
                scannedSlots = (int)pending.GetType().GetField("_count", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(pending)!;
            }
            finally
            {
                gate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            }

            Assert.Equal(43, ReadPersisted(storage, keys[0]));
            Assert.Equal(1, pendingPages);
            // Reused dictionaries must forget earlier generations' removed slots.
            Assert.Equal(pendingPages, scannedSlots);
        }

        [Fact]
        public async Task ConcurrentSessionPageReadsDoNotAllocateAdditionalAwaiters()
        {
            var provider = new Reservoir.TestDataProvider();
            var (manager, storage) = await CreateManager("session_read_allocations", fileProvider: provider);
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "pages", 1);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.True(session.SupportsConcurrentReads);
            var key = keys[0];
            Func<Task<ReadOnlyMemory<byte>>> directRead = () => session.Read(key).AsTask();
            Func<Task<ReadOnlyMemory<byte>>> clientRead = () => sync.ReadPage(key);

            async Task<long> MeasureReads(Func<Task<ReadOnlyMemory<byte>>> read, int count)
            {
                long allocated = 0;
                for (int i = 0; i < count; i++)
                {
                    provider.BlockMemoryReads();
                    Task<ReadOnlyMemory<byte>>? pending = null;
                    try
                    {
                        // Count allocations before any continuation can resume.
                        var before = GC.GetAllocatedBytesForCurrentThread();
                        pending = read();
                        allocated += GC.GetAllocatedBytesForCurrentThread() - before;
                        Assert.False(pending.IsCompleted);
                    }
                    finally
                    {
                        provider.UnblockMemoryReads();
                        if (pending != null)
                        {
                            var bytes = await pending.WaitAsync(Timeout);
                            Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(bytes.Span));
                        }
                    }
                }
                return allocated;
            }

            await MeasureReads(directRead, 32);
            await MeasureReads(clientRead, 32);
            var directAllocated = await MeasureReads(directRead, 64);
            var clientAllocated = await MeasureReads(clientRead, 64);
            Assert.True(clientAllocated <= directAllocated,
                $"Concurrent session forwarding allocated {clientAllocated - directAllocated} additional bytes for 64 pending reads.");
        }

        [Fact]
        public async Task RestartDoesNotExposeWritesFromAnAbandonedSession()
        {
            var factory = new RecordingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions
            {
                DirectoryPath = "./data/bgcommit_review_abandoned_restart/temp"
            }));
            var (manager, storage) = await CreateManager("review_abandoned_restart", fileCacheFactory: factory);
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "pages", 0);
            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(1));
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var page = (await client.GetValue(key))!;
            page.Value = 2;
            client.AddOrUpdate(key, page);
            page.Return();

            using var beforeWrite = new ManualResetEventSlim(false);
            using var afterWrite = new ManualResetEventSlim(false);
            session.ArmWriteGate(beforeWrite, new HashSet<long> { key });
            var cache = factory.Created.Single();
            cache.ArmFreeGate(afterWrite, key);
            Task? oldWorker = null;
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await session.WriterBlocked.WaitAsync(Timeout);
                oldWorker = ((StateClient)client).WaitForCommitAsync();
                manager.RequestStopCommits();
                manager.Dispose();
                Assert.False(oldWorker.IsCompleted);

                var restart = manager.InitializeAsync();
                await Task.WhenAny(restart, Task.Delay(TimeSpan.FromSeconds(1)));
                if (!restart.IsCompleted) afterWrite.Set();
                // Abandoned writes cannot alter recovered checkpoint values.
                beforeWrite.Set();
                await restart.WaitAsync(Timeout);
                await Task.WhenAny(cache.FreeEntered, oldWorker).WaitAsync(Timeout);
                using var recoveredSession = storage.CreateSession();
                var recovered = await recoveredSession.Read(key);
                Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(recovered.Span));
            }
            finally
            {
                beforeWrite.Set();
                afterWrite.Set();
                if (oldWorker != null) await Record.ExceptionAsync(() => oldWorker.WaitAsync(Timeout));
                manager.Dispose();
                await WaitUntil(() => session.Disposed, "abandoned session disposal");
            }
        }

        [Fact]
        public async Task RestartWaitsForAbandonedClientDisposal()
        {
            var (manager, storage) = await CreateManager("review_disposal_restart");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            using var beforeWrite = new ManualResetEventSlim(false);
            using var finishDisposal = new ManualResetEventSlim(false);
            var disposalEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var serializer = new TestPageSerializer
            {
                DisposeHook = () =>
                {
                    disposalEntered.TrySetResult();
                    if (!finishDisposal.Wait(Timeout)) throw new TimeoutException("Disposal was not released");
                }
            };
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "pages", 1, serializer);
            session.ArmWriteGate(beforeWrite, keys.ToHashSet());
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await session.WriterBlocked.WaitAsync(Timeout);
                manager.RequestStopCommits();
                manager.Dispose();
                beforeWrite.Set();
                await disposalEntered.Task.WaitAsync(Timeout);

                var restart = manager.InitializeAsync();
                await Task.WhenAny(restart, Task.Delay(200));
                Assert.False(restart.IsCompleted);
                finishDisposal.Set();
                await restart.WaitAsync(Timeout);
                Assert.True(session.Disposed);
                await CreateClientWithPages(manager, storage, "pages", 0);
            }
            finally
            {
                beforeWrite.Set();
                finishDisposal.Set();
                manager.Dispose();
                await WaitUntil(() => session.Disposed, "abandoned session disposal");
            }
        }

        [Fact]
        public async Task AbandonedSessionIsDisposedWhenSerializerDisposalFails()
        {
            var (manager, storage) = await CreateManager("review_failed_disposal");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            using var beforeWrite = new ManualResetEventSlim(false);
            var serializer = new TestPageSerializer
            {
                DisposeHook = () => throw new IOException("Injected serializer disposal failure")
            };
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "pages", 1, serializer);
            session.ArmWriteGate(beforeWrite, keys.ToHashSet());
            Task? disposal = null;
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await session.WriterBlocked.WaitAsync(Timeout);
                manager.RequestStopCommits();
                manager.Dispose();
                disposal = ((StateClient)client).DisposalTask;
                beforeWrite.Set();
                await Record.ExceptionAsync(() => disposal.WaitAsync(Timeout));
                // Serializer disposal failures must not strand sessions.
                Assert.True(session.Disposed);
            }
            finally
            {
                beforeWrite.Set();
                if (disposal != null) await Record.ExceptionAsync(() => disposal.WaitAsync(Timeout));
                session.Dispose();
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ManagerDisposesAllClientsWhenClientDisposalFails(bool secondClientFails)
        {
            var (manager, storage) = await CreateManager($"review_manager_disposal_{secondClientFails}");
            using var storageLifetime = storage;
            var failure = new IOException("Injected first serializer disposal failure");
            var firstSerializer = new TestPageSerializer { DisposeHook = () => throw failure };
            var secondDisposalCount = 0;
            var secondSerializer = new TestPageSerializer
            {
                DisposeHook = () =>
                {
                    secondDisposalCount++;
                    if (secondClientFails) throw new IOException("Injected second serializer disposal failure");
                }
            };
            var (firstClient, firstSession, _) = await CreateClientWithPages(manager, storage, "first", 0, firstSerializer);
            var (secondClient, secondSession, _) = await CreateClientWithPages(manager, storage, "second", 0, secondSerializer);
            try
            {
                var error = Record.Exception(manager.Dispose);
                Assert.Same(failure, error);
                Assert.True(firstSession.Disposed);
                // One client failure must not skip remaining cleanup.
                Assert.True(secondSession.Disposed);
                Assert.Equal(1, secondDisposalCount);
                Assert.False(manager.Initialized);
                Assert.Null(Record.Exception(manager.Dispose));
                Assert.Equal(1, secondDisposalCount);

                await manager.InitializeAsync().WaitAsync(Timeout);
                var (newFirstClient, _, _) = await CreateClientWithPages(manager, storage, "first", 0);
                var (newSecondClient, _, _) = await CreateClientWithPages(manager, storage, "second", 0);
                Assert.NotSame(firstClient, newFirstClient);
                Assert.NotSame(secondClient, newSecondClient);
            }
            finally
            {
                firstSerializer.DisposeHook = null;
                secondSerializer.DisposeHook = null;
                manager.Dispose();
            }
        }

        [Fact]
        public async Task CommitReturnsWhilePagesAreStillBeingWritten()
        {
            var (manager, storage) = await CreateManager("returns");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "returns", 8);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            Assert.All(keys, k => Assert.Equal(0, session.WriteCount(k)));

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            Assert.All(keys, k => Assert.Equal(1, session.WriteCount(k)));
            for (int i = 0; i < keys.Count; i++)
            {
                Assert.Equal(i, ReadPersisted(storage, keys[i]));
            }
            manager.Dispose();
        }

        [Fact]
        public async Task FetchOfPendingPageWritesTheCommittedContentFirst()
        {
            var (manager, storage) = await CreateManager("fetch");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "fetch", 8);
            using var gate = new WalkGate(manager);
            var key = keys[^1];

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            // The fetch writes the page and hands it out, the write after it belongs to the next commit.
            var page = await client.GetValue(key).AsTask().WaitAsync(Timeout);
            Assert.NotNull(page);
            Assert.Equal(1, session.WriteCount(key));
            page.Value = 999;
            client.AddOrUpdate(key, page);
            page.Return();

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            Assert.Equal(1, session.WriteCount(key));
            Assert.Equal(7, ReadPersisted(storage, key));

            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(2, session.WriteCount(key));
            Assert.Equal(999, ReadPersisted(storage, key));
            manager.Dispose();
        }

        [Fact]
        public async Task TryGetCachedValueDeclinesAPendingPage()
        {
            var (manager, storage) = await CreateManager("trycached");
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "trycached", 4);
            using var gate = new WalkGate(manager);
            var key = keys[^1];

            Assert.True(client.TryGetCachedValue(key, out var before));
            before!.Return();

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            Assert.False(client.TryGetCachedValue(key, out _));

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            Assert.True(client.TryGetCachedValue(key, out var after));
            after!.Return();
            manager.Dispose();
        }

        /// <summary>
        /// A reference kept without a rent is invisible to Commit, writing through it is refused.
        /// </summary>
        [Fact]
        public async Task WriteToPendingPageWithoutRentThrows()
        {
            var (manager, storage) = await CreateManager("held");
            using var storageLifetime = storage;
            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                "held",
                new StateClientOptions<TestPage>() { ValueSerializer = new TestPageSerializer() },
                GlobalMemoryManager.Instance);
            var key = client.GetNewPageId();
            var page = new TestPage(1);
            client.AddOrUpdate(key, page);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            Assert.Throws<InvalidOperationException>(() => client.AddOrUpdate(key, page));

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// A page someone holds is written before Commit returns, so it is never pending.
        /// </summary>
        [Fact]
        public async Task HeldPageIsWrittenBeforeCommitReturns()
        {
            var (manager, storage) = await CreateManager("heldwrite");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "heldwrite", 4);
            using var gate = new WalkGate(manager);
            var key = keys[^1];
            var held = await client.GetValue(key);
            Assert.NotNull(held);

            await client.Commit().AsTask().WaitAsync(Timeout);
            Assert.Equal(1, session.WriteCount(key));
            held.Value = 999;
            client.AddOrUpdate(key, held);

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(1, session.WriteCount(key));
            Assert.Equal(3, ReadPersisted(storage, key));
            held.Return();
            manager.Dispose();
        }

        /// <summary>
        /// With background commit off, Commit returns with every page in the session.
        /// </summary>
        [Fact]
        public async Task InlineCommitWritesEveryPageBeforeReturning()
        {
            var (manager, storage) = await CreateManager("inline", backgroundCommit: false);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "inline", 8);

            await client.Commit().AsTask().WaitAsync(Timeout);

            Assert.All(keys, k => Assert.Equal(1, session.WriteCount(k)));
            Assert.True(((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync().IsCompletedSuccessfully);
            for (int i = 0; i < keys.Count; i++)
            {
                var bytes = await session.Read(keys[i]);
                Assert.Equal(i, BinaryPrimitives.ReadInt32LittleEndian(bytes.Span));
            }
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.All(keys, key => Assert.True(storage.TryGetValue(key, out _)));
            manager.Dispose();
        }

        #if DEBUG
        [Fact(Skip = "Allocation assertions require a Release build; Debug allocates async state machines.")]
        #else
        [Fact]
        #endif
        public async Task WarmInlineIdleCommitsAllocateNothing()
        {
            var (manager, storage) = await CreateManager("reusedcommit", backgroundCommit: false);
            using var storageLifetime = storage;
            using (manager)
            {
                var (client, _, _) = await CreateClientWithPages(manager, storage, "reusedcommit", 0);
                for (int i = 0; i < 32; i++)
                {
                    await client.Commit();
                }

                // Inline idle commits isolate generation bookkeeping from scheduler and page IO.
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < 256; i++)
                {
                    var commit = client.Commit();
                    Assert.True(commit.IsCompletedSuccessfully);
                }
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.Equal(0, allocated);
            }
        }

        /// <summary>
        /// The worker still owns its buffers after publishing its session commit.
        /// </summary>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task NextCommitJoinsWorkerCleanupBeforeReusingGeneration(bool failCleanup)
        {
            var (manager, storage) = await CreateManager($"cleanup_join_{failCleanup}");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            using var release = new ManualResetEventSlim(false);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var failure = new IOException("Injected cleanup failure");
            var serializer = new TestPageSerializer
            {
                ClearTemporaryAllocationsHook = () =>
                {
                    entered.TrySetResult();
                    if (!release.Wait(Timeout)) throw new TimeoutException("Cleanup was not released");
                    if (failCleanup) throw failure;
                }
            };
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "cleanup", 1, serializer);
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await entered.Task.WaitAsync(Timeout);
                var nextCommit = client.Commit().AsTask();
                Assert.False(nextCommit.IsCompleted);
                Assert.True(manager.HasCommitsInFlight);
                release.Set();

                if (failCleanup)
                {
                    Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() => nextCommit.WaitAsync(Timeout)));
                    Assert.True(((StateClient)client).HasCommitFault);
                    Assert.Same(failure, ((StateClient)client).CommitFault);
                }
                else
                {
                    await nextCommit.WaitAsync(Timeout);
                    await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                    Assert.Equal(0, ReadPersisted(storage, keys[0]));
                }
            }
            finally
            {
                release.Set();
            }
        }

        [Fact]
        public async Task BackgroundCommitCleanupDoesNotOverlapEvictionSerialization()
        {
            var (manager, storage) = await CreateManager("review_cleanup_overlap", backgroundCommit: true);
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            using var finishCleanup = new ManualResetEventSlim(false);
            var cleanupEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cleanupActive = 0;
            var serializationOverlapped = false;
            var serializer = new TestPageSerializer
            {
                ClearTemporaryAllocationsHook = () =>
                {
                    Interlocked.Exchange(ref cleanupActive, 1);
                    cleanupEntered.TrySetResult();
                    if (!finishCleanup.Wait(Timeout)) throw new TimeoutException("Cleanup was not released");
                    Interlocked.Exchange(ref cleanupActive, 0);
                },
                SerializeHook = _ =>
                {
                    if (Volatile.Read(ref cleanupActive) != 0) serializationOverlapped = true;
                }
            };
            var (client, _, _) = await CreateClientWithPages(manager, storage, "pages", 2, serializer);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            Task? worker = null;
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                worker = sync.WaitForCommitAsync();
                await cleanupEntered.Task.WaitAsync(Timeout);
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(3));
                Assert.True(manager.TryPeekCacheEntry(key, out var entry));

                await sync.Evict(new List<(S3FifoCacheEntry, long)> { (entry, entry.Version) }, false).WaitAsync(Timeout);

                finishCleanup.Set();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                // Cleanup must not overlap with page serialization.
                Assert.False(serializationOverlapped);
            }
            finally
            {
                finishCleanup.Set();
                if (worker != null) await worker.WaitAsync(Timeout);
            }
        }

        [Fact]
        public async Task EvictionCleanupDoesNotOverlapOnFetchSerialization()
        {
            var (manager, storage) = await CreateManager("review_eviction_cleanup_overlap");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            using var finishCleanup = new ManualResetEventSlim(false);
            var cleanupEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cleanupArmed = 1;
            var cleanupActive = 0;
            var serializationOverlapped = false;
            var serializer = new TestPageSerializer
            {
                ClearTemporaryAllocationsHook = () =>
                {
                    if (Interlocked.Exchange(ref cleanupArmed, 0) == 0) return;
                    Volatile.Write(ref cleanupActive, 1);
                    cleanupEntered.TrySetResult();
                    if (!finishCleanup.Wait(Timeout)) throw new TimeoutException("Cleanup was not released");
                    Volatile.Write(ref cleanupActive, 0);
                },
                SerializeHook = _ =>
                {
                    if (Volatile.Read(ref cleanupActive) != 0) serializationOverlapped = true;
                }
            };
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "pages", 2, serializer);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            using var walkGate = new WalkGate(manager);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await walkGate.Blocked.WaitAsync(Timeout);
            var newKey = client.GetNewPageId();
            client.AddOrUpdate(newKey, new TestPage(99));
            Assert.True(manager.TryPeekCacheEntry(newKey, out var entry));
            var eviction = Task.Run(() => sync.Evict(new List<(S3FifoCacheEntry, long)> { (entry, entry.Version) }, true));
            Task<TestPage?>? fetch = null;
            try
            {
                await cleanupEntered.Task.WaitAsync(Timeout);
                fetch = client.GetValue(keys[0]).AsTask();
                finishCleanup.Set();
                await eviction.WaitAsync(Timeout);
                var page = await fetch.WaitAsync(Timeout);
                Assert.NotNull(page);
                Assert.Equal(0, page.Value);
                page.Return();
                fetch = null;
                walkGate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                Assert.Equal(1, session.WriteCount(keys[0]));
                // Eviction cleanup must not overlap pending page serialization.
                Assert.False(serializationOverlapped);
            }
            finally
            {
                finishCleanup.Set();
                walkGate.Release();
                await eviction.WaitAsync(Timeout);
                if (fetch != null) (await fetch.WaitAsync(Timeout))?.Return();
                await sync.WaitForCommitAsync().WaitAsync(Timeout);
            }
        }

        /// <summary>
        /// A pending page has no holder, so a delete leaves it cached until the walk has written it.
        /// </summary>
        [Fact]
        public async Task DeleteOfPendingPageIsHeldUntilItsWrite()
        {
            var (manager, storage) = await CreateManager("delete");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "delete", 8);
            using var gate = new WalkGate(manager);
            var key = keys[^1];

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            client.Delete(key);
            Assert.True(manager.CacheTable.TryPeekEntry(key, out _), "the delete must not drop a page that still owes its checkpoint write");

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            Assert.Equal(1, session.WriteCount(key));
            Assert.Equal(7, ReadPersisted(storage, key));
            Assert.False(manager.CacheTable.TryPeekEntry(key, out _), "the held back delete must run once the page is written");

            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.False(storage.TryGetValue(key, out _));
            manager.Dispose();
        }

        [Fact]
        public async Task EvictionSpillsAPendingPageForItsCommit()
        {
            var (manager, storage) = await CreateManager("evict", cachePageCount: 0);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "evict", 8);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            // Without read cache a clean page is dropped on eviction, a pending one must be spilled instead.
            await manager.CacheTable.ForceCleanup();
            Assert.All(keys, k => Assert.False(manager.CacheTable.TryPeekEntry(k, out _)));

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            for (int i = 0; i < keys.Count; i++)
            {
                Assert.Equal(1, session.WriteCount(keys[i]));
                Assert.Equal(i, ReadPersisted(storage, keys[i]));
            }
            manager.Dispose();
        }

        [Fact]
        public async Task CheckpointWaitsForTheBackgroundCommit()
        {
            var (manager, storage) = await CreateManager("checkpoint");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "checkpoint", 4);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var checkpoint = manager.CheckpointAsync().AsTask();
            await Task.Delay(200);
            Assert.False(checkpoint.IsCompleted, "the checkpoint completed before the pages were written");

            gate.Release();
            await checkpoint.WaitAsync(Timeout);
            Assert.All(keys, k => Assert.Equal(1, session.WriteCount(k)));
            manager.Dispose();
        }

        [Fact]
        public async Task BackgroundCommitFailureSurfacesAtTheCheckpoint()
        {
            var (manager, storage) = await CreateManager("fault");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "fault", 4);
            session.FaultingKeys[keys[1]] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);

            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));
            await Assert.ThrowsAsync<IOException>(() => client.Commit().AsTask().WaitAsync(Timeout));
            manager.Dispose();
        }

        [Fact]
        public async Task RecoveryWaitsForTheBackgroundCommit()
        {
            var (manager, storage) = await CreateManager("recover");
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "recover", 4);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var recovery = manager.InitializeAsync();
            await Task.Delay(200);
            Assert.False(recovery.IsCompleted, "recovery started its reset under a commit still in flight");

            gate.Release();
            await recovery.WaitAsync(Timeout);
            manager.Dispose();
        }

        [Fact]
        public async Task DisposeGivesUpAnInFlightCommit()
        {
            var (manager, storage) = await CreateManager("dispose");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "dispose", 8);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var dispose = Task.Run(() => manager.Dispose());
            gate.Release();
            await dispose.WaitAsync(Timeout);
        }


        private static StateManagerSync<StateManagerMetadata> CreateReservoirManager(string name, IStreamMemoryManager? memoryManager = null)
        {
            return new StateManagerSync<StateManagerMetadata>(new StateManagerOptions()
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new MemoryFileProvider()
                }),
                CachePageCount = 1000,
                MinCachePageCount = 100,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/bgcommit_{name}/temp" }
            }, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter($"bgcommit_{name}"), $"bgcommit_{name}", memoryManager ?? GlobalMemoryManager.Instance);
        }

        private static ValueTask<IFlowtideQueue<long, PrimitiveListValueContainer<long>>> CreateQueue(StateManagerSync manager, int? pageSizeBytes = null)
        {
            return CreateQueue(manager, GlobalMemoryManager.Instance, pageSizeBytes);
        }

        private static ValueTask<IFlowtideQueue<long, PrimitiveListValueContainer<long>>> CreateQueue(StateManagerSync manager, IMemoryAllocator allocator, int? pageSizeBytes)
        {
            return manager.GetOrCreateClient("node").GetOrCreateQueue("queue", new FlowtideQueueOptions<long, PrimitiveListValueContainer<long>>()
            {
                MemoryAllocator = allocator,
                ValueSerializer = new PrimitiveListValueContainerSerializer<long>(allocator),
                PageSizeBytes = pageSizeBytes
            });
        }

        internal static ValueTask<IAppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>> CreateAppendTree(StateManagerSync manager)
        {
            return manager.GetOrCreateClient("node").GetOrCreateAppendTree("append", new BPlusTreeOptions<long, long, ListKeyContainer<long>, ListValueContainer<long>>()
            {
                BucketSize = 16,
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<long>(new LongSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            });
        }

        /// <summary>
        /// The queue keeps its right page across Commit, an enqueue right after must neither trip
        /// the guard nor leak into the checkpoint.
        /// </summary>
        [Fact]
        public async Task QueueEnqueueAfterCommitStaysOutOfTheCheckpoint()
        {
            var manager = CreateReservoirManager("queue");
            await manager.InitializeAsync();
            var queue = await CreateQueue(manager);
            for (long i = 0; i < 5000; i++)
            {
                await queue.Enqueue(i);
            }
            using var gate = new WalkGate(manager);

            await queue.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);
            for (long i = 5000; i < 5100; i++)
            {
                await queue.Enqueue(i);
            }

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            manager.Dispose();
            await manager.InitializeAsync();
            var recovered = await CreateQueue(manager);
            Assert.Equal(5000, recovered.Count);
            for (long i = 0; i < 5000; i++)
            {
                Assert.Equal(i, await recovered.Dequeue());
            }
            manager.Dispose();
        }

        /// <summary>
        /// Dequeue deletes and disposes the drained left page. Drained right after Commit, that
        /// page still owes its checkpoint write.
        /// </summary>
        [Fact]
        public async Task QueueDequeueAfterCommitKeepsTheDrainedPagesInTheCheckpoint()
        {
            var manager = CreateReservoirManager("dequeue");
            await manager.InitializeAsync();
            var queue = await CreateQueue(manager, pageSizeBytes: 256);
            for (long i = 0; i < 5000; i++)
            {
                await queue.Enqueue(i);
            }
            using var gate = new WalkGate(manager);

            await queue.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);
            for (long i = 0; i < 2500; i++)
            {
                Assert.Equal(i, await queue.Dequeue());
            }

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            manager.Dispose();
            await manager.InitializeAsync();
            var recovered = await CreateQueue(manager, pageSizeBytes: 256);
            Assert.Equal(5000, recovered.Count);
            for (long i = 0; i < 5000; i++)
            {
                Assert.Equal(i, await recovered.Dequeue());
            }
            manager.Dispose();
        }

        /// <summary>
        /// Same for the append tree's held right leaf.
        /// </summary>
        [Fact]
        public async Task AppendAfterCommitStaysOutOfTheCheckpoint()
        {
            var manager = CreateReservoirManager("append");
            await manager.InitializeAsync();
            var tree = await CreateAppendTree(manager);
            for (long i = 0; i < 5000; i++)
            {
                await tree.Append(i, i);
            }
            using var gate = new WalkGate(manager);

            await tree.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);
            for (long i = 5000; i < 5100; i++)
            {
                await tree.Append(i, i);
            }

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            manager.Dispose();
            await manager.InitializeAsync();
            var recovered = await CreateAppendTree(manager);
            using var iterator = recovered.CreateIterator();
            await iterator.Seek(0);
            long expected = 0;
            await foreach (var kv in iterator)
            {
                Assert.Equal(expected, kv.Key);
                Assert.Equal(expected, kv.Value);
                expected++;
            }
            Assert.Equal(5000, expected);
            manager.Dispose();
        }
        // Review findings, one red test each, kept red until the fix lands.

        /// <summary>
        /// Finding 1: the walk's session writes must never overlap the operator's session reads,
        /// the SqlServer and FasterKV sessions are not safe for it.
        /// </summary>
        [Fact]
        public async Task SessionReadNeverOverlapsAWalkWrite()
        {
            var (manager, storage) = await CreateManager("sessionoverlap", cachePageCount: 0, threadSafeSession: false);
            using var storageLifetime = storage;
            var (client, session, oldKeys) = await CreateClientWithPages(manager, storage, "sessionoverlap", 8);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            // Clean pages are dropped without read cache, so the next read of them goes to the session.
            await manager.CacheTable.ForceCleanup();
            Assert.All(oldKeys, k => Assert.False(manager.CacheTable.TryPeekEntry(k, out _)));

            var newKeys = new HashSet<long>();
            for (int i = 0; i < 8; i++)
            {
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(100 + i));
                newKeys.Add(key);
            }
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, newKeys);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            // The walk is inside a session write, an operator read of a committed page now.
            var read = client.GetValue(oldKeys[0]).AsTask();
            await Task.Delay(200);

            gate.Set();
            var page = await read.WaitAsync(Timeout);
            page!.Return();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.False(session.ReadDuringWrite, "a session read ran while the walk was inside a session write");
            manager.Dispose();
        }

        /// <summary>
        /// A session that declares itself thread-safe pays no lock, a read completes while the walk writes.
        /// </summary>
        [Fact]
        public async Task ThreadSafeSessionReadDoesNotWaitForAWalkWrite()
        {
            var (manager, storage) = await CreateManager("sessionfree", cachePageCount: 0, threadSafeSession: true);
            using var storageLifetime = storage;
            var (client, session, oldKeys) = await CreateClientWithPages(manager, storage, "sessionfree", 8);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            await manager.CacheTable.ForceCleanup();

            var newKeys = new HashSet<long>();
            for (int i = 0; i < 8; i++)
            {
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(100 + i));
                newKeys.Add(key);
            }
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, newKeys);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            var page = await client.GetValue(oldKeys[0]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            page!.Return();
            Assert.True(session.ReadDuringWrite);

            gate.Set();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// Finding 2: a structure clearing itself must not swallow a failed walk, the checkpoint
        /// would seal pages that were never written.
        /// </summary>
        [Fact]
        public async Task ResetAfterAFailedWalkStillSurfacesTheFailure()
        {
            var (manager, storage) = await CreateManager("resetfault");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "resetfault", 8);
            session.FaultingKeys[keys[1]] = 1;
            var versionBefore = manager.LastCompletedCheckpointVersion;

            await client.Commit().AsTask().WaitAsync(Timeout);

            await Assert.ThrowsAsync<IOException>(() => client.Reset(true).AsTask().WaitAsync(Timeout));
            AssertCausedBy<IOException>(await Record.ExceptionAsync(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout)));
            Assert.Equal(versionBefore, manager.LastCompletedCheckpointVersion);
            manager.Dispose();
        }

        /// <summary>
        /// A reset while the checkpoint still joins an earlier client must not let it seal the failed walk.
        /// </summary>
        [Fact]
        public async Task ResetDuringTheCheckpointJoinDoesNotLetTheCheckpointSealAFailedWalk()
        {
            var (manager, storage) = await CreateManager("resetrace");
            using var storageLifetime = storage;
            // Joined first, the dictionary keeps insertion order.
            var (first, _, _) = await CreateClientWithPages(manager, storage, "resetrace_first", 2);
            var (second, secondSession, secondKeys) = await CreateClientWithPages(manager, storage, "resetrace_second", 4);
            secondSession.FaultingKeys[secondKeys[1]] = 1;
            var versionBefore = manager.LastCompletedCheckpointVersion;

            var firstBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.PageWriteHookForTests = (name, key) =>
            {
                if (name != "resetrace_first")
                {
                    return Task.CompletedTask;
                }
                firstBlocked.TrySetResult();
                return releaseFirst.Task;
            };
            try
            {
                await first.Commit().AsTask().WaitAsync(Timeout);
                await firstBlocked.Task.WaitAsync(Timeout);
                await second.Commit().AsTask().WaitAsync(Timeout);

                var checkpoint = manager.CheckpointAsync().AsTask();
                await Assert.ThrowsAsync<IOException>(() => second.Reset(true).AsTask().WaitAsync(Timeout));
                Assert.False(checkpoint.IsCompleted, "the checkpoint must still be joining the first client");

                releaseFirst.TrySetResult();
                AssertCausedBy<IOException>(await Record.ExceptionAsync(() => checkpoint.WaitAsync(Timeout)));
                Assert.Equal(versionBefore, manager.LastCompletedCheckpointVersion);
            }
            finally
            {
                releaseFirst.TrySetResult();
                manager.PageWriteHookForTests = null;
                manager.Dispose();
            }
        }

        /// <summary>
        /// Finding 3: a page whose fetch-time write failed is still owed, the walk must not
        /// commit the generation without it.
        /// </summary>
        [Fact]
        public async Task FailedOnFetchWriteFaultsTheWalk()
        {
            var (manager, storage) = await CreateManager("fetchfault");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "fetchfault", 8);
            using var gate = new WalkGate(manager);
            var key = keys[^1];
            session.FaultingKeys[key] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            await Assert.ThrowsAsync<IOException>(() => client.GetValue(key).AsTask().WaitAsync(Timeout));

            gate.Release();
            await Assert.ThrowsAnyAsync<Exception>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));
            manager.Dispose();
        }

        /// <summary>
        /// Finding 4: a walk that dispose gave up must not look like a landed commit to a
        /// checkpoint that was already waiting for it.
        /// </summary>
        [Fact]
        public async Task CheckpointDoesNotSealAWalkThatDisposeGaveUp()
        {
            var (manager, storage) = await CreateManager("disposeseal");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "disposeseal", 8);
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, keys.ToHashSet());

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            var checkpoint = manager.CheckpointAsync().AsTask();
            var dispose = Task.Run(() => manager.Dispose());
            await Task.Delay(200);
            Assert.False(checkpoint.IsCompleted);

            // The walk finishes this page, sees the dispose and gives the rest up.
            gate.Set();
            await dispose.WaitAsync(Timeout);
            await Assert.ThrowsAnyAsync<Exception>(() => checkpoint.WaitAsync(Timeout));
        }

        /// <summary>
        /// Finding 5: a walk still running when the manager is disposed gives up on the stop or the torn down table.
        /// </summary>
        [Fact]
        public async Task WalkGivesUpWhenTheManagerIsDisposedUnderIt()
        {
            var (manager, storage) = await CreateManager("disposetable");
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "disposetable", 8);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var walk = ((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync();
            manager.Dispose();

            gate.Release();
            var fault = await Record.ExceptionAsync(() => walk.WaitAsync(Timeout));
            Assert.True(fault is OperationCanceledException || fault is ObjectDisposedException, "the walk ran into the torn down cache table: " + fault);
        }

        /// <summary>
        /// Finding 6: a page held across Commit and changed in place must not leak the change
        /// into the checkpoint, whether or not the holder ever calls AddOrUpdate.
        /// </summary>
        [Fact]
        public async Task HeldPageChangedInPlaceAfterCommitStaysOutOfTheCheckpoint()
        {
            var (manager, storage) = await CreateManager("heldinplace");
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "heldinplace", 8);
            using var gate = new WalkGate(manager);
            var key = keys[^1];
            var held = await client.GetValue(key);
            Assert.NotNull(held);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            held.Value = 999;

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            Assert.Equal(7, ReadPersisted(storage, key));
            held.Return();
            manager.Dispose();
        }

        /// <summary>
        /// Finding 7: Pop makes the held left page the right page again without a fetch, an
        /// enqueue then writes into a page that still owes its checkpoint copy.
        /// </summary>
        [Fact]
        public async Task QueuePopThenEnqueueAfterCommitKeepsTheLeftPageInTheCheckpoint()
        {
            var manager = CreateReservoirManager("popenqueue");
            await manager.InitializeAsync();
            var queue = await CreateQueue(manager, pageSizeBytes: 256);
            for (long i = 0; i < 5000; i++)
            {
                await queue.Enqueue(i);
            }
            using var gate = new WalkGate(manager);

            await queue.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);
            while (queue.Count > 1)
            {
                await queue.Pop();
            }
            await queue.Enqueue(999999);

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            manager.Dispose();
            await manager.InitializeAsync();
            var recovered = await CreateQueue(manager, pageSizeBytes: 256);
            Assert.Equal(5000, recovered.Count);
            for (long i = 0; i < 5000; i++)
            {
                Assert.Equal(i, await recovered.Dequeue());
            }
            manager.Dispose();
        }

        /// <summary>
        /// Finding 9: a delete of a page that owes its checkpoint copy must not park the
        /// operator behind whoever holds the commit lock.
        /// </summary>
        [Fact]
        public async Task DeleteOfPendingPageDoesNotWaitForTheWalk()
        {
            var (manager, storage) = await CreateManager("deletewait");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "deletewait", 8);
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, keys.ToHashSet());
            var key = keys[^1];

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            // The walk holds the commit lock inside a session write of another page.
            var delete = Task.Run(() => client.Delete(key));
            var finished = await Task.WhenAny(delete, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.True(ReferenceEquals(finished, delete), "Delete blocked behind the walk's page write");

            gate.Set();
            await delete.WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(7, ReadPersisted(storage, key));
            manager.Dispose();
        }

        // Second review, one red test each.

        /// <summary>
        /// A synchronization context that never runs what is posted to it.
        /// </summary>
        private sealed class BlackHoleSynchronizationContext : SynchronizationContext
        {
            public override void Post(SendOrPostCallback d, object? state)
            {
            }

            public override void Send(SendOrPostCallback d, object? state)
            {
            }
        }

        /// <summary>
        /// Review 2, finding 2: a read of a temporary location must not run while the writer rolls
        /// its file, the roll shifts the segment indices the read measures its bytes with.
        /// </summary>
        [Fact]
        public async Task ReservoirReadDoesNotOverlapAFileRoll()
        {
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
            {
                FileProvider = new MemoryFileProvider(),
                MaxFileSize = 1024
            });
            var manager = new StateManagerSync<StateManagerMetadata>(new StateManagerOptions()
            {
                PersistentStorage = storage,
                CachePageCount = 1000,
                MinCachePageCount = 100,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = "./data/bgcommit_roll/temp" }
            }, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("bgcommit_roll"), "bgcommit_roll", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            var session = (ReservoirPersistentSession)storage.CreateSession();

            await session.Write(1000, new SerializableObject(new byte[64]));

            using var rolling = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            session.FileRollHookForTests = () =>
            {
                rolling.Set();
                release.Wait();
            };
            var roll = Task.Run(() => session.Write(1001, new SerializableObject(new byte[2048])));
            Assert.True(rolling.Wait(Timeout));

            // The roll is open, a read of the earlier page must wait for it.
            var read = Task.Run(async () => await session.Read(1000));
            await Task.Delay(200);
            Assert.False(read.IsCompleted, "a read ran while the writer was rolling its file");

            release.Set();
            await roll.WaitAsync(Timeout);
            var bytes = await read.WaitAsync(Timeout);
            Assert.Equal(64, bytes.Length);
            manager.Dispose();
        }

        /// <summary>
        /// Review 2, finding 3: a held-page write that fails at Commit must not leave the client
        /// believing its metadata was committed, recovery reads a page that never landed.
        /// </summary>
        [Fact]
        public async Task HeldPageWriteFailureDoesNotStrandTheClientMetadata()
        {
            var (manager, storage) = await CreateManager("heldfault", reservoir: true);
            using var storageLifetime = storage;
            // A checkpoint before the client exists, so recovery reads client metadata instead of clearing it.
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "heldfault", 4);
            var key = keys[^1];
            var held = await client.GetValue(key);
            Assert.NotNull(held);
            session.FaultingKeys[key] = 1;

            await Assert.ThrowsAsync<IOException>(() => client.Commit().AsTask().WaitAsync(Timeout));
            held.Return();

            await manager.InitializeAsync().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// Review 2, finding 4: dispose gives a wedged walk up instead of waiting on storage.
        /// </summary>
        [Fact]
        public async Task DisposeDoesNotWaitForAWedgedWalk()
        {
            var (manager, storage) = await CreateManager("wedged");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "wedged", 8);
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, keys.ToHashSet());

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            // Returns while the write is still parked.
            await Task.Run(() => manager.Dispose()).WaitAsync(TimeSpan.FromSeconds(10));
            gate.Set();
            await Record.ExceptionAsync(() => ((StateClient)client).WaitForCommitAsync().WaitAsync(Timeout));
            await WaitUntil(() => session.Disposed, "the abandoned worker to release its session");
        }

        /// <summary>
        /// Review 2, finding 4: the join inside dispose must not need the caller's synchronization context.
        /// </summary>
        [Fact]
        public async Task DisposeDoesNotNeedTheCallersSynchronizationContext()
        {
            var (manager, storage) = await CreateManager("synccontext");
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "synccontext", 8);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var disposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var thread = new Thread(() =>
            {
                SynchronizationContext.SetSynchronizationContext(new BlackHoleSynchronizationContext());
                try
                {
                    manager.Dispose();
                    disposed.TrySetResult();
                }
                catch (Exception e)
                {
                    disposed.TrySetException(e);
                }
            });
            thread.IsBackground = true;
            thread.Start();
            await Task.Delay(300);
            gate.Release();

            await disposed.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }

        /// <summary>
        /// Review 2, finding 5: eviction must get its turn between the walk's pages.
        /// </summary>
        [Fact]
        public async Task EvictionRunsWhileTheWalkIsInFlight()
        {
            var (manager, storage) = await CreateManager("evictwalk", cachePageCount: 0);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "evictwalk", 400);
            session.WriteDelay = TimeSpan.FromMilliseconds(1);

            await client.Commit().AsTask().WaitAsync(Timeout);
            var walk = ((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync();
            Assert.NotNull(walk);

            var evictedDuringWalk = false;
            while (!walk.IsCompleted)
            {
                await manager.CacheTable.ForceCleanup();
                if (keys.Any(k => !manager.CacheTable.TryPeekEntry(k, out _)) && !walk.IsCompleted)
                {
                    evictedDuringWalk = true;
                    break;
                }
            }
            Assert.True(evictedDuringWalk, "no page of the client was evicted while its walk was running");

            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// A page whose fetch-time write failed still owes its write, and the generation is
        /// failed there and then: the walk writes nothing more and reports that failure, a retry
        /// could land after a half-written page in the reservoir's file.
        /// </summary>
        [Fact]
        public async Task FailedOnFetchWriteFailsTheGenerationWithoutRetrying()
        {
            var (manager, storage) = await CreateManager("fetchowed");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "fetchowed", 8);
            using var gate = new WalkGate(manager);
            var key = keys[^1];
            session.FaultingKeys[key] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            await Assert.ThrowsAsync<IOException>(() => client.GetValue(key).AsTask().WaitAsync(Timeout));
            Assert.False(client.TryGetCachedValue(key, out _), "a page whose write failed must still be owed");

            session.FaultingKeys.TryRemove(key, out _);
            gate.Release();
            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));
            Assert.Equal(0, session.WriteCount(keys[0]));
            Assert.False(client.TryGetCachedValue(key, out _));
            manager.Dispose();
        }

        /// <summary>
        /// Review 2, finding 10: Clear must forget the old tree's right spine.
        /// </summary>
        [Fact]
        public async Task AppendTreeClearForgetsTheOldRightSpine()
        {
            var manager = CreateReservoirManager("appendclear");
            await manager.InitializeAsync();
            var tree = await CreateAppendTree(manager);
            for (long i = 0; i < 200; i++)
            {
                await tree.Append(i, i);
            }

            await tree.Clear();
            for (long i = 0; i < 100; i++)
            {
                await tree.Append(i, i);
            }

            using var iterator = tree.CreateIterator();
            await iterator.Seek(0);
            long expected = 0;
            await foreach (var kv in iterator)
            {
                Assert.Equal(expected, kv.Key);
                expected++;
            }
            Assert.Equal(100, expected);
            manager.Dispose();
        }

        // Third review, one red test each.

        /// <summary>
        /// Review 3: once a pending page was written, later reads of it must take the lock-free
        /// path again instead of asking the pending set on every fetch.
        /// </summary>
        [Fact]
        public async Task WrittenPendingPageLeavesTheLockedProbeBehind()
        {
            var (manager, storage) = await CreateManager("probe");
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "probe", 8);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            using var gate = new WalkGate(manager);
            var key = keys[^1];

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var page = await client.GetValue(key).AsTask().WaitAsync(Timeout);
            page!.Return();
            var hits = sync.LookupTableHitsForTests;
            for (int i = 0; i < 10; i++)
            {
                page = await client.GetValue(key).AsTask().WaitAsync(Timeout);
                page!.Return();
            }
            Assert.Equal(hits + 10, sync.LookupTableHitsForTests);

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// Review 3: Commit must not allocate generation-sized buffers on the operator thread.
        /// </summary>
        [Fact]
        public async Task CommitAllocatesNoGenerationSizedBuffers()
        {
            const int count = 8192;
            var (manager, storage) = await CreateManager("alloc", cachePageCount: count * 2);
            using var storageLifetime = storage;
            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                "alloc",
                new StateClientOptions<TestPage>() { ValueSerializer = new TestPageSerializer() },
                GlobalMemoryManager.Instance);
            var keys = new long[count];
            var pages = new TestPage[count];
            for (int i = 0; i < count; i++)
            {
                keys[i] = client.GetNewPageId();
                pages[i] = new TestPage(i);
                client.AddOrUpdate(keys[i], pages[i]);
            }

            long allocated = 0;
            for (int round = 0; round < 2; round++)
            {
                for (int i = 0; i < count; i++)
                {
                    client.AddOrUpdate(keys[i], pages[i]);
                }
                var before = GC.GetAllocatedBytesForCurrentThread();
                await client.Commit().AsTask().WaitAsync(Timeout);
                allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            }
            Assert.True(allocated < 16 * 1024, $"Commit allocated {allocated} bytes for {count} pages on the caller's thread");
            manager.Dispose();
        }

        /// <summary>
        /// Review 4: after a generation failed fast with its pages spilled, recovery leaves a client
        /// that reads and commits again.
        /// </summary>
        [Fact]
        public async Task RecoveryAfterAFailedWalkCommitsAgain()
        {
            var (manager, storage) = await CreateManager("failrecover", cachePageCount: 0, reservoir: true);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "failrecover", 8);
            using var gate = new WalkGate(manager);
            session.FaultingKeys[keys[1]] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);
            // Every pending page is spilled before the walk gets to write any of them.
            await manager.CacheTable.ForceCleanup();
            gate.Release();
            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            await manager.InitializeAsync().WaitAsync(Timeout);
            session.FaultingKeys.Clear();
            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(7));
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var page = await client.GetValue(key).AsTask().WaitAsync(Timeout);
            Assert.Equal(7, page!.Value);
            page.Return();
            Assert.Equal(7, ReadPersisted(storage, key));
            manager.Dispose();
        }

        /// <summary>
        /// Review 3: Clear must dispose a right leaf that never reached the cache.
        /// </summary>
        [Fact]
        public async Task AppendTreeClearDisposesAnUncachedRightLeaf()
        {
            var memory = new StreamMemoryManager("bgcommit_appendleak");
            var allocator = memory.CreateOperatorMemoryManager("tree");
            var manager = CreateReservoirManager("appendleak", memory);
            await manager.InitializeAsync();
            var tree = await manager.GetOrCreateClient("node").GetOrCreateAppendTree("append", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
            {
                BucketSize = 16,
                Comparer = new PrimitiveListComparer<long>(),
                KeySerializer = new PrimitiveListKeyContainerSerializer<long>(allocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<long>(allocator),
                MemoryAllocator = allocator
            });
            var baseline = memory.GetAllocatedMemory();

            // Past one bucket, the right leaf is a fresh node the cache has not seen yet.
            for (long i = 0; i < 40; i++)
            {
                await tree.Append(i, i);
            }
            await tree.Clear();

            Assert.Equal(baseline, memory.GetAllocatedMemory());
            manager.Dispose();
        }

        // Fourth review, one red test each.

        /// <summary>
        /// Review 4: a page the fast path hands out after its write must be writable, whatever
        /// the walk is still doing with the key.
        /// </summary>
        [Fact]
        public async Task WrittenPageCanBeWrittenBackWhileTheWalkFreesItsSpill()
        {
            var factory = new RecordingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions() { DirectoryPath = "./data/bgcommit_freewindow/temp" }));
            var (manager, storage) = await CreateManager("freewindow", fileCacheFactory: factory);
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "freewindow", 4);
            var cache = factory.Created.Single();
            using var freeGate = new ManualResetEventSlim(false);
            cache.ArmFreeGate(freeGate, keys[0]);
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                // The walk wrote the page and is now inside FreeSpill.
                await cache.FreeEntered.WaitAsync(Timeout);

                var fetch = client.GetValue(keys[0]);
                if (!fetch.IsCompleted)
                {
                    // Kept owed until the walk is done with it, let the walk finish.
                    freeGate.Set();
                }
                var page = await fetch.AsTask().WaitAsync(Timeout);
                Assert.NotNull(page);
                page.Value = 42;
                client.AddOrUpdate(keys[0], page);
                page.Return();
            }
            finally
            {
                freeGate.Set();
                manager.Dispose();
            }
        }

        /// <summary>
        /// Review 4: a walk the stop gave up on still owns the serializer and the session, they
        /// go when the walk ends and not while it may be writing through them.
        /// </summary>
        [Fact]
        public async Task DisposeLeavesTheSerializerAndSessionToTheWalkItGaveUp()
        {
            var serializer = new TestPageSerializer();
            var (manager, storage) = await CreateManager("latedispose", recoveryCommitWaitTimeout: TimeSpan.FromSeconds(1));
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "latedispose", 8, serializer);
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, keys.ToHashSet());

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);
            var walk = ((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync();
            Assert.NotNull(walk);

            await Task.Run(() => manager.Dispose()).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.False(walk.IsCompleted);
            Assert.False(serializer.Disposed, "the serializer was disposed under a walk still writing");
            Assert.False(session.Disposed, "the session was disposed under a walk still writing");

            gate.Set();
            try
            {
                await walk.WaitAsync(Timeout);
            }
            catch (Exception)
            {
            }
            await WaitUntil(() => serializer.Disposed && session.Disposed, "the walk to dispose what it was left with");
        }

        /// <summary>
        /// Review 4: a walk that gets the commit lock back after the stop gave it up exits there,
        /// it must not write the page it was about to take.
        /// </summary>
        [Fact]
        public async Task AbandonedWalkWritesNothingAfterItGetsTheLockBack()
        {
            var (manager, storage) = await CreateManager("relock", recoveryCommitWaitTimeout: TimeSpan.FromSeconds(1));
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "relock", 4);
            using var walkGate = new WalkGate(manager);
            using var writeGate = new ManualResetEventSlim(false);
            session.ArmWriteGate(writeGate, new HashSet<long> { keys[1] });

            await client.Commit().AsTask().WaitAsync(Timeout);
            await walkGate.Blocked.WaitAsync(Timeout);
            var walk = ((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync();
            Assert.NotNull(walk);
            // A fetch-time write holds the commit lock, blocked inside the session.
            var fetch = Task.Run(() => client.GetValue(keys[1]).AsTask());
            await session.WriterBlocked.WaitAsync(Timeout);

            await Task.Run(() => manager.Dispose()).WaitAsync(TimeSpan.FromSeconds(10));
            // The walk passed its dispose check before the gate, now it queues on the lock.
            walkGate.Release();
            writeGate.Set();
            try
            {
                (await fetch.WaitAsync(Timeout))?.Return();
            }
            catch (Exception)
            {
            }
            var failure = await Record.ExceptionAsync(() => walk.WaitAsync(Timeout));

            // A stop is a cancellation, the torn-down table throws disposed, so the two stay apart.
            Assert.IsType<OperationCanceledException>(failure);
            Assert.Equal(0, session.WriteCount(keys[0]));
        }

        /// <summary>
        /// Review 4: Clear must dispose the queue's own nodes, the same leak Clear had on the append tree.
        /// </summary>
        [Fact]
        public async Task QueueClearDisposesItsNodes()
        {
            var memory = new StreamMemoryManager("bgcommit_queueleak");
            var allocator = memory.CreateOperatorMemoryManager("queue");
            var manager = CreateReservoirManager("queueleak", memory);
            await manager.InitializeAsync();
            var queue = await CreateQueue(manager, allocator, pageSizeBytes: 256);
            var baseline = memory.GetAllocatedMemory();

            // Past one page, the right node is a fresh node the cache has not seen yet.
            for (long i = 0; i < 200; i++)
            {
                await queue.Enqueue(i);
            }
            await queue.Clear();

            Assert.Equal(baseline, memory.GetAllocatedMemory());
            manager.Dispose();
        }

        /// <summary>
        /// Review 4: the recovered queue rents its right node once too often, so even a correct
        /// Clear cannot dispose it.
        /// </summary>
        [Fact]
        public async Task QueueClearAfterRecoveryDisposesItsNodes()
        {
            var memory = new StreamMemoryManager("bgcommit_queuerecover");
            var allocator = memory.CreateOperatorMemoryManager("queue");
            var manager = CreateReservoirManager("queuerecover", memory);
            await manager.InitializeAsync();
            var queue = await CreateQueue(manager, allocator, pageSizeBytes: 256);
            for (long i = 0; i < 10; i++)
            {
                await queue.Enqueue(i);
            }
            await queue.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();

            await manager.InitializeAsync();
            var beforeRecovery = memory.GetAllocatedMemory();
            var recovered = await CreateQueue(manager, allocator, pageSizeBytes: 256);
            Assert.Equal(10, recovered.Count);
            Assert.True(memory.GetAllocatedMemory() > beforeRecovery, "the recovered node holds memory");

            await recovered.Clear();

            Assert.Equal(beforeRecovery, memory.GetAllocatedMemory());
            manager.Dispose();
        }

        /// <summary>
        /// Review 4: a second Commit after a failed one is refused before it touches the metadata,
        /// so CommitedOnce keeps saying what the store has.
        /// </summary>
        [Fact]
        public async Task RepeatedCommitAfterAFailedGenerationKeepsCommitedOnceFalse()
        {
            var (manager, storage) = await CreateManager("commitedonce");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "commitedonce", 4);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            var held = await client.GetValue(keys[^1]);
            Assert.NotNull(held);
            session.FaultingKeys[keys[^1]] = 1;

            await Assert.ThrowsAsync<IOException>(() => client.Commit().AsTask().WaitAsync(Timeout));
            Assert.False(sync.CommitedOnceForTests);

            await Assert.ThrowsAsync<InvalidOperationException>(() => client.Commit().AsTask().WaitAsync(Timeout));
            Assert.False(sync.CommitedOnceForTests, "the refused commit left CommitedOnce set for a metadata page that never landed");
            held.Return();
            manager.Dispose();
        }

        // Fifth review, one red test each.

        /// <summary>
        /// Review 5: a drain that gave the walks up is followed by a restart on the same clients,
        /// they must commit again afterwards.
        /// </summary>
        [Fact]
        public async Task RecoveryAfterAnAbandonedDrainCommitsAgain()
        {
            var (manager, storage) = await CreateManager("abandon", reservoir: true);
            using var storageLifetime = storage;
            var (client, _, _) = await CreateClientWithPages(manager, storage, "abandon", 8);
            using var gate = new WalkGate(manager);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            // The engine's drain ran out.
            manager.RequestStopCommits();
            gate.Release();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            // A failure restart initializes the same manager and the same clients again.
            await manager.InitializeAsync().WaitAsync(Timeout);
            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(5));
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(5, ReadPersisted(storage, key));
            manager.Dispose();
        }

        /// <summary>
        /// Review 5: a restart after the drain gave a wedged walk up must fail within the stop
        /// budget instead of joining the walk forever.
        /// </summary>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RecoveryDoesNotHangOnAWalkTheDrainGaveUp(bool disposeBeforeRecovery)
        {
            var (manager, storage) = await CreateManager($"wedgedrecover_{disposeBeforeRecovery}", reservoir: true, recoveryCommitWaitTimeout: TimeSpan.FromSeconds(1));
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "wedgedrecover", 8);
            using var writeGate = new ManualResetEventSlim(false);
            session.ArmWriteGate(writeGate, keys.ToHashSet());
            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);
            manager.RequestStopCommits();
            if (disposeBeforeRecovery) manager.Dispose();

            try
            {
                var failure = await Record.ExceptionAsync(() => manager.InitializeAsync().WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.IsType<InvalidOperationException>(failure);
                writeGate.Set();
                await manager.InitializeAsync().WaitAsync(Timeout);
            }
            finally
            {
                writeGate.Set();
            }
        }

        /// <summary>
        /// Review 5: an idle checkpoint in between must not cost the next active commit a fresh buffer.
        /// </summary>
        [Fact]
        public async Task CommitAllocatesNothingAfterAnIdleCommit()
        {
            const int count = 8192;
            var (manager, storage) = await CreateManager("idlealloc", cachePageCount: count * 2);
            using var storageLifetime = storage;
            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                "idlealloc",
                new StateClientOptions<TestPage>() { ValueSerializer = new TestPageSerializer() },
                GlobalMemoryManager.Instance);
            var keys = new long[count];
            var pages = new TestPage[count];
            for (int i = 0; i < count; i++)
            {
                keys[i] = client.GetNewPageId();
                pages[i] = new TestPage(i);
                client.AddOrUpdate(keys[i], pages[i]);
            }
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            // A run of idle checkpoints, longer than the shrink hysteresis.
            for (int i = 0; i < 10; i++)
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            }

            for (int i = 0; i < count; i++)
            {
                client.AddOrUpdate(keys[i], pages[i]);
            }
            var before = GC.GetAllocatedBytesForCurrentThread();
            await client.Commit().AsTask().WaitAsync(Timeout);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.True(allocated < 16 * 1024, $"Commit allocated {allocated} bytes right after an idle commit");
            manager.Dispose();
        }

        /// <summary>
        /// Review 5: a generation whose session commit failed is refused until a reset, even when
        /// a recovery consumed the fault and then gave up before the reset.
        /// </summary>
        [Fact]
        public async Task FailedSessionCommitRefusesTheNextCommitUntilReset()
        {
            var (manager, storage) = await CreateManager("sessionfail");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "sessionfail", 4);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            session.FaultCommit = true;
            await client.Commit().AsTask().WaitAsync(Timeout);
            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));
            session.FaultCommit = false;

            // Paused and resumed like a recovery that fails before it resets the clients.
            await sync.PauseCommitsAsync(Timeout).WaitAsync(Timeout);
            sync.ResumeCommits();

            await Assert.ThrowsAsync<InvalidOperationException>(() => client.Commit().AsTask().WaitAsync(Timeout));
            manager.Dispose();
        }

        /// <summary>
        /// A node the queue drops while a walk is in flight was either written at Commit (the queue
        /// held it) or on the fetch that made it the left node, so the walk never sees it disposed.
        /// </summary>
        [Fact]
        public async Task QueueDequeuePastNodesDuringTheWalkNeverHandsTheWalkADisposedNode()
        {
            var manager = CreateReservoirManager("dequeuewalk");
            await manager.InitializeAsync();
            var queue = await CreateQueue(manager, pageSizeBytes: 256);
            for (long i = 0; i < 200; i++)
            {
                await queue.Enqueue(i);
            }
            using var gate = new WalkGate(manager);
            await queue.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            // Drains past several nodes while the walk is parked, each drop is Delete then Dispose.
            for (long i = 0; i < 100; i++)
            {
                Assert.Equal(i, await queue.Dequeue());
            }

            gate.Release();
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();

            await manager.InitializeAsync();
            var recovered = await CreateQueue(manager, pageSizeBytes: 256);
            Assert.Equal(200, recovered.Count);
            for (long i = 0; i < 200; i++)
            {
                Assert.Equal(i, await recovered.Dequeue());
            }
            manager.Dispose();
        }

        /// <summary>
        /// A walk given up mid-page reaches the table after Dispose, it must see the stop and not a null.
        /// </summary>
        [Fact]
        public async Task DisposedManagerRefusesTableAccessAsDisposed()
        {
            var (manager, storage) = await CreateManager("tablegone");
            using var storageLifetime = storage;
            var (_, _, keys) = await CreateClientWithPages(manager, storage, "tablegone", 2);
            manager.Dispose();

            Assert.Throws<ObjectDisposedException>(() => manager.TryRentCacheEntryForCommit(keys[0], out _));
            Assert.Throws<ObjectDisposedException>(() => manager.DeleteFromCache(keys[0]));
            Assert.Throws<ObjectDisposedException>(() => manager.TryPeekCacheEntry(keys[0], out _));
        }

        // Sixth review, one red test each.

        /// <summary>
        /// Review 6: a recovery that starts under a walk must join the walk before it resets the
        /// sessions, or a page the walk writes afterwards lands in the new epoch's writer and the
        /// next checkpoint seals it over the checkpointed one.
        /// </summary>
        [Fact]
        public async Task RecoveryJoinsTheWalkBeforeItResetsTheSessions()
        {
            var (manager, storage) = await CreateManager("resetorder", reservoir: true);
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "resetorder", 4);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            // The next epoch changes two pages, the walk is parked at each of them in turn.
            foreach (var (key, value) in new[] { (keys[0], 42), (keys[1], 43) })
            {
                var page = await client.GetValue(key).AsTask().WaitAsync(Timeout);
                page!.Value = value;
                client.AddOrUpdate(key, page);
                page.Return();
            }
            var page0Reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var page0Go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var page1Reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var page1Go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.PageWriteHookForTests = async (_, key) =>
            {
                if (key == keys[0])
                {
                    page0Reached.TrySetResult();
                    await page0Go.Task;
                }
                else if (key == keys[1])
                {
                    page1Reached.TrySetResult();
                    await page1Go.Task;
                }
            };
            await client.Commit().AsTask().WaitAsync(Timeout);
            await page0Reached.Task.WaitAsync(Timeout);

            // A failure recovery starts while the walk is parked, then the walk writes one page and is given up.
            var recovery = manager.InitializeAsync();
            page0Go.SetResult();
            await page1Reached.Task.WaitAsync(Timeout);
            manager.RequestStopCommits();
            page1Go.SetResult();
            await recovery.WaitAsync(Timeout);
            manager.PageWriteHookForTests = null;

            // The recovered epoch changes a third page only and checkpoints.
            var page2 = await client.GetValue(keys[2]).AsTask().WaitAsync(Timeout);
            page2!.Value = 44;
            client.AddOrUpdate(keys[2], page2);
            page2.Return();
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            manager.Dispose();
            await manager.InitializeAsync().WaitAsync(Timeout);
            Assert.Equal(0, ReadPersisted(storage, keys[0]));
            Assert.Equal(44, ReadPersisted(storage, keys[2]));
            manager.Dispose();
        }

        /// <summary>
        /// Review 6: a fetch of a page that owes its write must not queue behind the eviction
        /// pass's flush, the lock is per page for the walk and must be for the pass too.
        /// </summary>
        [Fact]
        public async Task FetchOfAnOwedPageDoesNotWaitForTheEvictionFlush()
        {
            var factory = new RecordingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions() { DirectoryPath = "./data/bgcommit_evictflush/temp" }));
            var (manager, storage) = await CreateManager("evictflush", cachePageCount: 0, fileCacheFactory: factory);
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "evictflush", 8);
            var cache = factory.Created.Single();
            using var walkGate = new WalkGate(manager);
            using var flushGate = new ManualResetEventSlim(false);
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await walkGate.Blocked.WaitAsync(Timeout);
                cache.ArmFlushGate(flushGate);
                var cleanup = Task.Run(() => manager.CacheTable.ForceCleanup());
                // The pass spilled its victims and is inside its flush.
                await cache.FlushEntered.WaitAsync(Timeout);

                var fetch = client.GetValue(keys[^1]).AsTask();
                var first = await Task.WhenAny(fetch, Task.Delay(TimeSpan.FromSeconds(1)));
                Assert.True(ReferenceEquals(first, fetch), "the fetch waited behind the eviction pass's flush");
                (await fetch)?.Return();

                flushGate.Set();
                await cleanup.WaitAsync(Timeout);
                walkGate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            }
            finally
            {
                flushGate.Set();
                manager.Dispose();
            }
        }

        /// <summary>
        /// An eviction pass gives the lock up after each spill write, a fetch of an owed page waits for one victim at most.
        /// </summary>
        [Fact]
        public async Task FetchOfAnOwedPageWaitsForOneEvictionVictimAtMost()
        {
            var factory = new RecordingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions() { DirectoryPath = "./data/bgcommit_evictvictim/temp" }));
            var (manager, storage) = await CreateManager("evictvictim", cachePageCount: 0, fileCacheFactory: factory);
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "evictvictim", 8);
            var cache = factory.Created.Single();
            using var walkGate = new WalkGate(manager);
            using var secondSpill = new ManualResetEventSlim(false);
            using var thirdSpill = new ManualResetEventSlim(false);
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await walkGate.Blocked.WaitAsync(Timeout);
                var secondEntered = cache.ArmWriteGate(2, secondSpill);
                _ = cache.ArmWriteGate(3, thirdSpill);
                var cleanup = Task.Run(() => manager.CacheTable.ForceCleanup());
                // The pass holds the lock inside its second spill write.
                await secondEntered.WaitAsync(Timeout);

                // Queued on the lock before the pass gives it up.
                var spilled = cache.WrittenKeys;
                var target = keys.First(key => !spilled.Contains(key));
                var fetch = client.GetValue(target).AsTask();
                secondSpill.Set();

                // The pass may decline instead of reaching its third spill, the fetch must finish either way.
                var first = await Task.WhenAny(fetch, Task.Delay(TimeSpan.FromSeconds(5)));
                Assert.True(ReferenceEquals(first, fetch), "the fetch waited for the rest of the eviction batch");
                (await fetch)?.Return();

                thirdSpill.Set();
                await cleanup.WaitAsync(Timeout);
                walkGate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                foreach (var key in keys)
                {
                    Assert.Equal(keys.IndexOf(key), ReadPersisted(storage, key));
                }
            }
            finally
            {
                secondSpill.Set();
                thirdSpill.Set();
                manager.Dispose();
            }
        }

        /// <summary>
        /// A pass that cannot get the lock back gives up what it handled and keeps the rest cached, no spill is written twice.
        /// </summary>
        [Fact]
        public async Task EvictionPassThatLosesTheLockReclaimsWhatItHandled()
        {
            var factory = new RecordingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions() { DirectoryPath = "./data/bgcommit_evictdecline/temp" }));
            var (manager, storage) = await CreateManager("evictdecline", cachePageCount: 0, fileCacheFactory: factory);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "evictdecline", 8);
            var cache = factory.Created.Single();
            using var walkGate = new WalkGate(manager);
            using var firstSpill = new ManualResetEventSlim(false);
            using var fetchWrite = new ManualResetEventSlim(false);
            try
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await walkGate.Blocked.WaitAsync(Timeout);
                var firstEntered = cache.ArmWriteGate(1, firstSpill);
                var cleanup = Task.Run(() => manager.CacheTable.ForceCleanup());
                await firstEntered.WaitAsync(Timeout);

                // The fetch takes the lock after the first spill and keeps it past the pass's wait budget.
                var spilled = cache.WrittenKeys;
                var target = keys.First(key => !spilled.Contains(key));
                session.ArmWriteGate(fetchWrite, new HashSet<long> { target });
                var fetch = client.GetValue(target).AsTask();
                firstSpill.Set();
                await session.WriterBlocked.WaitAsync(Timeout);
                await cleanup.WaitAsync(Timeout);

                var handled = Assert.Single(cache.WrittenKeys);
                Assert.False(manager.TryPeekCacheEntry(handled, out _), "the spilled victim was not reclaimed");
                foreach (var key in keys.Where(key => key != handled))
                {
                    Assert.True(manager.TryPeekCacheEntry(key, out _), $"page {key} left the cache without its spill");
                }

                fetchWrite.Set();
                (await fetch.WaitAsync(Timeout))?.Return();

                // The next pass finishes the job and skips the spill it already has.
                await manager.CacheTable.ForceCleanup().WaitAsync(Timeout);
                var writes = cache.WrittenKeys;
                Assert.Equal(writes.Count, writes.Distinct().Count());
                Assert.True(writes.Count > 1, "the second pass spilled nothing");

                walkGate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                foreach (var key in keys)
                {
                    Assert.Equal(keys.IndexOf(key), ReadPersisted(storage, key));
                }
            }
            finally
            {
                firstSpill.Set();
                fetchWrite.Set();
                manager.Dispose();
            }
        }

        /// <summary>
        /// Review 6: the walk serializes a page inside the session on its own thread, a read on
        /// the same session must not wait for that serialization.
        /// </summary>
        [Fact]
        public async Task ReservoirReadDoesNotWaitForAWriteSerializingAPage()
        {
            var serializer = new TestPageSerializer();
            var (manager, storage) = await CreateManager("serializelock", reservoir: true);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "serializelock", 4, serializer);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            // Straight into the reservoir session, the way the walk writes a page from the cache.
            var inner = session.Inner;
            using var gate = new ManualResetEventSlim(false);
            serializer.ArmSerializeGate(gate, 21);
            var write = Task.Run(() => inner.Write(client.GetNewPageId(), new SerializableObject(new TestPage(21), serializer)));
            try
            {
                await serializer.SerializeEntered.WaitAsync(Timeout);

                // The session takes its lock before it hands the task back, so the read gets its own thread.
                var read = Task.Run(() => inner.Read(keys[0]).AsTask());
                var first = await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(1)));
                Assert.True(ReferenceEquals(first, read), "a read waited for a write to serialize its page");
                Assert.Equal(4, (await read).Length);
            }
            finally
            {
                gate.Set();
            }
            await write.WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// A file roll sums the whole file, reads of its pages go on meanwhile.
        /// </summary>
        [Fact]
        public async Task ReservoirReadDoesNotWaitForTheRollChecksum()
        {
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
            {
                FileProvider = new MemoryFileProvider(),
                MaxFileSize = 64 * 1024
            });
            var manager = new StateManagerSync<StateManagerMetadata>(new StateManagerOptions()
            {
                PersistentStorage = storage,
                CachePageCount = 1000,
                MinCachePageCount = 100,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = "./data/bgcommit_rollsum/temp" }
            }, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("bgcommit_rollsum"), "bgcommit_rollsum", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            var session = (ReservoirPersistentSession)storage.CreateSession();
            var serializer = new TestPageSerializer();

            // Crosses the 16 KB segment boundary, its read measures bytes across segments.
            var spanning = new byte[20 * 1024];
            new Random(1).NextBytes(spanning);
            await session.Write(1000, new SerializableObject(spanning));
            await session.Write(1001, new SerializableObject(new TestPage(7), serializer));

            using var release = new ManualResetEventSlim(false);
            var summing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            session.FileChecksumHookForTests = () =>
            {
                summing.TrySetResult();
                release.Wait(Timeout);
            };
            var roll = Task.Run(() => session.Write(1002, new SerializableObject(new byte[48 * 1024])));
            try
            {
                await summing.Task.WaitAsync(Timeout);

                // The session takes its lock before it hands the task back, so the reads get their own threads.
                var raw = Task.Run(() => session.Read(1000).AsTask());
                var typed = Task.Run(() => session.Read(1001, serializer).AsTask());
                var both = Task.WhenAll(raw, typed);
                var first = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(5)));
                Assert.True(ReferenceEquals(first, both), "a read waited for the roll to sum its file");
                Assert.Equal(spanning, (await raw).ToArray());
                var page = await typed;
                Assert.Equal(7, page.Value);
                page.Return();
            }
            finally
            {
                release.Set();
            }
            await roll.WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// A session disposed under a roll waits for the file being summed, its buffers go back to the pool otherwise.
        /// </summary>
        [Fact]
        public async Task ReservoirSessionDisposeWaitsForTheRollChecksum()
        {
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
            {
                FileProvider = new MemoryFileProvider(),
                MaxFileSize = 64 * 1024
            });
            var manager = new StateManagerSync<StateManagerMetadata>(new StateManagerOptions()
            {
                PersistentStorage = storage,
                CachePageCount = 1000,
                MinCachePageCount = 100,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = "./data/bgcommit_rolldispose/temp" }
            }, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("bgcommit_rolldispose"), "bgcommit_rolldispose", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            var session = (ReservoirPersistentSession)storage.CreateSession();

            using var release = new ManualResetEventSlim(false);
            var summing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            session.FileChecksumHookForTests = () =>
            {
                summing.TrySetResult();
                release.Wait(Timeout);
            };
            var roll = Task.Run(() => session.Write(1000, new SerializableObject(new byte[70 * 1024])));
            try
            {
                await summing.Task.WaitAsync(Timeout);

                var dispose = Task.Run(() => session.Dispose());
                var first = await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromMilliseconds(500)));
                Assert.False(ReferenceEquals(first, dispose), "the session returned its writer while the roll was summing it");

                release.Set();
                await dispose.WaitAsync(Timeout);
            }
            finally
            {
                release.Set();
            }
            await roll.WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// Review 6: a second Commit that got past the refusal before the first published its
        /// generation must still be refused once it holds the lock.
        /// </summary>
        [Fact]
        public async Task ConcurrentCommitIsRefusedUnderTheLock()
        {
            var serializer = new TestPageSerializer();
            var (manager, storage) = await CreateManager("twocommits");
            using var storageLifetime = storage;
            var (client, _, _) = await CreateClientWithPages(manager, storage, "twocommits", 8, serializer);
            serializer.CheckpointHold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            // The first Commit parks inside its serializer checkpoint, holding the commit lock.
            var first = Task.Run(() => client.Commit().AsTask());
            await serializer.CheckpointEntered.WaitAsync(Timeout);
            // The second passes the refusal, nothing is published yet, and queues on the lock.
            var second = Task.Run(() => client.Commit().AsTask());
            await Task.Delay(200);
            serializer.CheckpointHold.SetResult();
            await first.WaitAsync(Timeout);

            await Assert.ThrowsAsync<InvalidOperationException>(() => second.WaitAsync(Timeout));
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// The checkpoint must hold the state as of Commit even though the tree keeps changing
        /// underneath the background walk.
        /// </summary>
        [Fact]
        public async Task TreeCheckpointHoldsTheStateAsOfCommitUnderConcurrentWrites()
        {
            // The reservoir keeps its checkpoints across a recovery, the file cache storage does not.
            var manager = CreateReservoirManager("tree");
            await manager.InitializeAsync();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree",
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            const int count = 5000;
            for (long i = 0; i < count; i++)
            {
                await tree.Upsert(i, (int)i);
            }
            // The walk is held, so every page is fetched and rewritten while it still owes its write.
            using (var gate = new WalkGate(manager))
            {
                await tree.Commit();
                await gate.Blocked.WaitAsync(Timeout);
                for (long i = 0; i < count; i++)
                {
                    await tree.Upsert(i, (int)i + 1);
                }
            }
            await manager.CheckpointAsync();

            // Recover to that checkpoint, the rewrites were never committed.
            await manager.InitializeAsync();
            for (long i = 0; i < count; i++)
            {
                var (found, value) = await tree.GetValue(i);
                Assert.True(found, $"key {i} missing after recovery");
                Assert.Equal((int)i, value);
            }

            // The next generation carries the rewrites.
            for (long i = 0; i < count; i++)
            {
                await tree.Upsert(i, (int)i + 1);
            }
            await tree.Commit();
            await manager.CheckpointAsync();
            await manager.InitializeAsync();
            for (long i = 0; i < count; i++)
            {
                var (found, value) = await tree.GetValue(i);
                Assert.True(found, $"key {i} missing after second recovery");
                Assert.Equal((int)i + 1, value);
            }
            manager.Dispose();
        }

        /// <summary>
        /// A failed commit must restore previous updated flag.
        /// </summary>
        [Fact]
        public async Task FailedCommitRestoresMetadataUpdatedFlag()
        {
            var (manager, storage) = await CreateManager("metaupdated");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "metaupdated", 1);
            client.Metadata = new TestMetadata();
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            client.Metadata.Updated = true;
            var failKey = client.GetNewPageId();
            client.AddOrUpdate(failKey, new TestPage(2));
            session.FaultingKeys.TryAdd(failKey, 0);

            var syncClient = (SyncStateClient<TestPage, TestMetadata>)client;
            await Assert.ThrowsAsync<IOException>(async () =>
            {
                await client.Commit();
                await syncClient.WaitForCommitAsync();
            });

            // A failed commit must restore previous updated flag.
            Assert.True(client.Metadata.Updated);
            manager.Dispose();
        }

        /// <summary>
        /// Faulted commit task must not report settled clean.
        /// </summary>
        [Fact]
        public async Task FaultedCommitDoesNotReportCleanSettle()
        {
            var (manager, storage) = await CreateManager("faultedsettle");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "faultedsettle", 1);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            var failKey = client.GetNewPageId();
            client.AddOrUpdate(failKey, new TestPage(2));
            session.FaultingKeys.TryAdd(failKey, 0);

            await client.Commit();
            var syncClient = (SyncStateClient<TestPage, TestMetadata>)client;
            try
            {
                await syncClient.WaitForCommitAsync();
            }
            catch (IOException)
            {
            }

            // Faulted commit task must not report settled clean.
            Assert.True(((StateClient)client).HasCommitFault);
            manager.Dispose();
        }

        /// <summary>
        /// Dequeue must not dispose node while rent is held.
        /// </summary>
        [Fact]
        public async Task QueueDequeueDoesNotDisposeNodeHeldByPeek()
        {
            var (manager, _) = await CreateManager("queueuaf", backgroundCommit: false);
            var client = (SyncStateClient<IBPlusTreeNode, FlowtideQueueMetadata>)await manager.CreateClientAsync<IBPlusTreeNode, FlowtideQueueMetadata>(
                "queue_client",
                new StateClientOptions<IBPlusTreeNode>
                {
                    ValueSerializer = new FlowtideQueueSerializer<int, PrimitiveListValueContainer<int>>(
                        new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance))
                },
                GlobalMemoryManager.Instance);
            await client.InitializeSerializerAsync();

            var queue = new FlowtideQueue<int, PrimitiveListValueContainer<int>>(
                client,
                new FlowtideQueueOptions<int, PrimitiveListValueContainer<int>>
                {
                    MemoryAllocator = GlobalMemoryManager.Instance,
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance),
                    PageSizeBytes = 64
                });
            await queue.InitializeAsync();

            for (int i = 0; i < 50; i++)
            {
                await queue.Enqueue(i);
            }

            var leftNode = queue._leftNode!;
            Assert.True(leftNode.TryRent());

            while (ReferenceEquals(queue._leftNode, leftNode))
            {
                await queue.Dequeue();
            }

            // Dequeue must not dispose node while rent is held.
            var val = leftNode.values.Get(0);
            Assert.Equal(0, val);
            leftNode.Return();
            manager.Dispose();
        }

        /// <summary>
        /// Traverse internal nodes without leaking child rent count.
        /// </summary>
        [Fact]
        public async Task AppendTreeCreateInternalNodesListDoesNotLeakChildRent()
        {
            var (manager, _) = await CreateManager("tree_leak", backgroundCommit: false);
            var client = (SyncStateClient<IBPlusTreeNode, AppendTreeMetadata>)await manager.CreateClientAsync<IBPlusTreeNode, AppendTreeMetadata>(
                "tree_client",
                new StateClientOptions<IBPlusTreeNode>
                {
                    ValueSerializer = new BPlusTreeSerializer<long, long, ListKeyContainer<long>, ListValueContainer<long>>(
                        new KeyListSerializer<long>(new LongSerializer()),
                        new ValueListSerializer<long>(new LongSerializer()),
                        GlobalMemoryManager.Instance)
                },
                GlobalMemoryManager.Instance);
            await client.InitializeSerializerAsync();

            var treeOptions = new BPlusTreeOptions<long, long, ListKeyContainer<long>, ListValueContainer<long>>
            {
                BucketSize = 2,
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<long>(new LongSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            };

            var tree = new AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>(client, treeOptions);
            await tree.InitializeAsync();

            for (long i = 0; i < 8; i++)
            {
                await tree.Append(i, i);
            }
            await tree.Commit();
            await manager.CheckpointAsync();

            var tree2 = new AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>(client, treeOptions);
            await tree2.InitializeAsync();

            // Traverse internal nodes without leaking child rent count.
            var rootId = client.Metadata!.Root;
            Assert.True(manager.CacheTable.TryPeekEntry(rootId, out var rootEntry));
            Assert.Equal(1, rootEntry.Value.RentCount);
            manager.Dispose();
        }

        [Theory]
        [InlineData(8192, 16)]
        [InlineData(10000, 20)]
        [InlineData(10240, 20)]
        [InlineData(16384, 32)]
        [InlineData(17408, 34)]
        [InlineData(20480, 40)]
        public void SerializedPagesFillExistingBuffersBeforeRentingMoreMemory(int pageSize, int expectedSegmentCount)
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                var pages = new byte[32][];
                var locations = new ReadOnlySequence<byte>[pages.Length];
                for (int i = 0; i < pages.Length; i++)
                {
                    pages[i] = new byte[pageSize];
                    Array.Fill(pages[i], (byte)(i + 1));
                    locations[i] = fileWriter.Write(i + 1, new SerializableObject(pages[i]));
                    Assert.Equal(pageSize, locations[i].Length);
                    Assert.Equal(pages[i], locations[i].ToArray());
                }

                fileWriter.Finish();
                for (int i = 0; i < pages.Length; i++)
                {
                    Assert.Equal(pageSize, locations[i].Length);
                    Assert.Equal(pages[i], locations[i].ToArray());
                }

                var segmentCount = 0;
                long reservedBytes = 0;
                for (var segment = fileWriter.DataStartSegment; segment != null; segment = segment._next)
                {
                    segmentCount++;
                    reservedBytes += segment.MemoryOwner!.Memory.Length;
                }
                Assert.Equal(expectedSegmentCount, segmentCount);
                Assert.Equal(expectedSegmentCount * 16384L, reservedBytes);
                Assert.Equal(pageSize * pages.Length + 64 + pages.Length * 12 + 4, fileWriter.WrittenData.Length);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Fact]
        public void DefaultBufferRequestsProvideSpaceAfterAnExactFit()
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                fileWriter.Write(1, new SerializableObject(new byte[16384]));
                Assert.False(fileWriter.GetSpan().IsEmpty);
                Assert.False(fileWriter.GetMemory().IsEmpty);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Theory]
        [InlineData(32768, 33)]
        [InlineData(49152, 64)]
        [InlineData(65536, 34)]
        [InlineData(98304, 65)]
        [InlineData(131072, 35)]
        [InlineData(262144, 36)]
        public void LargeSerializedPagesUseLargerSegmentsWithoutReservingExtraMemory(int pageSize, int expectedSegmentCount)
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                var pages = new byte[32][];
                var locations = new ReadOnlySequence<byte>[pages.Length];
                for (int i = 0; i < pages.Length; i++)
                {
                    pages[i] = new byte[pageSize];
                    Array.Fill(pages[i], (byte)(i + 1));
                    locations[i] = fileWriter.Write(i + 1, new SerializableObject(pages[i]));
                    Assert.Equal(pageSize, locations[i].Length);
                    Assert.Equal(pages[i], locations[i].ToArray());
                }

                fileWriter.Finish();
                for (int i = 0; i < pages.Length; i++)
                {
                    Assert.Equal(pageSize, locations[i].Length);
                    Assert.Equal(pages[i], locations[i].ToArray());
                    Assert.Equal(System.IO.Hashing.Crc32.HashToUInt32(pages[i]), fileWriter.Crc32s[i]);
                }

                var segmentCount = 0;
                long reservedBytes = 0;
                for (var segment = fileWriter.DataStartSegment; segment != null; segment = segment._next)
                {
                    segmentCount++;
                    reservedBytes += segment.MemoryOwner!.Memory.Length;
                }
                Assert.Equal(expectedSegmentCount, segmentCount);
                Assert.Equal((long)pageSize * pages.Length, reservedBytes);
                Assert.Equal(pageSize * pages.Length + 64 + pages.Length * 12 + 4, fileWriter.WrittenData.Length);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Theory]
        [InlineData(false, 0)]
        [InlineData(true, 0)]
        [InlineData(false, 10000)]
        [InlineData(true, 10000)]
        public void ExactFitBufferRequestsReuseTheCurrentSegment(bool requestMemory, int writtenLength)
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                fileWriter.GetSpan(writtenLength).Slice(0, writtenLength).Clear();
                fileWriter.Advance(writtenLength);
                var segment = fileWriter.CurrentSegment;
                var remainingLength = 16384 - writtenLength;

                if (requestMemory)
                {
                    Assert.Equal(remainingLength, fileWriter.GetMemory(remainingLength).Length);
                }
                else
                {
                    Assert.Equal(remainingLength, fileWriter.GetSpan(remainingLength).Length);
                }

                Assert.Same(segment, fileWriter.CurrentSegment);
                Assert.Equal(writtenLength, fileWriter.CurrentIndex);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Fact]
        public void SerializedPagesWithDifferentSizesPreserveDataAndChecksums()
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                var sizes = new[] { 0, 1, 16383, 65536, 17408, 262145, 7, 98304, 0 };
                var pages = new byte[sizes.Length][];
                var locations = new ReadOnlySequence<byte>[sizes.Length];
                long totalLength = 0;
                for (int i = 0; i < sizes.Length; i++)
                {
                    pages[i] = new byte[sizes[i]];
                    Array.Fill(pages[i], (byte)(i + 1));
                    locations[i] = fileWriter.Write(i + 1, new SerializableObject(pages[i]));
                    Assert.Equal(pages[i], locations[i].ToArray());
                    totalLength += sizes[i];
                }

                fileWriter.Finish();
                for (int i = 0; i < sizes.Length; i++)
                {
                    Assert.Equal(sizes[i], locations[i].Length);
                    Assert.Equal(pages[i], locations[i].ToArray());
                    Assert.Equal(System.IO.Hashing.Crc32.HashToUInt32(pages[i]), fileWriter.Crc32s[i]);
                }

                long reservedBytes = 0;
                for (var segment = fileWriter.DataStartSegment; segment != null; segment = segment._next)
                {
                    reservedBytes += segment.MemoryOwner!.Memory.Length;
                }
                Assert.Equal((totalLength + 16383) / 16384 * 16384, reservedBytes);
                Assert.Equal(totalLength + 64 + sizes.Length * 12 + 4, fileWriter.WrittenData.Length);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Theory]
        [InlineData(16384)]
        [InlineData(262144)]
        public void EmptySerializedPagesReuseFullSegmentsWithoutRentingMoreMemory(int pageSize)
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                var payload = new byte[pageSize];
                Array.Fill(payload, (byte)42);
                var page = fileWriter.Write(1, new SerializableObject(payload));
                var segment = fileWriter.CurrentSegment;
                var index = fileWriter.CurrentIndex;

                var empty = fileWriter.Write(2, new SerializableObject(ReadOnlyMemory<byte>.Empty));

                Assert.Same(segment, fileWriter.CurrentSegment);
                Assert.Equal(index, fileWriter.CurrentIndex);
                Assert.Null(segment._next);
                Assert.Equal(0, empty.Length);
                Assert.Equal(pageSize, fileWriter.WrittenLength);
                Assert.Equal(2, fileWriter.PageIds.Count);
                Assert.Equal(pageSize, fileWriter.PageOffsets[1]);
                Assert.Equal(System.IO.Hashing.Crc32.HashToUInt32(ReadOnlySpan<byte>.Empty), fileWriter.Crc32s[1]);

                fileWriter.Finish();

                Assert.Equal(payload, page.ToArray());
                Assert.Equal(0, empty.Length);
                Assert.Equal(pageSize + 64 + 2 * 12 + 4, fileWriter.WrittenData.Length);
                long reservedBytes = 0;
                for (var current = fileWriter.DataStartSegment; current != null; current = current._next)
                {
                    reservedBytes += current.MemoryOwner!.Memory.Length;
                }
                Assert.Equal(pageSize, reservedBytes);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Fact]
        public void FinishedBlobFileWritersRejectEmptySerializedPages()
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                fileWriter.Finish();
                Assert.Throws<InvalidOperationException>(() => fileWriter.Write(1, new SerializableObject(ReadOnlyMemory<byte>.Empty)));
            }
            finally
            {
                fileWriter.Return();
            }
        }

        [Fact]
        public void SerializedPageCopyDoesNotAllocateWhenTheBufferHasCapacity()
        {
            var page = new SerializableObject(new byte[8192]);
            var writer = new ArrayBufferWriter<byte>(8192);
            page.Serialize(writer);
            writer.Clear();

            var before = GC.GetAllocatedBytesForCurrentThread();
            page.Serialize(writer);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.Equal(0, allocated);
            Assert.Equal(8192, writer.WrittenCount);
        }

        [Theory]
        [InlineData(16374)]
        [InlineData(16380)]
        [InlineData(16384)]
        public void CompressedPageReadsDoNotAllocateAnAdditionalReassemblyBuffer(int paddingLength)
        {
            using var serializer = new CompressedStateSerializer<TestPage>(new TestPageSerializer(), 3, GlobalMemoryManager.Instance);
            var buffer = new ArrayBufferWriter<byte>();
            serializer.Serialize(buffer, new TestPage(42));
            var contiguous = new ReadOnlySequence<byte>(buffer.WrittenMemory);
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                fileWriter.Write(1, new SerializableObject(new byte[paddingLength]));
                var location = fileWriter.Write(2, new SerializableObject(buffer.WrittenMemory));
                for (int i = 0; i < 16; i++)
                {
                    serializer.Deserialize(contiguous, buffer.WrittenCount).Return();
                    serializer.Deserialize(location, buffer.WrittenCount).Return();
                }

                var before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < 100; i++)
                {
                    serializer.Deserialize(contiguous, buffer.WrittenCount).Return();
                }
                var contiguousAllocations = GC.GetAllocatedBytesForCurrentThread() - before;
                before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < 100; i++)
                {
                    serializer.Deserialize(location, buffer.WrittenCount).Return();
                }
                var locationAllocations = GC.GetAllocatedBytesForCurrentThread() - before;

                Assert.Equal(contiguousAllocations, locationAllocations);
                var page = serializer.Deserialize(location, buffer.WrittenCount);
                Assert.Equal(42, page.Value);
                page.Return();
            }
            finally
            {
                fileWriter.Return();
            }
        }

        /// <summary>
        /// Intermediate write sequence length must match byte count.
        /// </summary>
        [Fact]
        public void BlobFileWriterEnsureCapacityDoesNotInflateRunningIndex()
        {
            var fileWriter = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            try
            {
                fileWriter.GetSpan(10000).Slice(0, 10000).Clear();
                fileWriter.Advance(10000);
                fileWriter.GetSpan(7000).Slice(0, 7000).Clear();
                fileWriter.Advance(7000);

                var endSeg = fileWriter.CurrentSegment;

                // Intermediate write sequence length must match byte count.
                Assert.Equal(10064, endSeg.RunningIndex);
            }
            finally
            {
                fileWriter.Return();
            }
        }

        /// <summary>
        /// A failed commit outlives a structure reset, only recovery lets the client commit again.
        /// </summary>
        [Fact]
        public async Task RecoveryClearsAFailedCommitThatAResetSurfaced()
        {
            var (manager, storage) = await CreateManager("resetfailed");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "resetfailed", 1);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            var failKey = client.GetNewPageId();
            client.AddOrUpdate(failKey, new TestPage(2));
            session.FaultingKeys.TryAdd(failKey, 0);
            await client.Commit();

            var syncClient = (SyncStateClient<TestPage, TestMetadata>)client;
            try
            {
                await syncClient.WaitForCommitAsync();
            }
            catch (IOException)
            {
            }

            await Assert.ThrowsAsync<IOException>(() => client.Reset(true).AsTask().WaitAsync(Timeout));
            session.FaultingKeys.Clear();
            await Assert.ThrowsAsync<InvalidOperationException>(() => client.Commit().AsTask().WaitAsync(Timeout));

            await manager.InitializeAsync().WaitAsync(Timeout);
            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(3));
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(3, ReadPersisted(storage, key));
            manager.Dispose();
        }

        /// <summary>
        /// A structure reset after a checkpoint does not erase that checkpoint, recovery reads its metadata again.
        /// </summary>
        [Fact]
        public async Task RecoveryAfterAResetRestoresTheCheckpointedMetadata()
        {
            var (manager, storage) = await CreateManager("resetrecover");
            using var storageLifetime = storage;
            var (client, _, _) = await CreateClientWithPages(manager, storage, "resetrecover", 4);
            client.Metadata = new TestMetadata() { Updated = true };
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            await client.Reset(true).AsTask().WaitAsync(Timeout);
            await manager.InitializeAsync().WaitAsync(Timeout);
            Assert.NotNull(client.Metadata);
            manager.Dispose();
        }

        /// <summary>
        /// A commit that fails after a structure reset must not take the checkpointed metadata with it.
        /// </summary>
        [Fact]
        public async Task RecoveryAfterAResetAndAFailedCommitRestoresTheCheckpointedMetadata()
        {
            var (manager, storage) = await CreateManager("resetfailrecover");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "resetfailrecover", 4);
            client.Metadata = new TestMetadata() { Updated = true };
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            // The structure recreates itself after the reset, then its commit fails.
            await client.Reset(true).AsTask().WaitAsync(Timeout);
            client.Metadata = new TestMetadata() { Updated = true };
            var failKey = client.GetNewPageId();
            client.AddOrUpdate(failKey, new TestPage(9));
            session.FaultingKeys[failKey] = 1;
            await client.Commit().AsTask().WaitAsync(Timeout);
            await Assert.ThrowsAnyAsync<Exception>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            session.FaultingKeys.Clear();
            await manager.InitializeAsync().WaitAsync(Timeout);
            Assert.NotNull(client.Metadata);
            manager.Dispose();
        }

        /// <summary>
        /// A recovery that gives up before it resets the clients must keep the failed commit.
        /// </summary>
        [Fact]
        public async Task AbortedRecoveryKeepsTheCommitFailure()
        {
            var (manager, storage) = await CreateManager("abortrecovery", recoveryCommitWaitTimeout: TimeSpan.FromMilliseconds(200));
            using var storageLifetime = storage;
            // Paused first, the dictionary keeps insertion order.
            var (failed, failedSession, failedKeys) = await CreateClientWithPages(manager, storage, "abortrecovery_failed", 4);
            var (wedged, _, _) = await CreateClientWithPages(manager, storage, "abortrecovery_wedged", 2);
            failedSession.FaultingKeys[failedKeys[1]] = 1;

            var wedgedBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseWedged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.PageWriteHookForTests = (name, key) =>
            {
                if (name != "abortrecovery_wedged")
                {
                    return Task.CompletedTask;
                }
                wedgedBlocked.TrySetResult();
                return releaseWedged.Task;
            };
            try
            {
                await failed.Commit().AsTask().WaitAsync(Timeout);
                await Assert.ThrowsAsync<IOException>(() => failed.Reset(true).AsTask().WaitAsync(Timeout));
                await wedged.Commit().AsTask().WaitAsync(Timeout);
                await wedgedBlocked.Task.WaitAsync(Timeout);

                // Pauses the failed client, then gives up on the wedged walk before any reset.
                await Assert.ThrowsAsync<InvalidOperationException>(() => manager.InitializeAsync().WaitAsync(Timeout));

                releaseWedged.TrySetResult();
                await ((StateClient)wedged).WaitForCommitAsync().WaitAsync(Timeout);
                AssertCausedBy<IOException>(await Record.ExceptionAsync(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout)));
            }
            finally
            {
                releaseWedged.TrySetResult();
                manager.PageWriteHookForTests = null;
                manager.Dispose();
            }
        }

        /// <summary>
        /// Clean commit must clear right node updated flag.
        /// </summary>
        [Fact]
        public async Task QueueCommitClearsRightNodeUpdatedFlag()
        {
            var (manager, storage) = await CreateManager("queueflag", backgroundCommit: false);
            using var storageLifetime = storage;
            var client = (SyncStateClient<IBPlusTreeNode, FlowtideQueueMetadata>)await manager.CreateClientAsync<IBPlusTreeNode, FlowtideQueueMetadata>(
                "queue_client",
                new StateClientOptions<IBPlusTreeNode>
                {
                    ValueSerializer = new FlowtideQueueSerializer<int, PrimitiveListValueContainer<int>>(
                        new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance))
                },
                GlobalMemoryManager.Instance);
            await client.InitializeSerializerAsync();

            var queue = new FlowtideQueue<int, PrimitiveListValueContainer<int>>(
                client,
                new FlowtideQueueOptions<int, PrimitiveListValueContainer<int>>
                {
                    MemoryAllocator = GlobalMemoryManager.Instance,
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance)
                });
            await queue.InitializeAsync();

            await queue.Enqueue(1);
            await queue.Commit();

            var session = storage.Sessions.Last();
            var rightId = queue._rightNode!.Id;
            var writesBefore = session.TotalWriteCount(rightId);

            // Clean commit must clear right node updated flag.
            await queue.Commit();
            var writesAfter = session.TotalWriteCount(rightId);

            Assert.Equal(writesBefore, writesAfter);
            manager.Dispose();
        }

        /// <summary>
        /// A structure reset keeps the checkpointed metadata, only a recovery that wipes the storage forgets it.
        /// </summary>
        [Fact]
        public async Task ResetKeepsCommitedOnceAndAWipingRecoveryClearsIt()
        {
            var (manager, storage) = await CreateManager("commitedonce_reset", backgroundCommit: false);
            using var storageLifetime = storage;
            var (client, _, _) = await CreateClientWithPages(manager, storage, "commitedonce_reset", 1);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var syncClient = (SyncStateClient<TestPage, TestMetadata>)client;
            Assert.True(syncClient.CommitedOnceForTests);

            await client.Reset(clearMetadata: true);
            Assert.True(syncClient.CommitedOnceForTests);

            await manager.InitializeAsync().WaitAsync(Timeout);
            Assert.True(syncClient.CommitedOnceForTests);

            // Version 0 resets the storage.
            await manager.InitializeAsync(checkpointVersion: 0).WaitAsync(Timeout);
            Assert.False(syncClient.CommitedOnceForTests);
            manager.Dispose();
        }

        /// <summary>
        /// Faulted background commit must not remain in flight.
        /// </summary>
        [Fact]
        public async Task FaultedCommitDoesNotReportAsInFlight()
        {
            var (manager, storage) = await CreateManager("inflight_fault");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "inflight_fault", 4);

            session.FaultCommit = true;
            await client.Commit().AsTask().WaitAsync(Timeout);
            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            // Faulted background commit must not remain in flight.
            Assert.False(manager.HasCommitsInFlight);
            manager.Dispose();
        }

        /// <summary>
        /// Failed session commit must not increment manager page commits counter.
        /// </summary>
        [Fact]
        public async Task SessionCommitFailureRollsBackMetadataCounters()
        {
            var (manager, storage) = await CreateManager("rollback_counters");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "rollback_counters", 4);
            var pageCommitsBefore = manager.PageCommits;

            session.FaultCommit = true;
            await client.Commit().AsTask().WaitAsync(Timeout);
            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            // Failed session commit must not increment manager page commits counter.
            Assert.Equal(pageCommitsBefore, manager.PageCommits);
            manager.Dispose();
        }

        /// <summary>
        /// Checkpoint must fail if client commit threw synchronously.
        /// </summary>
        [Fact]
        public async Task CheckpointFailsWhenClientCommitThrowsBeforeStartingBackgroundTask()
        {
            var (manager, storage) = await CreateManager("p0_3_fail");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "p0_3_fail", 1);
            var metaId = ((StateClient)client).MetadataId;

            // Inject write failure on metadata key during commit.
            session.FaultingKeys[metaId] = 1;

            // Commit must throw synchronously before starting background task.
            await Assert.ThrowsAsync<IOException>(() => client.Commit().AsTask().WaitAsync(Timeout));

            // Checkpoint must fail when a client commit faulted.
            await Assert.ThrowsAsync<Exception>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            manager.Dispose();
        }

        /// <summary>
        /// Read cache must not return stale modified page.
        /// </summary>
        [Fact]
        public async Task ReadCacheModeDoesNotReturnStaleVersionAfterPageIsModified()
        {
            var (manager, storage) = await CreateManager("p1_2_stale", useReadCache: true, cachePageCount: 0);
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "p1_2_stale", 1);
            var key = keys[0];

            // Initial commit and checkpoint in read cache mode.
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            // Foreground update modifies page after read cache populate.
            var page = await client.GetValue(key);
            Assert.NotNull(page);
            page.Value = 999;
            client.AddOrUpdate(key, page);
            page.Return();

            // Eviction forces read cache fetch on next lookup.
            await manager.CacheTable.ForceCleanup();

            // Read cache must not return obsolete stale version.
            var fetched = await client.GetValue(key);
            Assert.NotNull(fetched);
            Assert.Equal(999, fetched.Value);

            manager.Dispose();
        }

        /// <summary>
        /// Reset must clear uncommitted new page allocation counter.
        /// </summary>
        [Fact]
        public async Task ResetClearsUncommittedNewPagesCounterSoNextCommitDoesNotInflatePageCount()
        {
            var (manager, storage) = await CreateManager("p2_4_count");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "p2_4_count", 0);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var initialPageCount = manager.PageCount;

            // Allocate new page IDs without committing any changes.
            for (int i = 0; i < 10; i++)
            {
                client.GetNewPageId();
            }

            // Reset must clear uncommitted allocated page counter.
            await client.Reset(false).AsTask().WaitAsync(Timeout);

            // Subsequent commit must not inflate metadata page count.
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(initialPageCount, manager.PageCount);

            manager.Dispose();
        }

        /// <summary>
        /// Walk failure reset purges pages written before failure occurred.
        /// </summary>
        [Fact]
        public async Task ResetPurgesCacheEntriesWrittenBeforeBackgroundWalkFailed()
        {
            var (manager, storage) = await CreateManager("p1_3_reset_purge");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "p1_3_reset_purge", 2);

            // Fault the second page write during background commit walk.
            session.FaultingKeys.TryAdd(keys[1], 0);

            // Commit begins background walk writing first page successfully.
            await client.Commit().AsTask().WaitAsync(Timeout);

            // Wait until the walk faults on the second page.
            await Assert.ThrowsAnyAsync<Exception>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            // Reset surfaces the failed walk once and still purges.
            await Assert.ThrowsAsync<IOException>(() => client.Reset(false).AsTask().WaitAsync(Timeout));

            // Cache must purge pages written before walk failed.
            Assert.False(manager.CacheTable.TryGetCacheValue(keys[0], out _));

            manager.Dispose();
        }

        /// <summary>
        /// Checkpoint exception preserves underlying commit failure exception.
        /// </summary>
        [Fact]
        public async Task CheckpointAsyncPreservesInnerExceptionWhenCommitFaults()
        {
            var (manager, storage) = await CreateManager("p1_1_fault");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "p1_1_fault", 2);

            // Hold page rent to force synchronous write.
            var page = await client.GetValue(keys[0]);

            // Inject write failure on the held page.
            session.FaultingKeys.TryAdd(keys[0], 0);

            // State client commit records synchronous write fault.
            await Record.ExceptionAsync(() => client.Commit().AsTask());

            // State manager checkpoint must preserve root cause.
            var ex = await Record.ExceptionAsync(() => manager.CheckpointAsync().AsTask());
            Assert.NotNull(ex);
            Assert.NotNull(ex.InnerException);

            manager.Dispose();
        }

        // Background-commit review of 2026-09-25.

        /// <summary>
        /// A delete must not knock another page out of the lock-free lookup slot it shares.
        /// </summary>
        [Fact]
        public async Task DeleteLeavesACollidingPageInTheLookupSlot()
        {
            var (manager, storage) = await CreateManager("lookupslot", cachePageCount: 5000);
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "lookupslot", 1010);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            var deleted = keys[0];
            var live = keys[1009];
            Assert.Equal(deleted % 1009, live % 1009);

            client.Delete(deleted);
            var hits = sync.LookupTableHitsForTests;
            var page = await client.GetValue(live).AsTask().WaitAsync(Timeout);
            page!.Return();
            Assert.Equal(hits + 1, sync.LookupTableHitsForTests);
            manager.Dispose();
        }

        /// <summary>
        /// A delete that waited for its checkpoint write must not knock another page out of the slot either.
        /// </summary>
        [Fact]
        public async Task DeferredDeleteLeavesACollidingPageInTheLookupSlot()
        {
            var (manager, storage) = await CreateManager("lookupslot_deferred", cachePageCount: 5000);
            using var storageLifetime = storage;
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "lookupslot_deferred", 1010);
            var sync = (SyncStateClient<TestPage, TestMetadata>)client;
            var deleted = keys[0];
            var live = keys[1009];
            Assert.Equal(deleted % 1009, live % 1009);

            using (var gate = new WalkGate(manager))
            {
                await client.Commit().AsTask().WaitAsync(Timeout);
                await gate.Blocked.WaitAsync(Timeout);
                client.Delete(deleted);
                var owed = await client.GetValue(live).AsTask().WaitAsync(Timeout);
                owed!.Return();
            }
            await ((StateClient)client).WaitForCommitAsync().WaitAsync(Timeout);

            var hits = sync.LookupTableHitsForTests;
            var page = await client.GetValue(live).AsTask().WaitAsync(Timeout);
            page!.Return();
            Assert.Equal(hits + 1, sync.LookupTableHitsForTests);
            manager.Dispose();
        }

        /// <summary>
        /// A walk failure must take CommitedOnce back, recovery would read a metadata page that never landed.
        /// </summary>
        [Fact]
        public async Task WalkWriteFailureDoesNotStrandTheClientMetadata()
        {
            var (manager, storage) = await CreateManager("walkfault_meta");
            using var storageLifetime = storage;
            // A checkpoint before the client exists, so recovery reads client metadata instead of clearing it.
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "walkfault_meta", 4);
            session.FaultingKeys[keys[1]] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);
            await Assert.ThrowsAsync<IOException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));

            session.FaultingKeys.Clear();
            await manager.InitializeAsync().WaitAsync(Timeout);
            manager.Dispose();
        }

        /// <summary>
        /// After the walk failed, fetching a page it still owes fails too instead of writing it.
        /// </summary>
        [Fact]
        public async Task FetchAfterAFailedWalkWritesNoOwedPage()
        {
            var (manager, storage) = await CreateManager("fetchafterfail");
            using var storageLifetime = storage;
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "fetchafterfail", 8);
            session.FaultingKeys[keys[0]] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);
            await Assert.ThrowsAsync<IOException>(() => ((StateClient)client).WaitForCommitAsync().WaitAsync(Timeout));

            var owed = keys[^1];
            await Assert.ThrowsAsync<IOException>(() => client.GetValue(owed).AsTask().WaitAsync(Timeout));
            Assert.Equal(0, session.WriteCount(owed));
            manager.Dispose();
        }

        /// <summary>
        /// A reset makes the next commit write the metadata once, even when the new metadata is not marked updated.
        /// </summary>
        [Fact]
        public async Task ResetWritesTheReplacedMetadataOnce()
        {
            var (manager, storage) = await CreateManager("replacedmeta");
            using var storageLifetime = storage;
            var (client, session, _) = await CreateClientWithPages(manager, storage, "replacedmeta", 1);
            var metadataId = ((StateClient)client).MetadataId;
            client.Metadata = new TestMetadata() { Updated = true };
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

            async Task<int> CommitAndCountMetadataWrites()
            {
                var before = session.TotalWriteCount(metadataId);
                await client.Commit().AsTask().WaitAsync(Timeout);
                await ((StateClient)client).WaitForCommitAsync().WaitAsync(Timeout);
                return session.TotalWriteCount(metadataId) - before;
            }

            await client.Reset(true).AsTask().WaitAsync(Timeout);
            client.Metadata = new TestMetadata() { Updated = false };
            Assert.Equal(1, await CommitAndCountMetadataWrites());
            Assert.Equal(0, await CommitAndCountMetadataWrites());

            // A recovery replaces the reset's metadata with the checkpointed one, nothing is owed.
            await client.Reset(true).AsTask().WaitAsync(Timeout);
            await manager.InitializeAsync().WaitAsync(Timeout);
            client.Metadata!.Updated = false;
            Assert.Equal(0, await CommitAndCountMetadataWrites());
            manager.Dispose();
        }
    }
}

