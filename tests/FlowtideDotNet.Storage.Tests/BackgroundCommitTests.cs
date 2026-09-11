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
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
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
        private class RecordingSession : IPersistentStorageSession
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

            public bool IsThreadSafe { get; set; }

            public int WriteCount(long key)
            {
                return WrittenValues.TryGetValue(key, out var values) ? values.Count : 0;
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
            private long _gatedFreeKey;

            /// <summary>
            /// Blocks the first Free of the key inside the call, the caller keeps whatever it holds.
            /// </summary>
            public void ArmFreeGate(ManualResetEventSlim gate, long key)
            {
                _gatedFreeKey = key;
                Volatile.Write(ref _freeGate, gate);
            }

            public Task FreeEntered => _freeEntered.Task;

            public void Write(long id, SerializableObject serializableObject) => _inner.Write(id, serializableObject);

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
                        _freeEntered.TrySetResult();
                        gate.Wait();
                    }
                }
                _inner.Free(pageKey);
            }

            public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);

            public void Flush() => _inner.Flush();

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

        private class RecordingStorage : IPersistentStorage
        {
            private readonly IPersistentStorage _inner;

            public RecordingStorage(IPersistentStorage inner)
            {
                _inner = inner;
            }

            public List<RecordingSession> Sessions { get; } = new List<RecordingSession>();

            public bool ThreadSafeSessions { get; set; }

            public IPersistentStorageSession CreateSession()
            {
                var session = new RecordingSession(_inner.CreateSession()) { IsThreadSafe = ThreadSafeSessions };
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
        private sealed class WalkGate : IDisposable
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

        private static async Task<(StateManagerSync<StateManagerMetadata> manager, RecordingStorage storage)> CreateManager(string name, int cachePageCount = 1000, bool useReadCache = false, bool backgroundCommit = true, bool threadSafeSession = false, bool reservoir = false, TimeSpan? stopCommitsTimeout = null, IFileCacheFactory? fileCacheFactory = null)
        {
            IPersistentStorage inner = reservoir
                ? new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
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
                StopCommitsTimeout = stopCommitsTimeout ?? TimeSpan.FromSeconds(10),
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

        [Fact]
        public async Task CommitReturnsWhilePagesAreStillBeingWritten()
        {
            var (manager, storage) = await CreateManager("returns");
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
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "inline", 8);

            await client.Commit().AsTask().WaitAsync(Timeout);

            Assert.All(keys, k => Assert.Equal(1, session.WriteCount(k)));
            Assert.True(((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync().IsCompletedSuccessfully);
            for (int i = 0; i < keys.Count; i++)
            {
                Assert.Equal(i, ReadPersisted(storage, keys[i]));
            }
            manager.Dispose();
        }

        /// <summary>
        /// A pending page has no holder, so a delete leaves it cached until the walk has written it.
        /// </summary>
        [Fact]
        public async Task DeleteOfPendingPageIsHeldUntilItsWrite()
        {
            var (manager, storage) = await CreateManager("delete");
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

        private static ValueTask<IAppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>> CreateAppendTree(StateManagerSync manager)
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
            var (manager, storage) = await CreateManager("sessionoverlap", cachePageCount: 0);
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
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "resetfault", 8);
            session.FaultingKeys[keys[1]] = 1;

            await client.Commit().AsTask().WaitAsync(Timeout);

            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await client.Reset(true).AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
            });
            manager.Dispose();
        }

        /// <summary>
        /// Finding 3: a page whose fetch-time write failed is still owed, the walk must not
        /// commit the generation without it.
        /// </summary>
        [Fact]
        public async Task FailedOnFetchWriteFaultsTheWalk()
        {
            var (manager, storage) = await CreateManager("fetchfault");
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
        /// Finding 5: the manager must stop the walks before it tears the cache table down.
        /// </summary>
        [Fact]
        public async Task DisposeStopsTheWalkBeforeTheCacheTableGoesAway()
        {
            var (manager, storage) = await CreateManager("disposetable");
            var (client, _, keys) = await CreateClientWithPages(manager, storage, "disposetable", 8);
            using var gate = new WalkGate(manager);

            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);

            var dispose = Task.Run(() => manager.Dispose());
            // Dispose has reached its teardown by now, the walk is still parked.
            await Task.Delay(300);

            gate.Release();
            await dispose.WaitAsync(Timeout);

            var walk = ((SyncStateClient<TestPage, TestMetadata>)client).WaitForCommitAsync();
            Assert.NotNull(walk);
            Assert.True(walk.IsCompleted);
            var fault = walk.Exception?.GetBaseException();
            Assert.True(fault == null || fault is ObjectDisposedException, "the walk ran into the torn down cache table: " + fault?.Message);
        }

        /// <summary>
        /// Finding 6: a page held across Commit and changed in place must not leak the change
        /// into the checkpoint, whether or not the holder ever calls AddOrUpdate.
        /// </summary>
        [Fact]
        public async Task HeldPageChangedInPlaceAfterCommitStaysOutOfTheCheckpoint()
        {
            var (manager, storage) = await CreateManager("heldinplace");
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
        /// Review 2, finding 4: dispose gives a wedged walk up after the configured time instead of
        /// waiting on storage forever.
        /// </summary>
        [Fact]
        public async Task DisposeGivesUpAWedgedWalkWithinTheStopTimeout()
        {
            var (manager, storage) = await CreateManager("wedged", stopCommitsTimeout: TimeSpan.FromSeconds(1));
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "wedged", 8);
            using var gate = new ManualResetEventSlim(false);
            session.ArmWriteGate(gate, keys.ToHashSet());

            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            var elapsed = System.Diagnostics.Stopwatch.StartNew();
            var dispose = Task.Run(() => manager.Dispose());
            await dispose.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(1.6), $"dispose took {elapsed.Elapsed.TotalSeconds:F1}s against a 1s stop budget, the budget was spent more than once");
            gate.Set();
        }

        /// <summary>
        /// Review 2, finding 4: the join inside dispose must not need the caller's synchronization context.
        /// </summary>
        [Fact]
        public async Task DisposeDoesNotNeedTheCallersSynchronizationContext()
        {
            var (manager, storage) = await CreateManager("synccontext");
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
            var (manager, storage) = await CreateManager("latedispose", stopCommitsTimeout: TimeSpan.FromSeconds(1));
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
            var (manager, storage) = await CreateManager("relock", stopCommitsTimeout: TimeSpan.FromSeconds(1));
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

            Assert.IsType<ObjectDisposedException>(failure);
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
        /// Review 5: one abandoned drain must not make every later Dispose of the recovered
        /// manager skip its wait.
        /// </summary>
        [Fact]
        public async Task DisposeWaitsAgainAfterAnAbandonedDrainWasRecovered()
        {
            var (manager, storage) = await CreateManager("reabandon", reservoir: true, stopCommitsTimeout: TimeSpan.FromSeconds(2));
            var (client, session, _) = await CreateClientWithPages(manager, storage, "reabandon", 8);
            using var gate = new WalkGate(manager);
            await client.Commit().AsTask().WaitAsync(Timeout);
            await gate.Blocked.WaitAsync(Timeout);
            manager.RequestStopCommits();
            gate.Release();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => manager.CheckpointAsync().AsTask().WaitAsync(Timeout));
            await manager.InitializeAsync().WaitAsync(Timeout);

            // A later stop with a walk in flight gets the bounded wait back.
            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(1));
            using var writeGate = new ManualResetEventSlim(false);
            session.ArmWriteGate(writeGate, new HashSet<long> { key });
            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);

            var dispose = Task.Run(() => manager.Dispose());
            await Task.Delay(300);
            Assert.False(dispose.IsCompleted, "Dispose gave the walk up without waiting");
            writeGate.Set();
            await dispose.WaitAsync(Timeout);
        }

        /// <summary>
        /// Review 5: a restart after the drain gave a wedged walk up must fail within the stop
        /// budget instead of joining the walk forever.
        /// </summary>
        [Fact]
        public async Task RecoveryDoesNotHangOnAWalkTheDrainGaveUp()
        {
            var (manager, storage) = await CreateManager("wedgedrecover", reservoir: true, stopCommitsTimeout: TimeSpan.FromSeconds(1));
            var (client, session, keys) = await CreateClientWithPages(manager, storage, "wedgedrecover", 8);
            using var writeGate = new ManualResetEventSlim(false);
            session.ArmWriteGate(writeGate, keys.ToHashSet());
            await client.Commit().AsTask().WaitAsync(Timeout);
            await session.WriterBlocked.WaitAsync(Timeout);
            manager.RequestStopCommits();

            var failure = await Record.ExceptionAsync(() => manager.InitializeAsync().WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.IsType<InvalidOperationException>(failure);

            writeGate.Set();
            await Task.Delay(500);
            manager.Dispose();
        }

        /// <summary>
        /// Review 5: an idle checkpoint in between must not cost the next active commit a fresh buffer.
        /// </summary>
        [Fact]
        public async Task CommitAllocatesNothingAfterAnIdleCommit()
        {
            const int count = 8192;
            var (manager, storage) = await CreateManager("idlealloc", cachePageCount: count * 2);
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

            // Nothing changed, an idle checkpoint.
            await client.Commit().AsTask().WaitAsync(Timeout);
            await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

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
            await tree.Commit();

            // Every page is fetched and rewritten while the walk is still going.
            for (long i = 0; i < count; i++)
            {
                await tree.Upsert(i, (int)i + 1);
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
    }
}
