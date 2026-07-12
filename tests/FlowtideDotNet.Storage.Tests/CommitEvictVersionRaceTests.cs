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

using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using Xunit;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// Reproduces the Commit-vs-Evict version race deterministically: an eviction that
    /// straddles a commit leaves a m_fileCacheVersion entry behind whose counter value can
    /// collide with the next commit interval's counter (both restarting from the same
    /// numbering), making the eviction dedup skip serializing a genuinely modified page —
    /// the modified content is then dropped from memory and lost.
    /// </summary>
    public class CommitEvictVersionRaceTests
    {
        private class TestPage : ICacheObject
        {
            private int _rentCount = 1;

            public TestPage(int value)
            {
                Value = value;
            }

            public int Value { get; set; }

            public bool RemovedFromCache { get; set; }

            public int RentCount => Volatile.Read(ref _rentCount);

            public bool TryRent()
            {
                var local = Volatile.Read(ref _rentCount);
                while (true)
                {
                    if (local == 0)
                    {
                        return false;
                    }
                    var observed = Interlocked.CompareExchange(ref _rentCount, local + 1, local);
                    if (observed == local)
                    {
                        return true;
                    }
                    local = observed;
                }
            }

            public void Return()
            {
                Interlocked.Decrement(ref _rentCount);
            }

            public bool TryReclaimForEviction()
            {
                return Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1;
            }

            public void EnterWriteLock() => Monitor.Enter(this);

            public void ExitWriteLock() => Monitor.Exit(this);
        }

        private class TestPageSerializer : IStateSerializer<TestPage>
        {
            public void Serialize(in IBufferWriter<byte> bufferWriter, in TestPage value)
            {
                var span = bufferWriter.GetSpan(4);
                BinaryPrimitives.WriteInt32LittleEndian(span, value.Value);
                bufferWriter.Advance(4);
            }

            public TestPage Deserialize(ReadOnlySequence<byte> bytes, int length)
            {
                var reader = new SequenceReader<byte>(bytes);
                if (!reader.TryReadLittleEndian(out int value))
                {
                    throw new InvalidOperationException("Corrupt test page");
                }
                return new TestPage(value);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
                => Serialize(bufferWriter, (TestPage)value);

            public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
                => Deserialize(bytes, length);

            public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
                where TMetadata : IStorageMetadata => Task.CompletedTask;

            public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
                where TMetadata : IStorageMetadata => Task.CompletedTask;

            public void ClearTemporaryAllocations()
            {
            }

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// Flags whenever two threads are inside <see cref="Serialize(in IBufferWriter{byte}, in TestPage)"/>
        /// at once. A commit and a background eviction both serialize pages through the state client's
        /// single (non-thread-safe) value serializer, so a concurrent serialize corrupts a page's bytes.
        /// The short spin widens the window so a real overlap is observed rather than missed.
        /// </summary>
        private class ConcurrencyTrackingSerializer : IStateSerializer<TestPage>
        {
            private int _active;

            public volatile bool ConcurrencyDetected;

            public void Serialize(in IBufferWriter<byte> bufferWriter, in TestPage value)
            {
                if (Interlocked.Increment(ref _active) > 1)
                {
                    ConcurrencyDetected = true;
                }
                Thread.SpinWait(2000);
                var span = bufferWriter.GetSpan(4);
                BinaryPrimitives.WriteInt32LittleEndian(span, value.Value);
                bufferWriter.Advance(4);
                Interlocked.Decrement(ref _active);
            }

            public TestPage Deserialize(ReadOnlySequence<byte> bytes, int length)
            {
                var reader = new SequenceReader<byte>(bytes);
                if (!reader.TryReadLittleEndian(out int value))
                {
                    throw new InvalidOperationException("Corrupt test page");
                }
                return new TestPage(value);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
                => Serialize(bufferWriter, (TestPage)value);

            public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
                => Deserialize(bytes, length);

            public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
                where TMetadata : IStorageMetadata => Task.CompletedTask;

            public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
                where TMetadata : IStorageMetadata => Task.CompletedTask;

            public void ClearTemporaryAllocations()
            {
            }

            public void Dispose()
            {
            }
        }

        private class TestMetadata : IStorageMetadata
        {
            public bool Updated { get; set; }
        }

        /// <summary>
        /// Wraps a file cache so a test can freeze an eviction inside its spill write while
        /// other work (a commit) runs through the open window.
        /// </summary>
        private class GatedFileCache : IFileCache
        {
            private readonly IFileCache _inner;
            private readonly SemaphoreSlim _writerBlocked = new SemaphoreSlim(0);
            private ManualResetEventSlim? _gate;
            private ManualResetEventSlim? _gateAfterWrite;

            public GatedFileCache(IFileCache inner)
            {
                _inner = inner;
            }

            public void ArmGate(ManualResetEventSlim gate)
            {
                Volatile.Write(ref _gate, gate);
            }

            /// <summary>
            /// Blocks the writer AFTER the inner write has completed, freezing an eviction
            /// between writing its spill data and whatever bookkeeping follows the write.
            /// </summary>
            public void ArmGateAfterWrite(ManualResetEventSlim gate)
            {
                Volatile.Write(ref _gateAfterWrite, gate);
            }

            public Task WaitForBlockedWriterAsync()
            {
                return _writerBlocked.WaitAsync();
            }

            public void Write(long id, SerializableObject serializableObject)
            {
                var gate = Interlocked.Exchange(ref _gate, null);
                if (gate != null)
                {
                    _writerBlocked.Release();
                    gate.Wait();
                }
                _inner.Write(id, serializableObject);
                var afterGate = Interlocked.Exchange(ref _gateAfterWrite, null);
                if (afterGate != null)
                {
                    _writerBlocked.Release();
                    afterGate.Wait();
                }
            }

            public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => _inner.Read(pageKey);

            public ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer)
                where T : ICacheObject => _inner.Read(pageKey, serializer);

            public void Free(in long pageKey) => _inner.Free(pageKey);

            public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);

            public void Flush() => _inner.Flush();

            public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();

            public void Dispose() => _inner.Dispose();
        }

        private class GatedFileCacheFactory : IFileCacheFactory
        {
            private readonly IFileCacheFactory _inner;

            public GatedFileCacheFactory(IFileCacheFactory inner)
            {
                _inner = inner;
            }

            public List<GatedFileCache> Created { get; } = new List<GatedFileCache>();

            public IFileCache Create(string name, IMemoryAllocator memoryAllocator)
            {
                var cache = new GatedFileCache(_inner.Create(name, memoryAllocator));
                lock (Created)
                {
                    Created.Add(cache);
                }
                return cache;
            }
        }

        /// <summary>
        /// Reproduces the "Segment not found" flake seen with CachePageCount = 0: an eviction
        /// writes its spill data BEFORE the commit's file-cache frees run, but records its
        /// m_fileCacheVersion entry AFTER the commit cleared the version map. The leftover
        /// version entry then points at freed file-cache storage, and the next read of the
        /// page throws instead of falling back to persistent storage.
        /// </summary>
        [Fact]
        public async Task EvictionStraddlingCommitMustNotLeaveVersionEntryForFreedData()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./commitEvictSegmentNotFound"
            });
            var factory = new GatedFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions()
            {
                DirectoryPath = "./commitEvictSegmentNotFoundTmp"
            }));
            var options = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                MinCachePageCount = 0,
                FileCacheFactory = factory
            };
            var manager = new StateManagerSync<StateManagerMetadata>(options, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp5"), "test", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            await manager.CacheTable.StopCleanupTask();

            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                "client",
                new StateClientOptions<TestPage>() { ValueSerializer = new TestPageSerializer() },
                GlobalMemoryManager.Instance);

            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(1));

            // Freeze the eviction between writing its spill data and recording the version
            // entry, run a full commit through the window (freeing the spill and clearing
            // the version map), then let the eviction finish its bookkeeping.
            var gate = new ManualResetEventSlim(false);
            var fileCache = factory.Created.Single();
            fileCache.ArmGateAfterWrite(gate);
            var cleanup = Task.Run(() => manager.CacheTable.ForceCleanup());
            await fileCache.WaitForBlockedWriterAsync();
            await client.Commit();
            gate.Set();
            await cleanup;

            // The page must be readable: either from a live spill or from persistent
            // storage. A version entry pointing at freed data throws "Segment not found".
            var after = await client.GetValue(key);
            Assert.NotNull(after);
            Assert.Equal(1, after!.Value);
            after.Return();
            manager.Dispose();
        }

        [Fact]
        public async Task WriteAfterStraddlingEvictionIsNotLost()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./commitEvictVersionRace"
            });
            var factory = new GatedFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions()
            {
                DirectoryPath = "./commitEvictVersionRaceTmp"
            }));
            var options = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                MinCachePageCount = 0,
                FileCacheFactory = factory
            };
            var manager = new StateManagerSync<StateManagerMetadata>(options, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp4"), "test", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            await manager.CacheTable.StopCleanupTask();

            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                "client",
                new StateClientOptions<TestPage>() { ValueSerializer = new TestPageSerializer() },
                GlobalMemoryManager.Instance);

            var key = client.GetNewPageId();
            client.AddOrUpdate(key, new TestPage(1));

            // Freeze the eviction inside its spill write, run a full commit through the
            // window, then let the eviction finish. The eviction now records a version
            // entry that survived the commit.
            var gate = new ManualResetEventSlim(false);
            var fileCache = factory.Created.Single();
            fileCache.ArmGate(gate);
            var cleanup = Task.Run(() => manager.CacheTable.ForceCleanup());
            await fileCache.WaitForBlockedWriterAsync();
            await client.Commit();
            gate.Set();
            await cleanup;

            // Next commit interval: reload the page and modify it once.
            var reloaded = await client.GetValue(key);
            Assert.NotNull(reloaded);
            Assert.Equal(1, reloaded!.Value);
            reloaded.Value = 2;
            client.AddOrUpdate(key, reloaded);
            reloaded.Return();

            // Evict again: if the new write's version collides with the stale entry left
            // by the straddling eviction, the spill write is skipped while the modified
            // page is still dropped from memory.
            await manager.CacheTable.ForceCleanup();

            var after = await client.GetValue(key);
            Assert.NotNull(after);
            Assert.Equal(2, after!.Value);
            after.Return();
            manager.Dispose();
        }

        /// <summary>
        /// A checkpoint commit and a background eviction both serialize pages through the same
        /// (non-thread-safe) state client value serializer. Under CachePageCount = 0 the background
        /// eviction task runs constantly, so without making the commit mutually exclusive with
        /// eviction the two serialize a page at the same time and corrupt the bytes the commit
        /// persists — a corruption that stays hidden behind the intact in-memory copy until a crash
        /// reverts to the corrupted checkpoint (observed as scrambled leaves in the acceptance suite).
        /// The commit must pause the background eviction task so the serializer is never used
        /// concurrently.
        /// </summary>
        [Fact]
        public async Task CommitAndBackgroundEvictionNeverSerializeConcurrently()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./commitEvictSerializerConcurrency"
            });
            var options = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                MinCachePageCount = 0
            };
            var serializer = new ConcurrencyTrackingSerializer();
            var manager = new StateManagerSync<StateManagerMetadata>(options, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmpConcur"), "test", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            // Leave the background cleanup task running so it evicts (and serializes) concurrently.

            var client = await manager.CreateClientAsync<TestPage, TestMetadata>(
                "client",
                new StateClientOptions<TestPage>() { ValueSerializer = serializer },
                GlobalMemoryManager.Instance);

            // Held pages keep an extra rent so they stay in the cache and every commit serializes
            // them, while the steady stream of throwaway pages keeps the background eviction task
            // serializing victims. Without the commit/eviction pause the two overlap.
            var held = new List<(long key, TestPage page)>();
            for (int i = 0; i < 16; i++)
            {
                var k = client.GetNewPageId();
                client.AddOrUpdate(k, new TestPage(i));
                var v = await client.GetValue(k);
                held.Add((k, v!));
            }

            for (int round = 0; round < 300 && !serializer.ConcurrencyDetected; round++)
            {
                foreach (var (k, page) in held)
                {
                    page.Value += 1;
                    client.AddOrUpdate(k, page);
                }
                for (int j = 0; j < 32; j++)
                {
                    var tk = client.GetNewPageId();
                    client.AddOrUpdate(tk, new TestPage(round * 1000 + j));
                }
                await client.Commit();
            }

            Assert.False(serializer.ConcurrencyDetected, "commit serialized a page concurrently with a background eviction");

            foreach (var (_, page) in held)
            {
                page.Return();
            }
            manager.Dispose();
        }
    }
}
