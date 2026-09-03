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

using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Sources;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// Regression test for a SynchronizationLockException thrown from the bulk inserter's n-way split.
    ///
    /// Node write locks are monitors and therefore thread affine. The n-way split used to fetch the next
    /// leaf, to update its previous pointer, while holding the write lock of the leaf being split. When
    /// that fetch completed asynchronously the continuation resumed on another thread, and the following
    /// Monitor.Exit threw "Object synchronization method was called from an unsynchronized block of code",
    /// leaving the leaf locked forever.
    ///
    /// The file cache always reads pages synchronously, so the asynchronous path cannot be reached by
    /// cache tuning alone. The test injects a file cache that completes the read of one specific page on
    /// another thread, which makes the failure deterministic instead of a race.
    /// </summary>
    public class BulkInserterAsyncFetchLockTests
    {
        [Fact]
        public async Task NWaySplitWithNonResidentNextLeafDoesNotLeakLeafWriteLock()
        {
            var fileCacheFactory = new AsyncReadFileCacheFactory(
                new DefaultFileCacheFactory(new FileCacheOptions() { DirectoryPath = "./data/asyncFetchLockTemp" }));

            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                // No pages may stay resident, the next leaf must be read back through the file cache.
                CachePageCount = 0,
                MinCachePageCount = 0,
                FileCacheFactory = fileCacheFactory,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = "./data/asyncFetchLockPersist" })
            }, NullLoggerFactory.Instance, new Meter("async_fetch_lock_test"), "async_fetch_lock_test", GlobalMemoryManager.Instance);

            using var stateManagerScope = stateManager;

            await stateManager.InitializeAsync();

            // The test decides when pages leave memory, the background eviction thread would make it timing dependent.
            await stateManager.CacheTable.StopCleanupTask();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = (BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)
                await nodeClient.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
                    new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                    {
                        PageSizeBytes = 100,
                        UseByteBasedPageSizes = true,
                        // Gates the next leaf fetch that used to run under the leaf write lock.
                        UsePreviousPointers = true,
                        Comparer = new PrimitiveListComparer<long>(),
                        KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                        ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                        MemoryAllocator = GlobalMemoryManager.Instance
                    });

            var bulkInserter = new BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(tree);

            // Seed with gaps between the keys so the second batch fits inside the first leaf.
            var seedCount = 100;
            var seedKeys = new long[seedCount];
            var seedValues = new long[seedCount];
            for (int i = 0; i < seedCount; i++)
            {
                seedKeys[i] = i * 10;
                seedValues[i] = i;
            }
            await bulkInserter.ApplyBatch(seedKeys, seedValues, seedCount, new UpsertMutator(), seedCount * 8);

            var firstLeafTask = tree.m_stateClient.GetValue(tree.m_stateClient.Metadata!.Left);
            var firstLeaf = (LeafNode<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)(await firstLeafTask)!;
            var secondLeafId = firstLeaf.next;
            var lowestKey = firstLeaf.keys.Get(0);
            var highestKey = firstLeaf.keys.Get(firstLeaf.keys.Count - 1);
            firstLeaf.Return();

            Assert.NotEqual(0, secondLeafId);

            // Push all pages out of memory, the next leaf now has to be read back from the file cache.
            await stateManager.CacheTable.ForceCleanup();

            fileCacheFactory.SetTrappedPage(secondLeafId);

            // Keys strictly inside the first leaf, so the batch only routes to that leaf and the second
            // leaf is only touched by the previous pointer update after the split.
            var fillKeyList = new List<long>();
            for (var key = lowestKey + 1; key < highestKey; key++)
            {
                if (key % 10 != 0)
                {
                    fillKeyList.Add(key);
                }
            }
            var fillKeys = fillKeyList.ToArray();
            var fillValues = new long[fillKeys.Length];
            Assert.NotEmpty(fillKeys);

            // Run on a dedicated thread so no synchronization context is captured at the await, the
            // continuation then resumes on the file cache thread instead of the one holding the lock.
            await Task.Factory.StartNew(
                    () => bulkInserter.ApplyBatch(fillKeys, fillValues, fillKeys.Length, new UpsertMutator(), fillKeys.Length * 8).AsTask(),
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)
                .Unwrap()
                .WaitAsync(TimeSpan.FromSeconds(30));

            // Without these the test would silently pass if the batch stopped reaching the split path.
            Assert.Equal(1, bulkInserter.LeafHitCount);
            Assert.True(fileCacheFactory.TrapHits > 0, "The next leaf was served from memory, the asynchronous fetch was never exercised.");

            var contents = await ReadAllFromTree(tree);
            Assert.Equal(seedCount + fillKeys.Length, contents.Count);
        }

        private static async Task<List<(long key, long value)>> ReadAllFromTree(
            BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> tree)
        {
            var result = new List<(long key, long value)>();
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    result.Add((kv.Key, kv.Value));
                }
            }
            return result;
        }

        private struct UpsertMutator : IRowMutator<long, long>
        {
            public void GetSizePrefixSum(long[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    sizes[i] = 8 * (i + 1);
                }
            }

            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData, int sortedIndex)
            {
                return GenericWriteOperation.Upsert;
            }
        }

        /// <summary>
        /// Wraps a file cache and completes the read of one page asynchronously, on a thread that is
        /// never the caller's thread.
        /// </summary>
        private sealed class AsyncReadFileCacheFactory : IFileCacheFactory
        {
            private readonly IFileCacheFactory _inner;
            private long _trappedPage = -1;
            private int _trapHits;

            public AsyncReadFileCacheFactory(IFileCacheFactory inner)
            {
                _inner = inner;
            }

            public int TrapHits => Volatile.Read(ref _trapHits);

            public void SetTrappedPage(long pageKey)
            {
                Volatile.Write(ref _trappedPage, pageKey);
            }

            public IFileCache Create(string name, IMemoryAllocator memoryAllocator)
            {
                return new AsyncReadFileCache(this, _inner.Create(name, memoryAllocator));
            }

            private sealed class AsyncReadFileCache : IFileCache
            {
                private readonly AsyncReadFileCacheFactory _owner;
                private readonly IFileCache _inner;

                public AsyncReadFileCache(AsyncReadFileCacheFactory owner, IFileCache inner)
                {
                    _owner = owner;
                    _inner = inner;
                }

                public ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer)
                    where T : ICacheObject
                {
                    var readTask = _inner.Read(pageKey, serializer);
                    if (pageKey != Volatile.Read(ref _owner._trappedPage))
                    {
                        return readTask;
                    }
                    Interlocked.Increment(ref _owner._trapHits);
                    var value = readTask.IsCompletedSuccessfully ? readTask.Result : readTask.AsTask().GetAwaiter().GetResult();
                    return new ForeignThreadValueTaskSource<T>(value).AsValueTask();
                }

                public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => _inner.Read(pageKey);

                public void Write(long id, SerializableObject serializableObject) => _inner.Write(id, serializableObject);

                public void Free(in long pageKey) => _inner.Free(pageKey);

                public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);

                public void Flush() => _inner.Flush();

                public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();

                public void Dispose() => _inner.Dispose();
            }
        }

        /// <summary>
        /// Always reports pending, and runs the continuation on a new thread. This is what makes the
        /// regression deterministic, the awaiting code can never resume on the thread that took the lock.
        /// </summary>
        private sealed class ForeignThreadValueTaskSource<T> : IValueTaskSource<T>
        {
            private readonly T _result;
            private int _completed;

            public ForeignThreadValueTaskSource(T result)
            {
                _result = result;
            }

            public ValueTask<T> AsValueTask() => new ValueTask<T>(this, 0);

            public ValueTaskSourceStatus GetStatus(short token)
            {
                return Volatile.Read(ref _completed) == 1 ? ValueTaskSourceStatus.Succeeded : ValueTaskSourceStatus.Pending;
            }

            public T GetResult(short token) => _result;

            public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            {
                // The scheduling context is ignored on purpose, the resume must not land on the calling thread.
                var thread = new Thread(() =>
                {
                    Volatile.Write(ref _completed, 1);
                    continuation(state);
                })
                {
                    IsBackground = true
                };
                thread.Start();
            }
        }
    }
}
