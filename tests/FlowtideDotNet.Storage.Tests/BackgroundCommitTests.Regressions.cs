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
using FlowtideDotNet.Storage.StateManager.Internal;
using Xunit;

namespace FlowtideDotNet.Storage.Tests
{
    public partial class BackgroundCommitTests
    {
        /// <summary>
        /// Parks the background writer after it claims the pending key but before it rents
        /// the original cached page. Reimplements ICacheObject so cache rents use this gate.
        /// </summary>
        private sealed class GatedRentPage : TestPage, ICacheObject, IDisposable
        {
            private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly ManualResetEventSlim _release = new(false);
            private int _armed;

            public GatedRentPage(int value) : base(value)
            {
            }

            public Task RentEntered => _entered.Task;

            public void Arm() => Volatile.Write(ref _armed, 1);

            public void ReleaseRent() => _release.Set();

            public new bool TryRent()
            {
                if (Interlocked.Exchange(ref _armed, 0) == 1)
                {
                    _entered.TrySetResult();
                    if (!_release.Wait(Timeout))
                    {
                        throw new TimeoutException("The pending-page rent was not released.");
                    }
                }
                return base.TryRent();
            }

            public void Dispose() => _release.Dispose();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task PendingSnapshotSurvivesReplacementThenDelete(bool recreateAgain)
        {
            var (manager, storage) = await CreateManager($"snapshot_repeat_{recreateAgain}", reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, session, _) = await CreateClientWithPages(manager, storage, "client", 0);
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(1));
                using var gate = new WalkGate(manager);

                await client.Commit().AsTask().WaitAsync(Timeout);
                await gate.Blocked.WaitAsync(Timeout);

                // Every operation below belongs to the next generation. Neither the second
                // delete nor another replacement may overwrite the checkpoint's value 1.
                client.Delete(key);
                client.AddOrUpdate(key, new TestPage(2));
                client.Delete(key);
                if (recreateAgain)
                {
                    client.AddOrUpdate(key, new TestPage(3));
                }

                gate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                Assert.Equal(1, ReadPersisted(storage, key));
                Assert.Equal(1, session.TotalWriteCount(key));
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task PendingSnapshotSurvivesReplacementWhileWriterClaimsPage()
        {
            // Dispose the rent gate after the manager has joined the background task.
            using var original = new GatedRentPage(1);
            var (manager, storage) = await CreateManager("snapshot_claim_replace", reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, session, _) = await CreateClientWithPages(manager, storage, "client", 0);
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, original);
                original.Arm();

                Task? replacement = null;
                try
                {
                    await client.Commit().AsTask().WaitAsync(Timeout);
                    await original.RentEntered.WaitAsync(Timeout);

                    var replacementStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    replacement = Task.Run(() =>
                    {
                        replacementStarted.TrySetResult();
                        client.Delete(key);
                        client.AddOrUpdate(key, new TestPage(2));
                    });
                    await replacementStarted.Task.WaitAsync(Timeout);

                    // The current implementation completes the replacement while the rent is
                    // parked. A fix may serialize replacement behind the rent, which is also
                    // valid: allow that wait, then release the writer instead of deadlocking.
                    await Task.WhenAny(replacement, Task.Delay(TimeSpan.FromSeconds(1)));
                }
                finally
                {
                    original.ReleaseRent();
                }

                Assert.NotNull(replacement);
                await replacement.WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                Assert.Equal(1, ReadPersisted(storage, key));
                Assert.Equal(1, session.TotalWriteCount(key));
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task RecreatedPageRemainsReadableAfterEvictionDuringDeleteGeneration()
        {
            var (manager, storage) = await CreateManager("recreated_page_eviction", cachePageCount: 0, reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, _, keys) = await CreateClientWithPages(manager, storage, "client", 2);
                var key = keys[0];
                var blocker = keys[1];
                await client.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                client.Delete(key);
                client.AddOrUpdate(blocker, new TestPage(11));
                var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                manager.PageWriteHookForTests = (_, page) =>
                {
                    if (page != blocker)
                    {
                        return Task.CompletedTask;
                    }
                    entered.TrySetResult();
                    return release.Task;
                };

                try
                {
                    await client.Commit().AsTask().WaitAsync(Timeout);
                    await entered.Task.WaitAsync(Timeout);

                    // The old deletion has been written, but its generation is still active.
                    client.AddOrUpdate(key, new TestPage(2));
                    await manager.CacheTable.ForceCleanup().WaitAsync(Timeout);
                    Assert.False(manager.CacheTable.TryPeekEntry(key, out _));

                    var fetched = await client.GetValue(key).AsTask().WaitAsync(Timeout);
                    Assert.NotNull(fetched);
                    try
                    {
                        Assert.Equal(2, fetched.Value);
                    }
                    finally
                    {
                        fetched.Return();
                    }
                }
                finally
                {
                    release.TrySetResult();
                    manager.PageWriteHookForTests = null;
                    await ((StateClient)client).WaitForCommitAsync().WaitAsync(Timeout);
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task DeletedReplacementCannotBeRentedFromCache(bool checkpointDeletesPage)
        {
            var (manager, storage) = await CreateManager($"deleted_replacement_{checkpointDeletesPage}", reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, session, keys) = await CreateClientWithPages(manager, storage, "client", 1);
                var key = keys[0];
                if (checkpointDeletesPage)
                {
                    await client.Commit().AsTask().WaitAsync(Timeout);
                    await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                    client.Delete(key);
                }
                using var gate = new WalkGate(manager);
                await client.Commit().AsTask().WaitAsync(Timeout);
                await gate.Blocked.WaitAsync(Timeout);

                client.Delete(key);
                client.AddOrUpdate(key, new TestPage(2));
                client.Delete(key);

                // A cache-only probe must not resurrect the deleted replacement in the lookup table.
                var wasCached = client.TryGetCachedValue(key, out var cached);
                cached?.Return();
                Assert.False(wasCached);
                var fetched = await client.GetValue(key).AsTask().WaitAsync(Timeout);
                fetched?.Return();
                Assert.Null(fetched);
                Assert.False(manager.CacheTable.TryPeekEntry(key, out _));

                gate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                if (checkpointDeletesPage)
                {
                    Assert.False(storage.TryGetValue(key, out _));
                }
                else
                {
                    Assert.Equal(0, ReadPersisted(storage, key));
                }
                Assert.Equal(1, session.TotalWriteCount(key));
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task DeletedReplacementDuringWriteDoesNotDiscardCheckpointSnapshot()
        {
            using var writeGate = new ManualResetEventSlim(false);
            var (manager, storage) = await CreateManager("deleted_replacement_during_write", reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, session, keys) = await CreateClientWithPages(manager, storage, "client", 1);
                var key = keys[0];
                session.ArmWriteGate(writeGate, keys.ToHashSet());
                try
                {
                    // A regression to a synchronous commit must time out without hanging the test thread.
                    await Task.Run(() => client.Commit().AsTask()).WaitAsync(Timeout);
                    await session.WriterBlocked.WaitAsync(Timeout);

                    // The writer owns the original rent; the replacement has no spill marker.
                    client.Delete(key);
                    client.AddOrUpdate(key, new TestPage(2));
                    client.Delete(key);

                    var wasCached = client.TryGetCachedValue(key, out var cached);
                    cached?.Return();
                    Assert.False(wasCached);
                    var fetched = await client.GetValue(key).AsTask().WaitAsync(Timeout);
                    fetched?.Return();
                    Assert.Null(fetched);
                    Assert.False(manager.CacheTable.TryPeekEntry(key, out _));
                }
                finally
                {
                    writeGate.Set();
                }

                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);
                Assert.Equal(0, ReadPersisted(storage, key));
                Assert.Equal(1, session.TotalWriteCount(key));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task NewEmptyQueueCanRecoverAfterCommit(bool backgroundCommit)
        {
            var (manager, storage) = await CreateManager($"new_empty_queue_{backgroundCommit}", backgroundCommit: backgroundCommit, reservoir: true);
            using (storage)
            using (manager)
            {
                var queue = await CreateQueue(manager);
                await queue.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                await manager.InitializeAsync().WaitAsync(Timeout);
                var recovered = await CreateQueue(manager).AsTask().WaitAsync(Timeout);
                Assert.Equal(0, recovered.Count);
                await recovered.Enqueue(2);
                Assert.Equal(2, await recovered.Dequeue());
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task EmptyQueueCanRecoverAfterClearAndCommit()
        {
            var (manager, storage) = await CreateManager("queue_clear_checkpoint", reservoir: true);
            using (storage)
            using (manager)
            {
                var queue = await CreateQueue(manager);
                await queue.Enqueue(1);
                await queue.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                // No enqueue follows Clear, so the empty replacement root itself must be dirty.
                await queue.Clear().AsTask().WaitAsync(Timeout);
                await queue.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                await manager.InitializeAsync().WaitAsync(Timeout);
                var recovered = await CreateQueue(manager).AsTask().WaitAsync(Timeout);
                Assert.Equal(0, recovered.Count);
                await recovered.Enqueue(2);
                Assert.Equal(2, await recovered.Dequeue());
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task PendingSnapshotDoesNotCaptureMutationsAppliedAfterCommitReturns()
        {
            var (manager, storage) = await CreateManager("snapshot_mutation_after_commit", reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, _, _) = await CreateClientWithPages(manager, storage, "client", 0);
                var key = client.GetNewPageId();
                var page = new TestPage(1);
                client.AddOrUpdate(key, page);

                using var gate = new WalkGate(manager);
                await client.Commit().AsTask().WaitAsync(Timeout);
                await gate.Blocked.WaitAsync(Timeout);

                // Mutations applied after commit returns must not leak into snapshot.
                page.Value = 999;

                gate.Release();
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                Assert.Equal(1, ReadPersisted(storage, key));
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task AddingPageWhileBackgroundCommitWorkerWritesPendingSnapshotDoesNotThrow()
        {
            var (manager, storage) = await CreateManager("add_during_write_pending", reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, session, _) = await CreateClientWithPages(manager, storage, "client", 0);
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(1));

                using var writeGate = new ManualResetEventSlim(false);
                session.ArmWriteGate(writeGate, new HashSet<long> { key });

                var commit = Task.Run(() => client.Commit().AsTask());
                await session.WriterBlocked.WaitAsync(Timeout);

                Exception? exception = null;
                try
                {
                    // Writing in-flight page must not throw before checkpoint completes.
                    client.AddOrUpdate(key, new TestPage(2));
                }
                catch (Exception e)
                {
                    exception = e;
                }
                finally
                {
                    writeGate.Set();
                }

                await commit;
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                Assert.Null(exception);
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task UpdatingSpilledPageWhileBackgroundWorkerWritesSessionInvalidatesObsoleteCache()
        {
            var (manager, storage) = await CreateManager("obsolete_cache_race", cachePageCount: 0, useReadCache: true, reservoir: true);
            using (storage)
            using (manager)
            {
                var (client, session, _) = await CreateClientWithPages(manager, storage, "client", 0);
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(1));

                using var writeGate = new ManualResetEventSlim(false);
                session.ArmWriteGate(writeGate, new HashSet<long> { key });

                var commit = Task.Run(() => client.Commit().AsTask());
                await session.WriterBlocked.WaitAsync(Timeout);

                // Update page while worker writes session snapshot.
                client.Delete(key);
                client.AddOrUpdate(key, new TestPage(2));

                writeGate.Set();
                await commit;
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                // Evict replacement so next read queries cache.
                manager.DeleteFromCache(key);

                var fetched = await client.GetValue(key).AsTask().WaitAsync(Timeout);
                Assert.NotNull(fetched);
                Assert.Equal(2, fetched.Value);
            }
        }

        private sealed class InterceptingFileCache : IFileCache
        {
            private readonly IFileCache _inner;
            public Action<long>? OnAfterFree { get; set; }

            public InterceptingFileCache(IFileCache inner) => _inner = inner;
            public void Write(long id, SerializableObject serializableObject) => _inner.Write(id, serializableObject);
            public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => _inner.Read(pageKey);
            public ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer) where T : ICacheObject => _inner.Read(pageKey, serializer);
            public void Free(in long pageKey)
            {
                _inner.Free(pageKey);
                OnAfterFree?.Invoke(pageKey);
            }
            public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);
            public void Flush() => _inner.Flush();
            public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();
            public void Dispose() => _inner.Dispose();
        }

        private sealed class InterceptingFileCacheFactory : IFileCacheFactory
        {
            private readonly IFileCacheFactory _inner;
            public InterceptingFileCache? Cache { get; private set; }

            public InterceptingFileCacheFactory(IFileCacheFactory inner) => _inner = inner;
            public IFileCache Create(string name, IMemoryAllocator memoryAllocator)
            {
                return Cache = new InterceptingFileCache(_inner.Create(name, memoryAllocator));
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task ReadingPageWhileFreeSpillRunsDoesNotThrowSegmentNotFound()
        {
            var cacheFactory = new InterceptingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions()
            {
                DirectoryPath = "./data/test_freespill_race"
            }));
            var (manager, storage) = await CreateManager("freespill_race", cachePageCount: 0, useReadCache: true, fileCacheFactory: cacheFactory, reservoir: true);
            using (storage)
            using (manager)
            {
                await manager.CacheTable.StopCleanupTask();
                var (client, session, _) = await CreateClientWithPages(manager, storage, "client", 0);
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(1));

                // Evict to file cache before committing.
                await manager.CacheTable.ForceCleanup();

                await client.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                Exception? readException = null;
                cacheFactory.Cache!.OnAfterFree = pageKey =>
                {
                    try
                    {
                        // Reading page during FreeSpill must not throw SegmentNotFound.
                        var task = client.GetValue(pageKey);
                        if (task.IsCompleted)
                        {
                            task.GetAwaiter().GetResult();
                        }
                        else
                        {
                            task.AsTask().GetAwaiter().GetResult();
                        }
                    }
                    catch (Exception ex)
                    {
                        readException = ex.InnerException ?? ex;
                    }
                };

                // FreeSpill frees the segment before removing the version entry.
                client.AddOrUpdate(key, new TestPage(2));

                Assert.Null(readException);
            }
        }
    }
}
