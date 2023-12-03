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

using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Simple;
using FASTER.core;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerSimple<TMetadata> : StateManagerSimple
    {
        public StateManagerSimple(StateManagerOptions options) : base(new StateManagerMetadataSerializer<TMetadata>(), options)
        {
        }

        public TMetadata? Metadata
        {
            get
            {
                if (m_metadata is StateManagerMetadata<TMetadata> val)
                {
                    return val.Metadata;
                }
                throw new InvalidOperationException("Metadata type missmatch");
            }
            set
            {
                if (m_metadata is StateManagerMetadata<TMetadata> val)
                {
                    val.Metadata = value;
                    return;
                }
                throw new InvalidOperationException("Metadata type missmatch");
            }
        }

        internal override StateManagerMetadata NewMetadata()
        {
            return new StateManagerMetadata<TMetadata>();
        }
    }

    public abstract class StateManagerSimple : IStateManager
    {
        private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private LRUTableSimple lruTable;
        private SemaphoreSlim m_semaphoreSlim;
        private Dictionary<long, long> m_modified = new Dictionary<long, long>();
        //private FasterLog m_temporaryStorage;
        private FileCache.FileCache m_fileCache;
        private long minimumTempLocation = long.MaxValue;
        private Functions m_functions;
        private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        private readonly Dictionary<string, IStateManagerClient> _clients = new Dictionary<string, IStateManagerClient>();
        private readonly Dictionary<string, StateClient> _stateClients = new Dictionary<string, StateClient>();
        private readonly IStateSerializer<StateManagerMetadata> m_metadataSerializer;
        internal StateManagerMetadata? m_metadata;
        private readonly object m_lock = new object();
        private CancellationTokenSource m_cancellationTokenSource;

        /// <summary>
        /// Used to find the next smallest temporary storage location when persisting a page.
        /// </summary>
        private SortedSet<long> m_storageLocations;

        private DateTimeOffset? emptySince;

        internal StateManagerSimple(IStateSerializer<StateManagerMetadata> metadataSerializer, StateManagerOptions options)
        {
            m_cancellationTokenSource = new CancellationTokenSource();
            m_semaphoreSlim = new SemaphoreSlim(1, 1);
            m_storageLocations = new SortedSet<long>();
            lruTable = new LRUTableSimple(options.CachePageCount, OnCacheRemove);

            m_fileCache = new FileCache.FileCache(options.TemporaryStorageOptions ?? new FileCacheOptions()
            {
                DirectoryPath = "./data/tempFiles"
            }, "simple");
            //m_temporaryStorage = new FASTER.core.FasterLog(new FASTER.core.FasterLogSettings()
            //{
            //    AutoCommit = true,
            //    LogDevice = options.TemporaryStorageFactory.Get(new FileDescriptor()
            //    {
            //        directoryName = "tmp",
            //        fileName = "log"
            //    })
            //});

            m_persistentStorage = new FasterKV<long, SpanByte>(new FasterKVSettings<long, SpanByte>()
            {
                RemoveOutdatedCheckpoints = options.RemoveOutdatedCheckpoints,
                MemorySize = 1024 * 1024 * 16,
                PageSize = 1024 * 1024,
                LogDevice = options.LogDevice,
                CheckpointManager = options.CheckpointManager,
                CheckpointDir = options.CheckpointDir,
                //ConcurrencyControlMode = ConcurrencyControlMode.RecordIsolation
            });
            m_functions = new Functions();
            m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);
            this.m_metadataSerializer = metadataSerializer;
            Task.Factory.StartNew(ClearCacheLoop, TaskCreationOptions.LongRunning);
        }


        private async Task ClearCacheLoop()
        {
            PeriodicTimer periodicTimer = new PeriodicTimer(TimeSpan.FromSeconds(10));
            while (!m_cancellationTokenSource.IsCancellationRequested)
            {
                await periodicTimer.WaitForNextTickAsync();
                await m_semaphoreSlim.WaitAsync(m_cancellationTokenSource.Token);
                if (m_modified.Count < 100)
                {
                    if (emptySince == null)
                    {
                        emptySince = DateTimeOffset.UtcNow;
                    }
                    else
                    {
                        // Clear cache if no operations in last minute 
                        if (emptySince.Value < DateTimeOffset.UtcNow.Subtract(TimeSpan.FromSeconds(60)))
                        {
                            lruTable.ClearAllExcept(m_modified.Keys);
                            emptySince = null;
                        }
                    }
                }
                else
                {
                    emptySince = null;
                }
                m_semaphoreSlim.Release();
            }
        }


        internal long GetNewPageId()
        {
            m_semaphoreSlim.Wait(m_cancellationTokenSource.Token);
            var id = GetNewPageId_Internal();
            m_semaphoreSlim.Release();
            return id;
        }

        private long GetNewPageId_Internal()
        {
            Debug.Assert(m_metadata != null);
            long id = m_metadata.PageCounter;
            m_metadata.PageCounter++;
            return id;
        }

        #region Read
        internal ValueTask<T?> ReadAsync<T>(long key, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            m_semaphoreSlim.Wait(m_cancellationTokenSource.Token);
            if (lruTable.TryGetValue(key, out var val))
            {
                m_semaphoreSlim.Release();
                return ValueTask.FromResult<T?>((T?)val);
            }
            return ReadAsync_Slow(key, serializer, session);
        }

        private async ValueTask<T?> ReadAsync_WaitSlow<T>(Task task, long key, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            await task.ConfigureAwait(false);
            if (lruTable.TryGetValue(key, out var val))
            {
                m_semaphoreSlim.Release();
                return (T?)val;
            }
            return await ReadAsync_Slow(key, serializer, session);
        }

        internal ValueTask<T?> ReadAsync_Slow<T>(long key, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            lock (m_modified)
            {
                if (m_modified.TryGetValue(key, out var storageLocation))
                {
                    return ReadTemporaryAsync_Slow(storageLocation, key, serializer);
                }
            }
            // Read from persistent storage
            var page = session.Read(key);

            if (!page.status.IsCompleted)
            {
                return ReadPersistentAsync_Slow(key, serializer, session);
            }

            var deserialized = serializer.Deserialize(new ByteMemoryOwner(page.output), page.output.Length, new StateSerializeOptions());
            var lruTask = lruTable.Add(key, deserialized, serializer);

            if (!lruTask.IsCompleted)
            {
                return ReadAsyncWaitForLru_Slow(lruTask, deserialized);
            }

            m_semaphoreSlim.Release();
            return ValueTask.FromResult(deserialized);
        }

        private async ValueTask<V?> ReadAsyncWaitForLru_Slow<V>(ValueTask valueTask, V? val)
        {
            try
            {
                await valueTask;
                return val;
            }
            finally
            {
                m_semaphoreSlim.Release();
            }
        } 

        private async ValueTask<T?> ReadTemporaryAsync_Slow<T>(long storageLocation, long key, IStateSerializer<T> serializer)
            where T : ICacheObject
        {
            if (storageLocation == -1)
            {
                m_semaphoreSlim.Release();
                return default;
            }
            if (storageLocation == 0)
            {
                m_semaphoreSlim.Release();
                throw new InvalidOperationException("Unexpected error, modified data not in LRU and not in temporary storage");
            }

            try
            {
                // Read from temporary storage
                var bytes = m_fileCache.Read(key);
                //var (bytes, length) = await m_temporaryStorage.ReadAsync(storageLocation);

                var deserialized = serializer.Deserialize(new ByteMemoryOwner(bytes), bytes.Length, new StateSerializeOptions());
                await lruTable.Add(key, deserialized, serializer);

                return deserialized;
            }
            finally
            {
                m_semaphoreSlim.Release();
            }
        }

        private async ValueTask<T?> ReadPersistentAsync_Slow<T>(long pageId, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            try
            {
                var outputs = await session.CompletePendingWithOutputsAsync();
                if (outputs.Next())
                {
                    if (outputs.Current.Key == pageId)
                    {
                        return serializer.Deserialize(new ByteMemoryOwner(outputs.Current.Output), outputs.Current.Output.Length, new StateSerializeOptions());
                    }
                    else
                    {
                        throw new Exception();
                    }
                }
                throw new Exception();
            }
            finally
            {
                m_semaphoreSlim.Release();
            } 
        }
        #endregion

        internal ValueTask AddOrUpdateAsync<V>(in long key, in V value, in IStateSerializer stateSerializer)
            where V: ICacheObject
        {
            m_semaphoreSlim.Wait(m_cancellationTokenSource.Token);
            lock (m_modified)
            {
                m_modified[key] = 0;
            }
            emptySince = null;

            // TODO: Investigate here if semaphore must be over all
            m_semaphoreSlim.Release();
            return lruTable.Add(key, value, stateSerializer);
        }

        internal async ValueTask AddOrUpdateAsync_Slow<V>(Task waitTask, long key, V value, IStateSerializer stateSerializer)
            where V : ICacheObject
        {
            await waitTask.ConfigureAwait(false);
            lock (m_modified)
            {
                m_modified[key] = 0;
            }
            emptySince = null;

            // TODO: Investigate here if semaphore must be over all
            m_semaphoreSlim.Release();
            await lruTable.Add(key, value, stateSerializer);
        }

        /// <summary>
        /// Resets all keys and removes from temporary storage
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        internal async ValueTask ResetKeys(IEnumerable<long> keys)
        {
            await m_semaphoreSlim.WaitAsync();
            foreach (var key in keys)
            {
                lock (m_modified)
                {
                    if (m_modified.TryGetValue(key, out var location))
                    {
                        if (location == 0)
                        {
                            lruTable.Delete(key);
                            m_fileCache.Free(key);
                        }
                        else
                        {
                            m_fileCache.Free(key);
                            m_storageLocations.Remove(location);
                        }
                        m_modified.Remove(key);
                    }
                }
            }

            //if (m_storageLocations.Min > minimumTempLocation)
            //{
            //    m_temporaryStorage.TruncateUntil(m_storageLocations.Min);
            //    minimumTempLocation = m_storageLocations.Min;
            //    await m_temporaryStorage.CommitAsync();
            //}

            m_semaphoreSlim.Release();
        }

        internal async ValueTask Commit(IEnumerable<long> keys, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            await m_semaphoreSlim.WaitAsync();
            foreach(var key in keys)
            {
                Monitor.Enter(m_modified);
                if (m_modified.TryGetValue(key, out var location))
                {
                    Monitor.Exit(m_modified);
                    //Monitor.Exit(m_modified);
                    if (location == -1)
                    {
                        // Delete
                        await DeleteAsync(key, session);
                        m_fileCache.Free(key);
                        m_modified.Remove(key);
                    }
                    else if (location == 0)
                    {
                        // from LRU
                        if (lruTable.TryGetValueNoRefresh(key, out var val))
                        {
                            // Serialize the value
                            var bytes = val!.Value.stateSerializer.Serialize(val.Value.value, new StateSerializeOptions());

                            // Write to persistent storage
                            await WriteAsync(key, bytes, session);
                            m_fileCache.Free(key);
                            m_modified.Remove(key);
                        }
                        else
                        {
                            throw new InvalidOperationException("Modified page is missing in LRU table, and does not exist in temporary storage.");
                        }
                    }
                    else
                    {
                        // Upsert from temporary storage
                        var bytes = m_fileCache.Read(key);
                        //var (bytes, length) = await m_temporaryStorage.ReadAsync(location);
                        await WriteAsync(key, bytes, session);
                        m_fileCache.Free(key);
                        m_storageLocations.Remove(location);
                        m_modified.Remove(key);
                    }
                }
                else
                {
                    Monitor.Exit(m_modified);
                }
                
            }
            // Check if the temporary storage can be truncated
            //if (m_storageLocations.Min > minimumTempLocation)
            //{
            //    m_temporaryStorage.TruncateUntil(m_storageLocations.Min);
            //    minimumTempLocation = m_storageLocations.Min;
            //    await m_temporaryStorage.CommitAsync();
            //}
            m_semaphoreSlim.Release();
        }

        internal void Delete(in long pageId)
        {
            m_semaphoreSlim.Wait();
            lock (m_modified)
            {
                m_modified[pageId] = -1;
            }
            lruTable.Delete(pageId);
            m_semaphoreSlim.Release();
        }

        private ValueTask DeleteAsync(in long pageId, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            var status = session.Delete(pageId);
            if (!status.IsCompletedSuccessfully)
            {
                return WriteAsyncSlow(session);
            }
            return ValueTask.CompletedTask;
        }

        internal async ValueTask WritePersistentAsync(long pageId, byte[] bytes, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            await m_semaphoreSlim.WaitAsync();
            await WriteAsync(pageId, bytes, session);
            m_semaphoreSlim.Release();
        }

        internal async ValueTask<byte[]> ReadPersistentAsync(long pageId, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            await m_semaphoreSlim.WaitAsync();
            var page = await session.ReadAsync(pageId);

            var (status, bytes) = page.Complete();
            m_semaphoreSlim.Release();
            return bytes;
        }

        private ValueTask WriteAsync(in long pageId, in byte[] bytes, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            var upsertOp = session.Upsert(pageId, SpanByte.FromFixedSpan(bytes));
            if (!upsertOp.IsCompletedSuccessfully)
            {
                return WriteAsyncSlow(session);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask WriteAsyncSlow(ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            await session.CompletePendingAsync();
        }

        internal async ValueTask<IStateClient<TValue, TMetadata>> CreateClientAsync<TValue, TMetadata>(string client, StateClientOptions<TValue> options)
            where TValue : ICacheObject
        {
            await m_semaphoreSlim.WaitAsync(m_cancellationTokenSource.Token);
            if (_stateClients.TryGetValue(client, out var cachedClient))
            {
                m_semaphoreSlim.Release();
                return (cachedClient as SimpleStateClient<TValue, TMetadata>)!;
            }
            if (m_metadata.ClientMetadataLocations.TryGetValue(client, out var location))
            {
                var readAsyncOp = await m_adminSession.ReadAsync(location);
                var (_, v) = readAsyncOp.Complete();
                var metadata = StateClientMetadataSerializer.Instance.Deserialize<TMetadata>(new ByteMemoryOwner(v), v.Length);
                var session = m_persistentStorage.For(m_functions).NewSession(m_functions);
                var stateClient = new SimpleStateClient<TValue, TMetadata>(this, location, metadata, session, options);
                _stateClients.Add(client, stateClient);
                m_semaphoreSlim.Release();
                return stateClient;
            }
            else
            {
                // Allocate a new page id for the client metadata.
                var clientMetadataPageId = GetNewPageId_Internal();
                var clientMetadata = new StateClientMetadata<TMetadata>();
                m_metadata.ClientMetadataLocations.Add(client, clientMetadataPageId);
                await m_adminSession.UpsertAsync(clientMetadataPageId, SpanByte.FromFixedSpan(StateClientMetadataSerializer.Instance.Serialize(clientMetadata)));
                var session = m_persistentStorage.For(m_functions).NewSession(m_functions);
                var stateClient = new SimpleStateClient<TValue, TMetadata>(this, clientMetadataPageId, clientMetadata, session, options);
                _stateClients.Add(client, stateClient);
                m_semaphoreSlim.Release();
                return stateClient;
            }
        }

        public bool Initialized { get; private set; }

        public async ValueTask CheckpointAsync()
        {
            m_metadata.CheckpointVersion = m_persistentStorage.CurrentVersion;
            var bytes = m_metadataSerializer.Serialize(m_metadata, new StateSerializeOptions());
            await m_adminSession.UpsertAsync(1, SpanByte.FromFixedSpan(bytes));

            var guid = await BeginCheckpointAsync();
        }

        internal async ValueTask<Guid> BeginCheckpointAsync()
        {
            bool success = false;
            Guid token;
            do
            {
                (success, token) = await m_persistentStorage.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
            } while (!success);
            return token;
        }

        public async Task Compact()
        {
            m_adminSession.Compact(m_persistentStorage.Log.SafeReadOnlyAddress, CompactionType.Lookup);
            m_persistentStorage.Log.Truncate();
            await m_persistentStorage.TakeFullCheckpointAsync(CheckpointType.Snapshot);
        }

        public IStateManagerClient GetOrCreateClient(string name)
        {
            m_semaphoreSlim.Wait();
            if (_clients.TryGetValue(name, out var client))
            {
                m_semaphoreSlim.Release();
                return client;
            }
            else
            {
                client = new StateManagerSimpleClient(name, this);
                _clients.Add(name, client);
                m_semaphoreSlim.Release();
                return client;
            }
        }

        internal abstract StateManagerMetadata NewMetadata();

        public async Task InitializeAsync()
        {
            bool newMetadata = false;
            try
            {
                await m_semaphoreSlim.WaitAsync(m_cancellationTokenSource.Token);
                lruTable.Clear();
                await m_persistentStorage.RecoverAsync().ConfigureAwait(false);
                var data = await m_adminSession.ReadAsync(1);
                if (data.Status.Found)
                {
                    m_metadata = m_metadataSerializer.Deserialize(new ByteMemoryOwner(data.Output), data.Output.Length, new StateSerializeOptions());
                    await m_persistentStorage.RecoverAsync(recoverTo: m_metadata.CheckpointVersion).ConfigureAwait(false);
                }
                else
                {
                    m_metadata = NewMetadata();
                    m_persistentStorage.Reset();
                    newMetadata = true;
                }
            }
            catch
            {
                m_metadata = NewMetadata();
                newMetadata = true;
            }

            Initialized = true;
            m_semaphoreSlim.Release();

            // Reset cached values in the state clients
            foreach (var stateClient in _stateClients)
            {
                await stateClient.Value.Reset(newMetadata);
            }

            
        }

        internal async ValueTask OnCacheRemove(IEnumerable<LRUTableSimple.LinkedListValue> toBeRemoved)
        {
            //await m_semaphoreSlim.WaitAsync();
            bool enqueued = false;
            try
            {
                foreach (var val in toBeRemoved)
                {
                    //lock (m_modified)
                    //{
                        if (m_modified.TryGetValue(val.key, out var storageIndex) && storageIndex == 0)
                        {
                            // Store the modified data to disk in temporary storage
                            // must lock the data before writing since this call can come from another thread
                            val.value.EnterWriteLock();
                            var bytes = val.stateSerializer.Serialize(val.value, new StateSerializeOptions());
                            
                            //var location = m_temporaryStorage.Enqueue(val.stateSerializer.Serialize(val.value));

                            //if (minimumTempLocation > location)
                            //{
                            //    minimumTempLocation = location;
                            //}
                            //m_storageLocations.Add(location);

                            
                            val.value.ExitWriteLock();
                        m_modified[val.key] = 1;
                        m_fileCache.WriteAsync(val.key, bytes);
                        enqueued = true;
                        }
                    //}
                }
                // Flush to disk
                m_fileCache.Flush();
            }
            finally
            {
               // m_semaphoreSlim.Release();
            }
            
        }
    }
}
