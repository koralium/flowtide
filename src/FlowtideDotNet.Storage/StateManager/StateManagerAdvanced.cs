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

using FlowtideDotNet.Storage.StateManager.Internal.Simple;
using FlowtideDotNet.Storage.StateManager.Internal;
using FASTER.core;
using System.Diagnostics;
using FlowtideDotNet.Storage.StateManager.Internal.Avanced;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerAdvanced<TMetadata> : StateManagerAdvanced
    {
        public StateManagerAdvanced(StateManagerOptions options) : base(new StateManagerMetadataSerializer<TMetadata>(), options)
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

    public abstract class StateManagerAdvanced : IStateManager
    {
        private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private LruTableAdvanced lruTable;
        private Dictionary<long, long> m_modified = new Dictionary<long, long>();
        //private FasterLog m_temporaryStorage;
        private long minimumTempLocation = long.MaxValue;
        private Functions m_functions;
        private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        private readonly Dictionary<string, IStateManagerClient> _clients = new Dictionary<string, IStateManagerClient>();
        private readonly Dictionary<string, StateClient> _stateClients = new Dictionary<string, StateClient>();
        private readonly IStateSerializer<StateManagerMetadata> m_metadataSerializer;
        internal StateManagerMetadata? m_metadata;
        private readonly object m_lock = new object();
        private SemaphoreSlim m_commitSemaphore = new SemaphoreSlim(1, 1);
        private FileCache.FileCache m_fileCache;
        /// <summary>
        /// Used to find the next smallest temporary storage location when persisting a page.
        /// </summary>
        private SortedSet<long> m_storageLocations;
        
        internal StateManagerAdvanced(IStateSerializer<StateManagerMetadata> metadataSerializer, StateManagerOptions options)
        {
            m_storageLocations = new SortedSet<long>();
            lruTable = new LruTableAdvanced(options.CachePageCount, OnCacheRemove);

            m_fileCache = new FileCache.FileCache(new FileCacheOptions()
            {
                DirectoryPath = "./data/tempFiles"
            }, "advanced");
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
                RemoveOutdatedCheckpoints = true,
                //MutableFraction = 0.1,
                MemorySize = 1024 * 1024 * 128,
                PageSize = 1024 * 1024 * 16,
                LogDevice = options.LogDevice,
                CheckpointManager = options.CheckpointManager,
                CheckpointDir = options.CheckpointDir,
                //ReadCacheEnabled = true,
                //ReadCopyOptions = ReadCopyOptions.None
                //ConcurrencyControlMode = ConcurrencyControlMode.RecordIsolation
            });
            m_functions = new Functions();
            m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);
            this.m_metadataSerializer = metadataSerializer;
        }

        public bool Initialized { get; private set; }

        internal long GetNewPageId()
        {
            lock (m_lock)
            {
                return GetNewPageId_Internal();
            }
        }

        private long GetNewPageId_Internal()
        {
            Debug.Assert(m_metadata != null);
            long id = m_metadata.PageCounter;
            m_metadata.PageCounter++;
            return id;
        }

        internal ValueTask<T?> ReadAsync<T>(long key, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            // Lru table is thread safe
            if (lruTable.TryGetValue(key, out var val))
            {
                return ValueTask.FromResult<T?>((T?)val);
            }
            return ReadAsync_Slow(key, serializer, session);
        }

        internal ValueTask<T?> ReadAsync_Slow<T>(long key, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            lock (m_lock)
            {
                if (m_modified.TryGetValue(key, out var storageLocation))
                {
                    return ReadTemporaryAsync_Slow(storageLocation, key, serializer);
                }
            }
            // Read from persistent storage, thread safe based on the session
            var page = session.Read(key);
            
            if (!page.status.IsCompleted)
            {
                return ReadPersistentAsync_Slow(key, serializer, session);
            }

            if (page.output.Length > 0 && page.output[0] == 0)
            {

            }
            var deserialized = serializer.Deserialize(new ByteMemoryOwner(page.output), page.output.Length);
            var lruTask = lruTable.Add(key, deserialized, serializer);

            if (!lruTask.IsCompleted)
            {
                return ReadAsyncWaitForLru_Slow(lruTask, deserialized);
            }

            return ValueTask.FromResult(deserialized);
        }

        private async ValueTask<V?> ReadAsyncWaitForLru_Slow<V>(ValueTask valueTask, V? val)
        {
            await valueTask;
            return val;
        }

        private async ValueTask<T?> ReadPersistentAsync_Slow<T>(long pageId, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            var outputs = await session.CompletePendingWithOutputsAsync();
            if (outputs.Next())
            {
                if (outputs.Current.Key == pageId)
                {
                    return serializer.Deserialize(new ByteMemoryOwner(outputs.Current.Output), outputs.Current.Output.Length);
                }
                else
                {
                    throw new Exception();
                }
            }
            throw new Exception();
        }

        private async ValueTask<T?> ReadTemporaryAsync_Slow<T>(long storageLocation, long key, IStateSerializer<T> serializer)
           where T : ICacheObject
        {
            if (storageLocation == -1)
            {
                return default;
            }
            if (storageLocation == 0)
            {
                throw new InvalidOperationException("Unexpected error, modified data not in LRU and not in temporary storage");
            }

            // Read from temporary storage
            var bytes = m_fileCache.Read(key);
            //var (bytes, length) = await m_temporaryStorage.ReadAsync(storageLocation);

            var deserialized = serializer.Deserialize(new ByteMemoryOwner(bytes), bytes.Length);
            await lruTable.Add(key, deserialized, serializer);

            return deserialized;
        }

        internal ValueTask AddOrUpdateAsync<V>(in long key, in V value, in IStateSerializer stateSerializer)
            where V : ICacheObject
        {
            lock (m_lock)
            {
                m_modified[key] = 0;
            }
            return lruTable.Add(key, value, stateSerializer);
        }

        /// <summary>
        /// Resets all keys and removes from temporary storage
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        internal async ValueTask ResetKeys(IEnumerable<long> keys)
        {
            foreach (var key in keys)
            {
                lock (m_lock)
                {
                    if (m_modified.TryGetValue(key, out var location))
                    {
                        if (location == 0)
                        {
                            lruTable.Delete(key);
                        }
                        else
                        {
                            m_storageLocations.Remove(location);
                        }
                        m_modified.Remove(key);
                    }
                }
            }

            //Monitor.Enter(m_lock);
            //if (m_storageLocations.Min > minimumTempLocation)
            //{
            //    m_temporaryStorage.TruncateUntil(m_storageLocations.Min);
            //    minimumTempLocation = m_storageLocations.Min;
            //    Monitor.Exit(m_lock);
            //    await m_temporaryStorage.CommitAsync();
            //}
            //else
            //{
            //    Monitor.Exit(m_lock);
            //}
        }

        internal async ValueTask Commit(IEnumerable<long> keys, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            foreach (var key in keys)
            {
                Monitor.Enter(m_lock);
                if (m_modified.TryGetValue(key, out var location))
                {
                    Monitor.Exit(m_lock);
                    //Monitor.Exit(m_modified);
                    if (location == -1)
                    {
                        // Delete
                        await DeleteAsync(key, session);
                    }
                    else if (location == 0)
                    {
                        // from LRU
                        if (lruTable.TryGetValueNoRefresh(key, out var val))
                        {
                            // Serialize the value
                            var bytes = val!.Value.stateSerializer.Serialize(val.Value.value);

                            // Write to persistent storage
                            await WriteAsync(key, bytes, session, "Commit1");
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
                        await WriteAsync(key, bytes, session, "Commit2");
                        lock (m_lock)
                        {
                            m_storageLocations.Remove(location);
                        }
                    }
                }
                else
                {
                    Monitor.Exit(m_lock);
                }

            }
            // Check if the temporary storage can be truncated
            //Monitor.Enter(m_lock);
            //if (m_storageLocations.Min > minimumTempLocation)
            //{
            //    m_temporaryStorage.TruncateUntil(m_storageLocations.Min);
            //    minimumTempLocation = m_storageLocations.Min;
            //    Monitor.Exit(m_lock);
            //    await m_temporaryStorage.CommitAsync();
            //}
            //else
            //{
            //    Monitor.Exit(m_lock);
            //}
        }

        private ValueTask WriteAsync(in long pageId, in byte[] bytes, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session, string from)
        {
            m_commitSemaphore.Wait();
            SpanByte pinned;
            try
            {
                pinned = SpanByte.FromPinnedMemory(bytes);
            }
            catch (Exception e)
            {
                
                Console.WriteLine("AAAAA");
                Console.WriteLine(bytes != null);
                Console.WriteLine(from);
                if (bytes != null)
                {
                    Console.WriteLine(bytes.Length);
                    for (int i = 0; i < bytes.Length; i++)
                    {
                        Console.WriteLine(bytes[i]);
                    }
                }
                
                throw e;
            }
            long page = pageId;
                var upsertOp = session.Upsert(ref page, ref pinned);
                if (!upsertOp.IsCompletedSuccessfully)
                {
                    return WriteAsyncSlow(session);
                }
                m_commitSemaphore.Release();
            
            
            return ValueTask.CompletedTask;
        }


        private ValueTask DeleteAsync(in long pageId, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            m_commitSemaphore.Wait();
            var status = session.Delete(pageId);
            if (!status.IsCompletedSuccessfully)
            {
                return WriteAsyncSlow(session);
            }
            m_commitSemaphore.Release();
            return ValueTask.CompletedTask;
        }

        private async ValueTask WriteAsyncSlow(ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            await session.CompletePendingAsync();
            m_commitSemaphore.Release();
        }

        internal void Delete(in long pageId)
        {
            lock (m_lock)
            {
                m_modified[pageId] = -1;
            }
            lruTable.Delete(pageId);
        }

        public async ValueTask CheckpointAsync()
        {
            Console.WriteLine("Begin checkpoint");
            byte[] bytes;
            lock (m_lock)
            {
                m_metadata.CheckpointVersion = m_persistentStorage.CurrentVersion;
                bytes = m_metadataSerializer.Serialize(m_metadata);
            }
            
            await m_adminSession.UpsertAsync(1, SpanByte.FromFixedSpan(bytes));

            var guid = await TakeCheckpointAsync();
        }

        internal async ValueTask<Guid> TakeCheckpointAsync()
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
            lock (m_lock)
            {
                if (_clients.TryGetValue(name, out var client))
                {
                    return client;
                }
                else
                {
                    client = new StateManagerAdvancedClient(name, this);
                    _clients.Add(name, client);
                    return client;
                }
            }
        }

        internal async ValueTask WritePersistentAsync(long pageId, byte[] bytes, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            await WriteAsync(pageId, bytes, session, "WritePersistentAsync");
        }

        internal async ValueTask<IStateClient<TValue, TMetadata>> CreateClientAsync<TValue, TMetadata>(string client, StateClientOptions<TValue> options)
            where TValue : ICacheObject
        {
            Monitor.Enter(m_lock);
            if (_stateClients.TryGetValue(client, out var cachedClient))
            {
                Monitor.Exit(m_lock);
                return (cachedClient as SimpleStateClient<TValue, TMetadata>)!;
            }
            if (m_metadata.ClientMetadataLocations.TryGetValue(client, out var location))
            {
                Monitor.Exit(m_lock);
                var readAsyncOp = await m_adminSession.ReadAsync(location);
                var (_, v) = readAsyncOp.Complete();
                lock (m_lock)
                {
                    var metadata = StateClientMetadataSerializer.Instance.Deserialize<TMetadata>(new ByteMemoryOwner(v), v.Length);
                    var session = m_persistentStorage.For(m_functions).NewSession(m_functions);
                    var stateClient = new AdvancedStateClient<TValue, TMetadata>(this, location, metadata, session, options);
                    _stateClients.Add(client, stateClient);
                    return stateClient;
                }
            }
            else
            {
                // Allocate a new page id for the client metadata.
                var clientMetadataPageId = GetNewPageId_Internal();
                var clientMetadata = new StateClientMetadata<TMetadata>();
                m_metadata.ClientMetadataLocations.Add(client, clientMetadataPageId);
                Monitor.Exit(m_lock);
                await m_adminSession.UpsertAsync(clientMetadataPageId, SpanByte.FromFixedSpan(StateClientMetadataSerializer.Instance.Serialize(clientMetadata)));
                
                lock (m_lock)
                {
                    var session = m_persistentStorage.For(m_functions).NewSession(m_functions);
                    var stateClient = new AdvancedStateClient<TValue, TMetadata>(this, clientMetadataPageId, clientMetadata, session, options);
                    _stateClients.Add(client, stateClient);
                    return stateClient;
                }
            }
        }

        internal abstract StateManagerMetadata NewMetadata();

        public async Task InitializeAsync()
        {
            bool newMetadata = false;
            try
            {
                lruTable.Clear();
                lock (m_lock)
                {
                    m_modified.Clear();
                }
                await m_persistentStorage.RecoverAsync().ConfigureAwait(false);
                var data = await m_adminSession.ReadAsync(1);
                if (data.Status.Found)
                {
                    lock (m_lock)
                    {
                        m_metadata = m_metadataSerializer.Deserialize(new ByteMemoryOwner(data.Output), data.Output.Length);
                    }
                    await m_persistentStorage.RecoverAsync(recoverTo: m_metadata.CheckpointVersion).ConfigureAwait(false);
                }
                else
                {
                    lock (m_lock)
                    {
                        m_metadata = NewMetadata();
                        newMetadata = true;
                    }
                    
                    m_persistentStorage.Reset();
                }
            }
            catch
            {
                lock (m_lock)
                {
                    m_metadata = NewMetadata();
                    newMetadata = true;
                }
            }

            // Reset cached values in the state clients
            foreach (var stateClient in _stateClients)
            {
                await stateClient.Value.Reset(newMetadata);
            }

            Initialized = true;
        }

        internal async ValueTask OnCacheRemove(IEnumerable<LruTableAdvanced.LinkedListValue> toBeRemoved)
        {
            await m_commitSemaphore.WaitAsync();
            bool enqueued = false;
            lock (m_lock)
            {
                foreach (var val in toBeRemoved)
                {
                
                    if (m_modified.TryGetValue(val.key, out var storageIndex) && storageIndex == 0)
                    {
                        // Store the modified data to disk in temporary storage
                        // must lock the data before writing since this call can come from another thread
                        val.value.EnterWriteLock();
                        m_fileCache.WriteAsync(val.key, val.stateSerializer.Serialize(val.value));
                        //var location = m_temporaryStorage.Enqueue(val.stateSerializer.Serialize(val.value));

                        //if (minimumTempLocation > location)
                        //{
                        //    minimumTempLocation = location;
                        //}
                        //m_storageLocations.Add(location);

                        m_modified[val.key] = 1;
                        val.value.ExitWriteLock();
                        enqueued = true;
                    }
                }
            }
            m_fileCache.Flush();
            //if (enqueued)
            //{
            //    await m_temporaryStorage.CommitAsync();
            //}
            m_commitSemaphore.Release();
        }
    }
}
