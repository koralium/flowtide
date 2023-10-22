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
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FASTER.core;
using System.Diagnostics;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerSync<TMetadata> : StateManagerSync
    {
        public StateManagerSync(StateManagerOptions options, ILogger logger) : base(new StateManagerMetadataSerializer<TMetadata>(), options, logger)
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

    public abstract class StateManagerSync : IStateManager, IDisposable
    {
        private LruTableSync m_lruTable;
        //private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private readonly IStateSerializer<StateManagerMetadata> m_metadataSerializer;
        private readonly StateManagerOptions options;
        private readonly object m_lock = new object();
        internal StateManagerMetadata? m_metadata;
        //private Functions m_functions;
        private FileCacheOptions m_fileCacheOptions;
        private bool disposedValue;

        //private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        readonly Dictionary<string, IStateManagerClient> _clients = new Dictionary<string, IStateManagerClient>();
        private readonly Dictionary<string, StateClient> _stateClients = new Dictionary<string, StateClient>();
        private readonly IPersistentStorage m_persistentStorage;

        public bool Initialized { get; private set; }

        internal StateManagerSync(IStateSerializer<StateManagerMetadata> metadataSerializer, StateManagerOptions options, ILogger logger)
        {
            m_lruTable = new LruTableSync(options.CachePageCount, logger);

            if (options.PersistentStorage == null)
            {
                m_persistentStorage = new FileCachePersistentStorage(new FileCacheOptions()
                {
                    DirectoryPath = "./data/fileCachePersistence"
                });
            }
            else
            {
                m_persistentStorage = options.PersistentStorage;
            }

            
            //m_persistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
            //{
            //    RemoveOutdatedCheckpoints = true,
            //    //MutableFraction = 0.1,
            //    MemorySize = 1024 * 1024 * 128,
            //    PageSize = 1024 * 1024 * 16,
            //    LogDevice = options.LogDevice,
            //    CheckpointManager = options.CheckpointManager,
            //    CheckpointDir = options.CheckpointDir,
            //    //ReadCacheEnabled = true,
            //    //ReadCopyOptions = ReadCopyOptions.None
            //    //ConcurrencyControlMode = ConcurrencyControlMode.RecordIsolation
            //});
            this.m_metadataSerializer = metadataSerializer;
            this.options = options;
            //m_functions = new Functions();
            //m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);
            m_fileCacheOptions = options.TemporaryStorageOptions ?? new FileCacheOptions()
            {
                DirectoryPath = "./data/tempFiles"
            };
        }

        internal long GetNewPageId()
        {
            lock (m_lock)
            {
                return GetNewPageId_Internal();
            }
        }

        private long GetNewPageId_Internal()
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            Debug.Assert(m_metadata != null);
            long id = m_metadata.PageCounter;
            m_metadata.PageCounter++;
            return id;
        }

        internal void AddOrUpdate<V>(in long key, in V value, in ILruEvictHandler evictHandler)
            where V : ICacheObject
        {
            m_lruTable.Add(key, value, evictHandler);
        }

        internal void DeleteFromCache(in long key)
        {
            m_lruTable.Delete(key);
        }

        internal bool TryGetValueFromCache<T>(in long key, out T? value)
            where T : ICacheObject
        {
            if (m_lruTable.TryGetValue(key, out var obj))
            {
                value = (T)obj!;
                return true;
            }
            value = default;
            return false;
        }

        internal void WriteToPersistentStore(in long key, in byte[] bytes, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            var spanByte = SpanByte.FromPinnedMemory(bytes);
            var status = session.Upsert(key, spanByte);
            if (status.IsCompleted || status.IsPending)
            {
                session.CompletePending(true);
            }
        }

        internal void DeleteFromPersistentStore(in long key, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            var status = session.Delete(key);
            if (!status.IsCompleted || status.IsPending)
            {
                session.CompletePending(true);
            }
        }

        internal byte[] ReadFromPersistentStore(in long key, in ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            var result = session.Read(key);
            if (result.status.IsCompleted && !result.status.IsPending)
            {
                return result.output;
            }
            if (session.CompletePendingWithOutputs(out var completedOutputs, true))
            {
                var hasNext = completedOutputs.Next();
                var bytes = completedOutputs.Current.Output;
                hasNext = completedOutputs.Next();
                if (hasNext)
                {
                    throw new Exception();
                }
                return bytes;
            }
            else
            {
                throw new Exception();
            }
        }

        public async ValueTask CheckpointAsync()
        {
            byte[] bytes;
            lock (m_lock)
            {
                m_metadata.CheckpointVersion = m_persistentStorage.CurrentVersion;
                bytes = m_metadataSerializer.Serialize(m_metadata);
            }

            await m_persistentStorage.CheckpointAsync(bytes);
            //var status = m_adminSession.Upsert(1, SpanByte.FromFixedSpan(bytes));
            //if (status.IsCompleted || status.IsPending)
            //{
            //    m_adminSession.CompletePending(true);
            //}

            //var guid = await TakeCheckpointAsync();
        }

        //internal async Task<Guid> TakeCheckpointAsync()
        //{
        //    bool success = false;
        //    Guid token;
        //    do
        //    {
        //        (success, token) = await m_persistentStorage.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
        //    } while (!success);
        //    return token;
        //}

        public async Task Compact()
        {
            await m_persistentStorage.CompactAsync();
            //m_adminSession.Compact(m_persistentStorage.Log.SafeReadOnlyAddress, CompactionType.Lookup);
            //m_persistentStorage.Log.Truncate();
            //await m_persistentStorage.TakeFullCheckpointAsync(CheckpointType.Snapshot);
        }

        internal async ValueTask<IStateClient<TValue, TMetadata>> CreateClientAsync<TValue, TMetadata>(string client, StateClientOptions<TValue> options)
            where TValue : ICacheObject
        {
            Monitor.Enter(m_lock);
            if (_stateClients.TryGetValue(client, out var cachedClient))
            {
                Monitor.Exit(m_lock);
                return (cachedClient as SyncStateClient<TValue, TMetadata>)!;
            }
            if (m_metadata.ClientMetadataLocations.TryGetValue(client, out var location))
            {
                Monitor.Exit(m_lock);
                if (m_persistentStorage.TryGetValue(location, out var bytes))
                {
                    var metadata = StateClientMetadataSerializer.Instance.Deserialize<TMetadata>(new ByteMemoryOwner(bytes), bytes.Length);
                    var persistentSession = m_persistentStorage.CreateSession();
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, location, metadata, persistentSession, options, m_fileCacheOptions);
                    return stateClient;
                }
                else
                {
                    throw new InvalidOperationException("Persistent data could not be found for client");
                }
            }
            else
            {
                // Allocate a new page id for the client metadata.
                var clientMetadataPageId = GetNewPageId_Internal();
                var clientMetadata = new StateClientMetadata<TMetadata>();
                m_metadata.ClientMetadataLocations.Add(client, clientMetadataPageId);
                Monitor.Exit(m_lock);

                m_persistentStorage.Write(clientMetadataPageId, StateClientMetadataSerializer.Instance.Serialize(clientMetadata));
                lock (m_lock)
                {
                    var session = m_persistentStorage.CreateSession();
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, clientMetadataPageId, clientMetadata, session, options, m_fileCacheOptions);
                    _stateClients.Add(client, stateClient);
                    return stateClient;
                }
            }
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
                    client = new StateManagerSyncClient(name, this);
                    _clients.Add(name, client);
                    return client;
                }
            }
        }

        internal abstract StateManagerMetadata NewMetadata();

        public async Task InitializeAsync()
        {
            bool newMetadata = false;

            m_lruTable.Clear();
            await m_persistentStorage.InitializeAsync().ConfigureAwait(false);

            if (m_persistentStorage.TryGetValue(1, out var metadataBytes))
            {
                lock (m_lock)
                {
                    m_metadata = m_metadataSerializer.Deserialize(new ByteMemoryOwner(metadataBytes), metadataBytes.Length);
                }
                await m_persistentStorage.RecoverAsync(m_metadata.CheckpointVersion).ConfigureAwait(false);
            }
            else
            {
                lock (m_lock)
                {
                    m_metadata = NewMetadata();
                    newMetadata = true;
                }
                await m_persistentStorage.ResetAsync();
            }

            // Reset cached values in the state clients
            foreach (var stateClient in _stateClients)
            {
                await stateClient.Value.Reset(newMetadata);
            }

            Initialized = true;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                    m_persistentStorage.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~StateManagerSync()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
