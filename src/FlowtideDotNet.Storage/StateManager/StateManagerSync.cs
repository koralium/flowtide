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
using System.Diagnostics;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;
using System.Diagnostics.CodeAnalysis;
using FlowtideDotNet.Storage.StateManager.Internal.ObjectState;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerSync<TMetadata> : StateManagerSync
    {
        public StateManagerSync(StateManagerOptions options, ILogger logger, Meter meter, string streamName) : base(new StateManagerMetadataSerializer<TMetadata>(), options, logger, meter, streamName)
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
        private LruTableSync? m_lruTable;
        //private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private readonly IStateSerializer<StateManagerMetadata> m_metadataSerializer;
        private readonly StateManagerOptions options;
        private readonly ILogger logger;
        private readonly Meter meter;
        private readonly string streamName;
        private readonly object m_lock = new object();
        internal StateManagerMetadata? m_metadata;
        //private Functions m_functions;
        private FileCacheOptions? m_fileCacheOptions;
        private bool disposedValue;

        //private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        readonly Dictionary<string, IStateManagerClient> _clients = new Dictionary<string, IStateManagerClient>();
        private readonly Dictionary<string, StateClient> _stateClients = new Dictionary<string, StateClient>();
        private IPersistentStorage? m_persistentStorage;

        /// <summary>
        /// Used for unit testing only
        /// </summary>
        internal LruTableSync LruTable => m_lruTable ?? throw new InvalidOperationException("Manager must be initialized before getting LRU table");

        public bool Initialized { get; private set; }

        internal StateSerializeOptions SerializeOptions => options?.SerializeOptions ?? throw new InvalidOperationException("Manager must be initialized before getting serialize options");

        public ulong PageCommits => m_metadata != null ? Volatile.Read(ref m_metadata.PageCommits) : throw new InvalidOperationException("Manager must be initialized before getting page commits");

        public ulong PageCommitsAtLastCompaction => m_metadata != null ? m_metadata.PageCommitsAtLastCompaction : throw new InvalidOperationException("Manager must be initialized before getting page commits");

        public long PageCommitsSinceLastCompaction => (long)(PageCommits - PageCommitsAtLastCompaction);

        public long PageCount => m_metadata != null ? Volatile.Read(ref m_metadata.PageCount) : throw new InvalidOperationException("Manager must be initialized before getting page count");

        private protected StateManagerSync(IStateSerializer<StateManagerMetadata> metadataSerializer, StateManagerOptions options, ILogger logger, Meter meter, string streamName)
        {
            this.m_metadataSerializer = metadataSerializer;
            this.options = options;
            this.logger = logger;
            this.meter = meter;
            this.streamName = streamName;
        }

        private void Setup()
        {
            if (m_lruTable == null)
            {
                m_lruTable = new LruTableSync(new LruTableOptions(streamName, logger, meter)
                {
                    MaxSize = options.CachePageCount,
                    MaxMemoryUsageInBytes = options.MaxProcessMemory,
                    MinSize = options.MinCachePageCount
                });
            }

            if (m_persistentStorage != null)
            {
                m_persistentStorage.Dispose();
                m_persistentStorage = null;
            }
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

        internal bool AddOrUpdate<V>(in long key, in V value, in ILruEvictHandler evictHandler)
            where V : ICacheObject
        {
            Debug.Assert(m_lruTable != null);
            return m_lruTable.Add(key, value, evictHandler);
        }

        internal Task WaitForNotFullAsync()
        {
            Debug.Assert(m_lruTable != null);
            return m_lruTable.Wait();
        }

        internal void DeleteFromCache(in long key)
        {
            Debug.Assert(m_lruTable != null);
            m_lruTable.Delete(key);
        }

        internal void ClearCache()
        {
            Debug.Assert(m_lruTable != null);
            m_lruTable.Clear();
        }

        internal bool TryGetValueFromCache<T>(in long key, [NotNullWhen(true)] out T? value)
            where T : ICacheObject
        {
            Debug.Assert(m_lruTable != null);
            if (m_lruTable.TryGetValue(key, out var obj))
            {
                value = (T)obj!;
                return true;
            }
            value = default;
            return false;
        }

        public async ValueTask CheckpointAsync(bool includeIndex = false)
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);
            Debug.Assert(options != null);
            byte[] bytes;
            lock (m_lock)
            {
                m_metadata.CheckpointVersion = m_persistentStorage.CurrentVersion;
                var bufferWriter = new ArrayBufferWriter<byte>();
                m_metadataSerializer.Serialize(bufferWriter, m_metadata);
                bytes = bufferWriter.WrittenSpan.ToArray();
            }

            await m_persistentStorage.CheckpointAsync(bytes, includeIndex);
        }

        public async Task Compact()
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);

            await m_persistentStorage.CompactAsync();
            m_metadata.PageCommitsAtLastCompaction = m_metadata.PageCommits;
        }

        /// <summary>
        /// Creates a state for a single object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        internal ValueTask<IObjectState<T>> CreateObjectStateAsync<T>(string client)
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);
            Debug.Assert(m_fileCacheOptions != null);
            
            bool foundStateClient = false;
            StateClient? cachedClient;
            lock (m_lock)
            {
                foundStateClient = _stateClients.TryGetValue(client, out cachedClient);
            }
            
            if (foundStateClient)
            {
                return ValueTask.FromResult((cachedClient as IObjectState<T>)!);
            }

            long location;
            lock (m_lock)
            {
                foundStateClient = m_metadata.ClientMetadataLocations.TryGetValue(client, out location);
            }

            if (foundStateClient)
            {
                if (m_persistentStorage.TryGetValue(location, out var bytes))
                {
                    var metadata = StateClientMetadataSerializer.Deserialize<T>(bytes.Value, bytes.Value.Length);
                    var persistentSession = m_persistentStorage.CreateSession();
                    var stateClient = new ObjectStateClient<T>(location, metadata, persistentSession);
                    lock (m_lock)
                    {
                        _stateClients.Add(client, stateClient);
                    }
                    return ValueTask.FromResult<IObjectState<T>>(stateClient);
                }
                else
                {
                    // Temporary tree or similar, return an empty metadata with the same id
                    var clientMetadata = new StateClientMetadata<T>();
                    var persistentSession = m_persistentStorage.CreateSession();
                    var stateClient = new ObjectStateClient<T>(location, clientMetadata, persistentSession);
                    lock (m_lock)
                    {
                        _stateClients.Add(client, stateClient);
                    }
                    return ValueTask.FromResult<IObjectState<T>>(stateClient);
                }
            }
            else
            {
                // Allocate a new page id for the client metadata.
                long clientMetadataPageId;
                lock (m_lock)
                {
                    clientMetadataPageId = GetNewPageId_Internal();
                    m_metadata.ClientMetadataLocations.Add(client, clientMetadataPageId);
                }
                
                var clientMetadata = new StateClientMetadata<T>();

                lock (m_lock)
                {
                    var session = m_persistentStorage.CreateSession();
                    var stateClient = new ObjectStateClient<T>(clientMetadataPageId, clientMetadata, session);
                    _stateClients.Add(client, stateClient);
                    return ValueTask.FromResult<IObjectState<T>>(stateClient);
                }
            }
        }

        internal ValueTask<IStateClient<TValue, TMetadata>> CreateClientAsync<TValue, TMetadata>(string client, StateClientOptions<TValue> options, IMemoryAllocator memoryAllocator)
            where TValue : ICacheObject
            where TMetadata : class, IStorageMetadata
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);
            Debug.Assert(m_fileCacheOptions != null);

            Monitor.Enter(m_lock);
            if (_stateClients.TryGetValue(client, out var cachedClient))
            {
                Monitor.Exit(m_lock);
                return ValueTask.FromResult<IStateClient<TValue, TMetadata>>((cachedClient as SyncStateClient<TValue, TMetadata>)!);
            }
            if (m_metadata.ClientMetadataLocations.TryGetValue(client, out var location))
            {
                Monitor.Exit(m_lock);
                if (m_persistentStorage.TryGetValue(location, out var bytes))
                {
                    var metadata = StateClientMetadataSerializer.Deserialize<TMetadata>(bytes.Value, bytes.Value.Length);
                    var persistentSession = m_persistentStorage.CreateSession();
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, location, metadata, persistentSession, options, m_fileCacheOptions, meter, this.options.UseReadCache, this.options.DefaultBPlusTreePageSize, this.options.DefaultBPlusTreePageSizeBytes, memoryAllocator);
                    
                    lock (m_lock)
                    {
                        _stateClients.Add(client, stateClient);
                    }
                    return ValueTask.FromResult<IStateClient<TValue, TMetadata>>(stateClient);
                }
                else
                {
                    // Temporary tree or similar, return an empty metadata with the same id
                    var clientMetadata = new StateClientMetadata<TMetadata>();
                    var persistentSession = m_persistentStorage.CreateSession();
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, location, clientMetadata, persistentSession, options, m_fileCacheOptions, meter, this.options.UseReadCache, this.options.DefaultBPlusTreePageSize, this.options.DefaultBPlusTreePageSizeBytes, memoryAllocator);
                    lock (m_lock)
                    {
                        _stateClients.Add(client, stateClient);
                    }
                    return ValueTask.FromResult<IStateClient<TValue, TMetadata>>(stateClient);
                }
            }
            else
            {
                // Allocate a new page id for the client metadata.
                var clientMetadataPageId = GetNewPageId_Internal();
                var clientMetadata = new StateClientMetadata<TMetadata>();
                m_metadata.ClientMetadataLocations.Add(client, clientMetadataPageId);
                Monitor.Exit(m_lock);

                lock (m_lock)
                {
                    var session = m_persistentStorage.CreateSession();
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, clientMetadataPageId, clientMetadata, session, options, m_fileCacheOptions, meter, this.options.UseReadCache, this.options.DefaultBPlusTreePageSize, this.options.DefaultBPlusTreePageSizeBytes, memoryAllocator);
                    _stateClients.Add(client, stateClient);
                    return ValueTask.FromResult<IStateClient<TValue, TMetadata>>(stateClient);
                }
            }
        }

        public IStateManagerClient GetOrCreateClient(string name, TagList tagList = default)
        {
            lock (m_lock)
            {
                if (_clients.TryGetValue(name, out var client))
                {
                    return client;
                }
                else
                {
                    client = new StateManagerSyncClient(name, this, tagList);
                    _clients.Add(name, client);
                    return client;
                }
            }
        }

        internal abstract StateManagerMetadata NewMetadata();

        public async Task InitializeAsync()
        {
            bool newMetadata = false;
            Setup();
            Debug.Assert(m_lruTable != null);
            Debug.Assert(m_persistentStorage != null);
            Debug.Assert(options != null);
            m_lruTable.Clear();
            await m_persistentStorage.InitializeAsync(new StorageInitializationMetadata(streamName)).ConfigureAwait(false);

            if (m_persistentStorage.TryGetValue(1, out var metadataBytes))
            {
                lock (m_lock)
                {
                    m_metadata = m_metadataSerializer.Deserialize(metadataBytes.Value, metadataBytes.Value.Length);
                }
                await m_persistentStorage.RecoverAsync(m_metadata.CheckpointVersion).ConfigureAwait(false);
            }
            else
            {
                lock (m_lock)
                {
                    m_metadata = NewMetadata();
                    // Increase the page counter to avoid using the same page id as the metadata page.
                    if (_stateClients.Count > 0)
                    {
                        m_metadata.PageCounter = _stateClients.Max(x => x.Value.MetadataId) + 1;
                    }
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
                    if (m_persistentStorage != null)
                    {
                        m_persistentStorage.Dispose();
                    }

                    foreach (var stateClient in _stateClients)
                    {
                        stateClient.Value.Dispose();
                    }
                    _stateClients.Clear();

                    if (m_lruTable != null)
                    {
                        m_lruTable.Dispose();
                    }
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
