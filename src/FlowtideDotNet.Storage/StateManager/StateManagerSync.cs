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
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.ObjectState;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerSync<TMetadata> : StateManagerSync
    {
        public StateManagerSync(StateManagerOptions options, ILoggerFactory loggerFactory, Meter meter, string streamName, IStreamMemoryManager streamMemoryManager) : base(new StateManagerMetadataSerializer<TMetadata>(), options, loggerFactory, meter, streamName, streamMemoryManager)
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
        private S3FifoTableSync? m_cacheTable;
        //private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private readonly IStateSerializer<StateManagerMetadata> m_metadataSerializer;
        private readonly StateManagerOptions options;
        private readonly ILoggerFactory m_loggerFactory;
        private readonly ILogger logger;
        private Meter meter;
        private readonly string m_meterName;
        private readonly string streamName;
        private readonly IStreamMemoryManager _streamMemoryManager;
        private readonly object m_lock = new object();
        internal StateManagerMetadata? m_metadata;
        //private Functions m_functions;
        private FileCacheOptions? m_fileCacheOptions;
        private IFileCacheFactory? m_fileCacheFactory;
        private bool disposedValue;
        private bool m_ownsPersistentStorage;

        //private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        readonly Dictionary<string, IStateManagerClient> _clients = new Dictionary<string, IStateManagerClient>();
        private readonly Dictionary<string, StateClient> _stateClients = new Dictionary<string, StateClient>();
        private IPersistentStorage? m_persistentStorage;

        /// <summary>
        /// Used for unit testing only
        /// </summary>
        internal S3FifoTableSync CacheTable => m_cacheTable ?? throw new InvalidOperationException("Manager must be initialized before getting cache table");

        /// <summary>
        /// The table for the client paths, gone once Dispose ran so a walk given up sees the stop, not a null.
        /// </summary>
        private S3FifoTableSync TableForClients => m_cacheTable ?? throw new ObjectDisposedException(nameof(StateManagerSync));

        /// <summary>
        /// Awaited by every client's background walk before it claims each page, with the client name and page id.
        /// </summary>
        internal Func<string, long, Task>? PageWriteHookForTests { get; set; }

        /// <summary>
        /// Set once the caller drained the walks itself, Dispose then does not wait again.
        /// </summary>
        private bool m_commitsAbandoned;

        /// <summary>
        /// True while a client's background commit is still writing. A walk starts at the
        /// operator's Commit and is only joined by the checkpoint, a teardown drains it here.
        /// </summary>
        public bool HasCommitsInFlight
        {
            get
            {
                lock (m_lock)
                {
                    foreach (var stateClient in _stateClients.Values)
                    {
                        if (stateClient.HasCommitInFlight)
                        {
                            return true;
                        }
                    }
                    return false;
                }
            }
        }

        /// <summary>
        /// True if any state client has a commit fault.
        /// </summary>
        public bool HasCommitFaults
        {
            get
            {
                lock (m_lock)
                {
                    foreach (var stateClient in _stateClients.Values)
                    {
                        if (stateClient.HasCommitFault)
                        {
                            return true;
                        }
                    }
                    return false;
                }
            }
        }

        /// <summary>
        /// Tells every walk to give up at its next page, for a caller whose own drain wait ran out.
        /// </summary>
        public void RequestStopCommits()
        {
            lock (m_lock)
            {
                m_commitsAbandoned = true;
                foreach (var stateClient in _stateClients.Values)
                {
                    stateClient.RequestStopCommits();
                }
            }
        }

        public bool Initialized { get; private set; }

        internal int LookupCacheSize => 0;

        internal StateSerializeOptions SerializeOptions => options?.SerializeOptions ?? throw new InvalidOperationException("Manager must be initialized before getting serialize options");

        public ulong PageCommits => m_metadata != null ? Volatile.Read(ref m_metadata.PageCommits) : throw new InvalidOperationException("Manager must be initialized before getting page commits");

        public ulong PageCommitsAtLastCompaction => m_metadata != null ? m_metadata.PageCommitsAtLastCompaction : throw new InvalidOperationException("Manager must be initialized before getting page commits");

        public long PageCommitsSinceLastCompaction => (long)(PageCommits - PageCommitsAtLastCompaction);

        public long PageCount => m_metadata != null ? Volatile.Read(ref m_metadata.PageCount) : throw new InvalidOperationException("Manager must be initialized before getting page count");

        public long CurrentVersion => m_persistentStorage?.CurrentVersion ?? 0;

        public long LastCompletedCheckpointVersion { get; private set; }

        private protected StateManagerSync(
            IStateSerializer<StateManagerMetadata> metadataSerializer, 
            StateManagerOptions options, 
            ILoggerFactory loggerFactory, 
            Meter meter, 
            string streamName,
            IStreamMemoryManager streamMemoryManager)
        {
            this.m_metadataSerializer = metadataSerializer;
            this.options = options;
            m_loggerFactory = loggerFactory;
            this.logger = loggerFactory.CreateLogger("StateManager");
            this.meter = meter;
            this.m_meterName = meter.Name;
            this.streamName = streamName;
            this._streamMemoryManager = streamMemoryManager;
        }

        private void Setup()
        {
            if (disposedValue)
            {
                // The engine disposes the manager when a stream stops and initializes it again if
                // the stream starts back up. A fresh meter replaces the instruments that went with
                // the disposed one, the state clients register on it again as they are recreated.
                meter = new Meter(m_meterName);
                disposedValue = false;
            }
            // An abandoned drain belongs to the teardown that gave up, the next one waits again.
            m_commitsAbandoned = false;

            if (m_cacheTable == null)
            {
                m_cacheTable = new S3FifoTableSync(new CacheTableOptions(streamName, logger, meter, new MemoryStatsWithGC(_streamMemoryManager))
                {
                    MaxSize = options.CachePageCount,
                    MaxMemoryUsageInBytes = options.MaxProcessMemory,
                    MinSize = options.MinCachePageCount,
                    DrainSmallQueueEarly = options.DrainSmallQueueEarly,
                    AdaptiveSmallQueueSize = options.AdaptiveSmallQueueSize
                });
            }

            if (m_persistentStorage != null)
            {
                m_persistentStorage.ClearForRestore();
            }
            else if (options.PersistentStorage == null)
            {
                m_persistentStorage = new FileCachePersistentStorage(new FileCacheOptions()
                {
                    DirectoryPath = "./data/fileCachePersistence"
                });
                m_ownsPersistentStorage = true;
            }
            else
            {
                m_persistentStorage = options.PersistentStorage;
                m_ownsPersistentStorage = false;
            }
            m_fileCacheOptions = options.TemporaryStorageOptions ?? new FileCacheOptions()
            {
                DirectoryPath = "./data/tempFiles"
            };
            m_fileCacheFactory = options.FileCacheFactory ?? new DefaultFileCacheFactory(m_fileCacheOptions);
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

        internal bool AddOrUpdate<V>(in long key, in V value, in ICacheEvictHandler evictHandler)
            where V : ICacheObject
        {
            return TableForClients.Add(key, value, evictHandler);
        }

        internal bool AddOrUpdate<V>(in long key, in V value, in ICacheEvictHandler evictHandler, out S3FifoCacheEntry entry)
            where V : ICacheObject
        {
            return TableForClients.Add(key, value, evictHandler, out entry);
        }

        internal Task WaitForNotFullAsync()
        {
            return TableForClients.Wait();
        }

        internal bool IsOverCapacity => TableForClients.IsOverCapacity;

        internal void DeleteFromCache(in long key)
        {
            TableForClients.Delete(key);
        }

        internal void ClearCache()
        {
            TableForClients.Clear();
        }

        internal int MaxHeldPages => m_cacheTable?.MaxHeldPages ?? 1;

        internal bool TryRentCachedValue(in long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            return TableForClients.TryRentCached(key, out entry);
        }

        internal void RegisterExternalHitCounter(Func<long> hitCounter)
        {
            TableForClients.RegisterExternalHitCounter(hitCounter);
        }

        internal bool TryPeekCacheEntry(in long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            return TableForClients.TryPeekEntry(key, out entry);
        }

        internal bool TryGetCacheValueFromCache(in long key, [NotNullWhen(true)] out S3FifoCacheEntry? value)
        {
            return TableForClients.TryGetCacheValue(key, out value);
        }

        internal bool TryRentCacheEntryForCommit(in long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            return TableForClients.TryRentForCommit(key, out entry);
        }

        public async ValueTask CheckpointAsync(bool includeIndex = false)
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);
            Debug.Assert(options != null);
            if (disposedValue || m_commitsAbandoned)
            {
                throw new ObjectDisposedException(nameof(StateManagerSync));
            }

            // Every client's background commit must have landed before the checkpoint seals them.
            List<StateClient> stateClients;
            lock (m_lock)
            {
                stateClients = _stateClients.Values.ToList();
            }
            foreach (var stateClient in stateClients)
            {
                try
                {
                    await stateClient.WaitForCommitAsync();
                }
                catch (OperationCanceledException)
                {
                    throw new ObjectDisposedException(nameof(StateManagerSync), "The checkpoint was abandoned on a stop request.");
                }
            }
            if (disposedValue || m_commitsAbandoned)
            {
                throw new ObjectDisposedException(nameof(StateManagerSync));
            }

            byte[] bytes;
            lock (m_lock)
            {
                m_metadata.CheckpointVersion = m_persistentStorage.CurrentVersion;
                var bufferWriter = new ArrayBufferWriter<byte>();
                m_metadataSerializer.Serialize(bufferWriter, m_metadata);
                bytes = bufferWriter.WrittenSpan.ToArray();
            }

            await m_persistentStorage.CheckpointAsync(bytes, includeIndex);
            LastCompletedCheckpointVersion = m_metadata.CheckpointVersion;
        }

        public async Task Compact()
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);

            ulong changesSinceLastCompaction = m_metadata.PageCommits - m_metadata.PageCommitsAtLastCompaction;

            await m_persistentStorage.CompactAsync(changesSinceLastCompaction, PageCommits);
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

        /// <summary>
        /// Checks whether a state client with the given name has been registered, without creating it.
        /// </summary>
        internal bool StateExists(string client)
        {
            Debug.Assert(m_metadata != null);
            lock (m_lock)
            {
                return _stateClients.ContainsKey(client) || m_metadata.ClientMetadataLocations.ContainsKey(client);
            }
        }

        internal ValueTask<IStateClient<TValue, TMetadata>> CreateClientAsync<TValue, TMetadata>(string client, StateClientOptions<TValue> options, IMemoryAllocator memoryAllocator)
            where TValue : ICacheObject
            where TMetadata : class, IStorageMetadata
        {
            Debug.Assert(m_metadata != null);
            Debug.Assert(m_persistentStorage != null);
            Debug.Assert(m_fileCacheOptions != null);
            Debug.Assert(m_fileCacheFactory != null);

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
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, location, metadata, persistentSession, options, m_fileCacheFactory, meter, this.options.UseReadCache, this.options.BackgroundCommit, this.options.DefaultBPlusTreePageSize, this.options.DefaultBPlusTreePageSizeBytes, memoryAllocator);

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
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, location, clientMetadata, persistentSession, options, m_fileCacheFactory, meter, this.options.UseReadCache, this.options.BackgroundCommit, this.options.DefaultBPlusTreePageSize, this.options.DefaultBPlusTreePageSizeBytes, memoryAllocator);
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
                    var stateClient = new SyncStateClient<TValue, TMetadata>(this, client, clientMetadataPageId, clientMetadata, session, options, m_fileCacheFactory, meter, this.options.UseReadCache, this.options.BackgroundCommit, this.options.DefaultBPlusTreePageSize, this.options.DefaultBPlusTreePageSizeBytes, memoryAllocator);
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

        public async Task InitializeAsync(StreamVersionInformation? streamVersionInformation = null, long? checkpointVersion = null)
        {
            bool newMetadata = false;

            // Pause eviction for the whole reset. An in-flight eviction could otherwise write a
            // stale page after the reset and route later reads to it.
            // The instance is captured, the finally must resume the table it paused.
            var cacheTable = m_cacheTable;
            if (cacheTable != null)
            {
                await cacheTable.PauseEvictionAsync();
            }

            // Drain in-flight client commits and hold new ones out for the whole reset.
            // A recovery that starts under a walk must join the walk before it resets the
            // sessions, or a page the walk writes afterwards lands in the new epoch's writer
            // and the next checkpoint seals it over the checkpointed one.
            var pausedClients = new List<StateClient>();
            // A snapshot, a teardown that gave up waiting can clear the dictionary meanwhile.
            List<StateClient> stateClients;
            lock (m_lock)
            {
                stateClients = _stateClients.Values.ToList();
            }
            try
            {
                foreach (var stateClient in stateClients)
                {
                    await stateClient.PauseCommitsAsync(options.StopCommitsTimeout);
                    pausedClients.Add(stateClient);
                }

                Setup();
                Debug.Assert(m_cacheTable != null);
                Debug.Assert(m_persistentStorage != null);
                Debug.Assert(options != null);

                // Returns the cache rents, the clients are reset below so no lookup handle
                // keeps serving a cleared entry.
                m_cacheTable.ClearAndReturnRents();
                await m_persistentStorage.InitializeAsync(new StorageInitializationMetadata(streamName, m_loggerFactory, _streamMemoryManager, streamVersionInformation)).ConfigureAwait(false);

                // Check that metadata exist, also that the checkpoint version is larger than 0
                // If zero we revert back to an empty state
                if (m_persistentStorage.TryGetValue(1, out var metadataBytes) && (!checkpointVersion.HasValue || checkpointVersion.Value > 0))
                {
                    // Never fall through, the else branch resets the stream.
                    var metadata = metadataBytes ?? throw new InvalidOperationException("Metadata page was found but empty.");
                    lock (m_lock)
                    {
                        m_metadata = m_metadataSerializer.Deserialize(new ReadOnlySequence<byte>(metadata), metadata.Length);
                    }
                    await m_persistentStorage.RecoverAsync(!checkpointVersion.HasValue ? m_metadata.CheckpointVersion : checkpointVersion.Value).ConfigureAwait(false);
                    LastCompletedCheckpointVersion = !checkpointVersion.HasValue ? m_metadata.CheckpointVersion : checkpointVersion.Value;
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
                    LastCompletedCheckpointVersion = 0;
                }

                // Reset cached values in the state clients
                foreach (var stateClient in stateClients)
                {
                    await stateClient.Reset(newMetadata);
                }
            }
            finally
            {
                foreach (var pausedClient in pausedClients)
                {
                    pausedClient.ResumeCommits();
                }
                cacheTable?.ResumeEviction();
            }

            logger.LogDebug("State manager initialized, requested version: {requestedVersion}, recovered version: {recoveredVersion}, new metadata: {newMetadata}, reset {stateClientCount} state clients", checkpointVersion, LastCompletedCheckpointVersion, newMetadata, _stateClients.Count);

            Initialized = true;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // The walks first, they read the cache table. Every walk is told before any is
                    // waited for, so the waits overlap, and the wait is bounded, a walk wedged on
                    // storage must not hang the teardown. A caller that drained already skips it.
                    List<StateClient> stateClients;
                    lock (m_lock)
                    {
                        stateClients = _stateClients.Values.ToList();
                    }
                    foreach (var stateClient in stateClients)
                    {
                        stateClient.RequestStopCommits();
                    }
                    if (!m_commitsAbandoned)
                    {
                        var stopDeadline = Stopwatch.GetTimestamp();
                        foreach (var stateClient in stateClients)
                        {
                            var remaining = options.StopCommitsTimeout - Stopwatch.GetElapsedTime(stopDeadline);
                            stateClient.StopCommits(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
                        }
                    }

                    // Dispose the cache table first so it stops the cleanup task.
                    // Otherwise an in-flight eviction writes through an already disposed client.
                    // Cleared so a later initialize builds a fresh one, the disposed table's
                    // eviction gate and cleanup task cannot be reused.
                    if (m_cacheTable != null)
                    {
                        m_cacheTable.Dispose();
                        m_cacheTable = null;
                    }

                    // Before the storage, the clients return their sessions to it.
                    foreach (var stateClient in stateClients)
                    {
                        stateClient.Dispose();
                    }
                    lock (m_lock)
                    {
                        _stateClients.Clear();
                    }

                    // A supplied storage belongs to the caller and must outlive a stop, otherwise
                    // the next start has nothing to recover from. Setup resets it for restore.
                    if (m_persistentStorage != null && m_ownsPersistentStorage)
                    {
                        m_persistentStorage.Dispose();
                        m_persistentStorage = null;
                    }

                    // Released after the clients, they register instruments on it.
                    meter.Dispose();
                    Initialized = false;
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
