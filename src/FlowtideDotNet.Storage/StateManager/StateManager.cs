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

using FlowtideDotNet.Storage.DeviceFactories;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Cache;
using FASTER.core;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManager<TMetadata> : StateManager
    {
        public StateManager(StateManagerOptions options) : base(new StateManagerMetadataSerializer<TMetadata>(), options)
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

    public abstract class StateManager : IStateManager
    {
        private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private Functions m_functions;
        private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        internal StateManagerMetadata? m_metadata;
        private readonly object m_metadata_lock = new object();
        private readonly IStateSerializer<StateManagerMetadata> m_metadataSerializer;
        private LruCache m_lruCache;
        internal INamedDeviceFactory m_temporaryStorageFactory;
        private readonly Dictionary<string, StateManagerClient> _clients = new Dictionary<string, StateManagerClient>();
        private readonly Dictionary<string, StateClient> _stateClients = new Dictionary<string, StateClient>();
        private SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public bool Initialized { get; private set; }

        internal StateManager(IStateSerializer<StateManagerMetadata> metadataSerializer, StateManagerOptions options)
        {
            m_temporaryStorageFactory = options.TemporaryStorageFactory == null ? new InMemoryDeviceFactory() : options.TemporaryStorageFactory;
            m_lruCache = new LruCache(options.CachePageCount);
            m_persistentStorage = new FasterKV<long, SpanByte>(new FasterKVSettings<long, SpanByte>()
            {
                RemoveOutdatedCheckpoints = true,
                MemorySize = 1024 * 1024 * 16,
                PageSize = 1024 * 1024,
                LogDevice = options.LogDevice,
                CheckpointManager = options.CheckpointManager,
                CheckpointDir = options.CheckpointDir,
                //ConcurrencyControlMode = ConcurrencyControlMode.RecordIsolation
            });

            m_functions = new Functions();
            m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);
            m_metadataSerializer = metadataSerializer;
        }

        internal long GetNewPageId()
        {
            lock (m_metadata_lock)
            {
                Debug.Assert(m_metadata != null);
                long id = m_metadata.PageCounter;
                m_metadata.PageCounter++;
                return id;
            }
        }

        internal abstract StateManagerMetadata NewMetadata();

        public async ValueTask CheckpointAsync()
        {
            m_metadata.CheckpointVersion = m_persistentStorage.CurrentVersion;
            var bytes = m_metadataSerializer.Serialize(m_metadata, new StateSerializeOptions());
            await m_adminSession.UpsertAsync(1, SpanByte.FromFixedSpan(bytes));
            
            var guid = await BeginCheckpointAsync();
            // add guid to metadata

            //// End the checkpoint by 
            //await EndCheckpointAsync();
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

        //internal async ValueTask EndCheckpointAsync()
        //{
        //    var bytes = m_metadataSerializer.Serialize(m_metadata);
        //    await m_adminSession.UpsertAsync(1, SpanByte.FromFixedSpan(bytes));

        //    bool success = false;
        //    Guid token;

        //    do
        //    {
        //        (success, token) = await m_persistentStorage.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
        //    } while (!success);


        //    var tokens = m_persistentStorage.CheckpointManager.GetLogCheckpointTokens();
        //    foreach (var oldToken in tokens)
        //    {
        //        if (oldToken.Equals(token))
        //        {
        //            continue;
        //        }
        //        if (m_metadata.Checkpoints.Contains(oldToken))
        //        {
        //            continue;
        //        }
        //        m_persistentStorage.CheckpointManager.Purge(oldToken);
        //    }
        //}

        public async Task Compact()
        {
            m_adminSession.Compact(m_persistentStorage.Log.SafeReadOnlyAddress, CompactionType.Lookup);
            m_persistentStorage.Log.Truncate();
            await m_persistentStorage.TakeFullCheckpointAsync(CheckpointType.Snapshot);
        }

        public IStateManagerClient GetOrCreateClient(string name)
        {
            lock(m_metadata_lock)
            {
                if (_clients.TryGetValue(name, out var client))
                {
                    return client;
                }
                else
                {
                    client = new StateManagerClient(name, this);
                    _clients.Add(name, client);
                    return client;
                }
            }
        }

        public async Task InitializeAsync()
        {
            bool newMetadata = false;
            try
            {
                m_lruCache.Clear();
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

            // Reset cached values in the state clients
            foreach(var stateClient in _stateClients)
            {
                await stateClient.Value.Reset(newMetadata);
            }

            Initialized = true;
        }

        internal void Flush()
        {
            m_persistentStorage.Log.FlushAndEvict(true);
        }

        internal async ValueTask<StateClient<TValue, TMetadata>> CreateClientAsync<TValue, TMetadata>(string client, StateClientOptions<TValue> options)
            where TValue: ICacheObject
        {
            if (_stateClients.TryGetValue(client, out var cachedClient))
            {
                return (cachedClient as StateClient<TValue, TMetadata>)!;
            }
            Monitor.Enter(m_metadata_lock);
            await _semaphore.WaitAsync();
            if (m_metadata.ClientMetadataLocations.TryGetValue(client, out var location))
            {
                Monitor.Exit(m_metadata_lock);
                var readAsyncOp = await m_adminSession.ReadAsync(location);
                var (_, v) = readAsyncOp.Complete();
                var metadata = StateClientMetadataSerializer.Instance.Deserialize<TMetadata>(new ByteMemoryOwner(v), v.Length);
                var session = m_persistentStorage.For(m_functions).NewSession(m_functions);
                var stateClient = new StateClient<TValue, TMetadata>(this, location, metadata, session, options);
                _stateClients.Add(client, stateClient);
                _semaphore.Release();
                return stateClient;
            }
            else
            {
                Monitor.Exit(m_metadata_lock);
                // Allocate a new page id for the client metadata.
                var clientMetadataPageId = GetNewPageId();
                var clientMetadata = new StateClientMetadata<TMetadata>();
                m_metadata.ClientMetadataLocations.Add(client, clientMetadataPageId);
                await m_adminSession.UpsertAsync(clientMetadataPageId, SpanByte.FromFixedSpan(StateClientMetadataSerializer.Instance.Serialize(clientMetadata)));
                var session = m_persistentStorage.For(m_functions).NewSession(m_functions);
                var stateClient = new StateClient<TValue, TMetadata>(this, clientMetadataPageId, clientMetadata, session, options);
                _stateClients.Add(client, stateClient);
                _semaphore.Release();
                return stateClient;
            }
        }

        internal async ValueTask WriteAsync<T>(long pageId, T value, IStateSerializer<T> serializer)
            where T : ICacheObject
        {
            await _semaphore.WaitAsync();
            var bytes = serializer.Serialize(value, new StateSerializeOptions());
            if (!(bytes[0] == 2 || bytes[0] == 3))
            {

            }
            var upsertOp = await m_adminSession.UpsertAsync(pageId, SpanByte.FromFixedSpan(bytes));
            upsertOp.Complete();
            _semaphore.Release();
        }

        internal async ValueTask WriteAsync(long pageId, byte[] bytes)
        {
            await _semaphore.WaitAsync();
            try
            {
                var upsertOp = await m_adminSession.UpsertAsync(pageId, SpanByte.FromFixedSpan(bytes));
                upsertOp.Complete();
            }
            finally
            {
                _semaphore.Release();
            }
        }

        internal ValueTask<T> ReadAsync<T>(long pageId, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            _semaphore.Wait();
            try
            {
                var page = session.Read(pageId);
                
                if (!page.status.IsCompleted)
                {
                    return ReadAsync_Slow(pageId, serializer, session);
                }
                //var (status, result) = page.Complete();
                var deserialized = serializer.Deserialize(new ByteMemoryOwner(page.output), page.output.Length, new StateSerializeOptions());
                _semaphore.Release();
                return ValueTask.FromResult(deserialized);
            }
            catch(Exception e)
            {
                _semaphore.Release();
                throw e;
            }
            //finally
            //{
                
            //}
        }

        private async ValueTask<T> ReadAsync_Slow<T>(long pageId, IStateSerializer<T> serializer, ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
            where T : ICacheObject
        {
            var outputs = await session.CompletePendingWithOutputsAsync();
            if (outputs.Next())
            {
                if (outputs.Current.Key == pageId)
                {
                    _semaphore.Release();
                    return serializer.Deserialize(new ByteMemoryOwner(outputs.Current.Output), outputs.Current.Output.Length, new StateSerializeOptions());
                }
                else
                {
                    throw new Exception();
                }
            }
            throw new Exception();
        }
    }
}
