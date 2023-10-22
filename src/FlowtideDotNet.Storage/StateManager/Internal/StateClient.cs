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

using FlowtideDotNet.Storage.StateManager.Internal.Cache;
using FlowtideDotNet.Storage.StateManager.Internal.Simple;
using FASTER.core;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal abstract class StateClient
    {
        public abstract ValueTask Reset(bool clearMetadata);
    }

    internal class StateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ICacheClient, IDisposable
        where V: ICacheObject
    {
        private readonly StateManager m_manager;
        private readonly long metadataId;
        private readonly StateClientMetadata<TMetadata> metadata;
        private readonly ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_session;
        private FasterLog m_temporaryStorage;
        private ConcurrentDictionary<long, long> m_modified;
        private IStateSerializer<V> m_serializer;
        private bool disposedValue;

        private LruTable lruTable;

        //private readonly object m_lock = new object();

        public TMetadata? Metadata
        {
            get
            {
                return metadata.Metadata;
            }
            set
            {
                metadata.Metadata = value;
            }
        }

        internal StateClient(
            StateManager manager,
            long metadataId,
            StateClientMetadata<TMetadata> metadata,
            ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session,
            StateClientOptions<V> options)
        {
            Debug.Assert(options.ValueSerializer != null);
            this.m_manager = manager;
            this.metadataId = metadataId;
            this.metadata = metadata;
            this.m_session = session;
            m_serializer = options.ValueSerializer;
            m_modified = new ConcurrentDictionary<long, long>();

            lruTable = new LruTable(1000000, OnCacheRemove);
            // Create the temporary storage for this client.
            m_temporaryStorage = new FASTER.core.FasterLog(new FASTER.core.FasterLogSettings()
            {
                AutoCommit = true,
                LogDevice = options.TemporaryLogDevice,
                MemorySize = 1024 * 1024 * 16,
                PageSize = 1024 * 1024,
            });
        }

        public ValueTask AddOrUpdate(in long key, in V value)
        {
            // Add or reset the modified index
            m_modified[key] = 0;
            return lruTable.Add(key, value);
        }

        public void Delete(in long key)
        {
            // Keys marked with -1 is deleted
            m_modified[key] = -1;
            lruTable.Delete(key);
        }

        public long GetNewPageId()
        {
            return m_manager.GetNewPageId();
        }

        /// <summary>
        /// This function commits all modified data from the client into persistent storage.
        /// This itself does not guarantee persistence, a checkpoint must be taken on the memory manager as well.
        /// </summary>
        /// <returns></returns>
        public async ValueTask Commit()
        {
            foreach (var modify in m_modified)
            {
                // Check if deleted
                if (modify.Value == -1)
                {
                    var deleteOp = await m_session.DeleteAsync(modify.Key);
                    deleteOp.Complete();
                }
                else if (modify.Value == 0)
                {
                    if (lruTable.TryGetValueNoRefresh(modify.Key, out var val))
                    {
                        await m_manager.WriteAsync(modify.Key, (V)val, m_serializer);
                        //var bytes = m_serializer.Serialize(((V)val!));
                        //var upsertOperation = await m_session.UpsertAsync(modify.Key, SpanByte.FromPinnedMemory(bytes));
                        //upsertOperation.Complete();
                    }
                    else
                    {
                        throw new InvalidOperationException("Modified page is missing in LRU table, and does not exist in temporary storage.");
                    }
                }
                else
                {
                    var (bytes, length) = await m_temporaryStorage.ReadAsync(modify.Value);
                    if (!(bytes[0] == 2 || bytes[0] == 3))
                    {

                    }
                    await m_manager.WriteAsync(modify.Key, bytes);
                    //if (!(bytes[0] == 2 || bytes[0] == 3))
                    //{

                    //}
                    //var spanByte = SpanByte.FromFixedSpan(bytes);
                    //var upsertOperation = await m_session.UpsertAsync(modify.Key, spanByte);
                    //upsertOperation.Complete();
                }
            }

            // Upsert the client metadata to persistence
            {
                //await m_manager.WriteAsync<StateClientMetadata>(metadataId, metadata, StateClientMetadataSerializer.Instance);
                var bytes = StateClientMetadataSerializer.Instance.Serialize(metadata);
                await m_manager.WriteAsync(metadataId, bytes);
                //var metadataUpsert = await m_session.UpsertAsync(metadataId, SpanByte.FromFixedSpan(bytes));
                //var status = metadataUpsert.Complete();
                //if (!status.IsCompletedSuccessfully)
                //{

                //}
            }


            // Clear the modified table
            m_modified.Clear();

            // Reset the temporary storage
            m_temporaryStorage.Reset();
        }

        public ValueTask<V?> GetValue(in long key, string from)
        {
            if (lruTable.TryGetValue(key, out var value))
            {
                if (value is V vVal)
                {
                    return ValueTask.FromResult(vVal);
                }
                
            }
            return GetValue_Slow(key);
        }

        private async ValueTask<V?> GetValue_Slow(long key)
        {
            if (m_modified.TryGetValue(key, out var storageLocation))
            {
                if (storageLocation == -1)
                {
                    return default;
                }
                if (storageLocation == 0)
                {
                    throw new InvalidOperationException("Unexpected error, modified data not in LRU and not in temporary storage");
                }
                var (bytes, length) = await m_temporaryStorage.ReadAsync(storageLocation);

                var deserialized = m_serializer.Deserialize(new ByteMemoryOwner(bytes), length);
                await lruTable.Add(key, deserialized);
                return deserialized;
            }
            {
                var deserialized = await m_manager.ReadAsync<V>(key, m_serializer, m_session);
                //var persistentRead = await m_session.ReadAsync(key);
                //var (status, output) = persistentRead.Complete();
                //var deserialized = m_serializer.Deserialize(output.Memory, output.Length);
                await lruTable.Add(key, deserialized);
                return deserialized;
            }
        }

        public void ResetModifiedAndCache()
        {
            // add a clear here for LRU table as well to reduce memory footprint.

            m_modified.Clear();
            m_temporaryStorage.Reset();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    m_session.Dispose();
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

        public ValueTask OnCacheRemove(IEnumerable<KeyValuePair<long, ICacheObject>> toBeRemoved)
        {
            bool enqueued = false;
            foreach (var val in toBeRemoved)
            {
                if (m_modified.TryGetValue(val.Key, out var storageIndex) && storageIndex == 0)
                {
                    // Store the modified data to disk in temporary storage
                    // must lock the data before writing since this call can come from another thread
                    val.Value.EnterWriteLock();
                    m_modified[val.Key] = m_temporaryStorage.Enqueue(m_serializer.Serialize((V)val.Value!));
                    val.Value.ExitWriteLock();
                    enqueued = true;
                }
            }
            if (enqueued)
            {
                return m_temporaryStorage.CommitAsync();
            }
            else
            {
                return ValueTask.CompletedTask;
            }
            throw new NotImplementedException();
        }

        public override ValueTask Reset(bool clearMetadata)
        {
            lruTable.Clear();
            m_modified.Clear();

            if (clearMetadata)
            {
                Metadata = default;
            }
            return ValueTask.CompletedTask;
        }
    }
}
