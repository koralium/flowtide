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

using FASTER.core;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Avanced
{
    internal class AdvancedStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, IDisposable
        where V : ICacheObject
    {
        private readonly StateManagerAdvanced m_manager;
        private readonly long metadataId;
        private readonly StateClientMetadata<TMetadata> metadata;
        private readonly ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_session;
        private bool disposedValue;
        private IStateSerializer<V> m_serializer;
        private HashSet<long> m_modified;
        private SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);
        private string inFunc;
        private string _from;

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

        internal AdvancedStateClient(
            StateManagerAdvanced manager,
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
            m_modified = new HashSet<long>();
        }

        public ValueTask AddOrUpdate(in long key, in V value)
        {
            return AddOrUpdate_Slow(key, value);
            //semaphoreSlim.Wait();
            //// Add or reset the modified index
            //m_modified.Add(key);
            //m_manager.AddOrUpdateAsync(key, value, m_serializer);
            //semaphoreSlim.Release();
        }

        private async ValueTask AddOrUpdate_Slow(long key, V value)
        {
            if (semaphoreSlim.CurrentCount == 0)
            {

            }
            await semaphoreSlim.WaitAsync();
            inFunc = "add";
            // Add or reset the modified index
            m_modified.Add(key);
            await m_manager.AddOrUpdateAsync(key, value, m_serializer);
            semaphoreSlim.Release();
        }

        public override async ValueTask Reset(bool clearMetadata)
        {
            await m_manager.ResetKeys(m_modified);
            m_modified.Clear();
            if (clearMetadata)
            {
                Metadata = default;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~SimpleStateClient()
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

        public long GetNewPageId()
        {
            return m_manager.GetNewPageId();
        }

        public ValueTask<V?> GetValue(in long key, string from)
        {
            return GetValue_Slow(key, from);
            //return m_manager.ReadAsync(key, m_serializer, m_session);
        }

        private async ValueTask<V?> GetValue_Slow(long key, string from)
        {
            if (semaphoreSlim.CurrentCount == 0)
            {

            }
            await semaphoreSlim.WaitAsync();
            inFunc = "get";
            _from = from;
            var val = await m_manager.ReadAsync(key, m_serializer, m_session);
            semaphoreSlim.Release();
            return val;
        }

        public async ValueTask Commit()
        {
            if (semaphoreSlim.CurrentCount == 0)
            {

            }
            await semaphoreSlim.WaitAsync();
            inFunc = "commit";
            await m_manager.Commit(m_modified, m_session);
            m_modified.Clear();
            // Commit metadata
            var bytes = StateClientMetadataSerializer.Instance.Serialize(metadata);
            await m_manager.WritePersistentAsync(metadataId, bytes, m_session);
            semaphoreSlim.Release();
        }

        public void Delete(in long key)
        {
            if (semaphoreSlim.CurrentCount == 0)
            {

            }
            semaphoreSlim.Wait();
            inFunc = "delete";
            m_manager.Delete(key);
            semaphoreSlim.Release();
        }
    }
}
