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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class TemporarySyncStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ILruEvictHandler
        where V : ICacheObject
        where TMetadata : class, IStorageMetadata
    {
        private StateClientMetadata<TMetadata> metadata;
        private readonly SyncStateClient<V, TMetadata> baseClient;

        public TemporarySyncStateClient(
            StateClientMetadata<TMetadata> metadata,
            IStateClient<V, TMetadata> baseClient)
        {
            this.metadata = metadata;
            this.baseClient = (baseClient as SyncStateClient<V, TMetadata>)!;
        }
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

        public int BPlusTreePageSize => baseClient.BPlusTreePageSize;

        public int BPlusTreePageSizeBytes => baseClient.BPlusTreePageSizeBytes;

        public long CacheMisses => baseClient.CacheMisses;

        public override long MetadataId => baseClient.MetadataId;

        public bool AddOrUpdate(in long key, V value)
        {
            return baseClient.AddOrUpdate(key, value);
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public void Delete(in long key)
        {
            baseClient.Delete(key);
        }

        public override void Dispose()
        {
            baseClient.Dispose();
        }

        public long GetNewPageId()
        {
            return baseClient.GetNewPageId();
        }

        public ValueTask<V?> GetValue(in long key, string from)
        {
            return baseClient.GetValue(key, from);
        }

        public override ValueTask Reset(bool clearMetadata)
        {
            return baseClient.Reset(clearMetadata);
        }

        public Task WaitForNotFullAsync()
        {
            return baseClient.WaitForNotFullAsync();
        }

        public void Evict(List<(LinkedListNode<LruTableSync.LinkedListValue>, long)> valuesToEvict, bool isCleanup)
        {
            baseClient.Evict(valuesToEvict, isCleanup);
        }
    }
}
