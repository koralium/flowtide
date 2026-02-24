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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    internal class ByteCacheObject : ICacheObject
    {
        public ByteCacheObject(byte[] data)
        {
            Data = data;
        }
        public int RentCount => 0;

        public bool RemovedFromCache { get; set; }
        public byte[] Data { get; }

        public void EnterWriteLock()
        {
        }

        public void ExitWriteLock()
        {
        }

        public void Return()
        {
        }

        public bool TryRent()
        {
            return true;
        }
    }

    internal class LocalCacheTestSerializer : IStateSerializer<ByteCacheObject>
    {
        public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public void ClearTemporaryAllocations()
        {
        }

        public ByteCacheObject Deserialize(ReadOnlySequence<byte> bytes, int length)
        {
            return new ByteCacheObject(bytes.Slice(0, length).ToArray());
        }

        public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public void Dispose()
        {
        }

        public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ByteCacheObject value)
        {
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
        {
        }
    }
}
