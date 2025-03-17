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

using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Storage.Tests.BPlusTreeByteBased
{
    internal class ListKeyWithSizeSerializer : IBPlusTreeKeySerializer<KeyValuePair<long, long>, ListKeyContainerWithSize>
    {
        private readonly int sizePerElement;

        public ListKeyWithSizeSerializer(int sizePerElement)
        {
            this.sizePerElement = sizePerElement;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ListKeyContainerWithSize CreateEmpty()
        {
            return new ListKeyContainerWithSize(1);
        }

        public ListKeyContainerWithSize Deserialize(ref SequenceReader<byte> reader)
        {
            throw new NotImplementedException();
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ListKeyContainerWithSize values)
        {
            throw new NotImplementedException();
        }
    }
}
