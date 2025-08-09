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

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeSerializerInitializeContext : IBPlusTreeSerializerInitializeContext
    {
        private readonly IStateSerializerInitializeReader stateSerializerInitializeReader;
        private readonly IReadOnlyList<long> keys;

        public BPlusTreeSerializerInitializeContext(IStateSerializerInitializeReader stateSerializerInitializeReader, IReadOnlyList<long> keys)
        {
            this.stateSerializerInitializeReader = stateSerializerInitializeReader;
            this.keys = keys;
        }

        public IReadOnlyList<long> SavedPageIds => keys;

        public long GetNewPageId()
        {
            return stateSerializerInitializeReader.GetNewPageId();
        }

        public Task<ReadOnlyMemory<byte>> ReadPage(long pageId)
        {
            return stateSerializerInitializeReader.ReadPage(pageId);
        }
    }
}
