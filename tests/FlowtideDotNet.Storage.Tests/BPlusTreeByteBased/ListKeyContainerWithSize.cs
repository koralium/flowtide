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

namespace FlowtideDotNet.Storage.Tests.BPlusTreeByteBased
{
    internal class ListKeyContainerWithSize : ListKeyContainer<KeyValuePair<long, long>>
    {
        private readonly int sizePerElement;

        public ListKeyContainerWithSize(int sizePerElement)
        {
            this.sizePerElement = sizePerElement;
        }

        public override int GetByteSize(int start, int end)
        {
            int result = 0;
            for (int i = start; i <= end; i++)
            {
                result += (int)_list[i].Value;
            }
            return result;
        }

        public override int GetByteSize()
        {
            return GetByteSize(0, _list.Count - 1);
        }
    }
}
