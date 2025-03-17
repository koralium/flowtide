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

namespace FlowtideDotNet.Storage.Tree
{
    public interface IKeyContainer<K> : IDisposable
    {
        int BinarySearch(K key, IComparer<K> comparer);

        void Add(K key);

        void Insert(int index, K key);

        void Insert_Internal(int index, K key);

        void Update(int index, K key);

        void RemoveAt(int index);

        int Count { get; }

        K Get(in int index);

        void AddRangeFrom(IKeyContainer<K> container, int start, int count);

        void RemoveRange(int start, int count);

        int GetByteSize();

        int GetByteSize(int start, int end);
    }
}
