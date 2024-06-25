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

namespace FlowtideDotNet.Storage.Tree
{
    public class ListKeyContainer<K> : IKeyContainer<K>
    {
        internal readonly List<K> _list;

        public ListKeyContainer()
        {
            _list = new List<K>();
        }

        public int Count => _list.Count;

        public void Add(K key)
        {
            _list.Add(key);
        }

        public void AddRangeFrom(IKeyContainer<K> container, int start, int count)
        {
            if (container is ListKeyContainer<K> listContainer)
            {
                _list.AddRange(listContainer._list.GetRange(start, count));
            }
            else
            {
                throw new NotImplementedException("List container can not copy values from other containers");
            }
        }

        public int BinarySearch(K key, IComparer<K> comparer)
        {
            return _list.BinarySearch(key, comparer);
        }

        public void Dispose()
        {
        }

        public K Get(in int index)
        {
            return _list[index];
        }

        public void Insert(int index, K key)
        {
            _list.Insert(index, key);
        }

        public void Insert_Internal(int index, K key)
        {
            _list.Insert(index, key);
        }

        public void RemoveAt(int index)
        {
            _list.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _list.RemoveRange(start, count);
        }

        public void Update(int index, K key)
        {
            _list[index] = key;
        }
    }
}
