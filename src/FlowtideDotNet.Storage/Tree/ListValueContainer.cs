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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    public class ListValueContainer<V> : IValueContainer<V>
    {
        internal readonly List<V> _values;

        public ListValueContainer()
        {
            _values = new List<V>();
        }

        public int Count => _values.Count;

        public void AddRangeFrom(IValueContainer<V> container, int start, int count)
        {
            if (container is ListValueContainer<V> listValueContainer)
            {
                _values.AddRange(listValueContainer._values.GetRange(start, count));
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Dispose()
        {
        }

        public V Get(int index)
        {
            return _values[index];
        }

        public int GetByteSize()
        {
            return -1;
        }

        public int GetByteSize(int start, int end)
        {
            return -1;
        }

        public ref V GetRef(int index)
        {
            return ref CollectionsMarshal.AsSpan(_values)[index];
        }

        public void Insert(int index, V value)
        {
            _values.Insert(index, value);
        }

        public void RemoveAt(int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public void Update(int index, V value)
        {
            _values[index] = value;
        }
    }
}
