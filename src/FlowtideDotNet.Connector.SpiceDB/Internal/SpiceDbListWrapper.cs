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

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class SpiceDbListWrapper<TInternal, TPublic> : IList<TPublic>
    where TInternal : class, TPublic
    {
        private readonly IList<TInternal> _internalList;
        public SpiceDbListWrapper(IList<TInternal> internalList) => _internalList = internalList;

        public TPublic this[int index] { get => _internalList[index]; set => _internalList[index] = (TInternal)value!; }
        public int Count => _internalList.Count;
        public bool IsReadOnly => _internalList.IsReadOnly;
        public void Add(TPublic item) => _internalList.Add((TInternal)item!);
        public void Clear() => _internalList.Clear();
        public bool Contains(TPublic item) => _internalList.Contains((TInternal)item!);
        public void CopyTo(TPublic[] array, int arrayIndex)
        {
            for (int i = 0; i < _internalList.Count; i++)
            {
                array[arrayIndex + i] = _internalList[i];
            }
        }
        public IEnumerator<TPublic> GetEnumerator() => _internalList.GetEnumerator();
        public int IndexOf(TPublic item) => _internalList.IndexOf((TInternal)item!);
        public void Insert(int index, TPublic item) => _internalList.Insert(index, (TInternal)item!);
        public bool Remove(TPublic item) => _internalList.Remove((TInternal)item!);
        public void RemoveAt(int index) => _internalList.RemoveAt(index);
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _internalList.GetEnumerator();
    }
}
