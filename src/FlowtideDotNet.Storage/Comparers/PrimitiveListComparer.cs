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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Comparers
{
    public class PrimitiveListComparer<T> : IBplusTreeComparer<T, PrimitiveListKeyContainer<T>>
        where T : unmanaged, IComparable<T>
    {
        private class Comparer : IComparer<T>
        {
            public int Compare(T x, T y)
            {
                return x.CompareTo(y);
            }
        }

        private Comparer m_comparer;

        public PrimitiveListComparer()
        {
            m_comparer = new Comparer();
        }
        public bool SeekNextPageForValue => true;

        public int CompareTo(in T x, in T y)
        {
            return x.CompareTo(y);
        }

        public int CompareTo(in T key, in PrimitiveListKeyContainer<T> keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in T key, in PrimitiveListKeyContainer<T> keyContainer)
        {
            return keyContainer.BinarySearch(key, m_comparer);
        }
    }
}
