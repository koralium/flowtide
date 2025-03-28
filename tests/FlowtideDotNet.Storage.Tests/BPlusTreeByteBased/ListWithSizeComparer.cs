﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    internal class ListWithSizeComparer : IBplusTreeComparer<KeyValuePair<long, long>, ListKeyContainerWithSize>
    {
        private readonly IComparer<KeyValuePair<long, long>> comparer;

        public ListWithSizeComparer(IComparer<long> comparer)
        {
            this.comparer = new KeyValuePairComparer();
        }

        public bool SeekNextPageForValue => false;

        public int CompareTo(in KeyValuePair<long, long> x, in KeyValuePair<long, long> y)
        {
            return comparer.Compare(x, y);
        }

        public int CompareTo(in KeyValuePair<long, long> key, in ListKeyContainerWithSize keyContainer, in int index)
        {
            return comparer.Compare(key, keyContainer.Get(index));
        }

        public int FindIndex(in KeyValuePair<long, long> key, in ListKeyContainerWithSize keyContainer)
        {
            return keyContainer._list.BinarySearch(key, comparer);
        }

        //public int CompareTo(in K x, in K y)
        //{
        //    return comparer.Compare(x, y);
        //}

        //public int CompareTo(in K key, in ListKeyContainerWithSize<K> keyContainer, in int index)
        //{
        //    return comparer.Compare(key, keyContainer.Get(index));
        //}

        //public int FindIndex(in K key, in ListKeyContainerWithSize<K> keyContainer)
        //{
        //    return keyContainer._list.BinarySearch(key, comparer);
        //}
    }
}
