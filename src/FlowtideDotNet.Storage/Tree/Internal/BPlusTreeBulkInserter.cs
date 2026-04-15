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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal readonly struct ExternalKeyComparer<T, TKeyContainer> : IComparer<int>
        where TKeyContainer : IKeyContainer<T>
    {
        private readonly T[] _keys;
        private readonly IBplusTreeComparer<T, TKeyContainer> _comparer;

        public ExternalKeyComparer(T[] keys, IBplusTreeComparer<T, TKeyContainer> comparer)
        {
            _keys = keys;
            _comparer = comparer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            return _comparer.CompareTo(_keys[x], _keys[y]);
        }
    }

    internal class BPlusTreeBulkInserter<K, V, TKeyContainer, TValueContainer> 
        : IBPlusTreeBulkInserter<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        

        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> _tree;
        private int[] _sortedIndices = Array.Empty<int>();
        private int _keyLength;
        List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _mappings;
        public BPlusTreeBulkInserter(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            _tree = tree;
            _mappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
        }

        public ValueTask StartBatch(K[] keys, V[] values)
        {
            if (keys.Length > _sortedIndices.Length)
            {
                _sortedIndices = new int[keys.Length];
            }
            _keyLength = keys.Length;

            // Sort the keys and values by keys
            var keyComparer = _tree.m_keyComparer;

            for (int i = 0; i < keys.Length; i++)
            {
                _sortedIndices[i] = i;
            }

            var indicesSpan = _sortedIndices.AsSpan().Slice(0, _keyLength);
            indicesSpan.Sort(new ExternalKeyComparer<K, TKeyContainer>(keys,keyComparer));

            return _tree.RouteBatchRootAsync(keys, _sortedIndices, _tree.m_keyComparer, _mappings);
        }


    }
}
