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

using System.Collections;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal struct BPlusTreePageIterator<K, V, TKeyContainer, TValueContainer> : IBPlusTreePageIterator<K, V>
        where TKeyContainer: IKeyContainer<K>
        where TValueContainer: IValueContainer<V>
    {
        internal struct Enumerator : IEnumerator<KeyValuePair<K, V>>
        {
            private readonly int _startIndex;
            private int index;
            private LeafNode<K, V, TKeyContainer, TValueContainer> leafNode;
            private KeyValuePair<K, V> _current;

            public Enumerator(in LeafNode<K, V, TKeyContainer, TValueContainer> leafNode, in int index)
            {
                _startIndex = index;
                this.index = index;
                this.leafNode = leafNode;
            }

            public KeyValuePair<K, V> Current => _current;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (index < leafNode.keys.Count)
                {
                    _current = new KeyValuePair<K, V>(leafNode.keys.Get(index), leafNode.values.Get(index));
                    index++;
                    return true;
                }
                return false;
            }

            public void Reset()
            {
                index = _startIndex;
            }
        }

        private readonly LeafNode<K, V, TKeyContainer, TValueContainer> leaf;
        private readonly int index;
        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> tree;

        public BPlusTreePageIterator(in LeafNode<K, V, TKeyContainer, TValueContainer> leaf, in int index, in BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            this.leaf = leaf;
            this.index = index;
            this.tree = tree;
        }

        public IKeyContainer<K> Keys => leaf.keys;

        public IValueContainer<V> Values => leaf.values;

        public ValueTask SavePage()
        {
            var isFull = tree.m_stateClient.AddOrUpdate(leaf.Id, leaf);
            if (isFull)
            {
                return WaitForNotFull();
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask WaitForNotFull()
        {
            await tree.m_stateClient.WaitForNotFullAsync();
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            return new Enumerator(leaf, index);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(leaf, index);
        }
    }
}
