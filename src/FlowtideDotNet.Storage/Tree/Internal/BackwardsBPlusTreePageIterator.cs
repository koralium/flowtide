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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BackwardsBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>
        : IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        internal class Enumerator : IEnumerator<KeyValuePair<K, V>>
        {
            private int _startIndex;
            private int index;
            private LeafNode<K, V, TKeyContainer, TValueContainer>? leafNode;
            private KeyValuePair<K, V> _current;

            public Enumerator()
            {
            }

            public void Reset(in LeafNode<K, V, TKeyContainer, TValueContainer> leafNode, in int index)
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
                Debug.Assert(leafNode != null);
                if (index >= 0)
                {
                    _current = new KeyValuePair<K, V>(leafNode.keys.Get(index), leafNode.values.Get(index));
                    index--;
                    return true;
                }
                return false;
            }

            public void Reset()
            {
                index = _startIndex;
            }
        }

        private LeafNode<K, V, TKeyContainer, TValueContainer>? leaf;
        private int index;
        private BPlusTree<K, V, TKeyContainer, TValueContainer> tree;
        private Enumerator enumerator;

        public BackwardsBPlusTreePageIterator(in BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            this.tree = tree;
            enumerator = new Enumerator();
        }

        public void Reset(in LeafNode<K, V, TKeyContainer, TValueContainer>? leaf, in int index)
        {
            this.leaf = leaf;
            this.index = index;
        }

        public TKeyContainer Keys => leaf != null ? leaf.keys : throw new InvalidOperationException("Tried getting keys on an inactive iterator");

        public TValueContainer Values => leaf != null ? leaf.values : throw new InvalidOperationException("Tried getting keys on an inactive iterator");

        public LeafNode<K, V, TKeyContainer, TValueContainer> CurrentPage => leaf ?? throw new InvalidOperationException("Tried getting current page on an inactive iterator");

        public ValueTask SavePage(bool checkForResize)
        {
            Debug.Assert(leaf != null);
            var byteSize = leaf.GetByteSize();
            if (leaf.keys.Count > 0 && checkForResize && tree.m_stateClient.Metadata!.PageSizeBytes < byteSize)
            {
                tree.m_stateClient.AddOrUpdate(leaf.Id, leaf);
                // Force a traversion of the tree to ensure that size is looked at for splits.
                var traverseTask = tree.RMWNoResult(leaf.keys.Get(0), default, (input, current, found) =>
                {
                    return (default, GenericWriteOperation.None);
                });
                if (!traverseTask.IsCompletedSuccessfully)
                {
                    return WaitForTraverseTask(traverseTask);
                }
            }
            else
            {
                var isFull = tree.m_stateClient.AddOrUpdate(leaf.Id, leaf);
                if (isFull)
                {
                    return WaitForNotFull();
                }
            }

            return ValueTask.CompletedTask;
        }

        private async ValueTask WaitForTraverseTask(ValueTask<GenericWriteOperation> traverseTask)
        {
            await traverseTask;
        }

        private async ValueTask WaitForNotFull()
        {
            await tree.m_stateClient.WaitForNotFullAsync();
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            Debug.Assert(leaf != null);
            enumerator.Reset(leaf, index);
            return enumerator;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            Debug.Assert(leaf != null);
            enumerator.Reset(leaf, index);
            return enumerator;
        }

        public void EnterWriteLock()
        {
            Debug.Assert(leaf != null);
            leaf.EnterWriteLock();
        }

        public void ExitWriteLock()
        {
            Debug.Assert(leaf != null);
            leaf.ExitWriteLock();
        }
    }
}
