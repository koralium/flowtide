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

using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    public class TreeBackwardIterator<K, V, TKeyContainer, TValueContainer>
        : IAsyncEnumerable<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        private Enumerator enumerator;

        internal TreeBackwardIterator(
            in BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            enumerator = new Enumerator(tree);
        }

        internal void Reset(LeafNode<K, V, TKeyContainer, TValueContainer> leafNode, int index)
        {
            enumerator.Reset(leafNode, index);
        }

        class Enumerator : IAsyncEnumerator<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>>
        {
            private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> tree;
            internal LeafNode<K, V, TKeyContainer, TValueContainer>? leafNode;
            private int index;
            private bool started;
            private BackwardsBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer> pageIterator;

            public Enumerator(in BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
            {
                this.tree = tree;
                pageIterator = new BackwardsBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>(tree);
            }

            public void Reset(LeafNode<K, V, TKeyContainer, TValueContainer>? leafNode, int index)
            {
                if (this.leafNode != null)
                {
                    this.leafNode.Return();
                }
                this.leafNode = leafNode;
                this.index = index;
                this.started = false;
                pageIterator.Reset(leafNode, index);
            }

            public IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer> Current => pageIterator;

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (leafNode == null)
                {
                    pageIterator.Reset(null, index);
                    return ValueTask.FromResult(false);
                }
                if (!started)
                {
                    started = true;
                    pageIterator.Reset(leafNode, index);
                    return ValueTask.FromResult(true);
                }
                if (leafNode.previous == 0)
                {
                    pageIterator.Reset(null, index);
                    return ValueTask.FromResult(false);
                }
                var getNextPageTask = tree.m_stateClient.GetValue(leafNode.previous);

                if (!getNextPageTask.IsCompleted)
                {
                    return MoveNextAsync_Slow(getNextPageTask);
                }
                leafNode.Return();
                leafNode = (getNextPageTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
                index = leafNode.keys.Count - 1;
                pageIterator.Reset(leafNode, index);
                return ValueTask.FromResult(true);
            }

            private async ValueTask<bool> MoveNextAsync_Slow(ValueTask<IBPlusTreeNode?> getPageTask)
            {
                var page = await getPageTask;
                leafNode!.Return();
                leafNode = (page as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
                index = leafNode.keys.Count - 1;
                pageIterator.Reset(leafNode, index);
                return true;
            }
        }

        public IAsyncEnumerator<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return enumerator;
        }
    }
}
