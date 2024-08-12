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

using System.Diagnostics;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeIterator<K, V, TKeyContainer, TValueContainer> : IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer: IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        internal class Enumerator : IAsyncEnumerator<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>>
        {
            private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> tree;
            internal LeafNode<K, V, TKeyContainer, TValueContainer>? leafNode;
            private int index;
            private bool started;
            private BPlusTreePageIterator<K, V, TKeyContainer, TValueContainer> pageIterator;

            public Enumerator(in BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
            {
                this.tree = tree;
                pageIterator = new BPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>(tree);
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
                if (leafNode.next == 0)
                {
                    pageIterator.Reset(null, index);
                    return ValueTask.FromResult(false);
                }
                var getNextPageTask = tree.m_stateClient.GetValue(leafNode.next, "MoveNextAsync2");

                if (!getNextPageTask.IsCompleted)
                {
                    return MoveNextAsync_Slow(getNextPageTask);
                }
                leafNode.Return();
                leafNode = (getNextPageTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
                index = 0;
                pageIterator.Reset(leafNode, index);
                return ValueTask.FromResult(true);
            }

            private async ValueTask<bool> MoveNextAsync_Slow(ValueTask<IBPlusTreeNode?> getPageTask)
            {
                var page = await getPageTask;
                leafNode!.Return();
                leafNode = (page as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
                index = 0;
                pageIterator.Reset(leafNode, index);
                return true;
            }
        }

        private LeafNode<K, V, TKeyContainer, TValueContainer>? leafNode;
        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> tree;
        private int index;
        private readonly Enumerator enumerator;

        public BPlusTreeIterator(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            this.tree = tree;
            enumerator = new Enumerator(tree);
        }

        public IAsyncEnumerator<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return enumerator;
        }

        public ValueTask Seek(in K key, in IBplusTreeComparer<K, TKeyContainer>? searchComparer = null)
        {
            if (enumerator.leafNode != null)
            {
                // Return previous rented node
                enumerator.leafNode.Return();
                enumerator.leafNode = null;
            }
            var comparer = searchComparer == null ? tree.m_keyComparer : searchComparer;
            var searchTask = tree.SearchRoot(key, comparer);
            if (!searchTask.IsCompleted)
            {
                return Seek_Slow(searchTask, key, comparer);
            }
            leafNode = searchTask.Result;
            AfterSeek(key, comparer);
            return ValueTask.CompletedTask;
        }

        private async ValueTask Seek_Slow(ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> task, K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            leafNode = await task;
            AfterSeek(key, searchComparer);
        }

        private void AfterSeek(in K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            Debug.Assert(leafNode != null);
            var i = searchComparer.FindIndex(key, leafNode.keys);
            if (i < 0)
            {
                i = ~i;
            }
            index = i;
            if (index >= leafNode.keys.Count && leafNode.next == 0)
            {
                leafNode.Return();
                leafNode = null;
            }
            enumerator.Reset(leafNode, index);
        }

        public async ValueTask SeekFirst()
        {
            leafNode = await tree.LeftLeaf();
            index = 0;
            enumerator.Reset(leafNode, index);
        }

        public void Dispose()
        {
            if (enumerator.leafNode != null)
            {
                enumerator.leafNode.Return();
                enumerator.leafNode = null;
            }
        }

        public void Reset()
        {
            if (enumerator.leafNode != null)
            {
                enumerator.leafNode.Return();
                enumerator.leafNode = null;
            }
        }
    }
}
