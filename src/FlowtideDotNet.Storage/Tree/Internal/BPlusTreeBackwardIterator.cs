using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeBackwardIterator<K, V, TKeyContainer, TValueContainer> : IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        internal class Enumerator : IAsyncEnumerator<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>>
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

        private LeafNode<K, V, TKeyContainer, TValueContainer>? leafNode;
        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> tree;
        private int index;
        private readonly Enumerator enumerator;

        public BPlusTreeBackwardIterator(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
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
            return AfterSeekTask(key, comparer);
        }

        private async ValueTask Seek_Slow(ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> task, K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            leafNode = await task;
            await AfterSeekTask(key, searchComparer);
        }

        private ValueTask AfterSeekTask(in K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            Debug.Assert(leafNode != null);
            var i = searchComparer.FindIndex(key, leafNode.keys);

            if (i == -1)
            {
                if (leafNode.previous == 0)
                {
                    leafNode.Return();
                    leafNode = null;
                }
            }
            else
            {
                if (i < 0)
                {
                    i = ~i;
                }
                if (i >= leafNode.keys.Count)
                {
                    i = leafNode.keys.Count - 1;
                }
                index = i;
            }
            
            enumerator.Reset(leafNode, index);
            return ValueTask.CompletedTask;
        }

        private async ValueTask AfterSeekTask_Slow(ValueTask<IBPlusTreeNode?> nextPageTask, K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            var nextPage = await nextPageTask;
            leafNode = (nextPage as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            await AfterSeekTask(key, searchComparer);
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
            if (index < 0 && leafNode.previous == 0)
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
            enumerator.Reset(default, 0);
            leafNode = null;
        }

        public void CloneSeekResultTo(IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> other)
        {
            if (other is BPlusTreeBackwardIterator<K, V, TKeyContainer, TValueContainer> otherIterator)
            {
                otherIterator.leafNode = this.leafNode;
                otherIterator.index = this.index;
                otherIterator.enumerator.Reset(this.leafNode, this.index);
                return;
            }
            throw new NotImplementedException();
        }
    }
}
