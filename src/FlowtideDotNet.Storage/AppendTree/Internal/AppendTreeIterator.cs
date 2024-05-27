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
using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    internal class AppendTreeIterator<K, V> : IAsyncEnumerable<KeyValuePair<K, V>>
    {
        private sealed class Enumerator : IAsyncEnumerator<KeyValuePair<K, V>>
        {
            private readonly AppendTree<K, V> _tree;
            private readonly LeafNode<K, V> _node;
            private int _index;

            public Enumerator(AppendTree<K, V> tree, LeafNode<K, V> node, int index)
            {
                _tree = tree;
                _node = node;
                _index = index;
            }
            public KeyValuePair<K, V> Current => throw new NotImplementedException();

            public ValueTask DisposeAsync()
            {
                throw new NotImplementedException();
            }

            public ValueTask<bool> MoveNextAsync()
            {
                throw new NotImplementedException();
            }
        }

        public IAsyncEnumerator<KeyValuePair<K, V>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
        //internal class Enumerator : IAsyncEnumerator<IBPlusTreePageIterator<K, V>>
        //{
        //    private readonly AppendTree<K, V> tree;
        //    private LeafNode<K, V>? leafNode;
        //    private int index;
        //    private bool started;

        //    public Enumerator(in AppendTree<K, V> tree)
        //    {
        //        this.tree = tree;
        //    }

        //    public void Reset(LeafNode<K, V>? leafNode, int index)
        //    {
        //        this.leafNode = leafNode;
        //        this.index = index;
        //        this.started = false;
        //    }

        //    public IBPlusTreePageIterator<K, V> Current => new BPlusTreePageIterator<K, V>(leafNode!, index, tree);

        //    public ValueTask DisposeAsync()
        //    {
        //        return ValueTask.CompletedTask;
        //    }

        //    public ValueTask<bool> MoveNextAsync()
        //    {
        //        if (leafNode == null)
        //        {
        //            return ValueTask.FromResult(false);
        //        }
        //        if (!started)
        //        {
        //            started = true;
        //            return ValueTask.FromResult(true);
        //        }
        //        if (leafNode.next == 0)
        //        {
        //            return ValueTask.FromResult(false);
        //        }
        //        var getNextPageTask = tree.m_stateClient.GetValue(leafNode.next, "MoveNextAsync2");

        //        if (!getNextPageTask.IsCompleted)
        //        {
        //            return MoveNextAsync_Slow(getNextPageTask);
        //        }
        //        leafNode = (getNextPageTask.Result as LeafNode<K, V>)!;
        //        index = 0;
        //        return ValueTask.FromResult(true);
        //    }

        //    private async ValueTask<bool> MoveNextAsync_Slow(ValueTask<IBPlusTreeNode?> getPageTask)
        //    {
        //        var page = await getPageTask;
        //        leafNode = (page as LeafNode<K, V>)!;
        //        index = 0;
        //        return true;
        //    }
        //}

        //private LeafNode<K, V>? leafNode;
        //private readonly AppendTree<K, V> _tree;
        //private int index;
        //private readonly Enumerator enumerator;

        //public AppendTreeIterator(AppendTree<K, V> tree)
        //{
        //    this._tree = tree;
        //    enumerator = new Enumerator(tree);
        //}
        //public IAsyncEnumerator<IBPlusTreePageIterator<K, V>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        //{
        //    return enumerator;
        //}

        //public ValueTask Seek(in K key, in IComparer<K>? searchComparer = null)
        //{
        //    var comparer = searchComparer == null ? _tree.m_keyComparer : searchComparer;            
        //    var searchTask = _tree.FindLeafNode(key, comparer);
        //    if (!searchTask.IsCompleted)
        //    {
        //        return Seek_Slow(searchTask, key, comparer);
        //    }
        //    leafNode = searchTask.Result;
        //    AfterSeek(key, comparer);
        //    return ValueTask.CompletedTask;
        //}

        //private async ValueTask Seek_Slow(ValueTask<LeafNode<K, V>> task, K key, IComparer<K> searchComparer)
        //{
        //    leafNode = await task;
        //    AfterSeek(key, searchComparer);
        //}

        //private void AfterSeek(in K key, IComparer<K> searchComparer)
        //{
        //    Debug.Assert(leafNode != null);
        //    var i = leafNode.keys.BinarySearch(key, searchComparer);
        //    if (i < 0)
        //    {
        //        i = ~i;
        //    }
        //    index = i;
        //    if (index >= leafNode.keys.Count && leafNode.next == 0)
        //    {
        //        leafNode = null;
        //    }
        //    enumerator.Reset(leafNode, index);
        //}

        //public ValueTask SeekFirst()
        //{
        //    throw new NotImplementedException();
        //}

    }
}
