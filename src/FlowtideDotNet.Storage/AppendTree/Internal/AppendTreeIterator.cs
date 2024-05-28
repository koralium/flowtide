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
    internal class AppendTreeIterator<K, V> : IAppendTreeIterator<K, V>
    {
        private sealed class Enumerator : IAsyncEnumerator<KeyValuePair<K, V>>
        {
            private readonly AppendTree<K, V> _tree;
            private LeafNode<K, V> _node;
            private int _index;
            private bool _started;

            public Enumerator(AppendTree<K, V> tree, LeafNode<K, V> node, int index)
            {
                _tree = tree;
                _node = node;
                _index = index;
                _started = false;
            }
            public KeyValuePair<K, V> Current => GetCurrent();

            private KeyValuePair<K, V> GetCurrent()
            {
                return new KeyValuePair<K, V>(_node.keys[_index], _node.values[_index]);
            }

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (!_started)
                {
                    _started = true;
                }
                else
                {
                    _index++;
                }
                if (_node.keys.Count <= _index && _node.next == 0)
                {
                    return ValueTask.FromResult(false);
                }
                else if (_node.next != 0 && _node.keys.Count == _index)
                {
                    return FetchNewPage();
                }
                return ValueTask.FromResult(true);
            }

            private async ValueTask<bool> FetchNewPage()
            {
                _node = ((await _tree.GetChildNode(_node.next)) as LeafNode<K, V>)!;
                _index = 0;
                if (_node.keys.Count == 0)
                {
                    return false;
                }
                return true;
            }
        }

        private readonly AppendTree<K, V> _tree;
        private LeafNode<K, V>? _node;
        private int? _index;

        public AppendTreeIterator(AppendTree<K, V> tree)
        {
            _tree = tree;
        }

        public IAsyncEnumerator<KeyValuePair<K, V>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (_node == null || _index == null)
            {
                throw new NotSupportedException("You must call seek before iterating");
            }
            return new Enumerator(_tree, _node, _index.Value);
        }

        public ValueTask Seek(in K key, in IComparer<K>? searchComparer = null)
        {
            // Do async seek directly right now
            return Seek_Slow(key, searchComparer);
        }

        private async ValueTask Seek_Slow(K key, IComparer<K>? searchComparer)
        {
            var comparer = searchComparer == null ? _tree.m_keyComparer : searchComparer;
            _node = await _tree.FindLeafNode(key, comparer);
            var i = _node.keys.BinarySearch(key, searchComparer);
            if (i < 0)
            {
                i = ~i;
            }
            _index = i;
        }
    }
}
