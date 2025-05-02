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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeUpdater<K, V, TKeyContainer, TValueContainer> : IBplusTreeUpdater<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        private LeafNode<K, V, TKeyContainer, TValueContainer>? _leafNode;
        private int _index;
        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> _tree;
        public BPlusTreeUpdater(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            _tree = tree;
        }

        public LeafNode<K, V, TKeyContainer, TValueContainer> CurrentPage => _leafNode ?? throw new Exception();

        public int CurrentIndex => _index;

        public bool Found { get; private set; }

        public ValueTask Seek(K key, IBplusTreeComparer<K, TKeyContainer>? searchComparer = null)
        {
            var comparer = searchComparer == null ? _tree.m_keyComparer : searchComparer;

            if (_leafNode != null)
            {
                var result = comparer.FindIndex(key, _leafNode.keys);
                if (result < 0)
                {
                    result = ~result;
                    Found = false;
                }
                if (result > 0 && (_leafNode.keys.Count > result || _leafNode.next == 0))
                {
                    _index = result;
                    Found = true;
                    return ValueTask.CompletedTask;
                }
                else
                {
                    _leafNode.Return();
                    _leafNode = null;
                }
            }

            var searchTask = _tree.SearchRoot(key, comparer);

            if (!searchTask.IsCompleted)
            {
                return Seek_Slow(searchTask, key, comparer);
            }

            _leafNode = searchTask.Result;
            return AfterSeekTask(key, comparer);
        }

        private async ValueTask Seek_Slow(ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> task, K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            _leafNode = await task;
            await AfterSeekTask(key, searchComparer);
        }

        private ValueTask AfterSeekTask(in K key, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            Debug.Assert(_leafNode != null);
            var i = searchComparer.FindIndex(key, _leafNode.keys);
            if (i < 0)
            {
                i = ~i;
                Found = false;
            }
            else
            {
                Found = true;
            }

            _index = i;
            return ValueTask.CompletedTask;
        }

        public ValueTask SavePage()
        {
            Debug.Assert(_leafNode != null);
            var byteSize = _leafNode.GetByteSize();
            if (_leafNode.keys.Count > 0 && _tree.m_stateClient.Metadata!.PageSizeBytes < byteSize)
            {
                _tree.m_stateClient.AddOrUpdate(_leafNode.Id, _leafNode);
                // Force a traversion of the tree to ensure that size is looked at for splits.
                var traverseTask = _tree.RMWNoResult(_leafNode.keys.Get(0), default, (input, current, found) =>
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
                var isFull = _tree.m_stateClient.AddOrUpdate(_leafNode.Id, _leafNode);
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
            await _tree.m_stateClient.WaitForNotFullAsync();
        }
    }
}
