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
using System.Reflection;
using System.Runtime.CompilerServices;
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
        private bool _writtenAtPage = false;
        private List<BPlusTreeNodeIndex> _nodePath;

        public BPlusTreeUpdater(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            _tree = tree;
            _nodePath = new List<BPlusTreeNodeIndex>();
        }

        private LeafNode<K, V, TKeyContainer, TValueContainer> CurrentPage => _leafNode ?? throw new Exception();

        public int CurrentIndex => _index;

        public bool Found { get; private set; }

        public ValueTask Seek(in K key, IBplusTreeComparer<K, TKeyContainer>? searchComparer = null)
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
                else
                {
                    Found = true;
                }
                if (result > 0 && (_leafNode.keys.Count > result || _leafNode.next == 0))
                {
                    _index = result;
                    return ValueTask.CompletedTask;
                }
                else
                {
                    if (_writtenAtPage)
                    {
                        var savePageTask = SavePage();
                        if (!savePageTask.IsCompleted)
                        {
                            return Seek_SavePageSlow(savePageTask, key, comparer);
                        }
                    }

                    _writtenAtPage = false;
                    _leafNode.Return();
                    _leafNode = null;
                }
            }

            _nodePath.Clear();
            var searchTask = _tree.SearchRootIterative(key, comparer, _nodePath);

            if (!searchTask.IsCompleted)
            {
                return Seek_Slow(searchTask, key, comparer);
            }

            _leafNode = searchTask.Result;
            return AfterSeekTask(key, comparer);
        }

        private async ValueTask Seek_SavePageSlow(ValueTask savePageTask, K key, IBplusTreeComparer<K, TKeyContainer> comparer)
        {
            await savePageTask;

            _writtenAtPage = false;
            _leafNode!.Return();
            _leafNode = null;

            _nodePath.Clear();
            _leafNode = await _tree.SearchRootIterative(key, comparer, _nodePath);
            await AfterSeekTask(key, comparer);
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
            //var byteSize = _leafNode.GetByteSize();
            _writtenAtPage = false;
            return _tree.SavePage(_leafNode, _nodePath);
        }

        public K GetKey()
        {
            return _leafNode!.keys.Get(_index);
        }

        public V GetValue()
        {
            return _leafNode!.values.Get(_index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask Upsert(in K key, in V value)
        {
            CurrentPage.EnterWriteLock();
            if (Found)
            {
                CurrentPage!.values.Update(_index, value);
            }
            else
            {
                CurrentPage.keys.Insert(_index, key);
                CurrentPage.values.Insert(_index, value);
            }
            CurrentPage.ExitWriteLock();
            _writtenAtPage = true;

            var byteSize = CurrentPage.GetByteSize();

            if (CurrentPage.keys.Count > 0 && _tree.m_stateClient.Metadata!.PageSizeBytes < byteSize)
            {
                return SavePage();
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask Delete()
        {
            if (!Found)
            {
                throw new InvalidOperationException("Key not found in the current page.");
            }
            _leafNode!.DeleteAt(_index);
            _writtenAtPage = true;

            var byteSize = CurrentPage.GetByteSize();

            if ((byteSize <= _tree.byteMinSize || _leafNode.keys.Count < BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSize))
            {
                return SavePage();
            }

            return ValueTask.CompletedTask;
        }

        public async ValueTask Commit()
        {
            if (_leafNode != null && _writtenAtPage)
            {
                await SavePage();
            }
            _leafNode = null;
            _writtenAtPage = false;
        }

        public void ClearCache()
        {
            if (_leafNode != null)
            {
                _leafNode.Return();
            }
            _leafNode = null;
            _writtenAtPage = false;
            _index = -1;
        }
    }
}
