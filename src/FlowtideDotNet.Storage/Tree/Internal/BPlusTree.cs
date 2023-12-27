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

using FlowtideDotNet.Storage.StateManager.Internal;
using System.Diagnostics;
using System.Text;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal partial class BPlusTree<K, V> : IBPlusTree<K, V>
    {
        internal readonly IStateClient<IBPlusTreeNode, BPlusTreeMetadata> m_stateClient;
        private readonly BPlusTreeOptions<K, V> m_options;
        internal IComparer<K> m_keyComparer;
        private int minSize;

        public BPlusTree(IStateClient<IBPlusTreeNode, BPlusTreeMetadata> stateClient, BPlusTreeOptions<K, V> options) 
        {
            this.m_stateClient = stateClient;
            this.m_options = options;
            minSize = options.BucketSize / 3;
            this.m_keyComparer = options.Comparer;
        }

        public Task InitializeAsync()
        {
            if (m_stateClient.Metadata == null)
            {
                var rootId = m_stateClient.GetNewPageId();
                var root = new LeafNode<K, V>(rootId);
                m_stateClient.Metadata = new BPlusTreeMetadata()
                {
                    Root = rootId,
                    BucketLength = m_options.BucketSize,
                    Left = rootId
                };
                m_stateClient.AddOrUpdate(rootId, root);
            }
            return Task.CompletedTask;
        }

        public async Task<string> Print()
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var root = (BaseNode<K>)(await m_stateClient.GetValue(m_stateClient.Metadata.Root, "PrintRoot"))!;

            var builder = new StringBuilder();
            builder.AppendLine("digraph g {");
            builder.AppendLine("splines=line");
            builder.AppendLine("node [shape = none,height=.1];");
            await root.Print(builder, async (id) => (BaseNode<K>)(await m_stateClient.GetValue(id, "GetPrint"))!);
            builder.AppendLine("}");
            return builder.ToString();
        }

        public ValueTask Upsert(in K key, in V value)
        {
            var writeTask = GenericWrite(key, value, (input, current, found) =>
            {
                return (input, GenericWriteOperation.Upsert);
            });

            if (!writeTask.IsCompletedSuccessfully)
            {
                return Upsert_Slow(writeTask);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask Upsert_Slow(ValueTask<GenericWriteOperation> writeOperation)
        {
            await writeOperation;
        }

        public ValueTask Commit()
        {
            return m_stateClient.Commit();
        }

        public ValueTask Delete(in K key)
        {
            var deleteTask = GenericWrite(key, default, (input, current, found) =>
            {
                if (found)
                {
                    return (default, GenericWriteOperation.Delete);
                }
                return (default, GenericWriteOperation.None);
            });

            if (!deleteTask.IsCompletedSuccessfully)
            {
                return Delete_Slow(deleteTask);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask Delete_Slow(ValueTask<GenericWriteOperation> writeOperation)
        {
            await writeOperation;
        }

        public ValueTask<(bool found, V? value)> GetValue(in K key)
        {
            return GetValue_Internal(key);
        }

        private async ValueTask<(bool found, V? value)> GetValue_Internal(K key)
        {
            var leaf = await SearchRoot(key, m_keyComparer);
            var index = leaf.keys.BinarySearch(key, m_keyComparer);
            if (index < 0)
            {
                return (false, default);
            }
            return (true, leaf.values[index]);
        }

        public IBPlusTreeIterator<K, V> CreateIterator()
        {
            return new BPlusTreeIterator<K, V>(this);
        }

        public ValueTask<(GenericWriteOperation operation, V? result)> RMW(in K key, in V? value, in GenericWriteFunction<V> function)
        {
            return RMW_Slow(key, value, function);
        }

        private sealed class RMWContainer
        {
            public V? Value { get; set; }
        }

        private async ValueTask<(GenericWriteOperation operation, V? result)> RMW_Slow(K key, V? value, GenericWriteFunction<V> function)
        {
            var func = function;
            var container = new RMWContainer();
            var operation = await GenericWrite(key, value, (input, current, found) =>
            {
                var (result, op) = func(input, current, found);
                container.Value = result;
                return (result, op);
            });

            return (operation, container.Value);
        }

        public async ValueTask Clear()
        {
            // Clear the current state from the state storage
            await m_stateClient.Reset(true);

            // Create a new root leaf
            var rootId = m_stateClient.GetNewPageId();
            var root = new LeafNode<K, V>(rootId);
            m_stateClient.Metadata = new BPlusTreeMetadata()
            {
                Root = rootId,
                BucketLength = m_options.BucketSize,
                Left = rootId
            };
            m_stateClient.AddOrUpdate(rootId, root);
        }
    }
}
