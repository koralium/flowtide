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
    internal partial class BPlusTree<K, V, TKeyContainer, TValueContainer> : IBPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        internal readonly IStateClient<IBPlusTreeNode, BPlusTreeMetadata> m_stateClient;
        private readonly BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> m_options;
        internal IBplusTreeComparer<K, TKeyContainer> m_keyComparer;
        private int minSize;
        private bool m_isByteBased;
        private int byteMinSize;
        private bool m_usePreviousPointer;

        public BPlusTree(IStateClient<IBPlusTreeNode, BPlusTreeMetadata> stateClient, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
        {
            Debug.Assert(options.BucketSize.HasValue);
            Debug.Assert(options.PageSizeBytes.HasValue);
            this.m_stateClient = stateClient;
            this.m_options = options;
            minSize = options.BucketSize.Value / 3;
            this.m_keyComparer = options.Comparer;
            m_isByteBased = options.UseByteBasedPageSizes;
            byteMinSize = (options.PageSizeBytes.Value) / 3;
            m_usePreviousPointer = options.UsePreviousPointers;
        }

        public long CacheMisses => m_stateClient.CacheMisses;

        public Task InitializeAsync()
        {
            Debug.Assert(m_options.BucketSize.HasValue);
            Debug.Assert(m_options.PageSizeBytes.HasValue);
            if (m_stateClient.Metadata == null)
            {
                var rootId = m_stateClient.GetNewPageId();

                var emptyKeyContainer = m_options.KeySerializer.CreateEmpty();
                var emptyValueContainer = m_options.ValueSerializer.CreateEmpty();
                var root = new LeafNode<K, V, TKeyContainer, TValueContainer>(rootId, emptyKeyContainer, emptyValueContainer);
                m_stateClient.Metadata = BPlusTreeMetadata.Create(m_options.BucketSize.Value, rootId, rootId, m_options.PageSizeBytes.Value, new List<long>(), new List<long>());
                m_stateClient.AddOrUpdate(rootId, root);
            }
            return Task.CompletedTask;
        }

        public async Task<string> Print()
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var root = (BaseNode<K, TKeyContainer>)(await m_stateClient.GetValue(m_stateClient.Metadata.Root))!;

            var builder = new StringBuilder();
            builder.AppendLine("digraph g {");
            builder.AppendLine("splines=line");
            builder.AppendLine("node [shape = none,height=.1];");
            await root.Print(builder, async (id) => (BaseNode<K, TKeyContainer>)(await m_stateClient.GetValue(id))!);
            builder.AppendLine("}");
            return builder.ToString();
        }

        public ValueTask Upsert(in K key, in V value)
        {
            var writeTask = GenericWrite(in key, in value, (input, current, found) =>
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
            var deleteTask = GenericWrite(in key, default, (input, current, found) =>
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
            var index = m_keyComparer.FindIndex(key, leaf.keys);
            if (index < 0)
            {
                return (false, default);
            }
            return (true, leaf.values.Get(index));
        }

        public ValueTask<(bool found, K? key)> GetKey(in K key)
        {
            return GetKey_Internal(key);
        }

        private async ValueTask<(bool found, K? key)> GetKey_Internal(K key)
        {
            var leaf = await SearchRoot(key, m_keyComparer);
            var index = m_keyComparer.FindIndex(key, leaf.keys);
            if (index < 0)
            {
                return (false, default);
            }
            return (true, leaf.keys.Get(index));
        }

        public IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> CreateIterator()
        {
            return new BPlusTreeIterator<K, V, TKeyContainer, TValueContainer>(this);
        }

        public ValueTask<GenericWriteOperation> RMWNoResult(in K key, in V? value, in GenericWriteFunction<V> function)
        {
            return GenericWrite(in key, in value, in function);
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
            var operation = await GenericWrite(in key, value, (input, current, found) =>
            {
                var (result, op) = func(input, current, found);
                container.Value = result;
                return (result, op);
            });

            return (operation, container.Value);
        }

        public async ValueTask Clear()
        {
            Debug.Assert(m_options.BucketSize.HasValue);
            Debug.Assert(m_options.PageSizeBytes.HasValue);
            // Clear the current state from the state storage
            await m_stateClient.Reset(true);

            // Create a new root leaf
            var rootId = m_stateClient.GetNewPageId();
            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var emptyValues = m_options.ValueSerializer.CreateEmpty();
            var root = new LeafNode<K, V, TKeyContainer, TValueContainer>(rootId, emptyKeys, emptyValues);
            m_stateClient.Metadata = BPlusTreeMetadata.Create(m_options.BucketSize.Value, rootId, rootId, m_options.PageSizeBytes.Value, new List<long>(), new List<long>());
            m_stateClient.AddOrUpdate(rootId, root);
        }
    }
}
