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

using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal partial class BPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        public ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> LeftLeaf()
        {
            Debug.Assert(m_stateClient.Metadata != null);

            var getFirstTask = m_stateClient.GetValue(m_stateClient.Metadata.Left);

            if (!getFirstTask.IsCompleted)
            {
                return LeftLeaf_Slow(getFirstTask);
            }
            return ValueTask.FromResult((getFirstTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!);
        }

        private async ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> LeftLeaf_Slow(ValueTask<IBPlusTreeNode?> getNodeTask)
        {
            var node = await getNodeTask;
            return (node as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
        }


        public ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchRoot(in K key, in IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            var rootTask = m_stateClient.GetValue(m_stateClient.Metadata.Root);

            if (!rootTask.IsCompletedSuccessfully)
            {
                return SearchRoot_Slow(key, rootTask, searchComparer);
            }
            return SearchLeafNodeForRead_AfterTask(key, rootTask.Result!, searchComparer);
        }

        private async ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchRoot_Slow(K key, ValueTask<IBPlusTreeNode?> task, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            var root = await task;
            return await SearchLeafNodeForRead_AfterTask(key, root!, searchComparer);
        }

        private ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchLeafNodeForRead_AfterTask(in K key, in IBPlusTreeNode node, in IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            if (node is LeafNode<K, V, TKeyContainer, TValueContainer> leaf)
            {
                return new ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>>(leaf);
            }
            else if (node is InternalNode<K, V, TKeyContainer> parentNode)
            {
                return SearchLeafNodeForReadInternal(key, parentNode, searchComparer);
            }
            throw new NotImplementedException();
        }

        private ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchLeafNodeForReadInternal(K key, InternalNode<K, V, TKeyContainer> node, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            var index = searchComparer.FindIndex(key, node.keys);
            if (index < 0)
            {
                index = ~index;
            }
            var child = node.children[index];
            node.Return();
            var childNodeTask = m_stateClient.GetValue(child);

            if (!childNodeTask.IsCompletedSuccessfully)
            {
                return SearchLeafNodeForReadInternal_Slow(key, childNodeTask, searchComparer);
            }
            return SearchLeafNodeForRead_AfterTask(key, childNodeTask.Result!, searchComparer);
        }

        private async ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchLeafNodeForReadInternal_Slow(K key, ValueTask<IBPlusTreeNode?> task, IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            var childNode = await task;
            return await SearchLeafNodeForRead_AfterTask(key, childNode!, searchComparer);
        }

        public readonly struct LeafBatchMapping
        {
            public readonly long LeafId;
            public readonly int Offset;
            public readonly int Length;
            public readonly long ParentId;

            public LeafBatchMapping(long leafId, int offset, int length, long parentId)
            {
                LeafId = leafId;
                Offset = offset;
                Length = length;
                ParentId = parentId;
            }
        }

        internal async ValueTask RouteBatchRootAsync(
            K[] keys, 
            int batchLength,
            int[] sortedIndices, 
            IBplusTreeComparer<K, TKeyContainer> searchComparer,
            List<LeafBatchMapping> mappings)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            int depth = m_stateClient.Metadata.Depth;

            int safeArenaSize = depth == 0 ? 0 : depth * batchLength;
            var workspace = depth > 0 ? ArrayPool<(long, int, int)>.Shared.Rent(safeArenaSize) : null;

            try
            {
                if (depth == 0)
                {
                    // If depth 0 it is just a leaf as root
                    mappings.Add(new LeafBatchMapping(m_stateClient.Metadata.Root, 0, batchLength, -1));
                }
                else
                {
                    var rootTask = m_stateClient.GetValue(m_stateClient.Metadata.Root);
                    var root = (rootTask.IsCompletedSuccessfully ? rootTask.Result : await rootTask)
                                as InternalNode<K, V, TKeyContainer>;

                    await RouteBatchInternalAsync(
                        keys, sortedIndices, 0, batchLength,
                        root!, depth, searchComparer, mappings, workspace!, 0);
                }
            }
            finally
            {
                if (workspace != null)
                {
                    ArrayPool<(long, int, int)>.Shared.Return(workspace);
                }
            }
        }

        private async ValueTask RouteBatchInternalAsync(
            K[] keys,
            int[] sortedIndices,
            int offset,
            int length,
            InternalNode<K, V, TKeyContainer> node,
            int currentDepth,
            IBplusTreeComparer<K, TKeyContainer> searchComparer,
            List<LeafBatchMapping> mappings,
            (long pointer, int offset, int length)[] workspace,
            int workspaceStartIndex)
        {
            int sliceCount = 0;
            int currentOffset = offset;
            int end = offset + length;

            while (currentOffset < end)
            {
                int firstKeyIndex = sortedIndices[currentOffset];
                int targetChildIndex = searchComparer.FindIndex(keys[firstKeyIndex], node.keys);
                if (targetChildIndex < 0) targetChildIndex = ~targetChildIndex;

                int itemsForThisChild;

                if (targetChildIndex == node.keys.Count)
                {
                    itemsForThisChild = end - currentOffset;
                }
                else
                {
                    // Normal look-ahead for non-last children
                    itemsForThisChild = 1;
                    while (currentOffset + itemsForThisChild < end)
                    {
                        int nextKeyIndex = sortedIndices[currentOffset + itemsForThisChild];
                        int nextChildIndex = searchComparer.FindIndex(keys[nextKeyIndex], node.keys);
                        if (nextChildIndex < 0) nextChildIndex = ~nextChildIndex;

                        if (nextChildIndex == targetChildIndex) itemsForThisChild++;
                        else break;
                    }
                }

                workspace[workspaceStartIndex + sliceCount] =
                    (node.children[targetChildIndex], currentOffset, itemsForThisChild);

                sliceCount++;
                currentOffset += itemsForThisChild;
            }

            node.Return();

            for (int i = 0; i < sliceCount; i++)
            {
                var slice = workspace[workspaceStartIndex + i];

                if (currentDepth == 1)
                {
                    // At leaf id
                    mappings.Add(new LeafBatchMapping(slice.pointer, slice.offset, slice.length, node.Id));
                }
                else
                {
                    var childNodeTask = m_stateClient.GetValue(slice.pointer);
                    var rawChildNode = childNodeTask.IsCompletedSuccessfully
                        ? childNodeTask.Result!
                        : (await childNodeTask)!;

                    var internalChild = (InternalNode<K, V, TKeyContainer>)rawChildNode;

                    await RouteBatchInternalAsync(
                        keys, sortedIndices, slice.offset, slice.length,
                        internalChild, currentDepth - 1, searchComparer,
                        mappings, workspace, workspaceStartIndex + sliceCount);
                }
            }
        }
    }
}
