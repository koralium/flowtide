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

using FlowtideDotNet.Storage.DataStructures;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    /// <summary>
    ///  Contains the code for generic write when it splits and merges based on byte size.
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    /// <typeparam name="TKeyContainer"></typeparam>
    /// <typeparam name="TValueContainer"></typeparam>
    internal partial class BPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        // These constants exist only to have a minimum size so there will be a sufficent fanout.
        // This must be atleast 2x larger than minpagesize
        private const int minPageSizeBeforeSplit = 16;
        private const int minPageSize = 4;
        private const int minPageSizeAfterSplit = 8;

        public ValueTask<GenericWriteOperation> GenericWriteByteBased(ref readonly K key, ref readonly V? value, ref readonly GenericWriteFunction<V> function)
        {
            return GenericWriteRootByteBased(in key, in value, in function);
        }

        /// <summary>
        /// Does a read modify write which takes a function that is called.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="function"></param>
        /// <returns></returns>
        private ValueTask<GenericWriteOperation> GenericWriteRootByteBased(
            ref readonly K key,
            ref readonly V? value,
            ref readonly GenericWriteFunction<V> function)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            var rootNodeTask = m_stateClient.GetValue(m_stateClient.Metadata.Root);

            if (!rootNodeTask.IsCompletedSuccessfully)
            {
                return GenericWriteRoot_SlowGetRootByteBased(key, value, function, rootNodeTask);
            }
            var rootNode = rootNodeTask.Result;
            return GenericWriteRoot_AfterGetRootByteBased(in rootNode, in key, in value, in function);
        }

        private async ValueTask<GenericWriteOperation> GenericWriteRoot_SlowGetRootByteBased(
            K key,
            V? value,
            GenericWriteFunction<V> function,
            ValueTask<IBPlusTreeNode?> getRootNodeTask)
        {
            var rootNode = await getRootNodeTask.ConfigureAwait(false);
            Debug.Assert(rootNode != null, nameof(rootNode));
            return await GenericWriteRoot_AfterGetRootByteBased(in rootNode, in key, in value, in function).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWriteRoot_AfterGetRootByteBased(
            ref readonly IBPlusTreeNode? rootNode,
            ref readonly K key,
            ref readonly V? value,
            ref readonly GenericWriteFunction<V> function)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            if (rootNode is LeafNode<K, V, TKeyContainer, TValueContainer> leafNode)
            {
                var result = GenericWrite_Leaf(in leafNode, in key, in value, in function);

                var byteSize = leafNode.GetByteSize();
                // No need to check for merging at root leaf, only check for split
                if (byteSize > m_stateClient.Metadata.PageSizeBytes && leafNode.keys.Count > minPageSizeBeforeSplit)
                {
                    var nextId = m_stateClient.GetNewPageId();
                    var emptyKeys = m_options.KeySerializer.CreateEmpty();
                    var newParentNode = new InternalNode<K, V, TKeyContainer>(nextId, emptyKeys, m_options.MemoryAllocator);

                    // No lock required
                    newParentNode.children.InsertAt(0, leafNode.Id);
                    m_stateClient.Metadata = m_stateClient.Metadata.UpdateRoot(nextId);
                    LeafNode<K, V, TKeyContainer, TValueContainer> newNode;

                    (newNode, _) = SplitLeafNodeBasedOnBytes(in newParentNode, 0, in leafNode, in byteSize);

                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newParentNode.Id, newParentNode);
                    isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);

                    if (isFull)
                    {
                        rootNode.Return();
                        return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                    }
                }
                else
                {
                    // Only update node if there is a change
                    if (result != GenericWriteOperation.None)
                    {
                        var isFull = m_stateClient.AddOrUpdate(leafNode.Id, leafNode);

                        if (isFull)
                        {
                            rootNode.Return();
                            return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                        }
                    }
                }
                rootNode.Return();
                return ValueTask.FromResult(result);
            }
            else if (rootNode is InternalNode<K, V, TKeyContainer> internalNode)
            {
                var resultTask = GenericWrite_InternalByteBased(in internalNode, in key, in value, in function);
                if (!resultTask.IsCompletedSuccessfully)
                {
                    return GenericWriteRoot_SlowInternalByteBased(resultTask, internalNode);
                }
                var result = resultTask.Result;
                return GenericWriteRoot_AfterInternalByteBased(in result, in internalNode);
            }
            throw new NotImplementedException();
        }

        private ValueTask<GenericWriteOperation> GenericWrite_InternalByteBased(
            ref readonly InternalNode<K, V, TKeyContainer> parentNode,
            ref readonly K key,
            ref readonly V? value,
            ref readonly GenericWriteFunction<V> function
            )
        {
            var index = m_keyComparer.FindIndex(in key, in parentNode.keys);

            if (index < 0)
            {
                index = ~index;
            }
            var childId = parentNode.children[index];
            var getChildTask = m_stateClient.GetValue(childId);

            if (!getChildTask.IsCompletedSuccessfully)
            {
                return GenericWrite_Internal_SlowGetNodeByteBased(index, getChildTask, parentNode, key, value, function);
            }
            var child = getChildTask.Result;
            return GenericWrite_Internal_AfterGetNodeByteBased(in index, in child, in parentNode, in key, in value, in function);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_SlowGetNodeByteBased(
            int index,
            ValueTask<IBPlusTreeNode?> getChildTask,
            InternalNode<K, V, TKeyContainer> parentNode,
            K key,
            V? value,
            GenericWriteFunction<V> function
            )
        {
            var child = await getChildTask.ConfigureAwait(false);
            return await GenericWrite_Internal_AfterGetNodeByteBased(in index, in child, in parentNode, in key, in value, in function).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNodeByteBased(
            in int index,
            in IBPlusTreeNode? child,
            in InternalNode<K, V, TKeyContainer> parentNode,
            in K key,
            in V? value,
            in GenericWriteFunction<V> function
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);

            if (child is LeafNode<K, V, TKeyContainer, TValueContainer> leafNode)
            {
                var result = GenericWrite_Leaf(in leafNode, in key, in value, in function);

                var byteSize = leafNode.GetByteSize();
                // Check if split is required
                if (byteSize > m_stateClient.Metadata.PageSizeBytes && leafNode.keys.Count >= minPageSizeBeforeSplit)
                {
                    LeafNode<K, V, TKeyContainer, TValueContainer> newNode;

                    (newNode, _) = SplitLeafNodeBasedOnBytes(in parentNode, in index, in leafNode, in byteSize);

                    // Save all the nodes that was changed
                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                    isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);

                    if (isFull)
                    {
                        leafNode.Return();
                        return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                    }
                }
                // Check if the node is too small
                else if (byteSize <= byteMinSize || leafNode.keys.Count < minPageSize)
                {
                    if (index == parentNode.keys.Count)
                    {
                        // Merge with left neighbor
                        var leftNeighborId = parentNode.children[index - 1];
                        var getLeftNodeTask = m_stateClient.GetValue(leftNeighborId);
                        if (!getLeftNodeTask.IsCompletedSuccessfully)
                        {
                            return GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_SlowGetLeftByteBased(getLeftNodeTask, leafNode, parentNode, index, result);
                        }
                        var leftNode = (getLeftNodeTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;

                        return GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeftByteBased(in leftNode, in leafNode, in parentNode, in index, in result);
                    }
                    else
                    {
                        // Merge with right neighbor
                        var rightNeighborId = parentNode.children[index + 1];
                        var getRightNodeTask = m_stateClient.GetValue(rightNeighborId);
                        if (!getRightNodeTask.IsCompletedSuccessfully)
                        {
                            return GenericWrite_Internal_AfterGetNode_NodeTooSmall_SlowGetRightByteBased(getRightNodeTask, leafNode, parentNode, index, result);
                        }
                        var rightNode = (getRightNodeTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;

                        return GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRightByteBased(in rightNode, in leafNode, in parentNode, in index, in result);
                    }
                }
                else
                {
                    var isFull = m_stateClient.AddOrUpdate(leafNode.Id, leafNode);

                    if (isFull)
                    {
                        leafNode.Return();
                        return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                    }
                }
                leafNode.Return();
                return ValueTask.FromResult(result);
            }
            else if (child is InternalNode<K, V, TKeyContainer> internalNode)
            {
                var rmwTask = GenericWrite_InternalByteBased(in internalNode, in key, in value, in function);
                if (!rmwTask.IsCompletedSuccessfully)
                {
                    return GenericWrite_Internal_AfterGetNode_SlowCallInternalByteBased(rmwTask, internalNode, parentNode, index);
                }
                var result = rmwTask.Result;
                return GenericWrite_Internal_AfterGetNode_AfterCallInternalByteBased(in internalNode, in parentNode, in index, in result);
            }
            throw new NotImplementedException();
        }

        private async ValueTask<GenericWriteOperation> GenericWriteRoot_SlowInternalByteBased(
            ValueTask<GenericWriteOperation> resultTask,
            InternalNode<K, V, TKeyContainer> internalNode
            )
        {
            var result = await resultTask.ConfigureAwait(false);
            return await GenericWriteRoot_AfterInternalByteBased(in result, in internalNode).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWriteRoot_AfterInternalByteBased(
            ref readonly GenericWriteOperation result,
            ref readonly InternalNode<K, V, TKeyContainer> internalNode
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var byteSize = internalNode.GetByteSize();
            if (byteSize > m_stateClient.Metadata.PageSizeBytes && internalNode.keys.Count > minPageSizeBeforeSplit)
            {
                var nextId = m_stateClient.GetNewPageId();
                var emptyKeys = m_options.KeySerializer.CreateEmpty();
                var newParentNode = new InternalNode<K, V, TKeyContainer>(nextId, emptyKeys, m_options.MemoryAllocator);
                // No lock requireds
                newParentNode.children.InsertAt(0, internalNode.Id);
                m_stateClient.Metadata = m_stateClient.Metadata.UpdateRoot(nextId);

                var (newNode, _) = SplitInternalNodeBasedOnBytes(in newParentNode, 0, in internalNode, in byteSize);

                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                isFull |= m_stateClient.AddOrUpdate(newParentNode.Id, newParentNode);
                isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);

                if (isFull)
                {
                    internalNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            if (internalNode.children.Count == 1)
            {
                m_stateClient.Metadata = m_stateClient.Metadata.UpdateRoot(internalNode.children[0]);
                m_stateClient.Delete(internalNode.Id);
            }
            else
            {
                internalNode.Return();
            }
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_SlowCallInternalByteBased(
            ValueTask<GenericWriteOperation> internalTask,
            InternalNode<K, V, TKeyContainer> internalNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index
            )
        {
            var result = await internalTask.ConfigureAwait(false);
            return await GenericWrite_Internal_AfterGetNode_AfterCallInternalByteBased(in internalNode, in parentNode, in index, in result).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternalByteBased(
            ref readonly InternalNode<K, V, TKeyContainer> internalNode,
            ref readonly InternalNode<K, V, TKeyContainer> parentNode,
            ref readonly int index,
            ref readonly GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            // Check if split is required
            var byteSize = internalNode.GetByteSize();
            if (byteSize > m_stateClient.Metadata.PageSizeBytes && internalNode.keys.Count > minPageSizeBeforeSplit)
            {
                var (newNode, splitKey) = SplitInternalNodeBasedOnBytes(in parentNode, in index, in internalNode, in byteSize);

                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);

                if (isFull)
                {
                    internalNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            // Check if the node is too small
            if (byteSize <= byteMinSize || internalNode.keys.Count < minPageSize)
            {
                if (index == parentNode.keys.Count)
                {
                    var leftNeighborId = parentNode.children[index - 1];
                    var getLeftNodeTask = m_stateClient.GetValue(leftNeighborId);
                    if (!getLeftNodeTask.IsCompletedSuccessfully)
                    {
                        return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetLeftByteBased(getLeftNodeTask, internalNode, parentNode, index, result);
                    }
                    var leftNode = (getLeftNodeTask.Result as InternalNode<K, V, TKeyContainer>)!;
                    return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetLeftByteBased(in leftNode, in internalNode, in parentNode, in index, in result);
                }
                else
                {
                    var rightNeighborId = parentNode.children[index + 1];
                    var getRightNodeTask = m_stateClient.GetValue(rightNeighborId);
                    if (!getRightNodeTask.IsCompletedSuccessfully)
                    {
                        return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetRightByteBased(getRightNodeTask, internalNode, parentNode, index, result);
                    }
                    var rightNode = (getRightNodeTask.Result as InternalNode<K, V, TKeyContainer>)!;

                    return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetRightByteBased(in rightNode, in internalNode, in parentNode, in index, in result);
                }
            }
            internalNode.Return();
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetLeftByteBased(
            ValueTask<IBPlusTreeNode?> getLeftNodeTask,
            InternalNode<K, V, TKeyContainer> internalNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var leftNode = (await getLeftNodeTask.ConfigureAwait(false) as InternalNode<K, V, TKeyContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetLeftByteBased(in leftNode, in internalNode, in parentNode, in index, in result).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetLeftByteBased(
            ref readonly InternalNode<K, V, TKeyContainer> leftNode,
            ref readonly InternalNode<K, V, TKeyContainer> internalNode,
            ref readonly InternalNode<K, V, TKeyContainer> parentNode,
            ref readonly int index,
            ref readonly GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var leftNodeSize = leftNode.GetByteSize();
            var rightNodeSize = internalNode.GetByteSize();
            if (
                ((leftNodeSize >= m_stateClient.Metadata.PageSizeBytes / 2) || (leftNodeSize + rightNodeSize) > m_stateClient.Metadata.PageSizeBytes)
                && (leftNode.keys.Count + internalNode.keys.Count > minPageSizeBeforeSplit) &&
                leftNode.keys.Count >= minPageSizeAfterSplit)
            {
                // Borrow
                var previousIndex = index - 1;
                var parentKey = parentNode.keys.Get(previousIndex);
                DistributeBetweenNodesInternalBasedOnBytes(in leftNode, in internalNode, in parentKey, in previousIndex, in parentNode);

                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(leftNode.Id, leftNode);
                isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);

                if (isFull)
                {
                    leftNode.Return();
                    internalNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            else
            {
                var parentKey = parentNode.keys.Get(index - 1);
                MergeInternalNodesIntoLeft(in leftNode, in internalNode, in parentKey);

                parentNode.EnterWriteLock();
                parentNode.keys.RemoveAt(index - 1);
                parentNode.children.RemoveAt(index);
                parentNode.ExitWriteLock();

                // Save all changes
                m_stateClient.Delete(internalNode.Id);

                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(leftNode.Id, leftNode);

                if (isFull)
                {
                    leftNode.Return();
                    internalNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            leftNode.Return();
            internalNode.Return();
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetRightByteBased(
            ValueTask<IBPlusTreeNode?> getRightNodeTask,
            InternalNode<K, V, TKeyContainer> internalNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var rightNode = (await getRightNodeTask.ConfigureAwait(false) as InternalNode<K, V, TKeyContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetRightByteBased(in rightNode, in internalNode, in parentNode, in index, in result).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetRightByteBased(
            ref readonly InternalNode<K, V, TKeyContainer> rightNode,
            ref readonly InternalNode<K, V, TKeyContainer> internalNode,
            ref readonly InternalNode<K, V, TKeyContainer> parentNode,
            ref readonly int index,
            ref readonly GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var byteSize = rightNode.GetByteSize();
            var leftSize = internalNode.GetByteSize();
            if (
                (byteSize >= (m_stateClient.Metadata.PageSizeBytes / 2) || (byteSize + leftSize) >= m_stateClient.Metadata.PageSizeBytes) &&
                ((rightNode.keys.Count + internalNode.keys.Count) > minPageSizeBeforeSplit))
            {
                var parentKey = parentNode.keys.Get(index);

                DistributeBetweenNodesInternalBasedOnBytes(in internalNode, in rightNode, in parentKey, in index, in parentNode);

                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(rightNode.Id, rightNode);
                isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);

                if (isFull)
                {
                    rightNode.Return();
                    internalNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            else
            {
                var parentKey = parentNode.keys.Get(index);
                MergeInternalNodesIntoLeft(internalNode, rightNode, parentKey);

                parentNode.EnterWriteLock();
                parentNode.keys.RemoveAt(index);
                parentNode.children.RemoveAt(index + 1);
                parentNode.ExitWriteLock();

                m_stateClient.Delete(rightNode.Id);
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);

                if (isFull)
                {
                    rightNode.Return();
                    internalNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            rightNode.Return();
            internalNode.Return();
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_SlowGetLeftByteBased(
            ValueTask<IBPlusTreeNode?> getLeftNodeTask,
            LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var leftNode = (await getLeftNodeTask.ConfigureAwait(false) as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeftByteBased(in leftNode, in leafNode, in parentNode, in index, in result).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeftByteBased(
            ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> leftNode,
            ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            ref readonly InternalNode<K, V, TKeyContainer> parentNode,
            ref readonly int index,
            ref readonly GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);

            var leftSize = leftNode.GetByteSize();
            var rightSize = leafNode.GetByteSize();
            // Check if the left node has more than half the allowed size and also more keys than the minimum allowed,
            // then adopt instead of merge, otherwise, move these into the other node
            if (
                // Size is at least half the size of the page or the combined size is larger than the allowed size
                // Also number of elements is larger than the minimum allowed size
                (leftSize >= m_stateClient.Metadata.PageSizeBytes / 2 || (leftSize + rightSize) > m_stateClient.Metadata.PageSizeBytes) &&
                ((leftNode.keys.Count + leafNode.keys.Count) > minPageSizeBeforeSplit))
            {
                var newSplitKey = SplitBetweenNodesByteBased(in leftNode, in leafNode);

                // Since this is the most right node, the key will always be on the left
                parentNode.EnterWriteLock();
                parentNode.keys.Update(index - 1, newSplitKey);
                parentNode.ExitWriteLock();

                // Save all changes
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                isFull |= m_stateClient.AddOrUpdate(leftNode.Id, leftNode);
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);

                leafNode.Return();
                leftNode.Return();
                if (isFull)
                {
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            else
            {
                MergeLeafNodesIntoLeft(in leftNode, in leafNode);

                parentNode.EnterWriteLock();
                parentNode.keys.RemoveAt(index - 1);
                parentNode.children.RemoveAt(index);
                parentNode.ExitWriteLock();

                // Save all changes
                m_stateClient.Delete(leafNode.Id);
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(leftNode.Id, leftNode);

                leftNode.Return();

                if (isFull)
                {
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_NodeTooSmall_SlowGetRightByteBased(
            ValueTask<IBPlusTreeNode?> getRightNodeTask,
            LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var rightNode = (await getRightNodeTask.ConfigureAwait(false) as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRightByteBased(in rightNode, in leafNode, in parentNode, in index, in result).ConfigureAwait(false);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRightByteBased(
            ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> rightNode,
            ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            ref readonly InternalNode<K, V, TKeyContainer> parentNode,
            ref readonly int index,
            ref readonly GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var rightNodeSize = rightNode.GetByteSize();
            var leftSize = leafNode.GetByteSize();
            if (
                ((rightNodeSize >= m_stateClient.Metadata.PageSizeBytes / 2) || (leftSize + rightNodeSize) > m_stateClient.Metadata.PageSizeBytes) &&
                ((rightNode.keys.Count + leafNode.keys.Count) > minPageSizeBeforeSplit))
            {
                var newSplitKey = SplitBetweenNodesByteBased(in leafNode, in rightNode);

                parentNode.EnterWriteLock();
                parentNode.keys.Update(index, newSplitKey);
                parentNode.ExitWriteLock();

                // Save all changes
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                isFull |= m_stateClient.AddOrUpdate(rightNode.Id, rightNode);
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);

                if (isFull)
                {
                    rightNode.Return();
                    leafNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            else
            {
                MergeLeafNodesIntoLeft(in leafNode, in rightNode);

                parentNode.EnterWriteLock();
                parentNode.keys.RemoveAt(index);
                parentNode.children.RemoveAt(index + 1);
                parentNode.ExitWriteLock();

                m_stateClient.Delete(rightNode.Id);
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);

                if (isFull)
                {
                    rightNode.Return();
                    leafNode.Return();
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            rightNode.Return();
            leafNode.Return();
            return ValueTask.FromResult(result);
        }

        /// <summary>
        /// Find a split point where the left side is close to the bytes to find
        /// </summary>
        /// <param name="node"></param>
        /// <param name="bytesToFind"></param>
        /// <returns></returns>
        private int BinarySearchFindLeftSize<T>(ref readonly T node, ref readonly int bytesToFind)
            where T : BaseNode<K, TKeyContainer>
        {
            int start = 0;
            int end = node.keys.Count - 1;

            while (end > start)
            {
                var mid = (start + end + 1) / 2;
                var newSize = node.GetByteSize(0, mid);

                // Check if left size is at most 30% bigger or smaller than half size if so, break
                if (newSize >= bytesToFind * 0.7 && newSize <= bytesToFind * 1.3)
                {
                    start = mid;
                    break;
                }

                if (newSize < bytesToFind)
                {
                    start = mid + 1;
                }
                else
                {
                    end = mid - 1;
                }
            }
            return start;
        }

        /// <summary>
        /// Tries to distribute the number of bytes evenly between the two internal nodes
        /// </summary>
        /// <param name="leftNode"></param>
        /// <param name="rightNode"></param>
        /// <param name="parentKey"></param>
        /// <returns></returns>
        internal void DistributeBetweenNodesInternalBasedOnBytes(
            ref readonly InternalNode<K, V, TKeyContainer> leftNode,
            ref readonly InternalNode<K, V, TKeyContainer> rightNode,
            ref readonly K parentKey,
            ref readonly int parentIndex,
            ref readonly InternalNode<K, V, TKeyContainer> parent)
        {
            leftNode.EnterWriteLock();
            rightNode.EnterWriteLock();
            var leftSize = leftNode.GetByteSize();
            var rightSize = rightNode.GetByteSize();
            var totalByteSize = leftSize + rightSize;

            var halfByteSize = totalByteSize / 2;

            //K? splitKey = default;
            //IDisposable? splitKeyDisposable = default;
            if ((leftSize < halfByteSize && rightNode.keys.Count > minPageSizeAfterSplit) || leftNode.keys.Count < minPageSizeAfterSplit)
            {
                var bytesToMove = halfByteSize - leftSize;

                // Binary search to try get the index that fulfills the bytes to move
                int splitIndex = BinarySearchFindLeftSize(in rightNode, in bytesToMove);

                if (splitIndex <= (minPageSizeAfterSplit / 2))
                {
                    splitIndex = 1;
                }
                if (splitIndex > rightNode.keys.Count - minPageSizeAfterSplit)
                {
                    splitIndex = rightNode.keys.Count - minPageSizeAfterSplit;
                }

                var remainder = splitIndex;
                // Add a new key with the most right value on left side
                leftNode.keys.Add(parentKey);
                leftNode.keys.AddRangeFrom(rightNode.keys, 0, remainder - 1);

                // Set the split key to the most right value
                var splitKey = rightNode.keys.Get(remainder - 1);
                parent.EnterWriteLock();
                parent.keys.Update(parentIndex, splitKey);
                parent.ExitWriteLock();
                leftNode.children.AddRangeFrom(rightNode.children, 0, remainder);

                var rightNodeSize = rightNode.keys.Count - remainder;
                var rightKeys = m_options.KeySerializer.CreateEmpty(); //new List<K>(rightNodeSize);
                var rightChildren = new PrimitiveList<long>(m_options.MemoryAllocator);

                rightKeys.AddRangeFrom(rightNode.keys, remainder, rightNodeSize);
                rightChildren.AddRangeFrom(rightNode.children, remainder, rightNode.children.Count - remainder);

                // Set the right node keys as disposable since it will no longer be used
                rightNode.keys.Dispose();

                rightNode.keys = rightKeys;
                rightNode.children = rightChildren;
            }
            // Left has more values
            else
            {
                var bytesToMove = halfByteSize - rightSize;
                var negated = leftSize - bytesToMove;

                int splitIndex = BinarySearchFindLeftSize(in leftNode, in negated);

                if (splitIndex < minPageSizeAfterSplit)
                {
                    splitIndex = minPageSizeAfterSplit; //Math.Min(minPageSize, leftNode.keys.Count - minPageSize);
                }
                if (splitIndex >= leftNode.keys.Count)
                {
                    splitIndex = leftNode.keys.Count - 1;
                }


                var dataToMove = leftNode.keys.Count - splitIndex;

                var rightKeys = m_options.KeySerializer.CreateEmpty(); // new List<K>(half);
                var rightChildren = new PrimitiveList<long>(m_options.MemoryAllocator);

                var remainder = dataToMove;


                leftNode.keys.Add(parentKey);
                // Copy values from left to right at the beginning
                rightKeys.AddRangeFrom(leftNode.keys, leftNode.keys.Count - remainder, remainder);
                rightKeys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);

                rightChildren.AddRangeFrom(leftNode.children, leftNode.children.Count - remainder, remainder);
                rightChildren.AddRangeFrom(rightNode.children, 0, rightNode.children.Count);

                leftNode.keys.RemoveRange(leftNode.keys.Count - remainder, remainder);
                leftNode.children.RemoveRange(leftNode.children.Count - remainder, remainder);

                // // Set the right node keys as disposable since it will no longer be used
                rightNode.keys.Dispose();

                rightNode.keys = rightKeys;
                rightNode.children = rightChildren;
                var splitKey = leftNode.keys.Get(leftNode.keys.Count - 1);

                parent.EnterWriteLock();
                parent.keys.Update(parentIndex, splitKey);
                parent.ExitWriteLock();

                leftNode.keys.RemoveAt(leftNode.keys.Count - 1);
            }

            // TODO: Remove when comfortable
            Debug.Assert(leftNode.keys.Count >= minPageSize && rightNode.keys.Count >= minPageSize, "Split did not work as expected");

            rightNode.ExitWriteLock();
            leftNode.ExitWriteLock();
        }

        private (LeafNode<K, V, TKeyContainer, TValueContainer>, K splitKey) SplitLeafNodeBasedOnBytes(
            ref readonly InternalNode<K, V, TKeyContainer> parent,
            in int index,
            ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> child,
            ref readonly int leafSize)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            // Check that both parent and child write locks are collected
            var newNodeId = m_stateClient.GetNewPageId();

            int halfSize = leafSize / 2;
            int start = BinarySearchFindLeftSize(in child, in halfSize);

            // Make sure that no page is smaller than the min page size
            if (start < minPageSizeAfterSplit)
            {
                start = minPageSizeAfterSplit;
            }
            if (start > child.keys.Count - minPageSizeAfterSplit)
            {
                start = child.keys.Count - minPageSizeAfterSplit;
            }

            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var emptyValues = m_options.ValueSerializer.CreateEmpty();
            var newNode = new LeafNode<K, V, TKeyContainer, TValueContainer>(newNodeId, emptyKeys, emptyValues);
            newNode.EnterWriteLock();

            // Set the next id on the new node to the now left childs next id.
            newNode.next = child.next;

            // Copy half of the values on the right to the new node
            newNode.keys.AddRangeFrom(child.keys, start, child.keys.Count - start);
            newNode.values.AddRangeFrom(child.values, start, child.values.Count - start);
            newNode.ExitWriteLock();

            child.EnterWriteLock();
            // Clear the values from the child, this is so the values and keys can be garbage collected
            child.keys.RemoveRange(start, child.keys.Count - start);
            child.values.RemoveRange(start, child.values.Count - start);

            // Set the next pointer on the child for iteration
            child.next = newNodeId;
            child.ExitWriteLock();

            var splitKey = child.keys.Get(child.keys.Count - 1);
            // Add the children to the parent node
            parent.EnterWriteLock();

            parent.keys.Insert_Internal(index, splitKey);

            parent.children.InsertAt(index + 1, newNodeId);
            parent.ExitWriteLock();

            // TODO: Remove when comfortable
            Debug.Assert(newNode.keys.Count >= minPageSizeAfterSplit && child.keys.Count >= minPageSizeAfterSplit, "Split did not work as expected");


            return (newNode, splitKey);
        }

        private (InternalNode<K, V, TKeyContainer>, K splitKey) SplitInternalNodeBasedOnBytes(
            ref readonly InternalNode<K, V, TKeyContainer> parent,
            in int index,
            ref readonly InternalNode<K, V, TKeyContainer> child,
            ref readonly int totalBytes)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            int halfSize = totalBytes / 2;
            int start = BinarySearchFindLeftSize(in child, in halfSize);

            // Before this method it has already been checked that node count >= minPageSize*2
            if (start <= minPageSizeAfterSplit)
            {
                start = minPageSizeAfterSplit + 1;
            }
            if (start > child.keys.Count - minPageSizeAfterSplit)
            {
                start = child.keys.Count - minPageSizeAfterSplit;
            }

            var newNodeId = m_stateClient.GetNewPageId();
            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var newNode = new InternalNode<K, V, TKeyContainer>(newNodeId, emptyKeys, m_options.MemoryAllocator);

            var childKeyCount = child.keys.Count;
            newNode.keys.AddRangeFrom(child.keys, start, childKeyCount - start);
            newNode.children.AddRangeFrom(child.children, start, (childKeyCount - start) + 1);

            var splitKey = child.keys.Get(start - 1);

            parent.EnterWriteLock();
            parent.keys.Insert_Internal(index, splitKey);
            parent.children.InsertAt(index + 1, newNodeId);
            parent.ExitWriteLock();

            child.EnterWriteLock();
            child.keys.RemoveRange(start - 1, (childKeyCount - start) + 1);
            child.children.RemoveRange(start, (childKeyCount - start) + 1);
            child.ExitWriteLock();

            // TODO: Remove when comfortable
            Debug.Assert(newNode.keys.Count >= minPageSizeAfterSplit && child.keys.Count >= minPageSizeAfterSplit, "Split did not work as expected");

            return (newNode, splitKey);
        }

        /// <summary>
        /// Tries to move elements between two leaf nodes to keep them balanced.
        /// It first tries to move based on byte sized, if that is not possible (total count is less than min allowed count) it will move based on key count.
        /// </summary>
        /// <param name="leftNode"></param>
        /// <param name="rightNode"></param>
        /// <returns></returns>
        private K SplitBetweenNodesByteBased(ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> leftNode, ref readonly LeafNode<K, V, TKeyContainer, TValueContainer> rightNode)
        {
            leftNode.EnterWriteLock();
            rightNode.EnterWriteLock();

            var leftSize = leftNode.GetByteSize();
            var rightSize = rightNode.GetByteSize();
            var totalSize = leftSize + rightSize;
            var totalCount = leftNode.keys.Count + rightNode.keys.Count;

            var halfCount = (totalCount + 1) / 2;
            var halfSize = totalSize / 2;

            // Left side is smaller and right node has enough keys to move values into left
            if (leftSize < halfSize && rightNode.keys.Count >= minPageSizeBeforeSplit)
            {
                var bytesToFind = rightSize - halfSize;
                var rightSideIndex = BinarySearchFindLeftSize(in rightNode, in bytesToFind);

                if ((rightNode.keys.Count - rightSideIndex) < minPageSizeAfterSplit)
                {
                    // The page would be smaller than the allowed min size, set it to the min size.
                    rightSideIndex = rightNode.keys.Count - minPageSizeAfterSplit;
                }
                if ((leftNode.keys.Count + rightSideIndex) < minPageSizeAfterSplit)
                {
                    // Move more elements from right so left side has at least minPageSize
                    rightSideIndex = minPageSizeAfterSplit - leftNode.keys.Count;
                }

                var remainder = rightSideIndex;
                leftNode.keys.AddRangeFrom(rightNode.keys, 0, remainder);
                leftNode.values.AddRangeFrom(rightNode.values, 0, remainder);

                var rightNodeSize = rightNode.keys.Count - remainder;
                var rightKeys = m_options.KeySerializer.CreateEmpty();
                var rightValues = m_options.ValueSerializer.CreateEmpty();

                rightKeys.AddRangeFrom(rightNode.keys, remainder, rightNodeSize);
                rightValues.AddRangeFrom(rightNode.values, remainder, rightNodeSize);

                // Dispose previous keys and values
                rightNode.keys.Dispose();
                rightNode.values.Dispose();

                rightNode.keys = rightKeys;
                rightNode.values = rightValues;
            }
            // Right side is smaller and left node has enough keys to move values into right
            else if (rightSize < halfSize && leftNode.keys.Count >= minPageSizeBeforeSplit)
            {
                var bytesToFind = leftSize - halfSize;
                var leftSideIndex = BinarySearchFindLeftSize(in leftNode, in bytesToFind);

                var numberOfElementsToCopy = leftNode.keys.Count - leftSideIndex;
                if (rightNode.keys.Count + numberOfElementsToCopy < minPageSizeAfterSplit)
                {
                    // The page would be smaller than the allowed min size, set it to the min size.
                    numberOfElementsToCopy = minPageSizeAfterSplit - rightNode.keys.Count;
                }
                if ((leftNode.keys.Count - numberOfElementsToCopy) < minPageSizeAfterSplit)
                {
                    // left side is already checked to be larger than minPageSizeBeforeSplit
                    numberOfElementsToCopy = leftNode.keys.Count - minPageSizeAfterSplit;
                }

                var rightKeys = m_options.KeySerializer.CreateEmpty();
                var rightValues = m_options.ValueSerializer.CreateEmpty();

                var remainder = numberOfElementsToCopy;

                // Copy values from left to right at the beginning
                rightKeys.AddRangeFrom(leftNode.keys, leftNode.keys.Count - remainder, remainder);
                rightKeys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);
                rightValues.AddRangeFrom(leftNode.values, leftNode.keys.Count - remainder, remainder);
                rightValues.AddRangeFrom(rightNode.values, 0, rightNode.values.Count);

                leftNode.keys.RemoveRange(leftNode.keys.Count - remainder, remainder);
                leftNode.values.RemoveRange(leftNode.values.Count - remainder, remainder);

                // Dispose previous keys and values
                rightNode.keys.Dispose();
                rightNode.values.Dispose();

                rightNode.keys = rightKeys;
                rightNode.values = rightValues;

            }
            // Fallback if it was not possible to move elements based on byte size
            else
            {
                if (leftNode.keys.Count < halfCount)
                {
                    var remainder = halfCount - leftNode.keys.Count;
                    leftNode.keys.AddRangeFrom(rightNode.keys, 0, remainder);
                    leftNode.values.AddRangeFrom(rightNode.values, 0, remainder);

                    var rightNodeSize = rightNode.keys.Count - remainder;
                    var rightKeys = m_options.KeySerializer.CreateEmpty();
                    var rightValues = m_options.ValueSerializer.CreateEmpty();

                    rightKeys.AddRangeFrom(rightNode.keys, remainder, rightNodeSize);
                    rightValues.AddRangeFrom(rightNode.values, remainder, rightNodeSize);

                    // Dispose previous keys and values
                    rightNode.keys.Dispose();
                    rightNode.values.Dispose();

                    rightNode.keys = rightKeys;
                    rightNode.values = rightValues;
                }
                // Left has more values
                else
                {
                    var remainder = halfCount - rightNode.keys.Count;

                    var rightKeys = m_options.KeySerializer.CreateEmpty();
                    var rightValues = m_options.ValueSerializer.CreateEmpty();

                    // Copy values from left to right at the beginning
                    rightKeys.AddRangeFrom(leftNode.keys, leftNode.keys.Count - remainder, remainder);

                    rightKeys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);
                    rightValues.AddRangeFrom(leftNode.values, leftNode.keys.Count - remainder, remainder);
                    rightValues.AddRangeFrom(rightNode.values, 0, rightNode.values.Count);

                    leftNode.keys.RemoveRange(leftNode.keys.Count - remainder, remainder);
                    leftNode.values.RemoveRange(leftNode.values.Count - remainder, remainder);

                    // Dispose previous keys and values
                    rightNode.keys.Dispose();
                    rightNode.values.Dispose();

                    rightNode.keys = rightKeys;
                    rightNode.values = rightValues;
                    //if (remainder > 0)
                    //{

                    //}
                }
            }

            rightNode.ExitWriteLock();
            leftNode.ExitWriteLock();

            // TODO: Remove when comfortable
            Debug.Assert(leftNode.keys.Count >= minPageSize && rightNode.keys.Count >= minPageSize, "Split did not work as expected");

            var splitKey = leftNode.keys.Get(leftNode.keys.Count - 1);
            return splitKey;
        }
    }
}
