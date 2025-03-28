﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    internal partial class BPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {


        public ValueTask<GenericWriteOperation> GenericWrite(ref readonly K key, in V? value, in GenericWriteFunction<V> function)
        {
            if (m_isByteBased)
            {
                return GenericWriteByteBased(in key, in value, in function);
            }
            return GenericWriteRoot(key, value, function);
        }

        /// <summary>
        /// Does a read modify write which takes a function that is called.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="function"></param>
        /// <returns></returns>
        private ValueTask<GenericWriteOperation> GenericWriteRoot(
            in K key,
            in V? value,
            in GenericWriteFunction<V> function)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            var rootNodeTask = m_stateClient.GetValue(m_stateClient.Metadata.Root);

            if (!rootNodeTask.IsCompletedSuccessfully)
            {
                return GenericWriteRoot_SlowGetRoot(key, value, function, rootNodeTask);
            }
            var rootNode = rootNodeTask.Result;
            return GenericWriteRoot_AfterGetRoot(rootNode, key, value, function);
        }

        private async ValueTask<GenericWriteOperation> GenericWriteRoot_SlowGetRoot(
            K key,
            V? value,
            GenericWriteFunction<V> function,
            ValueTask<IBPlusTreeNode?> getRootNodeTask)
        {
            var rootNode = await getRootNodeTask;
            Debug.Assert(rootNode != null, nameof(rootNode));
            return await GenericWriteRoot_AfterGetRoot(rootNode, key, value, function);
        }

        private ValueTask<GenericWriteOperation> GenericWriteRoot_AfterGetRoot(
            in IBPlusTreeNode? rootNode,
            in K key,
            in V? value,
            in GenericWriteFunction<V> function)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            if (rootNode is LeafNode<K, V, TKeyContainer, TValueContainer> leafNode)
            {
                var result = GenericWrite_Leaf(leafNode, key, value, function);

                // No need to check for merging at root leaf, only check for split
                if (leafNode.keys.Count == m_stateClient.Metadata.BucketLength)
                {

                    var nextId = m_stateClient.GetNewPageId();
                    var emptyKeys = m_options.KeySerializer.CreateEmpty();
                    var newParentNode = new InternalNode<K, V, TKeyContainer>(nextId, emptyKeys, m_options.MemoryAllocator);

                    // No lock required
                    newParentNode.children.InsertAt(0, leafNode.Id);
                    m_stateClient.Metadata = m_stateClient.Metadata.UpdateRoot(nextId);

                    var (newNode, _) = SplitLeafNode(newParentNode, 0, leafNode);

                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newParentNode.Id, newParentNode);
                    isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);

                    if (m_usePreviousPointer && newNode.next != 0)
                    {
                        return UpdateRightPrevious(result, newNode, isFull, false);
                    }

                    if (isFull)
                    {
                        rootNode.Return();
                        return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                    }
                }
                else
                {
                    var isFull = m_stateClient.AddOrUpdate(leafNode.Id, leafNode);

                    if (isFull)
                    {
                        rootNode.Return();
                        return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                    }
                }
                rootNode.Return();
                return ValueTask.FromResult(result);
            }
            else if (rootNode is InternalNode<K, V, TKeyContainer> internalNode)
            {
                var resultTask = GenericWrite_Internal(internalNode, key, value, function);
                if (!resultTask.IsCompletedSuccessfully)
                {
                    return GenericWriteRoot_SlowInternal(resultTask, internalNode);
                }
                var result = resultTask.Result;
                return GenericWriteRoot_AfterInternal(result, internalNode);
            }
            throw new NotImplementedException();
        }

        private async ValueTask<GenericWriteOperation> GenericWriteRoot_SlowInternal(
            ValueTask<GenericWriteOperation> resultTask,
            InternalNode<K, V, TKeyContainer> internalNode
            )
        {
            var result = await resultTask;
            return await GenericWriteRoot_AfterInternal(result, internalNode);
        }

        private ValueTask<GenericWriteOperation> GenericWriteRoot_AfterInternal(
            in GenericWriteOperation result,
            in InternalNode<K, V, TKeyContainer> internalNode
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            if (internalNode.keys.Count == m_stateClient.Metadata.BucketLength)
            {
                var nextId = m_stateClient.GetNewPageId();
                var emptyKeys = m_options.KeySerializer.CreateEmpty();
                var newParentNode = new InternalNode<K, V, TKeyContainer>(nextId, emptyKeys, m_options.MemoryAllocator);
                // No lock requireds
                newParentNode.children.InsertAt(0, internalNode.Id);
                m_stateClient.Metadata = m_stateClient.Metadata.UpdateRoot(nextId);

                var (newNode, _) = SplitInternalNode(newParentNode, 0, internalNode);

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

        private ValueTask<GenericWriteOperation> GenericWrite_Internal(
            in InternalNode<K, V, TKeyContainer> parentNode,
            in K key,
            in V? value,
            in GenericWriteFunction<V> function
            )
        {
            var index = m_keyComparer.FindIndex(key, parentNode.keys);

            if (index < 0)
            {
                index = ~index;
            }
            var childId = parentNode.children[index];
            var getChildTask = m_stateClient.GetValue(childId);

            if (!getChildTask.IsCompletedSuccessfully)
            {
                return GenericWrite_Internal_SlowGetNode(index, getChildTask, parentNode, key, value, function);
            }
            var child = getChildTask.Result;
            return GenericWrite_Internal_AfterGetNode(index, child, parentNode, key, value, function);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_SlowGetNode(
            int index,
            ValueTask<IBPlusTreeNode?> getChildTask,
            InternalNode<K, V, TKeyContainer> parentNode,
            K key,
            V? value,
            GenericWriteFunction<V> function
            )
        {
            var child = await getChildTask;
            return await GenericWrite_Internal_AfterGetNode(index, child, parentNode, key, value, function);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode(
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
                var result = GenericWrite_Leaf(leafNode, key, value, function);

                // Check if split is required
                if (leafNode.keys.Count >= m_stateClient.Metadata.BucketLength)
                {
                    var (newNode, _) = SplitLeafNode(parentNode, index, leafNode);
                    // Save all the nodes that was changed
                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                    isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);

                    if (m_usePreviousPointer && newNode.next != 0)
                    {
                        leafNode.Return();
                        return UpdateRightPrevious(result, newNode, isFull, false);
                    }

                    if (isFull)
                    {
                        leafNode.Return();
                        return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                    }
                }
                // Check if the node is too small
                else if (leafNode.keys.Count <= minSize)
                {
                    if (index == parentNode.keys.Count)
                    {
                        // Merge with left neighbor
                        var leftNeighborId = parentNode.children[index - 1];
                        var getLeftNodeTask = m_stateClient.GetValue(leftNeighborId);
                        if (!getLeftNodeTask.IsCompletedSuccessfully)
                        {
                            return GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_SlowGetLeft(getLeftNodeTask, leafNode, parentNode, index, result);
                        }
                        var leftNode = (getLeftNodeTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;

                        return GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeft(leftNode, leafNode, parentNode, index, result);
                    }
                    else
                    {
                        // Merge with right neighbor
                        var rightNeighborId = parentNode.children[index + 1];
                        var getRightNodeTask = m_stateClient.GetValue(rightNeighborId);
                        if (!getRightNodeTask.IsCompletedSuccessfully)
                        {
                            return GenericWrite_Internal_AfterGetNode_NodeTooSmall_SlowGetRight(getRightNodeTask, leafNode, parentNode, index, result);
                        }
                        var rightNode = (getRightNodeTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;

                        return GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRight(rightNode, leafNode, parentNode, index, result);
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
                var rmwTask = GenericWrite_Internal(internalNode, key, value, function);
                if (!rmwTask.IsCompletedSuccessfully)
                {
                    return GenericWrite_Internal_AfterGetNode_SlowCallInternal(rmwTask, internalNode, parentNode, index);
                }
                var result = rmwTask.Result;
                return GenericWrite_Internal_AfterGetNode_AfterCallInternal(internalNode, parentNode, index, result);
            }
            throw new NotImplementedException();
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_SlowCallInternal(
            ValueTask<GenericWriteOperation> internalTask,
            InternalNode<K, V, TKeyContainer> internalNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index
            )
        {
            var result = await internalTask;
            return await GenericWrite_Internal_AfterGetNode_AfterCallInternal(internalNode, parentNode, index, result);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal(
            in InternalNode<K, V, TKeyContainer> internalNode,
            in InternalNode<K, V, TKeyContainer> parentNode,
            in int index,
            in GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            // Check if split is required
            if (internalNode.keys.Count == m_stateClient.Metadata.BucketLength)
            {
                var (newNode, splitKey) = SplitInternalNode(parentNode, index, internalNode);

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
            if (internalNode.keys.Count <= minSize)
            {
                if (index == parentNode.keys.Count)
                {
                    var leftNeighborId = parentNode.children[index - 1];
                    var getLeftNodeTask = m_stateClient.GetValue(leftNeighborId);
                    if (!getLeftNodeTask.IsCompletedSuccessfully)
                    {
                        return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetLeft(getLeftNodeTask, internalNode, parentNode, index, result);
                    }
                    var leftNode = (getLeftNodeTask.Result as InternalNode<K, V, TKeyContainer>)!;
                    return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetLeft(leftNode, internalNode, parentNode, index, result);
                }
                else
                {
                    var rightNeighborId = parentNode.children[index + 1];
                    var getRightNodeTask = m_stateClient.GetValue(rightNeighborId);
                    if (!getRightNodeTask.IsCompletedSuccessfully)
                    {
                        return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetRight(getRightNodeTask, internalNode, parentNode, index, result);
                    }
                    var rightNode = (getRightNodeTask.Result as InternalNode<K, V, TKeyContainer>)!;

                    return GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetRight(rightNode, internalNode, parentNode, index, result);
                }
            }
            internalNode.Return();
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetRight(
            ValueTask<IBPlusTreeNode?> getRightNodeTask,
            InternalNode<K, V, TKeyContainer> internalNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var rightNode = (await getRightNodeTask as InternalNode<K, V, TKeyContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetRight(rightNode, internalNode, parentNode, index, result);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetRight(
            in InternalNode<K, V, TKeyContainer> rightNode,
            in InternalNode<K, V, TKeyContainer> internalNode,
            in InternalNode<K, V, TKeyContainer> parentNode,
            in int index,
            in GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            if (rightNode.keys.Count >= m_stateClient.Metadata.BucketLength / 2)
            {
                var parentKey = parentNode.keys.Get(index);
                var (newSplitKey, splitKeyDisposable) = DistributeBetweenNodesInternal(internalNode, rightNode, parentKey);

                parentNode.EnterWriteLock();
                parentNode.keys.Update(index, newSplitKey);
                parentNode.ExitWriteLock();

                // Done with the split key, it was updated into parent, can now dispose it
                splitKeyDisposable.Dispose();

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

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_NodeTooSmall_SlowGetRight(
            ValueTask<IBPlusTreeNode?> getRightNodeTask,
            LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var rightNode = (await getRightNodeTask as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRight(rightNode, leafNode, parentNode, index, result);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRight(
            in LeafNode<K, V, TKeyContainer, TValueContainer> rightNode,
            in LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            in InternalNode<K, V, TKeyContainer> parentNode,
            in int index,
            in GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            if (rightNode.keys.Count >= m_stateClient.Metadata.BucketLength / 2)
            {
                var newSplitKey = SplitBetweenNodes(leafNode, rightNode);

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
                MergeLeafNodesIntoLeft(leafNode, rightNode);

                parentNode.EnterWriteLock();
                parentNode.keys.RemoveAt(index);
                parentNode.children.RemoveAt(index + 1);
                parentNode.ExitWriteLock();

                m_stateClient.Delete(rightNode.Id);
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);

                if (m_usePreviousPointer && leafNode.next != 0)
                {
                    return UpdateRightPrevious(result, leafNode, isFull, true);
                }

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

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_SlowGetLeft(
            ValueTask<IBPlusTreeNode?> getLeftNodeTask,
            LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var leftNode = (await getLeftNodeTask as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeft(leftNode, leafNode, parentNode, index, result);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeft(
            in LeafNode<K, V, TKeyContainer, TValueContainer> leftNode,
            in LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            in InternalNode<K, V, TKeyContainer> parentNode,
            in int index,
            in GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            // Check if the left node has more than half keys, then adopt instead of merge
            if (leftNode.keys.Count >= m_stateClient.Metadata.BucketLength / 2)
            {
                var newSplitKey = SplitBetweenNodes(leftNode, leafNode);

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
                MergeLeafNodesIntoLeft(leftNode, leafNode);

                parentNode.EnterWriteLock();
                parentNode.keys.RemoveAt(index - 1);
                parentNode.children.RemoveAt(index);
                parentNode.ExitWriteLock();

                // Save all changes
                m_stateClient.Delete(leafNode.Id);
                var isFull = false;
                isFull |= m_stateClient.AddOrUpdate(parentNode.Id, parentNode);
                isFull |= m_stateClient.AddOrUpdate(leftNode.Id, leftNode);

                if (m_usePreviousPointer && leftNode.next != 0)
                {
                    return UpdateRightPrevious(result, leftNode, isFull, true);
                }

                leftNode.Return();

                if (isFull)
                {
                    return GenericWrite_SlowUpsert(result, m_stateClient.WaitForNotFullAsync());
                }
            }
            return ValueTask.FromResult(result);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_SlowGetLeft(
            ValueTask<IBPlusTreeNode?> getLeftNodeTask,
            InternalNode<K, V, TKeyContainer> internalNode,
            InternalNode<K, V, TKeyContainer> parentNode,
            int index,
            GenericWriteOperation result
            )
        {
            var leftNode = (await getLeftNodeTask as InternalNode<K, V, TKeyContainer>)!;
            return await GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetLeft(leftNode, internalNode, parentNode, index, result);
        }

        private ValueTask<GenericWriteOperation> GenericWrite_Internal_AfterGetNode_AfterCallInternal_InternalTooSmall_AfterGetLeft(
            in InternalNode<K, V, TKeyContainer> leftNode,
            in InternalNode<K, V, TKeyContainer> internalNode,
            in InternalNode<K, V, TKeyContainer> parentNode,
            in int index,
            in GenericWriteOperation result
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
            if (leftNode.keys.Count >= m_stateClient.Metadata.BucketLength / 2)
            {
                // Borrow
                var parentKey = parentNode.keys.Get(index - 1);
                var (newSplitKey, splitKeyDisposable) = DistributeBetweenNodesInternal(leftNode, internalNode, parentKey);

                parentNode.EnterWriteLock();
                parentNode.keys.Update(index - 1, newSplitKey);
                parentNode.ExitWriteLock();

                // Done with the split key, it was updated into parent, can now dispose it
                splitKeyDisposable.Dispose();

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
                MergeInternalNodesIntoLeft(leftNode, internalNode, parentKey);

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

        private GenericWriteOperation GenericWrite_Leaf(
            in LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            in K key,
            in V? value,
            in GenericWriteFunction<V> function)
        {
            var index = m_keyComparer.FindIndex(in key, in leafNode.keys);
            if (index < 0)
            {
                var result = function(value, default, false);
                if (result.operation == GenericWriteOperation.None)
                {
                    return result.operation;
                }
                if (result.operation == GenericWriteOperation.Upsert)
                {
                    Debug.Assert(result.result != null, "Value returned was null, cant insert a null value.");
                    index = ~index;
                    leafNode.InsertAt(key, result.result, index);
                    return result.operation;
                }
                if (result.operation == GenericWriteOperation.Delete)
                {
                    throw new InvalidOperationException("Cant delete an element since it does not exist");
                }
                throw new NotImplementedException();
            }
            else
            {
                var currentValue = leafNode.values.Get(index);
                var result = function(value, currentValue, true);
                if (result.operation == GenericWriteOperation.Upsert)
                {
                    leafNode.UpdateValueAt(index, result.result!);
                }
                else if (result.operation == GenericWriteOperation.Delete)
                {
                    leafNode.DeleteAt(index);
                }
                return result.operation;
            }
        }

        private (LeafNode<K, V, TKeyContainer, TValueContainer>, K splitKey) SplitLeafNode(
            in InternalNode<K, V, TKeyContainer> parent,
            in int index,
            in LeafNode<K, V, TKeyContainer, TValueContainer> child)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            // Check that both parent and child write locks are collected
            var newNodeId = m_stateClient.GetNewPageId();// _metadata.GetNextId();

            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var emptyValues = m_options.ValueSerializer.CreateEmpty();
            var newNode = new LeafNode<K, V, TKeyContainer, TValueContainer>(newNodeId, emptyKeys, emptyValues);
            newNode.EnterWriteLock();

            // Set the next id on the new node to the now left childs next id.
            newNode.next = child.next;

            if (m_usePreviousPointer)
            {
                newNode.previous = child.Id;
            }

            // Copy half of the values on the right to the new node
            newNode.keys.AddRangeFrom(child.keys, m_stateClient.Metadata.BucketLength / 2, child.keys.Count - m_stateClient.Metadata.BucketLength / 2);
            newNode.values.AddRangeFrom(child.values, m_stateClient.Metadata.BucketLength / 2, child.values.Count - m_stateClient.Metadata.BucketLength / 2);
            newNode.ExitWriteLock();

            child.EnterWriteLock();
            // Clear the values from the child, this is so the values and keys can be garbage collected
            child.keys.RemoveRange(m_stateClient.Metadata.BucketLength / 2, child.keys.Count - m_stateClient.Metadata.BucketLength / 2);
            child.values.RemoveRange(m_stateClient.Metadata.BucketLength / 2, child.values.Count - m_stateClient.Metadata.BucketLength / 2);

            // Set the next pointer on the child for iteration
            child.next = newNodeId;
            child.ExitWriteLock();

            var splitKey = child.keys.Get(child.keys.Count - 1);
            // Add the children to the parent node
            parent.EnterWriteLock();
            parent.keys.Insert_Internal(index, splitKey);
            parent.children.InsertAt(index + 1, newNodeId);
            parent.ExitWriteLock();
            return (newNode, splitKey);
        }

        private async ValueTask<GenericWriteOperation> GenericWrite_SlowUpsert(
            GenericWriteOperation result,
            Task task
            )
        {
            await task;
            return result;
        }

        private K SplitBetweenNodes(LeafNode<K, V, TKeyContainer, TValueContainer> leftNode, LeafNode<K, V, TKeyContainer, TValueContainer> rightNode)
        {
            leftNode.EnterWriteLock();
            rightNode.EnterWriteLock();
            var totalCount = leftNode.keys.Count + rightNode.keys.Count;

            var half = totalCount / 2;

            // Move values from right to left since left has less than half of the values
            if (leftNode.keys.Count < half)
            {
                var remainder = half - leftNode.keys.Count;
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
                var rightKeys = m_options.KeySerializer.CreateEmpty();  //new List<K>(half);
                var rightValues = m_options.ValueSerializer.CreateEmpty(); //new List<V>(half);

                var remainder = half - rightNode.keys.Count;

                // Copy values from left to right at the beginning
                rightKeys.AddRangeFrom(leftNode.keys, leftNode.keys.Count - remainder, remainder);
                rightKeys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);
                rightValues.AddRangeFrom(leftNode.values, leftNode.keys.Count - remainder, remainder);
                rightValues.AddRangeFrom(rightNode.values, 0, rightNode.values.Count);
                //rightKeys.AddRange(leftNode.keys.GetRange(leftNode.keys.Count - remainder, remainder));
                //rightKeys.AddRange(rightNode.keys);
                //rightValues.AddRange(leftNode.values.GetRange(leftNode.keys.Count - remainder, remainder));
                //rightValues.AddRange(rightNode.values);

                leftNode.keys.RemoveRange(leftNode.keys.Count - remainder, remainder);
                leftNode.values.RemoveRange(leftNode.values.Count - remainder, remainder);

                // Dispose previous keys and values
                rightNode.keys.Dispose();
                rightNode.values.Dispose();

                rightNode.keys = rightKeys;
                rightNode.values = rightValues;
            }

            rightNode.ExitWriteLock();
            leftNode.ExitWriteLock();
            var splitKey = leftNode.keys.Get(leftNode.keys.Count - 1);
            return splitKey;
        }

        private void MergeLeafNodesIntoLeft(in LeafNode<K, V, TKeyContainer, TValueContainer> leftNode, in LeafNode<K, V, TKeyContainer, TValueContainer> rightNode)
        {
            leftNode.keys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);
            leftNode.values.AddRangeFrom(rightNode.values, 0, rightNode.values.Count);
            leftNode.next = rightNode.next;
        }

        internal (K, IDisposable) DistributeBetweenNodesInternal(InternalNode<K, V, TKeyContainer> leftNode, InternalNode<K, V, TKeyContainer> rightNode, K parentKey)
        {
            leftNode.EnterWriteLock();
            rightNode.EnterWriteLock();
            var totalCount = leftNode.keys.Count + rightNode.keys.Count;

            var half = totalCount / 2;

            K? splitKey = default;
            IDisposable? splitKeyDisposable = default;
            if (leftNode.keys.Count < half)
            {
                var remainder = half - leftNode.keys.Count;
                // Add a new key with the most right value on left side
                leftNode.keys.Add(parentKey);
                leftNode.keys.AddRangeFrom(rightNode.keys, 0, remainder - 1);
                //leftNode.keys.AddRange(rightNode.keys.GetRange(0, remainder - 1));

                // Set the split key to the most right value
                splitKey = rightNode.keys.Get(remainder - 1);

                leftNode.children.AddRangeFrom(rightNode.children, 0, remainder);
                var rightNodeSize = rightNode.keys.Count - remainder;
                var rightKeys = m_options.KeySerializer.CreateEmpty(); //new List<K>(rightNodeSize);
                var rightChildren = new PrimitiveList<long>(m_options.MemoryAllocator);

                rightKeys.AddRangeFrom(rightNode.keys, remainder, rightNodeSize);

                rightChildren.AddRangeFrom(rightNode.children, remainder, rightNode.children.Count - remainder);
                // Set the right node keys as disposable since it will no longer be used
                splitKeyDisposable = rightNode.keys;

                rightNode.keys = rightKeys;
                rightNode.children = rightChildren;
            }
            // Left has more values
            else
            {
                var rightKeys = m_options.KeySerializer.CreateEmpty(); // new List<K>(half);
                var rightChildren = new PrimitiveList<long>(m_options.MemoryAllocator);

                var remainder = half - rightNode.keys.Count;


                leftNode.keys.Add(parentKey);
                // Copy values from left to right at the beginning
                rightKeys.AddRangeFrom(leftNode.keys, leftNode.keys.Count - remainder, remainder);
                rightKeys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);

                rightChildren.AddRangeFrom(leftNode.children, leftNode.children.Count - remainder, remainder);
                rightChildren.AddRangeFrom(rightNode.children, 0, rightNode.children.Count);

                leftNode.keys.RemoveRange(leftNode.keys.Count - remainder, remainder);
                leftNode.children.RemoveRange(leftNode.children.Count - remainder, remainder);

                // // Set the right node keys as disposable since it will no longer be used
                splitKeyDisposable = rightNode.keys;

                rightNode.keys = rightKeys;
                rightNode.children = rightChildren;
                splitKey = leftNode.keys.Get(leftNode.keys.Count - 1);
                leftNode.keys.RemoveAt(leftNode.keys.Count - 1);
            }
            rightNode.ExitWriteLock();
            leftNode.ExitWriteLock();

            return (splitKey, splitKeyDisposable);
        }

        internal void MergeInternalNodesIntoLeft(in InternalNode<K, V, TKeyContainer> leftNode, in InternalNode<K, V, TKeyContainer> rightNode, in K parentKey)
        {
            leftNode.EnterWriteLock();
            leftNode.keys.Add(parentKey);

            leftNode.keys.AddRangeFrom(rightNode.keys, 0, rightNode.keys.Count);

            leftNode.children.AddRangeFrom(rightNode.children, 0, rightNode.children.Count);

            leftNode.ExitWriteLock();
        }

        private (InternalNode<K, V, TKeyContainer>, K splitKey) SplitInternalNode(
            in InternalNode<K, V, TKeyContainer> parent,
            in int index,
            in InternalNode<K, V, TKeyContainer> child)
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var newNodeId = m_stateClient.GetNewPageId(); // _metadata.GetNextId();
            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var newNode = new InternalNode<K, V, TKeyContainer>(newNodeId, emptyKeys, m_options.MemoryAllocator);

            newNode.keys.AddRangeFrom(child.keys, m_stateClient.Metadata.BucketLength / 2, child.keys.Count - m_stateClient.Metadata.BucketLength / 2);
            //newNode.keys.AddRange(child.keys.GetRange(m_stateClient.Metadata.BucketLength / 2, child.keys.Count - m_stateClient.Metadata.BucketLength / 2));

            newNode.children.AddRangeFrom(child.children, m_stateClient.Metadata.BucketLength / 2, (m_stateClient.Metadata.BucketLength / 2) + 1);

            var splitKey = child.keys.Get(m_stateClient.Metadata.BucketLength / 2 - 1);

            // Insert the split key to parent before removing from child since the removal from child can clear memory.
            parent.EnterWriteLock();
            parent.keys.Insert_Internal(index, splitKey);
            parent.children.InsertAt(index + 1, newNodeId);
            parent.ExitWriteLock();

            child.EnterWriteLock();
            child.keys.RemoveRange(m_stateClient.Metadata.BucketLength / 2 - 1, m_stateClient.Metadata.BucketLength / 2 + 1);
            child.children.RemoveRange(m_stateClient.Metadata.BucketLength / 2, m_stateClient.Metadata.BucketLength / 2 + 1);
            child.ExitWriteLock();

            return (newNode, splitKey);
        }
    }
}
