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
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal partial class BPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        public ValueTask SavePage(LeafNode<K, V, TKeyContainer, TValueContainer> leafNode, List<BPlusTreeNodeIndex> nodePath)
        {
            var byteSize = leafNode.GetByteSize();

            if (byteSize > m_stateClient.Metadata!.PageSizeBytes && leafNode.keys.Count >= minPageSizeBeforeSplit ||
                (byteSize <= byteMinSize || leafNode.keys.Count < minPageSize))
            {
                return SavePageLeaf(leafNode, nodePath, byteSize);
            }
            else
            {
                m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask SavePageLeaf(LeafNode<K, V, TKeyContainer, TValueContainer> leafNode, List<BPlusTreeNodeIndex> nodePath, int byteSize)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            // Check if the leaf node is a root node
            if (nodePath.Count == 1)
            {
                if (byteSize > m_stateClient.Metadata!.PageSizeBytes && leafNode.keys.Count >= minPageSizeBeforeSplit)
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

                    if (m_usePreviousPointer && newNode.next != 0)
                    {
                        await UpdateRightPrevious(GenericWriteOperation.Upsert, newNode, isFull, false);
                        return;
                    }
                    if (isFull)
                    {
                        await GenericWrite_SlowUpsert(GenericWriteOperation.Upsert, m_stateClient.WaitForNotFullAsync());
                        return;
                    }
                }
            }
            else
            {
                // Check that the leaf node is too big or too small, then run it
                if (
                    (byteSize > m_stateClient.Metadata!.PageSizeBytes && leafNode.keys.Count >= minPageSizeBeforeSplit) ||
                    (byteSize <= byteMinSize || leafNode.keys.Count < minPageSize))
                {
                    var leafParentNodeId = nodePath[nodePath.Count - 2];
                    var LeafParentNode = ((await m_stateClient.GetValue(leafParentNodeId.NodeId)) as InternalNode<K, V, TKeyContainer>)!;

                    await Updater_Internal_Leaf(leafParentNodeId.ChildIndex, leafNode, LeafParentNode);

                    InternalNode<K, V, TKeyContainer> internalNode = LeafParentNode;
                    
                    // Check parent nodes
                    // TODO: This code must check if the root node is being updated or not, to update the root index
                    for (int i = nodePath.Count - 3; i >= -1; i--)
                    {
                        byteSize = internalNode.GetByteSize();

                        if (
                           (byteSize > m_stateClient.Metadata!.PageSizeBytes && internalNode.keys.Count > minPageSizeBeforeSplit) ||
                           (byteSize <= byteMinSize || internalNode.keys.Count < minPageSize))
                        {
                            if (i < 0)
                            {
                                await GenericWriteRoot_AfterInternalByteBased(GenericWriteOperation.Upsert, in internalNode);
                            }
                            else
                            {
                                var parentNodeId = nodePath[i];
                                var parentNode = ((await m_stateClient.GetValue(parentNodeId.NodeId)) as InternalNode<K, V, TKeyContainer>)!;

                                await Updater_Internal_InternalNode(parentNodeId.ChildIndex, internalNode, parentNode);
                                internalNode = parentNode;
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
        }

        private ValueTask<GenericWriteOperation> Updater_Internal_Leaf(
            in int index,
            in LeafNode<K, V, TKeyContainer, TValueContainer> leafNode,
            in InternalNode<K, V, TKeyContainer> parentNode
            )
        {
            Debug.Assert(m_stateClient.Metadata != null);
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

                if (m_usePreviousPointer && newNode.next != 0)
                {
                    return UpdateRightPrevious(GenericWriteOperation.Upsert, newNode, isFull, false);
                }

                if (isFull)
                {
                    return GenericWrite_SlowUpsert(GenericWriteOperation.Upsert, m_stateClient.WaitForNotFullAsync());
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
                        return GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_SlowGetLeftByteBased(getLeftNodeTask, leafNode, parentNode, index, GenericWriteOperation.Upsert);
                    }
                    var leftNode = (getLeftNodeTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;

                    return GenericWrite_Internal_AfterGetNode_LeafNodeTooSmall_AfterGetLeftByteBased(in leftNode, in leafNode, in parentNode, in index, GenericWriteOperation.Upsert);
                }
                else
                {
                    // Merge with right neighbor
                    var rightNeighborId = parentNode.children[index + 1];
                    var getRightNodeTask = m_stateClient.GetValue(rightNeighborId);
                    if (!getRightNodeTask.IsCompletedSuccessfully)
                    {
                        return GenericWrite_Internal_AfterGetNode_NodeTooSmall_SlowGetRightByteBased(getRightNodeTask, leafNode, parentNode, index, GenericWriteOperation.Upsert);
                    }
                    var rightNode = (getRightNodeTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;

                    return GenericWrite_Internal_AfterGetNode_NodeTooSmall_AfterGetRightByteBased(in rightNode, in leafNode, in parentNode, in index, GenericWriteOperation.Upsert);
                }
            }
            else
            {
                var isFull = m_stateClient.AddOrUpdate(leafNode.Id, leafNode);

                if (isFull)
                {
                    return GenericWrite_SlowUpsert(GenericWriteOperation.Upsert, m_stateClient.WaitForNotFullAsync());
                }
            }
            return ValueTask.FromResult(GenericWriteOperation.Upsert);
        }

        private ValueTask<GenericWriteOperation> Updater_Internal_InternalNode(
            in int index,
            in InternalNode<K, V, TKeyContainer> child,
            in InternalNode<K, V, TKeyContainer> parentNode
            )
        {
            return GenericWrite_Internal_AfterGetNode_AfterCallInternalByteBased(in child, in parentNode, in index, GenericWriteOperation.Upsert);
        }
    }
}
