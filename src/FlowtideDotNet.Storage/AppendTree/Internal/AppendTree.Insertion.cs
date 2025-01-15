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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    internal partial class AppendTree<K, V, TKeyContainer, TValueContainer>
    {
        public ValueTask Append(in K key, in V value)
        {
            return Append_AfterFetchRightNode(key, value);
        }

        private async ValueTask Append_AwaitGetRightNode(ValueTask<IBPlusTreeNode?> rightNodeTask, K key, V value)
        {
            var rightNode = await rightNodeTask;
            m_rightNode = (rightNode as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            await Append_AfterFetchRightNode(key, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask Append_AfterFetchRightNode(in K key, in V value)
        {
            if (m_rightNode!.keys.Count > 0 && m_keyComparer.CompareTo(m_rightNode!.keys.Get(m_rightNode.keys.Count - 1), key) >= 0)
            {
                throw new InvalidOperationException("Key must be greater than the last key in the right node.");
            }
            if (m_rightNode!.keys.Count < m_bucketSize)
            {
                // Locking is done insert of insert at.
                m_rightNode.InsertAt(key, value, m_rightNode.keys.Count);
                return ValueTask.CompletedTask;
            }

            // Append from the top
            return AppendFullLeaf(key, value);
        }

        private async ValueTask AppendFullLeaf(K key, V value)
        {
            Debug.Assert(m_rightNode != null);
            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var emptyValues = m_options.ValueSerializer.CreateEmpty();
            var newRightNode = new LeafNode<K, V, TKeyContainer, TValueContainer>(m_stateClient.GetNewPageId(), emptyKeys, emptyValues);
            newRightNode.InsertAt(key, value, 0);
            m_rightNode.EnterWriteLock();
            m_rightNode.next = newRightNode.Id;
            m_rightNode.ExitWriteLock();
            m_stateClient.AddOrUpdate(m_rightNode.Id, m_rightNode);

            var parentKey = m_rightNode.keys.Get(m_rightNode.keys.Count - 1);

            var node = (BaseNode<K, TKeyContainer>)m_rightNode;
            var newNode = (BaseNode<K, TKeyContainer>)newRightNode;

            // Set right node returns the previous node
            SetRightNode(newRightNode);

            bool updatedParent = true;
            // Iterate over the internal nodes but not the root node since that requires special handling.
            for (int i = m_rightInternalNodes.Count - 1; i > 0; i--)
            {
                var internalNodeId = m_rightInternalNodes[i];
                var internalNode = (await GetChildNode(internalNodeId) as InternalNode<K, V, TKeyContainer>)!;
                if (internalNode.keys.Count < m_bucketSize)
                {
                    internalNode.EnterWriteLock();
                    internalNode.keys.Add(parentKey);
                    internalNode.children.Add(newNode.Id);
                    internalNode.ExitWriteLock();
                    var isFull = m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
                    if (isFull)
                    {
                        await m_stateClient.WaitForNotFullAsync();
                    }
                    internalNode.Return();
                    updatedParent = false;
                    break;
                }
                else
                {
                    // Create an internal node that has no keys and only one child
                    var newInternalNode = new InternalNode<K, V, TKeyContainer>(m_stateClient.GetNewPageId(), m_options.KeySerializer.CreateEmpty(), m_options.MemoryAllocator);
                    newInternalNode.children.Add(newNode.Id);

                    // Replace the most right node at this level
                    m_rightInternalNodes[i] = newInternalNode.Id;
                    m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
                    m_stateClient.AddOrUpdate(newInternalNode.Id, newInternalNode);
                    if (node.Id != m_rightNode.Id)
                    {
                        // If the node is not the most right node, return it
                        node.Return();
                    }
                    node = internalNode;
                    newNode = newInternalNode;
                    updatedParent = true;
                }
            }

            if (node.Id != m_rightNode.Id)
            {
                node.Return();
            }

            if (m_rightInternalNodes.Count > 0 && updatedParent)
            {
                // Handle the root node
                var rootNodeId = m_rightInternalNodes[0];
                var rootNode = (await GetChildNode(rootNodeId) as InternalNode<K, V, TKeyContainer>)!;
                if (rootNode.keys.Count < m_bucketSize)
                {
                    rootNode.EnterWriteLock();
                    rootNode.keys.Add(parentKey);
                    rootNode.children.Add(newNode.Id);
                    rootNode.ExitWriteLock();
                    var isFull = m_stateClient.AddOrUpdate(rootNode.Id, rootNode);
                    if (isFull)
                    {
                        await m_stateClient.WaitForNotFullAsync();
                    }
                    rootNode.Return();
                }
                else
                {
                    // Create a new root node
                    var newRoot = new InternalNode<K, V, TKeyContainer>(m_stateClient.GetNewPageId(), m_options.KeySerializer.CreateEmpty(), m_options.MemoryAllocator);
                    var intermediateNode = new InternalNode<K, V, TKeyContainer>(m_stateClient.GetNewPageId(), m_options.KeySerializer.CreateEmpty(), m_options.MemoryAllocator);
                    intermediateNode.children.Add(newNode.Id);
                    m_rightInternalNodes[0] = intermediateNode.Id;

                    newRoot.keys.Add(parentKey);
                    newRoot.children.Add(rootNode.Id);
                    newRoot.children.Add(intermediateNode.Id);

                    m_rightInternalNodes.Insert(0, newRoot.Id);

                    m_stateClient.Metadata!.Root = newRoot.Id;
                    m_stateClient.AddOrUpdate(rootNode.Id, rootNode);
                    m_stateClient.AddOrUpdate(newRoot.Id, newRoot);
                    m_stateClient.AddOrUpdate(intermediateNode.Id, intermediateNode);
                    rootNode.Return();
                }
            }
            else if (m_rightInternalNodes.Count == 0)
            {
                // Create new root node
                var newRootNode = new InternalNode<K, V, TKeyContainer>(m_stateClient.GetNewPageId(), m_options.KeySerializer.CreateEmpty(), m_options.MemoryAllocator);
                newRootNode.keys.Add(parentKey);
                newRootNode.children.Add(node.Id);
                newRootNode.children.Add(newNode.Id);

                m_stateClient.Metadata!.Root = newRootNode.Id;
                m_stateClient.AddOrUpdate(newRootNode.Id, newRootNode);
                m_rightInternalNodes.Add(newRootNode.Id);
            }
        }
    }
}
