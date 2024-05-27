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
    internal partial class AppendTree<K, V>
    {
        public ValueTask Append(in K key, in V value)
        {
            if (m_rightNode == null)
            {
                var rightNodeTask = m_stateClient.GetValue(m_stateClient.Metadata!.Right, "");
                if (rightNodeTask.IsCompletedSuccessfully)
                {
                    m_rightNode = (rightNodeTask.Result as LeafNode<K, V>)!;
                }
                else
                {
                    return Append_AwaitGetRightNode(rightNodeTask, key, value);
                }
            }
            
            return Append_AfterFetchRightNode(key, value);
        }

        private async ValueTask Append_AwaitGetRightNode(ValueTask<IBPlusTreeNode?> rightNodeTask, K key, V value)
        {
            var rightNode = await rightNodeTask;
            m_rightNode = (rightNode as LeafNode<K, V>)!;
            await Append_AfterFetchRightNode(key, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask Append_AfterFetchRightNode(in K key, in V value)
        {
            if (m_rightNode.keys.Count > 0 && m_keyComparer.Compare(m_rightNode!.keys[m_rightNode.keys.Count -1], key) >= 0)
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

        private ValueTask AppendFullLeaf(in K key, in V value)
        {
            Debug.Assert(m_rightNode != null);
            var newRightNode = new LeafNode<K, V>(m_stateClient.GetNewPageId());
            newRightNode.InsertAt(key, value, 0);
            m_rightNode.EnterWriteLock();
            m_rightNode.next = newRightNode.Id;
            m_rightNode.ExitWriteLock();
            m_stateClient.AddOrUpdate(m_rightNode.Id, m_rightNode);

            var parentKey = m_rightNode.keys[m_rightNode.keys.Count - 1];

            var node = (BaseNode<K>)m_rightNode;
            var newNode = (BaseNode<K>)newRightNode;

            m_rightNode = newRightNode;
            m_stateClient.Metadata!.Right = newRightNode.Id;

            bool updatedParent = true;
            // Iterate over the internal nodes but not the root node since that requires special handling.
            for (int i = m_rightInternalNodes.Count -1; i > 0; i--)
            {
                var internalNode = m_rightInternalNodes[i];
                if (internalNode.keys.Count < m_bucketSize)
                {
                    internalNode.EnterWriteLock();
                    internalNode.keys.Add(parentKey);
                    internalNode.children.Add(newNode.Id);
                    internalNode.ExitWriteLock();
                    updatedParent = false;
                    break;
                }
                else
                {
                    // Create an internal node that has no keys and only one child
                    var newInternalNode = new InternalNode<K, V>(m_stateClient.GetNewPageId());
                    newInternalNode.children.Add(newNode.Id);

                    // Replace the most right node at this level
                    m_rightInternalNodes[i] = newInternalNode;
                    m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
                    m_stateClient.AddOrUpdate(newInternalNode.Id, newInternalNode);
                    node = internalNode;
                    newNode = newInternalNode;
                    updatedParent = true;
                }
            }

            if (m_rightInternalNodes.Count > 0 && updatedParent)
            {
                // Handle the root node
                var rootNode = m_rightInternalNodes[0];
                if (rootNode.keys.Count < m_bucketSize)
                {
                    rootNode.EnterWriteLock();
                    rootNode.keys.Add(parentKey);
                    rootNode.children.Add(newNode.Id);
                    rootNode.ExitWriteLock();
                }
                else
                {
                    // Create a new root node
                    var newRoot = new InternalNode<K, V>(m_stateClient.GetNewPageId());
                    var intermediateNode = new InternalNode<K, V>(m_stateClient.GetNewPageId());
                    intermediateNode.children.Add(newNode.Id);
                    m_rightInternalNodes[0] = intermediateNode;
                    
                    newRoot.keys.Add(parentKey);
                    newRoot.children.Add(rootNode.Id);
                    newRoot.children.Add(intermediateNode.Id);

                    m_rightInternalNodes.Insert(0, newRoot);
                    
                    m_stateClient.Metadata!.Root = newRoot.Id;
                    m_stateClient.AddOrUpdate(rootNode.Id, rootNode);
                    m_stateClient.AddOrUpdate(newRoot.Id, newRoot);
                    m_stateClient.AddOrUpdate(intermediateNode.Id, intermediateNode);
                }
            }
            else if (m_rightInternalNodes.Count == 0)
            {
                // Create new root node
                var newRootNode = new InternalNode<K, V>(m_stateClient.GetNewPageId());
                newRootNode.keys.Add(parentKey);
                newRootNode.children.Add(node.Id);
                newRootNode.children.Add(newNode.Id);

                m_stateClient.Metadata!.Root = newRootNode.Id;
                m_stateClient.AddOrUpdate(newRootNode.Id, newRootNode);
                m_rightInternalNodes.Add(newRootNode);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask<IBPlusTreeNode?> GetChildNode(long id)
        {
            // Must always check if it is the right node since it is not commited to state before full.
            if (id == m_stateClient.Metadata!.Right)
            {
                if (m_rightNode == null)
                {
                    m_rightNode = (await m_stateClient.GetValue(id, "")) as LeafNode<K, V>;
                }
                return m_rightNode!;
            }
            try
            {
                return await m_stateClient.GetValue(id, "");
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
