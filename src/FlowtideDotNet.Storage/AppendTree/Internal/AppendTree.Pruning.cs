//// Licensed under the Apache License, Version 2.0 (the "License")
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////  
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//using FlowtideDotNet.Storage.Tree.Internal;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace FlowtideDotNet.Storage.AppendTree.Internal
//{
//    internal partial class AppendTree<K, V>
//    {
//        /// <summary>
//        /// Prunes the tree until this key.
//        /// </summary>
//        /// <param name="key"></param>
//        /// <returns></returns>
//        public async ValueTask Prune(K key)
//        {
//            var rootNode = await GetChildNode(m_stateClient.Metadata!.Root);

//            await Prune_AfterGetRoot(rootNode, key);
//        }

//        /// <summary>
//        /// Prunes the tree of data that is before the key.
//        /// It only prunes if an entire leaf node can be removed. This is done because it then does not need to load the data into RAM
//        /// to iterate over the leaf node which could contain alot of data.
//        /// </summary>
//        /// <param name="rootNode"></param>
//        /// <param name="key"></param>
//        /// <returns></returns>
//        private async ValueTask Prune_AfterGetRoot(IBPlusTreeNode? rootNode, K key)
//        {
//            // We only prune if entire l
//            if (rootNode is LeafNode<K, V> leafNode)
//            {
//                leafNode.EnterWriteLock();
//                var index = leafNode.keys.BinarySearch(key, m_keyComparer);
//                if (index < 0)
//                {
//                    index = ~index;
//                }
//                leafNode.keys.RemoveRange(0, index);
//                leafNode.values.RemoveRange(0, index);
//                leafNode.ExitWriteLock();
//            }
//            else if (rootNode is InternalNode<K, V> internalNode)
//            {
//                int i = await TryPruneInternalNode(internalNode, key, 0, false);
//                if (i > 0)
//                {
//                    internalNode.EnterWriteLock();
//                    internalNode.keys.RemoveRange(0, i);
//                    internalNode.children.RemoveRange(0, i);
//                    internalNode.ExitWriteLock();
//                }
//                if (internalNode.children.Count == 0)
//                {
//                    var newRoot = new LeafNode<K, V>(m_stateClient.GetNewPageId());
//                    m_stateClient.Metadata!.Root = newRoot.Id;
//                    m_stateClient.Delete(internalNode.Id);
//                    m_stateClient.AddOrUpdate(newRoot.Id, newRoot);
//                }
//                else if (internalNode.children.Count == 1)
//                {
//                    m_stateClient.Metadata!.Root = internalNode.children[0];
//                    m_stateClient.Delete(internalNode.Id);
//                    m_rightInternalNodes.RemoveAt(0);
//                    var newRootId = internalNode.children[0];
//                    while (true)
//                    {
//                        // Loop to clean up any single child internal nodes
//                        var newRoot = await GetChildNode(newRootId);
//                        if (newRoot is InternalNode<K, V> newRootInternal &&
//                            newRootInternal.children.Count == 1)
//                        {
//                            newRootId = newRootInternal.children[0];
//                            m_stateClient.Metadata!.Root = newRootId;
//                            m_stateClient.Delete(newRootInternal.Id);
//                            m_rightInternalNodes.RemoveAt(0);
//                        }
//                        else
//                        {
//                            break;
//                        }
//                    }
//                }
//                else
//                {
//                    m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
//                }
//            }
//        }

//        private async Task<int> TryPruneInternalNode(InternalNode<K, V> internalNode, K key, int depth, bool pruneAll)
//        {
//            // Check if we are at the depth that all children are leaf nodes
//            if (depth == m_rightInternalNodes.Count - 1)
//            {
//                int i = 0;
//                if (pruneAll)
//                {
//                    for (; i < internalNode.children.Count; i++)
//                    {
//                        m_stateClient.Delete(internalNode.children[i]);
//                    }
//                    return i;
//                }
//                else
//                {
//                    for (; i < internalNode.keys.Count; i++)
//                    {
//                        if (m_keyComparer.Compare(internalNode.keys[i], key) <= 0)
//                        {
//                            m_stateClient.Delete(internalNode.children[i]);
//                        }
//                        else
//                        {
//                            break;
//                        }
//                    }
//                    return i;
//                }
//            }
//            else
//            {
//                int i = 0;
//                for (; i < internalNode.children.Count; i++)
//                {
//                    var childNode = await GetChildNode(internalNode.children[i]);
//                    if (pruneAll)
//                    {
//                        await Prune_internalNode(childNode, key, depth + 1, pruneAll);
//                        continue;
//                    }
//                    // Check if all leafs can be pruned in this tree
//                    if (i < internalNode.keys.Count &&
//                        m_keyComparer.Compare(internalNode.keys[i], key) <= 0)
//                    {
//                        await Prune_internalNode(childNode, key, depth + 1, true);
//                    }
//                    else
//                    {
//                        if (!await Prune_internalNode(childNode, key, depth + 1, false))
//                        {
//                            break;
//                        }
//                    }
//                }
//                return i;
//            }
//        }

//        /// <summary>
//        /// Returns true if the node is pruned completely
//        /// </summary>
//        /// <param name="node"></param>
//        /// <param name="key"></param>
//        /// <returns></returns>
//        private async ValueTask<bool> Prune_internalNode(IBPlusTreeNode? node, K key, int depth, bool pruneAll)
//        {
//            if (node is LeafNode<K, V> leafNode)
//            {
//                // This code should not be reached, but it is here for completeness, if one wants to prune inside of a leaf node
//                leafNode.EnterWriteLock();
//                var index = leafNode.keys.BinarySearch(key, m_keyComparer);
//                if (index < 0)
//                {
//                    index = ~index;
//                }
//                leafNode.keys.RemoveRange(0, index);
//                leafNode.values.RemoveRange(0, index);
//                leafNode.ExitWriteLock();

//                if (leafNode.keys.Count == 0)
//                {
//                    m_stateClient.Delete(leafNode.Id);
//                    return true;
//                }
//                else
//                {
//                    m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
//                }
//                return false;
//            }
//            else if (node is InternalNode<K, V> internalNode)
//            {
//                int i = await TryPruneInternalNode(internalNode, key, depth, pruneAll);
//                if (i > 0)
//                {
//                    internalNode.EnterWriteLock();
//                    internalNode.keys.RemoveRange(0, Math.Min(i, internalNode.keys.Count));
//                    internalNode.children.RemoveRange(0, i);
//                    internalNode.ExitWriteLock();
//                }
//                if (internalNode.children.Count == 0)
//                {
//                    m_stateClient.Delete(internalNode.Id);
//                    return true;
//                }
//                else
//                {
//                    m_stateClient.AddOrUpdate(internalNode.Id, internalNode);
//                }
//                return false;
//            }
//            throw new NotImplementedException();
//        }
//    }
//}
