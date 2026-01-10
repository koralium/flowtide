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
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    /// <summary>
    /// Contains the logic for searching the AppendTree.
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    internal partial class AppendTree<K, V, TKeyContainer, TValueContainer>
    {

        internal ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> FindLeafNode(in K key, in IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            Debug.Assert(m_rightNode != null);
            if (m_stateClient.Metadata!.Root == m_rightNode.Id && m_rightNode.keys.Count == 0)
            {
                // If the tree is empty, return the right node which is the root
                return new ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>>(m_rightNode);
            }
            // Check if the most right node contains the data, if so no need to search the tree
            if (searchComparer.CompareTo(m_rightNode.keys.Get(0), key) <= 0)
            {
                return new ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>>(m_rightNode);
            }
            // Might be better here to start a search with the most right internal nodes, since they are more likely to be in the cache

            var rootTask = GetChildNode(m_stateClient.Metadata!.Root);

            if (!rootTask.IsCompletedSuccessfully)
            {
                return FindLeafNode_Slow(key, searchComparer, rootTask);
            }

            return SearchLeafNodeForRead_AfterTask(key, rootTask.Result!, searchComparer);
        }

        private async ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> FindLeafNode_Slow(K key, IBplusTreeComparer<K, TKeyContainer> searchComparer, ValueTask<IBPlusTreeNode?> task)
        {
            var root = await task;
            return await SearchLeafNodeForRead_AfterTask(key, root!, searchComparer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchLeafNodeForReadInternal(in K key, in InternalNode<K, V, TKeyContainer> node, in IBplusTreeComparer<K, TKeyContainer> searchComparer)
        {
            // Must enter lock before accessing keys and children.
            node.EnterWriteLock();
            var index = searchComparer.FindIndex(key, node.keys);
            if (index < 0)
            {
                index = ~index;
            }
            var child = node.children[index];
            node.ExitWriteLock();
            node.Return();
            var childNodeTask = GetChildNode(child);

            if (!childNodeTask.IsCompletedSuccessfully)
            {
                return SearchLeafNodeForReadInternal_Slow(key, searchComparer, childNodeTask);
            }

            return SearchLeafNodeForRead_AfterTask(key, childNodeTask.Result!, searchComparer);
        }

        private async ValueTask<LeafNode<K, V, TKeyContainer, TValueContainer>> SearchLeafNodeForReadInternal_Slow(K key, IBplusTreeComparer<K, TKeyContainer> searchComparer, ValueTask<IBPlusTreeNode?> task)
        {
            var childNode = await task;
            return await SearchLeafNodeForRead_AfterTask(key, childNode!, searchComparer);
        }
    }
}
