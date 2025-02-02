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
            return  SearchLeafNodeForRead_AfterTask(key, rootTask.Result!, searchComparer);
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
    }
}
