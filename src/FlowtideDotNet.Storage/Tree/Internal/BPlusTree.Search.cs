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
    internal partial class BPlusTree<K, V>
    {
        public ValueTask<LeafNode<K, V>> LeftLeaf()
        {
            var getFirstTask = m_stateClient.GetValue(m_stateClient.Metadata.Left, "Leftleaf");

            if (!getFirstTask.IsCompleted)
            {
                return LeftLeaf_Slow(getFirstTask);
            }
            return ValueTask.FromResult((getFirstTask.Result as LeafNode<K, V>)!);
        }

        private async ValueTask<LeafNode<K, V>> LeftLeaf_Slow(ValueTask<IBPlusTreeNode?> getNodeTask)
        {
            var node = await getNodeTask;
            return (node as LeafNode<K, V>)!;
        }


        public async ValueTask<LeafNode<K, V>> SearchRoot(K key, IComparer<K> searchComparer)
        {
            Debug.Assert(m_stateClient.Metadata != null);

            var root = await m_stateClient.GetValue(m_stateClient.Metadata.Root, "SearchRoot");

            return await SearchLeafNodeForRead_AfterTask(key, root, searchComparer);
        }

        private ValueTask<LeafNode<K, V>> SearchLeafNodeForRead_AfterTask(in K key, in IBPlusTreeNode node, in IComparer<K> searchComparer)
        {
            if (node is LeafNode<K, V> leaf)
            {
                return new ValueTask<LeafNode<K, V>>(leaf);
            }
            else if (node is InternalNode<K, V> parentNode)
            {
                return SearchLeafNodeForReadInternal(key, parentNode, searchComparer);
            }
            throw new NotImplementedException();
        }

        private async ValueTask<LeafNode<K, V>> SearchLeafNodeForReadInternal(K key, InternalNode<K, V> node, IComparer<K> searchComparer)
        {
            var index = node.keys.BinarySearch(key, searchComparer);
            if (index < 0)
            {
                index = ~index;
            }
            var child = node.children[index];
            var childNode = await m_stateClient.GetValue(child, "SearchLeafNodeForReadInternal");
            return await SearchLeafNodeForRead_AfterTask(key, childNode, searchComparer);
        }
    }
}
