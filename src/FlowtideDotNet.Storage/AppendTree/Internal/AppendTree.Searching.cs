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

using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    /// <summary>
    /// Contains the logic for searching the AppendTree.
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    internal partial class AppendTree<K, V>
    {

        internal async ValueTask<LeafNode<K, V>> FindLeafNode(K key, IComparer<K> searchComparer)
        {
            var root = await m_stateClient.GetValue(m_stateClient.Metadata!.Root, "SearchRoot");

            return await SearchLeafNodeForRead_AfterTask(key, root!, searchComparer);
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
            return await SearchLeafNodeForRead_AfterTask(key, childNode!, searchComparer);
        }
    }
}
