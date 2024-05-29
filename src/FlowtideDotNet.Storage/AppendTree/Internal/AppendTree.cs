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

using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    internal partial class AppendTree<K, V> : IAppendTree<K, V>
    {
        internal readonly IStateClient<IBPlusTreeNode, AppendTreeMetadata> m_stateClient;
        private readonly BPlusTreeOptions<K, V> m_options;
        internal IComparer<K> m_keyComparer;
        private int minSize;
        private LeafNode<K, V>? m_rightNode;
        private int m_bucketSize;
        private List<InternalNode<K, V>> m_rightInternalNodes;

        public AppendTree(IStateClient<IBPlusTreeNode, AppendTreeMetadata> stateClient, BPlusTreeOptions<K, V> options)
        {
            Debug.Assert(options.BucketSize.HasValue);
            this.m_stateClient = stateClient;
            this.m_options = options;
            minSize = options.BucketSize.Value / 3;
            m_bucketSize = options.BucketSize.Value;
            this.m_keyComparer = options.Comparer;
            m_rightInternalNodes = new List<InternalNode<K, V>>();
        }

        public async Task InitializeAsync()
        {
            Debug.Assert(m_options.BucketSize.HasValue);
            if (m_stateClient.Metadata == null)
            {
                var rootId = m_stateClient.GetNewPageId();
                var root = new LeafNode<K, V>(rootId);
                m_stateClient.Metadata = new AppendTreeMetadata()
                {
                    Root = rootId,
                    BucketLength = m_options.BucketSize.Value,
                    Left = rootId,
                    Right = rootId
                };
                m_stateClient.AddOrUpdate(rootId, root);
                m_rightNode = root;
            }
            else
            {
                // Fill up the right internal nodes
                m_rightInternalNodes.Clear();
                await CreateInternalNodesList(m_stateClient.Metadata.Root);
            }
        }

        private async ValueTask CreateInternalNodesList(long id)
        {
            if (m_stateClient.Metadata!.Right == id)
            {
                return;
            }
            var node = await GetChildNode(id);

            if (node is InternalNode<K, V> internalNode)
            {
                m_rightInternalNodes.Add(internalNode);
                await CreateInternalNodesList(internalNode.children[internalNode.children.Count - 1]);
            }
        }

        public ValueTask Commit()
        {
            if (m_rightNode != null)
            {
                m_stateClient.AddOrUpdate(m_rightNode.Id, m_rightNode);
            }
            return m_stateClient.Commit();
        }

        public async Task<string> Print()
        {
            Debug.Assert(m_stateClient.Metadata != null);
            var root = (BaseNode<K>)(await GetChildNode(m_stateClient.Metadata.Root))!;

            var builder = new StringBuilder();
            builder.AppendLine("digraph g {");
            builder.AppendLine("splines=line");
            builder.AppendLine("node [shape = none,height=.1];");
            await root.Print(builder, async (id) => (BaseNode<K>)(await GetChildNode(id))!);
            builder.AppendLine("}");
            return builder.ToString();
        }

        public IAppendTreeIterator<K, V> CreateIterator()
        {
            return new AppendTreeIterator<K, V>(this);
        }

        internal async ValueTask<IBPlusTreeNode?> GetChildNode(long id)
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
            return await m_stateClient.GetValue(id, "");
        }

        public async ValueTask Clear()
        {
            Debug.Assert(m_options.BucketSize.HasValue);
            // Clear the current state from the state storage
            await m_stateClient.Reset(true);

            // Create a new root leaf
            var rootId = m_stateClient.GetNewPageId();
            var root = new LeafNode<K, V>(rootId);
            m_stateClient.Metadata = new AppendTreeMetadata()
            {
                Root = rootId,
                BucketLength = m_options.BucketSize.Value,
                Left = rootId,
                Right = rootId
            };
            m_rightNode = root;
            m_rightInternalNodes.Clear();
            m_stateClient.AddOrUpdate(rootId, root);
        }
    }
}
