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
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Diagnostics;
using System.Text;

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    internal partial class AppendTree<K, V, TKeyContainer, TValueContainer> : IAppendTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        internal readonly IStateClient<IBPlusTreeNode, AppendTreeMetadata> m_stateClient;
        private readonly BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> m_options;
        internal IBplusTreeComparer<K, TKeyContainer> m_keyComparer;
        private LeafNode<K, V, TKeyContainer, TValueContainer>? m_rightNode;
        private readonly int m_bucketSize;
        private List<long> m_rightInternalNodes;

        public AppendTree(IStateClient<IBPlusTreeNode, AppendTreeMetadata> stateClient, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
        {
            Debug.Assert(options.BucketSize.HasValue);
            this.m_stateClient = stateClient;
            this.m_options = options;
            m_bucketSize = options.BucketSize.Value;
            this.m_keyComparer = options.Comparer;
            m_rightInternalNodes = new List<long>();
        }

        public async Task InitializeAsync()
        {
            Debug.Assert(m_options.BucketSize.HasValue);
            if (m_stateClient.Metadata == null)
            {
                var rootId = m_stateClient.GetNewPageId();
                var emptyKeys = m_options.KeySerializer.CreateEmpty();
                var emptyValues = m_options.ValueSerializer.CreateEmpty();
                var root = new LeafNode<K, V, TKeyContainer, TValueContainer>(rootId, emptyKeys, emptyValues);
                m_stateClient.Metadata = new AppendTreeMetadata()
                {
                    Root = rootId,
                    BucketLength = m_options.BucketSize.Value,
                    Left = rootId,
                    Right = rootId
                };
                m_stateClient.AddOrUpdate(rootId, root);
                m_rightNode = root;
                m_rightNode.TryRent();
            }
            else
            {
                m_rightNode = (await m_stateClient.GetValue(m_stateClient.Metadata.Right)) as LeafNode<K, V, TKeyContainer, TValueContainer>;
                m_rightNode!.TryRent();
                m_rightInternalNodes.Clear();
                await CreateInternalNodesList(m_stateClient.Metadata.Root);
            }
        }

        internal void ReturnNode(IBPlusTreeNode? node)
        {
            if (node != null && node.Id != m_rightNode!.Id)
            {
                node.Return();
            }
        }

        private async ValueTask CreateInternalNodesList(long id)
        {
            if (m_stateClient.Metadata!.Right == id)
            {
                return;
            }
            var node = await GetChildNode(id);

            if (node is InternalNode<K, V, TKeyContainer> internalNode)
            {
                m_rightInternalNodes.Add(internalNode.Id);
                await CreateInternalNodesList(internalNode.children[internalNode.children.Count - 1]);
            }
        }


        private void SetRightNode(LeafNode<K, V, TKeyContainer, TValueContainer> node)
        {
            node.TryRent();
            var previous = m_rightNode;
            m_rightNode = node;
            m_stateClient.Metadata!.Right = node.Id;
            // The previous right node is returned in the insertion loop
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
            var root = (BaseNode<K, TKeyContainer>)(await GetChildNode(m_stateClient.Metadata.Root))!;

            var builder = new StringBuilder();
            builder.AppendLine("digraph g {");
            builder.AppendLine("splines=line");
            builder.AppendLine("node [shape = none,height=.1];");
            await root.Print(builder, async (id) => (BaseNode<K, TKeyContainer>)(await GetChildNode(id))!);
            builder.AppendLine("}");
            return builder.ToString();
        }

        public IAppendTreeIterator<K, V, TKeyContainer> CreateIterator()
        {
            return new AppendTreeIterator<K, V, TKeyContainer, TValueContainer>(this);
        }

        internal async ValueTask<IBPlusTreeNode?> GetChildNode(long id)
        {
            // Must always check if it is the right node since it is not commited to state before full.
            if (id == m_stateClient.Metadata!.Right)
            {
                return m_rightNode!;
            }
            return await m_stateClient.GetValue(id);
        }

        public async ValueTask Clear()
        {
            Debug.Assert(m_options.BucketSize.HasValue);
            // Clear the current state from the state storage
            await m_stateClient.Reset(true);

            // Create a new root leaf
            var rootId = m_stateClient.GetNewPageId();
            var emptyKeys = m_options.KeySerializer.CreateEmpty();
            var emptyValues = m_options.ValueSerializer.CreateEmpty();
            var root = new LeafNode<K, V, TKeyContainer, TValueContainer>(rootId, emptyKeys, emptyValues);
            m_stateClient.Metadata = new AppendTreeMetadata()
            {
                Root = rootId,
                BucketLength = m_options.BucketSize.Value,
                Left = rootId,
                Right = rootId
            };
            m_rightNode = root;
            m_stateClient.AddOrUpdate(rootId, root);
        }
    }
}
