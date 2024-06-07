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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal partial class BPlusTree<K, V>
    {
        internal struct SplitInfo
        {
            public IBPlusTreeNode? parent;
            public IBPlusTreeNode child;
            public int index;
        }

        List<Task> waitList = new List<Task>();
        Dictionary<long, SplitInfo> splitList = new Dictionary<long, SplitInfo>();

        public async ValueTask BulkWriteSortedData(List<KeyValuePair<K, V>> items)
        {
            var root = await m_stateClient.GetValue(m_stateClient.Metadata!.Root, "SearchRoot");

            waitList.Clear();
            splitList.Clear();
            BulkWriteSortedData_Node(waitList, splitList, items, root!, default, 0, 0, items.Count);

            // Wait for all inserts to complete
            if (waitList.Count > 0)
            {
                await Task.WhenAll(waitList);
            }
            if (splitList.Count > 0)
            {
                if (splitList.Count > 1)
                {

                }
                // Handle all splits
                await HandleSplits();
            }
        }

        private async Task HandleSplits()
        {
            var split = splitList.First();
            await SplitNode(split.Value);
            //foreach (var split in splitList)
            //{
            //    await SplitNode(split.Value);
            //}
            //for (int i = 0; i < splitList.Count; i++)
            //{
            //    var splitInfo = splitList[i];

            //}
        }

        private async Task SplitNode(SplitInfo splitInfo)
        {
            if (splitInfo.parent == null)
            {
                // Root node
                if (splitInfo.child is LeafNode<K, V> leafNode)
                {
                    var nextId = m_stateClient.GetNewPageId();
                    var newParentNode = new InternalNode<K, V>(nextId);

                    // No lock required
                    newParentNode.children.Insert(0, leafNode.Id);
                    m_stateClient.Metadata!.Root = nextId;

                    var (newNode, _) = SplitLeafNode(newParentNode, 0, leafNode);

                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newParentNode.Id, newParentNode);
                    isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);

                    await m_stateClient.WaitForNotFullAsync();
                }
                else if (splitInfo.child is InternalNode<K, V> internalNode)
                {
                    var nextId = m_stateClient.GetNewPageId();
                    var newParentNode = new InternalNode<K, V>(nextId);
                    // No lock required
                    newParentNode.children.Insert(0, internalNode.Id);
                    m_stateClient.Metadata!.Root = nextId;

                    var (newNode, _) = SplitInternalNode(newParentNode, 0, internalNode);

                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                    isFull |= m_stateClient.AddOrUpdate(newParentNode.Id, newParentNode);
                    isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);

                    await m_stateClient.WaitForNotFullAsync();
                }
            }
            else
            {
                var parent = (splitInfo.parent as InternalNode<K, V>)!;
                if (splitInfo.child is LeafNode<K, V> leafNode)
                {

                    var (newNode, _) = SplitLeafNode(parent, splitInfo.index, leafNode);
                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                    isFull |= m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    isFull |= m_stateClient.AddOrUpdate(parent.Id, parent);

                    if (isFull)
                    {
                        await m_stateClient.WaitForNotFullAsync();
                    }
                }
                else if (splitInfo.child is InternalNode<K, V> internalNode)
                {
                    var (newNode, splitKey) = SplitInternalNode(parent, splitInfo.index, internalNode);

                    if (newNode.keys.Count == newNode.children.Count)
                    {

                    }
                    if (internalNode.keys.Count == internalNode.children.Count)
                    {

                    }
                    var isFull = false;
                    isFull |= m_stateClient.AddOrUpdate(newNode.Id, newNode);
                    isFull |= m_stateClient.AddOrUpdate(parent.Id, parent);
                    isFull |= m_stateClient.AddOrUpdate(internalNode.Id, internalNode);

                    if (isFull)
                    {
                        await m_stateClient.WaitForNotFullAsync();
                    }
                }
            }
        }

        public void BulkWriteSortedData_Node(
            List<Task> waitList,
            Dictionary<long, SplitInfo> splitList,
            List<KeyValuePair<K, V>> items,
            IBPlusTreeNode node,
            IBPlusTreeNode? parent,
            int childIndex,
            int insertIndex,
            int insertCount)
        {
            if (node is LeafNode<K, V> leaf)
            {
                BulkWriteLeafNode(items, leaf, insertIndex, insertCount);

                // Check for split
                if (leaf.keys.Count >= this.m_stateClient.Metadata!.BucketLength)
                {
                    lock (splitList)
                    {
                        if (splitList.ContainsKey(leaf.Id))
                        {

                        }
                        splitList.Add(leaf.Id, new SplitInfo()
                        {
                            parent = parent,
                            child = leaf,
                            index = childIndex
                        });
                    }
                }
                // Check for split
            }
            else if (node is InternalNode<K, V> internalNode)
            {
                BulkWriteSortedData_Internal(waitList, splitList, items, 0, items.Count, internalNode, 0, internalNode.keys.Count);

                // Check for split
                if (internalNode.keys.Count >= this.m_stateClient.Metadata!.BucketLength)
                {
                    lock (splitList)
                    {
                        if (splitList.ContainsKey(internalNode.Id))
                        {

                        }
                        splitList.Add(internalNode.Id, new SplitInfo()
                        {
                            parent = parent,
                            child = internalNode,
                            index = childIndex
                        });
                    }
                }
            }
        }

        private void BulkWriteLeafNode(
            List<KeyValuePair<K, V>> items,
            LeafNode<K, V> leaf,
            int insertIndex,
            int insertCount)
        {
            var stop = insertIndex + insertCount;
            for (int i = insertIndex; i < stop; i++)
            {
                // Insert all here
                var index = leaf.keys.BinarySearch(items[i].Key, m_keyComparer);
                if (index < 0)
                {
                    index = ~index;
                    leaf.InsertAt(items[i].Key, items[i].Value, index);
                }
            }
        }

        private void BulkWriteSortedData_Internal(
            List<Task> waitList,
            Dictionary<long, SplitInfo> splitList,
            List<KeyValuePair<K, V>> items,
            int index,
            int count,
            InternalNode<K, V> node,
            int searchIndex,
            int searchCount)
        {
            if (searchCount == 0)
            {
                if (searchIndex < 0 || searchIndex >= node.children.Count)
                {

                }
                var getChildTask = m_stateClient.GetValue(node.children[searchIndex], "SearchRoot");

                // Check if child is already fetched, if so, continue, otherwise start task that will wait for the result.
                if (getChildTask.IsCompletedSuccessfully)
                {
                    BulkWriteSortedData_Node(waitList, splitList, items, getChildTask.Result!, node, searchIndex, index, count);
                }
                else
                {
                    var task = getChildTask.AsTask().ContinueWith(childNode =>
                    {
                        BulkWriteSortedData_Node(waitList, splitList, items, childNode.Result!, node, searchIndex, index, count);
                    });
                    lock (waitList)
                    {
                        waitList.Add(task);
                    }
                }
                return;
            }

            // Calculate median of  index and count
            var medianItemIndex = (index + (index + count)) / 2;


            //var medianItemIndex = count / 2;
            var leftIndex = node.keys.BinarySearch(searchIndex, searchCount, items[index].Key, m_keyComparer);
            var rightIndex = node.keys.BinarySearch(searchIndex, searchCount, items[Math.Max(index + count - 1, 0)].Key, m_keyComparer);
            var bucketIndex = node.keys.BinarySearch(searchIndex, searchCount, items[medianItemIndex].Key, m_keyComparer);
            if (leftIndex < 0)
            {
                leftIndex = ~leftIndex;
            }
            if (rightIndex < 0)
            {
                rightIndex = ~rightIndex;
            }
            if (bucketIndex < 0)
            {
                bucketIndex = ~bucketIndex;
            }
            if (leftIndex == rightIndex)
            {
                // All items are in the same bucket
                BulkWriteSortedData_Internal(waitList, splitList, items, index, count, node, leftIndex, rightIndex - leftIndex);
                return;
            }
            else
            {
                if (leftIndex == bucketIndex)       
                {
                    BulkWriteSortedData_Internal(waitList, splitList, items, index, medianItemIndex + 1, node, leftIndex, 0);
                    BulkWriteSortedData_Internal(waitList, splitList, items, index + medianItemIndex + 1, count - medianItemIndex - 1, node, bucketIndex + 1, rightIndex - bucketIndex - 1);
                }
                else if (rightIndex == bucketIndex)
                {
                    BulkWriteSortedData_Internal(waitList, splitList, items, index, medianItemIndex - 1, node, leftIndex, bucketIndex - leftIndex);
                    BulkWriteSortedData_Internal(waitList, splitList, items, index + medianItemIndex - 1, count - medianItemIndex + 1, node, bucketIndex, rightIndex - bucketIndex);
                }
                else
                {
                    BulkWriteSortedData_Internal(waitList, splitList, items, index, medianItemIndex + 1, node, leftIndex, bucketIndex - leftIndex);
                    BulkWriteSortedData_Internal(waitList, splitList, items, index + medianItemIndex + 1, count - medianItemIndex - 1, node, bucketIndex + 1, rightIndex - bucketIndex - 1);
                }
                
                //if ((rightIndex - bucketIndex) == 0)
                //{

                //}
                //BulkWriteSortedData_Internal(waitList, splitList, items, index + medianItemIndex, count - medianItemIndex, node, bucketIndex, rightIndex - bucketIndex);
            }
            
        }
    }
}
