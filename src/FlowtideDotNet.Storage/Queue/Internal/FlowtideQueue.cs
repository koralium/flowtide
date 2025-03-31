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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.Queue.Internal
{
    /// <summary>
    /// A linked list queue with nodes of multiple values that can be off-loaded to disk.
    /// 
    /// Format:
    /// Leaf1 <-> Leaf2 <-> Leaf3 <-> Leaf4 <-> Leaf5
    /// 
    /// The leaf node has both forward and backward pointers to allow dequeuing from the left and right (FIFO and LIFO).
    /// </summary>
    /// <typeparam name="V"></typeparam>
    /// <typeparam name="TValueContainer"></typeparam>
    internal class FlowtideQueue<V, TValueContainer> : IFlowtideQueue<V, TValueContainer>
        where TValueContainer : IValueContainer<V>
    {
        private readonly PrimitiveListKeyContainerSerializer<long> _keySerializer;
        private readonly IStateClient<IBPlusTreeNode, FlowtideQueueMetadata> _stateClient;
        private readonly FlowtideQueueOptions<V, TValueContainer> _options;
        public QueueNode<V, TValueContainer>? _leftNode;
        public QueueNode<V, TValueContainer>? _rightNode;
        private readonly int _pageSizeBytes;

        private bool _rightNodeUpdated = false;
        private bool _commited = false;

        public long Count => _stateClient.Metadata?.QueueSize ?? 0;

        public FlowtideQueue(IStateClient<IBPlusTreeNode, FlowtideQueueMetadata> stateClient, FlowtideQueueOptions<V, TValueContainer> options)
        {
            _stateClient = stateClient;
            this._options = options;
            _keySerializer = new PrimitiveListKeyContainerSerializer<long>(options.MemoryAllocator);
            _pageSizeBytes = options.PageSizeBytes ?? 16 * 1024;
        }

        public async Task InitializeAsync()
        {
            if (_stateClient.Metadata == null)
            {
                var nodeId = _stateClient.GetNewPageId();
                var emptyKeys = _keySerializer.CreateEmpty();
                var emptyValues = _options.ValueSerializer.CreateEmpty();
                _rightNode = new QueueNode<V, TValueContainer>(nodeId, emptyValues);
                _leftNode = _rightNode;
                _rightNode.TryRent();
                _stateClient.Metadata = new FlowtideQueueMetadata()
                {
                    DequeueIndex = 0,
                    InsertIndex = 0,
                    Left = nodeId,
                    Right = nodeId,
                    QueueSize = 0
                };
            }
            else
            {
                // Fetch left and right nodes
                _rightNode = (await _stateClient.GetValue(_stateClient.Metadata.Right)) as QueueNode<V, TValueContainer>;
                _rightNode!.TryRent();

                if (_stateClient.Metadata.Right != _stateClient.Metadata.Left)
                {
                    _leftNode = (await _stateClient.GetValue(_stateClient.Metadata.Left)) as QueueNode<V, TValueContainer>;
                    _leftNode!.TryRent();
                }
                else
                {
                    _leftNode = _rightNode;
                }
            }
        }

        public ValueTask Enqueue(in V value)
        {
            Debug.Assert(_rightNode != null, "Queue must be initialized before enqueueing");
            Debug.Assert(_leftNode != null);
            Debug.Assert(_stateClient.Metadata != null);
            var nodeSize = _rightNode.GetByteSize();
            _rightNodeUpdated = true;
            _stateClient.Metadata.QueueSize++;

            if (_rightNode.values.Count > _stateClient.Metadata.InsertIndex)
            {
                // There are more values in the node, just update the value
                _rightNode.values.Update(_stateClient.Metadata.InsertIndex, value);
                _stateClient.Metadata.InsertIndex++;
            }
            else
            {
                if (nodeSize < _pageSizeBytes)
                {
                    _rightNode.values.Insert(_stateClient.Metadata.InsertIndex, value);
                    _stateClient.Metadata.InsertIndex++;
                }
                else
                {
                    var newNodeId = _stateClient.GetNewPageId();
                    var emptyValues = _options.ValueSerializer.CreateEmpty();
                    var newNode = new QueueNode<V, TValueContainer>(newNodeId, emptyValues);
                    newNode.previous = _rightNode.Id;
                    _rightNode.next = newNodeId;
                    bool isFull = _stateClient.AddOrUpdate(_rightNode.Id, _rightNode);
                    if (_rightNode.Id != _leftNode.Id)
                    {
                        _rightNode.Return();
                    }
                    _rightNode = newNode;
                    _rightNode.TryRent();
                    _stateClient.Metadata.Right = newNodeId;
                    _stateClient.Metadata.InsertIndex = 1;

                    // Insert the new value in the queue
                    _rightNode.values.Insert(0, value);

                    if (isFull)
                    {
                        return new ValueTask(_stateClient.WaitForNotFullAsync());
                    }
                }
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask<V> Dequeue()
        {
            Debug.Assert(_leftNode != null, "Queue must be initialized before dequeuing");
            Debug.Assert(_rightNode != null);
            Debug.Assert(_stateClient.Metadata != null);

            if (_leftNode.values.Count == _stateClient.Metadata.DequeueIndex)
            {
                if (_leftNode.next == 0)
                {
                    throw new InvalidOperationException("Queue is empty");
                }
                var oldLeftNode = _leftNode;
                if (_leftNode.next == _rightNode.Id)
                {
                    _leftNode = _rightNode;
                }
                else
                {
                    var getNextNodeTask = _stateClient.GetValue(_leftNode.next);
                    if (!getNextNodeTask.IsCompleted)
                    {
                        return Dequeue_Slow(getNextNodeTask);
                    }
                    _leftNode = (getNextNodeTask.Result) as QueueNode<V, TValueContainer>;
                    _leftNode!.TryRent();
                }
                _stateClient.Metadata.Left = _leftNode.Id;
                _stateClient.Metadata.DequeueIndex = 0;

                _stateClient.Delete(oldLeftNode.Id);
                oldLeftNode.Dispose();
            }

            var value = _leftNode.values.Get(_stateClient.Metadata.DequeueIndex);
            _stateClient.Metadata.DequeueIndex++;
            _stateClient.Metadata.QueueSize--;
            return ValueTask.FromResult(value);
        }

        private async ValueTask<V> Dequeue_Slow(ValueTask<IBPlusTreeNode?> getNextNodeTask)
        {
            Debug.Assert(_stateClient.Metadata != null);
            Debug.Assert(_leftNode != null);

            var nextNode = (await getNextNodeTask) as QueueNode<V, TValueContainer>;

            if (nextNode == null)
            {
                throw new InvalidOperationException("Could not fetch the next data page in queue.");
            }
            var oldLeftNode = _leftNode;
            _leftNode = nextNode;
            _stateClient.Metadata.Left = _leftNode.Id;
            _stateClient.Metadata.DequeueIndex = 0; 

            _stateClient.Delete(oldLeftNode.Id);
            oldLeftNode.Dispose();

            var value = _leftNode.values.Get(_stateClient.Metadata.DequeueIndex);
            _stateClient.Metadata.DequeueIndex++;
            _stateClient.Metadata.QueueSize--;
            return value;
        }

        public ValueTask Commit()
        {
            Debug.Assert(_rightNode != null);
            if (_rightNodeUpdated)
            {
                _stateClient.AddOrUpdate(_rightNode.Id, _rightNode);
            }
            _commited = true;
            return _stateClient.Commit();
        }

        public async ValueTask Clear()
        {
            Debug.Assert(_leftNode != null);
            Debug.Assert(_rightNode != null);
            Debug.Assert(_stateClient.Metadata != null);

            if (!_commited && (_leftNode.Id == _rightNode.Id))
            {
                // If only a single page is in use, just reset the indices
                _stateClient.Metadata.DequeueIndex = 0;
                _stateClient.Metadata.InsertIndex = 0;
                _stateClient.Metadata.QueueSize = 0;
                return;
            }
            await _stateClient.Reset(true);
            var nodeId = _stateClient.GetNewPageId();
            var emptyKeys = _keySerializer.CreateEmpty();
            var emptyValues = _options.ValueSerializer.CreateEmpty();
            _rightNode = new QueueNode<V, TValueContainer>(nodeId, emptyValues);
            _leftNode = _rightNode;
            _rightNode.TryRent();
            _stateClient.Metadata = new FlowtideQueueMetadata()
            {
                DequeueIndex = 0,
                InsertIndex = 0,
                Left = nodeId,
                Right = nodeId,
                QueueSize = 0
            };
        }

        public ValueTask<V> Pop()
        {
            Debug.Assert(_stateClient.Metadata != null);
            Debug.Assert(_rightNode != null);
            Debug.Assert(_leftNode != null);
            if (_stateClient.Metadata.InsertIndex == 0)
            {
                if (_rightNode.previous == 0)
                {
                    throw new InvalidOperationException("Queue is empty");
                }
                var oldRightNode = _rightNode;
                if (_rightNode.previous == _leftNode.Id)
                {
                    _rightNode = _leftNode;
                }
                else
                {
                    var getPreviousNodeTask = _stateClient.GetValue(_rightNode.previous);
                    if (!getPreviousNodeTask.IsCompleted)
                    {
                        return Pop_Slow(getPreviousNodeTask);
                    }
                    _rightNode = (getPreviousNodeTask.Result) as QueueNode<V, TValueContainer>;
                    _rightNode!.TryRent();
                }
                _stateClient.Metadata.InsertIndex = _rightNode.values.Count;
                _stateClient.Delete(oldRightNode.Id);
                oldRightNode.Dispose();
            }
            _stateClient.Metadata.InsertIndex--;
            _stateClient.Metadata.QueueSize--;
            return ValueTask.FromResult(_rightNode.values.Get(_stateClient.Metadata.InsertIndex));
        }

        private async ValueTask<V> Pop_Slow(ValueTask<IBPlusTreeNode?> getPreviousNodeTask)
        {
            Debug.Assert(_stateClient.Metadata != null);
            Debug.Assert(_rightNode != null);
            Debug.Assert(_leftNode != null);

            var previousNode = (await getPreviousNodeTask) as QueueNode<V, TValueContainer>;

            if (previousNode == null)
            {
                throw new InvalidOperationException("Could not fetch the previous data page in queue.");
            }
            var oldRightNode = _rightNode;
            _rightNode = previousNode;
            _stateClient.Metadata.InsertIndex = _rightNode.values.Count - 1;
            _stateClient.Metadata.QueueSize--;

            _stateClient.Delete(oldRightNode.Id);
            oldRightNode.Dispose();

            return _rightNode.values.Get(_stateClient.Metadata.InsertIndex);
        }
    }
}
