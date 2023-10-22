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

namespace FlowtideDotNet.Storage.StateManager.Internal.Avanced
{
    internal class LruTableAdvanced
    {
        internal struct LinkedListValue
        {
            public required long key;
            public required ICacheObject value;
            public required IStateSerializer stateSerializer;
            public long version;
            public required int useCount;
        }

        private long cacheHits;
        private long cacheMisses;


        public delegate ValueTask OnRemoveDelegate(IEnumerable<LinkedListValue> toBeRemoved);

        private readonly int m_maxSize;
        private readonly OnRemoveDelegate m_onRemove;
        Dictionary<long, LinkedListNode<LinkedListValue>> m_lookup;
        LinkedList<LinkedListValue> m_nodes;
        private readonly object m_lock = new object();
        private readonly int removeCount;
        private SemaphoreSlim _evictSemaphore = new SemaphoreSlim(1, 1);
        private bool evictRunning = false;
        private const int _maxUseCount = 5;

        public LruTableAdvanced(int maxSize, OnRemoveDelegate onRemove)
        {
            m_maxSize = maxSize;
            removeCount = (int)Math.Ceiling(maxSize * 0.3);
            m_onRemove = onRemove;
            m_lookup = new Dictionary<long, LinkedListNode<LinkedListValue>>();
            m_nodes = new LinkedList<LinkedListValue>();
        }

        public void Clear()
        {
            // TODO: Should send all values to on remove
            lock (m_lock)
            {
                m_lookup.Clear();
                m_nodes.Clear();
            }
        }

        public void Delete(in long key)
        {
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    m_nodes.Remove(node);
                    m_lookup.Remove(key);
                }
            }
        }

        public bool TryGetValueNoRefresh(in long key, out LinkedListValue? value)
        {
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    value = node.Value;
                    return true;
                }
            }
            value = default;
            return false;
        }

        public bool TryGetValue(in long key, out ICacheObject? value)
        {
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    value = node.ValueRef.value;
                    node.ValueRef.useCount = Math.Min(_maxUseCount, node.ValueRef.useCount + 1);
                    cacheHits++;
                    //m_nodes.Remove(node);
                    //m_nodes.AddLast(node);
                    return true;
                }
                cacheMisses++;
            }
            value = default;
            return false;
        }

        public ValueTask Add(in long key, in ICacheObject value, in IStateSerializer stateSerializer)
        {
            Monitor.Enter(m_lock);
            if (m_lookup.TryGetValue(key, out var node))
            {
                if (value.Equals(node.Value.value))
                {
                    node.ValueRef.version = node.ValueRef.version + 1;
                    m_nodes.Remove(node);
                    m_nodes.AddLast(node);
                    Monitor.Exit(m_lock);
                    return ValueTask.CompletedTask;
                }
                m_nodes.Remove(node);
                var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
                {
                    key = key,
                    value = value,
                    stateSerializer = stateSerializer,
                    useCount = 0
                });
                m_lookup[key] = newNode;
                m_nodes.AddLast(newNode);
                Monitor.Exit(m_lock);
                return ValueTask.CompletedTask;
            }

            if (m_nodes.Count >= m_maxSize)
            {
                if (!evictRunning)
                {
                    evictRunning = true;
                    Monitor.Exit(m_lock);
                    return Add_Evict(key, value, stateSerializer);
                }
            }
            Add_AfterRemove(key, value, stateSerializer);
            Monitor.Exit(m_lock);
            return ValueTask.CompletedTask;
        }

        private async ValueTask Add_Evict(long key, ICacheObject value, IStateSerializer stateSerializer)
        {
            // Async wait for eviction if another thread is doing eviction
            await _evictSemaphore.WaitAsync();
            Monitor.Enter(m_lock);

            // Check that no one else has cleaned up the cache
            if (m_nodes.Count >= m_maxSize)
            {
                List<LinkedListValue> toBeRemoved = new List<LinkedListValue>();
                List<LinkedListNode<LinkedListValue>> nodesToDelete = new List<LinkedListNode<LinkedListValue>>();
                var firstNode = m_nodes.First;
                //{
                //    nodesToDelete.Add(firstNode);
                //    toBeRemoved.Add(firstNode.Value);
                //}

                while (toBeRemoved.Count < removeCount)
                {
                    var currentNode = firstNode;
                    firstNode = currentNode.Next;
                    if (currentNode.ValueRef.useCount == 0)
                    {
                        nodesToDelete.Add(currentNode);
                        toBeRemoved.Add(currentNode.Value);
                    }
                    else
                    {
                        currentNode.ValueRef.useCount = currentNode.ValueRef.useCount - 1;
                        m_nodes.Remove(currentNode);
                        m_nodes.AddLast(currentNode);
                    }
                    
                }

                //for (int i = 0; i < removeCount - 1; i++)
                //{
                //    firstNode = firstNode.Next;
                //    nodesToDelete.Add(firstNode);
                //    toBeRemoved.Add(firstNode.Value);
                //}
                Monitor.Exit(m_lock);
                await m_onRemove(toBeRemoved);
                Monitor.Enter(m_lock);
                for (int i = 0; i < toBeRemoved.Count; i++)
                {
                    var copiedValue = toBeRemoved[i];
                    var node = nodesToDelete[i];

                    // If the version differs, an update has been made and it should not be removed from the cache
                    if (copiedValue.version != node.ValueRef.version)
                    {
                        continue;
                    }
                    if (node.List == null)
                    {

                    }
                    else
                    {
                        m_lookup.Remove(node!.ValueRef.key);
                        m_nodes.Remove(node);
                    }

                }
            }
            Add_AfterRemove(key, value, stateSerializer);
            evictRunning = false;
            Monitor.Exit(m_lock);
            _evictSemaphore.Release();
        }

        private void Add_AfterRemove(in long key, in ICacheObject value, IStateSerializer stateSerializer)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
            {
                key = key,
                value = value,
                stateSerializer = stateSerializer,
                useCount = 0
            });
            m_nodes.AddLast(newNode);
            m_lookup.Add(key, newNode);
        }
    }
}
