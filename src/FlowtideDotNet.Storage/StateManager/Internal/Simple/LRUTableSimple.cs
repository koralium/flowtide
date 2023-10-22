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

namespace FlowtideDotNet.Storage.StateManager.Internal.Simple
{
    internal class LRUTableSimple
    {
        internal struct LinkedListValue
        {
            public required long key;
            public required ICacheObject value;
            public required IStateSerializer stateSerializer;
        }

        public delegate ValueTask OnRemoveDelegate(IEnumerable<LinkedListValue> toBeRemoved);

        private readonly int m_maxSize;
        private readonly OnRemoveDelegate m_onRemove;
        Dictionary<long, LinkedListNode<LinkedListValue>> m_lookup;
        LinkedList<LinkedListValue> m_nodes;
        private readonly object m_lock = new object();
        private readonly int removeCount;
        private SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private long cacheHits;
        private long cacheMiss;

        public LRUTableSimple(int maxSize, OnRemoveDelegate onRemove)
        {
            m_maxSize = maxSize;
            removeCount = (int)Math.Ceiling(maxSize * 0.3);
            this.m_onRemove = onRemove;
            m_lookup = new Dictionary<long, LinkedListNode<LinkedListValue>>();
            m_nodes = new LinkedList<LinkedListValue>();
        }

        public void Clear()
        {
            // TODO: Should send all values to on remove
            m_lookup.Clear();
            m_nodes.Clear();
        }

        public void ClearAllExcept(IEnumerable<long> keys)
        {
            _semaphore.Wait();
            List<KeyValuePair<long, LinkedListNode<LinkedListValue>>> toKeep = new List<KeyValuePair<long, LinkedListNode<LinkedListValue>>>();

            foreach(var key in keys)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    toKeep.Add(new KeyValuePair<long, LinkedListNode<LinkedListValue>>(key, node));
                }
            }

            m_lookup.Clear();
            m_nodes.Clear();

            foreach(var k in toKeep)
            {
                m_lookup.Add(k.Key, k.Value);
                m_nodes.AddLast(k.Value);
            }

            _semaphore.Release();
        }

        public void Delete(in long key)
        {
            _semaphore.Wait();
            if (m_lookup.TryGetValue(key, out var node))
            {
                m_nodes.Remove(node);
                m_lookup.Remove(key);
            }
            _semaphore.Release();
        }

        public bool TryGetValueNoRefresh(in long key, out LinkedListValue? value)
        {
            _semaphore.Wait();
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    value = node.Value;
                    _semaphore.Release();
                    return true;
                }
            }
            _semaphore.Release();
            value = default;
            return false;
        }

        public bool TryGetValue(in long key, out ICacheObject? value)
        {
            _semaphore.Wait();
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    value = node.ValueRef.value;
                    m_nodes.Remove(node);
                    m_nodes.AddLast(node);
                    _semaphore.Release();
                    cacheHits++;
                    return true;
                }
                cacheMiss++;
            }
            _semaphore.Release();
            value = default;
            return false;
        }

        public ValueTask Add(in long key, in ICacheObject value, in IStateSerializer stateSerializer)
        {
            _semaphore.Wait();
            Monitor.Enter(m_lock);
            if (m_lookup.TryGetValue(key, out var node))
            {
                if (value.Equals(node.Value))
                {
                    m_nodes.Remove(node);
                    m_nodes.AddLast(node);
                    _semaphore.Release();
                    Monitor.Exit(m_lock);
                    return ValueTask.CompletedTask;
                }
                m_nodes.Remove(node);
                var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
                {
                    key = key,
                    value = value,
                    stateSerializer = stateSerializer
                });
                m_lookup[key] = newNode;
                m_nodes.AddLast(newNode);
                _semaphore.Release();
                Monitor.Exit(m_lock);
                return ValueTask.CompletedTask;
            }

            if (m_nodes.Count >= m_maxSize)
            {
                List<LinkedListValue> toBeRemoved = new List<LinkedListValue>();
                List<LinkedListNode<LinkedListValue>> nodesToDelete = new List<LinkedListNode<LinkedListValue>>();
                var firstNode = m_nodes.First;
                {
                    nodesToDelete.Add(firstNode);
                    toBeRemoved.Add(firstNode.Value);
                }

                for (int i = 0; i < removeCount - 1; i++)
                {
                    firstNode = firstNode.Next;
                    nodesToDelete.Add(firstNode);
                    toBeRemoved.Add(firstNode.Value);
                }

                Monitor.Exit(m_lock);
                var onRemoveTask = m_onRemove(toBeRemoved);
                return Add_Slow(onRemoveTask, key, value, nodesToDelete, stateSerializer);
            }
            Add_AfterRemove(key, value, stateSerializer);
            Monitor.Exit(m_lock);
            _semaphore.Release();
            return ValueTask.CompletedTask;
        }

        private void Add_AfterRemove(in long key, in ICacheObject value, IStateSerializer stateSerializer)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
            {
                key = key,
                value = value,
                stateSerializer = stateSerializer
            });
            m_nodes.AddLast(newNode);
            m_lookup.Add(key, newNode);
        }

        private async ValueTask Add_Slow(ValueTask removeTask, long key, ICacheObject value, List<LinkedListNode<LinkedListValue>> nodesToDelete, IStateSerializer stateSerializer)
        {
            await removeTask;
            Monitor.Enter(m_lock);

            foreach (var toRemove in nodesToDelete)
            {
                m_lookup.Remove(toRemove!.ValueRef.key);
                m_nodes.Remove(toRemove);
            }

            Add_AfterRemove(key, value, stateSerializer);
            Monitor.Exit(m_lock);
            _semaphore.Release();
        }
    }
}
