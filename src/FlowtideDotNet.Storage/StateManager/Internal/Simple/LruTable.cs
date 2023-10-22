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
    internal class LruTable
    {
        struct LinkedListValue
        {
            public long key;
            public ICacheObject value;
        }

        public delegate ValueTask OnRemoveDelegate(IEnumerable<KeyValuePair<long, ICacheObject>> toBeRemoved);

        private readonly int m_maxSize;
        private readonly OnRemoveDelegate m_onRemove;
        Dictionary<long, LinkedListNode<LinkedListValue>> m_lookup;
        LinkedList<LinkedListValue> m_nodes;
        private readonly object m_lock = new object();
        private readonly int removeCount;
        private SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public LruTable(int maxSize, OnRemoveDelegate onRemove)
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

        public bool TryGetValueNoRefresh(in long key, out ICacheObject? value)
        {
            _semaphore.Wait();
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    value = node.ValueRef.value;
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
                    return true;
                }
            }
            _semaphore.Release();
            value = default;
            return false;
        }

        public ValueTask Add(in long key, in ICacheObject value)
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
                    value = value
                });
                m_lookup[key] = newNode;
                m_nodes.AddLast(newNode);
                _semaphore.Release();
                Monitor.Exit(m_lock);
                return ValueTask.CompletedTask;
            }

            if (m_nodes.Count >= m_maxSize)
            {
                List<KeyValuePair<long, ICacheObject>> toBeRemoved = new List<KeyValuePair<long, ICacheObject>>();
                List<LinkedListNode<LinkedListValue>> nodesToDelete = new List<LinkedListNode<LinkedListValue>>();
                var firstNode = m_nodes.First;
                {
                    nodesToDelete.Add(firstNode);
                    toBeRemoved.Add(new KeyValuePair<long, ICacheObject>(firstNode.ValueRef.key, firstNode.ValueRef.value));
                }

                for (int i = 0; i < removeCount - 1; i++)
                {
                    firstNode = firstNode.Next;
                    nodesToDelete.Add(firstNode);
                    toBeRemoved.Add(new KeyValuePair<long, ICacheObject>(firstNode.ValueRef.key, firstNode.ValueRef.value));
                }


                //List<KeyValuePair<long, ICacheObject>> toBeRemoved = new List<KeyValuePair<long, ICacheObject>>();
                //for (int i = 0; i < removeCount; i++)
                //{
                //    var firstNode = m_nodes.First;
                //    m_lookup.Remove(firstNode!.ValueRef.key);
                //    m_nodes.Remove(firstNode);
                //    toBeRemoved.Add(new KeyValuePair<long, ICacheObject>(firstNode.ValueRef.key, firstNode.ValueRef.value));
                //}

                Monitor.Exit(m_lock);
                var onRemoveTask = m_onRemove(toBeRemoved);
                return Add_Slow(onRemoveTask, key, value, nodesToDelete);
                //if (!onRemoveTask.IsCompleted)
                //{
                    
                //}
                //Monitor.Enter(m_lock);
            }
            Add_AfterRemove(key, value);
            Monitor.Exit(m_lock);
            _semaphore.Release();
            return ValueTask.CompletedTask;
        }

        private void Add_AfterRemove(in long key, in ICacheObject value)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
            {
                key = key,
                value = value
            });
            m_nodes.AddLast(newNode);
            m_lookup.Add(key, newNode);
        }

        private async ValueTask Add_Slow(ValueTask removeTask, long key, ICacheObject value, List<LinkedListNode<LinkedListValue>> nodesToDelete)
        {
            await removeTask;
            Monitor.Enter(m_lock);

            foreach(var toRemove in nodesToDelete)
            {
                m_lookup.Remove(toRemove!.ValueRef.key);
                m_nodes.Remove(toRemove);
            }

            Add_AfterRemove(key, value);
            Monitor.Exit(m_lock);
            _semaphore.Release();
        }
    }
}
