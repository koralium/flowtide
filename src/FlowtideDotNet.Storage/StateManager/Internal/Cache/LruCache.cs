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

namespace FlowtideDotNet.Storage.StateManager.Internal.Cache
{
    internal class LruCache
    {
        struct LinkedListValue
        {
            public required long key;
            public required ICacheObject value;
            public required ICacheClient client;
        }

        private readonly int m_maxSize;
        Dictionary<long, LinkedListNode<LinkedListValue>> m_lookup;
        LinkedList<LinkedListValue> m_nodes;
        private readonly object m_lock = new object();
        private readonly int removeCount;
        private SemaphoreSlim purgeLock = new SemaphoreSlim(1, 1);

        public LruCache(int maxSize)
        {
            m_maxSize = maxSize;
            removeCount = (int)Math.Ceiling(maxSize * 0.3);
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
            if (m_lookup.TryGetValue(key, out var node))
            {
                m_nodes.Remove(node);
                m_lookup.Remove(key);
            }
        }

        public bool TryGetValueNoRefresh(in long key, out ICacheObject? value)
        {
            lock (m_lock)
            {
                if (m_lookup.TryGetValue(key, out var node))
                {
                    value = node.ValueRef.value;
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
                    m_nodes.Remove(node);
                    m_nodes.AddLast(node);
                    return true;
                }
            }

            value = default;
            return false;
        }

        public ValueTask Add(in long key, in ICacheObject value, in ICacheClient cacheClient)
        {
            Monitor.Enter(m_lock);
            if (m_lookup.TryGetValue(key, out var node))
            {
                if (value.Equals(node.Value))
                {
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
                    client = cacheClient
                });
                m_lookup[key] = newNode;
                m_nodes.AddLast(newNode);
                Monitor.Exit(m_lock);
                return ValueTask.CompletedTask;
            }

            if (m_nodes.Count >= m_maxSize)
            {
                Monitor.Exit(m_lock);
                purgeLock.Wait();
                Monitor.Enter(m_lock);
                if (m_nodes.Count >= m_maxSize)
                {
                    List<LinkedListNode<LinkedListValue>> nodesToDelete = new List<LinkedListNode<LinkedListValue>>();
                    Dictionary<ICacheClient, List<KeyValuePair<long, ICacheObject>>> toBeRemoved = new Dictionary<ICacheClient, List<KeyValuePair<long, ICacheObject>>>();
                    //List<KeyValuePair<long, object>> toBeRemoved = new List<KeyValuePair<long, object>>();
                    var firstNode = m_nodes.First;
                    {
                        nodesToDelete.Add(firstNode);
                        if (!toBeRemoved.TryGetValue(firstNode.ValueRef.client, out var list))
                        {
                            list = new List<KeyValuePair<long, ICacheObject>>();
                            toBeRemoved.Add(firstNode.ValueRef.client, list);
                        }
                        list.Add(new KeyValuePair<long, ICacheObject>(firstNode.ValueRef.key, firstNode.ValueRef.value));
                    }

                    for (int i = 0; i < removeCount - 1; i++)
                    {
                        firstNode = firstNode.Next;
                        nodesToDelete.Add(firstNode);
                        //m_lookup.Remove(firstNode!.ValueRef.key);
                        //m_nodes.Remove(firstNode);
                        if (!toBeRemoved.TryGetValue(firstNode.ValueRef.client, out var list))
                        {
                            list = new List<KeyValuePair<long, ICacheObject>>();
                            toBeRemoved.Add(firstNode.ValueRef.client, list);
                        }
                        list.Add(new KeyValuePair<long, ICacheObject>(firstNode.ValueRef.key, firstNode.ValueRef.value));
                    }

                    Monitor.Exit(m_lock);

                    return Add_RemoveFromCacheSlow(toBeRemoved, key, value, cacheClient, nodesToDelete);
                }
                else
                {
                    purgeLock.Release();
                }
            }
            Add_AfterRemove(key, value, cacheClient);
            Monitor.Exit(m_lock);
            return ValueTask.CompletedTask;
        }

        private async ValueTask Add_RemoveFromCacheSlow(
            Dictionary<ICacheClient, List<KeyValuePair<long, ICacheObject>>> toBeRemoved, 
            long key, 
            ICacheObject value, 
            ICacheClient cacheClient,
            List<LinkedListNode<LinkedListValue>> nodesToDelete)
        {
            List<ValueTask> tasks = new List<ValueTask>();
            foreach (var client in toBeRemoved)
            {
                tasks.Add(client.Key.OnCacheRemove(client.Value));
            }
            foreach(var task in tasks)
            {
                await task;
            }
            Monitor.Enter(m_lock);
            foreach(var node in nodesToDelete)
            {
                m_lookup.Remove(node!.ValueRef.key);
                try
                {
                    m_nodes.Remove(node);
                }
                catch(Exception e)
                {
                    //throw e;
                }
                
            }
            Add_AfterRemove(key, value, cacheClient);
            Monitor.Exit(m_lock);
            purgeLock.Release();
            
            
        }

        private void Add_AfterRemove(in long key, in ICacheObject value, in ICacheClient cacheClient)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
            {
                key = key,
                value = value,
                client = cacheClient
            });
            m_nodes.AddLast(newNode);
            m_lookup.Add(key, newNode);
        }
    }
}
