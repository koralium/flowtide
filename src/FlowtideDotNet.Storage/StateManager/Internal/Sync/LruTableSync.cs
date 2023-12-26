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

using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class LruTableSync : IDisposable
    {
        internal struct LinkedListValue
        {
            public required long key;
            public required ICacheObject value;
            public required ILruEvictHandler evictHandler;
            public long version;
            public required int useCount;
            public bool removed;
        }

        private ConcurrentDictionary<long, LinkedListNode<LinkedListValue>> cache;
        private LinkedList<LinkedListValue> m_nodes;
        private Task? m_cleanupTask;
        private int maxSize;
        private readonly ILogger logger;
        private readonly Meter meter;
        private readonly long maxMemoryUsageInBytes;
        private int cleanupStart;
        private readonly SemaphoreSlim _fullLock;
        private int m_count;
        private long m_cacheHits;
        private long m_lastSeenCacheHits;
        private int m_sameCaheHitsCount;
        private bool m_disposedValue;
        private readonly Process _currentProcess;
        private readonly CancellationTokenSource m_cleanupTokenSource;

        public LruTableSync(int maxSize, ILogger logger, Meter meter, long maxMemoryUsageInBytes = -1)
        {
            cache = new ConcurrentDictionary<long, LinkedListNode<LinkedListValue>>();
            m_nodes = new LinkedList<LinkedListValue>();
            this.maxSize = maxSize;
            this.logger = logger;
            this.meter = meter;
            this.maxMemoryUsageInBytes = maxMemoryUsageInBytes;
            cleanupStart = (int)Math.Ceiling(maxSize * 0.7);
            _fullLock = new SemaphoreSlim(1);
            m_cleanupTokenSource = new CancellationTokenSource();
            StartCleanupTask();
            _currentProcess = Process.GetCurrentProcess();

            meter.CreateObservableGauge("lru_table_size", () => Volatile.Read(ref m_count));
            meter.CreateObservableGauge("lru_table_max_size", () => this.maxSize);
            meter.CreateObservableGauge("lru_table_cleanup_start", () => cleanupStart);
        }

        public void Clear()
        {
            lock (m_nodes)
            {
                cache.Clear();
                m_nodes.Clear();
                Volatile.Write(ref m_count, 0);
            }
        }

        public void Delete(in long key)
        {
            if (cache.TryGetValue(key, out var node))
            {
                lock (node)
                {
                    node.ValueRef.removed = true;
                    if (cache.TryRemove(key, out _))
                    {
                        Interlocked.Decrement(ref m_count);
                    }
                    try
                    {
                        lock (m_nodes)
                        {
                            m_nodes.Remove(node);
                        }
                    }
                    catch(Exception e)
                    {
                        throw;
                    }
                    
                }
                
            }
        }

        private void StartCleanupTask()
        {
            m_cleanupTask = Task.Factory.StartNew(async () =>
            {
                await CleanupTask();
            }, TaskCreationOptions.LongRunning)
                .Unwrap()
                .ContinueWith((task) =>
                {
                    if (m_cleanupTokenSource.IsCancellationRequested)
                    {
                        // Do not start a new task if we are cancelled
                        return;
                    }
                    if (task.IsFaulted)
                    {
                        this.logger.LogError(task.Exception, "Exception in LRU Table cleanup");
                    }
                    else
                    {
                        this.logger.LogWarning("Cleanup task closed without error.");
                    }
                    if (!task.IsCompletedSuccessfully)
                    {
                        StartCleanupTask();
                    }
                });
        }

        private async Task CleanupTask()
        {
            while (true)
            {
                m_cleanupTokenSource.Token.ThrowIfCancellationRequested();
                await Task.Delay(10);
                try
                {
                    await _fullLock.WaitAsync();
                    await Cleanup();
                }
                finally
                {
                    _fullLock.Release();
                }
                
            }
        }

        public bool TryGetValue(long key, out ICacheObject? cacheObject)
        {
            if (cache.TryGetValue(key, out var node))
            {
                lock (node)
                {
                    if (node.ValueRef.removed)
                    {
                        cacheObject = default;
                        return false;
                    }
                    node.ValueRef.useCount = Math.Min(node.ValueRef.useCount + 1, 5);
                    cacheObject = node.ValueRef.value;
                    Interlocked.Increment(ref m_cacheHits);
                    return true;
                }
            }
            cacheObject = default;
            return false;
        }

        public async Task Wait()
        {
            logger.LogWarning("LRU Table is full, waiting for cleanup to finish.");
            await _fullLock.WaitAsync().ConfigureAwait(false);
            _fullLock.Release();
            logger.LogInformation("LRU Table is no longer full.");
        }

        public bool Add(long key, ICacheObject value, ILruEvictHandler evictHandler)
        {
            bool full = false;
            if (Volatile.Read(ref m_count) > maxSize)
            {
                full = true;
            }
            cache.AddOrUpdate(key, (key) =>
            {
                var newNode = new LinkedListNode<LinkedListValue>(new LinkedListValue()
                {
                    key = key,
                    value = value,
                    evictHandler = evictHandler,
                    useCount = 0
                });

                lock (m_nodes)
                {
                    m_nodes.AddLast(newNode);
                }

                // Add to count
                Interlocked.Increment(ref m_count);

                return newNode;
            }, (key, old) =>
            {
                lock (old)
                {
                    if (value.Equals(old.ValueRef.value))
                    {
                        old.ValueRef.version = old.ValueRef.version + 1;
                        return old;
                    }
                    else
                    {
                        throw new Exception();
                    }
                }
            });

            return full;
        }

        private async Task Cleanup()
        {
            var currentCount = Volatile.Read(ref m_count);
            int cleanupStartLocal = cleanupStart;
            bool isCleanup = false;
            if (currentCount <= cleanupStartLocal)
            {
                var cacheHitsLocal = Volatile.Read(ref m_cacheHits);
                if (m_lastSeenCacheHits == cacheHitsLocal)
                {
                    m_sameCaheHitsCount++;
                    if (m_sameCaheHitsCount >= 1000 && currentCount > 0)
                    {
                        // No cache hits during a long time, clear the entire cache
                        isCleanup = true;
                        cleanupStartLocal = 0;
                        m_sameCaheHitsCount = 0;
                    }
                    else
                    {
                        return;
                    }
                }
                else
                {
                    m_lastSeenCacheHits = cacheHitsLocal;
                    m_sameCaheHitsCount = 0;
                    return;
                }
            }

            // Take cleanup count before increasing memory, to try and reduce semaphore locks
            var toBeRemovedCount = currentCount - cleanupStartLocal;
            if (maxMemoryUsageInBytes > 0)
            {
                _currentProcess.Refresh();
                var percentage = (float)currentCount / maxSize;
                if (_currentProcess.WorkingSet64 < (maxMemoryUsageInBytes * percentage))
                {
                    Volatile.Write(ref maxSize, (int)Math.Ceiling(maxSize * 1.1));
                    Volatile.Write(ref cleanupStart, (int)Math.Ceiling(maxSize * 0.7));
                    return;
                }
                else
                {
                    Volatile.Write(ref maxSize, (int)Math.Floor(maxSize * 0.9));
                    Volatile.Write(ref cleanupStart, (int)Math.Ceiling(maxSize * 0.7));
                    // On reduction, remove more directly
                    toBeRemovedCount = currentCount - cleanupStartLocal;
                }
            }
           
            LinkedListNode<LinkedListValue>? iteratorNode;
            lock (m_nodes)
            {
                iteratorNode = m_nodes.First;
            }
            
            Dictionary<ILruEvictHandler, List<(LinkedListNode<LinkedListValue>, long)>> groupedValues = new Dictionary<ILruEvictHandler, List<(LinkedListNode<LinkedListValue>, long)>>();
            List<(LinkedListNode<LinkedListValue>, long)> toBeRemoved = new List<(LinkedListNode<LinkedListValue>, long)>();
            while (iteratorNode != null && (toBeRemoved.Count < toBeRemovedCount))
            {
                bool moveToEnd = false;
                lock (iteratorNode)
                {
                    if (iteratorNode.ValueRef.useCount == 0)
                    {
                        if (!groupedValues.TryGetValue(iteratorNode.ValueRef.evictHandler, out var list))
                        {
                            list = new List<(LinkedListNode<LinkedListValue>, long)>();
                            groupedValues.Add(iteratorNode.ValueRef.evictHandler, list);
                        }
                        list.Add((iteratorNode, iteratorNode.ValueRef.version));
                        toBeRemoved.Add((iteratorNode, iteratorNode.ValueRef.version));
                    }
                    else
                    {
                        iteratorNode.ValueRef.useCount = iteratorNode.ValueRef.useCount - 1;
                        moveToEnd = true;
                    }
                }
                lock (m_nodes)
                {
                    iteratorNode = iteratorNode.Next;

                    if (iteratorNode == null)
                    {
                        iteratorNode = m_nodes.First;
                    }
                }
            }

            List<Task> evictTasks = new List<Task>();
            foreach(var group in groupedValues)
            {
                evictTasks.Add(Task.Factory.StartNew(() =>
                {
                    group.Key.Evict(group.Value, isCleanup);
                }));
            }

            await Task.WhenAll(evictTasks);

            foreach(var group in groupedValues)
            {
                // Go through each value and remove them from the cache
                foreach (var val in group.Value)
                {
                    lock (val.Item1)
                    {
                        if (val.Item2 != val.Item1.ValueRef.version)
                        {
                            continue;
                        }
                        if (!val.Item1.ValueRef.removed)
                        {
                            val.Item1.ValueRef.removed = true;
                            if (cache.TryRemove(val.Item1.ValueRef.key, out _))
                            {
                                Interlocked.Decrement(ref m_count);
                            }

                            try
                            {
                                lock (m_nodes)
                                {
                                    m_nodes.Remove(val.Item1);
                                }
                            }
                            catch (Exception e)
                            {
                                throw;
                            }
                        }
                    }
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_disposedValue)
            {
                if (disposing)
                {
                    m_cleanupTokenSource.Cancel();

                    if (m_cleanupTask != null)
                    {
                        m_cleanupTask.Wait();
                        m_cleanupTask.Dispose();
                    }
                    m_cleanupTokenSource.Dispose();
                    meter.Dispose();
                    _fullLock.Dispose();
                }
                m_disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
