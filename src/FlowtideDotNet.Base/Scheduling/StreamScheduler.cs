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
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Scheduling
{
    internal class StreamScheduler
    {
        [ThreadStatic]
        private static bool _currentThreadIsProcessingItems;

        // The list of tasks to be executed
        private readonly LinkedList<OperatorTask> _tasks = new LinkedList<OperatorTask>(); // protected by lock(_tasks)
        private readonly PriorityQueue<OperatorTask, float> _highPrioTasks = new PriorityQueue<OperatorTask, float>();

        // The maximum concurrency level allowed by this scheduler.
        private readonly int _maxDegreeOfParallelism;

        // Indicates whether the scheduler is currently processing work items.
        private int _delegatesQueuedOrRunning = 0;

        // Creates a new instance with the specified degree of parallelism.
        public StreamScheduler(int maxDegreeOfParallelism)
        {
            if (maxDegreeOfParallelism < 1) throw new ArgumentOutOfRangeException("maxDegreeOfParallelism");
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
        }

        // Queues a task to the scheduler.
        public void QueueTask(OperatorTask task, float prio = 0.0f)
        {
            // Add the task to the list of tasks to be processed.  If there aren't enough
            // delegates currently queued or running to process tasks, schedule another.
            lock (_tasks)
            {
                if (prio > 0.0f)
                {
                    _highPrioTasks.Enqueue(task, 1 - prio);
                }
                else
                {
                    _highPrioTasks.Enqueue(task, 1.0f); // Default priority
                    //_tasks.AddLast(task);
                }
                
                if (_delegatesQueuedOrRunning < _maxDegreeOfParallelism)
                {
                    ++_delegatesQueuedOrRunning;
                    NotifyThreadPoolOfPendingWork();
                }
            }
        }

        public bool TryExecuteTaskInline(Task task, OperatorScheduler operatorScheduler, bool taskWasPreviouslyQueued)
        {
            // If this thread isn't already processing a task, we don't support inlining
            if (!_currentThreadIsProcessingItems) return false;

            // If the task was previously queued, remove it from the queue
            if (taskWasPreviouslyQueued)
                return false;
            else
                return operatorScheduler.ExecuteTask(task);
        }

        // Inform the ThreadPool that there's work to be executed for this scheduler.
        private void NotifyThreadPoolOfPendingWork()
        {
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                // Note that the current thread is now processing work items.
                // This is necessary to enable inlining of tasks into this thread.
                _currentThreadIsProcessingItems = true;
                try
                {
                    // Process all available items in the queue.
                    while (true)
                    {
                        OperatorTask item;
                        lock (_tasks)
                        {
                            while (_tasks.Count > 0)
                            {
                                var task = _tasks.First!.Value;
                                _tasks.RemoveFirst();
                                var busy = 0.0f; //task.GetBusy();
                                _highPrioTasks.Enqueue(task, 1 - busy);
                            }
                            // When there are no more items to be processed,
                            // note that we're done processing, and get out.
                            if (_tasks.Count == 0 && _highPrioTasks.Count == 0)
                            {
                                --_delegatesQueuedOrRunning;
                                break;
                            }

                            if (_highPrioTasks.Count > 0)
                            {
                                item = _highPrioTasks.Dequeue();
                            }
                            else
                            {
                                continue;
                            }
                        }

                        // Execute the task we pulled out of the queue
                        item.Execute();
                    }
                }
                // We're done processing items on the current thread
                finally { _currentThreadIsProcessingItems = false; }
            }, null);
        }
    }
}
