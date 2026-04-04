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

namespace FlowtideDotNet.Base.Engine
{
    /// <summary>
    /// The default <see cref="IStreamScheduler"/> implementation that dispatches trigger events
    /// to stream operators at configurable recurring intervals.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Scheduling is backed by a min-heap priority queue keyed on the next UTC fire time, together with a
    /// dictionary that maps each <c>{operatorName}.{triggerName}</c> key to its recurrence interval.
    /// When <see cref="Schedule"/> is called, the trigger's first fire time is set to
    /// <c>DateTime.UtcNow + interval</c> and the entry is enqueued. On each <see cref="Tick"/> call,
    /// all entries whose fire time is in the past are dequeued, re-enqueued at the next interval, and
    /// dispatched via the <see cref="IStreamTriggerCaller"/> supplied at <see cref="Initialize"/>.
    /// </para>
    /// <para>
    /// <see cref="Tick"/> is not called automatically; it must be driven externally. When using
    /// <see cref="DataflowStream.RunAsync"/>, the stream's internal 10 ms periodic loop calls
    /// <see cref="Tick"/> on every iteration. Custom <see cref="IStreamScheduler"/> implementations
    /// or hosting scenarios that call <see cref="DataflowStream.StartAsync"/> directly are responsible
    /// for driving <see cref="Tick"/> at the desired resolution.
    /// </para>
    /// <para>
    /// All public members are declared <see langword="virtual"/> so that derived classes can override
    /// individual scheduling behaviours without replacing the entire implementation.
    /// </para>
    /// </remarks>
    public class DefaultStreamScheduler : IStreamScheduler
    {
        private IStreamTriggerCaller? _streamTriggerCaller;
        private readonly Dictionary<string, TimeSpan> _existingSchedules;
        private readonly PriorityQueue<(string, string), DateTime> _queue;
        private readonly object _lock = new object();

        /// <summary>
        /// Initializes a new instance of <see cref="DefaultStreamScheduler"/> with an empty schedule.
        /// </summary>
        public DefaultStreamScheduler()
        {
            _existingSchedules = new Dictionary<string, TimeSpan>();
            _queue = new PriorityQueue<(string, string), DateTime>();
        }

        /// <summary>
        /// Connects this scheduler to the stream engine, supplying the caller used to dispatch
        /// trigger events when their scheduled time arrives.
        /// </summary>
        /// <remarks>
        /// Called automatically by <c>StreamContext</c> immediately after the scheduler is assigned
        /// to the stream. Must be called before <see cref="Tick"/> can dispatch any triggers.
        /// </remarks>
        /// <param name="streamTriggerCaller">
        /// The <see cref="IStreamTriggerCaller"/> provided by the stream engine that routes trigger
        /// events to the appropriate operator vertices.
        /// </param>
        public virtual void Initialize(IStreamTriggerCaller streamTriggerCaller)
        {
            _streamTriggerCaller = streamTriggerCaller;
        }

        /// <summary>
        /// Removes a previously registered trigger schedule so that it is no longer dispatched.
        /// </summary>
        /// <remarks>
        /// Only the interval dictionary entry is removed. Any pending entry in the priority queue is
        /// left in place and will be silently dropped the next time it is dequeued during <see cref="Tick"/>.
        /// </remarks>
        /// <param name="triggerName">The name of the trigger to remove.</param>
        /// <param name="operatorName">The name of the operator that owns the trigger.</param>
        /// <returns>A completed task.</returns>
        public virtual Task RemoveSchedule(string triggerName, string operatorName)
        {
            lock (_lock)
            {
                _existingSchedules.Remove($"{operatorName}.{triggerName}");
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Registers a recurring trigger that will be dispatched at the specified interval.
        /// </summary>
        /// <remarks>
        /// The trigger is enqueued immediately with a first fire time of <c>DateTime.UtcNow + interval</c>.
        /// On each subsequent fire, it is automatically re-enqueued at <c>DateTime.UtcNow + interval</c>
        /// from the moment it was dispatched. The combined key <c>{operatorName}.{triggerName}</c> must be
        /// unique; registering a duplicate key will throw a <see cref="ArgumentException"/> from the
        /// underlying dictionary.
        /// </remarks>
        /// <param name="triggerName">The name identifying the trigger within the operator.</param>
        /// <param name="operatorName">The name of the operator that owns the trigger.</param>
        /// <param name="interval">The recurring interval at which the trigger should fire.</param>
        /// <returns>A completed task.</returns>
        public virtual Task Schedule(string triggerName, string operatorName, TimeSpan interval)
        {
            lock (_lock)
            {
                _existingSchedules.Add($"{operatorName}.{triggerName}", interval);
                _queue.Enqueue((operatorName, triggerName), DateTime.UtcNow.Add(interval));
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Checks the schedule queue and dispatches all triggers whose fire time has passed.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Each overdue trigger is dequeued, re-enqueued at <c>DateTime.UtcNow + interval</c> to
        /// schedule its next recurrence, and then dispatched via <see cref="IStreamTriggerCaller.CallTrigger"/>.
        /// If the trigger has been removed via <see cref="RemoveSchedule"/> since it was enqueued,
        /// it is silently discarded without being dispatched.
        /// </para>
        /// <para>
        /// The lock is released around each <c>CallTrigger</c> invocation so that scheduling operations
        /// from other threads can proceed while the trigger handler executes.
        /// </para>
        /// <para>
        /// This method must be called externally to advance the scheduler. When using
        /// <see cref="DataflowStream.RunAsync"/>, it is called automatically inside the 10 ms timer loop.
        /// </para>
        /// </remarks>
        /// <returns>A task that completes when all overdue triggers for this tick have been dispatched.</returns>
        public virtual async Task Tick()
        {
            Monitor.Enter(_lock);
            // Check if there is any trigger in queue that should be run.
            while (_queue.TryPeek(out _, out var time) && time.CompareTo(DateTime.UtcNow) <= 0)
            {

                var triggerInfo = _queue.Dequeue();

                // Try and schedule the next trigger
                if (_existingSchedules.TryGetValue($"{triggerInfo.Item1}.{triggerInfo.Item2}", out var interval))
                {
                    _queue.Enqueue(triggerInfo, DateTime.UtcNow.Add(interval));
                }
                else
                {
                    // Trigger has been removed
                    continue;
                }
                Monitor.Exit(_lock);
                // Call the trigger
                if (_streamTriggerCaller != null)
                {
                    await _streamTriggerCaller.CallTrigger(triggerInfo.Item1, triggerInfo.Item2, null);
                }
                Monitor.Enter(_lock);
            }
            Monitor.Exit(_lock);
        }
    }
}
