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
    /// This class handles the scheduling of triggers.
    /// </summary>
    public class DefaultStreamScheduler : IStreamScheduler
    {
        private IStreamTriggerCaller? _streamTriggerCaller;
        private readonly Dictionary<string, TimeSpan> _existingSchedules;
        private readonly PriorityQueue<(string, string), DateTime> _queue;
        private readonly object _lock = new object();

        public DefaultStreamScheduler()
        {
            _existingSchedules = new Dictionary<string, TimeSpan>();
            _queue = new PriorityQueue<(string, string), DateTime>();
        }

        public virtual void Initialize(IStreamTriggerCaller streamTriggerCaller)
        {
            _streamTriggerCaller = streamTriggerCaller;
        }

        public virtual Task RemoveSchedule(string triggerName, string operatorName)
        {
            lock (_lock)
            {
                _existingSchedules.Remove($"{operatorName}.{triggerName}");
            }
            return Task.CompletedTask;
        }

        public virtual Task Schedule(string triggerName, string operatorName, TimeSpan interval)
        {
            lock (_lock)
            {
                _existingSchedules.Add($"{operatorName}.{triggerName}", interval);
                _queue.Enqueue((operatorName, triggerName), DateTime.UtcNow.Add(interval));
            }

            return Task.CompletedTask;
        }

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
