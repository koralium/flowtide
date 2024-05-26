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

using FlowtideDotNet.Base.Engine;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Internal
{
    internal class OrleansStreamScheduler : IStreamScheduler
    {
        private readonly RegisterTimer registerTimerFunc;
        private IStreamTriggerCaller? _streamTriggerCaller;
        private readonly ConcurrentDictionary<string, IDisposable> _registeredTimers = new ConcurrentDictionary<string, IDisposable>();

        public delegate IDisposable RegisterTimer(Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period);

        public OrleansStreamScheduler(RegisterTimer registerTimerFunc)
        {
            this.registerTimerFunc = registerTimerFunc;
        }

        public void Initialize(IStreamTriggerCaller streamTriggerCaller)
        {
            _streamTriggerCaller = streamTriggerCaller;
        }

        public Task RemoveSchedule(string triggerName, string operatorName)
        {
            if (_registeredTimers.TryRemove($"{triggerName}_{operatorName}", out var disposable))
            {
                disposable.Dispose();
            }
            return Task.CompletedTask;
        }

        private record TimerState(string TriggerName, string OperatorName);

        public Task Schedule(string triggerName, string operatorName, TimeSpan interval)
        {
            var disposable = registerTimerFunc(async (state) =>
            {
                if (_streamTriggerCaller != null)
                {
                    var timerState = (state as TimerState)!;
                    await _streamTriggerCaller.CallTrigger(timerState.OperatorName, timerState.TriggerName, default);
                }
            }, new TimerState(triggerName, operatorName), interval, interval);

            _registeredTimers.AddOrUpdate($"{triggerName}_{operatorName}", disposable, (key, old) =>
            {
                old.Dispose();
                return disposable;
            });
            return Task.CompletedTask;
        }
    }
}
