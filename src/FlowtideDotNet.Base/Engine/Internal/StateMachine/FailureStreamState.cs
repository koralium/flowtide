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

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class FailureStreamState : StreamStateMachineState
    {
        private readonly object _lock = new object();
        private Task? _currentTask;

        public override void Initialize(StreamStateValue previousState)
        {
            lock (_lock)
            {
                if (_currentTask != null)
                {
                    return;
                }
                _currentTask = Task.Factory.StartNew(async () =>
                {
                    await StopAndDispose();
                })
                    .Unwrap()
                    .ContinueWith(t =>
                    {
                        lock(_lock)
                        {
                            _currentTask = null;
                        }
                        
                        if (t.IsFaulted)
                        {
                            Debug.Assert(_context != null, nameof(_context));
                            return _context.OnFailure(t.Exception);
                        }
                        return Task.CompletedTask;
                    })
                    .Unwrap();
            }
        }

        private async Task StopAndDispose()
        {
            Debug.Assert(_context != null, nameof(_context));
            // Clear all triggers before cancelling and stop registering new triggers
            _context.CancelTriggerRegistration();
            await _context.ClearTriggers();

            lock (_context._checkpointLock)
            {
                if (_context.checkpointTask != null)
                {
                    _context.checkpointTask.SetCanceled();
                    _context.checkpointTask = null;
                }
            }

            _context.ForEachBlock((key, block) =>
            {
                block.Fault(new Exception());
            });

            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });

            await _context.ForEachBlockAsync(async (key, block) =>
            {
                await block.DisposeAsync();
            });

            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await TransitionTo(StreamStateValue.Starting);
        }

        public override Task OnFailure()
        {
            Initialize(StreamStateValue.Failure);
            return Task.CompletedTask;
        }

        public override void EgressCheckpointDone(string name)
        {
            // Do nothing
        }

        public override Task TriggerCheckpoint(bool isScheduled = false)
        {
            Debug.Assert(_context != null, nameof(_context));

            if (isScheduled)
            {
                // Reschedule checkpoint
                _context._scheduleCheckpointTask = null;
                _context._triggerCheckpointTime = null;
                _context._scheduleCheckpointCancelSource = null;
                _context.TryScheduleCheckpointIn(TimeSpan.FromSeconds(10));
                return Task.CompletedTask;
            }
            return Task.FromException(new InvalidOperationException("Cant trigger a checkpoint when the stream is failing"));
        }

        public override Task CallTrigger(string operatorName, string triggerName, object? state)
        {
            Debug.Assert(_context != null, nameof(_context));

            return _context.CallTrigger_Internal(operatorName, triggerName, state);
        }

        public override Task CallTrigger(string triggerName, object? state)
        {
            Debug.Assert(_context != null, nameof(_context));

            return _context.CallTrigger_Internal(triggerName, state);
        }

        public override Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            Debug.Assert(_context != null, nameof(_context));

            return _context.AddTrigger_Internal(operatorName, triggerName, schedule);
        }

        public override Task StartAsync()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return TransitionTo(StreamStateValue.Deleting);
        }
    }
}
