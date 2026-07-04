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

using FlowtideDotNet.Base.Exceptions;
using FlowtideDotNet.Base.Utils;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class FailureStreamState : StreamStateMachineState
    {
        private readonly object _lock = new object();
        private Task? _currentTask;
        private bool _isFailing = false;

        public override async Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            _context.CheckForPause();

            try
            {
                lock (_lock)
                {
                    if (_isFailing)
                    {
                        return;
                    }
                    _isFailing = true;
                }

                // Run stop and dispose linearly to make sure that the caller does
                // not return before any dependencies have been stopped and disposed
                // This is useful in distributed mode to make sure all other streams are stopped
                // and have recieved correct restore checkpoint version before going to starting.
                await StopAndDispose();
            }
            catch(Exception e)
            {
                _context._logger.FailedStopAndDispose(e, _context.streamName);
                _isFailing = false;
                await Initialize(previousState);
                return;
            }
            

            lock (_lock)
            {
                _context.SetStatus(StreamStatus.Failing);
                if (_currentTask != null)
                {
                    return;
                }
                // Transitioning to start is done in a separate task to avoid blocking the caller
                _currentTask = Task.Factory.StartNew(async () =>
                {
                    await Transition();
                })
                    .Unwrap()
                    .ContinueWith(t =>
                    {
                        lock (_lock)
                        {
                            _currentTask = null;
                            _isFailing = false;
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
                // Clear all checkpoint scheduling state, stale values from before the failure
                // would otherwise make scheduling requests after the recovery compare against
                // trigger times in the past and be dropped, leaving the stream without
                // checkpoints until an external trigger arrives.
                _context.inQueueCheckpoint = null;
                _context._currentProvidedCheckpointVersion = default;
                _context._scheduledProvidedCheckpointVersion = default;
                if (_context._scheduleCheckpointCancelSource != null)
                {
                    _context._scheduleCheckpointCancelSource.Cancel();
                    _context._scheduleCheckpointCancelSource.Dispose();
                    _context._scheduleCheckpointCancelSource = null;
                }
                _context._scheduleCheckpointTask = null;
                _context._triggerCheckpointTime = null;
            }

            _context.ForEachBlock((key, block) =>
            {
                block.Fault(new BlockStopException($"Faulting block due to stream failure."));
            });

            _context._logger.LogDebug("Failure handling waiting for block completion on stream {stream}", _context.streamName);
            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });

            // Call failure for all blocks
            if (_context._restoreCheckpointVersion.HasValue)
            {
                await _context.ForEachBlockAsync(async (key, block) =>
                {
                    _context._logger.LogDebug("Failure handling calling on failure on block {block} on stream {stream}", key, _context.streamName);
                    await block.OnFailure(_context._restoreCheckpointVersion.Value);
                });
            }

            await _context.ForEachBlockAsync(async (key, block) =>
            {
                _context._logger.LogDebug("Failure handling disposing block {block} on stream {stream}", key, _context.streamName);
                await block.DisposeAsync();
            });
            _context._logger.LogDebug("Failure handling stop and dispose finished on stream {stream}", _context.streamName);
        }

        private async Task Transition()
        {
            Debug.Assert(_context != null, nameof(_context));

            await Task.Delay(TimeSpan.FromMilliseconds(500));

            // Check if the stream should be in not started
            if (_context._wantedState == StreamStateValue.NotStarted)
            {
                // Dispose state
                _context._stateManager.Dispose();
                lock (_context._checkpointLock)
                {
                    // Check if any stop task source exist
                    if (_context._stopTask != null)
                    {
                        _context._stopTask.SetResult();
                        _context._stopTask = null;
                    }
                }
                // Transition to not started, the stream must not fall through and restart
                // after honoring the stop.
                await TransitionTo(StreamStateValue.NotStarted);
                return;
            }

            await TransitionTo(StreamStateValue.Starting);
        }

        public override Task OnFailure()
        {
            return Initialize(StreamStateValue.Failure);
        }

        public override void EgressCheckpointDone(string name, ILockingEvent? lockingEvent)
        {
            // Do nothing
        }

        public override void EgressDependenciesDone(string name, ILockingEvent? lockingEvent)
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
                _context.TryScheduleCheckpointIn(TimeSpan.FromSeconds(10), default);
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

        public override Task StopAsync()
        {
            Debug.Assert(_context != null, nameof(_context));
            // The failure handling may be mid way through faulting and disposing the blocks,
            // transitioning to stopping here would start a stop checkpoint against blocks
            // that can never complete it and the stop would hang. The wish is honored by
            // Transition when the cleanup has finished, which also completes the stop task
            // the caller awaits.
            _context._wantedState = StreamStateValue.NotStarted;
            return Task.CompletedTask;
        }
    }
}
