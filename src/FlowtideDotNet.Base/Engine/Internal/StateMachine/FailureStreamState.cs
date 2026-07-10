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
            // Runs synchronously inside StreamContext.OnFailure's _contextLock, so a paused stream
            // that faults wedges here until resume. An awaited pause removes the wedge but lets the
            // tick loop and peer messages race the failing teardown, breaking distributed pause-crash
            // recovery, so it stays blocking for now.
            _context.CheckForPause();

            while (true)
            {
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
                    break;
                }
                catch (Exception e)
                {
                    _context._logger.FailedStopAndDispose(e, _context.streamName);
                    _isFailing = false;
                    // Paced retry, never recursion: a teardown that keeps failing, for
                    // example on storage that is down, would otherwise grow the stack
                    // without bound and kill the process with a stack overflow.
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
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

            // Wait for any in-flight checkpoint commit or compaction to finish before
            // tearing anything down. Faulting or disposing blocks, or disposing the state
            // manager, while the state manager is being written corrupts it. Bounded, a
            // write wedged on unresponsive storage cannot be made safe by waiting and must
            // not hang the recovery forever.
            var writeWaitStart = Stopwatch.GetTimestamp();
            while (true)
            {
                if (System.Threading.Volatile.Read(ref _context._stateManagerWriteCount) > 0)
                {
                    if (Stopwatch.GetElapsedTime(writeWaitStart) > _context._dataflowStreamOptions.StopDrainTimeout)
                    {
                        _context._logger.LogWarning("Failure teardown on stream {stream} proceeded while a state manager write was still active after {timeout}, the write may be wedged on storage.", _context.streamName, _context._dataflowStreamOptions.StopDrainTimeout);
                        break;
                    }
                    await Task.Delay(10);
                    continue;
                }
                // Commits and compactions claim their write count at the decision point, under
                // the checkpoint lock, before their task is scheduled. Re-reading under that
                // lock means every claim decided against the pre-failure state is visible, so a
                // zero here cannot race a task that was scheduled but not yet counted.
                bool settled;
                lock (_context._checkpointLock)
                {
                    settled = System.Threading.Volatile.Read(ref _context._stateManagerWriteCount) == 0;
                }
                if (settled)
                {
                    break;
                }
            }

            // Decide the restore version now that any in-flight commit has settled. A
            // checkpoint that completed during the failure is a valid, more recent recovery
            // point and is kept. A peer requested rollback version, captured earlier through
            // FailAndRollback, caps this so the substreams roll back to the lowest common
            // version.
            lock (_context._checkpointLock)
            {
                var completed = _context._stateManager.LastCompletedCheckpointVersion;
                if (!_context._restoreCheckpointVersion.HasValue || _context._restoreCheckpointVersion.Value > completed)
                {
                    _context._restoreCheckpointVersion = completed;
                }
            }

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
                // Dependency done signals stashed while a prior startup was still running belong
                // to the aborted generation, the peer re-acks after the rollback. Keeping them
                // would let the first checkpoint after the restart complete without a real
                // acknowledgement from the other substream.
                _context._earlyDependenciesDone.Clear();
            }

            StreamContext.BeforeFailureDisposeForTests?.Invoke(_context.streamName);

            bool blocksClaimed;
            lock (_context._blockClaimLock)
            {
                blocksClaimed = _context._blocksCreated == 1;
                _context._blocksCreated = 0;
            }
            if (!blocksClaimed)
            {
                // The failure happened before the start created the blocks (for example at
                // storage initialization), there is nothing to fault or dispose. Faulting,
                // completing or disposing never-created blocks throws, which would retry
                // this teardown forever. A superseded start that created blocks after this
                // read cleans them up itself when it observes its abort.
                _context._logger.LogDebug("Failure handling skipping block teardown on stream {stream}, the blocks were never created.", _context.streamName);
                return;
            }

            _context.ForEachBlock((key, block) =>
            {
                _context._logger.LogDebug("Failure handling faulting block {block} on stream {stream}", key, _context.streamName);
                block.Fault(new BlockStopException($"Faulting block due to stream failure."));
            });

            _context._logger.LogDebug("Failure handling waiting for block completion on stream {stream}", _context.streamName);
            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });

            // Call failure for all blocks
            StreamContext.RestoreVersionForTests?.Invoke(_context.streamName, _context._restoreCheckpointVersion ?? -1);
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

        // Internal so tests can shorten it, every recovery hop in a test otherwise pays the
        // full settle delay.
        internal static TimeSpan RecoveryRestartDelay = TimeSpan.FromMilliseconds(500);

        private async Task Transition()
        {
            Debug.Assert(_context != null, nameof(_context));

            await Task.Delay(RecoveryRestartDelay);

            // A pending delete takes precedence over a pending stop: the wish holds only the
            // last requested value, but a created delete task means a caller awaits a delete,
            // and a delete implies the stop, the deleted state completes both tasks.
            bool deletePending;
            lock (_context._checkpointLock)
            {
                deletePending = _context._deleteTask != null;
            }
            if (deletePending || _context._wantedState == StreamStateValue.Deleting)
            {
                // A delete was requested during the failure handling, the cleanup has
                // finished so the delete can run now without racing it. The failure
                // handling disposed every block, they must be created before delete can
                // be called, see NotStartedStreamState.DeleteAsync.
                _context.ForEachBlock((key, block) =>
                {
                    block.Setup(_context.streamName, key);
                    block.CreateBlock();
                });
                lock (_context._blockClaimLock)
                {
                    _context._blockGeneration++;
                    _context._blocksCreated = 1;
                }
                await TransitionTo(StreamStateValue.Deleting);
                return;
            }

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
            Debug.Assert(_context != null, nameof(_context));
            // The failure handling may be mid way through disposing the blocks, deleting now
            // would work on the same blocks and state manager concurrently and corrupt them.
            // The wish is honored by Transition when the cleanup has finished.
            _context._wantedState = StreamStateValue.Deleting;
            return Task.CompletedTask;
        }

        public override Task StopAsync()
        {
            Debug.Assert(_context != null, nameof(_context));
            // The failure handling may be mid way through disposing the blocks, a stop
            // checkpoint against them would hang. The wish is honored by Transition when
            // the cleanup has finished.
            _context._wantedState = StreamStateValue.NotStarted;
            return Task.CompletedTask;
        }
    }
}
