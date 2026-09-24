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
        public override Task Initialize(StreamStateValue previousState)
        {
            // TransitionTo already fenced the run synchronously. A callback may await
            // this acknowledgement while it still owns a write claim or a dataflow block.
            // Recovery must run independently so it can wait for that callback to exit.
            _context!.ForEachVersionAgreement(agreement => agreement.AbortPendingOperations());
            lock (_lock)
            {
                _currentTask ??= Task.Run(Recover);
            }
            return Task.CompletedTask;
        }

        private async Task Recover()
        {
            Debug.Assert(_context != null, nameof(_context));
            while (_context.IsCurrentState(this))
            {
                try
                {
                    await _context.CheckForPauseAsync();
                    if (_context.IsDisposed) return;
                    await StopAndDispose();
                    if (!_context.IsCurrentState(this)) return;
                    _context.SetStatus(StreamStatus.Failing);
                    await Transition();
                    return;
                }
                catch (Exception e)
                {
                    if (!_context.IsCurrentState(this)) return;
                    _context._logger.FailedStopAndDispose(e, _context.streamName);
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }

        private async Task StopAndDispose()
        {
            Debug.Assert(_context != null, nameof(_context));

            // This includes vertex initialization and finality callbacks. A timeout
            // cannot transfer storage ownership while any of them still uses it.
            await _context.WaitForStateManagerToSettle();
            if (_context.IsDisposed) return;

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
                // The version to beat before the backoff clears, see CheckpointCompleted
                _context._checkpointVersionAtLastFailure = _context._restoreCheckpointVersion.Value;
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
                _context._currentProvidedCheckpointToken = default;
                _context._scheduledProvidedCheckpointToken = default;
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

            await _context._blockTeardownGate.WaitAsync();
            try
            {
                if (_context.IsDisposed) return;
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
            finally { _context._blockTeardownGate.Release(); }

        }

        // Internal so tests can shorten it, every recovery hop in a test otherwise pays the
        // full settle delay.
        // The backoff grows the slice count, never the slice length
        internal static TimeSpan RecoveryRestartDelay = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// How long to wait before the next restart, in slices.
        /// Flat inside the grace count, then doubling to the cap.
        /// </summary>
        private int RestartDelaySlices(int consecutiveFailures)
        {
            Debug.Assert(_context != null, nameof(_context));

            var options = _context._dataflowStreamOptions;
            var overGrace = consecutiveFailures - options.FailureRestartGraceCount;
            if (overGrace <= 0)
            {
                return 1;
            }
            // Shifting past the cap would overflow, and clamps there anyway
            if (overGrace >= 31)
            {
                return options.MaxFailureRestartDelaySlices;
            }
            return Math.Min(1 << overGrace, options.MaxFailureRestartDelaySlices);
        }

        private async Task Transition()
        {
            Debug.Assert(_context != null, nameof(_context));

            var consecutiveFailures = Interlocked.Exchange(ref _context._realFailurePending, 0) == 1
                ? Interlocked.Increment(ref _context._consecutiveFailures)
                : Volatile.Read(ref _context._consecutiveFailures);
            var slices = RestartDelaySlices(consecutiveFailures);
            if (slices > 1 && consecutiveFailures == _context._dataflowStreamOptions.FailureRestartGraceCount + 1)
            {
                // Logged once as the backoff starts, not on every hop
                _context._logger.LogWarning("Stream {stream} has failed {count} times in a row without completing a checkpoint, backing off the restarts up to {max} times the restart delay.", _context.streamName, consecutiveFailures, _context._dataflowStreamOptions.MaxFailureRestartDelaySlices);
            }

            for (int slice = 0; slice < slices; slice++)
            {
                await Task.Delay(RecoveryRestartDelay);

                if (_context.IsDisposed)
                {
                    // The transition below is refused anyway, skip the rest
                    return;
                }
                if (_context._wantedState == StreamStateValue.NotStarted ||
                    _context._wantedState == StreamStateValue.Deleting)
                {
                    // A stop or delete is honored without waiting further
                    break;
                }
            }

            if (_context.IsDisposed)
            {
                // A dispose parks the same wish the branches below honor
                return;
            }

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
                lock (_context._checkpointLock)
                {
                    // Reschedule checkpoint, a superseded timer has nothing to reschedule.
                    if (_context.TryConsumeFiringSchedule())
                    {
                        _context.TryScheduleCheckpointIn_NoLock(TimeSpan.FromSeconds(10), default);
                    }
                }
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
