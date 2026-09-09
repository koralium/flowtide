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
using FlowtideDotNet.Base.Vertices;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class StoppingStreamState : StreamStateMachineState
    {
        private HashSet<string>? nonCheckpointedEgresses;
        private Checkpoint? _currentCheckpoint;
        private long _stoppingStartedTimestamp;
        private int _stopAllStarted;
        // Drain minted cycles never commit, the peer never matches them.
        private bool _stopCommitTaken;
        // Drain polls readiness, mints no cycles.
        private int _drainPollStarted;
        internal static TimeSpan DrainPollInterval = TimeSpan.FromMilliseconds(25);

        public override Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            return Task.CompletedTask;
        }

        public override Task CallTrigger(string operatorName, string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        public override Task CallTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            Debug.Assert(_context != null, nameof(_context));
            // The stop is already draining the substreams and must not race a delete over
            // the same blocks. The wish is honored when the stop finishes in the not started
            // state, which runs the delete against the stopped stream, the delete task the
            // caller awaits completes when the delete is done.
            _context._wantedState = StreamStateValue.Deleting;
            return Task.CompletedTask;
        }

        public override void EgressCheckpointDone(string name, ILockingEvent? lockingEvent)
        {
            Debug.Assert(_context != null, nameof(_context));

            if (lockingEvent != null && lockingEvent is not ICheckpointEvent)
            {
                // A non checkpoint locking event must not be counted towards the final
                // checkpoint completion, see RunningStreamState.EgressCheckpointDone. This
                // runs before the assert below, a filtered acknowledgement can arrive before
                // the first stop cycle has created the tracking set.
                return;
            }

            lock (_context._checkpointLock)
            {
                if (nonCheckpointedEgresses == null)
                {
                    // No stop cycle has started yet, the acknowledgement belongs to a cycle
                    // of a previous state and is ignored.
                    return;
                }
                nonCheckpointedEgresses.Remove(name);

                // Check if all egresses has done their checkpoint
                if (nonCheckpointedEgresses.Count == 0)
                {
                    StartCheckpointDoneTask();
                }
            }
        }

        public override void EgressDependenciesDone(string name, ILockingEvent? lockingEvent)
        {
            // Peer ack landed, the drain re-checks readiness at once.
            if (Volatile.Read(ref _drainPollStarted) == 1)
            {
                // The ack thread holds exchange and context locks.
                _ = Task.Run(FinishStopGuarded);
            }
        }

        private void StartCheckpointDoneTask()
        {
            Debug.Assert(_context != null, nameof(_context));

            _context._logger.StartCheckpointDoneTask(_context.streamName);
            // The stop commit is claimed as an in-flight state manager write here, at the
            // decision (the caller holds the checkpoint lock), not when the thread pool runs
            // the task: a failure during the stop tears down through StopAll, whose write
            // wait could otherwise read zero in the scheduling gap and dispose the state
            // manager the queued commit is about to write. The task body releases the count
            // in its finally.
            System.Threading.Interlocked.Increment(ref _context._stateManagerWriteCount);
            Task.Factory.StartNew(async (state) =>
            {
                var run = (StoppingStreamState)state!;
                Debug.Assert(run._context != null, nameof(_context));
                Debug.Assert(run._currentCheckpoint != null, nameof(_context));

                bool committed = false;
                try
                {
                    // Holds the task in the window between being scheduled and starting its
                    // work, the window a failure during the stop races.
                    var scheduledHook = StreamContext.CheckpointCommitScheduledHookForTests;
                    if (scheduledHook != null)
                    {
                        await scheduledHook(run._context.streamName);
                    }

                    var commitHook = StreamContext.CheckpointCommitHookForTests;
                    if (commitHook != null)
                    {
                        await commitHook(run._context.streamName, run._context._stateManager.LastCompletedCheckpointVersion);
                    }

                    if (run._stopCommitTaken)
                    {
                        // Only the first stop cycle commits.
                        _context._logger.LogDebug("Stop drain cycle on stream {stream} is waiting for the other substreams, keeping checkpoint version {version}.", _context.streamName, run._context._stateManager.LastCompletedCheckpointVersion);
                    }
                    else
                    {
                        // Write the latest state
                        run._context._lastState = new StreamState(
                            run._currentCheckpoint.CheckpointTime,
                            _context._streamVersionInformation?.Hash ?? string.Empty);

                        run._context._stateManager.Metadata = run._context._lastState;

                        long changesSinceLastCompaction = run._context._stateManager.PageCommitsSinceLastCompaction;
                        var compactionThreshold = (long)(run._context._stateManager.PageCount * 0.3);

                        // Compaction: if more than 30% of the pages has been changed since last compaction, do compaction
                        if (changesSinceLastCompaction > compactionThreshold)
                        {
                            await run._context._stateManager.Compact();
                        }

                        // Take state checkpoint
                        _context._logger.StartingStateManagerCheckpoint(_context.streamName);
                        await run._context._stateManager.CheckpointAsync(false);
                        _context._logger.StateManagerCheckpointDone(_context.streamName);

                        run._stopCommitTaken = true;
                        committed = true;
                    }
                }
                finally
                {
                    System.Threading.Interlocked.Decrement(ref run._context._stateManagerWriteCount);
                }

                if (!committed)
                {
                    // Checkpoint to nobody, nothing written, listeners already notified.
                    return;
                }

                if (_context._notificationReciever != null)
                {
                    _context._notificationReciever.OnCheckpointComplete();
                }

                await _context.ForEachIngressBlockAsync((key, block) =>
                {
                    if (block is IStreamIngressVertex streamIngressVertex)
                    {
                        return streamIngressVertex.CheckpointDone(run._context._stateManager.LastCompletedCheckpointVersion);
                    }
                    return Task.CompletedTask;
                });
                // The egress blocks must also be notified so exchanges send their final
                // checkpoint done message to other substreams, a substream with a pending
                // checkpoint would otherwise wait forever for it and never finish stopping.
                await _context.ForEachEgressBlockAsync((key, block) =>
                {
                    return block.CheckpointDone(run._context._stateManager.LastCompletedCheckpointVersion);
                });
            }, this)
                .Unwrap()
                 .ContinueWith(async (t, state) =>
                 {
                     StoppingStreamState @this = (StoppingStreamState)state!;
                     if (t.IsFaulted)
                     {
                         await _context.OnFailure(t.Exception);
                         @this.CheckpointCompleted();
                         _context._logger.ShutdownCheckpointDone(_context.streamName);
                         await @this.StopAll(faultBlocks: true);
                         return;
                     }
                     // Finish the checkpoint
                     @this.CheckpointCompleted();
                     _context._logger.ShutdownCheckpointDone(_context.streamName);
                     if (@this.AllVerticesReadyToStop())
                     {
                         await @this.StopAll(faultBlocks: false);
                     }
                     else if (Stopwatch.GetElapsedTime(@this._stoppingStartedTimestamp) > _context._dataflowStreamOptions.StopDrainTimeout)
                     {
                         // Another substream is not making progress, it may have crashed or
                         // never started. Stop anyway, data that did not reach the other
                         // substream is regenerated by replay when the streams start again.
                         _context._logger.LogWarning("Stopping stream {stream} timed out waiting for other substreams to drain, stopping anyway.", _context.streamName);
                         await @this.StopAll(faultBlocks: true);
                     }
                     else
                     {
                         // Waiting on the peer, another cycle cannot help, poll readiness.
                         @this.EnsureDrainPoll();
                     }
                 }, this)
                 .Unwrap();
        }

        /// <summary>
        /// Starts the readiness poll after the committing cycle, runs once.
        /// </summary>
        private void EnsureDrainPoll()
        {
            Debug.Assert(_context != null, nameof(_context));

            if (Interlocked.Exchange(ref _drainPollStarted, 1) == 1)
            {
                return;
            }
            var context = _context;
            _ = Task.Run(async () =>
            {
                try
                {
                    // Dispose leaves the state, poll must not outlive it.
                    while (Volatile.Read(ref _stopAllStarted) == 0 && !context.IsDisposed && context.currentState == StreamStateValue.Stopping)
                    {
                        await Task.Delay(DrainPollInterval);
                        if (await TryFinishStop())
                        {
                            return;
                        }
                    }
                }
                catch (Exception e)
                {
                    context._logger.LogError(e, "The stop drain poll on stream {stream} failed.", context.streamName);
                    await context.OnFailure(e);
                }
            });
        }

        /// <summary>
        /// Readiness re-check off the caller's thread, failures reach OnFailure.
        /// </summary>
        private async Task FinishStopGuarded()
        {
            Debug.Assert(_context != null, nameof(_context));

            try
            {
                await TryFinishStop();
            }
            catch (Exception e)
            {
                _context._logger.LogError(e, "The stop drain on stream {stream} failed.", _context.streamName);
                await _context.OnFailure(e);
            }
        }

        /// <summary>
        /// Ends the drain on ready or timeout, StopAll runs once.
        /// </summary>
        private async Task<bool> TryFinishStop()
        {
            Debug.Assert(_context != null, nameof(_context));

            // Dispose tore the blocks down, nothing left to stop.
            if (Volatile.Read(ref _stopAllStarted) == 1 || _context.IsDisposed)
            {
                return true;
            }
            if (AllVerticesReadyToStop())
            {
                await StopAll(faultBlocks: false);
                return true;
            }
            if (Stopwatch.GetElapsedTime(_stoppingStartedTimestamp) > _context._dataflowStreamOptions.StopDrainTimeout)
            {
                // Peer stalled, stop anyway, the fault path rolls it back.
                _context._logger.LogWarning("Stopping stream {stream} timed out waiting for other substreams to drain, stopping anyway.", _context.streamName);
                await StopAll(faultBlocks: true);
                return true;
            }
            return false;
        }

        private bool AllVerticesReadyToStop()
        {
            Debug.Assert(_context != null, nameof(_context));

            bool ready = true;
            _context.ForEachBlock((key, block) =>
            {
                if (block is IStreamIngressVertex ingressVertex && !ingressVertex.ReadyToStop)
                {
                    _context._logger.LogDebug("Ingress {operator} is not ready to stop, the drain keeps polling.", key);
                    ready = false;
                }
                else if (block is IStreamEgressVertex egressVertex && !egressVertex.ReadyToStop)
                {
                    _context._logger.LogDebug("Egress {operator} is not ready to stop, the drain keeps polling.", key);
                    ready = false;
                }
            });
            return ready;
        }

        private void CheckpointCompleted()
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                _context._minimumIntervalThrottleArmed = true;
                if (_context.checkpointTask != null)
                {
                    _context._scheduleCheckpointTask = null;
                    _context.checkpointTask.SetResult();
                    _context.checkpointTask = null;
                    _currentCheckpoint = null;
                }
            }
        }

        /// <summary>
        /// Runs the sinks' Compact for the durable stop checkpoint.
        /// </summary>
        private async Task CompactEgressBlocks()
        {
            Debug.Assert(_context != null, nameof(_context));

            // Claimed like the running compaction, a dispose waits for it.
            Interlocked.Increment(ref _context._stateManagerWriteCount);
            try
            {
                foreach (var block in _context.egressBlocks)
                {
                    try
                    {
                        await block.Value.Compact();
                    }
                    catch (Exception e)
                    {
                        // A failed hook must not wedge the stop, OnInitialize reconciles.
                        _context._logger.LogError(e, "Compaction of {operator} on stream {stream} failed during the stop.", block.Key, _context.streamName);
                    }
                }
            }
            finally
            {
                Interlocked.Decrement(ref _context._stateManagerWriteCount);
            }
        }

        private async Task StopAll(bool faultBlocks)
        {
            Debug.Assert(_context != null, nameof(_context));

            // StopAll can be reached from multiple paths at once, for example the stop
            // checkpoint continuation racing a block failure whose OnFailure also stops all.
            // The blocks must only be completed and disposed once, dispose is not idempotent
            // in every operator.
            if (Interlocked.Exchange(ref _stopAllStarted, 1) == 1)
            {
                return;
            }

            // Wait for an in-flight or scheduled stop checkpoint commit to finish before
            // tearing anything down, faulting or disposing blocks or the state manager while
            // it is being written corrupts it - the same wait the failure state's teardown
            // runs. On the graceful path the commit already completed and this is a no-op.
            // Bounded, a write wedged on unresponsive storage cannot be made safe by waiting
            // and must not hang the stop forever.
            var writeWaitStart = Stopwatch.GetTimestamp();
            while (true)
            {
                if (Volatile.Read(ref _context._stateManagerWriteCount) > 0)
                {
                    if (Stopwatch.GetElapsedTime(writeWaitStart) > _context._dataflowStreamOptions.StopDrainTimeout)
                    {
                        _context._logger.LogWarning("Stop teardown on stream {stream} proceeded while a state manager write was still active after {timeout}, the write may be wedged on storage.", _context.streamName, _context._dataflowStreamOptions.StopDrainTimeout);
                        break;
                    }
                    await Task.Delay(10);
                    continue;
                }
                // The stop commit claims its write count at the decision point, under the
                // checkpoint lock, before its task is scheduled. Re-reading under that lock
                // means every claim decided before this teardown is visible, so a zero here
                // cannot race a commit that was scheduled but not yet counted.
                bool settled;
                lock (_context._checkpointLock)
                {
                    settled = Volatile.Read(ref _context._stateManagerWriteCount) == 0;
                }
                if (settled)
                {
                    break;
                }
            }

            if (faultBlocks)
            {
                StreamContext.BeforeFailureDisposeForTests?.Invoke(_context.streamName);
                // The stop did not finish its drain, the pipeline can hold in flight data
                // that has nowhere to go. Graceful completion waits for every block to
                // drain its queues, a blocked pipeline then never completes and the stop
                // hangs forever. Faulting discards the queued data, it is regenerated by
                // replay when the stream starts again.
                _context.ForEachBlock((key, block) =>
                {
                    block.Fault(new BlockStopException("Faulting block due to stream failure during stop."));
                });
            }
            else
            {
                // Clean stop, sinks commit from Compact, peers confirmed.
                await CompactEgressBlocks();
                _context.ForEachBlock((key, block) =>
                {
                    block.Complete();
                });
            }
            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });

            if (faultBlocks)
            {
                // Failing stop, roll peers back like the failure state does.
                long restoreVersion;
                lock (_context._checkpointLock)
                {
                    var completed = _context._stateManager.LastCompletedCheckpointVersion;
                    if (!_context._restoreCheckpointVersion.HasValue || _context._restoreCheckpointVersion.Value > completed)
                    {
                        _context._restoreCheckpointVersion = completed;
                    }
                    restoreVersion = _context._restoreCheckpointVersion.Value;
                }
                await _context.ForEachBlockAsync(async (key, block) =>
                {
                    await block.OnFailure(restoreVersion);
                });
            }

            await _context.ForEachBlockAsync(async (key, block) =>
            {
                await block.DisposeAsync();
            });

            _context._stateManager.Dispose();

            // A teardown minted cycle reached nobody, drop its state.
            lock (_context._checkpointLock)
            {
                _context.checkpointTask?.TrySetCanceled();
                _context.checkpointTask = null;
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
            }

            await TransitionTo(StreamStateValue.NotStarted);
            _context._logger.StoppedStream(_context.streamName);

            lock (_context._checkpointLock)
            {
                if (_context._stopTask != null)
                {
                    _context._stopTask.SetResult();
                    _context._stopTask = null;
                }
            }
        }

        public override Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null);
            // The stop supersedes a pause, the drain must never park on the pause marker.
            _context.SetStatus(StreamStatus.Stopping);
            _context._logger.StoppingStream(_context.streamName);
            // Monotonic clock, a wall clock step during the drain would extend or cut the
            // drain timeout by the step size.
            _stoppingStartedTimestamp = Stopwatch.GetTimestamp();
            // Bypasses the minimum checkpoint interval, see the reschedule in
            // StartCheckpointDoneTask, so the stop is not delayed by that interval.
            _context.TryScheduleCheckpointIn(TimeSpan.FromMilliseconds(1), default, bypassMinimumInterval: true);
            return Task.CompletedTask;
        }

        public override async Task OnFailure()
        {
            // On failure stop all
            await StopAll(faultBlocks: true);
        }

        public override Task StartAsync()
        {
            throw new NotSupportedException("Stream is stopping.");
        }

        public override Task TriggerCheckpoint(bool isScheduled = false)
        {
            Debug.Assert(_context != null, nameof(_context));

            StopStreamCheckpoint? checkpoint = null;
            lock (_context._checkpointLock)
            {
                // Only support a single concurrent checkpoint for now for simplicity
                if (_context.checkpointTask != null)
                {
                    return _context.checkpointTask.Task;
                }
                if (Volatile.Read(ref _stopAllStarted) == 1)
                {
                    // Teardown began, a cycle now reaches nobody.
                    if (isScheduled)
                    {
                        _context._scheduleCheckpointTask = null;
                        _context._triggerCheckpointTime = null;
                        _context._scheduleCheckpointCancelSource = null;
                    }
                    return Task.CompletedTask;
                }
                _context._logger.StartingShutdownCheckpoint(_context.streamName);
                nonCheckpointedEgresses = new HashSet<string>();
                foreach (var key in _context.egressBlocks.Keys)
                {
                    nonCheckpointedEgresses.Add(key);
                }
                _context.checkpointTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var newTime = _context.producingTime + 1;
                checkpoint = new StopStreamCheckpoint(_context.producingTime, newTime, _context._stateManager.CurrentVersion);
                _context.producingTime = newTime;
                _currentCheckpoint = checkpoint;

                if (isScheduled)
                {
                    _context._scheduleCheckpointTask = null;
                    _context._triggerCheckpointTime = null;
                    _context._scheduleCheckpointCancelSource = null;
                }
                foreach (var ingress in _context.ingressBlocks)
                {
                    ingress.Value.DoLockingEvent(checkpoint);
                }
            }
            return _context.checkpointTask.Task;
        }

        public override Task StopAsync()
        {
            return Task.CompletedTask;
        }
    }
}
