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
        private int _failureRequested;
        internal override bool AllowsPublication => Volatile.Read(ref _failureRequested) == 0;
        // Drain minted cycles never commit, the peer never matches them.
        private bool _stopCommitTaken;
        // Drain polls readiness, mints no cycles.
        private int _drainPollStarted;
        internal static TimeSpan DrainPollInterval = TimeSpan.FromMilliseconds(25);
        // One stop decision at a time, ready beats timeout.
        private readonly object _finishLock = new object();
        // Teardown claimed by the drain timeout, not a failure.
        private bool _drainTimedOut;

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
            // the task: a failure during the stop tears down through StopAllClaimed, whose write
            // wait could otherwise read zero in the scheduling gap and dispose the state
            // manager the queued commit is about to write. The claim also covers the engine's
            // CheckpointDone callbacks, which are not joined by dataflow block completion.
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

                        // Compacted before the stop checkpoint like on main, but only once the stop commits have landed and are counted.
                        await run._context._stateManager.WaitForCommitsAsync();

                        long changesSinceLastCompaction = run._context._stateManager.PageCommitsSinceLastCompaction;
                        var compactionThreshold = (long)(run._context._stateManager.PageCount * 0.3);

                        // Compaction: if more than 30% of the pages has been changed since last compaction, do compaction
                        // An unagreed version can still be rolled back below.
                        if (changesSinceLastCompaction > compactionThreshold && run._context.IsVersionAgreed(run._context._stateManager.LastCompletedCheckpointVersion))
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
                    if (!committed || !run._context.IsCurrentState(run))
                    {
                        return;
                    }

                    var version = run._context._stateManager.LastCompletedCheckpointVersion;
                    run._context._notificationReciever?.OnCheckpointComplete();

                    await run._context.ForEachIngressBlockAsync((key, block) =>
                    {
                        return run._context.IsCurrentState(run) && block is IStreamIngressVertex ingress
                            ? ingress.CheckpointDone(version) : Task.CompletedTask;
                    });
                    // Exchanges must notify their peers too, while this run still owns
                    // the callbacks. A callback may itself request failure and fence the rest.
                    await run._context.ForEachEgressBlockAsync((key, block) =>
                    {
                        return run._context.IsCurrentState(run)
                            ? block.CheckpointDone(version) : Task.CompletedTask;
                    });
                }
                finally
                {
                    System.Threading.Interlocked.Decrement(ref run._context._stateManagerWriteCount);
                }
            }, this)
                .Unwrap()
                 .ContinueWith(async (t, state) =>
                 {
                     StoppingStreamState @this = (StoppingStreamState)state!;
                     if (t.IsFaulted || t.IsCanceled)
                     {
                         if (@this._context!.IsCurrentState(@this))
                         {
                             await @this._context.OnFailure(t.Exception ?? (Exception)new TaskCanceledException(t));
                         }
                         return;
                     }
                     // Finish the checkpoint
                     if (!@this.CheckpointCompleted()) return;
                     _context._logger.ShutdownCheckpointDone(_context.streamName);
                     if (!await @this.TryFinishStop())
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
                if (_context.IsCurrentState(this)) await _context.OnFailure(e);
            }
        }

        /// <summary>
        /// Ends the drain on ready or timeout, claiming teardown once.
        /// </summary>
        private async Task<bool> TryFinishStop()
        {
            Debug.Assert(_context != null, nameof(_context));

            bool faultBlocks;
            lock (_finishLock)
            {
                // Dispose tore the blocks down, nothing left to stop.
                if (Volatile.Read(ref _stopAllStarted) == 1 || !_context.IsCurrentState(this))
                {
                    return true;
                }
                if (AllVerticesReadyToStop())
                {
                    faultBlocks = false;
                }
                else if (Stopwatch.GetElapsedTime(_stoppingStartedTimestamp) > _context._dataflowStreamOptions.StopDrainTimeout)
                {
                    // Peer stalled, stop anyway, the fault path rolls it back.
                    _context._logger.LogWarning("Stopping stream {stream} timed out waiting for other substreams to drain, stopping anyway.", _context.streamName);
                    _drainTimedOut = true;
                    faultBlocks = true;
                }
                else
                {
                    return false;
                }
                // Claimed with the decision, a failure may have claimed first.
                if (Interlocked.Exchange(ref _stopAllStarted, 1) == 1)
                {
                    return true;
                }
            }
            await StopAllClaimed(faultBlocks);
            return true;
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

        private bool CheckpointCompleted()
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                // The callback task released its ownership before this continuation ran.
                // Teardown may already have completed and a successor may own checkpointTask.
                if (!_context.IsCurrentState(this)) return false;
                _context._minimumIntervalThrottleArmed = true;
                if (_context.checkpointTask != null)
                {
                    _context._scheduleCheckpointTask = null;
                    _context.checkpointTask.SetResult();
                    _context.checkpointTask = null;
                    _currentCheckpoint = null;
                }
                return true;
            }
        }

        /// <summary>
        /// Commits the stop version only if every connected stream is already durable at it,
        /// drain readiness says nothing about that. Otherwise the next start commits it.
        /// </summary>
        private async Task CommitStopVersionIfAgreed()
        {
            Debug.Assert(_context != null, nameof(_context));

            var version = _context._stateManager.LastCompletedCheckpointVersion;
            // Claimed before the decision, a dispose either waits for it or is seen here.
            Interlocked.Increment(ref _context._stateManagerWriteCount);
            try
            {
                if (_context.IsDisposed)
                {
                    return;
                }
                if (!_context.IsVersionAgreed(version))
                {
                    _context._logger.LogInformation("Stream {stream} stops before version {version} is agreed, the next start commits it.", _context.streamName, version);
                    return;
                }
                await _context.CommitVersionOnEgresses(version, this);
            }
            catch (Exception e)
            {
                // A failed commit must not wedge the stop, the next start commits it.
                _context._logger.LogError(e, "Committing version {version} on stream {stream} failed during the stop.", version, _context.streamName);
            }
            finally
            {
                Interlocked.Decrement(ref _context._stateManagerWriteCount);
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
                        if (!_context.IsCurrentState(this)) return;
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

        private async Task StopAllClaimed(bool faultBlocks)
        {
            Debug.Assert(_context != null, nameof(_context));

            // On the graceful path the stop commit already completed and this is a no-op.
            await _context.WaitForStateManagerToSettle("Stop teardown");

            await _context._blockTeardownGate.WaitAsync();
            try
            {
                if (_context.IsDisposed) return;
                if (faultBlocks)
                {
                    StreamContext.BeforeFailureDisposeForTests?.Invoke(_context.streamName);
                    if (_drainTimedOut && AllVerticesReadyToStop())
                    {
                        // Confirmed during the teardown wait, stop cleanly after all.
                        _context._logger.LogInformation("Stopping stream {stream} was confirmed by the other substreams before its teardown, stopping cleanly.", _context.streamName);
                        faultBlocks = false;
                    }
                }
                if (!faultBlocks)
                {
                    await CommitStopVersionIfAgreed();
                    if (AllowsPublication) await CompactEgressBlocks();
                    faultBlocks = !AllowsPublication;
                }
                // A timed-out drain can still become a clean stop above. Cancel only
                // once that decision is final; explicit failures already requested it.
                if (faultBlocks) _context.RequestVertexCancellation();
                await _context.WaitForVertexCancellation();
                lock (_context._blockClaimLock) { _context._blocksCreated = 0; }
                if (faultBlocks)
                {
                    _context.ForEachBlock((key, block) =>
                        block.Fault(new BlockStopException("Faulting block due to stream failure during stop.")));
                }
                else
                {
                    _context.ForEachBlock((key, block) => block.Complete());
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
                // The run is over for the connected streams too, a failing stop told them already.
                _context.ForEachVersionAgreement(agreement => agreement.StreamStopped());

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
            finally { _context._blockTeardownGate.Release(); }

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

        public override Task OnFailure()
        {
            Interlocked.Exchange(ref _failureRequested, 1);
            _context!.RequestVertexCancellation();
            _context.ForEachVersionAgreement(agreement => agreement.AbortPendingOperations());
            // Fence stop-checkpoint admission before acknowledging a callback's request.
            if (Interlocked.Exchange(ref _stopAllStarted, 1) == 0)
            {
                _ = Task.Run(async () =>
                {
                    try { await StopAllClaimed(faultBlocks: true); }
                    catch (Exception e)
                    {
                        _context!._logger.LogError(e, "Failure teardown during stop failed.");
                        _context.FailTeardownWaiters(e);
                    }
                });
            }
            return Task.CompletedTask;
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
                // Same lock scope as the cycle start, a replacement cannot interleave.
                if (isScheduled && !_context.TryConsumeFiringSchedule())
                {
                    return Task.CompletedTask;
                }
                // Only support a single concurrent checkpoint for now for simplicity
                if (_context.checkpointTask != null)
                {
                    return _context.checkpointTask.Task;
                }
                if (Volatile.Read(ref _stopAllStarted) == 1)
                {
                    // Teardown began, a cycle now reaches nobody.
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
