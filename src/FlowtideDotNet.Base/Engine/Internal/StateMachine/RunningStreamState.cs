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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class RunningStreamState : StreamStateMachineState
    {
        private Task? _initialBatchTask;
        private readonly object _lock = new object();
        private HashSet<string>? nonCheckpointedEgresses;
        private HashSet<string>? waitingForDependencies;
        // Initialized at declaration since a dependency done signal from another substream
        // can arrive before Initialize has run on this state.
        private readonly HashSet<string> _preCompletedDependencies = new HashSet<string>();
        private Checkpoint? _currentCheckpoint;
        private bool _doingCheckpoint = false;
        private bool _initialCheckpointTaken = false;
        private bool _compactionStarted = false;

        public override void EgressCheckpointDone(string name, ILockingEvent? lockingEvent)
        {
            Debug.Assert(_context != null, nameof(_context));

            if (lockingEvent != null && lockingEvent is not ICheckpointEvent)
            {
                // A late acknowledgement of a non checkpoint locking event, for example an
                // init watermarks event from the starting phase. Counting it would commit an
                // in-flight checkpoint before the barrier reached the egress.
                return;
            }

            lock (_context._checkpointLock)
            {
                if (nonCheckpointedEgresses == null)
                {
                    // No checkpoint has started in this state instance yet, the
                    // acknowledgement belongs to a cycle of a previous state instance and is
                    // ignored.
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
            Debug.Assert(_context != null, nameof(_context));

            if (lockingEvent != null && lockingEvent is not ICheckpointEvent)
            {
                // See EgressCheckpointDone, a non checkpoint locking event must not be counted
                // towards checkpoint completion.
                return;
            }

            lock (_context._checkpointLock)
            {
                if (waitingForDependencies == null || !waitingForDependencies.Contains(name))
                {
                    // This stream has not yet started checkpointing, but a dependency is already done
                    // Add it to pre completed
                    _context._logger.LogDebug("Operator {Operator} has completed dependencies before checkpoint started on stream {Stream}, marking as precompleted.", name, _context.streamName);
                    _preCompletedDependencies.Add(name);
                    return;
                }
                waitingForDependencies.Remove(name);
                _context._logger.LogDebug("Dependencies done for operator {Operator} on stream {Stream}, remaining: [{Remaining}], initial checkpoint taken: {InitialCheckpointTaken}", name, _context.streamName, string.Join(",", waitingForDependencies), _initialCheckpointTaken);

                // Check if all egresses has done their dependencies. Do not start
                // compaction once a teardown has moved the stream out of the running state,
                // it would write the state manager the teardown is about to dispose.
                if (waitingForDependencies.Count > 0 || !_initialCheckpointTaken || _compactionStarted || _context.currentState != StreamStateValue.Running)
                {
                    return;
                }
                _compactionStarted = true;
            }

            Task.Factory.StartNew(async (state) =>
            {
                var run = (RunningStreamState)state!;
                Debug.Assert(run._context != null);

                try
                {
                    await DoCompaction();
                }
                catch(Exception e)
                {
                    await run._context.OnFailure(e);
                    return;
                }
                
                // Finish the checkpoint
                run.CheckpointCompleted();
                run._context._logger.CheckpointDone(_context.streamName);
            }, this, default, TaskCreationOptions.None, TaskScheduler.Default)
                .Unwrap();
        }

        private void StartCheckpointDoneTask()
        {
            Debug.Assert(_context != null, nameof(_context));

            _context._logger.StartCheckpointDoneTask(_context.streamName);
            Task.Factory.StartNew(async (state) =>
            {
                var run = (RunningStreamState)state!;
                Debug.Assert(run._context != null, nameof(_context));
                Debug.Assert(run._currentCheckpoint != null, nameof(_context));
                Debug.Assert(run.waitingForDependencies != null);

                System.Threading.Interlocked.Increment(ref run._context._stateManagerWriteCount);
                try
                {
                    var commitHook = StreamContext.CheckpointCommitHookForTests;
                    if (commitHook != null)
                    {
                        await commitHook(run._context.streamName, run._context._stateManager.LastCompletedCheckpointVersion);
                    }

                    // Write the latest state
                    run._context._lastState = new StreamState(
                        run._currentCheckpoint.CheckpointTime,
                        _context._streamVersionInformation?.Hash ?? string.Empty);

                    run._context._stateManager.Metadata = run._context._lastState;

                    await _context.ForEachBlockAsync(static async (key, block) =>
                    {
                        await block.BeforeSaveCheckpoint();
                    });

                    // Take state checkpoint
                    _context._logger.StartingStateManagerCheckpoint(_context.streamName);
                    await run._context._stateManager.CheckpointAsync(false);
                    _context._logger.StateManagerCheckpointDone(_context.streamName);

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
                    await _context.ForEachEgressBlockAsync((key, block) =>
                    {
                        return block.CheckpointDone(run._context._stateManager.LastCompletedCheckpointVersion);
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
                     RunningStreamState @this = (RunningStreamState)state!;
                     Debug.Assert(@this.waitingForDependencies != null);
                     if (t.IsFaulted)
                     {
                         await _context.OnFailure(t.Exception);
                         return;
                     }

                     lock (_context._checkpointLock)
                     {
                         _initialCheckpointTaken = true;
                         // Check if all egresses has done their dependencies. Do not start
                         // compaction once a teardown has moved the stream out of the running
                         // state, it would write the state manager the teardown disposes.
                         if (@this.waitingForDependencies.Count > 0 || _compactionStarted || _context.currentState != StreamStateValue.Running)
                         {
                             return;
                         }
                         _compactionStarted = true;
                     }

                     try
                     {
                         await DoCompaction();
                     }
                     catch (Exception e)
                     {
                         await _context.OnFailure(e);
                         return;
                     }

                     
                     // Finish the checkpoint
                     @this.CheckpointCompleted();
                     _context._logger.CheckpointDone(_context.streamName);
                 }, this)
                 .Unwrap();
        }

        private async Task DoCompaction()
        {
            Debug.Assert(_context != null);

            // Compaction writes the state manager, so it counts as an in-flight write a
            // teardown must wait for, the same as the checkpoint commit. This runs after the
            // commit body in a separate continuation, so the count is taken again here.
            System.Threading.Interlocked.Increment(ref _context._stateManagerWriteCount);
            try
            {
                var compactionHook = StreamContext.CompactionHookForTests;
                if (compactionHook != null)
                {
                    await compactionHook(_context.streamName);
                }

                // After writing do compaction
                _context._logger.StartingCompactionOnVertices(_context.streamName);
                List<Task> tasks = new List<Task>();
                foreach (var ingressNode in _context.ingressBlocks)
                {
                    tasks.Add(ingressNode.Value.Compact());
                }
                foreach (var block in _context.propagatorBlocks)
                {
                    tasks.Add(block.Value.Compact());
                }
                foreach (var block in _context.egressBlocks)
                {
                    tasks.Add(block.Value.Compact());
                }

                await Task.WhenAll(tasks);

                await _context._stateManager.Compact();
                _context._logger.CompactionDoneOnVertices(_context.streamName);
            }
            finally
            {
                System.Threading.Interlocked.Decrement(ref _context._stateManagerWriteCount);
            }
        }

        private void CheckpointCompleted()
        {
            Debug.Assert(_context != null, nameof(_context));
            StreamStateValue? wishTransition = null;
            lock (_context._checkpointLock)
            {
                if (_context.RawStatus == StreamStatus.Failing)
                {
                    // If the stream was in the failure status, we can now set it to running to mark that it is operational
                    _context.SetStatus(StreamStatus.Running);
                }
                _context._initialCheckpointTaken = true;
                if (_context.checkpointTask != null)
                {
                    _context._scheduleCheckpointTask = null;
                    _context.checkpointTask.SetResult();
                    _context.checkpointTask = null;
                    _currentCheckpoint = null;
                    _context._currentProvidedCheckpointVersion = default;

                    if (_context._wantedState == StreamStateValue.NotStarted ||
                        _context._wantedState == StreamStateValue.Deleting)
                    {
                        // A stop or delete was requested, a completed cycle is a safe point
                        // to honor it. Must not depend on a checkpoint being in progress,
                        // initial data completion also lands here without one.
                        _doingCheckpoint = false;
                        if (_context._wantedState == StreamStateValue.NotStarted)
                        {
                            // A stop that arrived while this placeholder held the checkpoint
                            // slot already transitioned the stream to Stopping, whose
                            // Initialize could only queue its stop cycle as inQueueCheckpoint
                            // because the slot was taken. Now that the slot is free the queued
                            // cycle must be promoted to a real schedule: the TransitionTo below
                            // is then a no-op (already Stopping) and nothing else would ever
                            // start the stop drain, hanging the stop forever. A failed
                            // promotion means a schedule already exists, fine during a stop, so
                            // it does not throw. A delete needs no promotion, it tears down
                            // directly in the deleting state and runs no checkpoint cycle, so
                            // scheduling one would only leave a checkpoint the deleting state
                            // ignores.
                            TryPromoteQueuedCheckpoint();
                        }
                        // The transition takes the context lock and must not run under the
                        // checkpoint lock: checkpoint done acknowledgements from other
                        // substreams take the context lock first and the checkpoint lock
                        // second, transitioning here would deadlock with them.
                        wishTransition = _context._wantedState == StreamStateValue.Deleting
                            ? StreamStateValue.Deleting
                            : StreamStateValue.Stopping;
                    }
                    else
                    {
                        _doingCheckpoint = false;
                        if (!TryPromoteQueuedCheckpoint())
                        {
                            throw new InvalidOperationException("Checkpoint could not be scheduled.");
                        }
                    }
                }
            }
            if (wishTransition.HasValue)
            {
                TransitionTo(wishTransition.Value);
            }
        }

        /// <summary>
        /// Promotes a checkpoint that was queued while a cycle held the checkpoint slot to a
        /// real schedule, now that the slot is free. Returns false only when there is a queued
        /// checkpoint that could not be scheduled because an earlier schedule already exists;
        /// true when it promoted the queued checkpoint or there was nothing queued. Must be
        /// called while holding the checkpoint lock.
        /// </summary>
        private bool TryPromoteQueuedCheckpoint()
        {
            Debug.Assert(_context != null, nameof(_context));
            if (!_context.inQueueCheckpoint.HasValue)
            {
                return true;
            }
            var span = _context.inQueueCheckpoint.Value.Subtract(DateTimeOffset.UtcNow);
            if (span.TotalMilliseconds < 0)
            {
                span = TimeSpan.FromMilliseconds(1);
            }
            if (_context.TryScheduleCheckpointIn_NoLock(span, _context._scheduledProvidedCheckpointVersion))
            {
                _context._scheduledProvidedCheckpointVersion = default;
                _context.inQueueCheckpoint = null;
                return true;
            }
            return false;
        }

        public override Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));

            if (_context.RawStatus != StreamStatus.Failing)
            {
                // Failure status is removed when a checkpoint has been made, since a failure could happen in the middle of doing a checkpoint
                _context.SetStatus(StreamStatus.Running);
            }

            // Apply the pause gates when the stream was paused before it started or while
            // it recovered, the stream then comes up with its data paths gated.
            _context.SyncPauseGates();

            _context._logger.StreamIsInRunningState(_context.streamName);
            lock (_lock)
            {
                if (_initialBatchTask != null)
                {
                    return Task.CompletedTask;
                }

                lock (_context._checkpointLock)
                {
                    // Reset the checkpoint version after the stream is in a running state
                    _context._restoreCheckpointVersion = default;

                    // Dependencies done signals that arrived while the stream was starting are
                    // consumed by the first checkpoint
                    foreach (var earlyDependency in _context._earlyDependenciesDone)
                    {
                        _preCompletedDependencies.Add(earlyDependency);
                    }
                    _context._earlyDependenciesDone.Clear();
                }

                if (_context._dataflowStreamOptions.WaitForCheckpointAfterInitialData)
                {
                    // Set the checkpoint task to stop any other checkpoint from happening
                    _context.checkpointTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                _initialBatchTask = Task.Factory.StartNew(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    foreach (var block in _context.ingressBlocks)
                    {
                        var key = block.Key;
                        // Signal to ingress blocks that all has been initialized and it can now start accepting data
                        tasks.Add(block.Value.InitializationCompleted().ContinueWith(t =>
                        {
                            _context._logger.LogDebug("Ingress {operator} completed its initial data, faulted: {faulted}", key, t.IsFaulted);
                            return t;
                        }).Unwrap());
                    }
                    await Task.WhenAll(tasks);
                    _context._logger.LogDebug("All ingress blocks completed their initial data");
                })
                    .Unwrap()
                    .ContinueWith((t) =>
                    {
                        lock (_lock)
                        {
                            _initialBatchTask = null;
                        }
                        if (t.IsFaulted)
                        {
                            return _context.OnFailure(t.Exception);
                        }
                        if (_context._dataflowStreamOptions.WaitForCheckpointAfterInitialData)
                        {
                            CheckpointCompleted();
                        }

                        return Task.CompletedTask;
                    })
                    .Unwrap();
            }
            return Task.CompletedTask;
        }

        public override Task OnFailure()
        {
            return TransitionTo(StreamStateValue.Failure);
        }

        public override Task TriggerCheckpoint(bool isScheduled = false)
        {
            Debug.Assert(_context != null, nameof(_context));

            StreamStateValue? wishTransition = null;
            lock (_context._checkpointLock)
            {
                // If we are stopping or deleting, we should not do a checkpoint
                if (_context._wantedState == StreamStateValue.NotStarted ||
                    _context._wantedState == StreamStateValue.Deleting)
                {
                    // A stop or delete has been requested, no new checkpoint should start.
                    // The wish must be honored here, between checkpoints nothing else picks
                    // it up and the caller would wait forever.
                    if (isScheduled)
                    {
                        _context._scheduleCheckpointTask = null;
                        _context._triggerCheckpointTime = null;
                        _context._scheduleCheckpointCancelSource = null;
                    }
                    if (_doingCheckpoint)
                    {
                        // A checkpoint is already in progress, its completion honors the
                        // wish.
                        return Task.CompletedTask;
                    }
                    wishTransition = _context._wantedState == StreamStateValue.Deleting
                        ? StreamStateValue.Deleting
                        : StreamStateValue.Stopping;
                }
            }
            if (wishTransition.HasValue)
            {
                // The transition takes the context lock and must run outside the checkpoint
                // lock, see CheckpointCompleted.
                TransitionTo(wishTransition.Value);
                return Task.CompletedTask;
            }
            return TriggerCheckpoint_StartCore(isScheduled);
        }

        private Task TriggerCheckpoint_StartCore(bool isScheduled)
        {
            Debug.Assert(_context != null, nameof(_context));
            StreamStateValue wishTransition;
            lock (_context._checkpointLock)
            {
                // The wish is re-checked under the lock: a stop or delete can set it between
                // the callers check and this point, starting a checkpoint here would then run
                // it concurrently with the stopping or deleting state working on the same
                // blocks and state manager.
                if (_context._wantedState == StreamStateValue.NotStarted ||
                    _context._wantedState == StreamStateValue.Deleting)
                {
                    if (_doingCheckpoint)
                    {
                        // A checkpoint is already in progress, its completion honors the wish.
                        return Task.CompletedTask;
                    }
                    wishTransition = _context._wantedState == StreamStateValue.Deleting
                        ? StreamStateValue.Deleting
                        : StreamStateValue.Stopping;
                }
                else
                {
                    // The wish check and the checkpoint start must share one lock scope. If
                    // the lock is released in between, a completing cycle can clear
                    // _doingCheckpoint and the new cycle would run without the flag that
                    // defers stop and delete while a checkpoint writes state.
                    _doingCheckpoint = true;

                    // Only support a single concurrent checkpoint for now for simplicity
                    if (_context.checkpointTask != null)
                    {
                        // Enqueue the checkpoint as soon as possible. The scheduled provided
                        // version must not be cleared here, the queued cycle is later promoted
                        // with it and the same version dedup would break without it.
                        _context.TryScheduleCheckpointIn_NoLock(TimeSpan.FromMilliseconds(1), _context._scheduledProvidedCheckpointVersion);
                        return _context.checkpointTask.Task;
                    }
                    _context._logger.StartingCheckpoint(_context.streamName);

                    _initialCheckpointTaken = false;
                    _compactionStarted = false;
                    nonCheckpointedEgresses = new HashSet<string>();
                    waitingForDependencies = new HashSet<string>();
                    foreach (var key in _context.egressBlocks.Keys)
                    {
                        nonCheckpointedEgresses.Add(key);
                        waitingForDependencies.Add(key);
                    }
                    foreach(var key in _context.ingressBlocks.Keys)
                    {
                        waitingForDependencies.Add(key);
                    }
                    foreach(var precompleted in _preCompletedDependencies)
                    {
                        waitingForDependencies.Remove(precompleted);
                    }
                    _preCompletedDependencies.Clear();
                    _context._logger.LogDebug("Checkpoint started on stream {Stream}, waiting for dependencies: [{Waiting}]", _context.streamName, string.Join(",", waitingForDependencies));

                    _context.checkpointTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var newTime = _context.producingTime + 1;
                    var checkpoint = new Checkpoint(_context.producingTime, newTime);
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
                    return _context.checkpointTask.Task;
                }
            }
            // The transition takes the context lock and must run outside the checkpoint
            // lock, see CheckpointCompleted.
            TransitionTo(wishTransition);
            return Task.CompletedTask;
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
            _context._wantedState = StreamStateValue.Deleting;
            lock (_context._checkpointLock)
            {
                if (_doingCheckpoint)
                {
                    // A checkpoint is in progress, deleting now would run concurrently with
                    // the state manager checkpoint and corrupt it. The wish is honored when
                    // the checkpoint completes, with the same bounded watchdog as a
                    // deferred stop.
                    _context._logger.LogDebug("Delete requested while a checkpoint is in progress, the delete runs when the checkpoint completes");
                    ArmDeferredWishWatchdog(_context, ObservedTask(_context, forDelete: true), forDelete: true);
                    return Task.CompletedTask;
                }
            }
            return TransitionTo(StreamStateValue.Deleting);
        }

        private static TaskCompletionSource? ObservedTask(StreamContext context, bool forDelete)
        {
            lock (context._checkpointLock)
            {
                return forDelete ? context._deleteTask : context._stopTask;
            }
        }

        /// <summary>
        /// Bounds a stop or delete that was deferred behind an in-progress checkpoint. The
        /// checkpoint can hang forever when another substream died mid cycle, a dead
        /// substream produces healthy looking empty fetches so nothing else detects it.
        /// After the drain timeout the stream fails so the failure handling honors the
        /// wish. The identity check makes sure the watchdog only fires for the request it
        /// was armed for, not for a later one after the stream was stopped and started
        /// again in the meantime. A checkpoint inside its local commit work is never
        /// interrupted, it is progressing and the wish is honored at its completion,
        /// failing there would run the failure teardown concurrently with the state
        /// manager checkpoint.
        /// </summary>
        private static void ArmDeferredWishWatchdog(StreamContext context, TaskCompletionSource? observedTask, bool forDelete)
        {
            var operation = forDelete ? "delete" : "stop";
            _ = Task.Run(async () =>
            {
                await Task.Delay(context._dataflowStreamOptions.StopDrainTimeout);
                while (WatchdogStillWaiting(context, observedTask, forDelete) && context.currentState == StreamStateValue.Running)
                {
                    if (System.Threading.Volatile.Read(ref context._stateManagerWriteCount) > 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1));
                        continue;
                    }
                    context._logger.LogWarning("The {operation} timed out waiting for the in-progress checkpoint on stream {stream}, failing the stream to complete it.", operation, context.streamName);
                    await context.OnFailure(new OperationCanceledException($"The {operation} timed out waiting for a checkpoint to complete."));
                    return;
                }
            });
        }

        /// <summary>
        /// Watchdog check for a deferred stop or delete. The checkpoint lock can itself be
        /// part of the hang the watchdog exists to break, so when it cannot be taken within
        /// a second the fields are read without it, a stale read only risks a spurious
        /// recovery, never a lost stop or delete.
        /// </summary>
        private static bool WatchdogStillWaiting(StreamContext context, TaskCompletionSource? observedTask, bool forDelete)
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(context._checkpointLock, TimeSpan.FromSeconds(1), ref lockTaken);
                var wantedState = forDelete ? StreamStateValue.Deleting : StreamStateValue.NotStarted;
                var task = forDelete ? context._deleteTask : context._stopTask;
                return context._wantedState == wantedState && task != null && ReferenceEquals(task, observedTask);
            }
            finally
            {
                if (lockTaken)
                {
                    Monitor.Exit(context._checkpointLock);
                }
            }
        }

        public override Task StopAsync()
        {
            Debug.Assert(_context != null, nameof(_context));
            _context._wantedState = StreamStateValue.NotStarted;
            lock (_context._checkpointLock)
            {
                if (_doingCheckpoint)
                {
                    _context._logger.LogDebug("Stop requested while a checkpoint is in progress, the stop runs when the checkpoint completes");
                    ArmDeferredWishWatchdog(_context, ObservedTask(_context, forDelete: false), forDelete: false);
                    return Task.CompletedTask;
                }
            }
            _context._logger.LogDebug("Stop requested, transitioning to stopping");

            // The transition takes the context lock and must not run under the checkpoint
            // lock: checkpoint done acknowledgements from other substreams take the context
            // lock first and the checkpoint lock second, transitioning under the checkpoint
            // lock deadlocks with them. A checkpoint that starts between the release and the
            // transition is harmless, the stopping state runs its own stop checkpoint cycles.
            TransitionTo(StreamStateValue.Stopping);
            return Task.CompletedTask;
        }

    }
}
