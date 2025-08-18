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
using FlowtideDotNet.Base.Vertices.Ingress;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class RunningStreamState : StreamStateMachineState
    {
        private Task? _initialBatchTask;
        private readonly object _lock = new object();
        private HashSet<string>? nonCheckpointedEgresses;
        private HashSet<string>? waitingForDependenciesEgresses;
        private Checkpoint? _currentCheckpoint;
        private bool _doingCheckpoint = false;
        private bool _initialCheckpointTaken = false;
        private bool _compactionStarted = false;

        public override void EgressCheckpointDone(string name)
        {
            Debug.Assert(_context != null, nameof(_context));
            Debug.Assert(nonCheckpointedEgresses != null, nameof(nonCheckpointedEgresses));

            lock (_context._checkpointLock)
            {
                nonCheckpointedEgresses.Remove(name);

                // Check if all egresses has done their checkpoint
                if (nonCheckpointedEgresses.Count == 0)
                {
                    StartCheckpointDoneTask();
                }
            }
        }

        public override void EgressDependenciesDone(string name)
        {
            Debug.Assert(_context != null, nameof(_context));
            Debug.Assert(waitingForDependenciesEgresses != null, nameof(waitingForDependenciesEgresses));
            lock (_context._checkpointLock)
            {
                waitingForDependenciesEgresses.Remove(name);

                // Check if all egresses has done their dependencies
                if (waitingForDependenciesEgresses.Count > 0 || !_initialCheckpointTaken || _compactionStarted)
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
            }, this)
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
                Debug.Assert(run.waitingForDependenciesEgresses != null);

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

                await run._context.stateHandler.WriteLatestState(run._context.streamName, run._context._lastState);

                await _context.ForEachIngressBlockAsync((key, block) =>
                {
                    if (block is IStreamIngressVertex streamIngressVertex)
                    {
                        return streamIngressVertex.CheckpointDone(run._currentCheckpoint.CheckpointTime);
                    }
                    return Task.CompletedTask;
                });
            }, this)
                .Unwrap()
                 .ContinueWith(async (t, state) =>
                 {
                     RunningStreamState @this = (RunningStreamState)state!;
                     Debug.Assert(@this.waitingForDependenciesEgresses != null);
                     if (t.IsFaulted)
                     {
                         await _context.OnFailure(t.Exception);
                         return;
                     }

                     lock (_context._checkpointLock)
                     {
                         _initialCheckpointTaken = true;
                         // Check if all egresses has done their dependencies
                         // if not return
                         if (@this.waitingForDependenciesEgresses.Count > 0 || _compactionStarted)
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

        private void CheckpointCompleted()
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                if (_context.Status == StreamStatus.Failing)
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

                    if (_context._wantedState == StreamStateValue.NotStarted && _doingCheckpoint)
                    {
                        _doingCheckpoint = false;
                        TransitionTo(StreamStateValue.Stopping);
                        return;
                    }

                    _doingCheckpoint = false;
                    if (_context.inQueueCheckpoint.HasValue)
                    {
                        var span = _context.inQueueCheckpoint.Value.Subtract(DateTimeOffset.UtcNow);
                        if (span.TotalMilliseconds < 0)
                        {
                            span = TimeSpan.FromMilliseconds(1);
                        }
                        if (_context.TryScheduleCheckpointIn_NoLock(span))
                        {
                            _context.inQueueCheckpoint = null;
                        }
                        else
                        {
                            throw new InvalidOperationException("Checkpoint could not be scheduled.");
                        }
                    }
                }
            }
        }

        public override Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            _context.CheckForPause();

            if (_context.Status != StreamStatus.Failing)
            {
                // Failure status is removed when a checkpoint has been made, since a failure could happen in the middle of doing a checkpoint
                _context.SetStatus(StreamStatus.Running);
            }

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
                }

                if (_context._dataflowStreamOptions.WaitForCheckpointAfterInitialData)
                {
                    // Set the checkpoint task to stop any other checkpoint from happening
                    _context.checkpointTask = new TaskCompletionSource();
                }

                _initialBatchTask = Task.Factory.StartNew(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    foreach (var block in _context.ingressBlocks)
                    {
                        // Signal to ingress blocks that all has been initialized and it can now start accepting data
                        tasks.Add(block.Value.InitializationCompleted());
                    }
                    await Task.WhenAll(tasks);
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
            Console.WriteLine("Exception");
            return TransitionTo(StreamStateValue.Failure);
        }

        public override Task TriggerCheckpoint(bool isScheduled = false)
        {
            Debug.Assert(_context != null, nameof(_context));

            Checkpoint? checkpoint = null;
            lock (_context._checkpointLock)
            {
                // If we are stopping, we should not do a checkpoint
                if (_context._wantedState == StreamStateValue.NotStarted)
                {
                    return Task.CompletedTask;
                }
                _doingCheckpoint = true;
                // Only support a single concurrent checkpoint for now for simplicity
                if (_context.checkpointTask != null)
                {
                    return _context.checkpointTask.Task;
                }
                _context._logger.StartingCheckpoint(_context.streamName);

                _initialCheckpointTaken = false;
                _compactionStarted = false;
                nonCheckpointedEgresses = new HashSet<string>();
                waitingForDependenciesEgresses = new HashSet<string>();
                foreach (var key in _context.egressBlocks.Keys)
                {
                    nonCheckpointedEgresses.Add(key);
                    waitingForDependenciesEgresses.Add(key);
                }
                _context.checkpointTask = new TaskCompletionSource();
                var newTime = _context.producingTime + 1;
                checkpoint = new Checkpoint(_context.producingTime, newTime);
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
            _context._wantedState = StreamStateValue.NotStarted;
            lock (_context._checkpointLock)
            {
                if (_doingCheckpoint)
                {
                    return Task.CompletedTask;
                }
                else
                {
                    TransitionTo(StreamStateValue.Stopping);
                }
            }

            return Task.CompletedTask;
        }

        public override void Pause()
        {
            Debug.Assert(_context != null, nameof(_context));
            _context.ForEachBlock((id, block) =>
            {
                block.Pause();
            });
        }

        public override void Resume()
        {
            Debug.Assert(_context != null, nameof(_context));
            _context.ForEachBlock((id, block) =>
            {
                block.Resume();
            });
        }
    }
}
