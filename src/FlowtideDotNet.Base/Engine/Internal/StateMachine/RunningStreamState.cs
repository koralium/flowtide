﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class RunningStreamState : StreamStateMachineState
    {
        private Task? _initialBatchTask;
        private readonly object _lock = new object();
        private HashSet<string>? nonCheckpointedEgresses;
        private Checkpoint? _currentCheckpoint;

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

        private void StartCheckpointDoneTask()
        {
            Debug.Assert(_context != null, nameof(_context));

            _context._logger.LogTrace("Starting checkpoint done task");
            Task.Factory.StartNew(async (state) =>
            {
                var run = (RunningStreamState)state!;
                Debug.Assert(run._context != null, nameof(_context));
                Debug.Assert(run._currentCheckpoint != null, nameof(_context));

                // Write the latest state
                run._context._lastState = new StreamState(
                    run._currentCheckpoint.CheckpointTime, 
                    run._currentCheckpoint.GetOperatorStates(), 
                    _context._streamVersionInformation?.Version ?? 0, 
                    _context._streamVersionInformation?.Hash ?? string.Empty);

                run._context._stateManager.Metadata = run._context._lastState;

                // Take state checkpoint
                _context._logger.LogTrace("Starting state manager checkpoint");
                await run._context._stateManager.CheckpointAsync();
                _context._logger.LogTrace("State manager checkpoint done");

                await run._context.stateHandler.WriteLatestState(run._context.streamName, run._context._lastState);


                // Compaction: if more than 30% of the pages has been changed since last compaction, do compaction
                long changesSinceLastCompaction = run._context._stateManager.PageCommitsSinceLastCompaction;
                var compactionThreshold = (long)(run._context._stateManager.PageCount * 0.3);
                if (changesSinceLastCompaction > compactionThreshold)
                {
                    await run._context._stateManager.Compact();
                }


                // After writing do compaction
                _context._logger.LogTrace("Starting compaction on all nodes");
                List<Task> tasks = new List<Task>();
                foreach (var ingressNode in run._context.ingressBlocks)
                {
                    tasks.Add(ingressNode.Value.Compact());
                }
                foreach (var block in run._context.propagatorBlocks)
                {
                    tasks.Add(block.Value.Compact());
                }
                foreach (var block in run._context.egressBlocks)
                {
                    tasks.Add(block.Value.Compact());
                }

                await Task.WhenAll(tasks);
                _context._logger.LogTrace("Compaction done on nodes");
            }, this)
                .Unwrap()
                 .ContinueWith((t, state) =>
                 {
                     RunningStreamState @this = (RunningStreamState)state!;
                     if (t.IsFaulted)
                     {
                         return _context.OnFailure(t.Exception);
                     }
                     // Finish the checkpoint
                     @this.CheckpointCompleted();
                     return Task.CompletedTask;
                 }, this)
                 .Unwrap();
        }

        private void CheckpointCompleted()
        {
            Debug.Assert(_context != null, nameof(_context));

            lock (_context._checkpointLock)
            {
                if (_context.checkpointTask != null)
                {
                    _context._logger.LogInformation("Checkpoint done.");
                    _context._scheduleCheckpointTask = null;
                    _context.checkpointTask.SetResult();
                    _context.checkpointTask = null;
                    _currentCheckpoint = null;

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

        public override void Initialize()
        {
            Debug.Assert(_context != null, nameof(_context));

            _context._logger.LogInformation("Stream is in running state");
            lock (_lock)
            {
                if (_initialBatchTask != null)
                {
                    return;
                }

                _initialBatchTask = Task.Factory.StartNew(async () =>
                {
                    foreach (var block in _context.ingressBlocks)
                    {
                        // Signal to ingress blocks that all has been initialized and it can now start accepting data
                        await block.Value.InitializationCompleted();
                    }
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
                        
                        return Task.CompletedTask;
                    })
                    .Unwrap();
            }
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
                // Only support a single concurrent checkpoint for now for simplicity
                if (_context.checkpointTask != null)
                {
                    return _context.checkpointTask.Task;
                }
                _context._logger.LogInformation("Starting checkpoint.");
                nonCheckpointedEgresses = new HashSet<string>();
                foreach (var key in _context.egressBlocks.Keys)
                {
                    nonCheckpointedEgresses.Add(key);
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
    }
}
