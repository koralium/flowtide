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
    internal class StoppingStreamState : StreamStateMachineState
    {
        private HashSet<string>? nonCheckpointedEgresses;
        private Checkpoint? _currentCheckpoint;

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
            throw new NotSupportedException("Stream is stopping.");
        }

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

            _context._logger.StartCheckpointDoneTask(_context.streamName);
            Task.Factory.StartNew(async (state) =>
            {
                var run = (StoppingStreamState)state!;
                Debug.Assert(run._context != null, nameof(_context));
                Debug.Assert(run._currentCheckpoint != null, nameof(_context));

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
                     StoppingStreamState @this = (StoppingStreamState)state!;
                     if (t.IsFaulted)
                     {
                         await _context.OnFailure(t.Exception);
                     }
                     // Finish the checkpoint
                     @this.CheckpointCompleted();
                     _context._logger.ShutdownCheckpointDone(_context.streamName);
                     await @this.StopAll();
                 }, this)
                 .Unwrap();
        }

        private void CheckpointCompleted()
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                _context._initialCheckpointTaken = true;
                if (_context.checkpointTask != null)
                {
                    _context._scheduleCheckpointTask = null;
                    _context.checkpointTask.SetResult();
                    _context.checkpointTask = null;
                    _currentCheckpoint = null;
                }
            }
        }

        private async Task StopAll()
        {
            Debug.Assert(_context != null, nameof(_context));

            _context.ForEachBlock((key, block) =>
            {
                block.Complete();
            });
            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });

            await _context.ForEachBlockAsync(async (key, block) =>
            {
                await block.DisposeAsync();
            });

            _context._stateManager.Dispose();


            await TransitionTo(StreamStateValue.NotStarted);
            _context._logger.StoppedStream(_context.streamName);

            if (_context._stopTask != null)
            {
                lock (_context._checkpointLock)
                {
                    _context._stopTask.SetResult();
                    _context._stopTask = null;
                }
            }
        }

        public override Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null);
            _context.CheckForPause();
            _context.SetStatus(StreamStatus.Stopping);
            _context._logger.StoppingStream(_context.streamName);
            _context.TryScheduleCheckpointIn(TimeSpan.FromMilliseconds(1));
            return Task.CompletedTask;
        }

        public override async Task OnFailure()
        {
            // On failure stop all
            await StopAll();
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
                _context._logger.StartingShutdownCheckpoint(_context.streamName);
                nonCheckpointedEgresses = new HashSet<string>();
                foreach (var key in _context.egressBlocks.Keys)
                {
                    nonCheckpointedEgresses.Add(key);
                }
                _context.checkpointTask = new TaskCompletionSource();
                var newTime = _context.producingTime + 1;
                checkpoint = new StopStreamCheckpoint(_context.producingTime, newTime);
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
