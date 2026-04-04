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
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class StartStreamState : StreamStateMachineState
    {
        private readonly object _lock = new object();
        private Task? _runningTask;
        private HashSet<string>? nonInitEgresses;
        private HashSet<string>? waitingForDependencies;

        public override Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            Debug.Assert(_context != null, nameof(_context));
            return _context.AddTrigger_Internal(operatorName, triggerName, schedule);
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

        public override Task DeleteAsync()
        {
            TransitionTo(StreamStateValue.Deleting);
            return Task.CompletedTask;
        }

        public override void EgressCheckpointDone(string name)
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                Debug.Assert(nonInitEgresses != null, nameof(nonInitEgresses));
                Debug.Assert(waitingForDependencies != null, nameof(waitingForDependencies));
                nonInitEgresses.Remove(name);

                // Check if all egresses has done their checkpoint
                if (nonInitEgresses.Count == 0 && waitingForDependencies.Count == 0)
                {
                    _context._logger.WatermarkSystemInitialized(_context.streamName);
                    InitEventsDone();
                }
            }
        }

        public override void EgressDependenciesDone(string name)
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                Debug.Assert(nonInitEgresses != null, nameof(nonInitEgresses));
                Debug.Assert(waitingForDependencies != null, nameof(waitingForDependencies));
                waitingForDependencies.Remove(name);

                // Check if all egresses has done their checkpoint
                if (nonInitEgresses.Count == 0 && waitingForDependencies.Count == 0)
                {
                    _context._logger.WatermarkSystemInitialized(_context.streamName);
                    InitEventsDone();
                }
            }
        }

        private void InitEventsDone()
        {
            // Start-up completed, go to running state
            TransitionTo(StreamStateValue.Running);
        }

        public override async Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            _context._logger.StartingStream(_context.streamName);

            if (_context.Status != StreamStatus.Failing)
            {
                // We only set starting if we are not failing.
                // Failure goes directly to running if everything is ok.
                _context.SetStatus(StreamStatus.Starting);
            }

            if (previousState == StreamStateValue.NotStarted)
            {
                await StartStream();
            }
            else
            {
                lock (_lock)
                {
                    if (_runningTask == null)
                    {
                        _runningTask = Task.Factory.StartNew(async () =>
                        {
                            await StartStream();
                        })
                            .Unwrap()
                            .ContinueWith(async (t, state) =>
                            {
                                StartStreamState run = (StartStreamState)state!;
                                Debug.Assert(run._context != null, nameof(_context));
                                if (t.IsFaulted)
                                {
                                    // Wait some time between starting up.
                                    await Task.Delay(100);
                                    await run._context.OnFailure(t.Exception);
                                }
                            }, this)
                            .Unwrap();
                    }
                }
            }
        }

        public override Task OnFailure()
        {
            // On failures, transition to the failure state
            return TransitionTo(StreamStateValue.Failure);
        }

        public override Task StartAsync()
        {
            return Task.CompletedTask;
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
            }
            // Will do no checkpoints during startup
            return Task.CompletedTask;
        }

        private async Task StartStream()
        {
            Debug.Assert(_context != null, nameof(_context));

            // Clear all triggers, they have to be readded by the stream
            await _context.ClearTriggers();
            // Enable trigger registration
            _context.EnableTriggerRegistration();


            long? restoreVersion;
            lock (_context._checkpointLock)
            {
                restoreVersion = _context._restoreCheckpointVersion;
            }

            // Initialize state
            await _context._stateManager.InitializeAsync(_context._streamVersionInformation, restoreVersion);
            _context._lastState = _context._stateManager.Metadata;
            _context._startCheckpointVersion = _context._stateManager.CurrentVersion;
            if (_context._lastState == null)
            {
                _context._lastState = await _context.stateHandler.LoadLatestState(_context.streamName);
                if (_context._lastState == null)
                {
                    _context._lastState = StreamState.NullState;
                }
            }
            else
            {
                if (_context._streamVersionInformation != null)
                {
                    _context._logger.CheckingStreamHashConsistency(_context.streamName);
                    if (_context._streamVersionInformation.Hash != _context._lastState.StreamHash)
                    {
                        throw new InvalidOperationException("Stream plan hash stored in storage is different than the hash used.");
                    }
                }
            }

            _context.producingTime = _context._lastState.Time + 1;

            // Create the blocks
            _context._logger.SettingUpBlocks(_context.streamName);
            _context.ForEachBlock((key, block) =>
            {
                block.Setup(_context.streamName, key);
                block.CreateBlock();
            });
            // Link the blocks
            _context._logger.LinkingBlocks(_context.streamName);
            _context.ForEachBlock((key, block) =>
            {
                block.Link();
            });

            try
            {
                _context._logger.InitializingPropagatorBLocks(_context.streamName);
                foreach (var block in _context.propagatorBlocks)
                {
                    TagList tags = new TagList()
                    {
                        { "stream", _context.streamName },
                        { "operator", block.Key }
                    };
                    var blockStateClient = _context._stateManager.GetOrCreateClient(block.Key, tags);
                    VertexHandler vertexHandler = new VertexHandler(
                        _context.streamName,
                        block.Key,
                        _context.TryScheduleCheckpointIn,
                        _context.AddTrigger,
                        _context._streamMetrics.GetOrCreateVertexMeter(block.Key, () => block.Value.DisplayName),
                        blockStateClient,
                        _context.loggerFactory,
                        _context._streamMemoryManager.CreateOperatorMemoryManager(block.Key),
                        _context.FailAndRollback);
                    await block.Value.Initialize(block.Key, _context._lastState!.Time, _context.producingTime, vertexHandler, _context._streamVersionInformation);
                }

                _context._logger.InitializingEgressBlocks(_context.streamName);
                foreach (var block in _context.egressBlocks)
                {
                    TagList tags = new TagList()
                    {
                        { "stream", _context.streamName },
                        { "operator", block.Key }
                    };
                    var blockStateClient = _context._stateManager.GetOrCreateClient(block.Key, tags);
                    VertexHandler vertexHandler = new VertexHandler(
                        _context.streamName,
                        block.Key,
                        _context.TryScheduleCheckpointIn,
                        _context.AddTrigger,
                        _context._streamMetrics.GetOrCreateVertexMeter(block.Key, () => block.Value.DisplayName),
                        blockStateClient,
                        _context.loggerFactory,
                        _context._streamMemoryManager.CreateOperatorMemoryManager(block.Key),
                        _context.FailAndRollback);
                    await block.Value.Initialize(block.Key, _context._lastState!.Time, _context.producingTime, vertexHandler, _context._streamVersionInformation);
                    block.Value.SetCheckpointDoneFunction(_context.EgressCheckpointDone, _context.EgressDependenciesDone);
                }

                _context._logger.InitializingIngressBlocks(_context.streamName);
                foreach (var block in _context.ingressBlocks)
                {
                    TagList tags = new TagList()
                    {
                        { "stream", _context.streamName },
                        { "operator", block.Key }
                    };
                    var blockStateClient = _context._stateManager.GetOrCreateClient(block.Key, tags);
                    VertexHandler vertexHandler = new VertexHandler(
                        _context.streamName,
                        block.Key,
                        _context.TryScheduleCheckpointIn,
                        _context.AddTrigger,
                        _context._streamMetrics.GetOrCreateVertexMeter(block.Key, () => block.Value.DisplayName),
                        blockStateClient,
                        _context.loggerFactory,
                        _context._streamMemoryManager.CreateOperatorMemoryManager(block.Key),
                        _context.FailAndRollback);
                    await block.Value.Initialize(block.Key, _context._lastState!.Time, _context.producingTime, vertexHandler, _context._streamVersionInformation);
                    block.Value.SetDependenciesDoneFunction(_context.EgressDependenciesDone);
                }
            }
            catch (Exception e)
            {

                await _context.OnFailure(e);
                return;
            }

            var completionTasks = _context.GetCompletionTasks();

            _context._onFailureTask = Task.WhenAny(completionTasks).ContinueWith((task) =>
            {
                foreach (var comp in completionTasks)
                {
                    if (comp.IsFaulted)
                    {
                        return _context.OnFailure(comp.Exception);
                    }
                }
                if (task.IsFaulted)
                {
                    return _context.OnFailure(task.Exception);
                }
                return Task.CompletedTask;
            }).Unwrap();

            // Run an init locking event through the stream to initialize information such as watermarks.
            // Watermark initialization must go through this flow so all blocks know what watermarks will pass through it.
            // Since a source can be joined with itself some blocks will get the same watermark from multiple inputs.
            _context._logger.InitializingWatermarkSystem(_context.streamName);
            nonInitEgresses = _context.egressBlocks.Keys.ToHashSet();
            waitingForDependencies = _context.egressBlocks.Keys.Union(_context.ingressBlocks.Keys).ToHashSet();
            foreach (var ingress in _context.ingressBlocks)
            {
                ingress.Value.DoLockingEvent(new InitWatermarksEvent());
            }
        }

        public override Task StopAsync()
        {
            Debug.Assert(_context != null, nameof(_context));
            _context._wantedState = StreamStateValue.NotStarted;
            return Task.CompletedTask;
        }
    }
}
