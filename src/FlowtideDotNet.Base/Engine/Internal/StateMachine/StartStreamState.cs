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
using System.Text.Json;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class StartStreamState : StreamStateMachineState
    {
        private readonly object _lock = new object();
        private Task? _runningTask;
        private HashSet<string>? nonInitEgresses;

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
                nonInitEgresses.Remove(name);

                // Check if all egresses has done their checkpoint
                if (nonInitEgresses.Count == 0)
                {
                    _context._logger.LogInformation("Watermark system initialized.");
                    InitEventsDone();
                }
            }
        }

        private void InitEventsDone()
        {
            // Start-up completed, go to running state
            TransitionTo(StreamStateValue.Running);
        }

        public override void Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            _context._logger.LogInformation("Starting stream");

            if (previousState == StreamStateValue.NotStarted)
            {
                StartStream().GetAwaiter().GetResult();
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

            await _context._stateManager.InitializeAsync();
            _context._lastState = _context._stateManager.Metadata;
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
                    _context._logger.LogTrace("Checking stream hash consistency");
                    if (_context._streamVersionInformation.Version != _context._lastState.StrreamVersion)
                    {
                        throw new InvalidOperationException("Stream version missmatch, the version stored in storage is different than the version used.");
                    }
                    if (_context._streamVersionInformation.Hash != _context._lastState.StreamHash)
                    {
                        throw new InvalidOperationException("Stream plan hash stored in storage is different than the hash used.");
                    }
                }
            }

            _context.producingTime = _context._lastState.Time + 1;

            // Create the blocks
            _context._logger.LogInformation("Setting up blocks");
            _context.ForEachBlock((key, block) =>
            {
                block.Setup(_context.streamName, key);
                block.CreateBlock();
            });
            // Link the blocks
            _context._logger.LogInformation("Linking blocks together");
            _context.ForEachBlock((key, block) =>
            {
                block.Link();
            });

            try
            {
                _context._logger.LogInformation("Initializing propagator blocks");
                foreach (var block in _context.propagatorBlocks)
                {
                    JsonElement? blockState = null;
                    if (_context._lastState != null && _context._lastState.OperatorStates.TryGetValue(block.Key, out var state))
                    {
                        blockState = state;
                    }
                    var blockStateClient = _context._stateManager.GetOrCreateClient(block.Key);
                    VertexHandler vertexHandler = new VertexHandler(_context.streamName, block.Key, _context.TryScheduleCheckpointIn, _context.AddTrigger, _context._streamMetrics.GetOrCreateVertexMeter(block.Key), blockStateClient, _context.loggerFactory);
                    await block.Value.Initialize(block.Key, _context._lastState!.Time, _context.producingTime, blockState, vertexHandler);
                }

                _context._logger.LogInformation("Initializing egress blocks");
                foreach (var block in _context.egressBlocks)
                {
                    JsonElement? blockState = null;
                    if (_context._lastState != null && _context._lastState.OperatorStates.TryGetValue(block.Key, out var state))
                    {
                        blockState = state;
                    }
                    var blockStateClient = _context._stateManager.GetOrCreateClient(block.Key);
                    VertexHandler vertexHandler = new VertexHandler(_context.streamName, block.Key, _context.TryScheduleCheckpointIn, _context.AddTrigger, _context._streamMetrics.GetOrCreateVertexMeter(block.Key), blockStateClient, _context.loggerFactory);
                    await block.Value.Initialize(block.Key, _context._lastState!.Time, _context.producingTime, blockState, vertexHandler);
                    block.Value.SetCheckpointDoneFunction(_context.EgressCheckpointDone);
                }

                _context._logger.LogInformation("Initializing ingress blocks");
                foreach (var block in _context.ingressBlocks)
                {
                    JsonElement? blockState = null;
                    if (_context._lastState != null && _context._lastState.OperatorStates.TryGetValue(block.Key, out var state))
                    {
                        blockState = state;
                    }
                    var blockStateClient = _context._stateManager.GetOrCreateClient(block.Key);
                    VertexHandler vertexHandler = new VertexHandler(_context.streamName, block.Key, _context.TryScheduleCheckpointIn, _context.AddTrigger, _context._streamMetrics.GetOrCreateVertexMeter(block.Key), blockStateClient, _context.loggerFactory);
                    await block.Value.Initialize(block.Key, _context._lastState!.Time, _context.producingTime, blockState, vertexHandler);
                }
            }
            catch(Exception e)
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
            _context._logger.LogInformation("Initializing watermark system.");
            nonInitEgresses = _context.egressBlocks.Keys.ToHashSet();
            foreach (var ingress in _context.ingressBlocks)
            {
                ingress.Value.DoLockingEvent(new InitWatermarksEvent());
            }
        }
    }
}
