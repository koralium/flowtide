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
    internal class StartStreamState : StreamStateMachineState
    {
        private readonly object _lock = new object();
        private Task? _runningTask;
        private HashSet<string>? nonInitEgresses;
        private HashSet<string>? waitingForDependencies;
        // Cancelled when a failure supersedes this start. The start task itself keeps
        // running after the state machine transitions away; unobserved it would continue
        // initializing and start source and fetch tasks that nothing ever faults or awaits,
        // and every following restart then dies on the running tasks it left behind.
        private readonly CancellationTokenSource _startAbort = new CancellationTokenSource();

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
            Debug.Assert(_context != null, nameof(_context));
            // Transitioning to deleting directly would run the delete concurrently with the
            // in-flight start, both paths work on the same blocks and state manager. The
            // start is aborted through the failure path, the failure handling cleans up and
            // then honors the delete wish, see StopAsync for the same pattern.
            _context._wantedState = StreamStateValue.Deleting;
            _ = Task.Run(() => _context.OnFailure(new OperationCanceledException("The stream was deleted while it was starting.")));
            return Task.CompletedTask;
        }

        public override void EgressCheckpointDone(string name, ILockingEvent? lockingEvent)
        {
            Debug.Assert(_context != null, nameof(_context));

            if (lockingEvent is not InitWatermarksEvent)
            {
                // During startup the only checkpoint done this state tracks is the init
                // watermarks event it injects. A checkpoint event here is stale: a checkpoint
                // that was in flight when the aborted generation failed can have its
                // acknowledgement delivered late, after the stream restarted into this state
                // (a parallel egress fires it from an unconditional continuation). Counting it
                // would drop an egress from the init tracking before its real init watermarks
                // event was handled, firing InitEventsDone and transitioning to running before
                // watermark initialization completed.
                return;
            }

            lock (_context._checkpointLock)
            {
                if (nonInitEgresses == null || waitingForDependencies == null)
                {
                    // The init tracking sets are created near the end of startup, just before
                    // the init watermarks event is injected. An acknowledgement arriving before
                    // that cannot be one this state is waiting for, so it is ignored (dropping
                    // the removal below that would otherwise dereference a null set).
                    return;
                }
                nonInitEgresses.Remove(name);

                // Check if all egresses has done their checkpoint
                if (nonInitEgresses.Count == 0 && waitingForDependencies.Count == 0)
                {
                    _context._logger.WatermarkSystemInitialized(_context.streamName);
                    InitEventsDone();
                }
            }
        }

        public override void EgressDependenciesDone(string name, ILockingEvent? lockingEvent)
        {
            Debug.Assert(_context != null, nameof(_context));
            lock (_context._checkpointLock)
            {
                if (waitingForDependencies == null || !waitingForDependencies.Remove(name))
                {
                    // An extra signal during startup, for example a checkpoint acknowledgement
                    // from another substream that arrived before or during initialization.
                    // It must not be lost, the first checkpoint in the running state consumes it.
                    _context._earlyDependenciesDone.Add(name);
                    if (waitingForDependencies == null)
                    {
                        return;
                    }
                }

                // Check if all egresses has done their checkpoint
                if (nonInitEgresses != null && nonInitEgresses.Count == 0 && waitingForDependencies.Count == 0)
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

            if (_context.RawStatus != StreamStatus.Failing)
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
            // Cancelled before the transition so the in-flight start observes it before the
            // failure teardown reads what the start has created.
            _startAbort.Cancel();
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
                _context.TryScheduleCheckpointIn(TimeSpan.FromSeconds(10), default);
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

            // This start creates the blocks below; a failure before that must not run the
            // block teardown.
            System.Threading.Volatile.Write(ref _context._blocksCreated, 0);

            if (StartAborted())
            {
                return;
            }

            // Initialize state
            await _context._stateManager.InitializeAsync(_context._streamVersionInformation, restoreVersion);

            if (StartAborted())
            {
                return;
            }
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

            // Seed the producing time with the wall clock so checkpoint times stay unique
            // across a rollback. The restored time comes from the state manager metadata,
            // which rolls back together with the state, so seeding with restored time + 1
            // alone would mint the same times as the aborted epoch, whose barriers can still
            // be in flight in exchange queues and messages between substreams. The pairing of
            // checkpoint barriers between substreams relies on the times being unique. The
            // max guards against a wall clock that moved backwards.
            _context.producingTime = Math.Max(_context._lastState.Time + 1, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            if (StartAborted())
            {
                return;
            }

            // Create the blocks
            _context._logger.SettingUpBlocks(_context.streamName);
            _context.ForEachBlock((key, block) =>
            {
                block.Setup(_context.streamName, key);
                block.CreateBlock();
            });
            System.Threading.Volatile.Write(ref _context._blocksCreated, 1);
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
                    await block.Value.Initialize(block.Key, _context._stateManager.LastCompletedCheckpointVersion, _context._stateManager.CurrentVersion, vertexHandler, _context._streamVersionInformation);
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
                    await block.Value.Initialize(block.Key, _context._stateManager.LastCompletedCheckpointVersion, _context._stateManager.CurrentVersion, vertexHandler, _context._streamVersionInformation);
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
                    await block.Value.Initialize(block.Key, _context._stateManager.LastCompletedCheckpointVersion, _context._stateManager.CurrentVersion, vertexHandler, _context._streamVersionInformation);
                    block.Value.SetDependenciesDoneFunction(_context.EgressDependenciesDone);
                }
            }
            catch (Exception e)
            {

                await _context.OnFailure(e);
                return;
            }

            if (StartAborted())
            {
                await AbandonStartedBlocks();
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
            var startupHook = StreamContext.StartupBeforeInitTrackingHookForTests;
            if (startupHook != null)
            {
                await startupHook(_context.streamName);
            }
            if (StartAborted())
            {
                await AbandonStartedBlocks();
                return;
            }
            nonInitEgresses = _context.egressBlocks.Keys.ToHashSet();
            waitingForDependencies = _context.egressBlocks.Keys.Union(_context.ingressBlocks.Keys).ToHashSet();
            foreach (var ingress in _context.ingressBlocks)
            {
                ingress.Value.DoLockingEvent(new InitWatermarksEvent());
            }
        }

        /// <summary>
        /// True when a failure superseded this start. The start abandons itself at the next
        /// phase boundary instead of continuing to initialize a stream the state machine has
        /// already moved away from.
        /// </summary>
        private bool StartAborted()
        {
            Debug.Assert(_context != null, nameof(_context));
            if (!_startAbort.IsCancellationRequested)
            {
                return false;
            }
            _context._logger.LogInformation("The start of stream {stream} was superseded by a failure, abandoning it.", _context.streamName);
            return true;
        }

        /// <summary>
        /// Tears down the blocks this abandoned start created, unless the failure teardown
        /// already claimed them; exactly one party cleans, a dispose must not run twice.
        /// </summary>
        private async Task AbandonStartedBlocks()
        {
            Debug.Assert(_context != null, nameof(_context));
            if (System.Threading.Interlocked.Exchange(ref _context._blocksCreated, 0) == 0)
            {
                return;
            }
            _context.ForEachBlock((key, block) =>
            {
                block.Fault(new BlockStopException("The start was superseded by a failure."));
            });
            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });
            await _context.ForEachBlockAsync(async (key, block) =>
            {
                await block.DisposeAsync();
            });
        }

        public override Task StopAsync()
        {
            Debug.Assert(_context != null, nameof(_context));
            _context._wantedState = StreamStateValue.NotStarted;
            // The start can block for a long time waiting on another substream, for example
            // the initialize handshake retries for close to a minute when the other
            // substream is also stopping and answers not started. Waiting for the start to
            // finish would delay the stop by that long, so the start is aborted through the
            // failure path. The failure handling cleans up the blocks, honors the stop wish
            // and completes the stop task the caller awaits. Nothing new has been committed
            // during the start, so stopping without a final checkpoint loses no data, the
            // next start replays from the last committed checkpoint.
            _ = Task.Run(() => _context.OnFailure(new OperationCanceledException("The stream was stopped while it was starting.")));
            return Task.CompletedTask;
        }
    }
}
