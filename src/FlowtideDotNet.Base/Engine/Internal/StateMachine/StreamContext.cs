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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Metrics.Counter;
using FlowtideDotNet.Base.Metrics.Gauge;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class StreamContext : IStreamTriggerCaller, IAsyncDisposable
    {
        private static readonly ActivitySource s_exceptionActivitySource = new ActivitySource("FlowtideDotNet.Base.StreamException");

        internal readonly string streamName;
        internal readonly string version;
        internal readonly Dictionary<string, IStreamVertex> propagatorBlocks;
        internal readonly Dictionary<string, IStreamIngressVertex> ingressBlocks;
        internal readonly Dictionary<string, IStreamEgressVertex> egressBlocks;
        internal readonly Dictionary<string, IStreamVertex> _blockLookup;
        internal readonly IStateHandler stateHandler;
        internal readonly StreamMetrics _streamMetrics;
        internal readonly StreamNotificationReceiver? _notificationReciever;
        private readonly StateManagerOptions stateManagerOptions;
        private readonly ILoggerFactory? loggerFactory1;
        internal readonly ILoggerFactory loggerFactory;
        internal readonly object _checkpointLock;
        internal readonly Dictionary<string, List<OperatorTrigger>> _triggers;
        internal readonly object _triggerLock;
        internal readonly IStreamScheduler _streamScheduler;
        private readonly object _contextLock = new object();
        internal readonly StreamVersionInformation? _streamVersionInformation;
        internal readonly DataflowStreamOptions _dataflowStreamOptions;
        internal readonly IStreamMemoryManager _streamMemoryManager;
        private readonly Meter _contextMeter;
        internal long? _restoreCheckpointVersion;

        internal StreamState? _lastState;
        internal long producingTime = 0;
        internal Task? _onFailureTask;
        internal TaskCompletionSource? checkpointTask;
        internal DateTimeOffset? inQueueCheckpoint;
        internal long? _currentProvidedCheckpointVersion;
        internal long? _scheduledProvidedCheckpointVersion;

        /// <summary>
        /// Dependencies done signals that arrived while the stream was still starting, for
        /// example checkpoint acknowledgements from other substreams. They are consumed by the
        /// first checkpoint in the running state. Guarded by <see cref="_checkpointLock"/>.
        /// </summary>
        internal readonly HashSet<string> _earlyDependenciesDone = new HashSet<string>();

        internal TaskCompletionSource? _stopTask;
        // Completed when a requested delete has fully finished, guarded by _checkpointLock.
        internal TaskCompletionSource? _deleteTask;

        private StreamStateMachineState? _state = null;

        internal Task? _scheduleCheckpointTask;
        internal DateTime? _triggerCheckpointTime;
        internal CancellationTokenSource? _scheduleCheckpointCancelSource;

        internal StreamStateValue currentState;
        // Volatile: written by stop/delete on caller threads, read by the state machine on its own
        // threads, often outside locks. The honoring points tolerate the check-then-act window by
        // re-checking at every safe point.
        internal volatile StreamStateValue _wantedState;

        // Counts state manager writes in flight (checkpoint commit and compaction). A teardown must
        // wait for zero before faulting/disposing, or it corrupts the state manager. A counter, not a
        // flag, so overlapping spans across a failure epoch don't let one finishing clear the guard.
        internal int _stateManagerWriteCount;

        // Test hooks, null in production. Each gets the stream name so a test can filter to its own
        // stream: CheckpointCommitHookForTests awaits inside the commit (so a test can hold a write in
        // flight), BeforeFailureDisposeForTests fires as the failure teardown disposes blocks, and
        // RestoreVersionForTests reports the rollback version.
        internal static Func<string, long, Task>? CheckpointCommitHookForTests;
        internal static Func<string, Task>? CompactionHookForTests;
        internal static Action<string>? BeforeFailureDisposeForTests;
        internal static Action<string, long>? RestoreVersionForTests;

        private StreamStatus _streamStatus;

        internal object _pauseLock = new object();
        private StreamStatus _statusBeforePause;
        internal TaskCompletionSource? _pauseSource;
        private IOptionsMonitor<FlowtidePauseOptions>? _pauseMonitor;

        /// <summary>
        /// Enables or disables trigger registration, used often during failures
        /// </summary>
        private bool _triggersEnabled = true;

        internal FlowtideDotNet.Storage.StateManager.StateManagerSync<StreamState> _stateManager;
        internal readonly ILogger<StreamContext> _logger;

        internal bool _initialCheckpointTaken = false;
        
        // Test variable
        internal long _startCheckpointVersion = 0;

        public StreamStatus Status => _streamStatus;

        /// <summary>
        /// The status the state machine recorded, ignoring the Paused mask. Internal
        /// protocol checks, for example Failing until a checkpoint proves the recovery,
        /// must read this, the masked Status would corrupt them while paused.
        /// </summary>
        internal StreamStatus RawStatus
        {
            get
            {
                lock (_pauseLock)
                {
                    return _pauseSource != null ? _statusBeforePause : _streamStatus;
                }
            }
        }

        public FlowtideHealth Health
        {
            get
            {
                switch (Status)
                {
                    case StreamStatus.Failing:
                        return FlowtideHealth.Unhealthy;
                    case StreamStatus.Running:
                        return FlowtideHealth.Healthy;
                    case StreamStatus.Paused:
                        if (currentState == StreamStateValue.Running)
                        {
                            return FlowtideHealth.Healthy;
                        }
                        if (currentState == StreamStateValue.Failure)
                        {
                            return FlowtideHealth.Unhealthy;
                        }
                        return FlowtideHealth.Degraded;
                    case StreamStatus.Starting:
                        return FlowtideHealth.Degraded;
                    case StreamStatus.Stopped:
                        return FlowtideHealth.Unhealthy;
                    case StreamStatus.Degraded:
                        return FlowtideHealth.Degraded;
                    default:
                        return FlowtideHealth.Unhealthy;
                }
            }
        }


        public StreamContext(
            string streamName,
            string version,
            Dictionary<string, IStreamVertex> propagatorBlocks,
            Dictionary<string, IStreamIngressVertex> ingressBlocks,
            Dictionary<string, IStreamEgressVertex> egressBlocks,
            IStateHandler stateHandler,
            StreamState? fromState,
            IStreamScheduler streamScheduler,
            StreamNotificationReceiver? notificationReciever,
            StateManagerOptions stateManagerOptions,
            ILoggerFactory? loggerFactory,
            StreamVersionInformation? streamVersionInformation,
            DataflowStreamOptions dataflowStreamOptions,
            IStreamMemoryManager streamMemoryManager,
            IOptionsMonitor<FlowtidePauseOptions>? pauseMonitor)
        {
            this.streamName = streamName;
            this.version = version;
            this.propagatorBlocks = propagatorBlocks;
            this.ingressBlocks = ingressBlocks;
            this.egressBlocks = egressBlocks;
            this.stateHandler = stateHandler;
            _lastState = fromState;
            _streamScheduler = streamScheduler;
            _streamMetrics = new StreamMetrics(streamName);
            _notificationReciever = notificationReciever;
            this.stateManagerOptions = stateManagerOptions;
            loggerFactory1 = loggerFactory;
            _streamVersionInformation = streamVersionInformation;
            this._dataflowStreamOptions = dataflowStreamOptions;
            _streamMemoryManager = streamMemoryManager;
            _contextMeter = new Meter($"flowtide.{streamName}");
            _contextMeter.CreateObservableGauge<float>("flowtide_health", () =>
            {
                var val = 0.0f;
                switch (Health)
                {
                    case FlowtideHealth.Healthy:
                        val = 1.0f;
                        break;
                    case FlowtideHealth.Unhealthy:
                        val = 0.0f;
                        break;
                    case FlowtideHealth.Degraded:
                        val = 0.5f;
                        break;
                    default:
                        val = 0.0f;
                        break;
                }
                return new Measurement<float>(val, new KeyValuePair<string, object?>("stream", streamName));
            });
            _contextMeter.CreateObservableGauge<int>("flowtide_status", () =>
            {
                return new Measurement<int>((int)Status, new KeyValuePair<string, object?>("stream", streamName));
            });
            _contextMeter.CreateObservableGauge<int>("flowtide_state", () =>
            {
                return new Measurement<int>((int)currentState, new KeyValuePair<string, object?>("stream", streamName));
            });
            _contextMeter.CreateObservableGauge<int>("flowtide_wanted_state", () =>
            {
                return new Measurement<int>((int)_wantedState, new KeyValuePair<string, object?>("stream", streamName));
            });
            _contextMeter.CreateObservableGauge<long>("flowtide_stream_checkpoint_version", () =>
            {
                Debug.Assert(_stateManager != null, nameof(_stateManager));
                return new Measurement<long>(_stateManager.CurrentVersion, new KeyValuePair<string, object?>("stream", streamName));
            });
            // Meter that tells which version the stream started on, useful for testing
            _contextMeter.CreateObservableGauge<long>("flowtide_stream_start_checkpoint_version", () =>
            {
                return new Measurement<long>(_startCheckpointVersion, new KeyValuePair<string, object?>("stream", streamName));
            });
            if (loggerFactory == null)
            {
                this.loggerFactory = new NullLoggerFactory();
            }
            else
            {
                this.loggerFactory = loggerFactory;
            }
            _logger = this.loggerFactory.CreateLogger<StreamContext>();

            _checkpointLock = new object();

            _stateManager = new FlowtideDotNet.Storage.StateManager.StateManagerSync<StreamState>(stateManagerOptions, this.loggerFactory, new Meter($"flowtide.{streamName}.storage"), streamName, _streamMemoryManager);


            _streamScheduler.Initialize(this);
            // Trigger init
            _triggers = new Dictionary<string, List<OperatorTrigger>>();
            _triggerLock = new object();

            _blockLookup = new Dictionary<string, IStreamVertex>();
            foreach (var block in ingressBlocks)
            {
                _blockLookup.Add(block.Key, block.Value);
            }
            foreach (var block in propagatorBlocks)
            {
                _blockLookup.Add(block.Key, block.Value);
            }
            foreach (var block in egressBlocks)
            {
                _blockLookup.Add(block.Key, block.Value);
            }

            currentState = StreamStateValue.NotStarted;
            _state = new NotStartedStreamState();
            _state.SetContext(this);
            _pauseMonitor = pauseMonitor;

            if (_pauseMonitor != null)
            {
                if (_pauseMonitor.CurrentValue.IsPaused)
                {
                    Pause();
                }
                _pauseMonitor.OnChange((opt) =>
                {
                    if (opt.IsPaused)
                    {
                        Pause();
                    }
                    else
                    {
                        Resume();
                    }
                });
            }
        }

        private Task TransitionTo(StreamStateMachineState current, StreamStateMachineState state, StreamStateValue newValue)
        {
            StreamStateValue previous;
            lock (_contextLock)
            {
                if (current != _state)
                {
                    // The calling state has already been replaced, for example a delete
                    // transitioned away while the failure handling still had a transition
                    // scheduled. A stale transition must not swap the state, and it must not
                    // update the reported state or notify about a transition that never
                    // happens.
                    return Task.CompletedTask;
                }
                previous = currentState;
                currentState = newValue;
                this._state = state;
                this._state.SetContext(this);
            }

            if (_notificationReciever != null)
            {
                try
                {
                    //The notification reciever exceptions should not interupt the transitions
                    _notificationReciever.OnStreamStateChange(newValue);
                }
                catch
                {
                    // All errors are catched so notification reciever cant break the stream
                }
            }
            return this._state.Initialize(previous);
        }

        public Task TransitionTo(StreamStateMachineState current, StreamStateValue newState)
        {
            switch (newState)
            {
                case StreamStateValue.Starting:
                    return TransitionTo(current, new StartStreamState(), newState);
                case StreamStateValue.Failure:
                    return TransitionTo(current, new FailureStreamState(), newState);
                case StreamStateValue.Running:
                    return TransitionTo(current, new RunningStreamState(), newState);
                case StreamStateValue.Deleting:
                    return TransitionTo(current, new DeletingStreamState(), newState);
                case StreamStateValue.Deleted:
                    return TransitionTo(current, new DeletedStreamState(), newState);
                case StreamStateValue.Stopping:
                    return TransitionTo(current, new StoppingStreamState(), newState);
                case StreamStateValue.NotStarted:
                    return TransitionTo(current, new NotStartedStreamState(), newState);
            }
            return Task.CompletedTask;
        }

        public Task CallTrigger(string triggerName, object? state)
        {
            lock (_contextLock)
            {
                Debug.Assert(_state != null, "CallTrigger while not in a state");
                return _state.CallTrigger(triggerName, state);
            }
        }

        public Task CallTrigger(string operatorName, string triggerName, object? state)
        {
            lock (_contextLock)
            {
                return _state!.CallTrigger(operatorName, triggerName, state);
            }
        }

        internal void SetStatus(StreamStatus status)
        {
            lock (_pauseLock)
            {
                if (_pauseSource != null && status != StreamStatus.Paused)
                {
                    // A paused stream keeps showing Paused. Transitions that happen while
                    // paused, for example a recovery, are recorded and become the visible
                    // status again at the resume.
                    _statusBeforePause = status;
                    return;
                }
                _streamStatus = status;
            }
        }

        internal Task CallTrigger_Internal(string triggerName, object? state)
        {
            List<Task>? tasks = null;
            var triggerEvent = new TriggerEvent(triggerName, state);
            lock (_triggerLock)
            {
                if (_triggers.TryGetValue(triggerName, out var list))
                {
                    tasks = new List<Task>();

                    foreach (var opName in list)
                    {
                        if (_blockLookup.TryGetValue(opName.OperatorName, out var vertex))
                        {
                            tasks.Add(vertex.QueueTrigger(triggerEvent));
                        }
                    }
                }
            }
            if (tasks != null)
            {
                return Task.WhenAll(tasks);
            }
            return Task.CompletedTask;
        }

        internal Task CallTrigger_Internal(string operatorName, string triggerName, object? state)
        {
            Task? task = null;
            lock (_triggerLock)
            {
                if (_triggers.TryGetValue(triggerName, out var operatorTriggers) &&
                    operatorTriggers.Any(x => x.OperatorName == operatorName) &&
                    _blockLookup.TryGetValue(operatorName, out var vertex))
                {
                    task = vertex.QueueTrigger(new TriggerEvent(triggerName, state));
                }
            }

            if (task != null)
            {
                return task;
            }
            return Task.CompletedTask;
        }

        internal void ForEachBlock(Action<string, IStreamVertex> action)
        {
            foreach (var block in _blockLookup)
            {
                action(block.Key, block.Value);
            }
        }

        internal async Task ForEachBlockAsync(Func<string, IStreamVertex, Task> action)
        {
            foreach (var block in _blockLookup)
            {
                await action(block.Key, block.Value);
            }
        }

        internal async Task ForEachIngressBlockAsync(Func<string, IStreamVertex, Task> action)
        {
            foreach (var block in ingressBlocks)
            {
                await action(block.Key, block.Value);
            }
        }

        internal async Task ForEachEgressBlockAsync(Func<string, IStreamEgressVertex, Task> action)
        {
            foreach (var block in egressBlocks)
            {
                await action(block.Key, block.Value);
            }
        }

        internal List<Task> GetCompletionTasks()
        {
            List<Task> completionTasks = new List<Task>();

            foreach (var block in propagatorBlocks)
            {
                completionTasks.Add(block.Value.Completion);
            }
            foreach (var block in ingressBlocks)
            {
                completionTasks.Add(block.Value.Completion);
            }
            foreach (var block in egressBlocks)
            {
                completionTasks.Add(block.Value.Completion);
            }
            return completionTasks;
        }

        internal void TryScheduleCheckpointIn(TimeSpan timeSpan, long? checkpointVersion)
        {
            lock (_checkpointLock)
            {
                TryScheduleCheckpointIn_NoLock(timeSpan, checkpointVersion);
            }
        }

        internal bool TryScheduleCheckpointIn_NoLock(TimeSpan timeSpan, long? checkpointVersion)
        {
            Debug.Assert(Monitor.IsEntered(_checkpointLock));

            // Check if minimum time has been set, if so default it to the minimum time.
            if (_dataflowStreamOptions.MinimumTimeBetweenCheckpoints != null &&
                _initialCheckpointTaken &&
                _dataflowStreamOptions.MinimumTimeBetweenCheckpoints.Value.CompareTo(timeSpan) > 0)
            {
                timeSpan = _dataflowStreamOptions.MinimumTimeBetweenCheckpoints.Value;
            }

            var triggerTime = DateTime.Now.Add(timeSpan);

            // Check if a checkpoint is already running, if so, add that a checkpoint is waiting
            // This is required so checkpoints are not missed.
            if (checkpointTask != null)
            {
                // If the provided version is the same as the running one, skip scheduling
                // This is to hinder multiple checkpoints after one another in distributed mode
                if (checkpointVersion.HasValue && _currentProvidedCheckpointVersion.HasValue && checkpointVersion.Value == _currentProvidedCheckpointVersion.Value)
                {
                    return false;
                }
                if (inQueueCheckpoint.HasValue && inQueueCheckpoint.Value.CompareTo(triggerTime) <= 0)
                {
                    return false;
                }
                else
                {
                    _scheduledProvidedCheckpointVersion = checkpointVersion;
                    inQueueCheckpoint = triggerTime;
                    return true;
                }
            }
            else
            {
                _currentProvidedCheckpointVersion = checkpointVersion;
            }

            if (_scheduleCheckpointTask != null)
            {
                if (_triggerCheckpointTime!.Value.CompareTo(triggerTime) <= 0)
                {
                    return false;
                }
                // Cancel previous, and do a new schedule
                _scheduleCheckpointCancelSource!.Cancel();
                _scheduleCheckpointCancelSource.Dispose();
                _scheduleCheckpointCancelSource = null;
            }
            _triggerCheckpointTime = triggerTime;
            _scheduleCheckpointCancelSource = new CancellationTokenSource();
            _scheduleCheckpointTask = Task.Delay(timeSpan, _scheduleCheckpointCancelSource.Token)
                .ContinueWith((t, state) =>
                {
                    var @this = (StreamContext)state!;
                    @this.TriggerCheckpoint(true);
                }, this, _scheduleCheckpointCancelSource.Token);
            return true;
        }

        internal Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            lock (_contextLock)
            {
                Debug.Assert(_state != null, nameof(_state));

                return _state.AddTrigger(operatorName, triggerName, schedule);
            }
        }

        internal Task AddTrigger_Internal(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            lock (_triggerLock)
            {
                if (!_triggersEnabled)
                {
                    return Task.CompletedTask;
                }
                if (!_triggers.TryGetValue(triggerName, out var list))
                {
                    list = new List<OperatorTrigger>();
                    _triggers.Add(triggerName, list);
                }
                if (!list.Any(x => x.OperatorName == operatorName))
                {
                    list.Add(new OperatorTrigger(operatorName, schedule));
                    // Add this to stream scheduler
                    if (schedule.HasValue)
                    {
                        _streamScheduler.Schedule(triggerName, operatorName, schedule.Value);
                    }
                }
            }
            return Task.CompletedTask;
        }

        internal void CancelTriggerRegistration()
        {
            lock (_triggerLock)
            {
                _triggersEnabled = false;
            }
        }

        internal void EnableTriggerRegistration()
        {
            lock (_triggerLock)
            {
                _triggersEnabled = true;
            }
        }

        internal async Task ClearTriggers()
        {
            List<Task> removeTriggerTasks = new List<Task>();
            lock (_triggerLock)
            {
                foreach (var trigger in _triggers)
                {
                    foreach (var val in trigger.Value)
                    {
                        if (val.Interval.HasValue)
                        {
                            removeTriggerTasks.Add(_streamScheduler.RemoveSchedule(trigger.Key, val.OperatorName));
                        }
                    }
                }
            }
            await Task.WhenAll(removeTriggerTasks);
            _triggers.Clear();
        }

        internal void EgressCheckpointDone(string name, ILockingEvent? lockingEvent)
        {
            lock (_contextLock)
            {
                _logger.CallingEgressCheckpointDone(streamName, currentState.ToString());
                _state!.EgressCheckpointDone(name, lockingEvent);
            }
        }

        internal void EgressDependenciesDone(string name)
        {
            // Signals without a locking event come from checkpoint acknowledgement paths and
            // are always treated as checkpoint related.
            EgressDependenciesDone(name, null);
        }

        internal void EgressDependenciesDone(string name, ILockingEvent? lockingEvent)
        {
            lock (_contextLock)
            {
                _state!.EgressDependenciesDone(name, lockingEvent);
            }
        }

        internal Task TriggerCheckpoint(bool isScheduled = false)
        {
            lock (_contextLock)
            {
                _state!.TriggerCheckpoint(isScheduled);
            }
            return Task.CompletedTask;
        }

        internal Task OnFailure(Exception? e)
        {
            // Ingore block stop exceptions
            // Since they are sent by the failure state to stop running blocks
            if (IsBlockStopException(e))
            {
                return Task.CompletedTask;
            }
            var activity = s_exceptionActivitySource.StartActivity("StreamFailure", ActivityKind.Internal, null);

            if (activity != null)
            {
                activity.SetStatus(ActivityStatusCode.Error);
                activity.SetTag("stream", streamName);
                if (e != null)
                {
                    activity.SetTag("exception", e);
                }
                activity.Stop();
                activity.Dispose();
            }

            // The local restore version is not captured here. A checkpoint may still be
            // committing, capturing the last completed version now would discard it. The
            // failure teardown decides the version after the commit settles, see
            // FailureStreamState.StopAndDispose. A peer requested rollback still caps it
            // through FailAndRollback.

            _logger.StreamError(e, streamName);
            lock (_contextLock)
            {
                if (_notificationReciever != null)
                {
                    _notificationReciever.OnFailure(e);
                }

                return _state!.OnFailure();
            }
        }

        private bool IsBlockStopException(Exception? exception)
        {
            if (exception == null)
            {
                return false;
            }
            if (exception is BlockStopException)
            {
                return true;
            }
            if (exception is AggregateException aggregate)
            {
                return aggregate.InnerExceptions.Any(IsBlockStopException);
            }
            return false;
        }

        internal Task StartAsync()
        {
            // A pause that was refused while a stop or delete was pending must still apply
            // to the next start. The options monitor only fires on changes, so the current
            // value is read here, otherwise the stream would run while the configuration
            // says paused.
            if (_pauseMonitor?.CurrentValue.IsPaused == true)
            {
                Pause();
            }
            return _state!.StartAsync();
        }

        internal Task DeleteAsync()
        {
            // The caller awaits the actual deletion, not just the transition into the
            // deleting state. The delete itself runs as a background task, deleting states
            // that only start it would let the caller dispose the stream while the delete
            // still works on the blocks and the state manager.
            Task result;
            lock (_checkpointLock)
            {
                if (_deleteTask == null)
                {
                    _deleteTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                result = _deleteTask.Task;
            }
            // The delete supersedes a pause. The resume runs after the delete task is
            // created, a pause that races this call either sees the pending delete and is
            // refused or is cleared here, a paused source would freeze the teardown.
            Resume();
            lock (_contextLock)
            {
                _ = _state!.DeleteAsync();
            }
            return result;
        }

        internal Task StopAsync()
        {
            // The context lock must never be taken inside the checkpoint lock: the state
            // machine takes the context lock first and the checkpoint lock second, for
            // example when checkpoint acknowledgements from other substreams arrive, nesting
            // them in the opposite order here deadlocks with those paths and every later
            // lock taker piles up behind them, freezing the whole stream.
            Task result;
            bool dispatchStop = false;
            lock (_checkpointLock)
            {
                if (_stopTask == null)
                {
                    _stopTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    dispatchStop = true;
                }
                result = _stopTask.Task;
            }
            if (dispatchStop)
            {
                // The stop supersedes a pause. The resume runs after the stop task is
                // created, a pause that races this call either sees the pending stop and is
                // refused or is cleared here, a paused source would freeze the stop drain.
                Resume();
                lock (_contextLock)
                {
                    _ = _state!.StopAsync();
                }
            }
            return result;
        }

        /// <summary>
        /// Disposes the stream, completes all blocks and then disposes them.
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            CancelTriggerRegistration();
            await ClearTriggers();

            lock (_checkpointLock)
            {
                if (checkpointTask != null)
                {
                    checkpointTask.SetCanceled();
                    checkpointTask = null;
                }
            }

            ForEachBlock((key, block) =>
            {
                block.Complete();
            });

            await ForEachBlockAsync(async (key, block) =>
            {
                await block.DisposeAsync();
            });

            _stateManager.Dispose();

            _streamMemoryManager.Dispose();
        }

        public StreamGraph GetGraph()
        {
            var metricsSnapshot = _streamMetrics.GetSnapshot();
            Dictionary<string, GraphNode> nodes = new Dictionary<string, GraphNode>();
            List<GraphEdge> edges = new List<GraphEdge>();
            // Get links
            foreach (var block in _blockLookup)
            {
                GraphNode? graphNode = null;
                if (metricsSnapshot.TryGetValue(block.Key, out var value))
                {
                    graphNode = new GraphNode(block.Key, block.Value.DisplayName, value.Counters, value.Gauges);
                }
                else
                {
                    graphNode = new GraphNode(block.Key, block.Value.DisplayName, new List<CounterSnapshot>(), new List<GaugeSnapshot>());
                }
                nodes.Add(block.Key, graphNode);

                var links = block.Value.GetLinks();

                foreach (var link in links)
                {
                    if (link is IStreamVertex streamVertex)
                    {
                        edges.Add(new GraphEdge(block.Key, streamVertex.Name));
                    }
                    else if (link is MultipleInputTargetHolder target)
                    {
                        edges.Add(new GraphEdge(block.Key, target.OperatorName));
                    }
                }
            }

            return new StreamGraph(nodes, edges, currentState);
        }

        internal ValueTask CheckForPauseAsync()
        {
            lock (_pauseLock)
            {
                if (_pauseSource != null)
                {
                    return new ValueTask(_pauseSource.Task);
                }
            }
            return ValueTask.CompletedTask;
        }

        internal void CheckForPause()
        {
            Task? task = default;
            lock (_pauseLock)
            {
                if (_pauseSource != null)
                {
                    task = _pauseSource.Task;
                }
            }
            if (task != null)
            {
                task.Wait();
            }
        }

        public void Pause()
        {
            lock (_pauseLock)
            {
                if (_pauseSource != null)
                {
                    return;
                }
                if (_stopTask != null || _deleteTask != null)
                {
                    // A stop or delete supersedes the pause. Gating the sources here would
                    // freeze the drain, a parked source can hold the ingress checkpoint
                    // lock that the stop cycle needs to inject its barrier.
                    return;
                }
                if (currentState == StreamStateValue.Stopping ||
                    currentState == StreamStateValue.Deleting ||
                    currentState == StreamStateValue.Deleted)
                {
                    // The task check above misses a teardown whose caller tasks were already
                    // completed or failed, for example a delete that gave up. Pausing here
                    // would mask the terminal status as Paused forever, nothing resumes a
                    // deleted stream.
                    return;
                }
                _pauseSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _statusBeforePause = Status;
                SetStatus(StreamStatus.Paused);
            }
            // The pause marker is the source of truth and the vertex gates are derived from
            // it, they are applied here and re-applied by SyncPauseGates when the stream
            // enters the running state for vertices that were rebuilt or not present yet.
            ForEachBlock((id, block) => block.Pause());
            bool cleared;
            lock (_pauseLock)
            {
                cleared = _pauseSource == null;
            }
            if (cleared)
            {
                // A concurrent resume won the race after the gates were applied, undo them.
                ForEachBlock((id, block) => block.Resume());
            }
        }

        public void Resume()
        {
            lock (_pauseLock)
            {
                if (_pauseSource == null)
                {
                    return;
                }
                // The marker must be cleared before the status is restored, SetStatus
                // records instead of writes while the marker is set.
                _pauseSource.SetResult();
                _pauseSource = null;
                SetStatus(_statusBeforePause);
            }
            // Release the gates directly from the block registry, the state here can be
            // failure or starting since a paused stream parks its recovery until the resume,
            // and the gates survive on the reused operators across a block rebuild.
            // Resuming a vertex that is not paused is a no-op.
            ForEachBlock((id, block) => block.Resume());
            bool marked;
            lock (_pauseLock)
            {
                marked = _pauseSource != null;
            }
            if (marked)
            {
                // A newer pause raced this resume and its gates were just released,
                // re-apply them, the same undo protocol as Pause uses for the
                // symmetric race.
                ForEachBlock((id, block) => block.Pause());
                bool cleared;
                lock (_pauseLock)
                {
                    cleared = _pauseSource == null;
                }
                if (cleared)
                {
                    ForEachBlock((id, block) => block.Resume());
                }
            }
        }

        /// <summary>
        /// Applies the vertex gates when the pause marker is set, called when the stream
        /// enters the running state, a pause requested while starting or failing only marks
        /// the context. Nothing is released when the marker is not set, gates are only ever
        /// applied together with the marker. The visible status is owned by Pause and
        /// Resume, SetStatus keeps it at Paused while the marker is set.
        /// </summary>
        internal void SyncPauseGates()
        {
            bool paused;
            lock (_pauseLock)
            {
                paused = _pauseSource != null;
            }
            if (paused)
            {
                ForEachBlock((id, block) => block.Pause());
                lock (_pauseLock)
                {
                    paused = _pauseSource != null;
                }
                if (!paused)
                {
                    // A concurrent resume won the race, undo the gates.
                    ForEachBlock((id, block) => block.Resume());
                }
            }
        }

        /// <summary>
        /// Fails the tasks that stop and delete callers await. Used when a teardown gives
        /// up, the callers must observe the failure instead of waiting forever. The stop
        /// task is failed too, a delete implies the stop and a stop caller must not
        /// outwait a failed delete.
        /// </summary>
        internal void FailTeardownWaiters(Exception exception)
        {
            TaskCompletionSource? deleteTask;
            TaskCompletionSource? stopTask;
            lock (_checkpointLock)
            {
                deleteTask = _deleteTask;
                _deleteTask = null;
                stopTask = _stopTask;
                _stopTask = null;
            }
            deleteTask?.TrySetException(exception);
            stopTask?.TrySetException(exception);
        }

        internal Task FailAndRollback(Exception? exception, long? restoreVersion = default)
        {
            lock (_checkpointLock)
            {
                if (restoreVersion.HasValue)
                {
                    // A peer requested rollback version, it caps how far the teardown can
                    // roll forward. The local last completed version is decided later, after
                    // any in-flight commit settled, see FailureStreamState.StopAndDispose.
                    if (!_restoreCheckpointVersion.HasValue || _restoreCheckpointVersion.Value > restoreVersion.Value)
                    {
                        _restoreCheckpointVersion = restoreVersion.Value;
                    }
                }
            }
            
            return OnFailure(exception);
        }
    }
}
