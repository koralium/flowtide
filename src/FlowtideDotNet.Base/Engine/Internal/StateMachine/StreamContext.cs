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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Metrics.Counter;
using FlowtideDotNet.Base.Metrics.Gauge;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    public enum StreamStateValue
    {
        NotStarted = 0,
        Starting = 1,
        Running = 2,
        Failure = 3,
        Deleting = 4,
        Deleted = 5,
        Stopping = 6
    }

    internal class StreamContext : IStreamTriggerCaller, IAsyncDisposable
    {
        internal readonly string streamName;
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

        internal StreamState? _lastState;
        internal long producingTime = 0;
        internal Task? _onFailureTask;
        internal TaskCompletionSource? checkpointTask;
        internal DateTimeOffset? inQueueCheckpoint;

        internal TaskCompletionSource? _stopTask;

        private StreamStateMachineState? _state = null;

        internal Task? _scheduleCheckpointTask;
        internal DateTime? _triggerCheckpointTime;
        internal CancellationTokenSource? _scheduleCheckpointCancelSource;

        internal StreamStateValue currentState;
        internal StreamStateValue _wantedState;

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

        public StreamStatus Status => _streamStatus;

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
                        if (currentState == Base.Engine.Internal.StateMachine.StreamStateValue.Running)
                        {
                            return FlowtideHealth.Healthy;
                        }
                        if (currentState == Base.Engine.Internal.StateMachine.StreamStateValue.Failure)
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

            _stateManager = new FlowtideDotNet.Storage.StateManager.StateManagerSync<StreamState>(stateManagerOptions, this.loggerFactory.CreateLogger("StateManager"), new Meter($"flowtide.{streamName}.storage"), streamName);


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

        private Task TransitionTo(StreamStateMachineState current, StreamStateMachineState state, StreamStateValue previous)
        {
            lock (_contextLock)
            {
                if (current != _state)
                {
                    return Task.CompletedTask;
                }
                this._state = state;
                this._state.SetContext(this);
            }
            this._state.Initialize(previous);
            return Task.CompletedTask;
        }

        public Task TransitionTo(StreamStateMachineState current, StreamStateValue newState)
        {
            if (_notificationReciever != null)
            {
                try
                {
                    //The notification reciever exceptions should not interupt the transitions
                    _notificationReciever.OnStreamStateChange(newState);
                }
                catch
                {
                    // All errors are catched so notification reciever cant break the stream
                }
            }
            var oldState = currentState;
            currentState = newState;
            switch (newState)
            {
                case StreamStateValue.Starting:
                    return TransitionTo(current, new StartStreamState(), oldState);
                case StreamStateValue.Failure:
                    return TransitionTo(current, new FailureStreamState(), oldState);
                case StreamStateValue.Running:
                    return TransitionTo(current, new RunningStreamState(), oldState);
                case StreamStateValue.Deleting:
                    return TransitionTo(current, new DeletingStreamState(), oldState);
                case StreamStateValue.Deleted:
                    return TransitionTo(current, new DeletedStreamState(), oldState);
                case StreamStateValue.Stopping:
                    return TransitionTo(current, new StoppingStreamState(), oldState);
                case StreamStateValue.NotStarted:
                    return TransitionTo(current, new NotStartedStreamState(), oldState);
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
            _streamStatus = status;
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

        internal void TryScheduleCheckpointIn(TimeSpan timeSpan)
        {
            lock (_checkpointLock)
            {
                TryScheduleCheckpointIn_NoLock(timeSpan);
            }
        }

        internal bool TryScheduleCheckpointIn_NoLock(TimeSpan timeSpan)
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
                if (inQueueCheckpoint.HasValue && inQueueCheckpoint.Value.CompareTo(triggerTime) <= 0)
                {
                    return false;
                }
                else
                {
                    inQueueCheckpoint = triggerTime;
                    return true;
                }
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

        internal void EgressCheckpointDone(string name)
        {
            lock (_contextLock)
            {
                _logger.CallingEgressCheckpointDone(streamName, currentState.ToString());
                _state!.EgressCheckpointDone(name);
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

        internal Task StartAsync()
        {
            return _state!.StartAsync();
        }

        internal Task DeleteAsync()
        {
            lock (_contextLock)
            {
                return _state!.DeleteAsync();
            }
        }

        internal Task StopAsync()
        {
            lock (_checkpointLock)
            {
                if (_stopTask == null)
                {
                    _stopTask = new TaskCompletionSource();
                    lock (_contextLock)
                    {
                        _ = _state!.StopAsync();
                    }
                }
                return _stopTask.Task;
            }
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
                _pauseSource = new TaskCompletionSource();
                _state!.Pause();
                _statusBeforePause = Status;
                SetStatus(StreamStatus.Paused);
            }
        }

        public void Resume()
        {
            lock (_pauseLock)
            {
                if (_pauseSource != null)
                {
                    SetStatus(_statusBeforePause);
                    _pauseSource.SetResult();
                    _pauseSource = null;
                    _state!.Resume();
                }
            }
        }
    }
}
