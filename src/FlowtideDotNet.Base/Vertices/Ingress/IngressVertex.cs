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

using FlowtideDotNet.Base.dataflow;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices
{
    internal class IngressState<TData>
    {
        public BufferBlock<IStreamEvent>? _block;
        public ISourceBlock<IStreamEvent>? _sourceBlock;
        public long _currentTime;
        public long _restoreTime;
        public IVertexHandler? _vertexHandler;
        public SemaphoreSlim? _checkpointLock;
        public bool _inCheckpointLock;
        public IngressOutput<TData>? _output;
        public CancellationTokenSource? _tokenSource;
        public IMeter? _metrics;
        public bool _taskEnabled = false;
        public int _linkCount;
    }

    /// <summary>
    /// Base class for stream ingress vertices (sources) that produce stream events into the dataflow pipeline.
    /// </summary>
    /// <typeparam name="TData">The type of the underlying data payload produced by this vertex.</typeparam>
    /// <remarks>
    /// This vertex acts as an <see cref="ISourceBlock{IStreamEvent}"/> in the TPL Dataflow pipeline.
    /// It handles common stream lifecycle events: state initialization or restoration, generating checkpoints, 
    /// managing triggers, and forwarding locking events. Derived classes must implement 
    /// custom data source reading logic and use <see cref="IngressOutput{TData}"/> to emit events.
    /// </remarks>
    public abstract class IngressVertex<TData> : ISourceBlock<IStreamEvent>, IStreamIngressVertex
    {
        private readonly object _stateLock;
        private readonly DataflowBlockOptions options;
        private readonly List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)> _links = new List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)>();
        private IngressState<TData>? _ingressState;
        private readonly Dictionary<int, Task> _runningTasks;
        private int _taskIdCounter;
        private ILogger? _logger;
        private bool _isHealthy = true;
        private Action<string>? _dependenciesDone;

        /// <summary>
        /// Gets the configured name of the vertex.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the name of the stream this vertex belongs to.
        /// </summary>
        protected string StreamName { get; private set; }

        /// <summary>
        /// Gets the version information of the currently running stream.
        /// </summary>
        public StreamVersionInformation? StreamVersion { get; private set; }

        /// <summary>
        /// Gets the display name for the specific vertex type, used mainly in logging and metrics.
        /// </summary>
        public abstract string DisplayName { get; }

        /// <summary>
        /// Gets the meter instance used to create metrics for this vertex.
        /// </summary>
        protected IMeter Metrics => _ingressState?._metrics ?? throw new NotSupportedException("Initialize must be called before accessing metrics");

        /// <summary>
        /// Gets the logger instance associated with this vertex.
        /// </summary>
        public ILogger Logger => _logger ?? throw new NotSupportedException("Logging must be done after Initialize");

        /// <summary>
        /// Gets the memory allocator for managing native memory allocations within this vertex.
        /// </summary>
        protected IMemoryAllocator MemoryAllocator => _ingressState?._vertexHandler?.MemoryManager ?? throw new NotSupportedException("Initialize must be called before accessing memory allocator");

        /// <summary>
        /// Gets or sets a value indicating whether dependencies should be automatically completed.
        /// </summary>
        /// <remarks>
        /// Dependencies could be other streams when running in a distributed mode, a checkpoint is not considered
        /// fully complete until all dependencies are completed.
        /// </remarks>
        protected bool AutoCompleteDependencies { get; set; } = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="IngressVertex{TData}"/> class with the specified execution options.
        /// </summary>
        /// <param name="options">The options that govern the execution behavior of the inner dataflow block.</param>
        protected IngressVertex(DataflowBlockOptions options)
        {
            _stateLock = new object();
            this.options = options;
            _runningTasks = new Dictionary<int, Task>();
            Name = "";
            StreamName = "";
        }

        private void InitializeBlock()
        {
            lock (_stateLock)
            {
                if (options.CancellationToken.IsCancellationRequested)
                {
                    throw new InvalidOperationException("ExecutionDataflowBlockOptions CancellationToken is already cancalled, can not create the block");
                }
                _ingressState = new IngressState<TData>();
                _ingressState._checkpointLock = new SemaphoreSlim(1, 1);
                _ingressState._block = new BufferBlock<IStreamEvent>(options);
                _ingressState._linkCount = _links.Count;

                ISourceBlock<IStreamEvent> source = _ingressState._block;

                if (_links.Count > 1)
                {
                    var broadcastBlock = new GuaranteedBroadcastBlock<IStreamEvent>(new ExecutionDataflowBlockOptions()
                    {
                        MaxDegreeOfParallelism = 1,
                        BoundedCapacity = options.BoundedCapacity
                    });
                    source = broadcastBlock;
                    _ingressState._block.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
                }

                _ingressState._sourceBlock = source;
                _ingressState._output = new IngressOutput<TData>(_ingressState, _ingressState._block);
                _ingressState._tokenSource = new CancellationTokenSource();
                _ingressState._block.Completion.ContinueWith(t =>
                {
                    Logger.LogDebug(t.Exception, "Block failure");
                    lock (_stateLock)
                    {
                        _ingressState._taskEnabled = false;
                    }

                    _ingressState._tokenSource.Cancel();
                });
            }
        }

        /// <summary>
        /// Gets a <see cref="Task"/> that represents the asynchronous operation and completion of the dataflow block.
        /// </summary>
        public Task Completion => GetCompletion();

        private Task GetCompletion()
        {
            lock (_stateLock)
            {
                Debug.Assert(_ingressState != null, nameof(_ingressState));
                Debug.Assert(_ingressState._block != null, nameof(_ingressState._block));

                List<Task> tasks = new List<Task>();
                tasks.Add(_ingressState._block.Completion);

                foreach (var task in _runningTasks.Values)
                {
                    tasks.Add(task);
                }

                return Task.WhenAll(tasks);
            }
        }

        /// <summary>
        /// Invoked when a checkpoint is triggered. Allows the vertex to persist any required state.
        /// </summary>
        /// <param name="checkpointTime">The logical timestamp for this checkpoint.</param>
        /// <returns>A task representing the state persistence operation.</returns>
        protected abstract Task OnCheckpoint(long checkpointTime);

        /// <summary>
        /// Signals to the dataflow block that it should not accept nor produce any more messages nor consume any more postponed messages.
        /// </summary>
        public void Complete()
        {
            Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));
            Debug.Assert(_ingressState?._tokenSource != null, nameof(_ingressState._tokenSource));

            lock (_stateLock)
            {
                _ingressState._taskEnabled = false;
                _ingressState._tokenSource.Cancel();
                _ingressState._block.Complete();
            }
        }

        /// <summary>
        /// Called by a linked <see cref="ITargetBlock{TInput}"/> to accept and consume a <see cref="DataflowMessageHeader"/> previously offered by this <see cref="ISourceBlock{TOutput}"/>.
        /// </summary>
        /// <param name="messageHeader">The message header to consume.</param>
        /// <param name="target">The target returning the message.</param>
        /// <param name="messageConsumed">true if the message was successfully consumed; otherwise, false.</param>
        /// <returns>The value of the consumed message.</returns>
        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));

            return _ingressState._sourceBlock.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        /// <summary>
        /// Causes the dataflow block to complete in a <see cref="TaskStatus.Faulted"/> state.
        /// </summary>
        /// <param name="exception">The <see cref="Exception"/> that caused the faulting.</param>
        public void Fault(Exception exception)
        {
            Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));
            Debug.Assert(_ingressState?._tokenSource != null, nameof(_ingressState._tokenSource));
            lock (_stateLock)
            {
                _ingressState._taskEnabled = false;
                _ingressState._tokenSource.Cancel();
                (_ingressState._block as IDataflowBlock).Fault(exception);
            }
        }

        /// <summary>
        /// Links the <see cref="ISourceBlock{TOutput}"/> to the specified <see cref="ITargetBlock{TInput}"/>.
        /// </summary>
        /// <param name="target">The <see cref="ITargetBlock{IStreamEvent}"/> to which to connect this source.</param>
        /// <param name="linkOptions">A <see cref="DataflowLinkOptions"/> instance that configures the link.</param>
        /// <returns>An IDisposable that, upon calling Dispose, will disconnect the source from the target.</returns>
        public IDisposable LinkTo(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            _links.Add((target, linkOptions));
            return default!;
        }

        private void LinkTo_Internal(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));
            _ingressState._sourceBlock.LinkTo(target, linkOptions);
        }

        /// <summary>
        /// Called by a linked <see cref="ITargetBlock{TInput}"/> to release a previously reserved <see cref="DataflowMessageHeader"/> by this <see cref="ISourceBlock{TOutput}"/>.
        /// </summary>
        /// <param name="messageHeader">The message header being released.</param>
        /// <param name="target">The target that reserved the message.</param>
        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));
            _ingressState._sourceBlock.ReleaseReservation(messageHeader, target);
        }

        /// <summary>
        /// Called by a linked <see cref="ITargetBlock{TInput}"/> to reserve a previously offered <see cref="DataflowMessageHeader"/> by this <see cref="ISourceBlock{TOutput}"/>.
        /// </summary>
        /// <param name="messageHeader">The message header to reserve.</param>
        /// <param name="target">The target reserving the message.</param>
        /// <returns>true if the message was successfully reserved; otherwise, false.</returns>
        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));
            return _ingressState._sourceBlock.ReserveMessage(messageHeader, target);
        }

        /// <summary>
        /// Invoked after initialization has completed to send the initial data over the stream.
        /// </summary>
        /// <param name="output">The <see cref="IngressOutput{TData}"/> used to emit events.</param>
        /// <returns>A task representing the initial data generation operation.</returns>
        protected abstract Task SendInitial(IngressOutput<TData> output);

        /// <summary>
        /// Signals the vertex to perform state compaction.
        /// </summary>
        /// <returns>A task representing the compaction process.</returns>
        public abstract Task Compact();

        private async Task RunLockingEvent(IngressOutput<TData> output, object? state)
        {
            Debug.Assert(_ingressState?._checkpointLock != null, nameof(_ingressState._checkpointLock));
            Debug.Assert(_ingressState?._output != null, nameof(_ingressState._output));

            var lockingEvent = (ILockingEvent)state!;
            await _ingressState._checkpointLock.WaitAsync(_ingressState._output.CancellationToken);
            _ingressState._inCheckpointLock = true;
            bool isStopStreamEvent = false;
            if (lockingEvent is ICheckpointEvent checkpoint)
            {
                if (checkpoint is StopStreamCheckpoint)
                {
                    isStopStreamEvent = true;
                    output.Stop();
                }
                await OnCheckpoint(checkpoint.CheckpointTime);
                await output.SendLockingEvent(lockingEvent);
                if (AutoCompleteDependencies)
                {
                    SetDependenciesDone();
                }
            }
            if (lockingEvent is InitWatermarksEvent initWatermark)
            {
                var names = await GetWatermarkNames();
                await output.SendLockingEvent(initWatermark.AddWatermarkNames(names));
                SetDependenciesDone();
            }

            if (!isStopStreamEvent)
            {
                _ingressState._inCheckpointLock = false;
                _ingressState._checkpointLock.Release();
            }
        }

        /// <summary>
        /// Retrieves the set of watermark names supported by this ingress vertex.
        /// </summary>
        /// <returns>A read-only set of watermark names.</returns>
        protected abstract Task<IReadOnlySet<string>> GetWatermarkNames();

        /// <summary>
        /// Marks the dependencies for this vertex as completed.
        /// </summary>
        protected void SetDependenciesDone()
        {
            if (_dependenciesDone != null)
            {
                _dependenciesDone(Name);
            }
            else
            {
                throw new InvalidOperationException("Dependencies done function is not set, cannot mark dependencies as done");
            }
        }

        /// <summary>
        /// Triggers a locking event asynchronously for the stream.
        /// </summary>
        /// <remarks>
        /// This method is used to send checkpoints or other event types that require synchronization with the stream's processing. 
        /// </remarks>
        /// <param name="lockingEvent">The locking event to execute.</param>
        public void DoLockingEvent(ILockingEvent lockingEvent)
        {
            RunTask(RunLockingEvent, lockingEvent);
        }

        /// <summary>
        /// Schedules a checkpoint to occur after the specified delay.
        /// </summary>
        /// <param name="inTime">The timespan indicating how long to wait before checkpointing.</param>
        protected void ScheduleCheckpoint(TimeSpan inTime)
        {
            Debug.Assert(_ingressState?._vertexHandler != null, nameof(_ingressState._vertexHandler));

            if (_ingressState._vertexHandler == null)
            {
                throw new NotSupportedException("Cannot schedule checkpoint before initialize");
            }
            _ingressState._vertexHandler.ScheduleCheckpoint(inTime);
        }

        private sealed record TaskState(Func<IngressOutput<TData>, object?, Task> func, IngressOutput<TData> ingressOutput, object? state, int taskId);

        /// <summary>
        /// Starts a background task linked to the stream's lifecycle that can output data to the stream.
        /// </summary>
        /// <param name="task">The task function taking an <see cref="IngressOutput{TData}"/> and an optional state.</param>
        /// <param name="state">Optional state passed to the task.</param>
        /// <param name="taskCreationOptions">Options to customize the task's creation and execution behavior.</param>
        /// <returns>The started <see cref="Task"/>.</returns>
        protected Task RunTask(Func<IngressOutput<TData>, object?, Task> task, object? state = null, TaskCreationOptions taskCreationOptions = TaskCreationOptions.None)
        {
            Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));
            Debug.Assert(_ingressState._output != null, nameof(_ingressState._output));

            TaskState tState;
            lock (_stateLock)
            {
                if (_ingressState._block.Completion.IsFaulted || !_ingressState._taskEnabled)
                {
                    return Task.CompletedTask;
                }

                var taskId = _taskIdCounter++;
                tState = new TaskState(task, _ingressState._output, state, taskId);
                var t = Task.Factory.StartNew((state) =>
                {
                    var taskState = (TaskState)state!;
                    return taskState.func(taskState.ingressOutput, taskState.state);
                }, tState, _ingressState._output.CancellationToken, taskCreationOptions, TaskScheduler.Default)
                .Unwrap();


                _runningTasks.Add(taskId, t);
                t.ContinueWith((task, state) =>
                {
                    var taskState = (TaskState)state!;
                    if (t.IsFaulted)
                    {
                        taskState.ingressOutput.Fault(task.Exception ?? new AggregateException("Error in task without exception"));
                    }
                    lock (_stateLock)
                    {
                        _runningTasks.Remove(taskState.taskId);
                    }
                }, tState, default, TaskContinuationOptions.None, TaskScheduler.Default);


                return t;
            }
        }

        /// <summary>
        /// Called when the internal initialization sequence has completed successfully.
        /// </summary>
        /// <returns>A task that handles post-initialization tasks, such as triggering initial data emission.</returns>
        public Task InitializationCompleted()
        {
            return RunTask(async (output, state) =>
            {
                await SendInitial(output);
                // Send event here that initial is completed
                await output.SendEvent(new InitialDataDoneEvent());
            }, taskCreationOptions: TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Asynchronously initializes the vertex, wiring up metrics, dependencies, and persistent state retrieval.
        /// </summary>
        /// <param name="name">The name assigned to the vertex.</param>
        /// <param name="restoreTime">The time representing the last known good state to restore from.</param>
        /// <param name="newTime">The new logical stream execution time.</param>
        /// <param name="vertexHandler">The handler containing stream environment references like state client and metrics.</param>
        /// <param name="streamVersionInformation">Configuration tracking the overall version of stream changes.</param>
        public async Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation)
        {
            Debug.Assert(_ingressState != null, nameof(_ingressState));

            Name = name;
            StreamName = vertexHandler.StreamName;
            StreamVersion = streamVersionInformation;

            if (_runningTasks.Count > 0)
            {
                throw new InvalidOperationException("Initialize while there are running tasks");
            }

            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);

            _ingressState._vertexHandler = vertexHandler;
            _ingressState._currentTime = newTime;
            _ingressState._restoreTime = restoreTime;
            _ingressState._metrics = vertexHandler.Metrics;

            Metrics.CreateObservableGauge("backpressure", () =>
            {
                Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));

                return ((float)_ingressState._block.Count) / options.BoundedCapacity;
            });
            Metrics.CreateObservableGauge<float>("busy", () =>
            {
                Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));

                var backpressurevalue = ((float)_ingressState._block.Count) / options.BoundedCapacity;
                if (_runningTasks.Count > 0)
                {
                    return (1.0f - backpressurevalue);
                }
                return 0.0f;
            });

            Metrics.CreateObservableGauge("health", () =>
            {
                return _isHealthy ? 1 : 0;
            });

            Metrics.CreateObservableGauge("metadata", () =>
            {
                TagList tags = new TagList()
                {
                    { "id", Name }
                };
                var links = GetLinks();
                StringBuilder outputLinks = new StringBuilder();
                outputLinks.Append('[');
                foreach (var link in links)
                {
                    if (link is IStreamVertex streamVertex)
                    {
                        outputLinks.Append(streamVertex.Name);
                    }
                    else if (link is MultipleInputTargetHolder target)
                    {
                        outputLinks.Append(target.OperatorName);
                    }
                    outputLinks.Append(',');
                }
                outputLinks.Remove(outputLinks.Length - 1, 1);
                outputLinks.Append(']');
                tags.Add("links", outputLinks.ToString());
                return new Measurement<int>(1, tags);
            });

            Metrics.CreateObservableGauge("link", () =>
            {
                var links = GetLinks();

                List<Measurement<int>> measurements = new List<Measurement<int>>();

                foreach (var link in links)
                {
                    TagList tags = new TagList
                    {
                        { "source", Name }
                    };
                    if (link is IStreamVertex streamVertex)
                    {
                        tags.Add("target", streamVertex.Name);
                        tags.Add("id", streamVertex.Name + "-" + Name);
                    }
                    else if (link is MultipleInputTargetHolder target)
                    {
                        tags.Add("target", target.OperatorName);
                        tags.Add("id", target.OperatorName + "-" + Name);
                    }
                    measurements.Add(new Measurement<int>(1, tags));
                }
                return measurements;
            });

            await InitializeOrRestore(restoreTime, vertexHandler.StateClient);

            lock (_stateLock)
            {
                _ingressState._taskEnabled = true;
            }
        }

        /// <summary>
        /// Sets the health status of this vertex.
        /// </summary>
        /// <param name="healthy">True if healthy, false otherwise.</param>
        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        /// <summary>
        /// Performs the specific state initialization or restoration logic using the state manager.
        /// </summary>
        /// <param name="restoreTime">The time to restore from.</param>
        /// <param name="stateManagerClient">The state manager client used to access persistent state.</param>
        /// <returns>A task representing the state initialization/restoration operation.</returns>
        protected abstract Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient);

        /// <summary>
        /// Queues a trigger event for asynchronous processing.
        /// </summary>
        /// <param name="triggerEvent">The trigger event to process.</param>
        /// <returns>A task representing the queueing operation.</returns>
        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            return OnTrigger(triggerEvent.Name, triggerEvent.State);
        }

        /// <summary>
        /// Invoked when a trigger activates.
        /// </summary>
        /// <param name="triggerName">The name of the fired trigger.</param>
        /// <param name="state">The optional state associated with the trigger.</param>
        /// <returns>A task representing the trigger processing operation.</returns>
        public abstract Task OnTrigger(string triggerName, object? state);

        /// <summary>
        /// Registers a trigger to fire optionally on a recurring schedule.
        /// </summary>
        /// <param name="name">The trigger name.</param>
        /// <param name="scheduleInterval">An optional interval indicating how often the trigger fires.</param>
        /// <returns>A task representing the trigger registration operation.</returns>
        protected Task RegisterTrigger(string name, TimeSpan? scheduleInterval = null)
        {
            Debug.Assert(_ingressState != null, nameof(_ingressState));

            if (_ingressState._vertexHandler == null)
            {
                throw new NotSupportedException("Cannot register trigger before initialize is called");
            }
            return _ingressState._vertexHandler.RegisterTrigger(name, scheduleInterval);
        }

        /// <summary>
        /// Asynchronously frees, releases, or resets unmanaged resources.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public virtual ValueTask DisposeAsync()
        {
            if (_ingressState != null)
            {
                if (_ingressState._output != null)
                {
                    _ingressState._output.Dispose();
                }
                if (_ingressState._inCheckpointLock && _ingressState._checkpointLock != null)
                {
                    _ingressState._inCheckpointLock = false;
                    _ingressState._checkpointLock.Release();
                }
            }

            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Establishes the previously configured links to target blocks in the dataflow pipeline.
        /// </summary>
        public void Link()
        {
            if (_links.Count > 0)
            {
                foreach (var link in _links)
                {
                    LinkTo_Internal(link.Item1, link.Item2);
                }
            }
        }

        /// <summary>
        /// Creates and configures the internal Dataflow block based on the generated execution options.
        /// </summary>
        public void CreateBlock()
        {
            InitializeBlock();
        }

        /// <summary>
        /// Deletes any persistent state associated with this vertex from the state storage.
        /// </summary>
        /// <returns>A task representing the deletion operation.</returns>
        public abstract Task DeleteAsync();

        /// <summary>
        /// Assigns the stream configuration onto the vertex before initialization.
        /// </summary>
        /// <param name="streamName">The name of the stream.</param>
        /// <param name="operatorName">The name of the operator.</param>
        public void Setup(string streamName, string operatorName)
        {
            Name = operatorName;
            StreamName = streamName;
        }

        /// <summary>
        /// Gets an enumeration of the current target links registered to this output block.
        /// </summary>
        /// <returns>An enumeration of linked <see cref="ITargetBlock{IStreamEvent}"/> instances.</returns>
        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _links.Select(x => x.Item1);
        }

        /// <summary>
        /// Invoked when a checkpoint has successfully completed.
        /// </summary>
        /// <param name="checkpointVersion">The completed checkpoint version.</param>
        /// <returns>A task representing the completion callback operation.</returns>
        public virtual Task CheckpointDone(long checkpointVersion)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Temporarily stops the data output generation for the connected streams.
        /// </summary>
        public void Pause()
        {
            Debug.Assert(_ingressState?._output != null, nameof(_ingressState._output));
            _ingressState._output.Stop();
        }

        /// <summary>
        /// Resumes normal data output generation for previously paused streams.
        /// </summary>
        public void Resume()
        {
            Debug.Assert(_ingressState?._output != null, nameof(_ingressState._output));
            _ingressState._output.Resume();
        }

        /// <summary>
        /// Overridable lifecycle method invoked before the save checkpoint step completes.
        /// </summary>
        /// <returns>A task representing the pre-checkpoint operations.</returns>
        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Triggers a rollback sequence on the data pipeline flow, optionally specifying the version and error.
        /// </summary>
        /// <param name="exception">The exception that triggered the rollback.</param>
        /// <param name="restoreVersion">The optional target version to restore state from.</param>
        /// <returns>A task representing the faulting sequence.</returns>
        protected Task FailAndRollback(Exception? exception = null, long? restoreVersion = null)
        {
            Debug.Assert(_ingressState?._vertexHandler != null, nameof(_ingressState._vertexHandler));

            if (_ingressState._vertexHandler == null)
            {
                throw new NotSupportedException("Cannot fail and rollback before initialize is called");
            }
            return _ingressState._vertexHandler.FailAndRollback(exception, restoreVersion);
        }

        void IStreamIngressVertex.SetDependenciesDoneFunction(Action<string> dependenciesDone)
        {
            _dependenciesDone = dependenciesDone;
        }

        /// <summary>
        /// Indicates a rollback behavior hook whenever a previous version is targeted for restoring due to errors.
        /// </summary>
        /// <param name="rollbackVersion">The version that the stream will be rolled back to.</param>
        public virtual Task OnFailure(long rollbackVersion)
        {
            return Task.CompletedTask;
        }
    }
}
