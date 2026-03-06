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

using DataflowStream.dataflow.Internal.Extensions;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.Egress.Internal;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Egress
{
    /// <summary>
    /// Abstract base class for stream egress vertices (sinks) that consume stream events from the dataflow pipeline.
    /// </summary>
    /// <typeparam name="T">The type of the underlying data payload consumed by this vertex.</typeparam>
    /// <remarks>
    /// This vertex acts as an <see cref="ITargetBlock{IStreamEvent}"/> in the TPL Dataflow pipeline, terminating
    /// a stream branch by receiving and handling all incoming messages without producing output.
    /// It handles common stream lifecycle events: state initialization or restoration, processing messages,
    /// recording watermark latency, managing checkpoints, and supporting trigger execution.
    /// Depending on whether parallel execution is enabled via <see cref="ExecutionDataflowBlockOptions"/>,
    /// the internal implementation dispatches to either a parallel or a non-parallel egress block.
    /// Derived classes must implement custom data sink logic via <see cref="OnRecieve"/> and persist
    /// any required state during checkpoints via <see cref="OnCheckpoint"/>.
    /// When a checkpoint completes, the vertex notifies the stream engine automatically through an internal
    /// callback registered by the stream infrastructure.
    /// </remarks>
    public abstract class EgressVertex<T> : ITargetBlock<IStreamEvent>, IStreamEgressVertex 
    {
        private Action<string>? _checkpointDone;
        private Action<string>? _dependenciesDone;
        private readonly ExecutionDataflowBlockOptions _executionDataflowBlockOptions;
        private IEgressImplementation? _targetBlock;
        private bool _isHealthy = true;
        private CancellationTokenSource? _cancellationTokenSource;
        private IHistogram<float>? _latencyHistogram;
        private IMemoryAllocator? _memoryAllocator;
        private IVertexHandler? _vertexHandler;

        private string? _name;
        private string? _streamName;
        private IMeter? _metrics;
        private ILogger? _logger;

        private TaskCompletionSource? _pauseSource;

        private StreamVersionInformation? _streamVersion;

        /// <summary>
        /// Gets the version information of the currently running stream.
        /// </summary>
        public StreamVersionInformation? StreamVersion => _streamVersion;

        /// <summary>
        /// Gets the unique configured operator name of the vertex.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Setup"/> or <see cref="Initialize"/> has been called.
        /// </exception>
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the name of the stream this vertex belongs to.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Setup"/> or <see cref="Initialize"/> has been called.
        /// </exception>
        protected string StreamName => _streamName ?? throw new InvalidOperationException("StreamName can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the meter instance used to create metrics for this vertex.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Initialize"/> has been called.
        /// </exception>
        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the display name for the specific vertex type, used mainly in logging and metrics visualization.
        /// </summary>
        public abstract string DisplayName { get; }

        /// <summary>
        /// Gets the logical checkpoint time identifier of the most recently processed checkpoint.
        /// </summary>
        public long CurrentCheckpointId { get; private set; }

        /// <summary>
        /// Gets the logger instance associated with this vertex.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Initialize"/> has been called.
        /// </exception>
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the cancellation token that is cancelled when the vertex is completing or faulting.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Initialize"/> has been called.
        /// </exception>
        protected CancellationToken CancellationToken => _cancellationTokenSource?.Token ?? throw new InvalidOperationException("Cancellation token can only be fetched after initialization.");

        /// <summary>
        /// Gets the memory allocator for managing native memory allocations within this vertex.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Initialize"/> has been called.
        /// </exception>
        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

        /// <summary>
        /// Initializes a new instance of <see cref="EgressVertex{T}"/> with the specified execution options.
        /// </summary>
        /// <param name="executionDataflowBlockOptions">
        /// Configuration options governing block capacities and parallel execution properties.
        /// When parallel execution is enabled, a <c>ParallelEgressVertex</c> is used internally;
        /// otherwise a <c>NonParallelEgressVertex</c> is used.
        /// </param>
        protected EgressVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            _executionDataflowBlockOptions = executionDataflowBlockOptions;
        }

        [MemberNotNull(nameof(_targetBlock))]
        private void InitializeBlocks()
        {
            if (_executionDataflowBlockOptions.GetSupportsParallelExecution())
            {
                _targetBlock = new ParallelEgressVertex<T>(_executionDataflowBlockOptions, HandleRecieve, HandleLockingEvent, HandleCheckpointDone, OnTrigger, HandleWatermark);
            }
            else
            {
                _targetBlock = new NonParallelEgressVertex<T>(_executionDataflowBlockOptions, HandleRecieve, HandleLockingEvent, HandleCheckpointDone, OnTrigger, HandleWatermark);
            }
        }

        private Task HandleWatermark(Watermark watermark)
        {
            Debug.Assert(_latencyHistogram != null);
            var span = DateTimeOffset.UtcNow.Subtract(watermark.StartTime);
            var latency = (float)span.TotalMilliseconds;
            if (watermark.SourceOperatorId != null)
            {
                _latencyHistogram.Record(latency, new KeyValuePair<string, object?>("source", watermark.SourceOperatorId));
            }
            else
            {
                Logger.RecievedWatermarkWithoutSourceOperator(StreamName, Name);
            }

            return OnWatermark(watermark);
        }

        /// <summary>
        /// Called when a <see cref="Watermark"/> arrives at this egress vertex. Allows derived classes to
        /// react to watermark advancement, for example to flush buffered output per time boundary.
        /// </summary>
        /// <param name="watermark">The watermark event received from upstream.</param>
        /// <returns>A task representing the watermark handling operation.</returns>
        protected virtual Task OnWatermark(Watermark watermark)
        {
            return Task.CompletedTask;
        }

        private void HandleCheckpointDone()
        {
            if (_checkpointDone != null && Name != null)
            {
                Logger.CallingCheckpointDone(StreamName, Name);
                _checkpointDone(Name);
            }
            else
            {
                Logger.CheckpointDoneFunctionNotSet(StreamName, Name ?? "");
            }
            DependenciesDone();
        }

        private Task HandleLockingEvent(ILockingEvent lockingEvent)
        {
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                return HandleCheckpoint(checkpointEvent);
            }
            return Task.CompletedTask;
        }

        private async Task HandleCheckpoint(ICheckpointEvent checkpointEvent)
        {
            CurrentCheckpointId = checkpointEvent.CheckpointTime;
            await OnCheckpoint(checkpointEvent.CheckpointTime);
        }

        /// <summary>
        /// Handles custom logic executed when the vertex processes a queued trigger event.
        /// </summary>
        /// <param name="name">The name identifier distinguishing the associated trigger.</param>
        /// <param name="state">Optional state associated with the trigger.</param>
        /// <returns>A task representing the trigger handling operation.</returns>
        public virtual Task OnTrigger(string name, object? state)
        {
            return Task.CompletedTask;
        }

        private void DependenciesDone()
        {
            if (_dependenciesDone != null && Name != null)
            {
                _dependenciesDone(Name);
            }
            else
            {
                throw new InvalidOperationException("Dependencies done function is not set or Name is null. Ensure that the vertex has been properly initialized before calling this method.");
            }
        }

        /// <summary>
        /// Invoked when a checkpoint is triggered. Allows the vertex to persist any required state.
        /// </summary>
        /// <param name="checkpointTime">The logical timestamp for this checkpoint.</param>
        /// <returns>A task representing the state persistence operation.</returns>
        protected abstract Task OnCheckpoint(long checkpointTime);

        private async Task HandleRecieve(T msg, long time)
        {
            await OnRecieve(msg, time).ConfigureAwait(false);
            if (msg is IRentable rentable)
            {
                rentable.Return();
            }
        }

        /// <summary>
        /// Called when the vertex receives data from upstream. Derived classes implement the actual sink logic here,
        /// such as writing records to a database, file, or external service.
        /// </summary>
        /// <param name="msg">The message payload received from upstream.</param>
        /// <param name="time">The logical stream time at which the message was produced.</param>
        /// <returns>A task representing the message handling operation.</returns>
        protected abstract Task OnRecieve(T msg, long time);

        /// <summary>
        /// Gets a <see cref="Task"/> that represents the asynchronous completion of the vertex's internal processing block.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// Thrown when accessed before <see cref="CreateBlock"/> has been called.
        /// </exception>
        public Task Completion => _targetBlock?.Completion ?? throw new NotSupportedException("CreateBlocks must be called before getting completion");

        /// <summary>
        /// Signals to the dataflow block that it should not accept nor produce any more messages nor consume any more postponed messages.
        /// </summary>
        public void Complete()
        {
            Debug.Assert(_targetBlock != null, "CreateBlocks must be called before completing");
            _targetBlock.Complete();
        }

        /// <summary>
        /// Puts the underlying dataflow block immediately into a faulted state due to a severe exception.
        /// </summary>
        /// <param name="exception">The exception that caused the fault.</param>
        public void Fault(Exception exception)
        {
            _cancellationTokenSource?.Cancel();
            Debug.Assert(_targetBlock != null, "CreateBlocks must be called before faulting");
            _targetBlock.Fault(exception);
        }

        /// <summary>
        /// Asynchronously initializes the vertex, wiring up metrics, memory, logging, and persistent state retrieval.
        /// </summary>
        /// <param name="name">The name assigned to this vertex.</param>
        /// <param name="restoreTime">The logical time representing the last known good state to restore from.</param>
        /// <param name="newTime">The new logical stream execution time after initialization.</param>
        /// <param name="vertexHandler">The handler providing state clients, memory managers, logger factories, and metrics.</param>
        /// <param name="streamVersionInformation">Optional version information used to handle stream upgrades or downgrades.</param>
        /// <returns>A task representing the asynchronous initialization and state restore operation.</returns>
        public Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation)
        {
            _memoryAllocator = vertexHandler.MemoryManager;
            _cancellationTokenSource = new CancellationTokenSource();
            _name = name;
            _streamName = vertexHandler.StreamName;
            _metrics = vertexHandler.Metrics;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _streamVersion = streamVersionInformation;
            CurrentCheckpointId = newTime;
            _vertexHandler = vertexHandler;

            Metrics.CreateObservableGauge("busy", () =>
            {
                Debug.Assert(_targetBlock != null, nameof(_targetBlock));
                return ((float)_targetBlock.InputQueue) / _targetBlock.MaxInputQueue;
            });

            Metrics.CreateObservableGauge("InputQueue", () =>
            {
                Debug.Assert(_targetBlock != null, nameof(_targetBlock));
                return _targetBlock.InputQueue;
            });

            Metrics.CreateObservableGauge("health", () =>
            {
                return _isHealthy ? 1 : 0;
            });
            Metrics.CreateObservableGauge("metadata", () =>
            {
                TagList tags = new TagList
                {
                    { "id", Name },
                    { "links", "[]" }
                };
                return new Measurement<int>(1, tags);
            });
            _latencyHistogram = Metrics.CreateHistogram<float>("latency");

            return InitializeOrRestore(restoreTime, vertexHandler.StateClient);
        }

        /// <summary>
        /// Sets the health status of this vertex, affecting the <c>health</c> metric gauge reported to the stream.
        /// </summary>
        /// <param name="healthy"><see langword="true"/> if the vertex is operating normally; <see langword="false"/> if it is in a degraded state.</param>
        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        /// <summary>
        /// Restores or initializes the operator's persistent state from the provided state manager client.
        /// </summary>
        /// <param name="restoreTime">The logical time to restore state from if recovering from a previous checkpoint.</param>
        /// <param name="stateManagerClient">The client used to access persistent storage for state restoration.</param>
        /// <returns>A task representing the asynchronous state restore or initialization operation.</returns>
        protected abstract Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient);

        /// <summary>
        /// Offers a message to this dataflow block, giving it the opportunity to accept or postpone the message.
        /// Generally called automatically by preceding linked source blocks in the pipeline.
        /// </summary>
        /// <param name="messageHeader">A <see cref="DataflowMessageHeader"/> instance that represents the header of the message being offered.</param>
        /// <param name="messageValue">The <see cref="IStreamEvent"/> being offered.</param>
        /// <param name="source">The <see cref="ISourceBlock{IStreamEvent}"/> offering the message, or <see langword="null"/> if there is no source.</param>
        /// <param name="consumeToAccept">
        /// <see langword="true"/> if the target must call
        /// <see cref="ISourceBlock{IStreamEvent}.ConsumeMessage"/> to consume the message; <see langword="false"/> if the message is consumed implicitly on acceptance.
        /// </param>
        /// <returns>
        /// A <see cref="DataflowMessageStatus"/> value indicating whether the message was accepted, declined, or postponed.
        /// </returns>
        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(_targetBlock != null, nameof(_targetBlock));
            return _targetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        void IStreamEgressVertex.SetCheckpointDoneFunction(Action<string> checkpointDone, Action<string> dependenciesDone)
        {
            _checkpointDone = checkpointDone;
            _dependenciesDone = dependenciesDone;
        }

        /// <summary>
        /// Requests the operator to compact its persistent storage.
        /// </summary>
        /// <returns>A task representing the compaction process.</returns>
        public abstract Task Compact();

        /// <summary>
        /// Enqueues an execution trigger to be processed by the vertex.
        /// </summary>
        /// <param name="triggerEvent">The event data concerning the trigger schedule and state.</param>
        /// <returns>A task representing the enqueue operation.</returns>
        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            Debug.Assert(_targetBlock != null, nameof(_targetBlock));
            return _targetBlock.SendAsync(triggerEvent);
        }

        /// <summary>
        /// Releases internal resources held by this vertex asynchronously.
        /// </summary>
        public virtual ValueTask DisposeAsync()
        {
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Dispose();
                _cancellationTokenSource = null;
            }

            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Creates the actual links between the configured inputs of this vertex and its upstream sources.
        /// This is a no-op for egress vertices as they have no downstream outputs to link.
        /// </summary>
        public void Link()
        {
        }

        /// <summary>
        /// Instantiates the internal TPL Dataflow processing block. Must be called before messages can be offered
        /// or before <see cref="Completion"/> is accessed.
        /// </summary>
        public void CreateBlock()
        {
            InitializeBlocks();
        }

        /// <summary>
        /// Initiates the deletion process, allowing derived classes to permanently clean up any persistent state
        /// associated with this vertex.
        /// </summary>
        /// <returns>A task representing the delete operation.</returns>
        public abstract Task DeleteAsync();

        /// <summary>
        /// Performs initial naming configuration for the vertex before it is fully initialized.
        /// </summary>
        /// <param name="streamName">The overarching name of the stream this vertex belongs to.</param>
        /// <param name="operatorName">The unique name for this operator within the stream.</param>
        public void Setup(string streamName, string operatorName)
        {
            _name = operatorName;
            _streamName = streamName;
        }

        /// <summary>
        /// Returns all downstream <see cref="ITargetBlock{IStreamEvent}"/> linked from this vertex.
        /// Egress vertices have no downstream links and always return an empty enumerable.
        /// </summary>
        /// <returns>An empty enumerable, as egress vertices have no output links.</returns>
        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return Enumerable.Empty<ITargetBlock<IStreamEvent>>();
        }

        /// <summary>
        /// Awaits any active pause before allowing the caller to continue processing.
        /// Returns immediately if the vertex is not currently paused.
        /// </summary>
        /// <returns>
        /// A <see cref="ValueTask"/> that completes when the vertex is resumed, or immediately if not paused.
        /// </returns>
        protected ValueTask CheckForPause()
        {
            if (_pauseSource != null)
            {
                return new ValueTask(_pauseSource.Task);
            }
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Suspends message processing, causing the vertex to hold output until <see cref="Resume"/> is called.
        /// </summary>
        public void Pause()
        {
            if (_pauseSource == null)
            {
                _pauseSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        /// <summary>
        /// Resumes message processing after a prior call to <see cref="Pause"/>.
        /// </summary>
        public void Resume()
        {
            if (_pauseSource != null)
            {
                _pauseSource.SetResult();
                _pauseSource = null;
            }
        }

        /// <summary>
        /// Called before the checkpoint state is persisted. Allows the vertex to flush any pending state
        /// before the checkpoint is written.
        /// </summary>
        /// <returns>A task representing the pre-checkpoint operation.</returns>
        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Requests the stream engine to fail and initiate a rollback to a previous safe state.
        /// </summary>
        /// <param name="exception">An optional exception describing the reason for the failure.</param>
        /// <param name="restoreVersion">An optional specific version to roll back to; if <see langword="null"/>, the latest checkpoint is used.</param>
        /// <returns>A task representing the fail-and-rollback operation.</returns>
        protected Task FailAndRollback(Exception? exception = null, long? restoreVersion = null)
        {
            Debug.Assert(_vertexHandler != null, nameof(_vertexHandler));

            if (_vertexHandler == null)
            {
                throw new NotSupportedException("Cannot fail and rollback before initialize is called");
            }
            return _vertexHandler.FailAndRollback(exception, restoreVersion);
        }

        /// <summary>
        /// Called when the stream encounters a failure, allowing the vertex to roll back to a safe state.
        /// </summary>
        /// <param name="rollbackVersion">The version to roll back to.</param>
        /// <returns>A task representing the rollback operation.</returns>
        public Task OnFailure(long rollbackVersion)
        {
            return Task.CompletedTask;
        }
    }
}
