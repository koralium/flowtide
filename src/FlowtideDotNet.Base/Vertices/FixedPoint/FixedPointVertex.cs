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
    /// <summary>
    /// Abstract base class for stream vertices that implement fixed-point iteration over a cyclic dataflow graph.
    /// </summary>
    /// <typeparam name="T">The type of the underlying data payload processed by this vertex.</typeparam>
    /// <remarks>
    /// <para>
    /// A fixed-point vertex coordinates two distinct input sources — an <em>ingress</em> input carrying new external data
    /// and a <em>feedback</em> input carrying data recycled from the loop — and routes processed results to two output sources:
    /// an <em>egress</em> output forwarding results downstream and a <em>loop</em> output feeding data back into the cycle.
    /// Derived classes express the per-message processing logic via <see cref="OnIngressRecieve"/> and
    /// <see cref="OnFeedbackRecieve"/>, mapping each output event to index <c>0</c> (egress) or <c>1</c> (loop).
    /// </para>
    /// <para>
    /// Checkpointing differs fundamentally from <see cref="MultipleInputVertex{T}"/>: when a checkpoint arrives on the
    /// ingress input, the vertex sends a <see cref="LockingEventPrepare"/> into the loop and waits for the loop to confirm
    /// that no new messages are in flight before propagating the checkpoint to the egress output. The method
    /// <see cref="NoReadSourceInLoop"/> controls this behaviour — returning <see langword="true"/> indicates no persistent
    /// read source exists in the loop, allowing the checkpoint to complete without waiting for an upstream acknowledgement.
    /// </para>
    /// </remarks>
    public abstract class FixedPointVertex<T> : IStreamVertex
    {
        private readonly MultipleInputTargetHolder _ingressTarget;
        private readonly MultipleInputTargetHolder _feedbackTarget;
        private readonly FixedPointSource _egressSource;
        private readonly FixedPointSource _loopSource;
        private readonly ExecutionDataflowBlockOptions _executionDataflowBlockOptions;
        private TransformManyBlock<KeyValuePair<int, IStreamEvent>, KeyValuePair<int, IStreamEvent>>? _transformBlock;
        private string? _name;
        private string? _streamName;
        private ILockingEvent? _waitingLockingEvent;
        private int _messageCountSinceLockingEventPrepare;
        private long _currentTime;
        private Watermark? _latestWatermark;
        private ILogger? _logger;

        /// <summary>
        /// Gets the logger instance associated with this vertex.
        /// </summary>
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        private IMeter? _metrics;
        private StreamVersionInformation? _streamVersion;

        /// <summary>
        /// Gets the meter instance used to create metrics for this vertex.
        /// </summary>
        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        private bool _isHealthy = true;
        private bool _sentLockingEvent;
        private int _targetPrepareCount = 0;
        private bool singleReadSource;
        private TaskCompletionSource? _pauseSource;
        private IMemoryAllocator? _memoryAllocator;
        private bool _receivedInitialLoadDoneEvent;

        /// <summary>
        /// Gets the memory allocator for managing native memory allocations within this vertex.
        /// </summary>
        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

        /// <summary>
        /// Gets the <see cref="ITargetBlock{IStreamEvent}"/> that accepts new external data entering the fixed-point loop.
        /// </summary>
        public ITargetBlock<IStreamEvent> IngressTarget => _ingressTarget;

        /// <summary>
        /// Gets the <see cref="ITargetBlock{IStreamEvent}"/> that accepts recycled data fed back from the loop output.
        /// </summary>
        public ITargetBlock<IStreamEvent> FeedbackTarget => _feedbackTarget;

        /// <summary>
        /// Gets the <see cref="ISourceBlock{IStreamEvent}"/> that emits processed results downstream out of the loop.
        /// </summary>
        public ISourceBlock<IStreamEvent> EgressSource => _egressSource;

        /// <summary>
        /// Gets the <see cref="ISourceBlock{IStreamEvent}"/> that emits data back into the loop for the next iteration.
        /// </summary>
        public ISourceBlock<IStreamEvent> LoopSource => _loopSource;

        /// <summary>
        /// Initializes a new instance of <see cref="FixedPointVertex{T}"/> with the specified execution options.
        /// </summary>
        /// <param name="executionDataflowBlockOptions">
        /// Configuration options governing block capacities and parallel properties.
        /// The bounded capacity is scaled up internally to accommodate both ingress and feedback buffering.
        /// </param>
        public FixedPointVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            executionDataflowBlockOptions.BoundedCapacity = executionDataflowBlockOptions.BoundedCapacity * 10;
            var clone = executionDataflowBlockOptions.DefaultOrClone();
            clone.EnsureOrdered = true;
            clone.MaxDegreeOfParallelism = 1;
            _executionDataflowBlockOptions = clone;
            _ingressTarget = new MultipleInputTargetHolder(0, clone);
            _feedbackTarget = new MultipleInputTargetHolder(1, clone);
            _egressSource = new FixedPointSource(clone);
            _loopSource = new FixedPointSource(clone);
        }

        /// <summary>
        /// Returns whether there is any persistent read source present within the fixed-point loop.
        /// </summary>
        /// <remarks>
        /// This value directly affects checkpoint coordination. When <see langword="true"/>, the vertex does not wait
        /// for an upstream loop acknowledgement before propagating the checkpoint, because no external read source
        /// can produce additional messages mid-checkpoint. When <see langword="false"/>, the vertex waits until
        /// the loop has confirmed it is idle before forwarding the checkpoint to the egress output.
        /// </remarks>
        /// <returns>
        /// <see langword="true"/> if no read source is present in the loop; otherwise <see langword="false"/>.
        /// </returns>
        public abstract bool NoReadSourceInLoop();

        /// <summary>
        /// Gets a <see cref="Task"/> that represents the asynchronous completion of the vertex's internal processing block.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="CreateBlock"/> has been called.
        /// </exception>
        public Task Completion => _transformBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        /// <summary>
        /// Gets the unique configured operator name of the vertex.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before <see cref="Setup"/> or <see cref="Initialize"/> has been called.
        /// </exception>
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the display name for the specific vertex type, used mainly in logging and metrics visualization.
        /// </summary>
        public abstract string DisplayName { get; }

        /// <summary>
        /// Requests the operator to compact its persistent storage.
        /// </summary>
        /// <returns>A task representing the compaction process.</returns>
        public virtual Task Compact()
        {
            return Task.CompletedTask;
        }

        private IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> IngressInCheckpoint(ILockingEvent ev)
        {
            // Set fields for locking events, the counter checks how many other messages have been
            // recieved from the loop until the prepare message is recieved.
            _waitingLockingEvent = ev;
            _messageCountSinceLockingEventPrepare = 0;
            bool isInitEvent = ev is InitWatermarksEvent;
            // Return a CheckpointPrepare message to the loop
            return new SingleAsyncEnumerable<KeyValuePair<int, IStreamEvent>>(new KeyValuePair<int, IStreamEvent>(1, new LockingEventPrepare(ev, isInitEvent)));
        }

        private IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> FeedbackInCheckpoint(ILockingEvent ev)
        {
            // Release both input targets from checkpoint so they can start to recieve data again
            _ingressTarget.ReleaseCheckpoint();
            _feedbackTarget.ReleaseCheckpoint();

            _sentLockingEvent = false;
            if (ev is ICheckpointEvent checkpoint)
            {
                _currentTime = checkpoint.NewTime;
            }

            List<KeyValuePair<int, IStreamEvent>> output = new List<KeyValuePair<int, IStreamEvent>>();
            if (_latestWatermark != null)
            {
                // Emit latest watermark if it exists
                output.Add(new KeyValuePair<int, IStreamEvent>(0, _latestWatermark));
                _latestWatermark = null;
            }
            else
            {
                if (_receivedInitialLoadDoneEvent)
                {
                    // If no watermark and we are in a checkpoint and we recieved an initial load done event, send it forward
                    output.Add(new KeyValuePair<int, IStreamEvent>(0, new InitialDataDoneEvent()));
                    _receivedInitialLoadDoneEvent = false;
                }
            }

            // Send out the checkpoint event out from the fixed point
            output.Add(new KeyValuePair<int, IStreamEvent>(0, ev));
            return output.ToAsyncEnumerable();
        }

        private IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> OnLockingPrepareEvent(LockingEventPrepare lockingEventPrepare)
        {
            _targetPrepareCount++;

            // Wait until all messages have been recieved from the loop
            if (_targetPrepareCount < _loopSource.LinksCount)
            {
                return EmptyAsyncEnumerable<KeyValuePair<int, IStreamEvent>>.Instance;
            }

            _targetPrepareCount = 0;
            // Check that no other messages have been recieved, and that there is no vertex that does not have a depedent input that is not yet in checkpoint.
            if (_messageCountSinceLockingEventPrepare == 0 && (!lockingEventPrepare.OtherInputsNotInCheckpoint || singleReadSource))
            {
                // Send out the locking event
                if (_waitingLockingEvent == null)
                {
                    if (lockingEventPrepare.IsInitEvent)
                    {
                        return EmptyAsyncEnumerable<KeyValuePair<int, IStreamEvent>>.Instance;
                    }
                    
                    throw new InvalidOperationException("Prepare locking event without a waiting checkpoint.");
                }
                if (!_sentLockingEvent)
                {
                    _sentLockingEvent = true;
                    var msgOut = _waitingLockingEvent;
                    _waitingLockingEvent = null;
                    return new SingleAsyncEnumerable<KeyValuePair<int, IStreamEvent>>(new KeyValuePair<int, IStreamEvent>(1, msgOut));
                }
                return EmptyAsyncEnumerable<KeyValuePair<int, IStreamEvent>>.Instance;
            }
            else
            {
                _messageCountSinceLockingEventPrepare = 0;
                return new SingleAsyncEnumerable<KeyValuePair<int, IStreamEvent>>(new KeyValuePair<int, IStreamEvent>(1, new LockingEventPrepare(lockingEventPrepare.LockingEvent, lockingEventPrepare.IsInitEvent)));
            }
        }

        /// <summary>
        /// Instantiates the internal TPL Dataflow blocks and wires up internal processing logic for the fixed-point loop.
        /// </summary>
        public void CreateBlock()
        {
            singleReadSource = false;
            _receivedInitialLoadDoneEvent = false;

            _transformBlock = new TransformManyBlock<KeyValuePair<int, IStreamEvent>, KeyValuePair<int, IStreamEvent>>((r) =>
            {
                if (r.Value is ILockingEvent ev)
                {
                    if (r.Key == 0)
                    {
                        return IngressInCheckpoint(ev);
                    }
                    else if (r.Key == 1)
                    {
                        return FeedbackInCheckpoint(ev);
                    }
                    else
                    {
                        throw new InvalidOperationException("Invalid targetId");
                    }
                }
                if (r.Value is LockingEventPrepare lockingEventPrepare)
                {
                    if (r.Key == 0)
                    {
                        // TODO: What should happen here must be decided
                        // This would mean its a loop inside of a loop
                        // One possible solution is that this sends out a locking event prepare to its own loop
                        // When it recieves confirmation it sends out the locking event prepare from its egress
                        // This would allow multiple loops to be nested inside each other and still do checkpointing
                        // correctly
                        throw new NotImplementedException("Loop inside of loop is not yet supported.");
                    }
                    else if (r.Key == 1)
                    {
                        return OnLockingPrepareEvent(lockingEventPrepare);
                    }
                    else
                    {
                        throw new InvalidOperationException("Invalid targetId");
                    }
                }
                if (r.Value is TriggerEvent triggerEvent)
                {
                    throw new NotSupportedException("Triggers are not supported in fixed point vertices");
                }
                if (r.Value is StreamMessage<T> streamMessage)
                {
                    // Recieve from ingress
                    if (r.Key == 0)
                    {
                        var enumerator = OnIngressRecieve(streamMessage.Data, streamMessage.Time);
                        if (_pauseSource != null)
                        {
                            enumerator = WaitForPause(enumerator);
                        }
                        if (streamMessage.Data is IRentable rentable)
                        {
                            return new AsyncEnumerableReturnRentable<KeyValuePair<int, StreamMessage<T>>, KeyValuePair<int, IStreamEvent>>(rentable, enumerator, (source) =>
                            {
                                if (source.Value.Data is IRentable rentable)
                                {
                                    if (source.Key == 0)
                                    {
                                        rentable.Rent(_egressSource.LinksCount);
                                    }
                                    else
                                    {
                                        rentable.Rent(_loopSource.LinksCount);
                                    }
                                }
                                return new KeyValuePair<int, IStreamEvent>(source.Key, source.Value);
                            });
                        }
                        else
                        {
                            return new AsyncEnumerableDowncast<KeyValuePair<int, StreamMessage<T>>, KeyValuePair<int, IStreamEvent>>(enumerator, (source) =>
                            {
                                if (source.Value.Data is IRentable rentable)
                                {
                                    if (source.Key == 0)
                                    {
                                        rentable.Rent(_egressSource.LinksCount);
                                    }
                                    else
                                    {
                                        rentable.Rent(_loopSource.LinksCount);
                                    }
                                }
                                return new KeyValuePair<int, IStreamEvent>(source.Key, source.Value);
                            });
                        }
                    }
                    // Recieve from feedback
                    if (r.Key == 1)
                    {
                        _messageCountSinceLockingEventPrepare++;
                        var enumerator = OnFeedbackRecieve(streamMessage.Data, streamMessage.Time);
                        if (_pauseSource != null)
                        {
                            enumerator = WaitForPause(enumerator);
                        }
                        if (streamMessage.Data is IRentable rentable)
                        {
                            return new AsyncEnumerableReturnRentable<KeyValuePair<int, StreamMessage<T>>, KeyValuePair<int, IStreamEvent>>(rentable, enumerator, (source) =>
                            {
                                if (source.Value.Data is IRentable rentable)
                                {
                                    if (source.Key == 0)
                                    {
                                        rentable.Rent(_egressSource.LinksCount);
                                    }
                                    else
                                    {
                                        rentable.Rent(_loopSource.LinksCount);
                                    }
                                }
                                return new KeyValuePair<int, IStreamEvent>(source.Key, source.Value);
                            });
                        }
                        else
                        {
                            return new AsyncEnumerableDowncast<KeyValuePair<int, StreamMessage<T>>, KeyValuePair<int, IStreamEvent>>(enumerator, (source) =>
                            {
                                if (source.Value.Data is IRentable rentable)
                                {
                                    if (source.Key == 0)
                                    {
                                        rentable.Rent(_egressSource.LinksCount);
                                    }
                                    else
                                    {
                                        rentable.Rent(_loopSource.LinksCount);
                                    }
                                }
                                return new KeyValuePair<int, IStreamEvent>(source.Key, source.Value);
                            });
                        }
                    }
                }
                if (r.Value is Watermark watermark)
                {
                    return HandleWatermark(r.Key, watermark);
                }
                if (r.Value is InitialDataDoneEvent initialDataDoneEvent)
                {
                    _receivedInitialLoadDoneEvent = true;
                    return EmptyAsyncEnumerable<KeyValuePair<int, IStreamEvent>>.Instance;
                }
                throw new NotSupportedException();
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1
            });

            // Link targets
            _ingressTarget.Initialize();
            _feedbackTarget.Initialize();
            _ingressTarget.LinkTo(_transformBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            _feedbackTarget.LinkTo(_transformBlock, new DataflowLinkOptions() { PropagateCompletion = true });


            // Create egress and loop source blocks
            _egressSource.Initialize();
            _loopSource.Initialize();

            // Link the egress and loop source blocks to the transform block
            _transformBlock.LinkTo(_egressSource.Target, x => x.Key == 0);
            _transformBlock.LinkTo(_loopSource.Target, x => x.Key == 1);
        }

        private IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> HandleWatermark(int targetId, Watermark watermark)
        {
            _latestWatermark = watermark;
            return EmptyAsyncEnumerable<KeyValuePair<int, IStreamEvent>>.Instance;
        }

        /// <summary>
        /// Called when a new message arrives on the ingress input. Determines how the data is processed and
        /// routed to either the egress or loop output.
        /// </summary>
        /// <param name="data">The message payload received from the ingress input.</param>
        /// <param name="time">The logical stream time at which the message was produced.</param>
        /// <returns>
        /// An asynchronous stream of key-value pairs where the key is <c>0</c> to route to <see cref="EgressSource"/>
        /// or <c>1</c> to route back into <see cref="LoopSource"/>.
        /// </returns>
        protected abstract IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> OnIngressRecieve(T data, long time);

        /// <summary>
        /// Called when a message arrives on the feedback input from the loop. Determines how the recycled data
        /// is processed and routed to either the egress or loop output for the next iteration.
        /// </summary>
        /// <param name="data">The message payload received from the feedback (loop) input.</param>
        /// <param name="time">The logical stream time at which the message was produced.</param>
        /// <returns>
        /// An asynchronous stream of key-value pairs where the key is <c>0</c> to route to <see cref="EgressSource"/>
        /// or <c>1</c> to route back into <see cref="LoopSource"/> for another iteration.
        /// </returns>
        protected abstract IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> OnFeedbackRecieve(T data, long time);

        /// <summary>
        /// Handles custom logic executed when the vertex processes a queued trigger event.
        /// </summary>
        /// <param name="name">The name identifier distinguishing the associated trigger.</param>
        /// <param name="state">Optional state associated with the trigger.</param>
        /// <returns>An asynchronous stream of output key-value pairs generated by the trigger.</returns>
        public virtual IAsyncEnumerable<KeyValuePair<int, T>> OnTrigger(string name, object? state)
        {
            return EmptyAsyncEnumerable<KeyValuePair<int, T>>.Instance;
        }

        private async IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> WaitForPause(IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> input)
        {
            var task = _pauseSource?.Task;
            if (task != null)
            {
                await task;
            }

            await foreach (var element in input)
            {
                yield return element;
            }
        }

        /// <summary>
        /// Deletes any persistent state associated with this vertex.
        /// </summary>
        /// <returns>A task representing the delete operation.</returns>
        public virtual Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Releases any resources held by this vertex asynchronously.
        /// </summary>
        public virtual ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Puts the underlying dataflow block immediately into a faulted state due to a severe exception.
        /// </summary>
        /// <param name="exception">The exception that caused the fault.</param>
        public void Fault(Exception exception)
        {
            Debug.Assert(_transformBlock != null);
            (_transformBlock as IDataflowBlock).Fault(exception);
        }

        /// <summary>
        /// Returns all downstream <see cref="ITargetBlock{IStreamEvent}"/> linked to the egress and loop sources.
        /// </summary>
        /// <returns>
        /// An enumerable of all target blocks connected to both <see cref="EgressSource"/> and <see cref="LoopSource"/>.
        /// </returns>
        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _loopSource.Links.Union(_egressSource.Links);
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
            _name = name;
            _currentTime = newTime;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _metrics = vertexHandler.Metrics;
            _streamVersion = streamVersionInformation;

            Metrics.CreateObservableGauge("busy", () =>
            {
                Debug.Assert(_transformBlock != null, nameof(_transformBlock));
                return ((float)_transformBlock.InputCount) / _executionDataflowBlockOptions.BoundedCapacity;
            });
            Metrics.CreateObservableGauge("backpressure", () =>
            {
                Debug.Assert(_transformBlock != null, nameof(_transformBlock));
                return ((float)_transformBlock.OutputCount) / _executionDataflowBlockOptions.BoundedCapacity;
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

            return InitializeOrRestore(vertexHandler.StateClient);
        }

        /// <summary>
        /// Restores or initializes the operator's persistent state from the provided state manager client.
        /// </summary>
        /// <param name="stateManagerClient">The client used to access persistent storage for state restoration.</param>
        /// <returns>A task representing the asynchronous state restore or initialization operation.</returns>
        protected abstract Task InitializeOrRestore(IStateManagerClient stateManagerClient);

        /// <summary>
        /// Creates the actual links between the egress and loop sources and their configured downstream targets.
        /// </summary>
        public void Link()
        {
            _egressSource.Link();
            _loopSource.Link();
        }

        /// <summary>
        /// Queuing triggers is not supported for fixed-point vertices.
        /// </summary>
        /// <param name="triggerEvent">The trigger event to queue.</param>
        /// <exception cref="NotSupportedException">Always thrown; triggers are not supported in fixed-point vertices.</exception>
        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            throw new NotSupportedException("Triggers are not supported in fixed point vertices");
        }

        /// <summary>
        /// Performs initial naming configuration for the vertex and its ingress, egress, loop, and feedback sub-components.
        /// </summary>
        /// <param name="streamName">The overarching name of the stream this vertex belongs to.</param>
        /// <param name="operatorName">The unique name for this operator within the stream.</param>
        public void Setup(string streamName, string operatorName)
        {
            _streamName = streamName;
            _name = operatorName;
            _ingressTarget.Setup(operatorName);
            _egressSource.Setup(streamName, operatorName);
            _loopSource.Setup(streamName, operatorName);
            _feedbackTarget.Setup(operatorName);
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
        /// Signals to the vertex that both ingress and feedback inputs should not accept any more messages.
        /// </summary>
        public void Complete()
        {
            _feedbackTarget.Complete();
            _ingressTarget.Complete();
        }

        /// <summary>
        /// Called before the checkpoint state is persisted. Allows the vertex to flush any pending state.
        /// </summary>
        /// <returns>A task representing the pre-checkpoint operation.</returns>
        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called when the stream encounters a failure, allowing the vertex to roll back to a safe state.
        /// </summary>
        /// <param name="rollbackVersion">The version to roll back to.</param>
        /// <returns>A task representing the rollback operation.</returns>
        public virtual Task OnFailure(long rollbackVersion)
        {
            return Task.CompletedTask;
        }
    }
}
