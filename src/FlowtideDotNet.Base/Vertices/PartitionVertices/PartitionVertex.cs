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
    /// Vertex responsible for partitioning an input stream into multiple target streams.
    /// </summary>
    /// <typeparam name="T">The type of the underlying data payload processed by this vertex.</typeparam>
    /// <remarks>
    /// This vertex acts as a router. It accepts a single stream of events and distributes them to multiple linked 
    /// target blocks based on predefined logic. It ensures that control events (watermarks, checkpoints) are multiplexed 
    /// accurately to all downstream targets keeping the pipeline synchronized.
    /// </remarks>
    public abstract class PartitionVertex<T> : ITargetBlock<IStreamEvent>, IStreamVertex
    {
        private TransformManyBlock<IStreamEvent, KeyValuePair<int, IStreamEvent>>? _inputBlock;
        private FixedPointSource[] _sources;
        private ITargetBlock<IStreamEvent>? _inputTargetBlock;
        private readonly int targetNumber;
        private readonly ExecutionDataflowBlockOptions _executionDataflowBlockOptions;
        private string? _name;
        private string? _streamName;
        private long _currentTime;
        private ILogger? _logger;
        private IMeter? _metrics;
        private bool _isHealthy = true;
        private IVertexHandler? _vertexHandler;
        private IMemoryAllocator? _memoryAllocator;
        private TaskCompletionSource? _pauseSource;
        private StreamVersionInformation? _streamVersion;

        /// <summary>
        /// Gets the logger instance associated with this vertex.
        /// </summary>
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        /// <summary>
        /// Gets the overall version information of the running stream, if applicable.
        /// </summary>
        public StreamVersionInformation? StreamVersion => _streamVersion;

        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        /// <summary>
        /// Gets the array of source blocks representing the output partitions maintained by this vertex.
        /// </summary>
        public ISourceBlock<IStreamEvent>[] Sources => _sources;

        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionVertex{T}"/> class.
        /// </summary>
        /// <param name="targetNumber">The number of outgoing target partitions.</param>
        /// <param name="executionDataflowBlockOptions">The execution options covering bounded capacity and parallelism.</param>
        public PartitionVertex(int targetNumber, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            this.targetNumber = targetNumber;
            this._executionDataflowBlockOptions = executionDataflowBlockOptions;
            _sources = new FixedPointSource[targetNumber];
            for (int i = 0; i < targetNumber; i++)
            {
                _sources[i] = new FixedPointSource(executionDataflowBlockOptions);
            }
        }

        /// <summary>
        /// Gets a task that represents the completion of the vertex processing block.
        /// </summary>
        public Task Completion => _inputBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        /// <summary>
        /// Gets the configured explicit identifier name of the vertex.
        /// </summary>
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the display name for the specific partitioning logic type, largely used for logging and metrics visualization.
        /// </summary>
        public abstract string DisplayName { get; }

        /// <summary>
        /// Signals the vertex to compress or compact any underlying persistent state.
        /// </summary>
        public Task Compact()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Signals to the initial input block that it should not receive any more inputs natively triggering sequence completion.
        /// </summary>
        public void Complete()
        {
            Debug.Assert(_inputBlock != null);
            _inputBlock.Complete();
        }

        protected abstract IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> PartitionData(T data, long time);

        /// <summary>
        /// Instantiates underlying processing pipeline dataflow blocks to route and branch messages to target components.
        /// </summary>
        public void CreateBlock()
        {
            _inputBlock = new TransformManyBlock<IStreamEvent, KeyValuePair<int, IStreamEvent>>(x =>
            {
                if (x is ILockingEvent ev)
                {
                    return HandleLockingEvent(ev);
                }
                if (x is LockingEventPrepare lockingEventPrepare)
                {
                    return HandleLockingEventPrepare(lockingEventPrepare);
                }
                if (x is StreamMessage<T> message)
                {
                    var enumerator = PartitionData(message.Data, message.Time);

                    if (_pauseSource != null)
                    {
                        enumerator = WaitForPause(enumerator);
                    }

                    if (message.Data is IRentable rentable)
                    {
                        return new AsyncEnumerableReturnRentable<KeyValuePair<int, StreamMessage<T>>, KeyValuePair<int, IStreamEvent>>(rentable, enumerator, (source) =>
                        {
                            if (source.Value.Data is IRentable rentable)
                            {

                                rentable.Rent(_sources[source.Key].LinksCount);
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
                                rentable.Rent(_sources[source.Key].LinksCount);
                            }
                            return new KeyValuePair<int, IStreamEvent>(source.Key, source.Value);
                        });
                    }

                }
                if (x is TriggerEvent triggerEvent)
                {
                    throw new NotSupportedException("Triggers are not supported in partition vertices");
                }
                if (x is Watermark watermark)
                {
                    return HandleWatermark(watermark);
                }
                if (x is InitialDataDoneEvent initialDataDoneEvent)
                {
                    return Broadcast(initialDataDoneEvent);
                }
                throw new NotSupportedException();
            }, _executionDataflowBlockOptions);
            _inputTargetBlock = _inputBlock;

            for (int i = 0; i < _sources.Length; i++)
            {
                var index = i;
                _sources[i].Initialize();
                _inputBlock.LinkTo(_sources[i].Target, new DataflowLinkOptions { PropagateCompletion = true }, x => x.Key == index);
            }

        }

        protected virtual Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            return Task.CompletedTask;
        }

        private async IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> HandleLockingEvent(ILockingEvent streamEvent)
        {
            await OnLockingEvent(streamEvent);

            for (int i = 0; i < targetNumber; i++)
            {
                yield return new KeyValuePair<int, IStreamEvent>(i, streamEvent);
            }
        }

        internal virtual Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            return Task.CompletedTask;
        }

        private async IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> HandleLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            await OnLockingEventPrepare(lockingEventPrepare);
            for (int i = 0; i < targetNumber; i++)
            {
                yield return new KeyValuePair<int, IStreamEvent>(i, lockingEventPrepare);
            }
        }

        protected virtual Task OnWatermark(Watermark watermark)
        {
            return Task.CompletedTask;
        }

        private IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> Broadcast(IStreamEvent e)
        {
            List<KeyValuePair<int, IStreamEvent>> output = new List<KeyValuePair<int, IStreamEvent>>();
            for (int i = 0; i < targetNumber; i++)
            {
                output.Add(new KeyValuePair<int, IStreamEvent>(i, e));
            }
            return output.ToAsyncEnumerable();
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

        private async IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> HandleWatermark(Watermark watermark)
        {
            await OnWatermark(watermark);
            for (int i = 0; i < targetNumber; i++)
            {
                yield return new KeyValuePair<int, IStreamEvent>(i, watermark);
            }
        }

        /// <summary>
        /// Explicitly deletes any persistent resources corresponding to this partitioning operator's internal tracking configuration.
        /// </summary>
        public Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Releases all underlying data structures inherently attached.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Attempts to dispatch a generic dataflow message to this operator. Exposes the TPL pipeline interface.
        /// </summary>
        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(_inputTargetBlock != null);
            return _inputTargetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        /// <summary>
        /// Manually trips the processing block into a permanently faulted state based on a registered exception.
        /// </summary>
        public void Fault(Exception exception)
        {
            Debug.Assert(_inputTargetBlock != null);
            _inputTargetBlock.Fault(exception);
        }

        /// <summary>
        /// Connects operator identity properties dynamically ahead of structural linking sequences.
        /// </summary>
        public void Setup(string streamName, string operatorName)
        {
            _streamName = streamName;
            _name = operatorName;
        }

        /// <summary>
        /// Initializes logical resources metrics and states for routing context prior to start execution.
        /// </summary>
        public Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation)
        {
            _memoryAllocator = vertexHandler.MemoryManager;
            _name = name;
            _currentTime = newTime;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _metrics = vertexHandler.Metrics;
            _vertexHandler = vertexHandler;
            _streamVersion = streamVersionInformation;

            Metrics.CreateObservableGauge("busy", () =>
            {
                Debug.Assert(_inputBlock != null, nameof(_inputBlock));
                return ((float)_inputBlock.InputCount) / _executionDataflowBlockOptions.BoundedCapacity;
            });
            Metrics.CreateObservableGauge("backpressure", () =>
            {
                Debug.Assert(_inputBlock != null, nameof(_inputBlock));
                return ((float)_inputBlock.OutputCount) / _executionDataflowBlockOptions.BoundedCapacity;
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

        protected abstract Task InitializeOrRestore(IStateManagerClient stateManagerClient);

        /// <summary>
        /// Links internal mapping routines dynamically to downstream destinations configured on sources representation.
        /// </summary>
        public void Link()
        {
            for (int i = 0; i < _sources.Length; i++)
            {
                _sources[i].Link();
            }
        }

        protected Task RegisterTrigger(string name, TimeSpan? scheduleInterval = null)
        {
            if (_vertexHandler == null)
            {
                throw new NotSupportedException("Cannot register trigger before initialize is called");
            }
            return _vertexHandler.RegisterTrigger(name, scheduleInterval);
        }

        /// <summary>
        /// Enqueues an execution trigger.
        /// Triggers are not supported in this vertex implementation as it is designed to operate purely as a data router without any internal stateful
        /// processing logic that would require time-based or event-based triggers.
        /// </summary>
        public virtual Task QueueTrigger(TriggerEvent triggerEvent)
        {
            throw new NotSupportedException("Triggers are not supported in partition vertices");
        }

        protected void ScheduleCheckpoint(TimeSpan inTime)
        {
            if (_vertexHandler == null)
            {
                throw new NotSupportedException("Cannot schedule checkpoint before initialize");
            }
            _vertexHandler.ScheduleCheckpoint(inTime);
        }

        /// <summary>
        /// Emits a collection composed uniquely to list downstream pipeline targets attached resolving flow branches.
        /// </summary>
        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            List<ITargetBlock<IStreamEvent>> output = new List<ITargetBlock<IStreamEvent>>();
            foreach (var source in _sources)
            {
                output.AddRange(source.GetLinks());
            }
            return output;
        }

        /// <summary>
        /// Halts current processing. Used during debug or suspension states.
        /// </summary>
        public void Pause()
        {
            if (_pauseSource == null)
            {
                _pauseSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        /// <summary>
        /// Resumes processing of events queued while suspended.
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
        /// A notification hook invoked prior to finalizing changes in persistent stores for a checkpoint.
        /// </summary>
        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }

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
        /// Indicates a rollback behavior hook whenever a previous version is targeted for restoring due to errors.
        /// </summary>
        /// <param name="rollbackVersion">The version that the stream will be rolled back to.</param>
        public virtual Task OnFailure(long rollbackVersion)
        {
            return Task.CompletedTask;
        }
    }
}
