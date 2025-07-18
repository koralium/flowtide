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

using DataflowStream.dataflow.Internal.Extensions;
using FlowtideDotNet.Base.dataflow;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Unary
{
    public abstract class UnaryVertex<T> : IPropagatorBlock<IStreamEvent, IStreamEvent>, IStreamVertex
    {
        private TransformManyBlock<IStreamEvent, IStreamEvent>? _transformBlock;
        private ParallelSource<IStreamEvent>? _parallelSource;
        private ParallelUnaryTarget<IStreamEvent>? _parallelTarget;
        private ITargetBlock<IStreamEvent>? _targetBlock;
        private ISourceBlock<IStreamEvent>? _sourceBlock;
        private readonly ExecutionDataflowBlockOptions executionDataflowBlockOptions;
        private readonly List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)> _links = new List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)>();
        private long _currentTime = 0;
        private IVertexHandler? _vertexHandler;
        private bool _isHealthy = true;
        private TaskCompletionSource? _pauseSource;

        private string? _name;
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        private string? _streamName;
        protected string StreamName => _streamName ?? throw new InvalidOperationException("StreamName can only be fetched after initialize or setup method calls");

        public abstract string DisplayName { get; }

        protected IMeter Metrics => _vertexHandler?.Metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        private ILogger? _logger;
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        private StreamVersionInformation? _streamVersion;
        public StreamVersionInformation? StreamVersion => _streamVersion;

        protected IMemoryAllocator MemoryAllocator => _vertexHandler?.MemoryManager ?? throw new NotSupportedException("Initialize must be called before accessing memory allocator");

        protected UnaryVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
        }

        private bool ShouldWait()
        {
            return _transformBlock?.OutputCount >= executionDataflowBlockOptions.BoundedCapacity;
        }

        [MemberNotNull(nameof(_transformBlock), nameof(_targetBlock), nameof(_sourceBlock))]
        private void InitializeBlocks()
        {
            _transformBlock = new TransformManyBlock<IStreamEvent, IStreamEvent>((streamEvent) =>
            {
                // Check if it is a checkpoint event
                if (streamEvent is ILockingEvent ev)
                {
                    Logger.LockingEventInOperator(StreamName, Name);
                    // TODO: Check if it has a parallel source
                    if (_parallelSource == null)
                    {
                        return HandleCheckpointEnumerable(ev);
                    }
                    else
                    {
                        return Passthrough(streamEvent);
                    }
                }
                if (streamEvent is LockingEventPrepare lockingEventPrepare)
                {
                    return Passthrough(lockingEventPrepare);
                }
                if (streamEvent is TriggerEvent triggerEvent)
                {
                    var enumerator = OnTrigger(triggerEvent.Name, triggerEvent.State);
                    // Inject data into the stream from the trigger
                    return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) =>
                    {
                        if (source is IRentable rentable)
                        {
                            rentable.Rent(_links.Count);
                        }
                        return new StreamMessage<T>(source, _currentTime);
                    });
                }
                if (streamEvent is StreamMessage<T> streamMessage)
                {
                    var enumerator = OnRecieve(streamMessage.Data, streamMessage.Time);

                    if (_pauseSource != null)
                    {
                        enumerator = WaitForPause(enumerator);
                    }

                    if (streamMessage.Data is IRentable inputRentable)
                    {
                        return new AsyncEnumerableReturnRentable<T, IStreamEvent>(inputRentable, enumerator, (source) =>
                        {
                            if (source is IRentable rentable)
                            {
                                rentable.Rent(_links.Count);
                            }
                            return new StreamMessage<T>(source, streamMessage.Time);
                        });
                    }
                    else
                    {
                        return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) =>
                        {
                            if (source is IRentable rentable)
                            {
                                rentable.Rent(_links.Count);
                            }
                            return new StreamMessage<T>(source, streamMessage.Time);
                        });
                    }
                }
                if (streamEvent is Watermark watermark)
                {
                    return new AsyncEnumerableWithWait<IStreamEvent, IStreamEvent>(HandleWatermark(watermark), (s) => s, ShouldWait);
                }
                if (streamEvent is InitialDataDoneEvent initialDataDoneEvent)
                {
                    return Passthrough(initialDataDoneEvent);
                }

                throw new NotSupportedException();
            }, executionDataflowBlockOptions);

            if (executionDataflowBlockOptions.GetSupportsParallelExecution())
            {
                if (!executionDataflowBlockOptions.EnsureOrdered)
                {
                    throw new NotSupportedException("Events must be ordered to ensure that checkpointing still works");
                }
                // Create the source for parallel execution to check for checkpoint events and link it.
                _parallelSource = new ParallelSource<IStreamEvent>(HandleCheckpointParallel, CheckpointSentParallel, executionDataflowBlockOptions);
                _transformBlock.LinkTo(_parallelSource, new DataflowLinkOptions() { PropagateCompletion = true });

                _parallelTarget = new ParallelUnaryTarget<IStreamEvent>(executionDataflowBlockOptions);
                _parallelTarget.LinkTo(_transformBlock, new DataflowLinkOptions() { PropagateCompletion = true });

                _targetBlock = _parallelTarget;
                _sourceBlock = _parallelSource;
            }
            else
            {
                _targetBlock = _transformBlock;
                _sourceBlock = _transformBlock;
            }

            if (_links.Count > 1)
            {
                var broadcastBlock = new GuaranteedBroadcastBlock<IStreamEvent>(executionDataflowBlockOptions);
                var existingSource = _sourceBlock;
                _sourceBlock = broadcastBlock;
                existingSource.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            }
        }

        private async IAsyncEnumerable<IStreamEvent> HandleWatermark(Watermark watermark)
        {
            await foreach (var e in OnWatermark(watermark))
            {
                if (e is IRentable rentable)
                {
                    rentable.Rent(_links.Count);
                }
                yield return new StreamMessage<T>(e, _currentTime);
            }
            yield return watermark;
        }

        protected virtual IAsyncEnumerable<T> OnWatermark(Watermark watermark)
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        public async Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation)
        {
            _name = name;
            _streamName = vertexHandler.StreamName;
            _vertexHandler = vertexHandler;

            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _streamVersion = streamVersionInformation;

            await InitializeOrRestore(vertexHandler.StateClient);

            Metrics.CreateObservableGauge("busy", () =>
            {
                Debug.Assert(_transformBlock != null, nameof(_transformBlock));
                return ((float)_transformBlock.InputCount) / executionDataflowBlockOptions.BoundedCapacity;
            });
            Metrics.CreateObservableGauge("backpressure", () =>
            {
                Debug.Assert(_transformBlock != null, nameof(_transformBlock));
                return ((float)_transformBlock.OutputCount) / executionDataflowBlockOptions.BoundedCapacity;
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

            _currentTime = newTime;
        }

        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        protected Task RegisterTrigger(string name, TimeSpan? scheduleInterval = null)
        {
            if (_vertexHandler == null)
            {
                throw new NotSupportedException("Cannot register trigger before initialize is called");
            }
            return _vertexHandler.RegisterTrigger(name, scheduleInterval);
        }

        public abstract Task Compact();

        protected abstract Task InitializeOrRestore(IStateManagerClient stateManagerClient);

        private async IAsyncEnumerable<IStreamEvent> HandleCheckpointEnumerable(ILockingEvent checkpointEvent)
        {
            var transformedCheckpoint = await HandleCheckpoint(checkpointEvent);
            yield return transformedCheckpoint;
        }

        private async Task<ILockingEvent> HandleCheckpointParallel(ILockingEvent checkpointEvent)
        {
            return await HandleCheckpoint(checkpointEvent);
        }

        private void CheckpointSentParallel()
        {
            _parallelTarget!.ReleaseCheckpoint();
        }

        private async IAsyncEnumerable<T> WaitForPause(IAsyncEnumerable<T> input)
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

        internal protected virtual async Task<ILockingEvent> HandleCheckpoint(ILockingEvent lockingEvent)
        {
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                Logger.CheckpointInOperator(StreamName, Name);
                _currentTime = checkpointEvent.NewTime;
                await OnCheckpoint();
                return checkpointEvent;
            }
            return lockingEvent;
        }

        public abstract IAsyncEnumerable<T> OnRecieve(T msg, long time);

        public abstract Task OnCheckpoint();

        public virtual IAsyncEnumerable<T> OnTrigger(string name, object? state)
        {
            return Empty();
        }

        private IAsyncEnumerable<T> Empty()
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        private IAsyncEnumerable<IStreamEvent> Passthrough(IStreamEvent streamEvent)
        {
            return new SingleAsyncEnumerable<IStreamEvent>(streamEvent);
        }

        public Task Completion => _transformBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after CreateBlocks.");

        public void Complete()
        {
            Debug.Assert(_transformBlock != null, nameof(_transformBlock));
            _transformBlock.Complete();
        }

        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
        {
            if (_parallelSource != null)
            {
                return (_parallelSource as ISourceBlock<IStreamEvent>).ConsumeMessage(messageHeader, target, out messageConsumed);
            }
            Debug.Assert(_transformBlock != null, nameof(_transformBlock));
            return (_transformBlock as ISourceBlock<IStreamEvent>).ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_transformBlock != null, nameof(_transformBlock));
            (_transformBlock as IDataflowBlock).Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            _links.Add((target, linkOptions));
            return default!;
        }

        private IDisposable LinkTo_Internal(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return _sourceBlock.LinkTo(target, linkOptions);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(_targetBlock != null, nameof(_targetBlock));
            return _targetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            _sourceBlock.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return _sourceBlock.ReserveMessage(messageHeader, target);
        }

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            Debug.Assert(_targetBlock != null, nameof(_targetBlock));
            return _targetBlock.SendAsync(triggerEvent);
        }

        public virtual ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

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

        public void CreateBlock()
        {
            InitializeBlocks();
        }

        public abstract Task DeleteAsync();

        public void Setup(string streamName, string operatorName)
        {
            _name = operatorName;
            _streamName = streamName;
        }

        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _links.Select(x => x.Item1);
        }

        public void Pause()
        {
            if (_pauseSource == null)
            {
                _pauseSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public void Resume()
        {
            if (_pauseSource != null)
            {
                _pauseSource.SetResult();
                _pauseSource = null;
            }
        }

        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }
    }
}
