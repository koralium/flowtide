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
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Base.dataflow;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Base.Metrics;
using System.Text;
using FlowtideDotNet.Base.Vertices.MultipleInput;

namespace FlowtideDotNet.Base.Vertices.Unary
{
    public abstract class UnaryVertex<T, TState> : IPropagatorBlock<IStreamEvent, IStreamEvent>, IStreamVertex
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

        private string? _name;
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        private string? _streamName;
        protected string StreamName => _streamName ?? throw new InvalidOperationException("StreamName can only be fetched after initialize or setup method calls");

        public abstract string DisplayName { get; }

        protected IMeter Metrics => _vertexHandler?.Metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        private ILogger? _logger;
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        protected UnaryVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
        }

        [MemberNotNull(nameof(_transformBlock), nameof(_targetBlock), nameof(_sourceBlock))]
        private void InitializeBlocks()
        {
            _transformBlock = new TransformManyBlock<IStreamEvent, IStreamEvent>((streamEvent) =>
            {
                // Check if it is a checkpoint event
                if (streamEvent is ILockingEvent ev)
                {
                    Logger.LogTrace("Locking event " + Name);
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
                    return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) => new StreamMessage<T>(source, _currentTime));
                }
                if (streamEvent is StreamMessage<T> streamMessage)
                {
                    var enumerator = OnRecieve(streamMessage.Data, streamMessage.Time);
                    return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) => new StreamMessage<T>(source, streamMessage.Time));
                }
                if (streamEvent is Watermark watermark)
                {
                    return HandleWatermark(watermark);
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
                yield return new StreamMessage<T>(e, _currentTime);
            }
            yield return watermark;
        }

        protected virtual async IAsyncEnumerable<T> OnWatermark(Watermark watermark)
        {
            yield break;
        }

        public async Task Initialize(string name, long restoreTime, long newTime, JsonElement? state, IVertexHandler vertexHandler)
        {
            _name = name;
            _streamName = vertexHandler.StreamName;
            _vertexHandler = vertexHandler;

            TState? parsedState = default;
            if (state.HasValue)
            {
                parsedState = JsonSerializer.Deserialize<TState>(state.Value);
            }
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            await InitializeOrRestore(parsedState, vertexHandler.StateClient);

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
                TagList tags = new TagList
                {
                    { "displayName", DisplayName }
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

        protected abstract Task InitializeOrRestore(TState? state, IStateManagerClient stateManagerClient);

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

        internal protected virtual async Task<ILockingEvent> HandleCheckpoint(ILockingEvent lockingEvent)
        {
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                Logger.LogInformation("Checkpoint in operator: {operator}", Name);
                _currentTime = checkpointEvent.NewTime;
                var checkpointState = await OnCheckpoint();
                checkpointEvent.AddState(Name, checkpointState);
                return checkpointEvent;
            }
            return lockingEvent;
        }

        public abstract IAsyncEnumerable<T> OnRecieve(T msg, long time);

        public abstract Task<TState> OnCheckpoint();

        public virtual IAsyncEnumerable<T> OnTrigger(string name, object? state)
        {
            return Empty();
        }

        private async IAsyncEnumerable<T> Empty()
        {
            yield break;
        }

        private async IAsyncEnumerable<IStreamEvent> Passthrough(IStreamEvent streamEvent)
        {
            yield return streamEvent;
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
            return null;
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
    }
}
