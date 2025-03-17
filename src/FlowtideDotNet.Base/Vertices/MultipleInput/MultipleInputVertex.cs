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

using DataflowStream.dataflow.Internal;
using DataflowStream.dataflow.Internal.Extensions;
using FlowtideDotNet.Base.dataflow;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.MultipleInput
{
    public abstract class MultipleInputVertex<T> : ISourceBlock<IStreamEvent>, IStreamVertex
    {
        private readonly MultipleInputTargetHolder[] _targetHolders;
        private TransformManyBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>? _transformBlock;
        private ISourceBlock<IStreamEvent>? _sourceBlock;

        private ILockingEvent?[]? _targetInCheckpoint;
        private ILockingEvent[]? _lastSeenCheckpointEvents;
        private readonly object _targetCheckpointLock;

        private IReadOnlySet<string>[]? _targetWatermarkNames;
        private Watermark[]? _targetWatermarks;
        private Watermark? _currentWatermark;
        private Watermark? _previousWatermark;
        private bool[]? _targetSentDataSinceLastWatermark;
        private bool[]? _targetSentWatermark;
        private bool _isInIteration = false;

        private ParallelSource<IStreamEvent>? _parallelSource;
        private long _currentTime;
        private readonly int targetCount;
        private readonly ExecutionDataflowBlockOptions executionDataflowBlockOptions;
        private readonly List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)> _links = new List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)>();
        private bool _isHealthy = true;
        private CancellationTokenSource? tokenSource;
        private IMemoryAllocator? _memoryAllocator;
        private TaskCompletionSource? _pauseSource;

        private string? _name;
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        private string? _streamName;
        protected string StreamName => _streamName ?? throw new InvalidOperationException("StreamName can only be fetched after initialize or setup method calls");

        public abstract string DisplayName { get; }

        private IMeter? _metrics;
        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        private ILogger? _logger;
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

        protected float Backpressure => ((float)(_transformBlock?.OutputCount ?? throw new InvalidOperationException("OutputCount can only be fetched after initialization."))) / executionDataflowBlockOptions.BoundedCapacity;

        protected MultipleInputVertex(int targetCount, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            _targetCheckpointLock = new object();
            var clone = executionDataflowBlockOptions.DefaultOrClone();
            clone.EnsureOrdered = true;
            this.targetCount = targetCount;
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
            _targetHolders = new MultipleInputTargetHolder[targetCount];

            for (int i = 0; i < targetCount; i++)
            {
                _targetHolders[i] = new MultipleInputTargetHolder(i, executionDataflowBlockOptions);
            }
        }

        private void InitializeBlock()
        {
            tokenSource = new CancellationTokenSource();
            executionDataflowBlockOptions.CancellationToken = tokenSource.Token;
            _transformBlock = new TransformManyBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>((r) =>
            {
                if (r.Value is ILockingEvent ev)
                {
                    if (TargetInCheckpoint(r.Key, ev, out var checkpoints))
                    {
                        _lastSeenCheckpointEvents = checkpoints;
                        if (_parallelSource == null)
                        {
                            return HandleCheckpointEnumerable(ev);
                        }
                        return Passthrough(r.Value);
                    }
                    return Empty();
                }
                if (r.Value is LockingEventPrepare lockingEventPrepare)
                {
                    return HandleLockingEventPrepare(r.Key, lockingEventPrepare);
                }
                if (r.Value is TriggerEvent triggerEvent)
                {
                    var enumerator = OnTrigger(triggerEvent.Name, triggerEvent.State);
                    if (_pauseSource != null)
                    {
                        enumerator = WaitForPause(enumerator);
                    }
                    return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) =>
                    {
                        if (source is IRentable rentable)
                        {
                            rentable.Rent(_links.Count);
                        }
                        return new StreamMessage<T>(source, _currentTime);
                    });
                }
                if (r.Value is StreamMessage<T> streamMessage)
                {
                    Debug.Assert(_targetSentDataSinceLastWatermark != null);
                    _targetSentDataSinceLastWatermark[r.Key] = true;
                    var enumerator = OnRecieve(r.Key, streamMessage.Data, streamMessage.Time);
                    if (_pauseSource != null)
                    {
                        enumerator = WaitForPause(enumerator);
                    }

                    if (streamMessage.Data is IRentable rentable)
                    {
                        return new AsyncEnumerableReturnRentable<T, IStreamEvent>(rentable, enumerator, (source) =>
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
                if (r.Value is Watermark watermark)
                {
                    return HandleWatermark(r.Key, watermark);
                }
                throw new NotSupportedException();
            }, executionDataflowBlockOptions);

            if (executionDataflowBlockOptions.GetSupportsParallelExecution())
            {
                _parallelSource = new ParallelSource<IStreamEvent>(HandleCheckpoint, CheckpointSent, executionDataflowBlockOptions);

                _transformBlock.LinkTo(_parallelSource, new DataflowLinkOptions()
                {
                    PropagateCompletion = true
                });
                _sourceBlock = _parallelSource;
            }
            else
            {
                _sourceBlock = _transformBlock;
            }

            _targetInCheckpoint = new ILockingEvent[targetCount];
            _targetWatermarks = new Watermark[targetCount];
            _targetSentDataSinceLastWatermark = new bool[targetCount];
            _targetSentWatermark = new bool[targetCount];

            foreach (var t in _targetHolders)
            {
                t.Initialize();
                t.LinkTo(_transformBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            }

            if (_links.Count > 1)
            {
                var broadcastBlock = new GuaranteedBroadcastBlock<IStreamEvent>(new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = executionDataflowBlockOptions.BoundedCapacity
                });
                _sourceBlock = broadcastBlock;
                if (_parallelSource != null)
                {
                    _parallelSource.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
                }
                else
                {
                    _transformBlock.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
                }
            }

            // Completed propogation
            Task.WhenAll(_targetHolders.Select(x => x.Completion)).ContinueWith((completed, state) =>
            {
                var block = (TransformManyBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>)state!;
                if (completed.IsFaulted)
                {
                    (block as ISourceBlock<IStreamEvent>).Fault(completed.Exception!);
                }
                block.Complete();
            }, _transformBlock, CancellationToken.None, Common.GetContinuationOptions(), TaskScheduler.Default);

            Task.WhenAny(_targetHolders.Select(x => x.Completion)).ContinueWith((completed, state) =>
            {
                var block = (MultipleInputVertex<T>)state!;
                foreach (var target in block._targetHolders)
                {
                    if (target.Completion.IsFaulted)
                    {
                        Debug.Assert(block._transformBlock != null, nameof(block._transformBlock));
                        (block._transformBlock as IDataflowBlock).Fault(target.Completion.Exception!);
                    }
                }
            }, this, CancellationToken.None, Common.GetContinuationOptions(), TaskScheduler.Default);
        }

        private IAsyncEnumerable<IStreamEvent> HandleLockingEventPrepare(int targetId, LockingEventPrepare lockingEventPrepare)
        {
            Debug.Assert(_targetInCheckpoint != null);
            // Set that this operator is in an iteration. This helps watermarks output.
            _isInIteration = true;
            // Check that all other inputs are waiting for checkpoint.
            bool allInCheckpoint = true;
            for (int i = 0; i < _targetInCheckpoint.Length; i++)
            {
                if (i == targetId)
                {
                    continue;
                }
                if (_targetInCheckpoint[i] == null)
                {
                    allInCheckpoint = false;
                }
            }
            if (!allInCheckpoint)
            {
                lockingEventPrepare.OtherInputsNotInCheckpoint = true;
            }
            return new SingleAsyncEnumerable<IStreamEvent>(lockingEventPrepare);
        }

        private async IAsyncEnumerable<IStreamEvent> HandleWatermark(int targetId, Watermark watermark)
        {
            Debug.Assert(_targetWatermarkNames != null, nameof(_targetWatermarkNames));
            Debug.Assert(_targetWatermarks != null, nameof(_targetWatermarks));
            Debug.Assert(_targetSentWatermark != null);
            Debug.Assert(_targetSentDataSinceLastWatermark != null);

            if (_currentWatermark == null)
            {
                _currentWatermark = new Watermark(ImmutableDictionary<string, long>.Empty, watermark.StartTime)
                {
                    SourceOperatorId = watermark.SourceOperatorId
                };
                _previousWatermark = _currentWatermark;
            }
            _targetSentWatermark[targetId] = true;
            _targetWatermarks[targetId] = watermark;

            var currentDict = _currentWatermark.Watermarks;
            foreach (var kv in watermark.Watermarks)
            {
                long watermarkValue = kv.Value;
                for (int i = 0; i < _targetWatermarkNames.Length; i++)
                {
                    // Check if any other target handles the same key
                    if (_targetWatermarkNames[i].Contains(kv.Key))
                    {
                        if (_targetWatermarks[i] != null &&
                            _targetWatermarks[i].Watermarks.TryGetValue(kv.Key, out var otherTargetOffset))
                        {
                            watermarkValue = Math.Min(watermarkValue, otherTargetOffset);
                        }
                        else
                        {
                            watermarkValue = 0;
                        }
                    }
                }
                if (watermarkValue > 0)
                {
                    currentDict = currentDict.SetItem(kv.Key, watermarkValue);
                }
            }
            var newWatermark = new Watermark(currentDict, watermark.StartTime)
            {
                SourceOperatorId = watermark.SourceOperatorId
            };

            // only output watermark if there is a difference in the numbers
            if (!newWatermark.Equals(_currentWatermark))
            {
                _currentWatermark = newWatermark;
            }

            if (!_isInIteration)
            {
                for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
                {
                    if (_targetSentDataSinceLastWatermark[i] && !_targetSentWatermark[i])
                    {
                        yield break;
                    }
                }
                for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
                {
                    _targetSentDataSinceLastWatermark[i] = false;
                    _targetSentWatermark[i] = false;
                }
            }

            // only output watermark if there is a difference in the numbers
            if (!_currentWatermark.Equals(_previousWatermark))
            {
                await foreach (var e in OnWatermark(newWatermark))
                {
                    if (e is IRentable rentable)
                    {
                        rentable.Rent(_links.Count);
                    }
                    yield return new StreamMessage<T>(e, _currentTime);
                }
                _previousWatermark = _currentWatermark;
                yield return _currentWatermark;
            }
        }

        protected virtual IAsyncEnumerable<T> OnWatermark(Watermark watermark)
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        private async IAsyncEnumerable<IStreamEvent> HandleCheckpointEnumerable(ILockingEvent checkpointEvent)
        {
            var transformedEvent = await HandleCheckpoint(checkpointEvent);
            CheckpointSent();
            yield return transformedEvent;
        }

        private async Task<ILockingEvent> HandleCheckpoint(ILockingEvent lockingEvent)
        {
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                _currentTime = checkpointEvent.NewTime;
                await OnCheckpoint();
                _lastSeenCheckpointEvents = null;
                return checkpointEvent;
            }
            if (lockingEvent is InitWatermarksEvent initWatermarksEvent)
            {
                Debug.Assert(_lastSeenCheckpointEvents != null, nameof(_lastSeenCheckpointEvents));

                HashSet<string> uniqueNames = new HashSet<string>();
                List<IReadOnlySet<string>> targetsWatermarks = new List<IReadOnlySet<string>>();
                foreach (var e in _lastSeenCheckpointEvents)
                {
                    if (e is InitWatermarksEvent previous)
                    {
                        foreach (var name in previous.WatermarkNames)
                        {
                            uniqueNames.Add(name);
                        }
                        initWatermarksEvent = initWatermarksEvent.AddWatermarkNames(previous.WatermarkNames);
                        targetsWatermarks.Add(previous.WatermarkNames);
                    }
                    else
                    {
                        targetsWatermarks.Add(new HashSet<string>());
                    }
                }
                _targetWatermarkNames = targetsWatermarks.ToArray();
                _currentWatermark = new Watermark(uniqueNames.Select(x => new KeyValuePair<string, long>(x, -1)).ToImmutableDictionary(), DateTimeOffset.UtcNow);


                return initWatermarksEvent;
            }
            return lockingEvent;
        }

        private void CheckpointSent()
        {
            foreach (var target in _targetHolders)
            {
                target.ReleaseCheckpoint();
            }
        }

        private IAsyncEnumerable<IStreamEvent> Passthrough(IStreamEvent streamEvent)
        {
            return new SingleAsyncEnumerable<IStreamEvent>(streamEvent);
        }

        private IAsyncEnumerable<IStreamEvent> Empty()
        {
            return EmptyAsyncEnumerable<IStreamEvent>.Instance;
        }

        public virtual IAsyncEnumerable<T> OnTrigger(string name, object? state)
        {
            return EmptyAsyncEnumerable<T>.Instance;
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

        private bool TargetInCheckpoint(int targetId, ILockingEvent checkpointEvent, out ILockingEvent[]? checkpoints)
        {
            lock (_targetCheckpointLock)
            {
                Debug.Assert(_targetInCheckpoint != null, nameof(_targetInCheckpoint));
                Logger.TargetInCheckpoint(targetId, StreamName, Name);
                _targetInCheckpoint[targetId] = checkpointEvent;

                bool allInCheckpoint = true;

                for (int i = 0; i < _targetInCheckpoint.Length; i++)
                {
                    if (_targetInCheckpoint[i] == null)
                    {
                        allInCheckpoint = false;
                        break;
                    }
                }

                if (allInCheckpoint)
                {
                    Logger.CheckpointInOperator(StreamName, Name);
                    // Create a new array here, have already checked that noone is null in the array
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
                    checkpoints = _targetInCheckpoint.ToArray();
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
                    // Reset
                    for (int i = 0; i < _targetInCheckpoint.Length; i++)
                    {
                        _targetInCheckpoint[i] = null;
                    }
                    return true;
                }
                checkpoints = null;
                return false;
            }
        }

        public abstract Task OnCheckpoint();

        public abstract IAsyncEnumerable<T> OnRecieve(int targetId, T msg, long time);

        public Task Completion => _transformBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        public MultipleInputTargetHolder[] Targets => _targetHolders;

        public void Complete()
        {
            Debug.Assert(_transformBlock != null, nameof(_transformBlock));

            foreach (var target in _targetHolders)
            {
                target.Complete();
            }
            if (tokenSource != null)
            {
                tokenSource.Cancel();
            }

            _transformBlock.Complete();
        }

        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return _sourceBlock.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_transformBlock != null, nameof(_transformBlock));
            if (tokenSource != null)
            {
                tokenSource.Cancel();
            }

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

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            if (_parallelSource != null)
            {
                (_parallelSource as ISourceBlock<IStreamEvent>).ReleaseReservation(messageHeader, target);
            }
            else
            {
                Debug.Assert(_transformBlock != null, nameof(_transformBlock));
                (_transformBlock as ISourceBlock<IStreamEvent>).ReleaseReservation(messageHeader, target);
            }
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return (_sourceBlock as ISourceBlock<IStreamEvent>).ReserveMessage(messageHeader, target);
        }

        public Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler)
        {
            _memoryAllocator = vertexHandler.MemoryManager;
            _name = name;
            _streamName = vertexHandler.StreamName;
            _metrics = vertexHandler.Metrics;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);

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
            return InitializeOrRestore(vertexHandler.StateClient);
        }

        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        protected abstract Task InitializeOrRestore(IStateManagerClient stateManagerClient);

        public abstract Task Compact();

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            // Trigger injection happens at the first target.
            // This is done to make sure that it doesnt run if a checkpoint is being done.
            return _targetHolders.First().SendAsync(triggerEvent);
        }

        public virtual ValueTask DisposeAsync()
        {
            if (tokenSource != null)
            {
                tokenSource.Dispose();
                tokenSource = null;
            }
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
            InitializeBlock();
        }

        public abstract Task DeleteAsync();

        public void Setup(string streamName, string operatorName)
        {
            _name = operatorName;
            _streamName = streamName;

            foreach (var target in Targets)
            {
                target.Setup(operatorName);
            }
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
    }
}
