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
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.FixedPoint
{
    /// <summary>
    /// Vertex that handles fixed point iteration.
    /// 
    /// This needs to be a custom vertex since it handles checkpointing differently from multiple input vertex.
    /// It also needs to support two different outputs, one for the loop and one for egress.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TState"></typeparam>
    public abstract class FixedPointVertex<T, TState> : IStreamVertex
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
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");
        private IMeter? _metrics;
        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");
        private bool _isHealthy = true;
        private bool _sentLockingEvent;
        private int _targetPrepareCount = 0;
        private bool singleReadSource;
        private TaskCompletionSource? _pauseSource;
        private IMemoryAllocator? _memoryAllocator;
        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

        public ITargetBlock<IStreamEvent> IngressTarget => _ingressTarget;
        public ITargetBlock<IStreamEvent> FeedbackTarget => _feedbackTarget;
        public ISourceBlock<IStreamEvent> EgressSource => _egressSource;
        public ISourceBlock<IStreamEvent> LoopSource => _loopSource;

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
        /// This method should return if there is any read source in the loop.
        /// This affects how checkpointing performs for the loop, so it waits for read sources checkpoint.
        /// </summary>
        /// <returns></returns>
        public abstract bool NoReadSourceInLoop();

        public Task Completion => _transformBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        public abstract string DisplayName { get; }

        public virtual Task Compact()
        {
            return Task.CompletedTask;
        }

        public void Complete()
        {
            _feedbackTarget.Complete();
            _ingressTarget.Complete();
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

        public void CreateBlock()
        {
            singleReadSource = false;

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

        protected abstract IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> OnIngressRecieve(T data, long time);

        protected abstract IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> OnFeedbackRecieve(T data, long time);

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

            await foreach(var element in input)
            {
                yield return element;
            }
        }

        public virtual Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public virtual ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_transformBlock != null);
            (_transformBlock as IDataflowBlock).Fault(exception);
        }

        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _loopSource.Links.Union(_egressSource.Links);
        }

        public Task Initialize(string name, long restoreTime, long newTime, JsonElement? state, IVertexHandler vertexHandler)
        {
            _memoryAllocator = vertexHandler.MemoryManager;
            _name = name;
            _currentTime = newTime;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _metrics = vertexHandler.Metrics;

            TState? parsedState = default;
            if (state.HasValue)
            {
                parsedState = JsonSerializer.Deserialize<TState>(state.Value);
            }

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

            return InitializeOrRestore(parsedState, vertexHandler.StateClient);
        }

        protected abstract Task InitializeOrRestore(TState? state, IStateManagerClient stateManagerClient);

        public void Link()
        {
            _egressSource.Link();
            _loopSource.Link();
        }

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            throw new NotSupportedException("Triggers are not supported in fixed point vertices");
        }

        public void Setup(string streamName, string operatorName)
        {
            _streamName = streamName;
            _name = operatorName;
            _ingressTarget.Setup(operatorName);
            _egressSource.Setup(streamName, operatorName);
            _loopSource.Setup(streamName, operatorName);
            _feedbackTarget.Setup(operatorName);
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
