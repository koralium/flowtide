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
using FlowtideDotNet.Base.Vertices.FixedPoint;
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.PartitionVertices
{
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
        private IMemoryAllocator? _memoryAllocator;
        private TaskCompletionSource? _pauseSource;

        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        public ISourceBlock<IStreamEvent>[] Sources => _sources;

        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

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
        public Task Completion => _inputBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        public abstract string DisplayName { get; }

        public Task Compact()
        {
            return Task.CompletedTask;
        }

        public void Complete()
        {
            Debug.Assert(_inputBlock != null);
            _inputBlock.Complete();
        }

        protected abstract IAsyncEnumerable<KeyValuePair<int, StreamMessage<T>>> PartitionData(T data, long time);


        public void CreateBlock()
        {
            _inputBlock = new TransformManyBlock<IStreamEvent, KeyValuePair<int, IStreamEvent>>(x =>
            {
                if (x is ILockingEvent ev)
                {
                    return Broadcast(ev);
                }
                if (x is LockingEventPrepare lockingEventPrepare)
                {
                    return Broadcast(lockingEventPrepare);
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
                    return Broadcast(watermark);
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

        private IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> Broadcast(IStreamEvent streamEvent)
        {
            KeyValuePair<int, IStreamEvent>[] output = new KeyValuePair<int, IStreamEvent>[targetNumber];
            for (int i = 0; i < targetNumber; i++)
            {
                output[i] = new KeyValuePair<int, IStreamEvent>(i, streamEvent);
            }
            return output.ToAsyncEnumerable();
        }

        public Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(_inputTargetBlock != null);
            return _inputTargetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_inputTargetBlock != null);
            _inputTargetBlock.Fault(exception);
        }

        public void Setup(string streamName, string operatorName)
        {
            _streamName = streamName;
            _name = operatorName;
        }

        public Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler)
        {
            _memoryAllocator = vertexHandler.MemoryManager;
            _name = name;
            _currentTime = newTime;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _metrics = vertexHandler.Metrics;

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

        public void Link()
        {
            for (int i = 0; i < _sources.Length; i++)
            {
                _sources[i].Link();
            }
        }

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            throw new NotSupportedException("Triggers are not supported in partition vertices");
        }

        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            List<ITargetBlock<IStreamEvent>> output = new List<ITargetBlock<IStreamEvent>>();
            foreach (var source in _sources)
            {
                output.AddRange(source.GetLinks());
            }
            return output;
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
