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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.FixedPoint;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.PartitionVertices
{
    public abstract class PartitionVertex<T, TState> : ITargetBlock<IStreamEvent>, IStreamVertex
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
        private Meter? _metrics;
        private bool _isHealthy = true;

        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        protected Meter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        public ISourceBlock<IStreamEvent>[] Sources => _sources;

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
                    return new AsyncEnumerableDowncast<KeyValuePair<int, StreamMessage<T>>, KeyValuePair<int, IStreamEvent>>(enumerator, (source) => new KeyValuePair<int, IStreamEvent>(source.Key, source.Value));
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

        private async IAsyncEnumerable<KeyValuePair<int, IStreamEvent>> Broadcast(IStreamEvent streamEvent)
        {
            for (int i = 0; i < targetNumber; i++)
            {
                yield return new KeyValuePair<int, IStreamEvent>(i, streamEvent);
            }
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

        public Task Initialize(string name, long restoreTime, long newTime, JsonElement? state, IVertexHandler vertexHandler)
        {
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

            return InitializeOrRestore(parsedState, vertexHandler.StateClient);
        }

        protected abstract Task InitializeOrRestore(TState? state, IStateManagerClient stateManagerClient);

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
    }
}
