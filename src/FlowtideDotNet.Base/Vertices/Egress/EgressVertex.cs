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
using FlowtideDotNet.Base.Vertices.Egress.Internal;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Egress
{
    public abstract class EgressVertex<T, TState> : ITargetBlock<IStreamEvent>, IStreamEgressVertex
    {
        private Action<string>? _checkpointDone;
        private readonly ExecutionDataflowBlockOptions _executionDataflowBlockOptions;
        private IEgressImplementation? _targetBlock;
        private bool _isHealthy = true;
        private CancellationTokenSource? _cancellationTokenSource;

        public string Name { get; private set; }

        protected string StreamName { get; private set; }

        protected IMeter Metrics { get; private set; }

        public abstract string DisplayName { get; }

        public ILogger Logger { get; private set; }

        protected CancellationToken CancellationToken => _cancellationTokenSource?.Token ?? throw new InvalidOperationException("Cancellation token can only be fetched after initialization.");

        protected EgressVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            _executionDataflowBlockOptions = executionDataflowBlockOptions;
        }

        [MemberNotNull(nameof(_targetBlock))]
        private void InitializeBlocks()
        {
            if (_executionDataflowBlockOptions.GetSupportsParallelExecution())
            {
                _targetBlock = new ParallelEgressVertex<T>(_executionDataflowBlockOptions, OnRecieve, HandleLockingEvent, HandleCheckpointDone, OnTrigger, OnWatermark);
            }
            else
            {
                _targetBlock = new NonParallelEgressVertex<T>(_executionDataflowBlockOptions, OnRecieve, HandleLockingEvent, HandleCheckpointDone, OnTrigger, OnWatermark);
            }
        }

        protected virtual Task OnWatermark(Watermark watermark)
        {
            return Task.CompletedTask;
        }

        private void HandleCheckpointDone()
        {
            if (_checkpointDone != null && Name != null)
            {
                Logger.LogTrace("Calling checkpoint done.");
                _checkpointDone(Name);
            }
            else
            {
                Logger.LogWarning("Checkpoint done function not set on egress, checkpoint wont be able to complete");
            }
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
            var newState = await OnCheckpoint(checkpointEvent.CheckpointTime);
            checkpointEvent.AddState(Name, newState);
        }

        public virtual Task OnTrigger(string name, object? state)
        {
            return Task.CompletedTask;
        }

        protected abstract Task<TState> OnCheckpoint(long checkpointTime);

        protected abstract Task OnRecieve(T msg, long time);

        public Task Completion => _targetBlock?.Completion ?? throw new NotSupportedException("CreateBlocks must be called before getting completion");

        public void Complete()
        {
            Debug.Assert(_targetBlock != null, "CreateBlocks must be called before completing");
            _targetBlock.Complete();
        }

        public void Fault(Exception exception)
        {
            _cancellationTokenSource?.Cancel();
            Debug.Assert(_targetBlock != null, "CreateBlocks must be called before faulting");
            _targetBlock.Fault(exception);
        }

        public Task Initialize(string name, long restoreTime, long newTime, JsonElement? state, IVertexHandler vertexHandler)
        {
             _cancellationTokenSource = new CancellationTokenSource();
            Name = name;
            StreamName = vertexHandler.StreamName;
            Metrics = vertexHandler.Metrics;
            TState? dState = default;
            if (state.HasValue)
            {
                dState = JsonSerializer.Deserialize<TState>(state.Value);
            }
            Logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);

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
                TagList tags = new TagList();
                tags.Add("links", "[]");
                return new Measurement<int>(1, tags);
            });

            return InitializeOrRestore(restoreTime, dState, vertexHandler.StateClient);
        }

        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        protected abstract Task InitializeOrRestore(long restoreTime, TState? state, IStateManagerClient stateManagerClient);

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(_targetBlock != null, nameof(_targetBlock));
            return _targetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        void IStreamEgressVertex.SetCheckpointDoneFunction(Action<string> checkpointDone)
        {
            _checkpointDone = checkpointDone;
        }

        public abstract Task Compact();

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            Debug.Assert(_targetBlock != null, nameof(_targetBlock));
            return _targetBlock.SendAsync(triggerEvent);
        }

        public virtual ValueTask DisposeAsync()
        {
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Dispose();
                _cancellationTokenSource = null;
            }
            
            return ValueTask.CompletedTask;
        }

        public void Link()
        {
        }

        public void CreateBlock()
        {
            InitializeBlocks();
        }

        public abstract Task DeleteAsync();

        public void Setup(string streamName, string operatorName)
        {
            Name = operatorName;
            StreamName = streamName;
        }

        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return Enumerable.Empty<ITargetBlock<IStreamEvent>>();
        }
    }
}
