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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Base.Vertices.PartitionVertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class ExchangeOperatorState
    {
        public long EventCounter { get; set; }

        public Dictionary<int, long> TargetsEventCounter { get; set; } = new Dictionary<int, long>();
    }

    internal class ExchangeOperator : PartitionVertex<StreamEventBatch>, IStreamEgressVertex
    {
        private const string PullBucketRequestTriggerPrefix = "exchange_";

        private readonly SubstreamCommunicationPointFactory _communicationPointFactory;
        internal readonly ExchangeRelation exchangeRelation;
        private readonly IExchangeKindExecutor _executor;
        private Action<string>? _checkpointDone;
        private Action<string>? _dependenciesDone;
        private IObjectState<ExchangeOperatorState>? _state;
        private bool _containPullBucket = false;

        private readonly object _dependenciesDoneLock = new object();
        private int _dependenciesDoneCalled = 0;
        private int _numberOfSubstreams = 0;


        public ExchangeOperator(ExchangeRelation exchangeRelation, SubstreamCommunicationPointFactory communicationPointFactory, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(CalculateTargetNumber(exchangeRelation), executionDataflowBlockOptions)
        {
            this.exchangeRelation = exchangeRelation;
            this._communicationPointFactory = communicationPointFactory;
            switch (exchangeRelation.ExchangeKind.Type)
            {
                case ExchangeKindType.Broadcast:
                    _executor = new BroadcastExecutor(exchangeRelation);
                    break;
                case ExchangeKindType.Scatter:
                    _executor = new ScatterExecutor(exchangeRelation, communicationPointFactory, functionsRegister, TargetCallDependenciesDone);
                    break;
                default:
                    throw new NotImplementedException(exchangeRelation.ExchangeKind.Type.ToString());
            }

            foreach(var target in exchangeRelation.Targets)
            {
                if (target.Type == ExchangeTargetType.PullBucket)
                {
                    _containPullBucket = true;
                    break;
                }
                if (target.Type == ExchangeTargetType.Substream)
                {
                    _numberOfSubstreams++;
                }
            }
        }

        private static int CalculateTargetNumber(ExchangeRelation exchangeRelation)
        {
            return exchangeRelation.Targets.Where(x => x.Type == ExchangeTargetType.StandardOutput).Count();
        }

        public override string DisplayName => "Exchange";

        private Task FailAndRecoverMethod(long recoveryPoint)
        {
            return FailAndRollback(restoreVersion: recoveryPoint);
        }

        protected override async Task InitializeOrRestore(long restoreVersion, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<ExchangeOperatorState>("state");
            if (_state.Value == null)
            {
                _state.Value = new ExchangeOperatorState();
            }

            await _executor.Initialize(restoreVersion, exchangeRelation, stateManagerClient, _state.Value, MemoryAllocator, FailAndRecoverMethod);

            foreach(var pullTarget in exchangeRelation.Targets)
            {
                // Register triggers for each pull exchange target so it can be reached from outside the stream.
                if (pullTarget is PullBucketExchangeTarget pullBucketExchangeTarget)
                {
                    await RegisterTrigger($"{PullBucketRequestTriggerPrefix}{pullBucketExchangeTarget.ExchangeTargetId}");
                }
            }
        }

        public override async Task QueueTrigger(TriggerEvent triggerEvent)
        {
            // Check if it is a request to fetch data from a pull bucket
            if (triggerEvent.State is ExchangeFetchDataMessage exchangeFetchDataRequest)
            {
                if (triggerEvent.Name.StartsWith(PullBucketRequestTriggerPrefix))
                {
                    var exchangeIdString = triggerEvent.Name.Substring(PullBucketRequestTriggerPrefix.Length);
                    if (int.TryParse(exchangeIdString, out var exchangeId))
                    {
                        await _executor.GetPullBucketData(exchangeId, exchangeFetchDataRequest);
                    }
                    else
                    {
                        throw new InvalidOperationException("Invalid exchange target id, it must be an integer value");
                    }
                }
            }
            if (triggerEvent.State is CheckpointRequestedMessage)
            {
                // This message can happen if the downstream wants to take a checkpoint.
                // Then it can send a message to this stream to request a checkpoint to be started.
                // Since checkpoints only happend on data changes, there might not be a data change in this stream that would trigger a checkpoint.
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        protected override async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            Debug.Assert(_state != null);
            await _executor.OnLockingEvent(lockingEvent);

            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                await OnCheckpoint();
                await _state.Commit();
            }

            if (_checkpointDone != null)
            {
                _checkpointDone(Name);
            }
            if (_dependenciesDone != null && (_numberOfSubstreams == 0 || lockingEvent is InitWatermarksEvent))
            {
                _dependenciesDone(Name);
            }
        }

        private void TargetCallDependenciesDone()
        {
            if (_dependenciesDone == null)
            {
                throw new InvalidOperationException("No dependencies done function set.");
            }

            lock (_dependenciesDoneLock)
            {
                _dependenciesDoneCalled++;

                if (_dependenciesDoneCalled == _numberOfSubstreams)
                {
                    _dependenciesDone(Name);
                    _dependenciesDoneCalled = 0;
                }
            }
        }

        internal override Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            return _executor.OnLockingEventPrepare(lockingEventPrepare);
        }

        protected override Task OnWatermark(Watermark watermark)
        {
            return _executor.OnWatermark(watermark);
        }

        protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            return _executor.PartitionData(data, time);
        }

        void IStreamEgressVertex.SetCheckpointDoneFunction(Action<string> checkpointDone, Action<string> dependenciesDone)
        {
            _checkpointDone = checkpointDone;
            _dependenciesDone = dependenciesDone;
        }

        private async Task OnCheckpoint()
        {
            Debug.Assert(_state?.Value != null);
            await _executor.AddCheckpointState(_state.Value);
        }

        public override Task OnFailure(long rollbackVersion)
        {
            return _executor.OnFailure(rollbackVersion);
        }

        public Task CheckpointDone(long checkpointVersion)
        {
            return _executor.CheckpointDone(checkpointVersion);
        }
    }
}
