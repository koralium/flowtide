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

    internal class ExchangeOperator : PartitionVertex<StreamEventBatch, ExchangeOperatorState>, IStreamEgressVertex
    {
        private const string PullBucketRequestTriggerPrefix = "exchange_";

        private readonly ExchangeRelation exchangeRelation;
        private readonly IExchangeKindExecutor _executor;
        private Action<string>? _checkpointDone;
        private ExchangeOperatorState? _state;

        public ExchangeOperator(ExchangeRelation exchangeRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(CalculateTargetNumber(exchangeRelation), executionDataflowBlockOptions)
        {
            this.exchangeRelation = exchangeRelation;

            switch (exchangeRelation.ExchangeKind.Type)
            {
                case ExchangeKindType.Broadcast:
                    _executor = new BroadcastExecutor();
                    break;
                case ExchangeKindType.Scatter:
                    _executor = new ScatterExecutor(exchangeRelation, functionsRegister);
                    break;
                default:
                    throw new NotImplementedException(exchangeRelation.ExchangeKind.Type.ToString());
            }
        }

        private static int CalculateTargetNumber(ExchangeRelation exchangeRelation)
        {
            return exchangeRelation.Targets.Where(x => x.Type == ExchangeTargetType.StandardOutput).Count();
        }

        public override string DisplayName => "Exchange";

        protected override async Task InitializeOrRestore(ExchangeOperatorState? state, IStateManagerClient stateManagerClient)
        {
            if (state == null)
            {
                state = new ExchangeOperatorState();
            }
            _state = state;

            await _executor.Initialize(exchangeRelation, stateManagerClient, state);

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
            await _executor.OnLockingEvent(lockingEvent);

            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                var checkpointData = await OnCheckpoint();
                checkpointEvent.AddState(Name, checkpointData);
            }

            if (_checkpointDone != null)
            {
                _checkpointDone(Name);
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

        void IStreamEgressVertex.SetCheckpointDoneFunction(Action<string> checkpointDone)
        {
            _checkpointDone = checkpointDone;
        }

        private async Task<ExchangeOperatorState> OnCheckpoint()
        {
            Debug.Assert(_state != null);
            await _executor.AddCheckpointState(_state);
            return _state;
        }
    }
}
