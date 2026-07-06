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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
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
        private Action<string, ILockingEvent?>? _checkpointDone;
        private Action<string, ILockingEvent?>? _dependenciesDone;
        private IObjectState<ExchangeOperatorState>? _state;

        private readonly object _dependenciesDoneLock = new object();
        private int _dependenciesDoneCalled = 0;
        private int _numberOfSubstreams = 0;
        // Checkpoint done signals from other substreams that arrived before this stream
        // finished starting, replayed on the first checkpoint cycle. Guarded by
        // _dependenciesDoneLock.
        private int _pendingDependenciesDoneSignals = 0;


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

        /// <summary>
        /// The stream may first finish stopping when other substreams have fetched this
        /// streams stop barrier from all substream targets.
        /// </summary>
        public bool ReadyToStop => _executor.ReadyToStop;

        private Task FailAndRecoverMethod(long recoveryPoint)
        {
            return FailAndRollback(restoreVersion: recoveryPoint);
        }

        protected override async Task InitializeOrRestore(long restoreVersion, IStateManagerClient stateManagerClient)
        {
            lock (_dependenciesDoneLock)
            {
                // Reset counts from a checkpoint that was aborted by a failure so an old
                // checkpoint done message cannot complete dependencies for a new checkpoint.
                _dependenciesDoneCalled = 0;
                // Buffered signals from before the restore belong to the aborted epoch. The
                // rollback re-aligns both substreams at a common version, the peer sends a
                // fresh acknowledgement when it completes a checkpoint in the new epoch.
                // Replaying a stale one would complete a new checkpoints dependencies before
                // the peer stored it, and every cycle after that completes one ack early.
                _pendingDependenciesDoneSignals = 0;
            }
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
                _checkpointDone(Name, lockingEvent);
            }
            if (_dependenciesDone != null && (_numberOfSubstreams == 0 || lockingEvent is InitWatermarksEvent))
            {
                _dependenciesDone(Name, lockingEvent);
            }
            else if (lockingEvent is ICheckpointEvent)
            {
                // Replay checkpoint done signals that arrived before this stream finished
                // starting. A cycle needs one signal per peer substream, so every buffered
                // signal is replayed at once, they can only belong to this cycle: no
                // barriers were produced while the callback was unwired, so each peer can
                // have at most one acknowledgement in flight from before the start.
                int replaySignals;
                lock (_dependenciesDoneLock)
                {
                    replaySignals = _pendingDependenciesDoneSignals;
                    _pendingDependenciesDoneSignals = 0;
                }
                if (replaySignals > 0)
                {
                    Logger.LogDebug("Exchange {name} processed checkpoint event, replays {count} buffered signals", Name, replaySignals);
                }
                for (int i = 0; i < replaySignals; i++)
                {
                    TargetCallDependenciesDone();
                }
            }
        }

        private void TargetCallDependenciesDone()
        {
            lock (_dependenciesDoneLock)
            {
                if (_dependenciesDone == null)
                {
                    // The signal arrived before this stream finished starting, the dependencies
                    // done callback is not wired yet. Each checkpoint cycle consumes the signals
                    // from the other substreams, so the signal must not be lost, it is buffered
                    // and replayed when a checkpoint runs.
                    _pendingDependenciesDoneSignals++;
                    Logger.LogDebug("Exchange {name} buffered a dependencies done signal, total buffered: {count}", Name, _pendingDependenciesDoneSignals);
                    return;
                }

                _dependenciesDoneCalled++;
                Logger.LogDebug("Exchange {name} dependencies done called {called} of {required}", Name, _dependenciesDoneCalled, _numberOfSubstreams);

                if (_dependenciesDoneCalled >= _numberOfSubstreams)
                {
                    // Checkpoint acknowledgements from the other substreams, always checkpoint
                    // related so no locking event instance is passed.
                    _dependenciesDone(Name, null);
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

        protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            Logger.ExchangePartitionInput(Name, data.Data.Weights.Count);
            await foreach (var partition in _executor.PartitionData(data, time))
            {
                Logger.ExchangePartitionOutput(Name, partition.Value.Data.Data.Weights.Count, partition.Key);
                yield return partition;
            }
        }

        void IStreamEgressVertex.SetCheckpointDoneFunction(Action<string, ILockingEvent?> checkpointDone, Action<string, ILockingEvent?> dependenciesDone)
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
