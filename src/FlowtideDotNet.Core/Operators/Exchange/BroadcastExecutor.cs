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
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Broadcasts events to all targets.
    /// </summary>
    internal class BroadcastExecutor : IExchangeKindExecutor
    {
        /// <summary>
        /// A single tree is enough in broadcast mode.
        /// </summary>
        private IAppendTree<long, IStreamEvent>? _events;

        private long _eventCounter;
        private bool hasStandardOutputTargets;
        private int standardOutputTargetNumber;
        private readonly object _lock = new object();

        private static bool HasPullBucketTargets(ExchangeRelation exchangeRelation)
        {
            return exchangeRelation.Targets.Exists(x => x.Type == ExchangeTargetType.PullBucket);
        }

        public async Task Initialize(ExchangeRelation exchangeRelation, IStateManagerClient stateManagerClient, ExchangeOperatorState exchangeOperatorState)
        {
            _eventCounter = exchangeOperatorState.EventCounter;

            hasStandardOutputTargets = exchangeRelation.Targets.Exists(x => x.Type == ExchangeTargetType.StandardOutput);
            standardOutputTargetNumber = exchangeRelation.Targets.Count(x => x.Type == ExchangeTargetType.StandardOutput);
            if (HasPullBucketTargets(exchangeRelation))
            {
                _events = await stateManagerClient.GetOrCreateAppendTree<long, IStreamEvent>("events", new BPlusTreeOptions<long, IStreamEvent>()
                {
                    Comparer = new LongComparer(),
                    KeySerializer = new LongSerializer(),
                    ValueSerializer = new StreamEventSerializer(),
                    BucketSize = 8
                });
            }
        }

        private long GetNewEventId()
        {
            lock (_lock)
            {
                return _eventCounter++;
            }
        }

        public async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            if (_events != null)
            {
                await _events.Append(GetNewEventId(), new StreamMessage<StreamEventBatch>(data, time));
            }
            if (hasStandardOutputTargets)
            {
                for (int i = 0; i < standardOutputTargetNumber; i++)
                {
                    yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(i, new StreamMessage<StreamEventBatch>(data, time));
                }
            }
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            if (_events != null)
            {
                await _events.Append(GetNewEventId(), lockingEvent);
            }
        }

        public async Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            if (_events != null)
            {
                await _events.Append(GetNewEventId(), lockingEventPrepare);
            }
        }

        public async Task OnWatermark(Watermark watermark)
        {
            if (_events != null)
            {
                await _events.Append(GetNewEventId(), watermark);
            }
        }

        public Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            exchangeOperatorState.EventCounter = _eventCounter;
            return Task.CompletedTask;
        }

        public async Task GetPullBucketData(int exchangeTargetId, ExchangeFetchDataMessage fetchDataRequest)
        {
            Debug.Assert(_events != null);

            lock (_lock)
            {
                if (_eventCounter <= fetchDataRequest.FromEventId)
                {
                    fetchDataRequest.LastEventId = fetchDataRequest.FromEventId;
                    fetchDataRequest.OutEvents = new List<IStreamEvent>();
                    return;
                }
            }

            var iterator = _events.CreateIterator();
            await iterator.Seek(fetchDataRequest.FromEventId);

            List<IStreamEvent> outputData = new List<IStreamEvent>();
            await foreach(var kv in iterator)
            {
                
                fetchDataRequest.LastEventId = kv.Key;
                outputData.Add(kv.Value);
                if (outputData.Count > 100)
                {
                    break;
                }
            }
            fetchDataRequest.OutEvents = outputData;
        }
    }
}
