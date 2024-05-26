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
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class PullBucketTarget : IExchangeTarget
    {
        private int _targetId;
        private long _eventCounter;
        private IBPlusTree<long, IStreamEvent>? _events;
        private List<RowEvent>? _eventBatchList;

        public async Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            Debug.Assert(_events != null);
            // Commit the tree with events
            await _events.Commit();
            // Add the current event counter
            exchangeOperatorState.TargetsEventCounter[_targetId] = _eventCounter;
        }

        public ValueTask AddEvent(RowEvent rowEvent)
        {
            if (_eventBatchList == null)
            {
                _eventBatchList = new List<RowEvent>();
            }
            _eventBatchList.Add(rowEvent);
            return ValueTask.CompletedTask;
        }

        public async ValueTask BatchComplete(long time)
        {
            Debug.Assert(_events != null);

            if (_eventBatchList != null)
            {
                await _events.Upsert(_eventCounter++, new StreamMessage<StreamEventBatch>(new StreamEventBatch(_eventBatchList), time));
                _eventBatchList = null;
            }
        }

        public async Task Initialize(int targetId, IStateManagerClient stateManagerClient, ExchangeOperatorState state)
        {
            _targetId = targetId;
            if (state.TargetsEventCounter.TryGetValue(_targetId, out var eventCounter))
            {
                _eventCounter = eventCounter;
            }
            _events = await stateManagerClient.GetOrCreateTree<long, IStreamEvent>($"events_target_{targetId}", new BPlusTreeOptions<long, IStreamEvent>()
            {
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StreamEventSerializer(),
                BucketSize = 32
            });
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            Debug.Assert(_events != null);
            await _events.Upsert(_eventCounter++, lockingEvent);
        }

        public async Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            Debug.Assert(_events != null);
            await _events.Upsert(_eventCounter++, lockingEventPrepare);
        }

        public async Task OnWatermark(Watermark watermark)
        {
            Debug.Assert(_events != null);
            await _events.Upsert(_eventCounter++, watermark);
        }

        public async Task FetchData(ExchangeFetchDataMessage message)
        {
            Debug.Assert(_events != null);
            var iterator = _events.CreateIterator();
            await iterator.Seek(message.FromEventId);

            List<IStreamEvent> outputData = new List<IStreamEvent>();
            await foreach (var page in iterator)
            {
                page.EnterLock();
                foreach (var kv in page)
                {
                    message.LastEventId = kv.Key;
                    outputData.Add(kv.Value);
                }
                page.ExitLock();
                if (outputData.Count > 100)
                {
                    break;
                }
            }
            message.OutEvents = outputData;
        }
    }
}
