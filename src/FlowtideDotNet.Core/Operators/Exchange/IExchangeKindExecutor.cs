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

using FlowtideDotNet.Base;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal interface IExchangeKindExecutor
    {
        Task Initialize(ExchangeRelation exchangeRelation, IStateManagerClient stateManagerClient, ExchangeOperatorState exchangeOperatorState, IMemoryAllocator memoryAllocator);

        IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time);

        Task OnLockingEvent(ILockingEvent lockingEvent);

        Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare);

        Task OnWatermark(Watermark watermark);

        Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState);

        Task GetPullBucketData(int exchangeTargetId, ExchangeFetchDataMessage fetchDataRequest);
    }
}
