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
using FlowtideDotNet.Storage.StateManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class StandardOutputTarget : IExchangeTarget
    {
        private List<RowEvent>? _events;

        public Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            return Task.CompletedTask;
        }

        public ValueTask AddEvent(RowEvent rowEvent)
        {
            if (_events == null)
            {
                _events = new List<RowEvent>();
            }
            _events.Add(rowEvent);
            return ValueTask.CompletedTask;
        }

        public ValueTask BatchComplete(long time)
        {
            return ValueTask.CompletedTask;
        }

        public List<RowEvent>? GetEvents()
        {
            var tmp = _events;
            _events = null;
            return tmp;
        }

        public Task Initialize(int targetId, IStateManagerClient stateManagerClient, ExchangeOperatorState state)
        {
            return Task.CompletedTask;
        }

        public Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            // Locking events for standard output are handled automatically
            return Task.CompletedTask;
        }

        public Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            // Locking event prepare for standard output are handled automatically
            return Task.CompletedTask;
        }

        public Task OnWatermark(Watermark watermark)
        {
            // Watermark for standard output are handled automatically
            return Task.CompletedTask;
        }

    }
}
