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
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Messages;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Orleans.Internal
{
    internal class PullExchangeReadOperator : IngressVertex<StreamEventBatch>
    {
        private readonly PullExchangeReferenceRelation exchangeReferenceRelation;
        private readonly IGrainFactory grainFactory;
        private IStreamGrain? _referenceGrain;
        private readonly object _lock = new object();
        private ICheckpointEvent? _currentCheckpoint;
        private TaskCompletionSource? _waitForCheckpoint;
        private Task? _fetchTask;

        public PullExchangeReadOperator(PullExchangeReferenceRelation exchangeReferenceRelation, IGrainFactory grainFactory, DataflowBlockOptions options) : base(options)
        {
            this.exchangeReferenceRelation = exchangeReferenceRelation;
            this.grainFactory = grainFactory;
        }

        public override string DisplayName => "Pull Data";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            throw new NotImplementedException();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            throw new NotImplementedException();
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _referenceGrain = grainFactory.GetGrain<IStreamGrain>(exchangeReferenceRelation.SubStreamName);

            
            return Task.CompletedTask;
        }

        private async Task FetchData(IngressOutput<StreamEventBatch> output, object? state)
        {
            long nextId = 0;
            while(!output.CancellationToken.IsCancellationRequested)
            {
                GetEventsResponse? events = null;
                while (true)
                {
                    events = await _referenceGrain.GetEventsAsync(new Messages.GetEventsRequest(nextId, exchangeReferenceRelation.ExchangeTargetId));
                        
                    if (!events.NotStarted)
                    {
                        break;
                    }
                    else
                    {
                        await Task.Delay(100);
                    }
                }
                
                if (events.Events.Count == 0)
                {
                    await Task.Delay(100);
                    continue;
                }
                nextId = events.LastEventId + 1;
                for (int i = 0; i < events.Events.Count; i++)
                {
                    var e = events.Events[i];

                    if (e is ICheckpointEvent checkpointEvent)
                    {
                        ICheckpointEvent? inStreamCheckpoint = default;
                        lock (_lock)
                        {
                            if (_currentCheckpoint != null)
                            {
                                inStreamCheckpoint = _currentCheckpoint;
                            }
                            else
                            {
                                _waitForCheckpoint = new TaskCompletionSource();
                                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                            }
                        }
                        if (inStreamCheckpoint == null)
                        {
                            // Wait until the checkpoint event has been collected from this stream.
                            Debug.Assert(_waitForCheckpoint != null);
                            await _waitForCheckpoint.Task;
                            lock (_lock)
                            {
                                inStreamCheckpoint = _currentCheckpoint;
                            }
                        }
                        if (inStreamCheckpoint != null)
                        {
                            await output.SendLockingEvent(checkpointEvent);
                            _currentCheckpoint = null;
                        }
                        else
                        {
                            throw new InvalidOperationException("Checkpoint event not found in stream");
                        }
                    }
                    else if (e is ILockingEvent lockingEvent)
                    {
                        await output.SendLockingEvent(lockingEvent);
                    }
                    else if (e is StreamMessage<StreamEventBatch> eventBatch)
                    {
                        await output.SendAsync(eventBatch.Data);
                    }
                    if (e is Watermark watermark)
                    {
                        await output.SendWatermark(watermark);
                    }
                }
            }
            
        }

        public override void DoLockingEvent(ILockingEvent lockingEvent)
        {
            // At this point the operator states are stored in the checkpoint object
            // So the checkpoint object must be stored to be used inside this stream.
            // If state is handled by the state manager client instead this is not required.
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                lock (_lock)
                {
                    _currentCheckpoint = checkpointEvent;
                    if (_waitForCheckpoint != null)
                    {
                        _waitForCheckpoint.SetResult();
                    }
                }
                
            }
            if (_fetchTask == null)
            {
                _fetchTask = RunTask(FetchData)
                    .ContinueWith(t =>
                    {

                    });
            }
        }

        protected override Task<object> OnCheckpoint(long checkpointTime)
        {
            throw new NotImplementedException();
        }

        protected override Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            return Task.CompletedTask;
        }
    }
}
