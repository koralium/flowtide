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

using Apache.Arrow.Memory;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Storage.Queue;
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
    internal class SubstreamReadOperator : IngressVertex<StreamEventBatch>
    {
        private readonly SubstreamCommunicationPoint _communicationPoint;
        private readonly SubstreamExchangeReferenceRelation _exchangeReferenceRelation;
        private readonly object _lock = new object();
        private ICheckpointEvent? _currentCheckpoint;
        private TaskCompletionSource? _waitForCheckpoint;
        private Task? _fetchTask;
        private IFlowtideQueue<IStreamEvent, StreamEventValueContainer>? _queue;
        private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1);
        private SemaphoreSlim? _waitLock;

        public SubstreamReadOperator(SubstreamCommunicationPoint communicationPoint, SubstreamExchangeReferenceRelation referenceRelation, DataflowBlockOptions options) : base(options)
        {
            this._communicationPoint = communicationPoint;
            _exchangeReferenceRelation = referenceRelation;
        }

        public override string DisplayName => "Substream Read";

        public override Task Compact()
        {
            throw new NotImplementedException();
        }

        public override Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            throw new NotImplementedException();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            throw new NotImplementedException();
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _waitLock = new SemaphoreSlim(0);
            _queue = await stateManagerClient.GetOrCreateQueue("queue", new FlowtideQueueOptions<IStreamEvent, StreamEventValueContainer>()
            {
                MemoryAllocator = MemoryAllocator,
                ValueSerializer = new StreamEventValueSerializer(MemoryAllocator)
            });
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            throw new NotImplementedException();
        }

        protected override Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            return Task.CompletedTask;
        }

        private async Task FetchData(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_waitLock != null);
            Debug.Assert(_queue != null);
            // Fetch data from the communication point
            _communicationPoint.Subscribe(_exchangeReferenceRelation.ExchangeTargetId, async (ev) =>
            {
                await _writeLock.WaitAsync();
                try
                {
                    await _queue.Enqueue(ev);
                    _waitLock.Release();
                }
                finally
                {
                    _writeLock.Release();
                }
            });

            while (true)
            {
                await _waitLock.WaitAsync();

                await _writeLock.WaitAsync();

                var ev = await _queue.Dequeue();

                _writeLock.Release();

                if (ev is ICheckpointEvent checkpointEvent)
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
                else if (ev is ILockingEvent lockingEvent)
                {
                    await output.SendLockingEvent(lockingEvent);
                }
                else if (ev is StreamMessage<StreamEventBatch> streamMessage)
                {
                    await output.SendAsync(streamMessage.Data);
                }
                else if (ev is Watermark watermark)
                {
                    await output.SendWatermark(watermark);
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
    }
}
