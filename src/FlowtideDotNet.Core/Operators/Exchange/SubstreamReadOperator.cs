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
        private SemaphoreSlim _writeLock = new SemaphoreSlim(1);
        private SemaphoreSlim? _waitLock;
        private IObjectState<HashSet<string>>? _watermarkNamesState;

        public SubstreamReadOperator(SubstreamCommunicationPoint communicationPoint, SubstreamExchangeReferenceRelation referenceRelation, DataflowBlockOptions options) : base(options)
        {
            this._communicationPoint = communicationPoint;
            _exchangeReferenceRelation = referenceRelation;
            _communicationPoint.RegisterReadOperator(this);
        }

        public override string DisplayName => "Substream Read";

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
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            Debug.Assert(_watermarkNamesState != null);
            Debug.Assert(_watermarkNamesState.Value != null);
            return Task.FromResult<IReadOnlySet<string>>(_watermarkNamesState.Value);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await _communicationPoint.InitializeOperator(restoreTime);
            _waitLock = new SemaphoreSlim(0);
            _writeLock.Release();
            _writeLock = new SemaphoreSlim(1);
            _queue = await stateManagerClient.GetOrCreateQueue("queue", new FlowtideQueueOptions<IStreamEvent, StreamEventValueContainer>()
            {
                MemoryAllocator = MemoryAllocator,
                ValueSerializer = new StreamEventValueSerializer(MemoryAllocator)
            });
            _watermarkNamesState = await stateManagerClient.GetOrCreateObjectStateAsync<HashSet<string>>("watermarkNames");
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_watermarkNamesState != null);
            await _watermarkNamesState.Commit();
        }

        protected override Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            return Task.CompletedTask;
        }

        private async Task FetchData(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_waitLock != null);
            Debug.Assert(_queue != null);
            Debug.Assert(_watermarkNamesState != null);

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

            while (!output.CancellationToken.IsCancellationRequested)
            {
                await _waitLock.WaitAsync(output.CancellationToken);

                await _writeLock.WaitAsync(output.CancellationToken);

                var ev = await _queue.Dequeue();

                _writeLock.Release();

                output.CancellationToken.ThrowIfCancellationRequested();

                if (ev is ICheckpointEvent checkpointEvent)
                {
                    ICheckpointEvent? inStreamCheckpoint = default;
                    bool scheduleCheckpoint = false;
                    lock (_lock)
                    {
                        if (_currentCheckpoint != null)
                        {
                            inStreamCheckpoint = _currentCheckpoint;
                        }
                        else if (_waitForCheckpoint == null)
                        {
                            _waitForCheckpoint = new TaskCompletionSource();
                            scheduleCheckpoint = true;
                        }
                    }
                    if (scheduleCheckpoint)
                    {
                        // Schedule outside the lock to hinder any deadlocks with OnLockingEvent
                        ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                    }
                    
                    if (inStreamCheckpoint == null)
                    {
                        // Wait until the checkpoint event has been collected from this stream.
                        Debug.Assert(_waitForCheckpoint != null);
                        await _waitForCheckpoint.Task;
                        
                        lock (_lock)
                        {
                            _waitForCheckpoint = null; // Reset wait for checkpoint after this is completed
                            inStreamCheckpoint = _currentCheckpoint;
                        }
                    }
                    if (inStreamCheckpoint != null)
                    {
                        await OnCheckpoint(inStreamCheckpoint.CheckpointTime);
                        await output.SendLockingEvent(checkpointEvent);
                        _currentCheckpoint = null;
                    }
                    else
                    {
                        throw new InvalidOperationException("Checkpoint event not found in stream");
                    }
                }
                else if (ev is InitWatermarksEvent initWatermarksEvent)
                {
                    // TODO: This might mean that the other stream failed and restarted.
                    // some check here is needed to check if this stream should fail
                    _watermarkNamesState.Value = initWatermarksEvent.WatermarkNames.ToHashSet();
                    await output.SendLockingEvent(initWatermarksEvent);
                    SetDependenciesDone();
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

        public override async Task OnFailure(long rollbackVersion)
        {
            _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
            await _communicationPoint.SendFailAndRecover(rollbackVersion);
        }

        public override Task CheckpointDone(long checkpointVersion)
        {
            // Send checkpoint done to the communication point so the other substream can set dependencies done.
            return _communicationPoint.SendCheckpointDone(checkpointVersion);
        }

        public void RecieveCheckpointDone(long checkpointVersion)
        {
            SetDependenciesDone();
        }

        public Task FailAndRecover(long recoveryPoint)
        {
            return FailAndRollback(restoreVersion: recoveryPoint);
        }

        public override void DoLockingEvent(ILockingEvent lockingEvent)
        {
            // At this point the operator states are stored in the checkpoint object
            // So the checkpoint object must be stored to be used inside this stream.
            // If state is handled by the state manager client instead this is not required.
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                TaskCompletionSource? taskSource = default;
                lock (_lock)
                {
                    _currentCheckpoint = checkpointEvent;
                    if (_waitForCheckpoint != null)
                    {
                        taskSource = _waitForCheckpoint;
                    }
                }
                if (taskSource != null)
                {
                    // Set task completion source outside of lock to hinder any deadlocks
                    taskSource.SetResult();
                }
            }
            if (_fetchTask == null)
            {
                _fetchTask = RunTask(FetchData)
                    .ContinueWith(t =>
                    {
                        _fetchTask = null;
                    });
            }
            if (lockingEvent is InitWatermarksEvent initWatermarksEvent &&
                _watermarkNamesState != null && 
                _watermarkNamesState.Value != null)
            {
                // Run task to send watermark values
                base.DoLockingEvent(lockingEvent);
            }
        }
    }
}
