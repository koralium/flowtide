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

using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Egress.Internal
{
    internal class NonParallelEgressVertex<T> : IEgressImplementation
    {
        private ActionBlock<IStreamEvent> _block;
        private readonly ExecutionDataflowBlockOptions _executionDataflowBlockOptions;
        private readonly Func<T, long, Task> _recieveFunc;
        private readonly Func<ILockingEvent, Task> _doCheckpoint;
        private readonly Action _checkpointDone;
        private readonly Func<string, object?, Task> _onTrigger;
        private readonly Func<Watermark, Task> _onWatermark;

        public NonParallelEgressVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions, Func<T, long, Task> recieveFunc, Func<ILockingEvent, Task> doCheckpoint, Action checkpointDone, Func<string, object?, Task> onTrigger, Func<Watermark, Task> onWatermark)
        {
            _executionDataflowBlockOptions = executionDataflowBlockOptions;
            _recieveFunc = recieveFunc;
            _doCheckpoint = doCheckpoint;
            _checkpointDone = checkpointDone;
            _onTrigger = onTrigger;
            _onWatermark = onWatermark;
            Initialize();
        }

        [MemberNotNull(nameof(_block))]
        internal void Initialize()
        {
            _block = new ActionBlock<IStreamEvent>((e) =>
            {
                if (e is ILockingEvent lockingEvent)
                {
                    return HandleLockingEvent(lockingEvent);
                }
                if (e is TriggerEvent triggerEvent)
                {
                    return _onTrigger(triggerEvent.Name, triggerEvent.State);
                }
                if (e is StreamMessage<T> msg)
                {
                    return _recieveFunc(msg.Data, msg.Time);
                }
                if (e is Watermark watermark)
                {
                    return _onWatermark(watermark);
                }
                return Task.CompletedTask;
            }, _executionDataflowBlockOptions);
        }

        private async Task HandleLockingEvent(ILockingEvent lockingEvent)
        {
            await _doCheckpoint(lockingEvent);
            _checkpointDone();
        }

        public Task Completion => _block.Completion;

        public long InputQueue => _block.InputCount;

        public long MaxInputQueue => _executionDataflowBlockOptions.BoundedCapacity;

        public void Complete()
        {
            _block.Complete();
        }

        public void Fault(Exception exception)
        {
            (_block as IDataflowBlock).Fault(exception);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            return (_block as ITargetBlock<IStreamEvent>).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }
    }
}
