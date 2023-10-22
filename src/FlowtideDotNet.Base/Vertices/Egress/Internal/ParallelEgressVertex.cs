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

using FlowtideDotNet.Base.Vertices.Unary;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Egress.Internal
{
    internal class ParallelEgressVertex<T> : IEgressImplementation
    {
        private TransformManyBlock<IStreamEvent, IStreamEvent>? _transformBlock;
        private ParallelUnaryTarget<IStreamEvent>? _target;
        private ParallelEgressSource<IStreamEvent> _source;
        private readonly ExecutionDataflowBlockOptions executionDataflowBlockOptions;
        private readonly Func<T, long, Task> _recieveFunc;
        private readonly Func<ILockingEvent, Task> _doCheckpoint;
        private readonly Action _checkpointDone;
        private readonly Func<string, object?, Task> _onTrigger;
        private readonly Func<Watermark, Task> _onWatermark;

        public ParallelEgressVertex(ExecutionDataflowBlockOptions executionDataflowBlockOptions, Func<T, long, Task> recieveFunc, Func<ILockingEvent, Task> doCheckpoint, Action checkpointDone, Func<string, object?, Task> onTrigger, Func<Watermark, Task> onWatermark)
        {
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
            _recieveFunc = recieveFunc;
            _doCheckpoint = doCheckpoint;
            _checkpointDone = checkpointDone;
            _onTrigger = onTrigger;
            _onWatermark = onWatermark;
            Initialize();
        }

        private void Initialize()
        {
            _transformBlock = new TransformManyBlock<IStreamEvent, IStreamEvent>((e) =>
            {
                if (e is ILockingEvent)
                {
                    return Passthrough(e);
                }
                if (e is TriggerEvent triggerEvent)
                {
                    _onTrigger(triggerEvent.Name, triggerEvent.State);
                    return Empty();
                }
                if (e is StreamMessage<T> msg)
                {
                    _recieveFunc(msg.Data, msg.Time);
                    return Empty();
                }
                if (e is Watermark watermark)
                {
                    _onWatermark(watermark);
                    return Empty();
                }
                throw new NotSupportedException();
            }, executionDataflowBlockOptions);

            _target = new ParallelUnaryTarget<IStreamEvent>(executionDataflowBlockOptions);
            _source = new ParallelEgressSource<IStreamEvent>(_doCheckpoint, HandleCheckpointDone);

            _target.LinkTo(_transformBlock, new DataflowLinkOptions()
            {
                PropagateCompletion = true
            });
            _transformBlock.LinkTo(_source);
        }

        private void HandleCheckpointDone()
        {
            Debug.Assert(_target != null, nameof(_target));
            // Release the lock on the target to allow it to start ingesting new data
            _target.ReleaseCheckpoint();
            _checkpointDone();
        }

        private async IAsyncEnumerable<IStreamEvent> Passthrough(IStreamEvent streamEvent)
        {
            yield return streamEvent;
        }

        private async IAsyncEnumerable<IStreamEvent> Empty()
        {
            yield break;
        }

        public Task Completion => _transformBlock?.Completion ?? throw new NotSupportedException("Must be initialized first");

        public long InputQueue => _transformBlock?.InputCount ?? throw new NotSupportedException("Must be initialized first");

        public long MaxInputQueue => executionDataflowBlockOptions.BoundedCapacity;

        public void Complete()
        {
            Debug.Assert(_target != null, nameof(_target));
            _target.Complete();
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_target != null, nameof(_target));
            (_target as IDataflowBlock).Fault(exception);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(_target != null, nameof(_target));
            return (_target as ITargetBlock<IStreamEvent>).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }
    }
}
