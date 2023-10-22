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

using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.dataflow
{
    /// <summary>
    /// An implementation of broadcast block that does not drop messages when under backpressure.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class GuaranteedBroadcastBlock<T> : IPropagatorBlock<T, T>
    {
        private readonly TransformManyBlock<T, KeyValuePair<int, T>> multiplierBlock;
        private readonly ExecutionDataflowBlockOptions executionDataflowBlockOptions;
        private readonly List<TransformBlock<KeyValuePair<int, T>, T>> _linkBlocks = new List<TransformBlock<KeyValuePair<int, T>, T>>();
        private readonly HashSet<ITargetBlock<T>> _targets = new HashSet<ITargetBlock<T>>();

        public GuaranteedBroadcastBlock(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            multiplierBlock = new TransformManyBlock<T, KeyValuePair<int, T>>(MultiplyFunction, executionDataflowBlockOptions);
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
        }

        private IEnumerable<KeyValuePair<int, T>> MultiplyFunction(T input)
        {
            var arr = new KeyValuePair<int, T>[_linkBlocks.Count];
            for (int i = 0; i < _linkBlocks.Count; i++)
            {
                arr[i] = new KeyValuePair<int, T>(i, input);
            }
            return arr;
        }

        public Task Completion => multiplierBlock.Completion;

        public void Complete()
        {
            multiplierBlock.Complete();
        }

        public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
        {
            throw new NotImplementedException();
        }

        public void Fault(Exception exception)
        {
            (multiplierBlock as IDataflowBlock).Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
        {
            if (_targets.Contains(target))
            {
                return default;
            }
            _targets.Add(target);
            int index = _linkBlocks.Count;
            var transformBlock = new TransformBlock<KeyValuePair<int, T>, T>(kvp => kvp.Value, executionDataflowBlockOptions);
            transformBlock.LinkTo(target, linkOptions);
            _linkBlocks.Add(transformBlock);
            multiplierBlock.LinkTo(transformBlock, new DataflowLinkOptions()
            {
                PropagateCompletion = true
            }, kvp => kvp.Key == index);
            return default;
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
        {
            return (multiplierBlock as ITargetBlock<T>).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
        {
            throw new NotImplementedException();
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
        {
            throw new NotImplementedException();
        }
    }
}
