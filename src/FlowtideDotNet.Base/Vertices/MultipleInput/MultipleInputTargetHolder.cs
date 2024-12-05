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

using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.MultipleInput
{
    public class MultipleInputTargetHolder : ITargetBlock<IStreamEvent>
    {
        private readonly int targetId;
        private readonly DataflowBlockOptions blockOptions;
        private MultipleInputTarget<IStreamEvent>? target;

        private string? _operatorName;
        public string OperatorName => _operatorName ?? throw new InvalidOperationException("OperatorName can only be fetched after setup.");

        public MultipleInputTargetHolder(int targetId, DataflowBlockOptions blockOptions)
        {
            this.targetId = targetId;
            this.blockOptions = blockOptions;
        }

        internal void Initialize()
        {
            target = new MultipleInputTarget<IStreamEvent>(targetId, blockOptions);
        }

        internal void LinkTo(ITargetBlock<KeyValuePair<int, IStreamEvent>> sourceBlock, DataflowLinkOptions dataflowLinkOptions)
        {
            Debug.Assert(target != null, nameof(target));
            target.LinkTo(sourceBlock, dataflowLinkOptions);
        }

        internal void ReleaseCheckpoint()
        {
            Debug.Assert(target != null, nameof(target));
            target.ReleaseCheckpoint();
        }

        public Task Completion => target?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after setup.");

        public void Complete()
        {
            Debug.Assert(target != null, nameof(target));
            target.Complete();
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(target != null, nameof(target));
            (target as IDataflowBlock).Fault(exception);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            Debug.Assert(target != null, nameof(target));
            return (target as ITargetBlock<IStreamEvent>).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        internal void Setup(string operatorName)
        {
            _operatorName = operatorName;
        }
    }
}
