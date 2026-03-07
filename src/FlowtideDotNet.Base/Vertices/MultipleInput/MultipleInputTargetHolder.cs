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
    /// <summary>
    /// Holder that represents one input slot of a <see cref="MultipleInputVertex{T}"/>,
    /// implementing <see cref="ITargetBlock{IStreamEvent}"/> so that upstream sources can be linked directly to it.
    /// </summary>
    /// <remarks>
    /// Each instance wraps a lazily initialized <see cref="MultipleInputTarget{T}"/> and is identified by a zero-based
    /// <c>targetId</c> that determines which input index is being fed when messages arrive at the parent vertex.
    /// The internal <see cref="MultipleInputTarget{T}"/> is created during the vertex setup phase via
    /// <c>Initialize</c>, and the holder must be fully set up (via <c>Setup</c>) before the
    /// <see cref="OperatorName"/> and <see cref="Completion"/> members are accessible.
    /// Upstream dataflow blocks are connected to this holder using standard TPL Dataflow link mechanics,
    /// which the holder forwards to the underlying target block.
    /// </remarks>
    public class MultipleInputTargetHolder : ITargetBlock<IStreamEvent>
    {
        private readonly int targetId;
        private readonly DataflowBlockOptions blockOptions;
        private MultipleInputTarget<IStreamEvent>? target;

        private string? _operatorName;

        /// <summary>
        /// Gets the name of the operator that owns this input holder.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before the holder has been configured via the internal <c>Setup</c> call.
        /// </exception>
        public string OperatorName => _operatorName ?? throw new InvalidOperationException("OperatorName can only be fetched after setup.");

        /// <summary>
        /// Initializes a new instance of <see cref="MultipleInputTargetHolder"/> for the given input index.
        /// </summary>
        /// <param name="targetId">
        /// The zero-based index identifying which input slot of the parent <see cref="MultipleInputVertex{T}"/> this holder represents.
        /// </param>
        /// <param name="blockOptions">
        /// The <see cref="DataflowBlockOptions"/> used to configure the underlying <see cref="MultipleInputTarget{T}"/>
        /// when it is initialized.
        /// </param>
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

        /// <summary>
        /// Gets a <see cref="Task"/> that represents the asynchronous operation and completion of this dataflow block.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when accessed before the holder has been initialized via the internal <c>Initialize</c> call.
        /// </exception>
        public Task Completion => target?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after setup.");

        /// <summary>
        /// Signals to this dataflow block that no more messages will be offered to it.
        /// </summary>
        public void Complete()
        {
            Debug.Assert(target != null, nameof(target));
            target.Complete();
        }

        /// <summary>
        /// Causes this dataflow block to complete in a faulted state.
        /// </summary>
        /// <param name="exception">The exception that caused the fault.</param>
        public void Fault(Exception exception)
        {
            Debug.Assert(target != null, nameof(target));
            (target as IDataflowBlock).Fault(exception);
        }

        /// <summary>
        /// Offers a message to this dataflow block, giving it the opportunity to accept or postpone the message.
        /// </summary>
        /// <param name="messageHeader">A <see cref="DataflowMessageHeader"/> instance that represents the header of the message being offered.</param>
        /// <param name="messageValue">The <see cref="IStreamEvent"/> being offered.</param>
        /// <param name="source">The <see cref="ISourceBlock{IStreamEvent}"/> offering the message, or <see langword="null"/> if there is no source.</param>
        /// <param name="consumeToAccept">
        /// <see langword="true"/> if the target must call
        /// <see cref="ISourceBlock{IStreamEvent}.ConsumeMessage"/> to consume the message; <see langword="false"/> if the message is consumed implicitly on acceptance.
        /// </param>
        /// <returns>
        /// A <see cref="DataflowMessageStatus"/> value indicating whether the message was accepted, declined, or postponed.
        /// </returns>
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
