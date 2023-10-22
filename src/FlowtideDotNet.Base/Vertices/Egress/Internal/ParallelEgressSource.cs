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

using DataflowStream.dataflow.Internal;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Egress.Internal
{
    internal class ParallelEgressSource<T> : ITargetBlock<T>
    {
        private readonly object _checkpointLock = new object();
        private readonly Func<ILockingEvent, Task> _onCheckpoint;
        private readonly Action _checkpointDone;

        public ParallelEgressSource(Func<ILockingEvent, Task> onCheckpoint, Action checkpointDone)
        {
            _onCheckpoint = onCheckpoint;
            _checkpointDone = checkpointDone;
        }

        public Task Completion => throw new NotImplementedException();

        public void Complete()
        {
        }

        public void Fault(Exception exception)
        {
        }

        private void ProcessCheckpoint(T checkpointEvent)
        {
            Task.Factory.StartNew(state =>
            {
                var thisBufferBlock = (ParallelEgressSource<T>)state!;
                if (checkpointEvent is ILockingEvent checkpoint)
                {
                    return thisBufferBlock._onCheckpoint(checkpoint);
                }
                return Task.CompletedTask;
            }, this, CancellationToken.None, Common.GetCreationOptionsForTask(), TaskScheduler.Default)
                .Unwrap()
                .ContinueWith((task, state) =>
                {
                    var thisBlock = (ParallelEgressSource<T>)state!;
                    thisBlock._checkpointDone();
                }, this);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
        {
            lock (_checkpointLock)
            {
                if (messageValue is ILockingEvent)
                {
                    ProcessCheckpoint(messageValue);
                    return DataflowMessageStatus.Accepted;
                }
            }
            return DataflowMessageStatus.Declined;
        }
    }
}
