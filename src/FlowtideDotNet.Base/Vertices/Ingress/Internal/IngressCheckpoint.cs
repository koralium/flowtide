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

namespace FlowtideDotNet.Base.Vertices.Ingress.Internal
{
    /// <summary>
    /// Helper class that allows a checkpoint to be added directly to the bounding state
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class IngressCheckpoint<T> : ISourceBlock<T>
    {
        public T CheckpointEvent { get; }

        public IngressCheckpoint(T checkpointEvent)
        {
            CheckpointEvent = checkpointEvent;
        }

        public Task Completion => throw new NotImplementedException();

        public void Complete()
        {
            throw new NotImplementedException();
        }

        public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
        {
            throw new NotImplementedException();
        }

        public void Fault(Exception exception)
        {
            throw new NotImplementedException();
        }

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
        {
            throw new NotImplementedException();
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
