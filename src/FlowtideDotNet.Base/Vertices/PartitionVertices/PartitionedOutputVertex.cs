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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices
{
    /// <summary>
    /// Represents a specialized vertex that gathers data from multiple upstream partitions and merges them back into a single continuous stream.
    /// </summary>
    /// <typeparam name="T">The type of data elements being merged.</typeparam>
    /// <remarks>
    /// This is an implementation of a <see cref="MultipleInputVertex{T}"/> designed specifically to act as a funnel 
    /// for partitioned workflows. It aligns stream barriers (like watermarks and checkpoints) across all incoming 
    /// branches, ensuring downstream operators receive synchronized state markers and a single stream of mixed <typeparamref name="T"/> records.
    /// </remarks>
    public class PartitionedOutputVertex<T> : MultipleInputVertex<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionedOutputVertex{T}"/> class.
        /// </summary>
        /// <param name="targetCount">The number of upstream partitions feeding into this vertex.</param>
        /// <param name="executionDataflowBlockOptions">Options defining block capacity and execution settings.</param>
        public PartitionedOutputVertex(int targetCount, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(targetCount, executionDataflowBlockOptions)
        {
        }

        /// <summary>
        /// Gets the display name of this operator type, typically used in logging and graphical graph representations.
        /// </summary>
        public override string DisplayName => "Partitioned Output";

        /// <summary>
        /// Represents a request to compact local storage space. As this vertex does not hold persistent records, it runs a completed task.
        /// </summary>
        /// <returns>A completed task.</returns>
        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Initiates the deletion process for cleaning up persistent storage data associated with this vertex if one existed.
        /// </summary>
        /// <returns>A completed task.</returns>
        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Hook activated when a checkpoint event is triggered on the vertex. 
        /// No specific action is required here since this node does not keep persistent state.
        /// </summary>
        /// <returns>A completed task.</returns>
        public override Task OnCheckpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Receives data synchronously from one of the attached upstream partitions and forwards it directly toward the output queue.
        /// </summary>
        /// <param name="targetId">The identifier representing which partition the data is arriving from.</param>
        /// <param name="msg">The typed data payload being handled.</param>
        /// <param name="time">The stream logical execution time metadata context.</param>
        /// <returns>An asynchronous stream that yields the forwarded message.</returns>
        public override IAsyncEnumerable<T> OnRecieve(int targetId, T msg, long time)
        {
            return new SingleAsyncEnumerable<T>(msg);
        }

        /// <summary>
        /// Handles the basic setup and state client initialization logic upon startup or recovery operations.
        /// </summary>
        /// <param name="stateManagerClient">The utility client for loading or saving persistent metrics or internal values.</param>
        /// <returns>A completed task.</returns>
        protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
