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

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Represents a checkpoint event within the dataflow stream.
    /// </summary>
    /// <remarks>
    /// Checkpoint events are propagated through the stream to coordinate state persistence and synchronization 
    /// across different vertices and operators. This ensures fault tolerance and data consistency.
    /// </remarks>
    public class Checkpoint : ICheckpointEvent
    {
        /// <summary>
        /// Gets the time of the checkpoint being finalized.
        /// </summary>
        public long CheckpointTime { get; }

        /// <summary>
        /// Gets the time of the newly created checkpoint span.
        /// </summary>
        public long NewTime { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Checkpoint"/> class.
        /// </summary>
        /// <param name="checkpointTime">The time of the checkpoint being finalized.</param>
        /// <param name="newTime">The time for the newly created checkpoint span.</param>
        public Checkpoint(long checkpointTime, long newTime)
        {
            CheckpointTime = checkpointTime;
            NewTime = newTime;
        }
    }
}
