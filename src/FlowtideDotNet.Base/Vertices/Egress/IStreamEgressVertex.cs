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

namespace FlowtideDotNet.Base.Vertices
{
    /// <summary>
    /// Represents an egress node (sink operator) within the Flowtide dataflow stream.
    /// </summary>
    public interface IStreamEgressVertex : IStreamVertex
    {
        internal void SetCheckpointDoneFunction(Action<string, ILockingEvent?> checkpointDone, Action<string, ILockingEvent?> dependenciesDone);

        /// <summary>
        /// Invoked when a checkpoint has successfully completed and its state is durable.
        /// </summary>
        /// <param name="checkpointVersion">
        /// The state manager version of the completed checkpoint, the same domain as the
        /// restore version the vertex receives at initialization.
        /// </param>
        Task CheckpointDone(long checkpointVersion);

        /// <summary>
        /// True when the vertex has everything it needs for the stream to finish stopping.
        /// Exchanges that send to other substreams return false until the other substream
        /// has fetched the stop barrier, so the stream does not dispose the exchanged events
        /// before the other substream has received them.
        /// </summary>
        bool ReadyToStop => true;
    }
}
