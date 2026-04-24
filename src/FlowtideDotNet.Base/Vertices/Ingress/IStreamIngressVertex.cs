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
    /// Represents an ingress node (source operator) within the Flowtide dataflow stream.
    /// </summary>
    /// <remarks>
    /// This interface extends <see cref="IStreamVertex"/> to provide additional lifecycle methods 
    /// specific to bringing data into the stream, such as completing initialization and handling checkpoints.
    /// Implementations typically act as an <see cref="System.Threading.Tasks.Dataflow.ISourceBlock{TOutput}"/> 
    /// for <see cref="IStreamEvent"/> within the TPL Dataflow pipeline.
    /// </remarks>
    public interface IStreamIngressVertex : IStreamVertex
    {
        /// <summary>
        /// Called when the internal initialization sequence has completed successfully.
        /// </summary>
        /// <returns>A task that handles post-initialization tasks, such as triggering initial data emission.</returns>
        Task InitializationCompleted();

        internal void DoLockingEvent(ILockingEvent lockingEvent);

        /// <summary>
        /// Invoked when a checkpoint has successfully completed.
        /// </summary>
        /// <param name="checkpointVersion">The completed checkpoint version.</param>
        /// <returns>A task representing the completion callback operation.</returns>
        Task CheckpointDone(long checkpointVersion);

        internal void SetDependenciesDoneFunction(Action<string> dependenciesDone);
    }
}
