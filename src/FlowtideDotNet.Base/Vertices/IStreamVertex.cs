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

using FlowtideDotNet.Storage;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices
{
    /// <summary>
    /// Represents a foundational node (operator or vertex) within the Flowtide dataflow stream. 
    /// Defines lifecycle methods (Setup, Initialize, CreateBlock, Link), operational hooks (Pause, Resume, Compact), 
    /// and core metadata properties.
    /// </summary>
    public interface IStreamVertex : IAsyncDisposable
    {
        /// <summary>
        /// Gets a task that represents the asynchronous completion of the vertex's underlying dataflow block.
        /// </summary>
        Task Completion { get; }

        /// <summary>
        /// Performs initial naming configuration before the vertex is fully initialized.
        /// </summary>
        /// <param name="streamName">The overarching name of the stream.</param>
        /// <param name="operatorName">The unique name for this operator within the stream.</param>
        void Setup(string streamName, string operatorName);

        /// <summary>
        /// Gets the unique configured operator name of the vertex.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the display name of the vertex type, typically used for logging and metrics visualization.
        /// </summary>
        string DisplayName { get; }

        /// <summary>
        /// Initializes the vertex, restoring state if necessary, and setting up metrics and handlers.
        /// </summary>
        /// <param name="name">The name of the vertex.</param>
        /// <param name="restoreTime">The time to restore state from if recovering.</param>
        /// <param name="newTime">The new logical time for the stream.</param>
        /// <param name="vertexHandler">The handler providing state clients, memory managers, and metrics.</param>
        /// <param name="streamVersionInformation">Optional version information to handle upgrades/downgrades.</param>
        Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation);

        /// <summary>
        /// Instantiates the internal <see cref="System.Threading.Tasks.Dataflow"/> blocks and internal processing logic.
        /// </summary>
        void CreateBlock();

        /// <summary>
        /// Creates the actual links between the configured inputs and outputs of this specific block.
        /// </summary>
        void Link();

        /// <summary>
        /// Explicitly transitions the underlying dataflow block into a faulted state due to an error.
        /// </summary>
        /// <param name="exception">The exception that caused the fault.</param>
        void Fault(Exception exception);

        /// <summary>
        /// Requests the vertex to compact its persistent state, if applicable, to save space.
        /// </summary>
        Task Compact();

        /// <summary>
        /// Signals to the vertex that it should not accept any more messages, and to finish processing its current workload.
        /// </summary>
        void Complete();

        /// <summary>
        /// Queues a trigger event into the vertex to be processed at the earliest convenience. 
        /// Usually driven by external schedules or signals.
        /// </summary>
        /// <param name="triggerEvent">The trigger event to queue.</param>
        Task QueueTrigger(TriggerEvent triggerEvent);

        /// <summary>
        /// Deletes all underlying persistent state and metadata associated with this vertex permanently.
        /// </summary>
        Task DeleteAsync();

        /// <summary>
        /// Gets an enumeration of all the dataflow targets this vertex is linked to.
        /// </summary>
        IEnumerable<ITargetBlock<IStreamEvent>> GetLinks();

        /// <summary>
        /// Pauses processing within the vertex. Data will queue up but not be emitted.
        /// </summary>
        void Pause();

        /// <summary>
        /// Resumes processing within the vertex, unblocking pending items.
        /// </summary>
        void Resume();

        /// <summary>
        /// This method is called directly before saving persistent data checkpoint.
        /// This method should be used sparingly since the vertex might have handled data
        /// that is after the checkpoint, so the vertex need to take that into consideration.
        /// 
        /// One use case is if a source has an offset (such as Kafka), and only a subset of events are in the result set.
        /// If no new event has been sent into the stream after the checkpoint event, this method can be used to update the offset
        /// to the latest one to skip loading in unnecessary data in the case of a crash.
        /// </summary>
        /// <returns>A task that represents the asynchronous pre-checkpoint operation.</returns>
        Task BeforeSaveCheckpoint();

        /// <summary>
        /// Invoked when a failure has occurred in the stream engine and the system is rolling back to a previous checkpoint.
        /// </summary>
        /// <param name="rollbackVersion">The specific version the state is rolling back to.</param>
        Task OnFailure(long rollbackVersion);
    }
}
