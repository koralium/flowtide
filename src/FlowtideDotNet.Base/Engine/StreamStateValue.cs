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

namespace FlowtideDotNet.Base.Engine
{
    /// <summary>
    /// Represents the high-level lifecycle state of a Flowtide dataflow stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="StreamStateValue"/> tracks the current position within the stream's lifecycle
    /// state machine, complementing the finer-grained <see cref="StreamStatus"/> which reflects
    /// the operational condition at a more detailed level. Where <see cref="StreamStatus"/> surfaces
    /// transient conditions such as pausing or degradation, <see cref="StreamStateValue"/> conveys
    /// the coarse lifecycle phase — for example whether the stream is starting up, running normally,
    /// recovering from a failure, or shutting down.
    /// </para>
    /// <para>
    /// The current value is exposed via <see cref="DataflowStream.State"/>. Components that need to
    /// react to lifecycle transitions can implement <see cref="IStreamStateChangeListener"/> and
    /// register via <see cref="DataflowStreamBuilder.AddStateChangeListener"/>. Each transition fires
    /// all registered listeners synchronously before the associated state machine work begins.
    /// </para>
    /// </remarks>
    public enum StreamStateValue
    {
        /// <summary>
        /// The stream has not been started, or has returned to an idle state after a graceful stop.
        /// This is the initial value assigned when a stream is built via <see cref="DataflowStreamBuilder"/>,
        /// and the state that the stopping sequence transitions back to once all blocks have been
        /// completed and disposed. <see cref="DataflowStream.RunAsync"/> exits when this state is reached.
        /// Corresponds to <see cref="StreamStatus.Stopped"/>.
        /// </summary>
        NotStarted = 0,

        /// <summary>
        /// The stream is actively initializing its operator graph, linking dataflow blocks, and
        /// restoring operator state from the last committed checkpoint. The stream remains in this
        /// state until all egress vertices have completed their initial watermark handshake, at which
        /// point it transitions to <see cref="Running"/>. Any unhandled exception during startup
        /// transitions the stream to <see cref="Failure"/>.
        /// Corresponds to <see cref="StreamStatus.Starting"/>.
        /// </summary>
        Starting = 1,

        /// <summary>
        /// The stream is fully initialized and is actively processing data through its operator graph.
        /// Triggers are active, checkpoints can be taken, and data flows through all connected vertices.
        /// The stream exits this state on a graceful stop request (<see cref="Stopping"/>), a delete
        /// request (<see cref="Deleting"/>), or an unhandled exception (<see cref="Failure"/>).
        /// Corresponds to <see cref="StreamStatus.Running"/>.
        /// </summary>
        Running = 2,

        /// <summary>
        /// An unhandled exception has occurred. All running blocks are stopped and disposed, the stream
        /// rolls back to the last committed checkpoint, and a restart is automatically scheduled by
        /// transitioning back to <see cref="Starting"/>. This enables automatic self-healing without
        /// external intervention.
        /// Corresponds to <see cref="StreamStatus.Failing"/>.
        /// </summary>
        Failure = 3,

        /// <summary>
        /// The stream is permanently removing all operator state and persisted data from storage. All
        /// dataflow blocks are completed, awaited, disposed, and individually deleted before the stream
        /// transitions to <see cref="Deleted"/>.
        /// Corresponds to <see cref="StreamStatus.Deleting"/>.
        /// </summary>
        Deleting = 4,

        /// <summary>
        /// All persistent state associated with the stream has been permanently removed from storage.
        /// This is a terminal state; <see cref="DataflowStream.StopAsync"/> throws
        /// <see cref="NotSupportedException"/> in this state.
        /// Corresponds to <see cref="StreamStatus.Deleted"/>.
        /// </summary>
        Deleted = 5,

        /// <summary>
        /// A graceful shutdown is in progress. The stream triggers a final checkpoint across all egress
        /// vertices, then completes and disposes all blocks before transitioning back to
        /// <see cref="NotStarted"/>. <see cref="DataflowStream.StopAsync"/> awaits this entire sequence
        /// before returning.
        /// Corresponds to <see cref="StreamStatus.Stopping"/>.
        /// </summary>
        Stopping = 6
    }
}
