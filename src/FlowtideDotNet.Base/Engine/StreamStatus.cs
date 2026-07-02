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
    /// Represents the fine-grained operational status of a Flowtide dataflow stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="StreamStatus"/> provides more granular operational information than
    /// <see cref="StreamStateValue"/>, which tracks the high-level lifecycle state machine position.
    /// Where <see cref="StreamStateValue"/> reflects the state machine transition (e.g. Starting,
    /// Running, Failure), <see cref="StreamStatus"/> reflects what the stream is actively doing
    /// at an operational level, including transient states such as pausing and graceful stopping.
    /// </para>
    /// <para>
    /// The current status is exposed via <see cref="DataflowStream.Status"/> and is also used to
    /// derive the <see cref="FlowtideHealth"/> value reported by <see cref="DataflowStream.Health"/>
    /// and the <c>flowtide_status</c> metric gauge. Both <see cref="StreamStatus"/> and
    /// <see cref="FlowtideHealth"/> are published as observable gauges on the
    /// <c>flowtide.{streamName}</c> meter for integration with external monitoring systems.
    /// </para>
    /// </remarks>
    public enum StreamStatus
    {
        /// <summary>
        /// The stream has not yet been started or has completed a graceful stop.
        /// This is the initial status before <see cref="DataflowStream.StartAsync"/> is called
        /// and the terminal status after a graceful stop sequence completes.
        /// Maps to <see cref="FlowtideHealth.Unhealthy"/>.
        /// </summary>
        Stopped,

        /// <summary>
        /// The stream is initializing its dataflow blocks, linking vertices, and restoring
        /// operator state from the last committed checkpoint.
        /// Set during the <c>StartStreamState</c> transition.
        /// Maps to <see cref="FlowtideHealth.Degraded"/>.
        /// </summary>
        Starting,

        /// <summary>
        /// The stream is fully initialized and is actively processing data through its operator graph.
        /// Set when the <c>RunningStreamState</c> transition completes successfully.
        /// Maps to <see cref="FlowtideHealth.Healthy"/>.
        /// </summary>
        Running,

        /// <summary>
        /// The stream is operational but experiencing reduced capacity or a transient non-fatal condition,
        /// for example database locks from sources.
        /// Maps to <see cref="FlowtideHealth.Degraded"/>.
        /// </summary>
        Degraded,

        /// <summary>
        /// An unhandled exception has occurred. The stream is stopping all running blocks, rolling back
        /// to the last committed checkpoint, and preparing to restart automatically.
        /// Set during <c>FailureStreamState</c> initialization.
        /// Maps to <see cref="FlowtideHealth.Unhealthy"/>.
        /// </summary>
        Failing,

        /// <summary>
        /// The stream is executing a graceful shutdown sequence, taking a final
        /// <see cref="StopStreamCheckpoint"/> across all egress vertices before completing.
        /// Set during <c>StoppingStreamState</c> initialization.
        /// Maps to <see cref="FlowtideHealth.Unhealthy"/>.
        /// </summary>
        Stopping,

        /// <summary>
        /// The stream is permanently removing all operator state from persistent storage.
        /// Set during <c>DeletingStreamState</c> initialization.
        /// Maps to <see cref="FlowtideHealth.Unhealthy"/>.
        /// </summary>
        Deleting,

        /// <summary>
        /// All persistent state associated with the stream has been successfully deleted.
        /// Set when <c>DeletedStreamState</c> initialization completes.
        /// Maps to <see cref="FlowtideHealth.Unhealthy"/>.
        /// </summary>
        Deleted,

        /// <summary>
        /// Message processing is temporarily suspended via <see cref="DataflowStream.Pause"/>.
        /// Data continues to queue in operator buffers but is not processed until
        /// <see cref="DataflowStream.Resume"/> is called.
        /// Maps to <see cref="FlowtideHealth.Healthy"/> when the underlying
        /// <see cref="StreamStateValue"/> is <see cref="StreamStateValue.Running"/>,
        /// <see cref="FlowtideHealth.Unhealthy"/> when it is <see cref="StreamStateValue.Failure"/>,
        /// and <see cref="FlowtideHealth.Degraded"/> otherwise.
        /// </summary>
        Paused
    }
}
