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
    /// Defines a receiver for checkpoint completion notifications raised by the stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementations are registered on a <see cref="DataflowStreamBuilder"/> via
    /// <see cref="DataflowStreamBuilder.AddCheckpointListener"/> and are invoked each time the stream
    /// successfully completes a full checkpoint boundary. Multiple listeners can be registered;
    /// each is called in registration order.
    /// </para>
    /// <para>
    /// <see cref="OnCheckpointComplete"/> is called after all egress vertices have signalled their
    /// checkpoint is done and after the state manager has durably written the checkpoint to persistent
    /// storage, but before ingress vertices receive their <c>CheckpointDone</c> callback.
    /// This makes it the correct integration point for any external system that must be notified
    /// only once stream state is safely committed — for example to advance a consumer offset, trigger
    /// a dependent workflow, or record a checkpoint metric.
    /// </para>
    /// <para>
    /// Exceptions thrown by an implementation are swallowed by the stream engine to ensure
    /// that a misbehaving listener cannot disrupt stream processing or block the checkpoint sequence.
    /// </para>
    /// </remarks>
    public interface ICheckpointListener
    {
        /// <summary>
        /// Called when the stream has successfully completed a full checkpoint and durably written
        /// all operator state to persistent storage.
        /// </summary>
        /// <remarks>
        /// This method is invoked synchronously within the checkpoint completion task.
        /// Implementations should avoid long-running or blocking operations; if coordination with
        /// an external system is required, consider starting a background task rather than
        /// blocking the checkpoint completion path.
        /// Access <see cref="StreamCheckpointNotification.StreamName"/> to identify the stream
        /// that completed the checkpoint.
        /// </remarks>
        /// <param name="notification">
        /// A <see cref="StreamCheckpointNotification"/> carrying the name of the stream that
        /// completed the checkpoint boundary.
        /// </param>
        void OnCheckpointComplete(StreamCheckpointNotification notification);
    }
}
