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
    /// Implemented by vertices that connect this stream to other streams, tells the engine
    /// when every connected stream is durable at a version.
    /// </summary>
    internal interface IStreamVersionAgreement
    {
        // Release waits on transport notifications/handshakes when the engine fences a run.
        // These waits own no storage I/O; durability sends retain their actual-call budget.
        void AbortPendingOperations() { }

        /// <summary>
        /// Called at every start once the state is restored, before any vertex initializes: joins the recovery the start is in.
        /// </summary>
        void ResetAgreement();

        /// <summary>
        /// Called once every vertex of this stream initialized at the restore version.
        /// </summary>
        void AnnounceInitialized(long restoreVersion);

        /// <summary>
        /// Completes once every connected stream is durable at the version or later.
        /// </summary>
        Task WhenVersionAgreed(long version, CancellationToken cancellationToken);

        /// <summary>
        /// True if every connected stream is already known to be durable at the version.
        /// </summary>
        bool IsVersionAgreed(long version);

        /// <summary>
        /// Completes with the version every connected stream has once all of them announced theirs, null without connected streams.
        /// </summary>
        Task<long?> WhenGroupVersionKnown(CancellationToken cancellationToken);

        /// <summary>
        /// Completes once every directly connected stream started its run at the group's version, nobody next to this stream still comes down.
        /// </summary>
        Task WhenGroupSettled(CancellationToken cancellationToken);

        /// <summary>
        /// This stream restarts at the group's version, which is below its own: the group re-initializes with it.
        /// </summary>
        void ComingDownTo(long groupVersion);

        /// <summary>
        /// The start reached running, the next failure is a new recovery.
        /// </summary>
        void StartCompleted();

        /// <summary>
        /// The run ended in a stop, the next start handshakes again.
        /// </summary>
        void StreamStopped();
    }
}
