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
        /// <summary>
        /// Called before a start chooses its restore version, nothing is learned or claimed until the reset.
        /// </summary>
        void InvalidateAgreement();

        /// <summary>
        /// Called at every start once the state is restored, before any vertex initializes.
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
        /// The highest version this stream ever knew every connected stream to be durable at, -1 if none.
        /// </summary>
        long HighestKnownDurableVersion { get; }
    }
}
