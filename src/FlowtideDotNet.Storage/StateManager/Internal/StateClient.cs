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
namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal abstract class StateClient : IDisposable
    {
        public abstract void Dispose();
        internal virtual Task DisposalTask => Task.CompletedTask;
        public abstract ValueTask Reset(bool clearMetadata);
        public abstract long MetadataId { get; }
        internal virtual bool HasCommitInFlight => false;
        internal virtual bool HasCommitFault => false;
        internal virtual Exception? CommitFault => null;

        /// <summary>
        /// Forgets a failed commit, called by recovery once it has reset this client.
        /// </summary>
        internal virtual void ClearCommitFault()
        {
        }

        /// <summary>
        /// The storage was wiped, no checkpoint holds this client's metadata any more.
        /// </summary>
        internal virtual void ForgetCheckpointedMetadata()
        {
        }

        /// <summary>
        /// Blocks this client's commits until ResumeCommits, joining an in-flight one for at most
        /// the given time first. Recovery holds it across the whole reset.
        /// </summary>
        internal virtual Task PauseCommitsAsync(TimeSpan walkTimeout)
        {
            return Task.CompletedTask;
        }

        internal virtual void ResumeCommits()
        {
        }

        /// <summary>
        /// Joins the actual worker, including session commit and temporary-allocation cleanup.
        /// Faulted when that commit failed. This does not establish checkpoint durability.
        /// </summary>
        internal virtual Task WaitForCommitAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Tells a walk in flight to give up at its next page, without waiting.
        /// </summary>
        internal virtual void RequestStopCommits()
        {
        }
    }
}
