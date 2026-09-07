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
        public abstract ValueTask Reset(bool clearMetadata);
        public abstract long MetadataId { get; }

        /// <summary>
        /// Blocks this client's commits until ResumeCommits, draining an in-flight one first.
        /// Recovery holds it across the whole reset, a commit overlapping the revert would
        /// persist aborted-epoch pages into the recovered store.
        /// Base is a no-op for clients whose commits do not race recovery.
        /// </summary>
        internal virtual Task PauseCommitsAsync()
        {
            return Task.CompletedTask;
        }

        internal virtual void ResumeCommits()
        {
        }
    }
}
