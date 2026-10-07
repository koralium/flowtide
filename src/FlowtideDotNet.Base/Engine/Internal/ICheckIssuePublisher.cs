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

namespace FlowtideDotNet.Base.Engine.Internal
{
    /// <summary>
    /// Holds sealed check issue batches until their checkpoint is committed, thread-safe.
    /// </summary>
    internal interface ICheckIssuePublisher
    {
        /// <summary>
        /// True when a check failure listener is registered.
        /// </summary>
        bool IssuesEnabled { get; }

        /// <summary>
        /// True when a check status listener is registered.
        /// </summary>
        bool StatusEnabled { get; }

        /// <summary>
        /// Queues a batch sealed at a checkpoint barrier, dropped when no listener can receive it.
        /// </summary>
        void Enqueue(CheckIssueBatch batch);

        /// <summary>
        /// Publishes a batch of already committed state right away.
        /// </summary>
        void Publish(CheckIssueBatch batch);

        /// <summary>
        /// Drops every unpublished batch of the check, called at operator (re)initialisation.
        /// </summary>
        void DiscardPending(string checkId);
    }
}
