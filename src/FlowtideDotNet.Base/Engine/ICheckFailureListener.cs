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
    /// Receives check issues of committed state, changes per checkpoint and a snapshot at every start.
    /// </summary>
    public interface ICheckFailureListener
    {
        /// <summary>
        /// Called when an issue becomes active.
        /// </summary>
        void OnCheckFailure(ref readonly CheckFailureNotification notification);

        /// <summary>
        /// Called when an active issue is no longer active.
        /// </summary>
        void OnCheckResolved(ref readonly CheckFailureNotification notification);

        /// <summary>
        /// Called to forget every issue of the check, its active issues follow as failures.
        /// </summary>
        void OnCheckReset(ref readonly CheckResetNotification notification);
    }
}
