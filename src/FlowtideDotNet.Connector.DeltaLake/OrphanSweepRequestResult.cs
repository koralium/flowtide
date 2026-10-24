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

namespace FlowtideDotNet.Connector.DeltaLake
{
    /// <summary>
    /// What a request for an orphan file sweep did.
    /// </summary>
    public enum OrphanSweepRequestResult
    {
        /// <summary>
        /// The running sink took the request; its next orphan sweep to conclude settles it, also one that was already running, and a restart before then drops it.
        /// </summary>
        Scheduled,

        /// <summary>
        /// An earlier request is not settled yet, and this one joins it.
        /// </summary>
        AlreadyPending,

        /// <summary>
        /// No running sink in this process sweeps the table: history retention is off, the storage is not qualified for sweeping, or the table is not maintained here.
        /// </summary>
        Unavailable
    }
}
