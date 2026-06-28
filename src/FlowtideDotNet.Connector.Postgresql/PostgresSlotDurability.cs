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

namespace FlowtideDotNet.Connector.PostgreSQL
{
    /// <summary>
    /// Controls the durability of the replication slot, trading off restart cost against operational cleanliness.
    /// </summary>
    public enum PostgresSlotDurability
    {
        /// <summary>
        /// A temporary slot that PostgreSQL drops automatically when the connection closes. Nothing is ever left
        /// pinned on the server, but a restart cannot resume the slot, so the table is re-snapshotted on every restart.
        /// This is the default.
        /// </summary>
        Temporary = 0,

        /// <summary>
        /// A named, permanent slot that survives restarts, so the stream resumes from the last durably-checkpointed
        /// position instead of re-snapshotting the whole table. The slot is <b>not</b> dropped automatically — it must
        /// be removed explicitly when the stream is permanently retired, otherwise it retains WAL on the server.
        /// Supported in both <see cref="PostgresReplicationMode.PerTable"/> and <see cref="PostgresReplicationMode.Shared"/>
        /// mode; in shared mode the single slot's confirmed position only advances to the minimum all of its tables have
        /// durably checkpointed.
        /// </summary>
        Persistent = 1
    }
}
