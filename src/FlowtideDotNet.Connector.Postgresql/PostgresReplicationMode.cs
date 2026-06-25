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
    /// Controls how the PostgreSQL source consumes logical replication for the tables in a stream.
    /// </summary>
    public enum PostgresReplicationMode
    {
        /// <summary>
        /// Each source table opens its own temporary replication slot and replication connection.
        /// Simplest and most isolated: a failure or slow downstream on one table does not affect others,
        /// and adding/removing a table only affects that table. The trade-off is that PostgreSQL decodes
        /// the WAL once per slot, so this scales poorly when many tables are read from the same database.
        /// </summary>
        PerTable = 0,

        /// <summary>
        /// All source tables for the same (stream, database) share a single temporary replication slot and
        /// a single replication connection, demultiplexed to each table by relation. PostgreSQL decodes the
        /// WAL only once regardless of how many tables are read, at the cost of head-of-line blocking
        /// (a stalled table stalls the shared stream) and a single re-snapshot of all tables on reconnect.
        /// </summary>
        Shared = 1
    }
}
