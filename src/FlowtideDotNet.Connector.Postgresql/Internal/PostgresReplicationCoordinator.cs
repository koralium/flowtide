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

using Npgsql;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// Owns the shared replication readers for a PostgreSQL source registration. Readers are keyed by
    /// (stream, database) - and because one OS process is one node, that key is also per-node. The coordinator
    /// is wired in by the factory but never owns a live connection itself: the reader is created lazily by the first
    /// table that attaches and disposed when the last one detaches.
    /// </summary>
    internal sealed class PostgresReplicationCoordinator
    {
        private static readonly ConcurrentDictionary<(string stream, string db), SharedReplicationReader> Readers = new();

        private readonly PostgresSourceOptions _options;
        private readonly object _membershipLock = new();
        // One entry per source operator (a self-join registers the same table more than once, and each occurrence is its
        // own operator that must be waited for before streaming starts).
        private readonly List<(string schema, string table)> _registrations = new();
        private readonly string _databaseId;

        public PostgresReplicationCoordinator(PostgresSourceOptions options)
        {
            _options = options;
            var builder = new NpgsqlConnectionStringBuilder(options.ConnectionStringFunc());
            _databaseId = $"{builder.Host}:{builder.Port}/{builder.Database}";
        }

        /// <summary>
        /// Records that a table is part of this stream. Called at plan time for every source so the reader can build a
        /// single publication and know how many tables to wait for before streaming.
        /// </summary>
        public void RegisterTable(string schema, string table)
        {
            lock (_membershipLock)
            {
                _registrations.Add((schema, table));
            }
        }

        public IPostgresChangeSource CreateChangeSource(PostgresChangeSourceContext context)
        {
            return new SharedChangeSource(this, context);
        }

        public SharedReplicationReader GetOrCreateReader(string streamName)
        {
            List<(string schema, string table)> membership;
            int sourceCount;
            lock (_membershipLock)
            {
                // Distinct tables drive the single publication; the raw registration count is how many source operators
                // the reader must wait to be ready before streaming (a self-join contributes two for one table).
                membership = _registrations.Distinct().ToList();
                sourceCount = _registrations.Count;
            }

            return Readers.GetOrAdd((streamName, _databaseId), key => new SharedReplicationReader(_options, key.stream, key.db, membership, sourceCount));
        }

        internal static void RemoveReader(string streamName, string databaseId)
        {
            Readers.TryRemove((streamName, databaseId), out _);
        }

        internal string DatabaseId => _databaseId;
    }
}
