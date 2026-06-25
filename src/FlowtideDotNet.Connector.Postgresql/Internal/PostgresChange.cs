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

using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    internal enum PostgresChangeKind
    {
        Insert,
        Update,
        Delete
    }

    /// <summary>
    /// A single decoded logical replication change, expressed in the read relation's schema column order.
    /// Built on the replication thread using plain CLR values so that all Flowtide column allocation happens
    /// later on the operator thread when the change is converted into a batch.
    /// </summary>
    internal sealed class PostgresChange
    {
        public required PostgresChangeKind Kind { get; init; }

        /// <summary>
        /// Values for the new tuple (insert/update) or the key of the deleted row (delete), in schema column order.
        /// Non-key columns of a delete are null. A <see cref="PostgresUtils.UnchangedToast"/> marker means the
        /// value was omitted from the WAL because it did not change and must be backfilled.
        /// </summary>
        public required object?[] Values { get; init; }

        /// <summary>
        /// For an update where one or more key columns changed, holds the previous key values (schema order, key
        /// positions only). Null otherwise.
        /// </summary>
        public object?[]? OldKeyValues { get; init; }

        public ulong Lsn { get; init; }
    }

    /// <summary>
    /// Everything a change source needs to attach to a table, supplied by the operator once it has resolved keys.
    /// </summary>
    internal sealed class PostgresChangeSourceContext
    {
        public required string StreamName { get; init; }

        public required string Schema { get; init; }

        public required string Table { get; init; }

        public required IReadOnlyList<string> SchemaNames { get; init; }

        public required IReadOnlyList<int> KeySchemaIndices { get; init; }

        public required Func<Exception, Task> FaultHandler { get; init; }
    }

    /// <summary>
    /// Information required by the initial snapshot read so it lines up exactly with the start of the replication stream.
    /// </summary>
    internal sealed class PostgresSnapshotInfo
    {
        public required string SnapshotName { get; init; }

        public required ulong ConsistentLsn { get; init; }
    }

    /// <summary>
    /// Abstraction over where a table's changes come from. In <see cref="PostgresReplicationMode.PerTable"/> mode this
    /// owns its own slot and connection; in <see cref="PostgresReplicationMode.Shared"/> mode it is a view over a
    /// per-table channel fed by the shared reader. The operator code is identical for both.
    /// </summary>
    internal interface IPostgresChangeSource : IAsyncDisposable
    {
        /// <summary>
        /// Prepares the source. Returns the snapshot to use for the initial full load, or null if no fresh snapshot is
        /// available (e.g. a re-snapshot after a reconnect, which reads current data instead).
        /// </summary>
        Task<PostgresSnapshotInfo?> InitializeAsync(CancellationToken ct);

        /// <summary>
        /// Signals that this table's initial snapshot read has completed. The shared reader will not begin streaming
        /// until every attached table has reported snapshot completion.
        /// </summary>
        Task SnapshotCompleteAsync(CancellationToken ct);

        /// <summary>
        /// Reads the next buffered change without blocking. Returns false when nothing is currently available.
        /// </summary>
        bool TryRead([NotNullWhen(true)] out PostgresChange? change);

        /// <summary>
        /// Acknowledges that changes up to and including <paramref name="lsn"/> have been processed so the server can
        /// release WAL during the session.
        /// </summary>
        void Acknowledge(ulong lsn);

        /// <summary>
        /// Set when a change could not be applied incrementally (e.g. an unresolvable unchanged-TOAST value or a
        /// decode error) and the table should be re-snapshotted. Cleared by the operator once it has scheduled a reload.
        /// </summary>
        bool NeedsResnapshot { get; }

        void ClearResnapshot();
    }
}
