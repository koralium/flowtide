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
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// One shared replication slot + connection for all the tables of a (stream, database). Decodes the single WAL
    /// stream once and demultiplexes each change to the matching table's bounded channel. The slot is temporary or
    /// persistent per <see cref="PostgresSourceOptions.SlotDurability"/>.
    /// </summary>
    internal sealed class SharedReplicationReader
    {
        private sealed class SourceEntry
        {
            public required string OperatorName { get; init; }
            public required string Schema { get; init; }
            public required string Table { get; init; }
            public required ChannelWriter<PostgresChange> Writer { get; init; }
            public required IReadOnlyList<string> SchemaNames { get; init; }
            public required IReadOnlyList<int> KeyIndices { get; init; }
            public required SharedChangeSource Source { get; init; }
            public RelationColumnMap? Map { get; set; }

            public bool MatchesRelation(string ns, string name)
                => string.Equals(Schema, ns, StringComparison.Ordinal) && string.Equals(Table, name, StringComparison.Ordinal);
        }

        private readonly PostgresSourceOptions _options;
        private readonly bool _persistent;
        private readonly string _streamName;
        private readonly string _databaseId;
        private readonly List<(string schema, string table)> _membership;
        private readonly int _expectedSourceCount;
        private readonly SemaphoreSlim _initLock = new(1, 1);
        // Keyed by operator name, not (schema, table): several operators can read the same table (e.g. a self-join), each
        // needing its own channel. Routing fans a decoded change out to every entry matching the relation.
        private readonly ConcurrentDictionary<string, SourceEntry> _sources = new();
        // Last durably-checkpointed LSN reported by each operator; confirmed_flush can only advance to the minimum.
        private readonly ConcurrentDictionary<string, long> _durableLsns = new();
        private readonly CancellationTokenSource _cts = new();

        private bool _initialized;
        private int _attached;     // guarded by _initLock
        private bool _disposed;    // guarded by _initLock - reader is unusable for new attaches
        private bool _tornDown;    // guarded by _initLock - resources have been released
        private int _ready;        // guarded by _initLock
        private LogicalReplicationConnection? _connection;
        private PgOutputReplicationSlot? _slot;
        private string _publicationName = string.Empty;
        private string _slotName = string.Empty;
        private PostgresSnapshotInfo? _snapshot;
        private Task? _streamTask;
        private DateTime _lastStatusSent = DateTime.MinValue;

        public SharedReplicationReader(PostgresSourceOptions options, string streamName, string databaseId, List<(string schema, string table)> membership, int expectedSourceCount)
        {
            _options = options;
            _persistent = options.SlotDurability == PostgresSlotDurability.Persistent;
            _streamName = streamName;
            _databaseId = databaseId;
            _membership = membership;
            _expectedSourceCount = expectedSourceCount;
        }

        /// <summary>
        /// Attaches a table to the shared reader. Returns null if this reader is already tearing down (a rollback race),
        /// in which case the caller should obtain a fresh reader and retry.
        /// </summary>
        public async Task<(PostgresSnapshotInfo? snapshot, ChannelReader<PostgresChange> channel)?> AttachAsync(SharedChangeSource source, long resumeLsn, CancellationToken ct)
        {
            await _initLock.WaitAsync(ct);
            try
            {
                if (_disposed)
                {
                    return null;
                }
                // The reader's publication and ready-gate are built from the membership captured when it was created.
                // A table outside that membership can only mean a second source registration targets this same
                // (stream, database) - it would otherwise attach, snapshot, and then silently receive no live changes
                // (it is absent from the publication and miscounts the ready-gate). Fail loudly instead.
                if (!_membership.Contains((source.Schema, source.Table)))
                {
                    throw new InvalidOperationException(
                        $"Table {source.Schema}.{source.Table} is not part of the shared replication reader for stream '{_streamName}'. " +
                        "This happens when more than one AddPostgresSource registration targets the same database in a single stream; " +
                        "use a single registration per database when ReplicationMode is Shared.");
                }
                if (!_initialized)
                {
                    await InitializeAsync(resumeLsn, ct);
                    _initialized = true;
                }

                var channel = Channel.CreateBounded<PostgresChange>(new BoundedChannelOptions(_options.ChannelCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true
                });

                _sources[source.OperatorName] = new SourceEntry
                {
                    OperatorName = source.OperatorName,
                    Schema = source.Schema,
                    Table = source.Table,
                    Writer = channel.Writer,
                    SchemaNames = source.SchemaNames,
                    KeyIndices = source.KeySchemaIndices,
                    Source = source
                };
                // Seed the durable position with the restored LSN so a resume confirms no further than every operator's state.
                _durableLsns[source.OperatorName] = resumeLsn;
                _attached++;

                return (_snapshot, channel.Reader);
            }
            finally
            {
                _initLock.Release();
            }
        }

        /// <summary>
        /// Signals that a table is ready to stream (its snapshot finished, or it is resuming a persistent slot). Streaming
        /// begins once every table in the membership is ready, so no change is decoded before its channel exists.
        /// </summary>
        public async Task SignalReadyAsync(CancellationToken ct)
        {
            await _initLock.WaitAsync(ct);
            try
            {
                // Start the single streaming loop once every table is ready. Publishing _streamTask under _initLock -
                // the same lock DetachAsync takes to read it - guarantees a concurrent teardown observes the started
                // task and awaits it before disposing the connection. Skip if the reader is already torn down.
                if (++_ready >= _expectedSourceCount && _streamTask == null && !_disposed)
                {
                    _streamTask = Task.Run(() => StreamLoopAsync(_cts.Token));
                }
            }
            finally
            {
                _initLock.Release();
            }
        }

        public void ReportDurableLsn(string operatorName, ulong lsn)
        {
            _durableLsns[operatorName] = (long)lsn;
        }

        public async Task DetachAsync(SharedChangeSource source)
        {
            bool teardown = false;
            Task? streamTask = null;
            await _initLock.WaitAsync();
            try
            {
                _sources.TryRemove(source.OperatorName, out _);
                // Deliberately keep this operator's entry in _durableLsns. Removing it would let MinDurableLsn() jump
                // forward during a staggered teardown (one table detaches while the loop still streams for another),
                // advancing the persistent slot's confirmed_flush past the detached table's durable position and losing
                // its WAL on resume. The whole dictionary is discarded when the reader tears down, so there is no leak.
                // Release resources once the last table detaches. The reader may already be marked disposed (e.g. the
                // streaming loop faulted and removed it from the registry); either way, the last detacher cleans up.
                if (--_attached <= 0 && !_tornDown)
                {
                    _tornDown = true;
                    MarkDisposedAndRemove_NoLock();
                    teardown = true;
                    // Captured under the lock so we never read a stale (unpublished) value and skip awaiting the loop.
                    streamTask = _streamTask;
                }
            }
            finally
            {
                _initLock.Release();
            }

            if (teardown)
            {
                // Done outside the lock (it awaits the streaming task). A temporary slot is dropped by closing the
                // connection; a persistent slot is intentionally left in place.
                _cts.Cancel();
                if (streamTask != null)
                {
                    try
                    {
                        await streamTask;
                    }
                    catch
                    {
                        // Ignore shutdown errors.
                    }
                }
                if (_connection != null)
                {
                    await _connection.DisposeAsync();
                }
                _cts.Dispose();
            }
        }

        private async Task InitializeAsync(long resumeLsn, CancellationToken ct)
        {
            var membershipKey = string.Join(",", _membership
                .OrderBy(t => t.schema, StringComparer.Ordinal)
                .ThenBy(t => t.table, StringComparer.Ordinal)
                .Select(t => $"{t.schema}.{t.table}"));

            await EnsurePublicationAsync(membershipKey, ct);

            _slotName = PostgresUtils.BuildIdentifier(_options.SlotPrefix, _streamName, _databaseId, membershipKey);
            _connection = new LogicalReplicationConnection(_options.ConnectionStringFunc());
            await _connection.Open(ct);

            if (_persistent && resumeLsn > 0 && await SlotIsHealthyAsync(ct))
            {
                // Resume the existing persistent slot from its confirmed position - no snapshot.
                _slot = new PgOutputReplicationSlot(_slotName);
                _snapshot = null;
                return;
            }

            if (_persistent)
            {
                await DropSlotIfExistsAsync(ct);
            }

            _slot = await _connection.CreatePgOutputReplicationSlot(
                _slotName,
                temporarySlot: !_persistent,
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.Export,
                cancellationToken: ct);

            _snapshot = new PostgresSnapshotInfo
            {
                SnapshotName = _slot.SnapshotName!,
                ConsistentLsn = (ulong)_slot.ConsistentPoint
            };
        }

        private async Task<bool> SlotIsHealthyAsync(CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync(ct);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT wal_status FROM pg_replication_slots WHERE slot_name = @name AND slot_type = 'logical'";
            cmd.Parameters.AddWithValue("name", _slotName);
            var walStatus = (string?)await cmd.ExecuteScalarAsync(ct);
            return walStatus != null && !string.Equals(walStatus, "lost", StringComparison.OrdinalIgnoreCase);
        }

        private async Task DropSlotIfExistsAsync(CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync(ct);
            try
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = @name";
                cmd.Parameters.AddWithValue("name", _slotName);
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException)
            {
                // Slot may be active or already gone; a real problem surfaces on the create below.
            }
        }

        private async Task EnsurePublicationAsync(string membershipKey, CancellationToken ct)
        {
            if (_options.PublicationName != null)
            {
                _publicationName = _options.PublicationName;
                return;
            }

            _publicationName = PostgresUtils.BuildIdentifier(_options.PublicationPrefix, _streamName, _databaseId, membershipKey);

            await using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync(ct);

            using (var check = connection.CreateCommand())
            {
                check.CommandText = "SELECT 1 FROM pg_publication WHERE pubname = @name";
                check.Parameters.AddWithValue("name", _publicationName);
                if (await check.ExecuteScalarAsync(ct) != null)
                {
                    return;
                }
            }

            var tableList = string.Join(", ", _membership.Select(t => PostgresUtils.QualifiedName(t.schema, t.table)));
            try
            {
                using var create = connection.CreateCommand();
                create.CommandText = $"CREATE PUBLICATION {PostgresUtils.QuoteIdentifier(_publicationName)} FOR TABLE {tableList}";
                await create.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "42710")
            {
                // Created concurrently - fine.
            }
        }

        private async Task StreamLoopAsync(CancellationToken ct)
        {
            try
            {
                var options = new PgOutputReplicationOptions(_publicationName, PgOutputProtocolVersion.V1);
                await foreach (var message in _connection!.StartReplication(_slot!, options, ct))
                {
                    switch (message)
                    {
                        case RelationMessage relation:
                            // Build a map for every source reading this relation (there can be more than one).
                            foreach (var entry in _sources.Values)
                            {
                                if (entry.MatchesRelation(relation.Namespace, relation.RelationName))
                                {
                                    entry.Map = RelationColumnMap.Build(relation, entry.SchemaNames, entry.KeyIndices);
                                }
                            }
                            break;
                        case InsertMessage insert:
                            await RouteAsync(insert.Relation, await PgOutputDecoder.ReadInsertAsync(insert, ct), ct);
                            break;
                        case UpdateMessage update:
                            await RouteAsync(update.Relation, await PgOutputDecoder.ReadUpdateAsync(update, ct), ct);
                            break;
                        case DeleteMessage delete:
                            await RouteAsync(delete.Relation, await PgOutputDecoder.ReadDeleteAsync(delete, ct), ct);
                            break;
                        case TruncateMessage:
                            foreach (var entry in _sources.Values)
                            {
                                entry.Source.RequestResnapshot();
                            }
                            break;
                    }

                    await ConfirmAsync(message.WalEnd, ct);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown.
            }
            catch (Exception ex)
            {
                // Mark the reader unusable and remove it from the registry FIRST, so that when the operators roll back
                // (staggered, at slightly different times) their re-init creates a FRESH reader instead of re-attaching
                // to this dead one - which would otherwise never restart its streaming loop (silent stall). Resource
                // teardown still happens when the last operator detaches. We must not drive the rollback from here (that
                // would dispose this reader and wait for this very task).
                await _initLock.WaitAsync();
                try
                {
                    MarkDisposedAndRemove_NoLock();
                }
                finally
                {
                    _initLock.Release();
                }
                foreach (var entry in _sources.Values)
                {
                    entry.Writer.TryComplete(ex);
                    entry.Source.SetFault(ex);
                }
            }
        }

        private void MarkDisposedAndRemove_NoLock()
        {
            if (!_disposed)
            {
                _disposed = true;
                PostgresReplicationCoordinator.RemoveReader(_streamName, _databaseId);
            }
        }

        private async Task ConfirmAsync(NpgsqlLogSequenceNumber walEnd, CancellationToken ct)
        {
            if (_persistent)
            {
                // confirmed_flush can only advance to the minimum LSN every table has durably checkpointed; tables that
                // are ahead simply re-process the gap on resume (idempotent via the reconciling apply).
                var min = MinDurableLsn();
                if (min == 0)
                {
                    return;
                }
                _connection!.SetReplicationStatus(new NpgsqlLogSequenceNumber((ulong)min));
            }
            else
            {
                _connection!.SetReplicationStatus(walEnd);
            }

            var now = DateTime.UtcNow;
            if (now - _lastStatusSent >= _options.StatusUpdateInterval)
            {
                _lastStatusSent = now;
                await _connection!.SendStatusUpdate(ct);
            }
        }

        private long MinDurableLsn()
        {
            long min = long.MaxValue;
            foreach (var lsn in _durableLsns.Values)
            {
                if (lsn < min)
                {
                    min = lsn;
                }
            }
            return min == long.MaxValue ? 0 : min;
        }

        // Fans a single decoded change out to every source reading this relation, projecting it onto each source's own
        // schema. The wire tuple was already read once into the relation-ordered RawPgChange, so each side of a
        // self-join gets the change in its own column order.
        private async Task RouteAsync(RelationMessage relation, RawPgChange raw, CancellationToken ct)
        {
            foreach (var entry in _sources.Values)
            {
                if (entry.Map != null && entry.MatchesRelation(relation.Namespace, relation.RelationName))
                {
                    await entry.Writer.WriteAsync(PgOutputDecoder.Project(raw, entry.Map), ct);
                }
            }
        }
    }
}
