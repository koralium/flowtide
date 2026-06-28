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
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// A change source that owns a dedicated replication slot and connection for a single table
    /// (<see cref="PostgresReplicationMode.PerTable"/>). The slot is temporary or persistent per
    /// <see cref="PostgresSourceOptions.SlotDurability"/>.
    /// </summary>
    internal sealed class PerTableChangeSource : IPostgresChangeSource
    {
        private readonly PostgresSourceOptions _options;
        private readonly bool _persistent;
        private readonly string _streamName;
        private readonly string _operatorName;
        private readonly string _schema;
        private readonly string _table;
        private readonly IReadOnlyList<string> _schemaNames;
        private readonly IReadOnlyList<int> _keySchemaIndices;

        private readonly Channel<PostgresChange> _channel;
        private readonly CancellationTokenSource _cts = new();
        private volatile Exception? _fault;

        private LogicalReplicationConnection? _connection;
        private PgOutputReplicationSlot? _slot;
        private string _publicationName = string.Empty;
        private string _slotName = string.Empty;
        private RelationColumnMap? _map;
        private Task? _streamTask;
        private volatile bool _needsResnapshot;
        private int _disposed;
        private long _confirmedLsn;
        private long _lastCommitLsn;
        private DateTime _lastStatusSent = DateTime.MinValue;

        public PerTableChangeSource(
            PostgresSourceOptions options,
            string streamName,
            string operatorName,
            string schema,
            string table,
            IReadOnlyList<string> schemaNames,
            IReadOnlyList<int> keySchemaIndices)
        {
            _options = options;
            _persistent = options.SlotDurability == PostgresSlotDurability.Persistent;
            _streamName = streamName;
            _operatorName = operatorName;
            _schema = schema;
            _table = table;
            _schemaNames = schemaNames;
            _keySchemaIndices = keySchemaIndices;
            _channel = Channel.CreateBounded<PostgresChange>(new BoundedChannelOptions(options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });
        }

        public bool NeedsResnapshot => _needsResnapshot;

        public void ClearResnapshot() => _needsResnapshot = false;

        public Exception? Fault => _fault;

        public ulong LastCommitLsn => (ulong)Interlocked.Read(ref _lastCommitLsn);

        public async Task<PostgresSnapshotInfo?> InitializeAsync(long resumeLsn, CancellationToken ct)
        {
            await EnsurePublicationAsync(ct);

            // The slot name includes the operator name so two operators reading the same table (e.g. a self-join) get
            // distinct slots. The publication stays per-table (it is shared by both operators).
            _slotName = PostgresUtils.BuildIdentifier(_options.SlotPrefix, _streamName, _schema, _table, _operatorName);
            _connection = new LogicalReplicationConnection(_options.ConnectionStringFunc());
            await _connection.Open(ct);

            if (_persistent && resumeLsn > 0 && await SlotIsHealthyAsync(ct))
            {
                // Resume an existing persistent slot from its confirmed position - no snapshot needed.
                _slot = new PgOutputReplicationSlot(_slotName);
                _confirmedLsn = resumeLsn;
                return null;
            }

            if (_persistent)
            {
                // Either first start, or a stale/invalidated slot with no durable state: ensure a clean slot before
                // creating a fresh one with an exported snapshot.
                await DropSlotIfExistsAsync(ct);
            }

            _slot = await _connection.CreatePgOutputReplicationSlot(
                _slotName,
                temporarySlot: !_persistent,
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.Export,
                cancellationToken: ct);

            return new PostgresSnapshotInfo
            {
                SnapshotName = _slot.SnapshotName!,
                ConsistentLsn = (ulong)_slot.ConsistentPoint
            };
        }

        public Task SnapshotCompleteAsync(CancellationToken ct)
        {
            // For a single table there is no cross-table coordination, so streaming begins as soon as the snapshot read
            // for this table has finished (which is also when the exported snapshot is no longer needed).
            StartStreaming();
            return Task.CompletedTask;
        }

        public Task BeginStreamingAsync(CancellationToken ct)
        {
            StartStreaming();
            return Task.CompletedTask;
        }

        private void StartStreaming()
        {
            _streamTask ??= Task.Run(() => StreamLoopAsync(_cts.Token));
        }

        public bool TryRead([NotNullWhen(true)] out PostgresChange? change)
        {
            return _channel.Reader.TryRead(out change);
        }

        public void Acknowledge(ulong lsn)
        {
            // For a temporary slot, over-acknowledging is safe (a restart re-snapshots), so the last processed position
            // can be confirmed to release WAL during the session. A persistent slot must only confirm durable progress,
            // which arrives via SetConfirmedFlushLsn at checkpoint, so this is ignored.
            if (!_persistent)
            {
                Interlocked.Exchange(ref _confirmedLsn, (long)lsn);
            }
        }

        public void SetConfirmedFlushLsn(ulong lsn)
        {
            if (_persistent)
            {
                Interlocked.Exchange(ref _confirmedLsn, (long)lsn);
            }
        }

        private async Task<bool> SlotIsHealthyAsync(CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync(ct);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT wal_status FROM pg_replication_slots WHERE slot_name = @name AND slot_type = 'logical'";
            cmd.Parameters.AddWithValue("name", _slotName);
            var walStatus = (string?)await cmd.ExecuteScalarAsync(ct);
            // No row -> doesn't exist; 'lost' -> invalidated (fell too far behind). Either way we cannot resume.
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
                // Slot may be active or already gone; the subsequent create will surface a real problem.
            }
        }

        private async Task EnsurePublicationAsync(CancellationToken ct)
        {
            if (_options.PublicationName != null)
            {
                _publicationName = _options.PublicationName;
                return;
            }

            _publicationName = PostgresUtils.BuildIdentifier(_options.PublicationPrefix, _streamName, _schema, _table);

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

            try
            {
                using var create = connection.CreateCommand();
                create.CommandText = $"CREATE PUBLICATION {PostgresUtils.QuoteIdentifier(_publicationName)} FOR TABLE {PostgresUtils.QualifiedName(_schema, _table)}";
                await create.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "42710")
            {
                // Created concurrently by another source - that is fine.
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
                        case RelationMessage relation when Matches(relation.Namespace, relation.RelationName):
                            _map = RelationColumnMap.Build(relation, _schemaNames, _keySchemaIndices);
                            break;
                        case InsertMessage insert when Matches(insert.Relation) && _map != null:
                            await _channel.Writer.WriteAsync(PgOutputDecoder.Project(await PgOutputDecoder.ReadInsertAsync(insert, ct), _map), ct);
                            break;
                        case UpdateMessage update when Matches(update.Relation) && _map != null:
                            await _channel.Writer.WriteAsync(PgOutputDecoder.Project(await PgOutputDecoder.ReadUpdateAsync(update, ct), _map), ct);
                            break;
                        case DeleteMessage delete when Matches(delete.Relation) && _map != null:
                            await _channel.Writer.WriteAsync(PgOutputDecoder.Project(await PgOutputDecoder.ReadDeleteAsync(delete, ct), _map), ct);
                            break;
                        case TruncateMessage:
                            // Re-snapshot to reflect the truncate; refined in a later phase to emit precise retractions.
                            _needsResnapshot = true;
                            break;
                        case CommitMessage commit:
                            // A transaction is now complete; publish its commit LSN as the latest releasable position.
                            Interlocked.Exchange(ref _lastCommitLsn, (long)(ulong)commit.CommitLsn);
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
                // Record the fault and exit. The operator polls Fault and drives the rollback from its own thread; the
                // loop must not, because rollback disposes this source and waits for this very task to complete.
                _fault = ex;
                _channel.Writer.TryComplete(ex);
            }
        }

        private async Task ConfirmAsync(NpgsqlLogSequenceNumber walEnd, CancellationToken ct)
        {
            // A temporary slot can confirm the received position directly (over-acknowledging is safe). A persistent
            // slot must only confirm the durably-checkpointed LSN; confirming less is safe (it just replays more on
            // restart, which is idempotent), confirming more would risk data loss.
            if (_persistent)
            {
                var confirmed = (ulong)Interlocked.Read(ref _confirmedLsn);
                if (confirmed == 0)
                {
                    return;
                }
                _connection!.SetReplicationStatus(new NpgsqlLogSequenceNumber(confirmed));
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

        private bool Matches(RelationMessage relation) => Matches(relation.Namespace, relation.RelationName);

        private bool Matches(string ns, string name)
            => string.Equals(ns, _schema, StringComparison.Ordinal) && string.Equals(name, _table, StringComparison.Ordinal);

        public async ValueTask DisposeAsync()
        {
            // Idempotent: a rollback-driven re-init followed by final teardown can dispose the same source twice.
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }
            _cts.Cancel();
            if (_streamTask != null)
            {
                try
                {
                    await _streamTask;
                }
                catch
                {
                    // Ignore shutdown errors.
                }
            }
            // A temporary slot is dropped by closing the connection. A persistent slot is intentionally left in place so
            // the next run can resume from it; it must be removed explicitly when the stream is permanently retired.
            if (_connection != null)
            {
                await _connection.DisposeAsync();
            }
            _cts.Dispose();
        }
    }
}
