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
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// A change source that owns a dedicated temporary replication slot and connection for a single table
    /// (<see cref="PostgresReplicationMode.PerTable"/>).
    /// </summary>
    internal sealed class PerTableChangeSource : IPostgresChangeSource
    {
        private readonly PostgresSourceOptions _options;
        private readonly string _streamName;
        private readonly string _schema;
        private readonly string _table;
        private readonly IReadOnlyList<string> _schemaNames;
        private readonly IReadOnlyList<int> _keySchemaIndices;
        private readonly Func<Exception, Task> _faultHandler;

        private readonly Channel<PostgresChange> _channel;
        private readonly CancellationTokenSource _cts = new();

        private LogicalReplicationConnection? _connection;
        private PgOutputReplicationSlot? _slot;
        private string _publicationName = string.Empty;
        private string _slotName = string.Empty;
        private RelationColumnMap? _map;
        private Task? _streamTask;
        private volatile bool _needsResnapshot;

        public PerTableChangeSource(
            PostgresSourceOptions options,
            string streamName,
            string schema,
            string table,
            IReadOnlyList<string> schemaNames,
            IReadOnlyList<int> keySchemaIndices,
            Func<Exception, Task> faultHandler)
        {
            _options = options;
            _streamName = streamName;
            _schema = schema;
            _table = table;
            _schemaNames = schemaNames;
            _keySchemaIndices = keySchemaIndices;
            _faultHandler = faultHandler;
            _channel = Channel.CreateBounded<PostgresChange>(new BoundedChannelOptions(options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });
        }

        public bool NeedsResnapshot => _needsResnapshot;

        public void ClearResnapshot() => _needsResnapshot = false;

        public async Task<PostgresSnapshotInfo?> InitializeAsync(CancellationToken ct)
        {
            await EnsurePublicationAsync(ct);

            _slotName = PostgresUtils.BuildIdentifier(_options.SlotPrefix, _streamName, _schema, _table);
            _connection = new LogicalReplicationConnection(_options.ConnectionStringFunc());
            await _connection.Open(ct);
            _slot = await _connection.CreatePgOutputReplicationSlot(
                _slotName,
                temporarySlot: true,
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
            _streamTask ??= Task.Run(() => StreamLoopAsync(_cts.Token));
            return Task.CompletedTask;
        }

        public bool TryRead([NotNullWhen(true)] out PostgresChange? change)
        {
            return _channel.Reader.TryRead(out change);
        }

        public void Acknowledge(ulong lsn)
        {
            // The streaming loop already confirms each message's WAL position as it is buffered. Over-acknowledging is
            // safe with a temporary slot because a restart re-snapshots rather than resuming from this position.
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
                            await _channel.Writer.WriteAsync(await PgOutputDecoder.DecodeInsertAsync(insert, _map, ct), ct);
                            break;
                        case UpdateMessage update when Matches(update.Relation) && _map != null:
                            await _channel.Writer.WriteAsync(await PgOutputDecoder.DecodeUpdateAsync(update, _map, ct), ct);
                            break;
                        case DeleteMessage delete when Matches(delete.Relation) && _map != null:
                            await _channel.Writer.WriteAsync(await PgOutputDecoder.DecodeDeleteAsync(delete, _map, ct), ct);
                            break;
                        case TruncateMessage:
                            // Re-snapshot to reflect the truncate; refined in a later phase to emit precise retractions.
                            _needsResnapshot = true;
                            break;
                    }

                    _connection!.SetReplicationStatus(message.WalEnd);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown.
            }
            catch (Exception ex)
            {
                _channel.Writer.TryComplete(ex);
                await _faultHandler(ex);
            }
        }

        private bool Matches(RelationMessage relation) => Matches(relation.Namespace, relation.RelationName);

        private bool Matches(string ns, string name)
            => string.Equals(ns, _schema, StringComparison.Ordinal) && string.Equals(name, _table, StringComparison.Ordinal);

        public async ValueTask DisposeAsync()
        {
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
            if (_connection != null)
            {
                await _connection.DisposeAsync();
            }
            _cts.Dispose();
        }
    }
}
