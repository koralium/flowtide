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
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// One shared replication slot + connection for all the tables of a (stream, database). Decodes the single WAL
    /// stream once and demultiplexes each change to the matching table's bounded channel.
    /// </summary>
    internal sealed class SharedReplicationReader
    {
        private sealed class SourceEntry
        {
            public required ChannelWriter<PostgresChange> Writer { get; init; }
            public required IReadOnlyList<string> SchemaNames { get; init; }
            public required IReadOnlyList<int> KeyIndices { get; init; }
            public required SharedChangeSource Source { get; init; }
            public RelationColumnMap? Map { get; set; }
        }

        private readonly PostgresSourceOptions _options;
        private readonly string _streamName;
        private readonly string _databaseId;
        private readonly List<(string schema, string table)> _membership;
        private readonly SemaphoreSlim _initLock = new(1, 1);
        private readonly ConcurrentDictionary<(string schema, string table), SourceEntry> _sources = new();
        private readonly CancellationTokenSource _cts = new();

        private bool _initialized;
        private int _attached;
        private int _snapshotDone;
        private int _streamingStarted;
        private int _disposed;
        private LogicalReplicationConnection? _connection;
        private PgOutputReplicationSlot? _slot;
        private string _publicationName = string.Empty;
        private string _slotName = string.Empty;
        private PostgresSnapshotInfo? _snapshot;
        private Task? _streamTask;

        public SharedReplicationReader(PostgresSourceOptions options, string streamName, string databaseId, List<(string schema, string table)> membership)
        {
            _options = options;
            _streamName = streamName;
            _databaseId = databaseId;
            _membership = membership;
        }

        public async Task<(PostgresSnapshotInfo snapshot, ChannelReader<PostgresChange> channel)> AttachAsync(SharedChangeSource source, CancellationToken ct)
        {
            await _initLock.WaitAsync(ct);
            try
            {
                if (!_initialized)
                {
                    await InitializeAsync(ct);
                    _initialized = true;
                }

                var channel = Channel.CreateBounded<PostgresChange>(new BoundedChannelOptions(_options.ChannelCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true
                });

                _sources[(source.Schema, source.Table)] = new SourceEntry
                {
                    Writer = channel.Writer,
                    SchemaNames = source.SchemaNames,
                    KeyIndices = source.KeySchemaIndices,
                    Source = source
                };
                Interlocked.Increment(ref _attached);

                return (_snapshot!, channel.Reader);
            }
            finally
            {
                _initLock.Release();
            }
        }

        public Task SignalSnapshotCompleteAsync(CancellationToken ct)
        {
            var done = Interlocked.Increment(ref _snapshotDone);
            if (done >= _membership.Count && Interlocked.Exchange(ref _streamingStarted, 1) == 0)
            {
                _streamTask = Task.Run(() => StreamLoopAsync(_cts.Token));
            }
            return Task.CompletedTask;
        }

        public void Acknowledge(ulong lsn)
        {
            // The streaming loop already confirms each message's WAL position as it is buffered. Over-acknowledging is
            // safe with a temporary slot, so per-consumer acknowledgements do not need to be coordinated here.
        }

        public async Task DetachAsync(SharedChangeSource source)
        {
            _sources.TryRemove((source.Schema, source.Table), out _);
            if (Interlocked.Decrement(ref _attached) <= 0)
            {
                // Tear down exactly once even if detach is called more times than attach (e.g. a rollback-driven
                // re-init that disposes a previous change source).
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
                if (_connection != null)
                {
                    await _connection.DisposeAsync();
                }
                PostgresReplicationCoordinator.RemoveReader(_streamName, _databaseId);
                _cts.Dispose();
            }
        }

        private async Task InitializeAsync(CancellationToken ct)
        {
            var membershipKey = string.Join(",", _membership
                .OrderBy(t => t.schema, StringComparer.Ordinal)
                .ThenBy(t => t.table, StringComparer.Ordinal)
                .Select(t => $"{t.schema}.{t.table}"));

            await EnsurePublicationAsync(membershipKey, ct);

            _slotName = PostgresUtils.BuildIdentifier(_options.SlotPrefix, _streamName, _databaseId, membershipKey);
            _connection = new LogicalReplicationConnection(_options.ConnectionStringFunc());
            await _connection.Open(ct);
            _slot = await _connection.CreatePgOutputReplicationSlot(
                _slotName,
                temporarySlot: true,
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.Export,
                cancellationToken: ct);

            _snapshot = new PostgresSnapshotInfo
            {
                SnapshotName = _slot.SnapshotName!,
                ConsistentLsn = (ulong)_slot.ConsistentPoint
            };
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
                            if (_sources.TryGetValue((relation.Namespace, relation.RelationName), out var relEntry))
                            {
                                relEntry.Map = RelationColumnMap.Build(relation, relEntry.SchemaNames, relEntry.KeyIndices);
                            }
                            break;
                        case InsertMessage insert:
                            await RouteAsync(insert.Relation, insert, static (m, map, ct) => PgOutputDecoder.DecodeInsertAsync((InsertMessage)m, map, ct), ct);
                            break;
                        case UpdateMessage update:
                            await RouteAsync(update.Relation, update, static (m, map, ct) => PgOutputDecoder.DecodeUpdateAsync((UpdateMessage)m, map, ct), ct);
                            break;
                        case DeleteMessage delete:
                            await RouteAsync(delete.Relation, delete, static (m, map, ct) => PgOutputDecoder.DecodeDeleteAsync((DeleteMessage)m, map, ct), ct);
                            break;
                        case TruncateMessage:
                            foreach (var entry in _sources.Values)
                            {
                                entry.Source.RequestResnapshot();
                            }
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
                foreach (var entry in _sources.Values)
                {
                    entry.Writer.TryComplete(ex);
                    await entry.Source.FaultHandler(ex);
                }
            }
        }

        private async Task RouteAsync(RelationMessage relation, PgOutputReplicationMessage message, Func<PgOutputReplicationMessage, RelationColumnMap, CancellationToken, Task<PostgresChange>> decode, CancellationToken ct)
        {
            if (_sources.TryGetValue((relation.Namespace, relation.RelationName), out var entry) && entry.Map != null)
            {
                var change = await decode(message, entry.Map, ct);
                await entry.Writer.WriteAsync(change, ct);
            }
        }
    }
}
