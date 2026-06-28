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
using System.Threading.Channels;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// A change source backed by a per-(stream, database) shared reader (<see cref="PostgresReplicationMode.Shared"/>).
    /// The reader owns the single slot and connection; this class is just this table's view onto its channel.
    /// </summary>
    internal sealed class SharedChangeSource : IPostgresChangeSource
    {
        private readonly PostgresReplicationCoordinator _coordinator;
        private readonly PostgresChangeSourceContext _context;
        private SharedReplicationReader? _reader;
        private ChannelReader<PostgresChange>? _channel;
        private volatile bool _needsResnapshot;
        private volatile Exception? _fault;

        public SharedChangeSource(PostgresReplicationCoordinator coordinator, PostgresChangeSourceContext context)
        {
            _coordinator = coordinator;
            _context = context;
        }

        public string OperatorName => _context.OperatorName;
        public string Schema => _context.Schema;
        public string Table => _context.Table;
        public IReadOnlyList<string> SchemaNames => _context.SchemaNames;
        public IReadOnlyList<int> KeySchemaIndices => _context.KeySchemaIndices;

        public bool NeedsResnapshot => _needsResnapshot;

        public void ClearResnapshot() => _needsResnapshot = false;

        internal void RequestResnapshot() => _needsResnapshot = true;

        public Exception? Fault => _fault;

        public ulong LastCommitLsn => _reader?.LastCommitLsn ?? 0;

        internal void SetFault(Exception exception) => _fault = exception;

        public async Task<PostgresSnapshotInfo?> InitializeAsync(long resumeLsn, CancellationToken ct)
        {
            // Retry to handle a rollback race: the reader we obtained may have been torn down between GetOrCreate and
            // Attach. Once disposed it is removed from the registry, so the next GetOrCreate returns a fresh one.
            while (true)
            {
                _reader = _coordinator.GetOrCreateReader(_context.StreamName);
                var result = await _reader.AttachAsync(this, resumeLsn, ct);
                if (result.HasValue)
                {
                    _channel = result.Value.channel;
                    return result.Value.snapshot;
                }
            }
        }

        public Task SnapshotCompleteAsync(CancellationToken ct)
        {
            return _reader!.SignalReadyAsync(ct);
        }

        public Task BeginStreamingAsync(CancellationToken ct)
        {
            // Resume path (persistent slot): this table is ready to stream without a snapshot.
            return _reader!.SignalReadyAsync(ct);
        }

        public void SetConfirmedFlushLsn(ulong lsn)
        {
            _reader?.ReportDurableLsn(OperatorName, lsn);
        }

        public bool TryRead([NotNullWhen(true)] out PostgresChange? change)
        {
            if (_channel != null)
            {
                return _channel.TryRead(out change);
            }
            change = null;
            return false;
        }

        public void Acknowledge(ulong lsn)
        {
            // The shared reader confirms WAL positions in its own loop (the latest received position for a temporary
            // slot, or the minimum durable position for a persistent slot), so per-table acks are not needed here.
        }

        public async ValueTask DisposeAsync()
        {
            if (_reader != null)
            {
                await _reader.DetachAsync(this);
            }
        }
    }
}
