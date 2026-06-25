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

        public SharedChangeSource(PostgresReplicationCoordinator coordinator, PostgresChangeSourceContext context)
        {
            _coordinator = coordinator;
            _context = context;
        }

        public string Schema => _context.Schema;
        public string Table => _context.Table;
        public IReadOnlyList<string> SchemaNames => _context.SchemaNames;
        public IReadOnlyList<int> KeySchemaIndices => _context.KeySchemaIndices;
        public Func<Exception, Task> FaultHandler => _context.FaultHandler;

        public bool NeedsResnapshot => _needsResnapshot;

        public void ClearResnapshot() => _needsResnapshot = false;

        internal void RequestResnapshot() => _needsResnapshot = true;

        public async Task<PostgresSnapshotInfo?> InitializeAsync(CancellationToken ct)
        {
            _reader = _coordinator.GetOrCreateReader(_context.StreamName);
            var (snapshot, channel) = await _reader.AttachAsync(this, ct);
            _channel = channel;
            return snapshot;
        }

        public Task SnapshotCompleteAsync(CancellationToken ct)
        {
            return _reader!.SignalSnapshotCompleteAsync(ct);
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

        public void Acknowledge(ulong lsn) => _reader?.Acknowledge(lsn);

        public async ValueTask DisposeAsync()
        {
            if (_reader != null)
            {
                await _reader.DetachAsync(this);
            }
        }
    }
}
