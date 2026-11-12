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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    // Files compaction declined, kept until the next epoch
    internal sealed class CompactionDeclines
    {
        private readonly int _capacity;
        private readonly ILogger _logger;
        private readonly string _table;
        private readonly Dictionary<int, Entry> _entries = new Dictionary<int, Entry>();
        private readonly long[] _files = new long[SizeClass.Counted];
        private readonly long[] _bytes = new long[SizeClass.Counted];
        private bool _suspended;
        private bool _warned;

        private readonly record struct Entry(string Path, byte Class, long Size, CompactionReason Reason);

        public CompactionDeclines(int capacity, ILogger logger, string table)
        {
            _capacity = capacity;
            _logger = logger;
            _table = table;
        }

        // Too many declines stop compaction until the next epoch
        public bool Suspended => Volatile.Read(ref _suspended);

        public int Count => _entries.Count;

        public bool Contains(int id)
        {
            return _entries.Count > 0 && _entries.ContainsKey(id);
        }

        public CompactionReason? ReasonOf(int id)
        {
            return _entries.TryGetValue(id, out var entry) ? entry.Reason : null;
        }

        public long DeclinedFiles(int sizeClass) => Volatile.Read(ref _files[sizeClass]);

        public long DeclinedBytes(int sizeClass) => Volatile.Read(ref _bytes[sizeClass]);

        // Counts only while the id still names the declined path
        public void Decline(int id, string expectedPath, DeltaFileRecord live, CompactionReason reason)
        {
            if (Suspended || live.Path != expectedPath || _entries.ContainsKey(id))
            {
                return;
            }
            if (_entries.Count >= _capacity)
            {
                Volatile.Write(ref _suspended, true);
                if (!_warned)
                {
                    _warned = true;
                    _logger.LogWarning("Delta table {table}: compaction declined more than {count} files and waits until the table is read again", _table, _capacity);
                }
                return;
            }
            var sizeClass = SizeClass.Of(live);
            _entries[id] = new Entry(live.Path, sizeClass, live.Size, reason);
            Tally(sizeClass, live.Size, 1);
        }

        public void Removed(int id, string path)
        {
            if (_entries.Count > 0 && _entries.TryGetValue(id, out var entry) && entry.Path == path)
            {
                _entries.Remove(id);
                Tally(entry.Class, entry.Size, -1);
            }
        }

        public void Clear()
        {
            _entries.Clear();
            for (int c = 0; c < _files.Length; c++)
            {
                Volatile.Write(ref _files[c], 0);
                Volatile.Write(ref _bytes[c], 0);
            }
            Volatile.Write(ref _suspended, false);
        }

        private void Tally(byte sizeClass, long size, int sign)
        {
            if (sizeClass == SizeClass.Ineligible)
            {
                return;
            }
            Volatile.Write(ref _files[sizeClass], _files[sizeClass] + sign);
            Volatile.Write(ref _bytes[sizeClass], _bytes[sizeClass] + sign * size);
        }
    }
}
