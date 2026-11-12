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
    // Keeps a table closed while deletes reach its older files
    internal sealed class CompactionGuard
    {
        private readonly CompactionWindow _window;
        private readonly long _halfColdMs;
        private readonly int _quietCommits;
        private readonly long _maxWindowMs;
        private readonly ILogger _logger;
        private readonly string _table;
        // Paths of the outputs this run applied
        private readonly HashSet<string> _outputs = new HashSet<string>(StringComparer.Ordinal);
        private long _sequence;
        private long _quietSince;
        private long _commits;

        public CompactionGuard(CompactionWindow window, TimeSpan coldAfter, CompactionLimits limits, long now, ILogger logger, string table)
        {
            _window = window;
            _halfColdMs = coldAfter.Ticks / 2 / TimeSpan.TicksPerMillisecond;
            _quietCommits = limits.QuietCommits;
            _maxWindowMs = limits.MaxQuietWindow.Ticks / TimeSpan.TicksPerMillisecond;
            _logger = logger;
            _table = table;
            // A stored window still covers half the cold-after
            _window.WindowMs = Math.Max(_window.WindowMs, _halfColdMs);
            // A clock that stepped back does not reopen the window early
            _quietSince = Math.Max(now, _window.LastSettledAt);
        }

        // Changes with every settled candidate
        public long Sequence => Volatile.Read(ref _sequence);

        public long QuietSince => Volatile.Read(ref _quietSince);

        public long Commits => Volatile.Read(ref _commits);

        public bool HasOutputs => _outputs.Count > 0;

        public long WindowMs => _window.WindowMs;

        // Files written at or before this are settled
        public long SettledBefore(long now)
        {
            return now - _halfColdMs;
        }

        public bool IsQuiet(long now)
        {
            return now - QuietSince >= _window.WindowMs && Commits >= _quietCommits;
        }

        // Checks a whole delete group before any file is read
        public long CheckGroup(List<int> candidates, int start, int end, Dictionary<int, DeltaFileRecord> records, long now)
        {
            var settledBefore = SettledBefore(now);
            long settled = 0;
            for (int c = start; c < end; c++)
            {
                if (records.TryGetValue(candidates[c], out var record) && (record.ModificationTime <= settledBefore || IsOutput(record.Path)))
                {
                    settled++;
                }
            }
            if (settled > 0)
            {
                SettledCandidate(now);
            }
            return settled;
        }

        public void SettledCandidate(long now)
        {
            if (IsQuiet(now))
            {
                // A relapse grows the window past the quiet spell
                var grown = Math.Min(_maxWindowMs, Math.Max(2 * _window.WindowMs, 2 * (now - QuietSince)));
                if (grown > _window.WindowMs)
                {
                    _window.WindowMs = grown;
                    _logger.LogInformation("Delta table {table}: deletes reached older files; compaction waits {window}", _table, TimeSpan.FromMilliseconds(grown));
                }
            }
            Volatile.Write(ref _sequence, _sequence + 1);
            Volatile.Write(ref _quietSince, Math.Max(_quietSince, now));
            Volatile.Write(ref _commits, 0);
            _window.LastSettledAt = Math.Max(_window.LastSettledAt, now);
        }

        // Counts data commits that selected no settled file
        public void CommitStaged(bool dataCommit, long sequenceAtStart)
        {
            if (dataCommit && _sequence == sequenceAtStart)
            {
                Volatile.Write(ref _commits, _commits + 1);
            }
        }

        // An epoch bump starts the window again
        public void Restart(long now)
        {
            Volatile.Write(ref _quietSince, Math.Max(_quietSince, now));
            Volatile.Write(ref _commits, 0);
        }

        public bool IsOutput(string path)
        {
            return _outputs.Count > 0 && _outputs.Contains(path);
        }

        public void AddOutput(string path)
        {
            _outputs.Add(path);
        }

        public void RemoveOutput(string path)
        {
            if (_outputs.Count > 0)
            {
                _outputs.Remove(path);
            }
        }

        public void ClearOutputs()
        {
            _outputs.Clear();
        }

        // Drops outputs another writer removed
        public void KeepOutputs(HashSet<string> livePaths)
        {
            _outputs.IntersectWith(livePaths);
        }
    }
}
