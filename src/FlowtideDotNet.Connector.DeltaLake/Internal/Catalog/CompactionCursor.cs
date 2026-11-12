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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    // Each due rule keeps its own cursor
    internal enum DueRule
    {
        Fill,
        Count,
        Remainder
    }

    // Why a discovery call stopped
    internal enum CursorStop
    {
        Off,
        Suspended,
        Paused,
        Full,
        Budget,
        LapEnd
    }

    internal readonly record struct CursorHit(int Id, DeltaFileRecord Record);

    // Finds cold files a slice at a time, without full scans
    internal sealed class CompactionCursor
    {
        private static readonly Comparer<YoungFile> s_byWritten = Comparer<YoungFile>.Create((a, b) => a.Written != b.Written ? a.Written.CompareTo(b.Written) : a.Id.CompareTo(b.Id));

        private readonly CompactionLimits _limits;
        private readonly long _coldMs;
        private readonly CompactionDeclines _declines;
        private readonly List<CursorHit> _hits = new List<CursorHit>();
        // The youngest files seen, oldest first
        private readonly List<YoungFile> _young = new List<YoungFile>();
        // Every id in the hits or the young files
        private readonly HashSet<int> _held = new HashSet<int>();
        private readonly List<int> _reads = new List<int>();
        private readonly SlotVisitor _visit;
        private IReadOnlySet<int>? _skip;
        private int _budget;
        private int _position;
        private int _wanted;
        private long _pausedUntil = long.MinValue;
        // A bin or a class gained mid-lap skips this lap's pause
        private bool _noPause;
        private bool _lapEnded;
        private long _readCount;
        private long _inspected;
        private long _leaves;
        private long _stalls;

        private readonly record struct YoungFile(long Written, int Id, string Path, byte Class);

        public CompactionCursor(CompactionLimits limits, TimeSpan coldAfter, CompactionDeclines declines)
        {
            _limits = limits;
            _coldMs = coldAfter.Ticks / TimeSpan.TicksPerMillisecond;
            _declines = declines;
            _visit = VisitSlot;
        }

        public int Position => _position;

        public IReadOnlyList<CursorHit> Hits => _hits;

        public int YoungFiles => _young.Count;

        public long PausedUntil => _pausedUntil;

        public long Reads => _readCount;

        public long Inspected => _inspected;

        public long Leaves => _leaves;

        public long Stalls => Volatile.Read(ref _stalls);

        public async Task<CursorStop> Step(PruningProjection projection, DeltaSinkCatalog catalog, int wanted, IReadOnlySet<int>? skip, long now)
        {
            if (_declines.Suspended)
            {
                return Suspend();
            }
            SetWanted(wanted & ((1 << SizeClass.Counted) - 1));
            if (skip != null && skip.Count > 0)
            {
                DropHits(skip);
            }
            var coldBefore = now - _coldMs;
            // A clock step back can make a buffered hit young again
            for (int i = _hits.Count - 1; i >= 0; i--)
            {
                var hit = _hits[i];
                if (hit.Record.ModificationTime > coldBefore)
                {
                    _hits.RemoveAt(i);
                    _held.Remove(hit.Id);
                    PushYoung(hit.Record.ModificationTime, hit.Id, hit.Record.Path, SizeClass.Of(hit.Record));
                }
            }
            if (now < _pausedUntil)
            {
                return CursorStop.Paused;
            }
            // Reaching the pause time only ends the pause
            _pausedUntil = long.MinValue;
            _lapEnded = false;

            // Young files that turned cold go first
            var used = await RevalidateYoung(catalog, skip, coldBefore);
            if (_declines.Suspended)
            {
                return Suspend();
            }
            if (_hits.Count >= _limits.Hits)
            {
                return CursorStop.Full;
            }
            _budget = _limits.ColdReads - used;
            if (_budget <= 0)
            {
                return CursorStop.Budget;
            }

            _reads.Clear();
            _skip = skip;
            var scan = await projection.ScanFrom(_position, _wanted, _limits.ScanSlots, _limits.ScanLeaves, _visit);
            _skip = null;
            _inspected += scan.Inspected;
            _leaves += scan.Leaves;
            var records = await catalog.GetFiles(_reads);
            _readCount += _reads.Count;
            foreach (var id in _reads)
            {
                if (!records.TryGetValue(id, out var record) || !Admits(id, record))
                {
                    if (_declines.Suspended)
                    {
                        return Suspend();
                    }
                    continue;
                }
                if (record.ModificationTime > coldBefore)
                {
                    PushYoung(record.ModificationTime, id, record.Path, SizeClass.Of(record));
                    continue;
                }
                _hits.Add(new CursorHit(id, record));
                _held.Add(id);
                if (_hits.Count >= _limits.Hits)
                {
                    // Later reads of this call are inspected again
                    _position = id + 1;
                    return CursorStop.Full;
                }
            }
            if (scan.Next >= 0)
            {
                _position = scan.Next;
                return CursorStop.Budget;
            }
            EndLap(now);
            return CursorStop.LapEnd;
        }

        // The planner formed a bin from these hits
        public void Consume(IReadOnlySet<int> ids)
        {
            DropHits(ids);
            if (!_lapEnded)
            {
                _noPause = true;
            }
            _pausedUntil = long.MinValue;
        }

        // Removed files also leave the young files
        public void Forget(int id, bool removed)
        {
            if (!_held.Contains(id))
            {
                return;
            }
            for (int i = 0; i < _hits.Count; i++)
            {
                if (_hits[i].Id == id)
                {
                    _hits.RemoveAt(i);
                    _held.Remove(id);
                    return;
                }
            }
            if (!removed)
            {
                return;
            }
            for (int i = 0; i < _young.Count; i++)
            {
                if (_young[i].Id == id)
                {
                    _young.RemoveAt(i);
                    _held.Remove(id);
                    return;
                }
            }
        }

        // A full buffer formed no bin
        public void Stall()
        {
            ClearHits();
            Volatile.Write(ref _stalls, _stalls + 1);
        }

        public void Reset()
        {
            _position = 0;
            _hits.Clear();
            _young.Clear();
            _held.Clear();
            _wanted = 0;
            _pausedUntil = long.MinValue;
            _noPause = false;
            _lapEnded = false;
        }

        // A slot costs a read unless held, declined or skipped
        private bool VisitSlot(int id)
        {
            if (_held.Contains(id) || _declines.Contains(id) || (_skip != null && _skip.Contains(id)))
            {
                return true;
            }
            _reads.Add(id);
            return _reads.Count < _budget;
        }

        private async Task<int> RevalidateYoung(DeltaSinkCatalog catalog, IReadOnlySet<int>? skip, long coldBefore)
        {
            if (_hits.Count >= _limits.Hits || _young.Count == 0 || _young[0].Written > coldBefore)
            {
                return 0;
            }
            _reads.Clear();
            for (int i = 0; i < _young.Count && i < _limits.ColdReads && _young[i].Written <= coldBefore; i++)
            {
                _reads.Add(_young[i].Id);
            }
            var records = await catalog.GetFiles(_reads);
            _readCount += _reads.Count;
            var processed = 0;
            for (; processed < _reads.Count && _hits.Count < _limits.Hits; processed++)
            {
                var young = _young[processed];
                _held.Remove(young.Id);
                if (records.TryGetValue(young.Id, out var record)
                    && record.Path == young.Path
                    && (skip == null || !skip.Contains(young.Id))
                    && !_declines.Contains(young.Id)
                    && Admits(young.Id, record)
                    && record.ModificationTime <= coldBefore)
                {
                    _hits.Add(new CursorHit(young.Id, record));
                    _held.Add(young.Id);
                }
            }
            _young.RemoveRange(0, processed);
            return processed;
        }

        private bool Admits(int id, DeltaFileRecord record)
        {
            if (((_wanted >> SizeClass.Of(record)) & 1) == 0)
            {
                return false;
            }
            // The copier reads only stored deletion vectors
            var vector = record.DeletionVector;
            if (vector != null && vector.StorageType != "u" && vector.StorageType != "p")
            {
                _declines.Decline(id, record.Path, record, vector.StorageType == "i" ? CompactionReason.InlineDv : CompactionReason.Malformed);
                return false;
            }
            return true;
        }

        private void PushYoung(long written, int id, string path, byte sizeClass)
        {
            if (_young.Count >= _limits.Hits)
            {
                var last = _young[^1];
                if (written >= last.Written)
                {
                    return;
                }
                _young.RemoveAt(_young.Count - 1);
                _held.Remove(last.Id);
            }
            var entry = new YoungFile(written, id, path, sizeClass);
            var index = _young.BinarySearch(entry, s_byWritten);
            _young.Insert(index < 0 ? ~index : index, entry);
            _held.Add(id);
        }

        // Without a bin, wait until enough young files turn cold
        private void EndLap(long now)
        {
            if (!_noPause)
            {
                var wanted = Math.Min(_limits.Hits - _hits.Count, _young.Count);
                // A future write time waits no longer than a fresh file
                _pausedUntil = (wanted > 0 ? Math.Min(_young[wanted - 1].Written, now) : now) + _coldMs;
            }
            _position = 0;
            _noPause = false;
            _lapEnded = true;
        }

        // The table waits for the next epoch, so no bin forms
        private CursorStop Suspend()
        {
            ClearHits();
            return CursorStop.Suspended;
        }

        private void ClearHits()
        {
            foreach (var hit in _hits)
            {
                _held.Remove(hit.Id);
            }
            _hits.Clear();
        }

        // Drop files of lost classes and unpause for new ones
        private void SetWanted(int wanted)
        {
            if (wanted == _wanted)
            {
                return;
            }
            var lost = _wanted & ~wanted;
            var gained = wanted & ~_wanted;
            _wanted = wanted;
            if (gained != 0)
            {
                _pausedUntil = long.MinValue;
                // Slots before the position were never scanned for it
                if (_position > 0)
                {
                    _noPause = true;
                }
            }
            if (lost != 0)
            {
                for (int i = _hits.Count - 1; i >= 0; i--)
                {
                    if (((lost >> SizeClass.Of(_hits[i].Record)) & 1) != 0)
                    {
                        _held.Remove(_hits[i].Id);
                        _hits.RemoveAt(i);
                    }
                }
                for (int i = _young.Count - 1; i >= 0; i--)
                {
                    if (((lost >> _young[i].Class) & 1) != 0)
                    {
                        _held.Remove(_young[i].Id);
                        _young.RemoveAt(i);
                    }
                }
            }
        }

        private void DropHits(IReadOnlySet<int> ids)
        {
            for (int i = _hits.Count - 1; i >= 0; i--)
            {
                if (ids.Contains(_hits[i].Id))
                {
                    _held.Remove(_hits[i].Id);
                    _hits.RemoveAt(i);
                }
            }
        }
    }
}
