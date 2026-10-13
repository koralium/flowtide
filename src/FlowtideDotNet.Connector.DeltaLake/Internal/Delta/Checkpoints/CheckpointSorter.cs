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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// Sort mode: entries buffered in the arena are written as sorted runs, then merged with fan-in 8 and reduced to one entry per key.
    /// </summary>
    internal sealed class CheckpointSorter : IDisposable
    {
        public const int FanIn = 8;

        private readonly JobScratch _scratch;
        private readonly JobMemory _memory;
        private readonly long _cap;
        private readonly RecordArena _arena;
        private readonly List<string> _runs = new List<string>();
        private long[] _offsets = Array.Empty<long>();
        private int _count;
        private byte[] _entry = Array.Empty<byte>();
        private byte[] _key = Array.Empty<byte>();
        private int _keyLength;

        public CheckpointSorter(JobScratch scratch, JobMemory memory, long arenaCap = TailReducer.DefaultArenaCap)
        {
            _scratch = scratch;
            _memory = memory;
            _cap = arenaCap;
            _arena = new RecordArena(memory, arenaCap);
        }

        public int Runs => _runs.Count;

        // Throws at the start of a merge, so tests can fail the build there
        public Action<string>? FaultForTests { get; init; }

        // Bytes written to runs, merge passes included
        public long SpillBytes { get; private set; }

        public void AddRun(RunWriter run)
        {
            _runs.Add(run.Path);
        }

        public void Add(ReadOnlySpan<byte> key, long version, long ordinal, ReadOnlySpan<byte> record)
        {
            var offsetGrowth = _count == _offsets.Length ? (long)Math.Max(1024, _offsets.Length * 2) * sizeof(long) : 0;
            var held = _arena.Bytes + (long)_offsets.Length * sizeof(long);
            if (_count > 0 && held + _arena.GrowthFor(key.Length, record.Length) + offsetGrowth > _cap)
            {
                FlushRun();
                offsetGrowth = _offsets.Length == 0 ? 1024 * sizeof(long) : 0;
            }
            if (offsetGrowth > 0)
            {
                GrowOffsets();
            }
            _offsets[_count++] = _arena.Append(key, version, ordinal, record);
        }

        private void GrowOffsets()
        {
            var length = Math.Max(1024, _offsets.Length * 2);
            _memory.Reserve((long)length * sizeof(long));
            var grown = new long[length];
            _offsets.AsSpan(0, _count).CopyTo(grown);
            _memory.Release((long)_offsets.Length * sizeof(long));
            _offsets = grown;
        }

        /// <summary>
        /// Sorts what the arena holds by key, then latest first, writes it as a run and empties the arena.
        /// </summary>
        public void FlushRun()
        {
            if (_count == 0)
            {
                return;
            }
            FaultForTests?.Invoke("run write");
            var arena = _arena;
            _offsets.AsSpan(0, _count).Sort((x, y) =>
            {
                var left = arena.Read(x);
                var right = arena.Read(y);
                return ArenaEntry.Compare(left, right);
            });
            using var run = new RunWriter(_scratch, _memory);
            for (int i = 0; i < _count; i++)
            {
                run.Write(_arena.Raw(_offsets[i]));
            }
            run.Close();
            _runs.Add(run.Path);
            SpillBytes += new FileInfo(run.Path).Length;
            _arena.Clear();
            _count = 0;
        }

        private void ReleaseBuffers()
        {
            _arena.Clear();
            _memory.Release((long)_offsets.Length * sizeof(long));
            _offsets = Array.Empty<long>();
            _count = 0;
        }

        /// <summary>
        /// Merges every run and calls the visitor once per key, in key order, with the entry that wins under the reconcile rules.
        /// </summary>
        public void Reduce(EntryVisitor visitor, CancellationToken cancellationToken = default)
        {
            FlushRun();
            ReleaseBuffers();
            // Earlier passes merge without reducing; consumed runs are deleted after their replacement is closed
            while (_runs.Count > FanIn)
            {
                var next = new List<string>();
                for (int i = 0; i < _runs.Count; i += FanIn)
                {
                    var group = _runs.GetRange(i, Math.Min(FanIn, _runs.Count - i));
                    if (group.Count == 1)
                    {
                        next.Add(group[0]);
                        continue;
                    }
                    using var merged = new RunWriter(_scratch, _memory);
                    Merge(group, entry => merged.Write(entry), cancellationToken);
                    merged.Close();
                    SpillBytes += new FileInfo(merged.Path).Length;
                    next.Add(merged.Path);
                    foreach (var path in group)
                    {
                        _scratch.Delete(path);
                    }
                }
                _runs.Clear();
                _runs.AddRange(next);
            }

            // Each key's first entry is the latest; only a txn's is held, as an older one may still win under the rules
            var previous = false;
            var heldTxn = false;
            Merge(_runs, raw =>
            {
                ArenaEntry.TryDecode(raw, out var entry);
                if (previous && entry.Key.SequenceEqual(_key.AsSpan(0, _keyLength)))
                {
                    if (heldTxn && !DeltaReconcileRules.LaterReplaces(RecordFamily.Txn, CheckpointRecord.TxnVersion(entry.Record), CheckpointRecord.TxnVersion(Decode(_entry).Record)))
                    {
                        Hold(raw);
                    }
                    return;
                }
                if (heldTxn)
                {
                    visitor(Decode(_entry));
                    heldTxn = false;
                }
                KeepKey(entry.Key);
                previous = true;
                if (CheckpointRecord.Family(CheckpointRecord.Kind(entry.Record)) == RecordFamily.Txn)
                {
                    Hold(raw);
                    heldTxn = true;
                }
                else
                {
                    visitor(entry);
                }
            }, cancellationToken);
            if (heldTxn)
            {
                visitor(Decode(_entry));
            }
            foreach (var path in _runs)
            {
                _scratch.Delete(path);
            }
            _runs.Clear();
            Free();
        }

        private static ArenaEntry Decode(byte[] buffer)
        {
            ArenaEntry.TryDecode(buffer, out var entry);
            return entry;
        }

        // Keeps a copy of the current winner, as the cursor that held it moves on
        private void Hold(ReadOnlySpan<byte> raw)
        {
            if (_entry.Length < raw.Length)
            {
                var grown = _memory.Allocate(Math.Max(raw.Length, 4096));
                if (_entry.Length > 0)
                {
                    _memory.Free(_entry);
                }
                _entry = grown;
            }
            raw.CopyTo(_entry);
        }

        private void KeepKey(ReadOnlySpan<byte> key)
        {
            if (_key.Length < key.Length)
            {
                var grown = _memory.Allocate(Math.Max(key.Length, 256));
                if (_key.Length > 0)
                {
                    _memory.Free(_key);
                }
                _key = grown;
            }
            key.CopyTo(_key);
            _keyLength = key.Length;
        }

        private void Free()
        {
            if (_entry.Length > 0)
            {
                _memory.Free(_entry);
                _entry = Array.Empty<byte>();
            }
            if (_key.Length > 0)
            {
                _memory.Free(_key);
                _key = Array.Empty<byte>();
            }
        }

        private delegate void RawVisitor(ReadOnlySpan<byte> raw);

        private void Merge(List<string> runs, RawVisitor visitor, CancellationToken cancellationToken)
        {
            var cursors = new List<RunCursor>();
            try
            {
                FaultForTests?.Invoke("merge");
                foreach (var path in runs)
                {
                    var cursor = new RunCursor(path, _memory);
                    cursors.Add(cursor);
                    cursor.MoveNext();
                }
                long merged = 0;
                while (true)
                {
                    if ((++merged & 1023) == 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    RunCursor? smallest = null;
                    foreach (var cursor in cursors)
                    {
                        if (cursor.HasCurrent && (smallest == null || ArenaEntry.Compare(cursor.Current, smallest.Current) < 0))
                        {
                            smallest = cursor;
                        }
                    }
                    if (smallest == null)
                    {
                        return;
                    }
                    visitor(smallest.CurrentRaw);
                    smallest.MoveNext();
                }
            }
            finally
            {
                foreach (var cursor in cursors)
                {
                    cursor.Dispose();
                }
            }
        }

        public void Dispose()
        {
            ReleaseBuffers();
            Free();
        }
    }
}
