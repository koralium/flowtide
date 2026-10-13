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
    /// Sort mode: the tail reducer drains into sorted runs, which are merged with fan-in 8 and reduced to one entry per key.
    /// </summary>
    internal sealed class CheckpointSorter : IDisposable
    {
        public const int FanIn = 8;

        private readonly JobScratch _scratch;
        private readonly JobMemory _memory;
        private readonly List<string> _runs = new List<string>();
        private byte[] _key = Array.Empty<byte>();
        private int _keyLength;

        public CheckpointSorter(JobScratch scratch, JobMemory memory)
        {
            _scratch = scratch;
            _memory = memory;
        }

        public int Runs => _runs.Count;

        // Throws at a run write or at the start of a merge, so tests can fail the build there
        public Action<string>? FaultForTests { get; init; }

        // Bytes written to runs, merge passes included
        public long SpillBytes { get; private set; }

        /// <summary>
        /// Writes what the reducer holds as a run in key order, which empties the reducer.
        /// </summary>
        public void Spill(TailReducer reducer)
        {
            FaultForTests?.Invoke("run write");
            using var run = new RunWriter(_scratch, _memory);
            reducer.DrainSorted(run);
            run.Close();
            _runs.Add(run.Path);
            SpillBytes += new FileInfo(run.Path).Length;
        }

        /// <summary>
        /// Spills what the reducer still holds, merges every run and calls the visitor once per key, in key order, with the key's latest entry.
        /// </summary>
        public void Reduce(TailReducer reducer, EntryVisitor visitor, CancellationToken cancellationToken = default)
        {
            if (reducer.Count > 0)
            {
                Spill(reducer);
            }
            // The merge phase holds no reducer
            reducer.Clear();
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

            // Each key's first entry is the latest, and the only one written
            var previous = false;
            Merge(_runs, raw =>
            {
                ArenaEntry.TryDecode(raw, out var entry);
                if (previous && entry.Key.SequenceEqual(_key.AsSpan(0, _keyLength)))
                {
                    return;
                }
                KeepKey(entry.Key);
                previous = true;
                visitor(entry);
            }, cancellationToken);
            foreach (var path in _runs)
            {
                _scratch.Delete(path);
            }
            _runs.Clear();
            Free();
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

        public void Dispose() => Free();
    }
}
