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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References
{
    /// <summary>
    /// A ledger-charged set of comparison keys, with exact deletion paths for candidates, that spills through the tail reducer and seals into one sorted run on scratch.
    /// </summary>
    internal sealed class PathSet : IDisposable
    {
        public const long DefaultArenaCap = 16L << 20;

        // Ends the comparison key, below every byte a key holds
        internal const byte Separator = 0;

        private readonly JobScratch _scratch;
        private readonly JobMemory _memory;
        private TailReducer? _reducer;
        private CheckpointSorter? _sorter;
        private byte[] _buffer = Array.Empty<byte>();
        private string? _run;
        private long _ordinal;
        private long _spillBytes;

        public PathSet(JobScratch scratch, JobMemory memory, PathRole role, long arenaCap = DefaultArenaCap)
        {
            _scratch = scratch;
            _memory = memory;
            Role = role;
            _reducer = new TailReducer(memory, arenaCap);
        }

        /// <summary>
        /// A set for keys of one canonicalizer domain only.
        /// </summary>
        public PathSet(JobScratch scratch, JobMemory memory, PathKeyDomain domain, PathRole role, long arenaCap = DefaultArenaCap)
            : this(scratch, memory, role, arenaCap)
        {
            Domain = domain;
        }

        // Whether the set protects what it holds or offers it for deletion, fixed at creation
        public PathRole Role { get; }

        // The canonicalizer domain of its keys, given at creation or taken from the first extractor; null for keys added without one
        public PathKeyDomain? Domain { get; private set; }

        // Entries offered, repeats included
        public long Added { get; private set; }

        // Distinct entries, known once sealed
        public long Count { get; private set; }

        // Sorted runs written while adding
        public int SpilledRuns { get; private set; }

        // Bytes written to scratch: spilled runs, merge passes and the sealed run
        public long SpillBytes => _spillBytes + (_sorter?.SpillBytes ?? 0);

        public bool Sealed => _run != null;

        /// <summary>
        /// Whether the set holds keys of the domain: a set with a domain only its own, an empty set without one takes it.
        /// </summary>
        public bool TryBind(PathKeyDomain domain)
        {
            if (Domain is { } bound)
            {
                return bound == domain;
            }
            // Keys added without a canonicalizer belong to no domain
            if (Added > 0)
            {
                return false;
            }
            Domain = domain;
            return true;
        }

        /// <summary>
        /// Adds a key, with the exact deletion path of a candidate; a repeated entry keeps its first payload, and after a ledger decline the set is only disposed.
        /// </summary>
        public void Add(string key, string? deletionPath = null, ReadOnlySpan<byte> payload = default)
        {
            var reducer = _reducer ?? throw new InvalidOperationException("A sealed path set takes no entries");
            if (key.Length == 0 || key.Contains('\0') || (deletionPath != null && deletionPath.Contains('\0')))
            {
                throw new ArgumentException("A path key is empty or holds the separator", nameof(key));
            }
            var keyBytes = Encoding.UTF8.GetByteCount(key);
            var length = keyBytes + 1 + (deletionPath == null ? 0 : Encoding.UTF8.GetByteCount(deletionPath));
            Reserve(length);
            Encoding.UTF8.GetBytes(key, _buffer);
            _buffer[keyBytes] = Separator;
            if (deletionPath != null)
            {
                Encoding.UTF8.GetBytes(deletionPath, _buffer.AsSpan(keyBytes + 1));
            }
            var entry = _buffer.AsSpan(0, length);
            // A repeat keeps the earlier entry and appends nothing
            var ordinal = -(++_ordinal);
            if (!reducer.TryAdd(entry, 0, ordinal, payload))
            {
                _sorter ??= new CheckpointSorter(_scratch, _memory);
                _sorter.Spill(reducer);
                SpilledRuns++;
                // An emptied reducer takes any entry
                if (!reducer.TryAdd(entry, 0, ordinal, payload))
                {
                    throw new InvalidOperationException("The emptied tail reducer refused an entry");
                }
            }
            Added++;
        }

        /// <summary>
        /// Sorts and deduplicates every entry into one run on scratch and releases all sorter memory; the run keeps its disk charge until disposal.
        /// </summary>
        public void Seal(CancellationToken cancellationToken = default)
        {
            var reducer = _reducer ?? throw new InvalidOperationException("The path set is already sealed");
            // The set keeps its reducer for disposal until the sealed run's writer exists
            var sealedRun = new RunWriter(_scratch, _memory);
            _reducer = null;
            long count = 0;
            try
            {
                if (_sorter == null)
                {
                    count = reducer.Count;
                    reducer.DrainSorted(sealedRun);
                }
                else
                {
                    _sorter.Reduce(reducer, (in ArenaEntry entry) =>
                    {
                        var length = ArenaEntry.EncodedLength(entry.Key.Length, entry.Record.Length);
                        Reserve(length);
                        ArenaEntry.Encode(_buffer, entry.Key, entry.Version, entry.Ordinal, entry.Record);
                        sealedRun.Write(_buffer.AsSpan(0, length));
                        count++;
                    }, cancellationToken);
                }
                sealedRun.Close();
            }
            catch
            {
                sealedRun.Dispose();
                _scratch.Delete(sealedRun.Path);
                reducer.Dispose();
                Release();
                throw;
            }
            sealedRun.Dispose();
            reducer.Dispose();
            if (_sorter != null)
            {
                _spillBytes += _sorter.SpillBytes;
            }
            Release();
            _run = sealedRun.Path;
            _spillBytes += new FileInfo(_run).Length;
            Count = count;
        }

        /// <summary>
        /// Streams the sealed entries in key order through one run cursor.
        /// </summary>
        public PathSetReader Open()
        {
            var run = _run ?? throw new InvalidOperationException("Only a sealed path set can be read");
            return new PathSetReader(new RunCursor(run, _memory));
        }

        /// <summary>
        /// Streams the candidates whose comparison key no protection entry holds, both sides read once in key order; each set must have been created for its side, and both hold keys of one domain.
        /// </summary>
        public static PathSetDifference Difference(PathSet candidates, PathSet protection)
        {
            if (candidates.Role != PathRole.Candidate || protection.Role != PathRole.Protection)
            {
                throw new ArgumentException($"A difference takes a candidate set and a protection set, not {candidates.Role} and {protection.Role}");
            }
            // Keys of different domains never compare equal, so nothing would be protected
            if (candidates.Domain != protection.Domain)
            {
                throw new ArgumentException($"A difference takes two sets of one key domain, not {candidates.Domain?.Identity} and {protection.Domain?.Identity}");
            }
            var left = candidates.Open();
            try
            {
                return new PathSetDifference(left, protection.Open());
            }
            catch
            {
                left.Dispose();
                throw;
            }
        }

        private void Reserve(int length)
        {
            if (_buffer.Length >= length)
            {
                return;
            }
            var grown = _memory.Allocate(Math.Max(length, 256));
            if (_buffer.Length > 0)
            {
                _memory.Free(_buffer);
            }
            _buffer = grown;
        }

        // Sorter memory and unmerged runs never outlive building
        private void Release()
        {
            _sorter?.Discard();
            _sorter = null;
            if (_buffer.Length > 0)
            {
                _memory.Free(_buffer);
                _buffer = Array.Empty<byte>();
            }
        }

        public void Dispose()
        {
            _reducer?.Dispose();
            _reducer = null;
            Release();
            if (_run != null)
            {
                _scratch.Delete(_run);
                _run = null;
            }
        }
    }

    /// <summary>
    /// One sealed path set in key order: comparison key, deletion path and payload of each entry.
    /// </summary>
    internal sealed class PathSetReader : IDisposable
    {
        private readonly RunCursor _cursor;
        private int _split;

        internal PathSetReader(RunCursor cursor)
        {
            _cursor = cursor;
        }

        public bool MoveNext()
        {
            if (!_cursor.MoveNext())
            {
                return false;
            }
            _split = _cursor.Current.Key.IndexOf(PathSet.Separator);
            if (_split < 0)
            {
                throw new InvalidDataException("A path set entry has no separator");
            }
            return true;
        }

        public ReadOnlySpan<byte> Key => _cursor.Current.Key.Slice(0, _split);

        // Empty for a protection entry
        public ReadOnlySpan<byte> DeletionPath => _cursor.Current.Key.Slice(_split + 1);

        public ReadOnlySpan<byte> Payload => _cursor.Current.Record;

        public void Dispose() => _cursor.Dispose();
    }

    /// <summary>
    /// Candidates minus protection, merged in key order with one cursor on each side; an entry without a deletion path is never emitted.
    /// </summary>
    internal sealed class PathSetDifference : IDisposable
    {
        private readonly PathSetReader _candidates;
        private readonly PathSetReader _protection;
        private bool _started;
        private bool _protectionHas;

        internal PathSetDifference(PathSetReader candidates, PathSetReader protection)
        {
            _candidates = candidates;
            _protection = protection;
        }

        // Candidates a protection key held
        public long Protected { get; private set; }

        // Candidates without a deletion path
        public long Untargeted { get; private set; }

        public long Emitted { get; private set; }

        public bool MoveNext()
        {
            if (!_started)
            {
                _started = true;
                _protectionHas = _protection.MoveNext();
            }
            while (_candidates.MoveNext())
            {
                var key = _candidates.Key;
                while (_protectionHas && _protection.Key.SequenceCompareTo(key) < 0)
                {
                    _protectionHas = _protection.MoveNext();
                }
                if (_protectionHas && _protection.Key.SequenceEqual(key))
                {
                    Protected++;
                    continue;
                }
                if (_candidates.DeletionPath.IsEmpty)
                {
                    Untargeted++;
                    continue;
                }
                Emitted++;
                return true;
            }
            return false;
        }

        public ReadOnlySpan<byte> Key => _candidates.Key;

        public ReadOnlySpan<byte> DeletionPath => _candidates.DeletionPath;

        public ReadOnlySpan<byte> Payload => _candidates.Payload;

        public string DeletionPathText => Encoding.UTF8.GetString(_candidates.DeletionPath);

        public void Dispose()
        {
            _candidates.Dispose();
            _protection.Dispose();
        }
    }
}
