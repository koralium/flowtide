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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using System.Diagnostics;
using System.Numerics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // Distinct stored delete rows and the copies each still needs
    internal sealed class DeleteBatch
    {
        private readonly IArrowArray[] _columns;
        private readonly IArrowComparer[] _comparers;
        private readonly bool _collide;
        private readonly int[] _head;
        private readonly int[] _next;
        private readonly uint[] _tag;
        private readonly int[] _row;
        private readonly long[] _remaining;
        // Copies asked for, kept only for change data
        private readonly long[]? _initial;
        private readonly PrefilterSet? _prefilter;
        private int _count;

        public DeleteBatch(RecordBatch stored, IReadOnlyList<long> demand, IArrowComparer[] comparers, bool changeData = false, bool collide = false, int prefilterColumn = -1)
        {
            Debug.Assert(stored.Length == demand.Count);
            _columns = new IArrowArray[stored.ColumnCount];
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i] = stored.Column(i);
            }
            _comparers = comparers;
            _collide = collide;
            var rows = stored.Length;
            _head = new int[BitOperations.RoundUpToPowerOf2((uint)Math.Max(rows, 1))];
            System.Array.Fill(_head, -1);
            _next = new int[rows];
            _tag = new uint[rows];
            _row = new int[rows];
            _remaining = new long[rows];

            // Stored rows that are equal share one entry
            for (int row = 0; row < rows; row++)
            {
                var hashed = TryHash(_columns, row, out var hash);
                Debug.Assert(hashed, "Stored timestamps are whole milliseconds");
                var entry = Find(hash, _columns, row);
                if (entry < 0)
                {
                    entry = _count++;
                    var bucket = Bucket(hash);
                    _next[entry] = _head[bucket];
                    _head[bucket] = entry;
                    _tag[entry] = (uint)(hash >> 32);
                    _row[entry] = row;
                }
                _remaining[entry] = checked(_remaining[entry] + demand[row]);
                Unresolved = checked(Unresolved + demand[row]);
            }
            if (changeData)
            {
                _initial = _remaining.AsSpan(0, _count).ToArray();
            }
            PrefilterColumn = prefilterColumn;
            if (prefilterColumn >= 0)
            {
                // The column's hash of each entry, as a file row's would be
                var values = new ulong[_count];
                for (int entry = 0; entry < _count; entry++)
                {
                    _comparers[prefilterColumn].TryHash(_columns[prefilterColumn], _row[entry], ref values[entry]);
                }
                _prefilter = new PrefilterSet(values);
            }
        }

        public int Count => _count;

        // The table column tested first, -1 when off
        public int PrefilterColumn { get; }

        // Copies still to find over every entry
        public long Unresolved { get; private set; }

        public long Remaining(int entry) => _remaining[entry];

        // The stored row each entry stands for
        public ReadOnlySpan<int> EntryRows => _row.AsSpan(0, _count);

        // The first entry with copies still to find, -1 for none
        public int FirstUnresolved()
        {
            for (int entry = 0; entry < _count; entry++)
            {
                if (_remaining[entry] > 0)
                {
                    return entry;
                }
            }
            return -1;
        }

        // Copies claimed so far, with change data on
        public long Matched(int entry)
        {
            Debug.Assert(_initial != null, "Matched counts are kept only with change data on");
            return _initial[entry] - _remaining[entry];
        }

        // False when no stored row can equal the row
        public bool TryHash(IArrowArray[] columns, int index, out ulong hash)
        {
            ulong state = 0;
            for (int i = 0; i < _comparers.Length; i++)
            {
                if (!_comparers[i].TryHash(columns[i], index, ref state))
                {
                    hash = 0;
                    return false;
                }
            }
            hash = _collide ? 0 : ArrowHash.Mix(state);
            return true;
        }

        // Row hashes of a file batch, column by column
        public void HashBatch(IArrowArray[] columns, Span<ulong> hashes, Span<bool> rejected)
        {
            hashes.Clear();
            rejected.Clear();
            for (int i = 0; i < _comparers.Length; i++)
            {
                _comparers[i].HashColumn(columns[i], hashes, rejected);
            }
            for (int row = 0; row < hashes.Length; row++)
            {
                hashes[row] = _collide ? 0 : ArrowHash.Mix(hashes[row]);
            }
        }

        // Hashes of the prefilter column alone
        public void HashPrefilter(IArrowArray column, Span<ulong> hashes, Span<bool> rejected)
        {
            hashes.Clear();
            rejected.Clear();
            _comparers[PrefilterColumn].HashColumn(column, hashes, rejected);
        }

        // False when no entry holds the column hash
        public bool MayMatch(ulong columnHash)
        {
            return _prefilter!.MayContain(columnHash);
        }

        // The equal entry, or -1
        public int Find(ulong hash, IArrowArray[] columns, int index)
        {
            var tag = (uint)(hash >> 32);
            for (var entry = _head[Bucket(hash)]; entry >= 0; entry = _next[entry])
            {
                if (_tag[entry] == tag && IsEqual(entry, columns, index))
                {
                    return entry;
                }
            }
            return -1;
        }

        public void Claim(int entry)
        {
            Debug.Assert(_remaining[entry] > 0);
            _remaining[entry]--;
            Unresolved--;
        }

        private int Bucket(ulong hash)
        {
            return (int)(hash & (ulong)(_head.Length - 1));
        }

        private bool IsEqual(int entry, IArrowArray[] columns, int index)
        {
            var row = _row[entry];
            for (int i = 0; i < _comparers.Length; i++)
            {
                if (!_comparers[i].IsEqual(row, _columns[i], index, columns[i]))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
