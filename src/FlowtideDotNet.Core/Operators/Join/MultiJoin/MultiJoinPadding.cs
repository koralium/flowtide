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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// What a change on the right side of one left join did to the weight each key matches, summed per key over the whole change.
    /// </summary>
    internal sealed class MultiJoinPadTable
    {
        private readonly MultiJoinNode _join;
        private readonly int _id;
        private readonly int _keyCount;
        private readonly IColumn[] _writeColumns;
        private readonly EventBatchData _writeBatch;
        private readonly Column[] _keys;
        private MultiJoinTreeWriter? _writer;
        private IMemoryAllocator? _allocator;
        private int[] _sums = Array.Empty<int>();

        // Up to this many keys of a change are kept in memory, more go through the tree
        internal static int MemoryKeys = 1024;
        private readonly Column[] _memoryKeys;
        private readonly BatchSorter _memorySorter;
        private MemoryKeyComparer? _memoryComparer;
        private int[] _memoryWeights = Array.Empty<int>();
        private int[] _memorySorted = Array.Empty<int>();
        private int[] _memoryTags = Array.Empty<int>();
        private int[] _memoryFirst = Array.Empty<int>();
        private int[] _gatherRows = Array.Empty<int>();
        private int[] _gatherOffsets = Array.Empty<int>();
        private int[] _gatherPositions = Array.Empty<int>();
        private int _memoryCount;
        private bool _treeHasRows;
        private bool _treeStale;

        public MultiJoinPadTable(MultiJoinNode join, bool keptLeft, int id)
        {
            _join = join;
            KeptLeft = keptLeft;
            _id = id;
            _keyCount = join.RightKey!.Columns.Length;
            _writeColumns = new IColumn[_keyCount];
            _writeBatch = new EventBatchData(_writeColumns);
            _keys = new Column[_keyCount];
            _memoryKeys = new Column[_keyCount];
            _memorySorter = new BatchSorter(_keyCount);
        }

        public MultiJoinNode Join => _join;

        // The side whose rows are kept without a match, the keys are looked up in the other one
        public bool KeptLeft { get; }

        public bool HasRows { get; private set; }

        /// <summary>
        /// Keys met while the table is filled, for tests.
        /// </summary>
        internal long KeysAdded { get; private set; }

        /// <summary>
        /// Leaves of the tree a drain has read, for tests.
        /// </summary>
        internal int LeavesDrained { get; private set; }

        // Looks keys up in the right side as stored
        public MultiJoinCursor MatchTotals { get; set; } = null!;

        // Looks keys up in the left side and carries the rows on as padded rows
        public MultiJoinCursor PaddedRows { get; set; } = null!;

        public async Task Initialize(IStateManagerClient client, IMemoryAllocator allocator)
        {
            _allocator = allocator;
            var order = new List<int>(_keyCount);
            for (int i = 0; i < _keyCount; i++)
            {
                order.Add(i);
            }
            _writer = await MultiJoinInputState.CreateWriter(client, $"t{_id}", order, _keyCount, allocator, ephemeral: true);
            await _writer.Tree.Clear();
            _treeHasRows = false;
            _treeStale = false;
            ClearMemory();
        }

        /// <summary>
        /// Adds the weight of every key row that met a row of the kept side. Other keys cannot have padded rows.
        /// </summary>
        public ValueTask Add(IColumn[] keyColumns, int count, bool[] found, int[] weights)
        {
            var keys = _memoryKeys;
            if (keys[0] == null)
            {
                for (int c = 0; c < _keyCount; c++)
                {
                    keys[c] = Column.Create(_allocator!);
                }
            }
            if (_gatherRows.Length < count)
            {
                int size = Math.Max(count, _gatherRows.Length * 2);
                _gatherRows = new int[size];
                _gatherOffsets = new int[size];
                _gatherPositions = new int[size];
            }
            int added = 0;
            for (int i = 0; i < count; i++)
            {
                if (found[i])
                {
                    _gatherRows[added++] = i;
                }
            }
            if (added > 0)
            {
                if (_memoryWeights.Length < _memoryCount + added)
                {
                    Array.Resize(ref _memoryWeights, Math.Max(_memoryCount + added, _memoryWeights.Length * 2));
                }
                for (int i = 0; i < added; i++)
                {
                    _memoryWeights[_memoryCount + i] = weights[_gatherRows[i]];
                }
                for (int c = 0; c < _keyCount; c++)
                {
                    Gather(keys[c], keyColumns[c], added);
                }
                _memoryCount += added;
                KeysAdded += added;
            }
            HasRows = _memoryCount > 0 || _treeHasRows;
            // A change that touches many keys does not have to fit in memory
            return _memoryCount > MemoryKeys ? MoveToTree() : ValueTask.CompletedTask;
        }

        // Copies the rows named in the gather list, a view is read through to the column it is over
        private void Gather(Column target, IColumn source, int count)
        {
            ReadOnlySpan<int> rows = _gatherRows.AsSpan(0, count);
            if (source is ColumnWithOffset view)
            {
                var offsets = view.Offsets.Span;
                for (int i = 0; i < count; i++)
                {
                    _gatherOffsets[i] = offsets[_gatherRows[i]];
                }
                rows = _gatherOffsets.AsSpan(0, count);
                source = view.InnerColumn;
            }
            if (MultiJoinTreeLookup.AppendedAsNulls(target, source, count))
            {
                return;
            }
            if (MultiJoinTreeLookup.TakesRowsAsObjects(target, source))
            {
                MultiJoinTreeLookup.CopyRuns(target, source, rows);
                return;
            }
            _gatherPositions.AsSpan(0, count).Fill(target.Count);
            ReadOnlySpan<int> positions = _gatherPositions.AsSpan(0, count);
            target.InsertFrom(source, in rows, in positions, ColumnWithOffset.NullValueIndex);
        }

        private async ValueTask MoveToTree()
        {
            var writer = _writer!;
            if (_treeStale)
            {
                await writer.Tree.Clear();
                _treeStale = false;
            }
            int count = _memoryCount;
            int size = count * sizeof(int);
            // Set before the write: one that fails half way has left keys behind too
            _treeHasRows = true;
            try
            {
                for (int c = 0; c < _keyCount; c++)
                {
                    _writeColumns[c] = _memoryKeys[c];
                    size += _memoryKeys[c].GetByteSize();
                }
                writer.Prepare(_writeBatch, count);
                MemoryMarshal.Cast<int, MultiJoinWeight>(_memoryWeights.AsSpan(0, count)).CopyTo(writer.Values);
                await writer.Inserter.ApplyBatch(
                    writer.Refs,
                    writer.Values,
                    count,
                    writer.Sorted,
                    writer.Tags,
                    new MultiJoinSecondaryMutator(_keyCount),
                    size);
            }
            finally
            {
                writer.Release(count);
                Array.Clear(_writeColumns);
            }
            ClearMemory();
        }

        /// <summary>
        /// Forgets the keys of a message that was cut short, the ones already in the tree are cleared before it is used again.
        /// </summary>
        public void Abandon()
        {
            _treeStale |= _treeHasRows;
            _treeHasRows = false;
            ClearMemory();
        }

        private void ClearMemory()
        {
            for (int c = 0; c < _keyCount; c++)
            {
                _memoryKeys[c]?.Dispose();
                _memoryKeys[c] = null!;
            }
            _memoryCount = 0;
            HasRows = _treeHasRows;
        }

        // The distinct keys in memory whose weights do not add up to nothing: per key one of its rows and its sum
        private int SumMemoryKeys()
        {
            int count = _memoryCount;
            if (_memorySorted.Length < count)
            {
                int size = Math.Max(count, _memorySorted.Length * 2);
                _memorySorted = new int[size];
                _memoryTags = new int[size];
                _memoryFirst = new int[size];
            }
            if (_sums.Length < count)
            {
                _sums = new int[Math.Max(count, _sums.Length * 2)];
            }
            var sorted = _memorySorted.AsSpan(0, count);
            var tags = _memoryTags.AsSpan(0, count);
            for (int i = 0; i < count; i++)
            {
                sorted[i] = i;
            }
            if (MemoryKeysArePlain())
            {
                _memorySorter.SortDataWithTags(_memoryKeys, ref sorted, ref tags);
            }
            else
            {
                // The batch sorter reads such keys as an object per value
                var comparer = _memoryComparer ??= new MemoryKeyComparer(_memoryKeys);
                sorted.Sort(comparer);
                int tag = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i > 0 && comparer.Compare(sorted[i - 1], sorted[i]) != 0)
                    {
                        tag++;
                    }
                    tags[i] = tag;
                }
            }
            int distinct = 0;
            int row = 0;
            while (row < count)
            {
                int tag = tags[row];
                int first = sorted[row];
                int sum = 0;
                while (row < count && tags[row] == tag)
                {
                    sum += _memoryWeights[sorted[row]];
                    row++;
                }
                if (sum != 0)
                {
                    _memoryFirst[distinct] = first;
                    _sums[distinct] = sum;
                    distinct++;
                }
            }
            return distinct;
        }

        private bool MemoryKeysArePlain()
        {
            if (_keyCount > MultiJoinTreeLookup.MaxComparedInPlace)
            {
                return false;
            }
            for (int c = 0; c < _keyCount; c++)
            {
                if (!MultiJoinTreeLookup.IsPlain(_memoryKeys[c].Type))
                {
                    return false;
                }
            }
            return true;
        }

        // Orders the rows of the keys in memory by their values, read into containers
        private sealed class MemoryKeyComparer : IComparer<int>
        {
            private readonly Column[] _keys;
            private readonly DataValueContainer _x = new DataValueContainer();
            private readonly DataValueContainer _y = new DataValueContainer();

            public MemoryKeyComparer(Column[] keys)
            {
                _keys = keys;
            }

            public int Compare(int x, int y)
            {
                for (int c = 0; c < _keys.Length; c++)
                {
                    _keys[c].GetValueAt(x, _x, default);
                    _keys[c].GetValueAt(y, _y, default);
                    int compared = FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer.CompareTo(_x, _y);
                    if (compared != 0)
                    {
                        return compared;
                    }
                }
                return 0;
            }
        }

        // Every start row names its key row, the rows in order without the list
        private static void FillStart(MultiJoinFrame frame, int count, int[]? keyRows)
        {
            frame.EnsureCapacity(count);
            frame.Count = count;
            for (int i = 0; i < count; i++)
            {
                frame.Parent[i] = keyRows == null ? i : keyRows[i];
                frame.Weight[i] = 1;
            }
        }

        /// <summary>
        /// The rows whose padding changes.
        /// </summary>
        private int SelectChanged(MultiJoinFrame frame, int[] before, int count)
        {
            bool mark = _join.IsMark;
            frame.EnsureCapacity(mark ? count * 2 : count);
            if (mark && frame.Mark.Length < count * 2)
            {
                frame.Mark = new bool[Math.Max(count * 2, frame.Mark.Length * 2)];
            }
            frame.HasMark = mark;
            int selected = 0;
            for (int i = 0; i < count; i++)
            {
                bool matched = before[i] != 0;
                if (matched == (before[i] + _sums[i] != 0))
                {
                    continue;
                }
                if (mark)
                {
                    // The rows leave with the mark they had and come back with the other
                    frame.Parent[selected] = i;
                    frame.Weight[selected] = -1;
                    frame.Mark[selected] = matched;
                    selected++;
                    frame.Parent[selected] = i;
                    frame.Weight[selected] = 1;
                    frame.Mark[selected] = !matched;
                    selected++;
                    continue;
                }
                frame.Parent[selected] = i;
                frame.Weight[selected] = matched ? 1 : -1;
                selected++;
            }
            frame.Count = selected;
            return selected;
        }

        /// <summary>
        /// Empties the table and gives the changes to padded rows it leads to.
        /// </summary>
        public async IAsyncEnumerable<StreamEventBatch> Drain()
        {
            if (!HasRows)
            {
                yield break;
            }
            if (_treeStale)
            {
                // What a message that was cut short left in the tree goes at the next chance
                await _writer!.Tree.Clear();
                _treeStale = false;
            }
            if (!_treeHasRows)
            {
                // The usual case: the keys of the change are few and never left memory
                int distinct = SumMemoryKeys();
                try
                {
                    if (distinct > 0)
                    {
                        await foreach (var batch in DrainKeys(_memoryKeys, distinct, _memoryFirst))
                        {
                            yield return batch;
                        }
                    }
                }
                finally
                {
                    ClearMemory();
                }
                yield break;
            }
            if (_memoryCount > 0)
            {
                await MoveToTree();
            }
            var tree = _writer!.Tree;
            using (var iterator = tree.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    var leaf = page.CurrentPage;
                    int count = leaf.keys.Count;
                    if (count == 0)
                    {
                        continue;
                    }
                    LeavesDrained++;
                    // Copied out, the lookups below may move the page out of memory
                    if (_sums.Length < count)
                    {
                        _sums = new int[Math.Max(count, _sums.Length * 2)];
                    }
                    for (int i = 0; i < count; i++)
                    {
                        _sums[i] = leaf.values.Get(i).Value;
                    }

                    try
                    {
                        for (int c = 0; c < _keyCount; c++)
                        {
                            _keys[c] = Column.Create(_allocator!);
                            _keys[c].InsertRangeFrom(0, leaf.keys._data.Columns[c], 0, count);
                        }
                        await foreach (var batch in DrainKeys(_keys, count, null))
                        {
                            yield return batch;
                        }
                    }
                    finally
                    {
                        for (int c = 0; c < _keyCount; c++)
                        {
                            _keys[c]?.Dispose();
                            _keys[c] = null!;
                        }
                    }
                }
            }
            await tree.Clear();
            _treeHasRows = false;
            HasRows = false;
        }

        // The keys with the sums next to them: what each matched before decides whether its padded rows change
        private async IAsyncEnumerable<StreamEventBatch> DrainKeys(Column[] keys, int count, int[]? keyRows)
        {
            MatchTotals.Reset();
            FillStart(MatchTotals.StartFrame, count, keyRows);
            MatchTotals.Begin(keys, keyRows == null, null);
            while (await MatchTotals.MoveNext())
            {
            }

            PaddedRows.Reset();
            var start = PaddedRows.StartFrame;
            int selected = SelectChanged(start, MatchTotals.StartTotals, count);
            if (selected == 0)
            {
                yield break;
            }
            if (keyRows != null)
            {
                for (int i = 0; i < selected; i++)
                {
                    start.Parent[i] = keyRows[start.Parent[i]];
                }
            }
            PaddedRows.Begin(keys, keyRows == null && selected == count && !_join.IsMark, null);
            while (await PaddedRows.MoveNext())
            {
                yield return PaddedRows.Current;
            }
        }
    }
}
