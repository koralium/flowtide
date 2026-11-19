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

        // With a condition on both sides: the columns of the padded input it reads, kept next to the key
        private readonly int[] _extra;
        private readonly int[] _paddedKey;
        private readonly int _columnCount;
        private readonly int _memoryColumns;
        // Never set back: entries in memory keep their numbers when a stale tree is cleared under them
        private long _nextEntry;
        private readonly Column[] _turnKeys;
        private MemoryKeyComparer? _turnComparer;
        private int[] _groupStart = Array.Empty<int>();
        private int[] _groupRow = Array.Empty<int>();
        private int[] _gatherStart = Array.Empty<int>();

        // The entries of a change that does not fit in memory: key and condition columns with their sum, looked up by key
        private MultiJoinTreeWriter? _entryWriter;
        private MultiJoinTreeLookup? _entryLookup;
        private readonly IColumn[] _entryWriteColumns;
        private readonly EventBatchData _entryWriteBatch;
        private int[] _ones = Array.Empty<int>();

        // When the kept columns the conditions read are on the input of the key: the kept rows a change joined, by key and those columns
        private readonly bool _fast;
        private readonly int[] _startColumns;
        private readonly int _treeKeyCount;
        private readonly Column[] _affected;
        private MemoryKeyComparer? _affectedComparer;
        private int _affectedCount;
        private int[] _affectedSorted = Array.Empty<int>();
        private int[] _startRows = Array.Empty<int>();
        private int[] _turnGroupOf = Array.Empty<int>();
        private readonly DataValueContainer _crossLeft = new DataValueContainer();
        private readonly DataValueContainer _crossRight = new DataValueContainer();
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
            (KeptInput, _startColumns, _fast) = StartColumnsOf(join, keptLeft);
            _treeKeyCount = _fast ? _startColumns.Length : _keyCount;
            _affected = new Column[_fast ? _startColumns.Length : 0];
            _writeColumns = new IColumn[_treeKeyCount];
            _writeBatch = new EventBatchData(_writeColumns);
            _keys = new Column[_treeKeyCount];
            var padded = keptLeft ? join.RightKey! : join.LeftKey!;
            PaddedInput = padded.Input;
            _paddedKey = padded.Columns;
            _extra = join.PadPerRow
                ? join.Checks.SelectMany(x => x.Reads).Where(x => !x.IsMark && x.Input == padded.Input).Select(x => x.Column).Distinct().ToArray()
                : Array.Empty<int>();
            _columnCount = _keyCount + _extra.Length;
            // Entries read row by row get a number after their columns: it tells them apart in their tree without comparing the columns
            _memoryColumns = join.PadPerRow ? _columnCount + 1 : _columnCount;
            _turnKeys = new Column[_keyCount];
            _entryWriteColumns = new IColumn[_memoryColumns];
            _entryWriteBatch = new EventBatchData(_entryWriteColumns);
            _memoryKeys = new Column[_memoryColumns];
            _memorySorter = new BatchSorter(_columnCount);
        }

        // The input whose rows are filled with nulls
        public int PaddedInput { get; }

        // The input of the kept side that holds the key
        public int KeptInput { get; }

        // Per start row of the drain the group of entries with its key
        public int[] TurnGroupOf => _turnGroupOf;

        /// <summary>
        /// The columns of the kept key input a drain starts from. Fast: the key and the columns of that input the conditions read, so only rows a change joined are looked at.
        /// </summary>
        internal static (int Input, int[] Columns, bool Fast) StartColumnsOf(MultiJoinNode join, bool keptLeft)
        {
            var kept = keptLeft ? join.LeftKey : join.RightKey;
            if (!join.PadPerRow || kept == null)
            {
                // The drain starts from the keys the table holds
                return (kept?.Input ?? -1, kept?.Columns ?? Array.Empty<int>(), false);
            }
            var padded = keptLeft ? join.RightKey! : join.LeftKey!;
            var reads = join.Checks.SelectMany(x => x.Reads).Where(x => !x.IsMark).ToList();
            if (join.Checks.Exists(x => x.ReadsInside))
            {
                // The kept rows would be told apart by whole maps or lists, of which the condition reads one value
                return (kept.Input, kept.Columns, false);
            }
            if (reads.Exists(x => x.Input != kept.Input && x.Input != padded.Input))
            {
                // A condition reads another input of the kept side, the drain starts from the keys
                return (kept.Input, kept.Columns, false);
            }
            var columns = kept.Columns.Concat(reads.Where(x => x.Input == kept.Input).Select(x => x.Column).Where(x => !kept.Columns.Contains(x)).Distinct()).ToArray();
            return (kept.Input, columns, true);
        }

        /// <summary>
        /// Remembers kept rows a change joined: the columns at the positions, of the rows named.
        /// </summary>
        public void AddAffected(Column[] columns, int[] positions, ReadOnlySpan<int> rows)
        {
            if (_affected[0] == null)
            {
                for (int c = 0; c < _affected.Length; c++)
                {
                    _affected[c] = Column.Create(_allocator!);
                }
            }
            if (_gatherOffsets.Length < rows.Length)
            {
                _gatherOffsets = new int[rows.Length];
                _gatherPositions = new int[rows.Length];
            }
            for (int c = 0; c < _affected.Length; c++)
            {
                Gather(_affected[c], columns[positions[c]], rows);
            }
            _affectedCount += rows.Length;
        }

        public bool NeedsSpill => _affectedCount > MemoryKeys;

        public ValueTask Spill() => MoveToTree();

        // The key columns of that input, in the order the table keeps them
        public int[] PaddedKey => _paddedKey;

        // The entries are in their tree, a step reads them through the lookup
        public bool TurnInTree { get; private set; }

        public MultiJoinTreeLookup EntryLookup => _entryLookup!;

        // Where the tree of entries keeps a column of the padded input, -1 when it does not
        public int TurnTreeColumn(int column)
        {
            int extra = Array.IndexOf(_extra, column);
            return extra >= 0 ? _keyCount + extra : Array.IndexOf(_paddedKey, column);
        }

        // Per key the first of its entries, the entries of a key stand together
        public int[] TurnGroupStart => _groupStart;

        public int[] TurnEntryRows => _memoryFirst;

        public int[] TurnEntrySums => _sums;

        // The values the entries have for a column of the padded input, null when the table does not keep it
        public Column? TurnColumn(int column)
        {
            int extra = Array.IndexOf(_extra, column);
            if (extra >= 0)
            {
                return _memoryKeys[_keyCount + extra];
            }
            int key = Array.IndexOf(_paddedKey, column);
            return key >= 0 ? _memoryKeys[key] : null;
        }

        public void CopyEntries(Column target, Column source, ReadOnlySpan<int> rows)
        {
            if (_gatherOffsets.Length < rows.Length)
            {
                _gatherOffsets = new int[rows.Length];
                _gatherPositions = new int[rows.Length];
            }
            Gather(target, source, rows);
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
            var order = new List<int>(_treeKeyCount);
            for (int i = 0; i < _treeKeyCount; i++)
            {
                order.Add(i);
            }
            _writer = await MultiJoinInputState.CreateWriter(client, $"t{_id}", order, _treeKeyCount, allocator, ephemeral: true);
            await _writer.Tree.Clear();
            if (_join.PadPerRow)
            {
                // Ordered by the key and the number of the entry
                var all = new List<int>(_keyCount + 1);
                for (int i = 0; i < _keyCount; i++)
                {
                    all.Add(i);
                }
                all.Add(_columnCount);
                _entryWriter = await MultiJoinInputState.CreateWriter(client, $"e{_id}", all, _memoryColumns, allocator, ephemeral: true, orderIsUnique: true);
                await _entryWriter.Tree.Clear();
                _entryLookup = new MultiJoinTreeLookup(_entryWriter.Tree, Enumerable.Range(0, _keyCount).ToList());
            }
            _treeHasRows = false;
            _treeStale = false;
            ClearMemory();
        }

        /// <summary>
        /// Adds the weight of every key row that met a row of the kept side. Other keys cannot have padded rows.
        /// </summary>
        public ValueTask Add(IColumn[] keyColumns, int count, bool[] found, int[] weights, IReadOnlyList<IColumn>? startColumns = null, int[]? startRows = null)
        {
            var keys = _memoryKeys;
            if (keys[0] == null)
            {
                for (int c = 0; c < _memoryColumns; c++)
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
                _gatherStart = new int[size];
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
                if (_extra.Length > 0)
                {
                    // Read from the rows of the change as they arrived
                    ReadOnlySpan<int> rows = _gatherRows.AsSpan(0, added);
                    if (startRows != null)
                    {
                        for (int i = 0; i < added; i++)
                        {
                            _gatherStart[i] = startRows[_gatherRows[i]];
                        }
                        rows = _gatherStart.AsSpan(0, added);
                    }
                    for (int e = 0; e < _extra.Length; e++)
                    {
                        Gather(keys[_keyCount + e], startColumns![_extra[e]], rows);
                    }
                }
                if (_join.PadPerRow)
                {
                    for (int i = 0; i < added; i++)
                    {
                        keys[_columnCount].Add(new Int64Value(_nextEntry++));
                    }
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
            Gather(target, source, _gatherRows.AsSpan(0, count));
        }

        private void Gather(Column target, IColumn source, ReadOnlySpan<int> rows)
        {
            int count = rows.Length;
            if (source is ColumnWithOffset view)
            {
                var offsets = view.Offsets.Span;
                for (int i = 0; i < count; i++)
                {
                    _gatherOffsets[i] = offsets[rows[i]];
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
            if (_treeStale)
            {
                await ClearTrees();
                _treeStale = false;
            }
            int count = _memoryCount;
            // Set before the write: one that fails half way has left keys behind too
            _treeHasRows = true;
            if (_join.PadPerRow)
            {
                // The keys are counted, not summed: a key whose entries add up to nothing still has rows to look at
                int starts = _fast ? _affectedCount : count;
                if (_ones.Length < starts)
                {
                    _ones = new int[Math.Max(starts, _ones.Length * 2)];
                    Array.Fill(_ones, 1);
                }
                if (starts > 0)
                {
                    await Write(_writer!, _writeColumns, _writeBatch, _fast ? _affected : _memoryKeys, _treeKeyCount, _ones, starts);
                }
                if (count > 0)
                {
                    await Write(_entryWriter!, _entryWriteColumns, _entryWriteBatch, _memoryKeys, _memoryColumns, _memoryWeights, count);
                }
            }
            else
            {
                await Write(_writer!, _writeColumns, _writeBatch, _memoryKeys, _keyCount, _memoryWeights, count);
            }
            ClearMemory();
        }

        private static async ValueTask Write(MultiJoinTreeWriter writer, IColumn[] writeColumns, EventBatchData writeBatch, Column[] source, int columns, int[] weights, int count)
        {
            int size = count * sizeof(int);
            try
            {
                for (int c = 0; c < columns; c++)
                {
                    writeColumns[c] = source[c];
                    size += source[c].GetByteSize();
                }
                writer.Prepare(writeBatch, count);
                MemoryMarshal.Cast<int, MultiJoinWeight>(weights.AsSpan(0, count)).CopyTo(writer.Values);
                await writer.Inserter.ApplyBatch(
                    writer.Refs,
                    writer.Values,
                    count,
                    writer.Sorted,
                    writer.Tags,
                    new MultiJoinSecondaryMutator(columns),
                    size);
            }
            finally
            {
                writer.Release(count);
                Array.Clear(writeColumns);
            }
        }

        private async ValueTask ClearTrees()
        {
            await _writer!.Tree.Clear();
            if (_entryWriter != null)
            {
                await _entryWriter.Tree.Clear();
            }
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
            for (int c = 0; c < _affected.Length; c++)
            {
                _affected[c]?.Dispose();
                _affected[c] = null!;
            }
            _affectedCount = 0;
            for (int c = 0; c < _memoryColumns; c++)
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
            if (_columnCount > MultiJoinTreeLookup.MaxComparedInPlace)
            {
                return false;
            }
            for (int c = 0; c < _columnCount; c++)
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
                await ClearTrees();
                _treeStale = false;
            }
            if (_join.PadPerRow && !_treeHasRows)
            {
                await foreach (var batch in DrainTurned())
                {
                    yield return batch;
                }
                yield break;
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
            if (_memoryCount > 0 || _affectedCount > 0)
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
                        for (int c = 0; c < _treeKeyCount; c++)
                        {
                            _keys[c] = Column.Create(_allocator!);
                            _keys[c].InsertRangeFrom(0, leaf.keys._data.Columns[c], 0, count);
                        }
                        await foreach (var batch in _join.PadPerRow ? DrainTurnedKeys(_keys, count) : DrainKeys(_keys, count, null))
                        {
                            yield return batch;
                        }
                    }
                    finally
                    {
                        for (int c = 0; c < _treeKeyCount; c++)
                        {
                            _keys[c]?.Dispose();
                            _keys[c] = null!;
                        }
                    }
                }
            }
            await ClearTrees();
            _treeHasRows = false;
            HasRows = false;
        }

        // The keys of a leaf of the key tree, their entries are read from the tree of entries
        private async IAsyncEnumerable<StreamEventBatch> DrainTurnedKeys(Column[] keys, int count)
        {
            PaddedRows.Reset();
            FillStart(PaddedRows.StartFrame, count, null);
            PaddedRows.TurnTable = this;
            TurnInTree = true;
            try
            {
                PaddedRows.Begin(keys, true, null);
                while (await PaddedRows.MoveNext())
                {
                    yield return PaddedRows.Current;
                }
            }
            finally
            {
                TurnInTree = false;
            }
        }

        // Rows turn one by one: the kept rows of every key are read with what they match as stored and what the entries of the key add
        private async IAsyncEnumerable<StreamEventBatch> DrainTurned()
        {
            try
            {
                for (int c = 0; c < _keyCount; c++)
                {
                    _turnKeys[c] = _memoryKeys[c];
                }
                int groups = GroupEntries();
                int starts = _fast ? AffectedStarts(groups) : KeyStarts(groups);
                if (starts == 0)
                {
                    yield break;
                }
                PaddedRows.Reset();
                FillStart(PaddedRows.StartFrame, starts, _fast ? _startRows : _groupRow);
                PaddedRows.TurnTable = this;
                PaddedRows.Begin(_fast ? _affected : _turnKeys, false, null);
                while (await PaddedRows.MoveNext())
                {
                    yield return PaddedRows.Current;
                }
            }
            finally
            {
                Array.Clear(_turnKeys);
                ClearMemory();
            }
        }

        // Every key is a start row and has its own group
        private int KeyStarts(int groups)
        {
            if (_turnGroupOf.Length < groups)
            {
                _turnGroupOf = new int[Math.Max(groups, _turnGroupOf.Length * 2)];
            }
            for (int i = 0; i < groups; i++)
            {
                _turnGroupOf[i] = i;
            }
            return groups;
        }

        // The kept rows a change joined, each key and columns once, with the group of entries of their key. Rows whose key has no entry left cannot turn
        private int AffectedStarts(int groups)
        {
            int count = _affectedCount;
            if (_affectedSorted.Length < count)
            {
                int size = Math.Max(count, _affectedSorted.Length * 2);
                _affectedSorted = new int[size];
                _startRows = new int[size];
                _turnGroupOf = new int[size];
            }
            if (count == 0 || groups == 0)
            {
                return 0;
            }
            var sorted = _affectedSorted.AsSpan(0, count);
            for (int i = 0; i < count; i++)
            {
                sorted[i] = i;
            }
            var comparer = _affectedComparer ??= new MemoryKeyComparer(_affected);
            sorted.Sort(comparer);
            int starts = 0;
            int group = 0;
            for (int i = 0; i < count; i++)
            {
                int row = sorted[i];
                if (i > 0 && comparer.Compare(sorted[i - 1], row) == 0)
                {
                    continue;
                }
                while (group < groups && CompareKeys(row, _groupRow[group]) > 0)
                {
                    group++;
                }
                if (group == groups)
                {
                    break;
                }
                if (CompareKeys(row, _groupRow[group]) != 0)
                {
                    continue;
                }
                _startRows[starts] = row;
                _turnGroupOf[starts] = group;
                starts++;
            }
            return starts;
        }

        // The key of a kept row against the key of an entry
        private int CompareKeys(int affectedRow, int entryRow)
        {
            for (int c = 0; c < _keyCount; c++)
            {
                _affected[c].GetValueAt(affectedRow, _crossLeft, default);
                _memoryKeys[c].GetValueAt(entryRow, _crossRight, default);
                int compared = FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer.CompareTo(_crossLeft, _crossRight);
                if (compared != 0)
                {
                    return compared;
                }
            }
            return 0;
        }

        // Puts the entries in the order of their key, by the comparer the kept rows are ordered by too. Only the key is compared: the columns the conditions read may hold maps or lists
        private int GroupEntries()
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
            if (_groupStart.Length < count + 1)
            {
                _groupStart = new int[Math.Max(count + 1, _groupStart.Length * 2)];
                _groupRow = new int[_groupStart.Length];
            }
            var sorted = _memorySorted.AsSpan(0, count);
            for (int i = 0; i < count; i++)
            {
                sorted[i] = i;
            }
            var comparer = _turnComparer ??= new MemoryKeyComparer(_turnKeys);
            sorted.Sort(comparer);
            int groups = 0;
            for (int d = 0; d < count; d++)
            {
                int row = sorted[d];
                _memoryFirst[d] = row;
                _sums[d] = _memoryWeights[row];
                if (d == 0 || comparer.Compare(sorted[d - 1], row) != 0)
                {
                    _groupStart[groups] = d;
                    _groupRow[groups] = row;
                    groups++;
                }
            }
            _groupStart[groups] = count;
            return groups;
        }

        // The keys with the sums next to them: what each matched before decides whether its padded rows change
        private async IAsyncEnumerable<StreamEventBatch> DrainKeys(Column[] keys, int count, int[]? keyRows)
        {
            MatchTotals.Reset();
            FillStart(MatchTotals.StartFrame, count, keyRows);
            MatchTotals.Begin(keys, keyRows == null, null);
            while (await MatchTotals.MoveNext())
            {
                // Run to its end for the totals, its rows are not used
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
