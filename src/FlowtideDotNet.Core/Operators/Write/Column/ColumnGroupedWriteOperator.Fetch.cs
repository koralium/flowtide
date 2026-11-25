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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using StoreColumn = FlowtideDotNet.Core.ColumnStore.Column;

namespace FlowtideDotNet.Core.Operators.Write.Column
{
    // The sink as host of a fetch: its tree keeps references
    public abstract partial class ColumnGroupedWriteOperator : IRowStoreFetchHostVertex
    {
        private const int FetchChunkRows = 1024;
        // The first chunk is small, its bytes are not known yet
        private const int FetchChunkStartRows = 64;
        private const long FetchChunkBytes = 8 * 1024 * 1024;

        private FetchRequiredColumnsRelation? _fetchRelation;
        private RowStoreHandle[]? _fetchHandles;
        private RowStoreFetcher[]? _fetchers;
        // Per write column: its tree column, or -1 when fetched
        private int[]? _writeTreeColumn;
        private int[]? _writeFetch;
        private int[]? _writeFetchColumn;
        // The key columns in the tree's layout
        private List<int>? _treeKeyColumns;
        private int[]? _neededTreeColumns;
        private readonly RowOrderScratch _fetchRowOrder = new RowOrderScratch();
        private int _fetchChunkRows = FetchChunkStartRows;
        private long _fetchedRows;
        private long _fetchColdLoads;
        private bool _fetchGauges;

        /// <summary>
        /// False by default for a sink that overrides OnRecieve.
        /// </summary>
        protected virtual bool CanHostFetch
        {
            get
            {
                var receive = GetType().GetMethod(nameof(OnRecieve), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, [typeof(StreamEventBatch), typeof(long)]);
                return receive == null || receive.DeclaringType == typeof(ColumnGroupedWriteOperator);
            }
        }

        bool IRowStoreFetchHostVertex.TryAttachFetch(FetchRequiredColumnsRelation fetch, IReadOnlyList<RowStoreHandle> handles)
        {
            if (!CanHostFetch || m_writeRelation.EmitSet)
            {
                return false;
            }
            int inputLength = fetch.Input.OutputLength;
            int width = m_writeRelation.OutputLength;
            int fetchedLength = 0;
            var needed = new SortedSet<int>();
            if (handles.Count != fetch.Fetches.Count)
            {
                throw new ArgumentException($"Got {handles.Count} row stores for {fetch.Fetches.Count} fetches.", nameof(handles));
            }
            for (int f = 0; f < fetch.Fetches.Count; f++)
            {
                if (fetch.Fetches[f].ReferenceColumn < 0 || fetch.Fetches[f].ReferenceColumn >= inputLength)
                {
                    throw new InvalidOperationException($"A fetch from row store '{fetch.Fetches[f].StoreName}' reads its references from column {fetch.Fetches[f].ReferenceColumn}, the input has {inputLength}.");
                }
                needed.Add(fetch.Fetches[f].ReferenceColumn);
                fetchedLength += fetch.Fetches[f].Columns.Count;
            }
            if (fetch.OutputLength != width)
            {
                throw new InvalidOperationException($"The fetch in front of sink '{m_writeRelation.NamedObject.DotSeperated}' gives {fetch.OutputLength} columns, the sink writes {width}.");
            }
            var treeColumn = new int[width];
            var writeFetch = new int[width];
            var writeFetchColumn = new int[width];
            for (int i = 0; i < width; i++)
            {
                int column = fetch.EmitSet ? fetch.Emit[i] : i;
                if (column < 0 || column >= inputLength + fetchedLength)
                {
                    throw new InvalidOperationException($"The fetch in front of sink '{m_writeRelation.NamedObject.DotSeperated}' emits column {column}, it has {inputLength + fetchedLength}.");
                }
                treeColumn[i] = column < inputLength ? column : -1;
                if (column < inputLength)
                {
                    needed.Add(column);
                    continue;
                }
                int position = column - inputLength;
                for (int f = 0; f < fetch.Fetches.Count; f++)
                {
                    if (position < fetch.Fetches[f].Columns.Count)
                    {
                        writeFetch[i] = f;
                        writeFetchColumn[i] = position;
                        break;
                    }
                    position -= fetch.Fetches[f].Columns.Count;
                }
            }
            _fetchRelation = fetch;
            _fetchHandles = handles.ToArray();
            _writeTreeColumn = treeColumn;
            _writeFetch = writeFetch;
            _writeFetchColumn = writeFetchColumn;
            _neededTreeColumns = needed.ToArray();
            _treeColumnCount = inputLength;
            return true;
        }

        // The tree's key columns, a key cannot lie behind a reference
        private List<int> TreeKeyColumns(IReadOnlyList<int> primaryKeyColumns)
        {
            if (_writeTreeColumn == null)
            {
                return primaryKeyColumns.ToList();
            }
            var keys = new List<int>(primaryKeyColumns.Count);
            for (int i = 0; i < primaryKeyColumns.Count; i++)
            {
                if (primaryKeyColumns[i] < 0 || primaryKeyColumns[i] >= _writeTreeColumn.Length)
                {
                    throw new InvalidOperationException($"The sink '{m_writeRelation.NamedObject.DotSeperated}' has column {primaryKeyColumns[i]} in its primary key, it writes {_writeTreeColumn.Length} columns.");
                }
                int column = _writeTreeColumn[primaryKeyColumns[i]];
                if (column < 0)
                {
                    throw new InvalidOperationException(
                        $"The sink '{m_writeRelation.NamedObject.DotSeperated}' has column {primaryKeyColumns[i]} in its primary key and the plan reads that column through a fetch. A key column must reach the sink as a value, keep it out of the row store.");
                }
                keys.Add(column);
            }
            return keys;
        }

        private FlowtideDotNet.Storage.StateManager.IObjectState<string>? _treeLayout;
        private bool _treeLayoutIsNew;

        // What the output tree stores, checked against its state
        private string TreeLayout()
        {
            if (_fetchRelation == null)
            {
                return "rows";
            }
            var layout = new System.Text.StringBuilder("references;");
            layout.Append(_treeColumnCount).Append(";keys ").AppendJoin(',', _treeKeyColumns!).Append(";write ").AppendJoin(',', _writeTreeColumn!);
            for (int i = 0; i < _fetchRelation.Fetches.Count; i++)
            {
                var fetch = _fetchRelation.Fetches[i];
                layout.Append(';').Append(fetch.StoreName).Append(' ').Append(fetch.ReferenceColumn).Append(':').AppendJoin(',', fetch.Columns);
            }
            return layout.ToString();
        }

        private async Task CheckTreeLayout(FlowtideDotNet.Storage.StateManager.IStateManagerClient stateManagerClient)
        {
            Debug.Assert(m_hasSentInitialData != null);
            _treeLayout = await stateManagerClient.GetOrCreateObjectStateAsync<string>("layout");
            string layout = TreeLayout();
            // A state written before the record existed holds whole rows
            string stored = _treeLayout.Value ?? (m_hasSentInitialData.Value ? "rows" : layout);
            if (stored != layout)
            {
                throw new InvalidOperationException(
                    $"The state of sink '{m_writeRelation.NamedObject.DotSeperated}' was written with the layout '{stored}', the stream now asks for '{layout}'. The output tree cannot be read with another layout.");
            }
            _treeLayoutIsNew = _treeLayout.Value == null && _fetchRelation != null;
        }

        // Only a sink with a fetch writes the record
        private async Task CommitTreeLayout()
        {
            if (_treeLayoutIsNew)
            {
                _treeLayoutIsNew = false;
                _treeLayout!.Value = TreeLayout();
                await _treeLayout.Commit();
            }
        }

        private void CreateFetchers()
        {
            DisposeFetchers();
            if (_fetchRelation == null)
            {
                return;
            }
            Debug.Assert(_fetchHandles != null);
            if (!_fetchGauges)
            {
                _fetchGauges = true;
                Metrics.CreateObservableGauge<long>("fetched_rows", () => Volatile.Read(ref _fetchedRows));
                Metrics.CreateObservableGauge<long>("fetch_cold_loads", () => Volatile.Read(ref _fetchColdLoads));
            }
            _fetchers = new RowStoreFetcher[_fetchHandles.Length];
            for (int i = 0; i < _fetchers.Length; i++)
            {
                _fetchers[i] = new RowStoreFetcher(_fetchRelation.Fetches[i], _fetchHandles[i], MemoryAllocator, _fetchRowOrder);
            }
        }

        private void DisposeFetchers()
        {
            if (_fetchers == null)
            {
                return;
            }
            for (int i = 0; i < _fetchers.Length; i++)
            {
                _fetchers[i].Dispose();
            }
            _fetchers = null;
        }

        // Rows of changed keys, copied out of the tree and fetched
        private sealed class FetchChunk : IDisposable
        {
            private readonly ColumnGroupedWriteOperator _owner;
            // Only the tree columns the write or a fetch needs
            private readonly StoreColumn?[] _tree;
            private readonly StoreColumn[] _deletedKeys;
            private readonly List<IColumn> _fetched = new List<IColumn>();
            private readonly List<PrimitiveList<int>> _offsets = new List<PrimitiveList<int>>();
            private readonly List<int> _weights = new List<int>();
            private IColumn[]? _write;
            private int _rows;

            // Per changed key: first row and count, zero deletes
            public readonly List<(int start, int count)> Keys = new List<(int start, int count)>();

            public FetchChunk(ColumnGroupedWriteOperator owner)
            {
                _owner = owner;
                _tree = new StoreColumn?[owner._treeColumnCount];
                var needed = owner._neededTreeColumns!;
                for (int i = 0; i < needed.Length; i++)
                {
                    _tree[needed[i]] = StoreColumn.Create(owner.MemoryAllocator);
                }
                _deletedKeys = new StoreColumn[owner._treeKeyColumns!.Count];
                for (int i = 0; i < _deletedKeys.Length; i++)
                {
                    _deletedKeys[i] = StoreColumn.Create(owner.MemoryAllocator);
                }
            }

            public int Rows => _rows;

            // Bytes of the copied and the fetched columns
            public long Bytes { get; private set; }

            public EventBatchData? WriteBatch { get; private set; }

            public EventBatchData? DeleteBatch { get; private set; }

            public IColumn[] Write => _write!;

            public int Weight(int row) => _weights[row];

            // Weights are only read when a key has several rows
            public void AddRows(EventBatchData page, int start, int count, ListValueContainer<int>? weights)
            {
                var needed = _owner._neededTreeColumns!;
                for (int i = 0; i < needed.Length; i++)
                {
                    var column = _tree[needed[i]]!;
                    column.InsertRangeFrom(column.Count, page.Columns[needed[i]], start, count);
                }
                for (int i = 0; i < count; i++)
                {
                    _weights.Add(weights == null ? 1 : weights.Get(start + i));
                }
                _rows += count;
            }

            // Takes back the rows of a key that grew too large
            public void RemoveRowsFrom(int first)
            {
                var needed = _owner._neededTreeColumns!;
                for (int i = 0; i < needed.Length; i++)
                {
                    _tree[needed[i]]!.RemoveRange(first, _rows - first);
                }
                _weights.RemoveRange(first, _rows - first);
                _rows = first;
            }

            // The modified tree holds a deleted key's columns in key order
            public void AddDelete(EventBatchData keys, int row)
            {
                Keys.Add((_deletedKeys[0].Count, 0));
                for (int i = 0; i < _deletedKeys.Length; i++)
                {
                    _deletedKeys[i].InsertRangeFrom(_deletedKeys[i].Count, keys.Columns[i], row, 1);
                }
            }

            // A key whose rows cancel is deleted, its key copied here
            public int AddDeleteOfRow(int row)
            {
                var treeKeys = _owner._treeKeyColumns!;
                for (int i = 0; i < _deletedKeys.Length; i++)
                {
                    _deletedKeys[i].InsertRangeFrom(_deletedKeys[i].Count, _tree[treeKeys[i]]!, row, 1);
                }
                return _deletedKeys[0].Count - 1;
            }

            public async ValueTask FetchAsync(CancellationToken cancellationToken)
            {
                var fetchers = _owner._fetchers!;
                var write = new IColumn[_owner._writeTreeColumn!.Length];
                var fetched = new IColumn[fetchers.Length][];
                for (int f = 0; f < fetchers.Length && _rows > 0; f++)
                {
                    long coldBefore = fetchers[f].ColdLoads;
                    var (columns, offsets) = await fetchers[f].FetchAsync(_tree[fetchers[f].Definition.ReferenceColumn]!, _rows, cancellationToken).ConfigureAwait(false);
                    _fetched.AddRange(columns);
                    if (offsets != null)
                    {
                        _offsets.Add(offsets);
                    }
                    Interlocked.Add(ref _owner._fetchedRows, _rows);
                    Interlocked.Add(ref _owner._fetchColdLoads, fetchers[f].ColdLoads - coldBefore);
                    fetched[f] = columns;
                    for (int c = 0; c < columns.Length; c++)
                    {
                        Bytes += columns[c].GetByteSize();
                    }
                }
                var needed = _owner._neededTreeColumns!;
                for (int i = 0; i < needed.Length; i++)
                {
                    Bytes += _tree[needed[i]]!.GetByteSize();
                }
                var deleted = new IColumn[write.Length];
                for (int i = 0; i < write.Length; i++)
                {
                    int treeColumn = _owner._writeTreeColumn[i];
                    deleted[i] = AlwaysNullColumn.Instance;
                    if (treeColumn >= 0)
                    {
                        write[i] = _tree[treeColumn]!;
                    }
                    else
                    {
                        write[i] = _rows > 0 ? fetched[_owner._writeFetch![i]][_owner._writeFetchColumn![i]] : AlwaysNullColumn.Instance;
                    }
                }
                var primaryKeys = _owner.m_primaryKeyColumns!;
                for (int k = 0; k < primaryKeys.Count; k++)
                {
                    deleted[primaryKeys[k]] = _deletedKeys[k];
                }
                _write = write;
                WriteBatch = new EventBatchData(write);
                DeleteBatch = new EventBatchData(deleted);
                // A reference no fetch reads must not reach a connector
                if (_rows > 0)
                {
                    RowRefGuard.ThrowIfPresent(WriteBatch, _owner);
                }
                if (_deletedKeys[0].Count > 0)
                {
                    RowRefGuard.ThrowIfPresent(DeleteBatch, _owner);
                }
            }

            public int Compare(int row, int other)
            {
                Debug.Assert(_write != null);
                for (int c = 0; c < _write.Length; c++)
                {
                    int compare = _write[c].CompareTo(_write[c], row, other);
                    if (compare != 0)
                    {
                        return compare;
                    }
                }
                return 0;
            }

            // The lowest written row whose weights do not cancel, or -1.
            // Rows that write the same values count as one row.
            public int Winner(int start, int count)
            {
                var order = new int[count];
                for (int i = 0; i < count; i++)
                {
                    order[i] = start + i;
                }
                Array.Sort(order, Compare);
                int run = 0;
                while (run < count)
                {
                    int weight = _weights[order[run]];
                    int next = run + 1;
                    while (next < count && Compare(order[next], order[run]) == 0)
                    {
                        weight += _weights[order[next]];
                        next++;
                    }
                    if (weight != 0)
                    {
                        return order[run];
                    }
                    run = next;
                }
                return -1;
            }

            public void Dispose()
            {
                for (int i = 0; i < _tree.Length; i++)
                {
                    _tree[i]?.Dispose();
                }
                for (int i = 0; i < _deletedKeys.Length; i++)
                {
                    _deletedKeys[i].Dispose();
                }
                // A view frees its gathered column, the offsets are ours
                for (int i = 0; i < _fetched.Count; i++)
                {
                    _fetched[i].Dispose();
                }
                for (int i = 0; i < _offsets.Count; i++)
                {
                    _offsets[i].Dispose();
                }
            }
        }

        // The next chunk's rows, from this one's bytes per row
        private void AdjustFetchChunk(FetchChunk chunk)
        {
            if (chunk.Rows == 0)
            {
                return;
            }
            long perRow = Math.Max(1, chunk.Bytes / chunk.Rows);
            _fetchChunkRows = (int)Math.Clamp(FetchChunkBytes / perRow, 1, FetchChunkRows);
        }

        private bool SameKey(EventBatchData treePage, int treeRow, EventBatchData keys, int keyRow)
        {
            Debug.Assert(_treeKeyColumns != null);
            for (int k = 0; k < _treeKeyColumns.Count; k++)
            {
                if (treePage.Columns[_treeKeyColumns[k]].CompareTo(keys.Columns[k], treeRow, keyRow) != 0)
                {
                    return false;
                }
            }
            return true;
        }

        // The rows of the sought key, page by page
        private async IAsyncEnumerable<(EventBatchData data, ListValueContainer<int> weights, int start, int count)> RowsOfKey(
            IBPlusTreeIterator<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>> treeIterator,
            EventBatchData keys,
            int keyRow)
        {
            Debug.Assert(m_writeTreeSearchComparer != null);
            int start = m_writeTreeSearchComparer.start;
            int end = m_writeTreeSearchComparer.end;
            bool first = true;
            await foreach (var page in treeIterator)
            {
                var data = page.Keys._data;
                if (first)
                {
                    first = false;
                    yield return (data, page.Values, start, end - start + 1);
                    if (end < data.Count - 1)
                    {
                        yield break;
                    }
                    continue;
                }
                int same = 0;
                while (same < data.Count && SameKey(data, same, keys, keyRow))
                {
                    same++;
                }
                if (same > 0)
                {
                    yield return (data, page.Values, 0, same);
                }
                if (same < data.Count)
                {
                    yield break;
                }
            }
        }

        private enum KeyAdded
        {
            Yes,
            // The chunk in work must be sent first
            ChunkIsFull,
            // The key has more rows than a chunk
            TooLarge
        }

        // Adds the key's rows when the chunk has room for them
        private async ValueTask<KeyAdded> AddRowsOfKey(
            IBPlusTreeIterator<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>> treeIterator,
            EventBatchData keys,
            int keyRow,
            FetchChunk chunk)
        {
            Debug.Assert(m_writeTreeSearchComparer != null);
            var container = m_writeTreeSearchComparer.CurrentContainer!;
            int start = m_writeTreeSearchComparer.start;
            int end = m_writeTreeSearchComparer.end;
            int first = chunk.Rows;
            if (start == end && end < container.Count - 1)
            {
                if (first >= _fetchChunkRows)
                {
                    return KeyAdded.ChunkIsFull;
                }
                // One row, its weight is never read
                chunk.AddRows(container._data, start, 1, null);
                chunk.Keys.Add((first, 1));
                return KeyAdded.Yes;
            }
            await foreach (var (data, weights, from, count) in RowsOfKey(treeIterator, keys, keyRow))
            {
                if (chunk.Rows + count > _fetchChunkRows)
                {
                    bool alone = chunk.Rows - first + count > _fetchChunkRows;
                    chunk.RemoveRowsFrom(first);
                    return alone ? KeyAdded.TooLarge : KeyAdded.ChunkIsFull;
                }
                chunk.AddRows(data, from, count, weights);
            }
            chunk.Keys.Add((first, chunk.Rows - first));
            return KeyAdded.Yes;
        }

        // One operation per changed key, in key order
        private IEnumerable<ColumnWriteOperation> Operations(FetchChunk chunk)
        {
            for (int i = 0; i < chunk.Keys.Count; i++)
            {
                var (start, count) = chunk.Keys[i];
                int row = count > 1 ? chunk.Winner(start, count) : start;
                if (count == 0)
                {
                    yield return new ColumnWriteOperation() { EventBatchData = chunk.DeleteBatch!, Index = start, IsDeleted = true };
                }
                else if (row < 0)
                {
                    yield return new ColumnWriteOperation() { EventBatchData = chunk.DeleteBatch!, Index = chunk.AddDeleteOfRow(start), IsDeleted = true };
                }
                else
                {
                    yield return new ColumnWriteOperation() { EventBatchData = chunk.WriteBatch!, Index = row, IsDeleted = false };
                }
            }
        }

        // One written row, kept while a large key is read
        private sealed class OwnedRow : IDisposable
        {
            private readonly StoreColumn[] _columns;

            public OwnedRow(int width, FlowtideDotNet.Storage.Memory.IMemoryAllocator memoryAllocator)
            {
                _columns = new StoreColumn[width];
                for (int i = 0; i < width; i++)
                {
                    _columns[i] = StoreColumn.Create(memoryAllocator);
                }
                Batch = new EventBatchData(_columns);
            }

            public EventBatchData Batch { get; }

            public bool HasRow => _columns[0].Count > 0;

            // Summed as the tree sums a row's weight
            public int Weight { get; set; }

            public void Set(FetchChunk chunk, int row)
            {
                for (int i = 0; i < _columns.Length; i++)
                {
                    if (_columns[i].Count > 0)
                    {
                        _columns[i].RemoveAt(0);
                    }
                    _columns[i].Add(chunk.Write[i].GetValueAt(row, default));
                }
            }

            public void Clear()
            {
                for (int i = 0; i < _columns.Length; i++)
                {
                    if (_columns[i].Count > 0)
                    {
                        _columns[i].RemoveAt(0);
                    }
                }
                Weight = 0;
            }

            public void CopyFrom(OwnedRow other)
            {
                for (int i = 0; i < _columns.Length; i++)
                {
                    _columns[i].Add(other._columns[i].GetValueAt(0, default));
                }
            }

            // Order of a chunk's row against this row
            public int CompareTo(FetchChunk chunk, int row)
            {
                for (int i = 0; i < _columns.Length; i++)
                {
                    int compare = chunk.Write[i].CompareTo(_columns[i], row, 0);
                    if (compare != 0)
                    {
                        return compare;
                    }
                }
                return 0;
            }

            public void Dispose()
            {
                for (int i = 0; i < _columns.Length; i++)
                {
                    _columns[i].Dispose();
                }
            }
        }

        // A key with more rows than a chunk, read in slices.
        // Keeps the lowest row and the sum of its equals.
        // When they cancel, the key is read again.
        private async ValueTask<bool> ResolveLargeKey(
            IBPlusTreeIterator<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>> treeIterator,
            ColumnRowReference key,
            OwnedRow winner)
        {
            Debug.Assert(m_writeTreeSearchComparer != null);
            using var below = new OwnedRow(m_writeRelation.OutputLength, MemoryAllocator);
            while (true)
            {
                winner.Clear();
                await treeIterator.Seek(key, m_writeTreeSearchComparer);
                var slice = new FetchChunk(this);
                try
                {
                    await foreach (var (data, weights, from, count) in RowsOfKey(treeIterator, key.referenceBatch, key.RowIndex))
                    {
                        int taken = 0;
                        while (taken < count)
                        {
                            int take = Math.Min(count - taken, Math.Max(1, _fetchChunkRows - slice.Rows));
                            slice.AddRows(data, from + taken, take, weights);
                            taken += take;
                            if (slice.Rows >= _fetchChunkRows)
                            {
                                await ReduceSlice(slice, winner, below);
                                slice.Dispose();
                                slice = new FetchChunk(this);
                            }
                        }
                    }
                    if (slice.Rows > 0)
                    {
                        await ReduceSlice(slice, winner, below);
                    }
                }
                finally
                {
                    slice.Dispose();
                }
                if (!winner.HasRow)
                {
                    return false;
                }
                if (winner.Weight != 0)
                {
                    return true;
                }
                // Its equals cancel, look for the next row above it
                below.Clear();
                below.CopyFrom(winner);
            }
        }

        private async ValueTask ReduceSlice(FetchChunk slice, OwnedRow winner, OwnedRow below)
        {
            await slice.FetchAsync(CancellationToken);
            AdjustFetchChunk(slice);
            for (int row = 0; row < slice.Rows; row++)
            {
                if (below.HasRow && below.CompareTo(slice, row) <= 0)
                {
                    continue;
                }
                int compare = winner.HasRow ? winner.CompareTo(slice, row) : -1;
                if (compare < 0)
                {
                    winner.Set(slice, row);
                    winner.Weight = slice.Weight(row);
                }
                else if (compare == 0)
                {
                    winner.Weight += slice.Weight(row);
                }
            }
        }

        // The changed keys with their rows fetched, chunk by chunk
        private async IAsyncEnumerable<ColumnWriteOperation> ResolveChangedKeys()
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(m_tree != null);
            Debug.Assert(m_writeTreeSearchComparer != null);

            using var modifiedIterator = m_modified.CreateIterator();
            await modifiedIterator.SeekFirst();
            using var treeIterator = m_tree.CreateIterator();
            using var large = new OwnedRow(m_writeRelation.OutputLength, MemoryAllocator);
            var chunk = new FetchChunk(this);
            try
            {
                await foreach (var page in modifiedIterator)
                {
                    var keys = page.Keys._data;
                    int pageCount = keys.Count;
                    for (int i = 0; i < pageCount; i++)
                    {
                        var key = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                        await treeIterator.Seek(key, m_writeTreeSearchComparer);
                        var added = m_writeTreeSearchComparer.noMatch ? KeyAdded.Yes : await AddRowsOfKey(treeIterator, keys, i, chunk);
                        if (added != KeyAdded.Yes && chunk.Keys.Count > 0)
                        {
                            // No room left, the chunk goes out before this key
                            await chunk.FetchAsync(CancellationToken);
                            foreach (var operation in Operations(chunk))
                            {
                                yield return operation;
                            }
                            AdjustFetchChunk(chunk);
                            chunk.Dispose();
                            chunk = new FetchChunk(this);
                        }
                        if (added == KeyAdded.ChunkIsFull)
                        {
                            await treeIterator.Seek(key, m_writeTreeSearchComparer);
                            added = await AddRowsOfKey(treeIterator, keys, i, chunk);
                            Debug.Assert(added != KeyAdded.ChunkIsFull);
                        }
                        if (m_writeTreeSearchComparer.noMatch)
                        {
                            chunk.AddDelete(keys, i);
                        }
                        else if (added == KeyAdded.TooLarge)
                        {
                            if (await ResolveLargeKey(treeIterator, key, large))
                            {
                                yield return new ColumnWriteOperation() { EventBatchData = large.Batch, Index = 0, IsDeleted = false };
                            }
                            else
                            {
                                chunk.AddDelete(keys, i);
                            }
                            continue;
                        }
                        if (chunk.Rows >= _fetchChunkRows || chunk.Keys.Count >= FetchChunkRows)
                        {
                            await chunk.FetchAsync(CancellationToken);
                            foreach (var operation in Operations(chunk))
                            {
                                yield return operation;
                            }
                            AdjustFetchChunk(chunk);
                            chunk.Dispose();
                            chunk = new FetchChunk(this);
                        }
                    }
                }
                if (chunk.Keys.Count > 0)
                {
                    await chunk.FetchAsync(CancellationToken);
                    foreach (var operation in Operations(chunk))
                    {
                        yield return operation;
                    }
                    AdjustFetchChunk(chunk);
                }
            }
            finally
            {
                chunk.Dispose();
            }
        }

        private readonly DataValueContainer _changedKeyValue = new DataValueContainer();
        private readonly DataValueContainer _existingKeyValue = new DataValueContainer();

        // Key order of a changed row and a destination row
        private int CompareKeys(in ColumnWriteOperation changed, in ColumnRowReference existing)
        {
            Debug.Assert(m_primaryKeyColumns != null);
            for (int k = 0; k < m_primaryKeyColumns.Count; k++)
            {
                int column = m_primaryKeyColumns[k];
                changed.EventBatchData.Columns[column].GetValueAt(changed.Index, _changedKeyValue, default);
                existing.referenceBatch.Columns[column].GetValueAt(existing.RowIndex, _existingKeyValue, default);
                int compare = DataValueComparer.CompareTo(_changedKeyValue, _existingKeyValue);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return 0;
        }

        private ColumnWriteOperation DeleteOfExisting(in ColumnRowReference existing)
        {
            Debug.Assert(m_primaryKeyColumns != null);
            for (int i = 0; i < m_primaryKeyColumns.Count; i++)
            {
                _deleteBatchColumns[m_primaryKeyColumns[i]] = existing.referenceBatch.Columns[m_primaryKeyColumns[i]];
            }
            return new ColumnWriteOperation() { EventBatchData = _deleteEventBatch, Index = existing.RowIndex, IsDeleted = true };
        }

        // The first upload against what the destination holds, rows fetched
        private async IAsyncEnumerable<ColumnWriteOperation> CalculateDeltaWithExistingDataFetched()
        {
            Debug.Assert(m_existingData != null);
            Debug.Assert(m_existingRowComparer != null);

            using var existingIterator = m_existingData.CreateIterator();
            await existingIterator.SeekFirst();
            var existing = IteratePerRow(existingIterator).GetAsyncEnumerator();
            bool hasOld = await existing.MoveNextAsync();

            await foreach (var changed in ResolveChangedKeys())
            {
                int comparison = -1;
                while (hasOld && (comparison = CompareKeys(changed, existing.Current)) > 0)
                {
                    // Only the destination has this key
                    yield return DeleteOfExisting(existing.Current);
                    hasOld = await existing.MoveNextAsync();
                }
                if (!hasOld || comparison < 0)
                {
                    // A key changed and then deleted no longer exists
                    if (!changed.IsDeleted)
                    {
                        yield return changed;
                    }
                    continue;
                }
                if (!changed.IsDeleted)
                {
                    var row = new ColumnRowReference() { referenceBatch = changed.EventBatchData, RowIndex = changed.Index };
                    if (m_existingRowComparer.CompareTo(row, existing.Current) != 0)
                    {
                        yield return changed;
                    }
                }
                hasOld = await existing.MoveNextAsync();
            }
            while (hasOld)
            {
                yield return DeleteOfExisting(existing.Current);
                hasOld = await existing.MoveNextAsync();
            }
            await m_existingData.Clear();
        }
    }
}
