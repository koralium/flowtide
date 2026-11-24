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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // A row store inside a source or normalization
    internal sealed class RowStoreHost : IDisposable, IRowHeapVisitor
    {
        private const byte Skip = 0;
        private const byte Insert = 1;
        private const byte NewVersion = 2;
        private const byte Verify = 3;
        private const byte KeptChanged = 4;
        private const byte Remove = 5;

        private readonly RowStoreHostLayout _layout;
        private readonly IMemoryAllocator _memoryAllocator;
        // Null in append mode, rows are then not known by key
        private readonly IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _tree;
        private readonly IBPlusTreeBulkInserter<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _inserter;
        private readonly IBplusTreeBulkSearch<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage, RowStoreHostKeyComparer>? _searcher;
        private readonly RowStore _store;
        private readonly RowHeapReader _reader;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly bool _zeroWeightDeletes;
        private readonly int[] _heapColumns;
        private readonly int _referenceIndex;
        private readonly int _hashIndex;
        private readonly BatchSorter _sorter;
        private readonly DataValueContainer _left = new DataValueContainer();
        private readonly DataValueContainer _right = new DataValueContainer();
        private ColumnRowReference[] _keys = Array.Empty<ColumnRowReference>();
        private ColumnRowReference[] _values = Array.Empty<ColumnRowReference>();
        private ulong[] _hashes = Array.Empty<ulong>();
        private ulong[] _oldReferences = Array.Empty<ulong>();
        private ulong[] _newReferences = Array.Empty<ulong>();
        private byte[] _states = Array.Empty<byte>();
        private bool[] _keptEqual = Array.Empty<bool>();
        private bool[] _removed = Array.Empty<bool>();
        private int[] _rows = Array.Empty<int>();
        private int[] _scratch = Array.Empty<int>();
        private int[] _rowOrder = Array.Empty<int>();
        private int[] _sorted = Array.Empty<int>();
        private int[] _tags = Array.Empty<int>();
        private int[] _searchSorted = Array.Empty<int>();
        private int[] _writeSorted = Array.Empty<int>();
        private int[] _writeTags = Array.Empty<int>();
        private int[] _sizes = Array.Empty<int>();
        private int[] _sizeRows = Array.Empty<int>();
        private int[] _sizeSums = Array.Empty<int>();
        private int _waiters;
        private EventBatchData? _verifyBatch;
        private RowStoreHostResult? _writeResult;
        private long _verifiedRows;
        private long _verifyReads;
        private volatile bool _failed;
        private bool _disposed;
        // Written with the first checkpoint
        private IObjectState<RowStoreHostLayoutRecord>? _newRecord;

        private RowStoreHost(
            RowStoreHostLayout layout,
            IMemoryAllocator memoryAllocator,
            IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? tree,
            RowStore store,
            RowHeapReader reader,
            bool zeroWeightDeletes)
        {
            _layout = layout;
            _memoryAllocator = memoryAllocator;
            _tree = tree;
            _store = store;
            _reader = reader;
            _zeroWeightDeletes = zeroWeightDeletes;
            _referenceIndex = layout.KeptColumns.Count;
            _hashIndex = layout.KeptColumns.Count + 1;
            _sorter = new BatchSorter(Math.Max(1, layout.KeyColumns.Count));
            _heapColumns = new int[layout.DeferredColumns.Count];
            for (int i = 0; i < _heapColumns.Length; i++)
            {
                _heapColumns[i] = i;
            }
            if (tree != null)
            {
                _inserter = tree.CreateBulkInserter();
                _searcher = tree.CreateBulkSearcher(new RowStoreHostKeyComparer(StoredOrder(layout.KeyColumns.Count)));
            }
        }

        // Time per step: hash, search, verify, append, write, compact, setup, sort
        internal long[] StepTicks { get; } = new long[8];

        // Test hook, replaces the hash of the stored columns
        internal Func<ulong, ulong>? HashHookForTests { get; set; }

        public RowStore Store => _store;

        // Calls that wait for the lock right now
        internal int WaitersForTests => Volatile.Read(ref _waiters);

        // Stored rows were not sent, the host closes
        public void MarkFailed()
        {
            _failed = true;
        }

        // True when rows are only added and not known by key
        public bool AppendOnly => _tree == null;

        public RowStoreHostLayout Layout => _layout;

        // Rows whose hash matched and were compared with the stored row
        public long VerifiedRows => _verifiedRows;

        // Pages read to compare stored rows
        public long VerifyReads => _verifyReads;

        // Pages loaded from storage, for verifies and lookups
        public long ColdLoads => _reader.ColdLoads;

        private static List<int> StoredOrder(int count)
        {
            var order = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                order.Add(i);
            }
            return order;
        }

        public static async Task<RowStoreHost> CreateAsync(
            IStateManagerClient stateManagerClient,
            string treeName,
            string storeName,
            RowStoreHostLayout layout,
            RowStoreOptions options,
            IMemoryAllocator memoryAllocator,
            bool zeroWeightDeletes = false)
        {
            if (layout.DeferredColumns.Count == 0)
            {
                throw new InvalidOperationException($"Row store '{storeName}' has no column to store, a host needs at least one.");
            }
            if (options.Mode == RowStoreMode.Keyed && layout.KeyColumns.Count == 0)
            {
                throw new InvalidOperationException($"Row store '{storeName}' is hosted by key and has no key column.");
            }
            var record = await stateManagerClient.GetOrCreateObjectStateAsync<RowStoreHostLayoutRecord>($"{storeName}_layout");
            var stored = record.Value;
            if (stored != null && !stored.Matches(layout, options.Mode))
            {
                throw new InvalidOperationException(
                    $"Row store '{storeName}' is stored in mode {(RowStoreMode)stored.Mode} with {stored.ColumnCount} columns, key columns [{string.Join(", ", stored.KeyColumns)}], kept columns [{string.Join(", ", stored.KeptColumns)}] " +
                    $"and stored columns [{string.Join(", ", stored.DeferredColumns)}] at output positions [{string.Join(", ", stored.DeferredPositions ?? [])}], the stream asks for mode {options.Mode} with {layout.ColumnCount} columns, [{string.Join(", ", layout.KeyColumns)}], " +
                    $"[{string.Join(", ", layout.KeptColumns)}] and [{string.Join(", ", layout.DeferredColumns)}] at [{string.Join(", ", layout.DeferredPositions ?? [])}].");
            }
            IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? tree = null;
            if (options.Mode == RowStoreMode.Keyed)
            {
                tree = await stateManagerClient.GetOrCreateTree(treeName,
                    new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                    {
                        // Keys and values reach the tree in stored order
                        Comparer = new RowStoreHostKeyComparer(StoredOrder(layout.KeyColumns.Count)),
                        KeySerializer = new NormalizeKeyStorageSerializer(StoredOrder(layout.KeyColumns.Count), memoryAllocator),
                        ValueSerializer = new NormalizeValueSerializer(StoredOrder(layout.KeptColumns.Count + 2), memoryAllocator),
                        UseByteBasedPageSizes = true,
                        MemoryAllocator = memoryAllocator
                    });
            }
            var store = await RowStore.CreateAsync(stateManagerClient, storeName, layout.DeferredColumns.Count, options, memoryAllocator);
            RowHeapReader reader;
            try
            {
                reader = store.CreateReader();
            }
            catch
            {
                // A store without its reader is closed again
                store.Dispose();
                throw;
            }
            var host = new RowStoreHost(layout, memoryAllocator, tree, store, reader, zeroWeightDeletes);
            if (stored == null)
            {
                record.Value = RowStoreHostLayoutRecord.From(layout, options.Mode);
                host._newRecord = record;
            }
            return host;
        }

        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> KeyTree
        {
            get
            {
                if (_tree == null)
                {
                    throw new InvalidOperationException($"Row store '{_store.Name}' is append-only, its rows are not known by key.");
                }
                return _tree;
            }
        }

        // Takes the lock, the host must still be open and whole
        private async ValueTask EnterAsync()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            Interlocked.Increment(ref _waiters);
            try
            {
                await _lock.WaitAsync().ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _waiters);
            }
            if (_disposed)
            {
                _lock.Release();
                throw new ObjectDisposedException(nameof(RowStoreHost));
            }
            if (_failed)
            {
                _lock.Release();
                throw new InvalidOperationException($"Row store '{_store.Name}' failed in an earlier call, it is opened anew from the last checkpoint.");
            }
        }

        // Applies one input batch, the result lists what to send
        public async ValueTask<RowStoreHostResult> ApplyAsync(EventBatchData batch, PrimitiveList<int> weights, Func<EventBatchData, int, bool>? filter, bool zeroWeightIsRow = false)
        {
            await EnterAsync().ConfigureAwait(false);
            var result = new RowStoreHostResult(_layout.KeptColumns.Count, _memoryAllocator);
            try
            {
                int count = weights.Count;
                EnsureCapacity(count);
                if (count > 0)
                {
                    await ApplyOwnedAsync(batch, weights, filter, zeroWeightIsRow, count, result).ConfigureAwait(false);
                }
                long mark = System.Diagnostics.Stopwatch.GetTimestamp();
                await _store.CompactAsync().ConfigureAwait(false);
                StepTicks[5] += System.Diagnostics.Stopwatch.GetTimestamp() - mark;
                // The batch is the caller's again
                Array.Clear(_keys, 0, Math.Min(count, _keys.Length));
                Array.Clear(_values, 0, Math.Min(count, _values.Length));
                return result;
            }
            catch
            {
                // Half a batch may be stored, nothing more is taken
                _failed = true;
                result.Dispose();
                throw;
            }
            finally
            {
                _lock.Release();
            }
        }

        // Every row is new, nothing is looked up or taken back
        private async ValueTask ApplyAppendOnlyAsync(EventBatchData batch, EventBatchData input, PrimitiveList<int> weights, Func<EventBatchData, int, bool>? filter, bool zeroWeightIsRow, RowStoreHostResult result)
        {
            int append = 0;
            for (int i = 0; i < weights.Count; i++)
            {
                if (weights[i] < 0)
                {
                    throw new InvalidOperationException($"Row {i} of the batch has weight {weights[i]}, row store '{_store.Name}' is append-only and takes no row back.");
                }
                bool isRow = weights[i] > 0 || zeroWeightIsRow;
                if (isRow && (filter == null || filter(input, i)))
                {
                    _rows[append++] = i;
                }
            }
            if (append == 0)
            {
                return;
            }
            await AppendAsync(batch, append, null).ConfigureAwait(false);
            for (int i = 0; i < append; i++)
            {
                result.AddInsert(_rows[i], _newReferences[_rows[i]]);
            }
        }

        private void EnsureCapacity(int count)
        {
            if (_states.Length < count)
            {
                int size = Math.Max(count, 1024);
                _hashes = new ulong[size];
                _oldReferences = new ulong[size];
                _newReferences = new ulong[size];
                _states = new byte[size];
                _keptEqual = new bool[size];
                _removed = new bool[size];
                _rows = new int[size];
                _scratch = new int[size];
                _rowOrder = new int[size];
                _keys = new ColumnRowReference[size];
                _values = new ColumnRowReference[size];
                _sorted = new int[size];
                _tags = new int[size];
                _searchSorted = new int[size];
                _writeSorted = new int[size];
                _writeTags = new int[size];
                _sizes = new int[size];
            }
        }

        // The key columns of a batch, in the tree's order
        private EventBatchData KeyBatch(EventBatchData batch)
        {
            var columns = new IColumn[_layout.KeyColumns.Count];
            for (int k = 0; k < columns.Length; k++)
            {
                columns[k] = batch.Columns[_layout.KeyColumns[k]];
            }
            return new EventBatchData(columns);
        }

        // Sorts by key, keeps each key's last effective row
        private int SortByKey(EventBatchData keyBatch, int count, bool everyRow)
        {
            for (int i = 0; i < count; i++)
            {
                _sorted[i] = i;
            }
            if (count > 1)
            {
                var sortedSpan = _sorted.AsSpan(0, count);
                var tagSpan = _tags.AsSpan(0, count);
                _sorter.SortDataWithTags((IColumn[])keyBatch.Columns, ref sortedSpan, ref tagSpan);
            }
            else
            {
                _tags[0] = 0;
            }
            int distinct = 0;
            int at = 0;
            while (at < count)
            {
                int tag = _tags[at];
                int last = -1;
                while (at < count && _tags[at] == tag)
                {
                    int row = _sorted[at];
                    at++;
                    // A row of weight zero that does nothing decides nothing
                    if (!everyRow && _states[row] == Skip && !_removed[row])
                    {
                        continue;
                    }
                    int earlier = Math.Min(last, row);
                    if (earlier >= 0)
                    {
                        _states[earlier] = Skip;
                        _removed[earlier] = false;
                    }
                    last = Math.Max(last, row);
                }
                if (last >= 0)
                {
                    _searchSorted[distinct++] = last;
                }
            }
            return distinct;
        }

        // Both modes read a batch without always-null columns
        private async ValueTask ApplyOwnedAsync(EventBatchData input, PrimitiveList<int> weights, Func<EventBatchData, int, bool>? filter, bool zeroWeightIsRow, int count, RowStoreHostResult result)
        {
            var owned = WithoutAlwaysNullColumns(input, count, out var batch);
            try
            {
                if (_tree == null)
                {
                    await ApplyAppendOnlyAsync(batch, input, weights, filter, zeroWeightIsRow, result).ConfigureAwait(false);
                }
                else
                {
                    // One sorted search and one sorted write
                    await ApplyKeyedCoreAsync(batch, input, weights, filter, zeroWeightIsRow, count, result).ConfigureAwait(false);
                }
            }
            finally
            {
                if (owned != null)
                {
                    for (int i = 0; i < owned.Count; i++)
                    {
                        owned[i].Dispose();
                    }
                }
            }
        }

        // A view over an always-null column cannot be sorted
        private IColumn OutputColumn(IColumn column, PrimitiveList<int> rows, out bool offsetsUsed)
        {
            if (column is AlwaysNullColumn || (column is ColumnWithOffset view && view.InnerColumn is AlwaysNullColumn))
            {
                offsetsUsed = false;
                var nulls = Column.Create(_memoryAllocator);
                nulls.InsertNullRange(0, rows.Count);
                return nulls;
            }
            return ColumnWithOffset.CreateFlattened(column, rows, _memoryAllocator, out offsetsUsed);
        }

        // An always-null column gets real rows for the batch
        private List<Column>? WithoutAlwaysNullColumns(EventBatchData input, int count, out EventBatchData batch)
        {
            List<Column>? owned = null;
            IColumn[]? columns = null;
            for (int i = 0; i < input.Columns.Count; i++)
            {
                var column = input.Columns[i];
                // A view over one reads as null on every row too
                if (column is AlwaysNullColumn || (column is ColumnWithOffset view && view.InnerColumn is AlwaysNullColumn))
                {
                    if (columns == null)
                    {
                        columns = new IColumn[input.Columns.Count];
                        for (int k = 0; k < columns.Length; k++)
                        {
                            columns[k] = input.Columns[k];
                        }
                        owned = new List<Column>();
                    }
                    var nulls = Column.Create(_memoryAllocator);
                    nulls.InsertNullRange(0, count);
                    owned!.Add(nulls);
                    columns[i] = nulls;
                }
            }
            batch = columns == null ? input : new EventBatchData(columns);
            return owned;
        }

        private async ValueTask ApplyKeyedCoreAsync(EventBatchData batch, EventBatchData input, PrimitiveList<int> weights, Func<EventBatchData, int, bool>? filter, bool zeroWeightIsRow, int count, RowStoreHostResult result)
        {
            long mark = System.Diagnostics.Stopwatch.GetTimestamp();
            bool allStored = true;
            for (int i = 0; i < count; i++)
            {
                int weight = weights[i];
                _removed[i] = false;
                bool isRow = weight > 0 || (weight == 0 && zeroWeightIsRow);
                if (isRow && (filter == null || filter(input, i)))
                {
                    // New until the search finds its key
                    _states[i] = Insert;
                }
                else
                {
                    // A deleted or filtered row leaves if it is stored
                    _states[i] = Skip;
                    _removed[i] = isRow || weight < 0 || _zeroWeightDeletes;
                    allStored = false;
                }
            }
            var keyBatch = KeyBatch(batch);
            long sort = System.Diagnostics.Stopwatch.GetTimestamp();
            StepTicks[6] += sort - mark;
            int distinct = SortByKey(keyBatch, count, false);
            long hash = System.Diagnostics.Stopwatch.GetTimestamp();
            StepTicks[7] += hash - sort;
            allStored &= distinct == count;
            HashStoredColumns(batch, count, allStored);

            // The value is the kept columns, the reference and the hash
            var valueColumns = new IColumn[_layout.KeptColumns.Count + 2];
            for (int k = 0; k < _layout.KeptColumns.Count; k++)
            {
                valueColumns[k] = batch.Columns[_layout.KeptColumns[k]];
            }
            using var referenceColumn = Column.Create(_memoryAllocator);
            using var hashColumn = Column.Create(_memoryAllocator);
            for (int i = 0; i < count; i++)
            {
                referenceColumn.Add(new RowRefValue(0));
                hashColumn.Add(new Int64Value((long)_hashes[i]));
            }
            valueColumns[_referenceIndex] = referenceColumn;
            valueColumns[_hashIndex] = hashColumn;
            var valueBatch = new EventBatchData(valueColumns);
            for (int i = 0; i < count; i++)
            {
                _keys[i] = new ColumnRowReference() { referenceBatch = keyBatch, RowIndex = i };
                _values[i] = new ColumnRowReference() { referenceBatch = valueBatch, RowIndex = i };
            }
            long search = System.Diagnostics.Stopwatch.GetTimestamp();
            StepTicks[0] += search - hash;

            int verify = await SearchAsync(batch, distinct).ConfigureAwait(false);
            long now = System.Diagnostics.Stopwatch.GetTimestamp();
            StepTicks[1] += now - search;
            mark = now;
            if (verify > 0)
            {
                await VerifyAsync(batch, count, verify).ConfigureAwait(false);
            }
            now = System.Diagnostics.Stopwatch.GetTimestamp();
            StepTicks[2] += now - mark;
            mark = now;

            int append = 0;
            for (int i = 0; i < count; i++)
            {
                if (_states[i] == Insert || _states[i] == NewVersion)
                {
                    _rows[append++] = i;
                }
            }
            if (append > 0)
            {
                await AppendAsync(batch, append, referenceColumn).ConfigureAwait(false);
            }
            now = System.Diagnostics.Stopwatch.GetTimestamp();
            StepTicks[3] += now - mark;
            mark = now;

            int writes = 0;
            for (int i = 0; i < distinct; i++)
            {
                int row = _searchSorted[i];
                byte state = _states[row];
                if (state == Skip)
                {
                    continue;
                }
                if (state == KeptChanged)
                {
                    referenceColumn.UpdateAt(row, new RowRefValue(_oldReferences[row]));
                    _newReferences[row] = _oldReferences[row];
                }
                _removed[row] = false;
                _writeSorted[writes] = row;
                _writeTags[writes] = writes;
                writes++;
            }
            if (writes > 0)
            {
                await WriteAsync(writes, result).ConfigureAwait(false);
                // Send in input order, retire what was replaced
                for (int i = 0; i < count; i++)
                {
                    byte state = _states[i];
                    if (state == Insert || state == NewVersion || state == KeptChanged)
                    {
                        result.AddInsert(i, _newReferences[i]);
                    }
                    if (state == NewVersion || (state == Remove && _removed[i]))
                    {
                        await _store.RetireAsync(_oldReferences[i]).ConfigureAwait(false);
                    }
                }
            }
            StepTicks[4] += System.Diagnostics.Stopwatch.GetTimestamp() - mark;
        }

        // Hashes the stored columns of the rows that may be stored
        private void HashStoredColumns(EventBatchData batch, int count, bool allStored)
        {
            if (allStored)
            {
                RowIdentityHash.HashRows(batch.Columns, _layout.DeferredColumns, 0, _hashes.AsSpan(0, count));
            }
            else
            {
                // A deleted row may come with its key alone
                for (int i = 0; i < count; i++)
                {
                    _hashes[i] = _states[i] == Insert ? RowIdentityHash.HashRow(batch.Columns, _layout.DeferredColumns, i) : 0;
                }
            }
            if (HashHookForTests != null)
            {
                for (int i = 0; i < count; i++)
                {
                    _hashes[i] = HashHookForTests(_hashes[i]);
                }
            }
        }

        // Marks each row, returns how many to verify
        private async ValueTask<int> SearchAsync(EventBatchData batch, int distinct)
        {
            int verify = 0;
            await _searcher!.Start(_keys, distinct, _searchSorted).ConfigureAwait(false);
            while (await _searcher.MoveNextLeaf().ConfigureAwait(false))
            {
                var stored = _searcher.CurrentLeaf.values._data;
                var results = _searcher.CurrentResults;
                var references = stored.Columns[_referenceIndex];
                var hashes = stored.Columns[_hashIndex];
                for (int r = 0; r < results.Count; r++)
                {
                    int at = results[r].LowerBound;
                    if (at < 0)
                    {
                        continue;
                    }
                    int row = results[r].KeyIndex;
                    if (_removed[row])
                    {
                        _states[row] = Remove;
                        continue;
                    }
                    if (_states[row] != Insert)
                    {
                        continue;
                    }
                    references.GetValueAt(at, _left, default);
                    _oldReferences[row] = (ulong)_left.AsLong;
                    hashes.GetValueAt(at, _left, default);
                    if ((ulong)_left.AsLong != _hashes[row])
                    {
                        _states[row] = NewVersion;
                        continue;
                    }
                    _keptEqual[row] = KeptColumnsEqual(batch, row, stored, at);
                    _states[row] = Verify;
                    verify++;
                }
            }
            return verify;
        }

        private bool KeptColumnsEqual(EventBatchData batch, int row, EventBatchData stored, int at)
        {
            var kept = _layout.KeptColumns;
            for (int k = 0; k < kept.Count; k++)
            {
                batch.Columns[kept[k]].GetValueAt(row, _left, default);
                stored.Columns[k].GetValueAt(at, _right, default);
                if (DataValueComparer.Instance.Compare(_left, _right) != 0)
                {
                    return false;
                }
            }
            return true;
        }

        // Compares rows of equal hash with the stored rows, in place
        private async ValueTask VerifyAsync(EventBatchData batch, int count, int verify)
        {
            using var references = Column.Create(_memoryAllocator);
            int n = 0;
            for (int i = 0; i < count; i++)
            {
                if (_states[i] == Verify)
                {
                    _rows[n++] = i;
                    references.Add(new RowRefValue(_oldReferences[i]));
                }
            }
            _verifyBatch = batch;
            try
            {
                await _reader.VisitAsync(references, verify, _rowOrder, this).ConfigureAwait(false);
            }
            finally
            {
                _verifyBatch = null;
            }
            _verifiedRows += verify;
        }

        void IRowHeapVisitor.VisitRows(EventBatchData page, int[] pageRows, int first, int length)
        {
            var batch = _verifyBatch!;
            var deferred = _layout.DeferredColumns;
            _verifyReads++;
            for (int j = 0; j < length; j++)
            {
                int row = _rows[_rowOrder[first + j]];
                bool same = true;
                for (int k = 0; k < deferred.Count && same; k++)
                {
                    batch.Columns[deferred[k]].GetValueAt(row, _left, default);
                    page.Columns[k].GetValueAt(pageRows[j], _right, default);
                    same = DataValueComparer.Instance.Compare(_left, _right) == 0;
                }
                if (!same)
                {
                    _states[row] = NewVersion;
                }
                else
                {
                    _states[row] = _keptEqual[row] ? Skip : KeptChanged;
                }
            }
        }

        // Stores the rows in _rows and notes their references
        private async ValueTask AppendAsync(EventBatchData batch, int append, Column? referenceColumn)
        {
            var deferred = _layout.DeferredColumns;
            var payload = new IColumn[deferred.Count];
            using var references = Column.Create(_memoryAllocator);
            bool together = _rows[append - 1] - _rows[0] == append - 1;
            if (together)
            {
                for (int k = 0; k < payload.Length; k++)
                {
                    payload[k] = batch.Columns[deferred[k]];
                }
                await _store.AppendAsync(new EventBatchData(payload), _rows[0], append, references).ConfigureAwait(false);
            }
            else
            {
                var copies = new Column?[deferred.Count];
                try
                {
                    for (int k = 0; k < copies.Length; k++)
                    {
                        var copy = Column.Create(_memoryAllocator);
                        copies[k] = copy;
                        var source = batch.Columns[deferred[k]];
                        for (int i = 0; i < append; i++)
                        {
                            source.GetValueAt(_rows[i], _left, default);
                            copy.Add(_left);
                        }
                        payload[k] = copy;
                    }
                    await _store.AppendAsync(new EventBatchData(payload), 0, append, references).ConfigureAwait(false);
                }
                finally
                {
                    for (int k = 0; k < copies.Length; k++)
                    {
                        copies[k]?.Dispose();
                    }
                }
            }
            for (int i = 0; i < append; i++)
            {
                references.GetValueAt(i, _left, default);
                ulong reference = (ulong)_left.AsLong;
                _newReferences[_rows[i]] = reference;
                referenceColumn?.UpdateAt(_rows[i], new RowRefValue(reference));
            }
        }

        // Writes the rows in _writeSorted through the tree's bulk inserter
        private async ValueTask WriteAsync(int writes, RowStoreHostResult result)
        {
            int bytes = WriteBytes(writes);
            _writeResult = result;
            try
            {
                await _inserter!.ApplyBatch(_keys, _values, writes, _writeSorted, _writeTags, new WriteMutator(this), bytes).ConfigureAwait(false);
            }
            finally
            {
                _writeResult = null;
            }
        }

        // Running byte sizes, a removed row adds none
        private void AddWrittenSizes(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            if (_sizeRows.Length < indices.Length)
            {
                _sizeRows = new int[Math.Max(indices.Length, 1024)];
                _sizeSums = new int[_sizeRows.Length];
            }
            int stored = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                if (_states[indices[i]] != Remove)
                {
                    _sizeRows[stored++] = indices[i];
                }
            }
            var sums = _sizeSums.AsSpan(0, stored);
            sums.Clear();
            if (stored > 0)
            {
                ReadOnlySpan<int> rows = _sizeRows.AsSpan(0, stored);
                var keyColumns = keys[rows[0]].referenceBatch.Columns;
                for (int k = 0; k < keyColumns.Count; k++)
                {
                    keyColumns[k].GetPrefixSumByteSizes(rows, sums);
                }
                var values = _values[rows[0]].referenceBatch.Columns;
                for (int k = 0; k < _referenceIndex; k++)
                {
                    values[k].GetPrefixSumByteSizes(rows, sums);
                }
            }
            int seen = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                if (_states[indices[i]] != Remove)
                {
                    seen++;
                }
                sizes[i] += (seen > 0 ? sums[seen - 1] : 0) + seen * 2 * sizeof(long);
            }
        }

        // The bytes the written rows take in the tree
        private int WriteBytes(int writes)
        {
            var sizes = _sizes.AsSpan(0, writes);
            sizes.Clear();
            new WriteMutator(this).GetSizePrefixSum(_keys, _writeSorted.AsSpan(0, writes), sizes);
            return sizes[writes - 1];
        }

        // Decides the write of one row during the sorted pass
        private readonly struct WriteMutator : IRowMutator<ColumnRowReference, ColumnRowReference>
        {
            private readonly RowStoreHost _host;

            public WriteMutator(RowStoreHost host)
            {
                _host = host;
            }

            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                _host.AddWrittenSizes(keys, indices, sizes);
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in ColumnRowReference existingData, ref ColumnRowReference incomingData, int sortedIndex)
            {
                int row = key.RowIndex;
                var host = _host;
                if (host._states[row] == Remove)
                {
                    if (!exists)
                    {
                        return GenericWriteOperation.None;
                    }
                    host._writeResult!.AddDelete(row, existingData, host._referenceIndex);
                    existingData.referenceBatch.Columns[host._referenceIndex].GetValueAt(existingData.RowIndex, host._left, default);
                    host._oldReferences[row] = (ulong)host._left.AsLong;
                    host._removed[row] = true;
                    return GenericWriteOperation.Delete;
                }
                if (exists)
                {
                    host._writeResult!.AddDelete(row, existingData, host._referenceIndex);
                }
                return GenericWriteOperation.Upsert;
            }
        }

        // Walks the keys in order
        public IBPlusTreeIterator<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> CreateKeyIterator()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_failed)
            {
                throw new InvalidOperationException($"Row store '{_store.Name}' failed in an earlier call, it is opened anew from the last checkpoint.");
            }
            return KeyTree.CreateIterator();
        }

        // Deletes sorted keys given in the tree's layout
        public async ValueTask RemoveAsync(EventBatchData keys, int count, RowStoreHostResult result)
        {
            // Asking an append-only store by key is refused, not a failure
            _ = KeyTree;
            await EnterAsync().ConfigureAwait(false);
            try
            {
                if (count == 0)
                {
                    return;
                }
                EnsureCapacity(count);
                for (int i = 0; i < count; i++)
                {
                    _keys[i] = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                    _values[i] = default;
                    _states[i] = Remove;
                    _removed[i] = false;
                    _writeSorted[i] = i;
                    _writeTags[i] = i;
                }
                _writeResult = result;
                try
                {
                    await _inserter!.ApplyBatch(_keys, _values, count, _writeSorted, _writeTags, new RemoveMutator(this), 0).ConfigureAwait(false);
                }
                finally
                {
                    _writeResult = null;
                }
                for (int i = 0; i < count; i++)
                {
                    if (_removed[i])
                    {
                        await _store.RetireAsync(_oldReferences[i]).ConfigureAwait(false);
                    }
                }
            }
            catch
            {
                _failed = true;
                throw;
            }
            finally
            {
                _lock.Release();
            }
        }

        // Deletes every key it is asked for
        private readonly struct RemoveMutator : IRowMutator<ColumnRowReference, ColumnRowReference>
        {
            private readonly RowStoreHost _host;

            public RemoveMutator(RowStoreHost host)
            {
                _host = host;
            }

            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in ColumnRowReference existingData, ref ColumnRowReference incomingData, int sortedIndex)
            {
                if (!exists)
                {
                    return GenericWriteOperation.None;
                }
                var host = _host;
                int row = key.RowIndex;
                host._writeResult!.AddDelete(row, existingData, host._referenceIndex);
                existingData.referenceBatch.Columns[host._referenceIndex].GetValueAt(existingData.RowIndex, host._left, default);
                host._oldReferences[row] = (ulong)host._left.AsLong;
                host._removed[row] = true;
                return GenericWriteOperation.Delete;
            }
        }

        public RowStoreHostResult CreateResult()
        {
            return new RowStoreHostResult(_layout.KeptColumns.Count, _memoryAllocator);
        }

        // Finds a key's row, the caller owns the columns
        public async ValueTask<Column[]?> LookupAsync(ColumnRowReference key)
        {
            _ = KeyTree;
            var keyColumns = key.referenceBatch.Columns;
            // One row of the caller's batch, read before any wait
            var single = new Column[_layout.KeyColumns.Count];
            try
            {
                // Before the lock, nothing of the host is used
                var value = new DataValueContainer();
                for (int k = 0; k < single.Length; k++)
                {
                    single[k] = Column.Create(_memoryAllocator);
                    keyColumns[_layout.KeyColumns[k]].GetValueAt(key.RowIndex, value, default);
                    single[k].Add(value);
                }
                var (rows, found) = await LookupSortedAsync(new EventBatchData(single), 1, false).ConfigureAwait(false);
                if (found == 0)
                {
                    return null;
                }
                return rows;
            }
            finally
            {
                for (int k = 0; k < single.Length; k++)
                {
                    single[k]?.Dispose();
                }
            }
        }

        private int[] _lookupRows = Array.Empty<int>();
        private int[] _lookupOrder = Array.Empty<int>();

        // Rows of the last LookupManyAsync, a single lookup keeps them
        public ReadOnlySpan<int> LookupRows(int found) => _lookupRows.AsSpan(0, found);

        // Finds many keys in one walk, in the tree's layout
        public ValueTask<(Column[]? rows, int found)> LookupManyAsync(EventBatchData keys, int count)
        {
            return LookupSortedAsync(keys, count, true);
        }

        private async ValueTask<(Column[]? rows, int found)> LookupSortedAsync(EventBatchData keys, int count, bool keepRows)
        {
            _ = KeyTree;
            if (count == 0)
            {
                return (null, 0);
            }
            await EnterAsync().ConfigureAwait(false);
            Column[]? output = null;
            Column[]? kept = null;
            try
            {
                EnsureCapacity(count);
                if (_lookupOrder.Length < count)
                {
                    _lookupOrder = new int[Math.Max(count, 16)];
                }
                if (keepRows && _lookupRows.Length < count)
                {
                    _lookupRows = new int[Math.Max(count, 16)];
                }
                for (int i = 0; i < count; i++)
                {
                    _keys[i] = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                    _states[i] = Skip;
                    _removed[i] = false;
                }
                int distinct = SortByKey(keys, count, true);
                kept = new Column[_layout.KeptColumns.Count];
                for (int k = 0; k < kept.Length; k++)
                {
                    kept[k] = Column.Create(_memoryAllocator);
                }
                using var references = Column.Create(_memoryAllocator);
                int found = 0;
                await _searcher!.Start(_keys, distinct, _searchSorted).ConfigureAwait(false);
                while (await _searcher.MoveNextLeaf().ConfigureAwait(false))
                {
                    var stored = _searcher.CurrentLeaf.values._data;
                    var results = _searcher.CurrentResults;
                    for (int r = 0; r < results.Count; r++)
                    {
                        int at = results[r].LowerBound;
                        if (at < 0)
                        {
                            continue;
                        }
                        // Copied while the searcher holds the leaf
                        for (int k = 0; k < kept.Length; k++)
                        {
                            stored.Columns[k].GetValueAt(at, _left, default);
                            kept[k].Add(_left);
                        }
                        stored.Columns[_referenceIndex].GetValueAt(at, _left, default);
                        references.Add(_left);
                        _lookupOrder[found++] = results[r].KeyIndex;
                    }
                }
                if (found == 0)
                {
                    return (null, 0);
                }
                output = new Column[_layout.ColumnCount];
                for (int i = 0; i < output.Length; i++)
                {
                    output[i] = Column.Create(_memoryAllocator);
                }
                var heapColumns = new Column[_heapColumns.Length];
                for (int k = 0; k < heapColumns.Length; k++)
                {
                    heapColumns[k] = output[_layout.DeferredColumns[k]];
                }
                if (_rowOrder.Length < found)
                {
                    _rowOrder = new int[found];
                }
                await _reader.GatherAsync(references, found, _heapColumns, heapColumns, _rowOrder).ConfigureAwait(false);
                // The gather sorts by reference, the rest follows its order
                for (int i = 0; i < found; i++)
                {
                    int position = _rowOrder[i];
                    int row = _lookupOrder[position];
                    if (keepRows)
                    {
                        _lookupRows[i] = row;
                    }
                    for (int k = 0; k < kept.Length; k++)
                    {
                        kept[k].GetValueAt(position, _left, default);
                        var target = output[_layout.KeptColumns[k]];
                        if (target.Count == i)
                        {
                            target.Add(_left);
                        }
                    }
                    for (int k = 0; k < _layout.KeyColumns.Count; k++)
                    {
                        keys.Columns[k].GetValueAt(row, _left, default);
                        var target = output[_layout.KeyColumns[k]];
                        if (target.Count == i)
                        {
                            target.Add(_left);
                        }
                    }
                }
                for (int c = 0; c < output.Length; c++)
                {
                    // A column that is neither key, kept nor stored
                    if (output[c].Count == 0)
                    {
                        output[c].InsertNullRange(0, found);
                    }
                }
                var rows = output;
                output = null;
                return (rows, found);
            }
            catch
            {
                _failed = true;
                throw;
            }
            finally
            {
                if (output != null)
                {
                    DisposeColumns(output);
                }
                if (kept != null)
                {
                    DisposeColumns(kept);
                }
                _lock.Release();
            }
        }

        private static void DisposeColumns(Column[] columns)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i]?.Dispose();
            }
        }

        // The layout a relation's row store asks for, null without one
        public static RowStoreHostLayout? ReadLayout(RowStoreDefinition? rowStore, int columnCount, List<int> keyColumns, List<int> emitList)
        {
            if (rowStore == null)
            {
                return null;
            }
            if (rowStore.StoredColumns.Count == 0)
            {
                throw new InvalidOperationException($"Row store '{rowStore.Name}' stores no column.");
            }
            var positions = new List<int>();
            var deferred = new List<int>();
            foreach (var position in rowStore.StoredColumns)
            {
                if (position < 0 || position >= emitList.Count)
                {
                    throw new InvalidOperationException($"Row store '{rowStore.Name}' stores output column {position}, the relation has {emitList.Count}.");
                }
                int column = emitList[position];
                if (keyColumns.Contains(column))
                {
                    throw new InvalidOperationException($"Row store '{rowStore.Name}' stores output column {position}, a key column is not stored behind a reference.");
                }
                if (positions.Contains(position))
                {
                    throw new InvalidOperationException($"Row store '{rowStore.Name}' names output column {position} twice.");
                }
                positions.Add(position);
                if (!deferred.Contains(column))
                {
                    deferred.Add(column);
                }
            }
            // A column sent at another position too is also kept
            var kept = new List<int>();
            for (int i = 0; i < emitList.Count; i++)
            {
                if (!positions.Contains(i) && !keyColumns.Contains(emitList[i]) && !kept.Contains(emitList[i]))
                {
                    kept.Add(emitList[i]);
                }
            }
            var storedColumns = new List<int>(positions.Count);
            foreach (var position in positions)
            {
                storedColumns.Add(deferred.IndexOf(emitList[position]));
            }
            return new RowStoreHostLayout(columnCount, keyColumns, kept, deferred) { DeferredPositions = positions, StoredColumnOfPosition = storedColumns };
        }

        // True when an output position is behind the reference
        private bool IsDeferred(int position, int column)
        {
            if (_layout.DeferredPositions != null)
            {
                return _layout.DeferredPositions.Contains(position);
            }
            return _layout.DeferredColumns.Contains(column);
        }

        // The rows to send: kept output columns, then the reference
        public (EventBatchWeighted? inserts, EventBatchWeighted? deletes) CreateBatches(EventBatchData batch, RowStoreHostResult result, List<int> emitList)
        {
            EventBatchWeighted? inserts = null;
            EventBatchWeighted? deletes = null;
            int width = 1;
            for (int i = 0; i < emitList.Count; i++)
            {
                if (!IsDeferred(i, emitList[i]))
                {
                    width++;
                }
            }
            if (result.InsertRows.Count > 0)
            {
                var columns = new IColumn[width];
                bool offsetsUsed = false;
                int at = 0;
                for (int i = 0; i < emitList.Count; i++)
                {
                    if (IsDeferred(i, emitList[i]))
                    {
                        continue;
                    }
                    columns[at++] = OutputColumn(batch.Columns[emitList[i]], result.InsertRows, out var used);
                    offsetsUsed |= used;
                }
                columns[at] = result.InsertReferences;
                inserts = new EventBatchWeighted(Weights(result.InsertRows.Count, 1), Iterations(result.InsertRows.Count), new EventBatchData(columns));
                if (!offsetsUsed)
                {
                    result.InsertRows.Dispose();
                }
            }
            else
            {
                result.InsertRows.Dispose();
                result.InsertReferences.Dispose();
            }
            if (result.DeleteRows.Count > 0)
            {
                var columns = new IColumn[width];
                bool offsetsUsed = false;
                bool[] keptUsed = new bool[result.DeleteKeptColumns.Length];
                int at = 0;
                for (int i = 0; i < emitList.Count; i++)
                {
                    int column = emitList[i];
                    if (IsDeferred(i, column))
                    {
                        continue;
                    }
                    int kept = _layout.KeptColumns.IndexOf(column);
                    if (kept >= 0 && !keptUsed[kept])
                    {
                        columns[at++] = result.DeleteKeptColumns[kept];
                        keptUsed[kept] = true;
                    }
                    else if (kept >= 0)
                    {
                        columns[at++] = result.DeleteKeptColumns[kept].Copy(_memoryAllocator);
                    }
                    else
                    {
                        columns[at++] = OutputColumn(batch.Columns[column], result.DeleteRows, out var used);
                        offsetsUsed |= used;
                    }
                }
                columns[at] = result.DeleteReferences;
                deletes = new EventBatchWeighted(Weights(result.DeleteRows.Count, -1), Iterations(result.DeleteRows.Count), new EventBatchData(columns));
                if (!offsetsUsed)
                {
                    result.DeleteRows.Dispose();
                }
                for (int i = 0; i < keptUsed.Length; i++)
                {
                    if (!keptUsed[i])
                    {
                        result.DeleteKeptColumns[i].Dispose();
                    }
                }
            }
            else
            {
                result.Dispose(false);
            }
            return (inserts, deletes);
        }

        // Rows a key removal deleted: keys, kept, reference
        public EventBatchWeighted? CreateDeleteBatch(Column[] keyColumns, RowStoreHostResult result, List<int> emitList)
        {
            int count = result.DeleteRows.Count;
            result.InsertRows.Dispose();
            result.InsertReferences.Dispose();
            if (count == 0)
            {
                result.Dispose(false);
                return null;
            }
            // Called outside the lock, with a container of its own
            var value = new DataValueContainer();
            var columns = new List<IColumn>();
            var keptUsed = new bool[result.DeleteKeptColumns.Length];
            for (int i = 0; i < emitList.Count; i++)
            {
                int column = emitList[i];
                if (IsDeferred(i, column))
                {
                    continue;
                }
                int key = _layout.KeyColumns.IndexOf(column);
                int kept = _layout.KeptColumns.IndexOf(column);
                if (key >= 0)
                {
                    var copy = Column.Create(_memoryAllocator);
                    for (int r = 0; r < count; r++)
                    {
                        keyColumns[key].GetValueAt(result.DeleteRows[r], value, default);
                        copy.Add(value);
                    }
                    columns.Add(copy);
                }
                else if (kept >= 0)
                {
                    columns.Add(keptUsed[kept] ? result.DeleteKeptColumns[kept].Copy(_memoryAllocator) : result.DeleteKeptColumns[kept]);
                    keptUsed[kept] = true;
                }
            }
            columns.Add(result.DeleteReferences);
            for (int k = 0; k < keptUsed.Length; k++)
            {
                if (!keptUsed[k])
                {
                    result.DeleteKeptColumns[k].Dispose();
                }
            }
            result.DeleteRows.Dispose();
            return new EventBatchWeighted(Weights(count, -1), Iterations(count), new EventBatchData(columns.ToArray()));
        }

        private PrimitiveList<int> Weights(int count, int weight)
        {
            var weights = new PrimitiveList<int>(_memoryAllocator);
            for (int i = 0; i < count; i++)
            {
                weights.Add(weight);
            }
            return weights;
        }

        private PrimitiveList<uint> Iterations(int count)
        {
            var iterations = new PrimitiveList<uint>(_memoryAllocator);
            for (int i = 0; i < count; i++)
            {
                iterations.Add(0);
            }
            return iterations;
        }

        // Called in OnCheckpoint, before the barrier is forwarded
        public async Task BeginCheckpointAsync()
        {
            await EnterAsync().ConfigureAwait(false);
            try
            {
                if (_tree != null)
                {
                    await _tree.Commit().ConfigureAwait(false);
                }
                if (_newRecord != null)
                {
                    await _newRecord.Commit().ConfigureAwait(false);
                    _newRecord = null;
                }
                await _store.BeginCheckpointAsync().ConfigureAwait(false);
            }
            catch
            {
                _failed = true;
                throw;
            }
            finally
            {
                _lock.Release();
            }
        }

        // Called in BeforeSaveCheckpoint, the sinks are done
        public async Task CheckpointAsync()
        {
            await EnterAsync().ConfigureAwait(false);
            try
            {
                await _store.CheckpointAsync().ConfigureAwait(false);
            }
            catch
            {
                _failed = true;
                throw;
            }
            finally
            {
                _lock.Release();
            }
        }

        // Waits for a call that is under way, then closes
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _lock.Wait();
            try
            {
                if (_disposed)
                {
                    return;
                }
                _disposed = true;
                _searcher?.Dispose();
                _reader.Dispose();
                _store.Dispose();
            }
            finally
            {
                _lock.Release();
            }
        }
    }
}
