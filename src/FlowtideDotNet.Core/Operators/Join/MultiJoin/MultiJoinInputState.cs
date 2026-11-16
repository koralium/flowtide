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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// The rows one lookup step adds to the rows before it.
    /// </summary>
    internal sealed class MultiJoinFrame
    {
        public int Count;

        // The rows stand for a padded side: no columns, every value is null
        public bool IsNull;

        // The rows carry the mark of a mark join
        public bool HasMark;
        public bool[] Mark = Array.Empty<bool>();

        // For row r: the row of the frame before this one it was found for
        public int[] Parent = Array.Empty<int>();

        // The stored weight when the lookup hands the frame over, the weight of the joined row after that
        public int[] Weight = Array.Empty<int>();

        // The copied columns of the part, in the order of the columns asked for
        public Column[] Columns = Array.Empty<Column>();

        public void EnsureCapacity(int capacity)
        {
            if (Parent.Length < capacity)
            {
                int size = Math.Max(capacity, Parent.Length * 2);
                Array.Resize(ref Parent, size);
                Array.Resize(ref Weight, size);
            }
        }
    }

    /// <summary>
    /// Bulk lookup of key rows on a leading part of one tree, handed out in parts.
    /// </summary>
    internal sealed class MultiJoinTreeLookup
    {
        private readonly IBplusTreeBulkSearch<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer, MultiJoinSearchComparer> _searcher;
        private readonly BatchSorter _sorter;
        private readonly IColumn[] _keyColumns;
        private readonly EventBatchData _keyBatch;
        private ColumnRowReference[] _refs = Array.Empty<ColumnRowReference>();
        private int[] _sorted = Array.Empty<int>();
        private int[] _leafIndices = Array.Empty<int>();
        private int[] _targetPositions = Array.Empty<int>();
        private int _count;
        private bool _exhausted;

        // Where the next part goes on in the leaf the searcher stands on
        private bool _leafOpen;
        private int _resultIndex;
        private int _matchOffset;

        /// <summary>
        /// Searches the tree once per distinct key of a lookup.
        /// </summary>
        internal static bool DistinctKeySearch = true;

        // The key rows searched for, and for each key row the next one with the same key
        private int[] _distinct = Array.Empty<int>();
        private int[] _tags = Array.Empty<int>();
        private int[] _sameKey = Array.Empty<int>();
        private bool _distinctSearch;
        private int _keyRow;

        /// <summary>
        /// The last part held every row that was left, the tree is not read again for these keys.
        /// </summary>
        public bool IsExhausted => _exhausted;

        public MultiJoinTreeLookup(
            IBPlusTree<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> tree,
            List<int> treeColumns,
            bool matchNulls = false)
        {
            var keyPositions = new List<int>(treeColumns.Count);
            for (int i = 0; i < treeColumns.Count; i++)
            {
                keyPositions.Add(i);
            }
            _searcher = tree.CreateBulkSearcher(new MultiJoinSearchComparer(treeColumns, keyPositions) { MatchNulls = matchNulls });
            _sorter = new BatchSorter(treeColumns.Count);
            _keyColumns = new IColumn[treeColumns.Count];
            // The key rows are addressed through one batch object, its columns are swapped per lookup
            _keyBatch = new EventBatchData(_keyColumns);
        }

        /// <summary>
        /// Where the caller puts the key columns of the next lookup before starting it.
        /// </summary>
        public IColumn[] KeyColumns => _keyColumns;

        public ValueTask Start(int count)
        {
            _count = count;
            _exhausted = false;
            _leafOpen = false;
            if (_refs.Length < count)
            {
                int size = Math.Max(count, _refs.Length * 2);
                _refs = new ColumnRowReference[size];
                _sorted = new int[size];
            }
            for (int i = 0; i < count; i++)
            {
                _refs[i] = new ColumnRowReference() { referenceBatch = _keyBatch, RowIndex = i };
                _sorted[i] = i;
            }
            var indirect = _sorted.AsSpan(0, count);
            _distinctSearch = DistinctKeySearch && count > 1 && KeysArePlain();
            if (!_distinctSearch)
            {
                _sorter.SortData(_keyColumns, ref indirect);
                return _searcher.Start(_refs, count, _sorted);
            }
            if (_tags.Length < count)
            {
                _tags = new int[_refs.Length];
                _distinct = new int[_refs.Length];
                _sameKey = new int[_refs.Length];
            }
            var tags = _tags.AsSpan(0, count);
            _sorter.SortDataWithTags(_keyColumns, ref indirect, ref tags);
            int distinct = 0;
            for (int i = 0; i < count; i++)
            {
                int row = indirect[i];
                _sameKey[row] = -1;
                if (i > 0 && tags[i] == tags[i - 1])
                {
                    _sameKey[indirect[i - 1]] = row;
                }
                else
                {
                    _distinct[distinct++] = row;
                }
            }
            return _searcher.Start(_refs, distinct, _distinct);
        }

        // Telling equal keys apart reads lists, maps, structs, columns of several types and any column past the seventh as an object per value
        private bool KeysArePlain()
        {
            if (_keyColumns.Length > MaxComparedInPlace)
            {
                return false;
            }
            for (int i = 0; i < _keyColumns.Length; i++)
            {
                if (!IsPlain(_keyColumns[i].Type))
                {
                    return false;
                }
            }
            return true;
        }

        // The batch sorter compares this many key columns without an object per value
        internal const int MaxComparedInPlace = 7;

        // A column of nothing but nulls is appended as a range, row by row every null would move the ones before it
        internal static bool AppendedAsNulls(Column target, IColumn source, int count)
        {
            if (source.Type != ArrowTypeId.Null)
            {
                return false;
            }
            target.InsertNullRange(target.Count, count);
            return true;
        }

        private void CopyOut(Column[] columns, IReadOnlyList<int> copyColumns, EventBatchData leafData, int leafCount)
        {
            if (leafCount == 0 || columns.Length == 0)
            {
                return;
            }
            if (_targetPositions.Length < leafCount)
            {
                _targetPositions = new int[Math.Max(leafCount, _targetPositions.Length * 2)];
            }
            int initialCount = columns[0].Count;
            var positions = _targetPositions.AsSpan(0, leafCount);
            positions.Fill(initialCount);
            ReadOnlySpan<int> lookup = _leafIndices.AsSpan(0, leafCount);
            ReadOnlySpan<int> readOnlyPositions = positions;
            for (int z = 0; z < columns.Length; z++)
            {
                var source = leafData.Columns[copyColumns[z]];
                if (AppendedAsNulls(columns[z], source, leafCount))
                {
                    continue;
                }
                if (TakesRowsAsObjects(columns[z], source))
                {
                    CopyRuns(columns[z], source, lookup);
                    continue;
                }
                columns[z].InsertFrom(source, in lookup, in readOnlyPositions, ColumnWithOffset.NullValueIndex);
            }
        }

        // Only a column of plain values of one type copies single rows without an object each, every other kind copies ranges that way
        internal static bool TakesRowsAsObjects(Column target, IColumn source)
        {
            var type = source.Type;
            if (!IsPlain(type) || !IsPlain(target.Type))
            {
                return true;
            }
            // Rows of another type turn the column into one of several types
            return type != ArrowTypeId.Null && target.Type != ArrowTypeId.Null && target.Type != type;
        }

        internal static bool IsPlain(ArrowTypeId type)
        {
            switch (type)
            {
                case ArrowTypeId.Null:
                case ArrowTypeId.Boolean:
                case ArrowTypeId.Int64:
                case ArrowTypeId.Double:
                case ArrowTypeId.String:
                case ArrowTypeId.Binary:
                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Timestamp:
                    return true;
                default:
                    // Lists, maps, structs and columns of several types
                    return false;
            }
        }

        // Appends the first rows of a column. A view is read through to the column it is over, so its rows are copied without an object per value
        internal static void AppendRows(Column target, IColumn source, int count, ref int[] positions)
        {
            if (source is not ColumnWithOffset view)
            {
                target.InsertRangeFrom(target.Count, source, 0, count);
                return;
            }
            ReadOnlySpan<int> rows = view.Offsets.Span.Slice(0, count);
            var inner = view.InnerColumn;
            if (AppendedAsNulls(target, inner, count))
            {
                return;
            }
            if (TakesRowsAsObjects(target, inner))
            {
                CopyRuns(target, inner, rows);
                return;
            }
            if (positions.Length < count)
            {
                positions = new int[Math.Max(count, positions.Length * 2)];
            }
            positions.AsSpan(0, count).Fill(target.Count);
            ReadOnlySpan<int> at = positions.AsSpan(0, count);
            target.InsertFrom(inner, in rows, in at, ColumnWithOffset.NullValueIndex);
        }

        internal static void CopyRuns(Column target, IColumn source, ReadOnlySpan<int> rows)
        {
            int i = 0;
            while (i < rows.Length)
            {
                int start = rows[i];
                int length = 1;
                if (start == ColumnWithOffset.NullValueIndex)
                {
                    // A view names no row here, the value is null
                    while (i + length < rows.Length && rows[i + length] == ColumnWithOffset.NullValueIndex)
                    {
                        length++;
                    }
                    target.InsertNullRange(target.Count, length);
                    i += length;
                    continue;
                }
                while (i + length < rows.Length && rows[i + length] == start + length)
                {
                    length++;
                }
                target.InsertRangeFrom(target.Count, source, start, length);
                i += length;
            }
        }

        /// <summary>
        /// Adds matches of the current leaf to the frame, from where the part before stopped, up to the row budget.
        /// </summary>
        private void CollectFromLeaf(MultiJoinFrame target, IReadOnlyList<int> copyColumns, int rowBudget, IMemoryAllocator allocator, ref bool created)
        {
            var leaf = _searcher.CurrentLeaf;
            leaf.EnterWriteLock();
            try
            {
                var values = leaf.values;
                var results = _searcher.CurrentResults;
                int room = rowBudget - target.Count;
                int leafCount = 0;
                while (_resultIndex < results.Count && leafCount < room)
                {
                    var result = results[_resultIndex];
                    int lower = (result.LowerBound < 0 ? ~result.LowerBound : result.LowerBound) + _matchOffset;
                    int upper = result.UpperBound;
                    int take = Math.Min(upper - lower + 1, room - leafCount);
                    if (take <= 0)
                    {
                        _resultIndex++;
                        _matchOffset = 0;
                        _keyRow = -1;
                        continue;
                    }
                    if (_keyRow < 0)
                    {
                        _keyRow = result.KeyIndex;
                    }
                    if (_leafIndices.Length < leafCount + take)
                    {
                        Array.Resize(ref _leafIndices, Math.Max(leafCount + take, _leafIndices.Length * 2));
                    }
                    target.EnsureCapacity(target.Count + take);
                    for (int k = lower; k < lower + take; k++)
                    {
                        _leafIndices[leafCount++] = k;
                        target.Parent[target.Count] = _keyRow;
                        target.Weight[target.Count] = values.Get(k).Value;
                        target.Count++;
                    }
                    if (lower + take > upper)
                    {
                        _matchOffset = 0;
                        // The same rows of the leaf once more for the next key row with this key
                        _keyRow = _distinctSearch ? _sameKey[_keyRow] : -1;
                        if (_keyRow < 0)
                        {
                            _resultIndex++;
                        }
                    }
                    else
                    {
                        _matchOffset += take;
                    }
                }
                if (leafCount > 0)
                {
                    if (!created)
                    {
                        created = true;
                        for (int i = 0; i < target.Columns.Length; i++)
                        {
                            target.Columns[i] = Column.Create(allocator);
                        }
                    }
                    CopyOut(target.Columns, copyColumns, leaf.keys._data, leafCount);
                }
                _leafOpen = _resultIndex < results.Count;
            }
            finally
            {
                leaf.ExitWriteLock();
            }
        }

        /// <summary>
        /// Fills the frame with the next matches, at most the row budget.
        /// </summary>
        public async ValueTask<bool> NextPart(MultiJoinFrame target, IReadOnlyList<int> copyColumns, int rowBudget, IMemoryAllocator allocator)
        {
            target.Count = 0;
            if (_exhausted)
            {
                return false;
            }
            if (target.Columns.Length != copyColumns.Count)
            {
                target.Columns = copyColumns.Count == 0 ? Array.Empty<Column>() : new Column[copyColumns.Count];
            }
            bool created = false;
            try
            {
                while (_leafOpen || await _searcher.MoveNextLeaf())
                {
                    if (!_leafOpen)
                    {
                        _resultIndex = 0;
                        _matchOffset = 0;
                        _keyRow = -1;
                    }
                    CollectFromLeaf(target, copyColumns, rowBudget, allocator, ref created);
                    if (target.Count >= rowBudget)
                    {
                        return true;
                    }
                }
            }
            catch
            {
                // Nobody has the columns of this part yet
                if (created)
                {
                    for (int i = 0; i < target.Columns.Length; i++)
                    {
                        target.Columns[i]?.Dispose();
                        target.Columns[i] = null!;
                    }
                }
                throw;
            }
            _exhausted = true;
            Array.Clear(_refs, 0, _count);
            Array.Clear(_keyColumns);
            return target.Count > 0;
        }

        /// <summary>
        /// Lets go of what a lookup that will not be finished still holds.
        /// </summary>
        public void Abandon()
        {
            _searcher.Dispose();
            _leafOpen = false;
            _exhausted = true;
            Array.Clear(_refs, 0, _count);
            Array.Clear(_keyColumns);
        }
    }

    /// <summary>
    /// One tree with what is needed to bulk insert into it. Owns its scratch.
    /// </summary>
    internal sealed class MultiJoinTreeWriter
    {
        public required IBPlusTree<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> Tree { get; init; }
        public required IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> Inserter { get; init; }
        public required MergeJoinInsertComparer Comparer { get; init; }
        public required BatchSorter Sorter { get; init; }
        public required IColumn[] SortColumns { get; init; }
        public required int ColumnCount { get; init; }

        public ColumnRowReference[] Refs = Array.Empty<ColumnRowReference>();
        public MultiJoinWeight[] Values = Array.Empty<MultiJoinWeight>();
        public int[] Sorted = Array.Empty<int>();
        public int[] Tags = Array.Empty<int>();

        public void EnsureCapacity(int count)
        {
            if (Refs.Length < count)
            {
                int size = Math.Max(count, Refs.Length * 2);
                Refs = new ColumnRowReference[size];
                Values = new MultiJoinWeight[size];
                Sorted = new int[size];
                Tags = new int[size];
            }
        }

        /// <summary>
        /// Points the scratch at the batch and sorts it in tree order. The values are filled in by the caller.
        /// </summary>
        public void Prepare(EventBatchData batch, int count)
        {
            EnsureCapacity(count);
            for (int i = 0; i < count; i++)
            {
                Refs[i] = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                Sorted[i] = i;
            }
            for (int i = 0; i < Comparer.ColumnOrder.Count; i++)
            {
                SortColumns[i] = batch.Columns[Comparer.ColumnOrder[i]];
            }
            var indirect = Sorted.AsSpan(0, count);
            var tags = Tags.AsSpan(0, count);
            Sorter.SortDataWithTags(SortColumns, ref indirect, ref tags);
        }

        public void Release(int count)
        {
            Array.Clear(Refs, 0, Math.Min(count, Refs.Length));
            Array.Clear(SortColumns);
        }
    }

    /// <summary>
    /// Adds the weight of a row to its stored row and removes the row when the weight reaches zero.
    /// </summary>
    internal readonly struct MultiJoinPrimaryMutator : IRowMutator<ColumnRowReference, MultiJoinWeight>
    {
        private readonly int _columnCount;
        private readonly sbyte[] _changes;

        public MultiJoinPrimaryMutator(int columnCount, sbyte[] changes)
        {
            _columnCount = columnCount;
            _changes = changes;
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            MultiJoinSecondaryMutator.AddSizes(_columnCount, keys, indices, sizes);
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in MultiJoinWeight existingData, ref MultiJoinWeight incomingData, int sortedIndex)
        {
            if (incomingData.Value == 0)
            {
                // A row without weight changes nothing and must not be stored
                return GenericWriteOperation.None;
            }
            if (!exists)
            {
                _changes[sortedIndex] = 1;
                return GenericWriteOperation.Upsert;
            }
            incomingData = new MultiJoinWeight(incomingData.Value + existingData.Value);
            if (incomingData.Value == 0)
            {
                _changes[sortedIndex] = -1;
                return GenericWriteOperation.Delete;
            }
            return GenericWriteOperation.Upsert;
        }
    }

    /// <summary>
    /// Counts the primary rows behind a secondary entry, the entry goes when the count reaches zero.
    /// </summary>
    internal readonly struct MultiJoinSecondaryMutator : IRowMutator<ColumnRowReference, MultiJoinWeight>
    {
        private readonly int _columnCount;

        public MultiJoinSecondaryMutator(int columnCount)
        {
            _columnCount = columnCount;
        }

        internal static void AddSizes(int columnCount, ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            var batch = keys[0].referenceBatch;
            for (int i = 0; i < columnCount; i++)
            {
                batch.Columns[i].GetPrefixSumByteSizes(indices, sizes);
            }
            const int valueSize = sizeof(int);
            int cumulative = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                cumulative += valueSize;
                sizes[i] += cumulative;
            }
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            AddSizes(_columnCount, keys, indices, sizes);
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in MultiJoinWeight existingData, ref MultiJoinWeight incomingData, int sortedIndex)
        {
            if (incomingData.Value == 0)
            {
                // Nothing to add, the entry stays as it is
                return GenericWriteOperation.None;
            }
            if (!exists)
            {
                return GenericWriteOperation.Upsert;
            }
            incomingData = new MultiJoinWeight(incomingData.Value + existingData.Value);
            return incomingData.Value == 0 ? GenericWriteOperation.Delete : GenericWriteOperation.Upsert;
        }
    }

    /// <summary>
    /// A narrow tree for further key lists of an input: their columns, then the primary columns they lack.
    /// </summary>
    internal sealed class MultiJoinSecondaryIndex
    {
        public required int[] Layout { get; init; }
        public required MultiJoinTreeWriter Writer { get; init; }

        public required List<int> AllColumns { get; init; }

        // Reused for the batch a change is written from
        public required IColumn[] WriteColumns { get; init; }
        public required EventBatchData WriteBatch { get; init; }
    }

    /// <summary>
    /// The lookup of a secondary index by one of the key lists it serves.
    /// </summary>
    internal sealed class MultiJoinSecondaryLookup
    {
        public required MultiJoinTreeLookup Lookup { get; init; }

        // For every column of the primary prefix this key list leads to, its position in the layout
        public required int[] PrefixPositions { get; init; }

        public required List<int> AllColumns { get; init; }

        // The entries of the secondary part a lookup is working through
        public MultiJoinFrame Entries { get; } = new MultiJoinFrame();
    }

    /// <summary>
    /// A lookup of one input by one of its key lists, in parts.
    /// </summary>
    internal sealed class MultiJoinInputLookup
    {
        private readonly MultiJoinInput _plan;
        private readonly MultiJoinTreeLookup _primary;
        private readonly MultiJoinSecondaryLookup? _secondary;
        private bool _primaryActive;
        private bool _entriesHeld;

        public MultiJoinInputLookup(MultiJoinInput plan, MultiJoinTreeLookup primary, MultiJoinSecondaryLookup? secondary)
        {
            _plan = plan;
            _primary = primary;
            _secondary = secondary;
        }

        /// <summary>
        /// Where the caller puts the key columns before starting.
        /// </summary>
        public IColumn[] KeyColumns => _secondary != null ? _secondary.Lookup.KeyColumns : _primary.KeyColumns;

        public ValueTask Start(int count)
        {
            _primaryActive = false;
            return _secondary != null ? _secondary.Lookup.Start(count) : _primary.Start(count);
        }

        /// <summary>
        /// The last part held every row that was left, the trees are not read again for these keys.
        /// </summary>
        public bool IsExhausted => _secondary == null ? _primary.IsExhausted : _secondary.Lookup.IsExhausted && _primary.IsExhausted;

        /// <summary>
        /// The parent of a row in the frame is the key row it was found for.
        /// </summary>
        public ValueTask<bool> NextPart(MultiJoinFrame target, int rowBudget, IMemoryAllocator allocator, bool copyColumns)
        {
            IReadOnlyList<int> columns = copyColumns ? _plan.NeededColumns : Array.Empty<int>();
            if (_secondary == null)
            {
                return _primary.NextPart(target, columns, rowBudget, allocator);
            }
            return NextPartThroughSecondary(target, columns, rowBudget, allocator);
        }

        /// <summary>
        /// Lets go of what a lookup that will not be finished still holds.
        /// </summary>
        public void Abandon()
        {
            _primary.Abandon();
            if (_secondary != null)
            {
                _secondary.Lookup.Abandon();
                ReleaseEntries();
            }
            _primaryActive = false;
        }

        private void ReleaseEntries()
        {
            if (!_entriesHeld)
            {
                return;
            }
            _entriesHeld = false;
            var columns = _secondary!.Entries.Columns;
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i]?.Dispose();
                columns[i] = null!;
            }
        }

        // Back to the key rows the secondary was asked for
        private static void Remap(MultiJoinFrame target, MultiJoinFrame entries)
        {
            var parent = target.Parent.AsSpan(0, target.Count);
            var entryParent = entries.Parent.AsSpan(0, entries.Count);
            for (int r = 0; r < parent.Length; r++)
            {
                parent[r] = entryParent[parent[r]];
            }
        }

        private async ValueTask<bool> NextPartThroughSecondary(MultiJoinFrame target, IReadOnlyList<int> columns, int rowBudget, IMemoryAllocator allocator)
        {
            var secondary = _secondary!;
            var entries = secondary.Entries;
            while (true)
            {
                if (!_primaryActive)
                {
                    // The secondary gives the leading primary columns of the rows with this key
                    if (!await secondary.Lookup.NextPart(entries, secondary.AllColumns, rowBudget, allocator))
                    {
                        return false;
                    }
                    _entriesHeld = true;
                    var prefixColumns = _primary.KeyColumns;
                    for (int p = 0; p < prefixColumns.Length; p++)
                    {
                        prefixColumns[p] = entries.Columns[secondary.PrefixPositions[p]];
                    }
                    await _primary.Start(entries.Count);
                    _primaryActive = true;
                }
                if (await _primary.NextPart(target, columns, rowBudget, allocator))
                {
                    Remap(target, entries);
                    return true;
                }
                _primaryActive = false;
                ReleaseEntries();
            }
        }
    }

    /// <summary>
    /// What a multi join stores for one input: the rows once, and a narrow index per further join key.
    /// </summary>
    internal sealed class MultiJoinInputState
    {
        public MultiJoinInput Plan { get; }
        public DeferredInsertBuffer Defer { get; } = new DeferredInsertBuffer();

        private readonly int _index;
        private MultiJoinTreeWriter? _primary;
        private MultiJoinInputLookup[] _lookups = Array.Empty<MultiJoinInputLookup>();
        private Task<int>[]? _secondaryTasks;

        /// <summary>
        /// Batches whose indexes were updated side by side, for tests.
        /// </summary>
        internal int ParallelIndexUpdates;
        private int[]? _secondaryHits;
        private MultiJoinSecondaryIndex[] _secondaries = Array.Empty<MultiJoinSecondaryIndex>();
        private sbyte[] _changes = Array.Empty<sbyte>();
        private int[] _changeDeltas = Array.Empty<int>();
        private PrimitiveList<int>? _changedRows;

        public MultiJoinInputState(MultiJoinInput plan, int index)
        {
            Plan = plan;
            _index = index;
        }

        internal static async Task<MultiJoinTreeWriter> CreateWriter(IStateManagerClient client, string name, List<int> order, int columnCount, IMemoryAllocator allocator, bool ephemeral = false)
        {
            var comparer = new MergeJoinInsertComparer(order, columnCount);
            var options = new BPlusTreeOptions<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer>()
            {
                Comparer = comparer,
                KeySerializer = new ColumnStoreSerializer(columnCount, allocator),
                ValueSerializer = new MultiJoinWeightSerializer(allocator),
                UseByteBasedPageSizes = true,
                MemoryAllocator = allocator
            };
            var tree = ephemeral ? await client.GetOrCreateEphemeralTree(name, options) : await client.GetOrCreateTree(name, options);
            return new MultiJoinTreeWriter()
            {
                Tree = tree,
                Inserter = tree.CreateBulkInserter(),
                Comparer = comparer,
                Sorter = new BatchSorter(comparer.ColumnOrder.Count),
                SortColumns = new IColumn[comparer.ColumnOrder.Count],
                ColumnCount = columnCount
            };
        }

        public async Task Initialize(IStateManagerClient client, IMemoryAllocator allocator)
        {
            ReleaseScratch();
            _primary = await CreateWriter(client, $"p{_index}", Plan.PrimaryOrder, Plan.ColumnCount, allocator);

            _secondaries = new MultiJoinSecondaryIndex[Plan.SecondaryLayouts.Count];
            for (int s = 0; s < _secondaries.Length; s++)
            {
                var layout = Plan.SecondaryLayouts[s];
                var allColumns = new List<int>(layout.Length);
                for (int i = 0; i < layout.Length; i++)
                {
                    allColumns.Add(i);
                }
                var writer = await CreateWriter(client, $"s{_index}_{s}", allColumns, layout.Length, allocator);
                var writeColumns = new IColumn[layout.Length];
                _secondaries[s] = new MultiJoinSecondaryIndex()
                {
                    Layout = layout,
                    Writer = writer,
                    AllColumns = allColumns,
                    WriteColumns = writeColumns,
                    WriteBatch = new EventBatchData(writeColumns)
                };
            }

            _lookups = new MultiJoinInputLookup[Plan.KeyLists.Count];
            _besideLookups = new MultiJoinInputLookup[Plan.KeyLists.Count];
            for (int j = 0; j < _lookups.Length; j++)
            {
                _lookups[j] = CreateLookup(j);
                _besideLookups[j] = CreateLookup(j);
            }
        }

        internal MultiJoinInputLookup CreateLookup(int j)
        {
            int secondary = Plan.SecondaryOf[j];
            // Coming from a secondary entry the key is the stored values of the row, nulls among them
            var primaryLookup = new MultiJoinTreeLookup(_primary!.Tree, Plan.PrimaryOrder.GetRange(0, Plan.PrefixLengths[j]), matchNulls: secondary >= 0);
            MultiJoinSecondaryLookup? secondaryLookup = null;
            if (secondary >= 0)
            {
                var index = _secondaries[secondary];
                // The lookup hands back the columns of the layout, the primary wants them in its own order
                var prefixPositions = new int[Plan.PrefixLengths[j]];
                for (int p = 0; p < prefixPositions.Length; p++)
                {
                    prefixPositions[p] = Array.IndexOf(index.Layout, Plan.PrimaryOrder[p]);
                }
                secondaryLookup = new MultiJoinSecondaryLookup()
                {
                    Lookup = new MultiJoinTreeLookup(index.Writer.Tree, index.AllColumns.GetRange(0, Plan.KeyLists[j].Length)),
                    PrefixPositions = prefixPositions,
                    AllColumns = index.AllColumns
                };
            }
            return new MultiJoinInputLookup(Plan, primaryLookup, secondaryLookup);
        }

        /// <summary>
        /// The lookup of this input by one of its key lists. One lookup per key list exists, it is reused.
        /// </summary>
        public MultiJoinInputLookup GetLookup(int keyList) => _lookups[keyList];

        // A second lookup per key list, for a step that is looked up while another lookup waits
        private MultiJoinInputLookup[] _besideLookups = Array.Empty<MultiJoinInputLookup>();

        public MultiJoinInputLookup GetBesideLookup(int keyList) => _besideLookups[keyList];

        public void AbandonLookups()
        {
            foreach (var lookup in _lookups)
            {
                lookup.Abandon();
            }
            foreach (var lookup in _besideLookups)
            {
                lookup.Abandon();
            }
        }

        public void ReleaseScratch()
        {
            _changedRows?.Dispose();
            _changedRows = null;
        }

        public async Task Commit()
        {
            await _primary!.Tree.Commit();
            foreach (var secondary in _secondaries)
            {
                await secondary.Writer.Tree.Commit();
            }
        }

        /// <summary>
        /// Applies a batch to the stored rows and then to the secondary indexes.
        /// </summary>
        public async ValueTask Insert(EventBatchData batch, PrimitiveList<int> weights, int count, IMemoryAllocator allocator, TaskScheduler? scheduler = null)
        {
            var primary = _primary!;
            if (_changes.Length < count)
            {
                int size = Math.Max(count, _changes.Length * 2);
                _changes = new sbyte[size];
                _changeDeltas = new int[size];
            }
            Array.Clear(_changes, 0, count);

            int changedCount = 0;
            PrimitiveList<int>? changed = null;
            try
            {
                primary.Prepare(batch, count);
                MemoryMarshal.Cast<int, MultiJoinWeight>(weights.Span.Slice(0, count)).CopyTo(primary.Values);
                await primary.Inserter.ApplyBatch(
                    primary.Refs,
                    primary.Values,
                    count,
                    primary.Sorted,
                    primary.Tags,
                    new MultiJoinPrimaryMutator(primary.ColumnCount, _changes),
                    batch.GetByteSize() + count * sizeof(int));

                if (_secondaries.Length > 0)
                {
                    // The rows that created or removed a stored row
                    changed = _changedRows ??= new PrimitiveList<int>(allocator);
                    changed.Clear();
                    for (int s = 0; s < count; s++)
                    {
                        if (_changes[s] != 0)
                        {
                            changed.Add(primary.Sorted[s]);
                            _changeDeltas[changedCount++] = _changes[s];
                        }
                    }
                }
            }
            finally
            {
                // The batch is not referred to once the call is over, however it ended
                primary.Release(count);
            }

            int signalRows = count;
            int signalHits = primary.Inserter.LeafHitCount;

            if (changed != null && changedCount > 0)
            {
                var hits = _secondaryHits ??= new int[_secondaries.Length];
                if (scheduler != null && _secondaries.Length > 1)
                {
                    // Each index has scratch of its own, the batch and the changed rows are only read
                    for (int i = 0; i < batch.Columns.Count; i++)
                    {
                        // A view works out on first use whether it holds nulls and keeps the answer, two threads may not do that at once
                        if (batch.Columns[i] is ColumnWithOffset view && view.InnerColumn is Column)
                        {
                            view.GetColumnState();
                        }
                    }
                    var tasks = _secondaryTasks ??= new Task<int>[_secondaries.Length];
                    int started = 0;
                    ParallelIndexUpdates++;
                    try
                    {
                        for (int s = 0; s < tasks.Length; s++)
                        {
                            var secondary = _secondaries[s];
                            int rows = changedCount;
                            tasks[s] = Task.Factory.StartNew(
                                () => ApplySecondary(secondary, batch, changed, rows, count, allocator).AsTask(),
                                CancellationToken.None,
                                TaskCreationOptions.None,
                                scheduler).Unwrap();
                            started++;
                        }
                        // Every index is waited for, the failure of the lowest one is reported
                        await Task.WhenAll(tasks);
                    }
                    catch
                    {
                        // An index that could not be started leaves the ones before it running, the batch is theirs until they are done
                        for (int s = 0; s < started; s++)
                        {
                            await tasks[s].ContinueWith(static x => _ = x.Exception, TaskScheduler.Default);
                        }
                        throw;
                    }
                    finally
                    {
                        for (int s = 0; s < tasks.Length; s++)
                        {
                            hits[s] = s < started && tasks[s].IsCompletedSuccessfully ? tasks[s].Result : 0;
                            tasks[s] = null!;
                        }
                    }
                }
                else
                {
                    for (int s = 0; s < _secondaries.Length; s++)
                    {
                        hits[s] = await ApplySecondary(_secondaries[s], batch, changed, changedCount, count, allocator);
                    }
                }
                for (int s = 0; s < hits.Length; s++)
                {
                    // The most scattered tree decides whether inserts get buffered
                    if ((long)changedCount * signalHits < (long)signalRows * hits[s])
                    {
                        signalRows = changedCount;
                        signalHits = hits[s];
                    }
                }
            }

            Defer.OnBatchApplied(signalRows, signalHits);
        }

        private static void Scatter(ReadOnlySpan<int> rows, int[] deltas, MultiJoinWeight[] values, int count)
        {
            for (int i = 0; i < count; i++)
            {
                values[rows[i]] = new MultiJoinWeight(deltas[i]);
            }
        }

        private async ValueTask<int> ApplySecondary(MultiJoinSecondaryIndex secondary, EventBatchData batch, PrimitiveList<int> changed, int changedCount, int batchCount, IMemoryAllocator allocator)
        {
            var columns = secondary.WriteColumns;
            int size = changedCount * sizeof(int);
            List<PrimitiveList<int>>? ownOffsets = null;
            var writer = secondary.Writer;
            try
            {
                if (changedCount == batchCount)
                {
                    // Every row of the batch changed a stored row, the usual case when loading.
                    for (int c = 0; c < columns.Length; c++)
                    {
                        columns[c] = batch.Columns[secondary.Layout[c]];
                        size += columns[c].GetByteSize();
                    }
                }
                else
                {
                    for (int c = 0; c < columns.Length; c++)
                    {
                        var view = ColumnWithOffset.CreateFlattened(batch.Columns[secondary.Layout[c]], changed, allocator, out var usedGiven);
                        if (!usedGiven)
                        {
                            (ownOffsets ??= new List<PrimitiveList<int>>()).Add(view.Offsets);
                        }
                        columns[c] = view;
                        // An upper bound, the exact size would be read row by row and only decides how closely a leaf is measured
                        size += batch.Columns[secondary.Layout[c]].GetByteSize();
                    }
                }

                writer.Prepare(secondary.WriteBatch, changedCount);
                if (changedCount == batchCount)
                {
                    // Row i of the write batch is batch row i, its delta sits at the position that row was sorted to
                    Scatter(changed.Span, _changeDeltas, writer.Values, changedCount);
                }
                else
                {
                    MemoryMarshal.Cast<int, MultiJoinWeight>(_changeDeltas.AsSpan(0, changedCount)).CopyTo(writer.Values);
                }
                await writer.Inserter.ApplyBatch(
                    writer.Refs,
                    writer.Values,
                    changedCount,
                    writer.Sorted,
                    writer.Tags,
                    new MultiJoinSecondaryMutator(writer.ColumnCount),
                    size);
            }
            finally
            {
                // The views are let go of however the write ended
                writer.Release(changedCount);
                Array.Clear(columns);
                if (ownOffsets != null)
                {
                    foreach (var offsets in ownOffsets)
                    {
                        offsets.Dispose();
                    }
                }
            }
            return writer.Inserter.LeafHitCount;
        }
    }
}
