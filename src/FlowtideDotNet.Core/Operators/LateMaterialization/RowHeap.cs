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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using System.Runtime.ExceptionServices;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal sealed partial class RowHeap : IDisposable
    {
        private const int FormatVersion = 2;

        // A directory value is the page and the sequence's live rows
        private const int LiveBits = 20;
        private const long LiveMask = (1L << LiveBits) - 1;

        private readonly string _name;
        private readonly int _columnCount;
        private readonly RowHeapOptions _options;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly ImmutablePageClient<RowHeapPage> _client;
        private readonly PageWindow<RowHeapPage> _window;
        private readonly IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _index;
        private readonly IObjectState<RowHeapState> _state;
        private readonly RowHeapDirectory _directory;
        private readonly RowHeapPageTable _pages;
        private readonly List<RowHeapReader> _readers;
        private long _nextSequence;
        private long _rowCount;
        private long _storedBytes;
        private long _oversizedBytes;
        private int[] _rows;
        private int[] _columnSizes;
        private long[] _sizes;
        private bool _disposed;

        private RowHeap(
            string name,
            int columnCount,
            RowHeapOptions options,
            IMemoryAllocator memoryAllocator,
            ImmutablePageClient<RowHeapPage> client,
            PageWindow<RowHeapPage> window,
            IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> index,
            IObjectState<RowHeapState> state)
        {
            _name = name;
            _columnCount = columnCount;
            _options = options;
            _memoryAllocator = memoryAllocator;
            _client = client;
            _window = window;
            _index = index;
            _state = state;
            _directory = new RowHeapDirectory();
            _pages = new RowHeapPageTable();
            _released = new RowHeapReleasedRows();
            _budget = options.CompactPagesPerCall;
            _small = new Queue<long>();
            // Above the ratio some page is always this sparse
            _sparse = new Queue<long>[(int)Math.Ceiling(BucketsPerPage / options.MaxStoredRatio)];
            for (int i = 0; i < _sparse.Length; i++)
            {
                _sparse[i] = new Queue<long>();
            }
            _unmeasured = new Queue<long>();
            _readSequences = new List<long>();
            _readCounts = new List<int>();
            _readers = new List<RowHeapReader>();
            _changedSequences = new HashSet<long>();
            _changedWords = new HashSet<long>();
            _changedPages = new HashSet<long>();
            _forgotten = new List<long>();
            _retiredNow = new List<long>();
            _retiredBefore = new List<long>();
            _rows = Array.Empty<int>();
            _columnSizes = Array.Empty<int>();
            _sizes = Array.Empty<long>();
            _positions = Array.Empty<int>();
            _slots = Array.Empty<ushort>();
            _nextSequence = 1;
            _persistedSequence = 1;
        }

        public string Name => _name;

        public int ColumnCount => _columnCount;

        // Pages in storage, with those that wait to be deleted
        public long PageCount => _pages.Count;

        public long SequenceCount => Volatile.Read(ref _nextSequence) - 1;

        // Rows that are not released
        public long RowCount => Volatile.Read(ref _rowCount);

        // Bytes of stored pages before compression, with slot maps
        public long StoredBytes => Volatile.Read(ref _storedBytes);

        // Bytes of live rows, estimated per page until measured
        public long LiveBytes => Volatile.Read(ref _liveBytes);

        public long DirectoryBytes => _directory.ByteSize;

        public long PageTableBytes => _pages.ByteSize;

        // Released rows, kept while their sequence has a row left
        public long ReleasedSlotBytes => _released.ByteSize;

        // Queues of pages to merge or measure, store's thread only
        public long MergeQueueBytes => QueuedPages * sizeof(long);

        public long RowsReleased => Volatile.Read(ref _rowsReleased);

        // Rows copied into merged pages
        public long RowsMerged => Volatile.Read(ref _rowsMerged);

        public long PagesMerged => Volatile.Read(ref _pagesMerged);

        public long PagesDeleted => Volatile.Read(ref _pagesDeleted);

        // Pages whose live bytes were counted from the page
        public long PagesMeasured => Volatile.Read(ref _pagesMeasured);

        // Pages read to measure or to merge them
        public long PagesRead => Volatile.Read(ref _pagesRead);

        // Pages that may still be read before the next checkpoint
        public int CompactBudget => Volatile.Read(ref _budget);

        internal RowHeapOptions Options => _options;

        internal IMemoryAllocator MemoryAllocator => _memoryAllocator;

        internal ImmutablePageClient<RowHeapPage> Client => _client;

        internal RowHeapDirectory Directory => _directory;

        internal static long Pack(long page, int live)
        {
            return ((page + 1) << LiveBits) | (uint)live;
        }

        internal static long PageOf(long entry)
        {
            return (entry >> LiveBits) - 1;
        }

        internal static int LiveOf(long entry)
        {
            return (int)(entry & LiveMask);
        }

        public static async Task<RowHeap> CreateAsync(IStateManagerClient stateManagerClient, string name, int columnCount, RowHeapOptions options, IMemoryAllocator memoryAllocator)
        {
            if (columnCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(columnCount), "A row heap stores at least one column.");
            }
            if (options.PageTargetBytes < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "The page target must be at least one byte.");
            }
            // Written so that NaN is refused too
            if (!(options.MaxStoredRatio >= MinStoredRatio && options.MaxStoredRatio <= MaxStoredRatio))
            {
                throw new ArgumentOutOfRangeException(nameof(options), $"The stored bytes may grow to between {MinStoredRatio} and {MaxStoredRatio} times the live rows, got {options.MaxStoredRatio}.");
            }
            if (options.CompactPagesPerCall < 2)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "A call must be let read at least two pages.");
            }
            if (stateManagerClient is not StateManagerSyncClient syncClient)
            {
                throw new NotSupportedException($"Row heap '{name}' needs the stream's own state manager client.");
            }
            // The stored layout is checked before a page client is made for it
            var state = await stateManagerClient.GetOrCreateObjectStateAsync<RowHeapState>($"{name}_state");
            var stored = state.Value;
            if (stored != null && (stored.FormatVersion != FormatVersion || stored.SlotBits != RowReference.SlotBits || stored.ColumnCount != columnCount))
            {
                throw new InvalidOperationException(
                    $"Row heap '{name}' is stored with format {stored.FormatVersion}, {stored.SlotBits} slot bits and {stored.ColumnCount} columns, " +
                    $"the stream expects format {FormatVersion}, {RowReference.SlotBits} slot bits and {columnCount} columns.");
            }
            var writeHook = options.PageWriteHookForTests;
            var pageOptions = new ImmutablePageClientOptions<RowHeapPage>(() => new RowHeapPageSerializer(columnCount, memoryAllocator, writeHook))
            {
                WriteQueueCapacity = options.WriteQueueCapacity,
                WindowSize = options.WindowSize
            };
            var client = syncClient.GetOrCreateImmutablePageClient($"{name}_pages", pageOptions, memoryAllocator, out bool created);
            PageWindow<RowHeapPage>? window = null;
            try
            {
                try
                {
                    window = client.CreateWindow();
                }
                catch (InvalidOperationException e) when (e is not ObjectDisposedException)
                {
                    throw new InvalidOperationException($"Row heap '{name}' cannot be opened, the cache has no room for the pages it reads when it merges.", e);
                }
                var index = await stateManagerClient.GetOrCreateTree($"{name}_index", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(memoryAllocator),
                    MemoryAllocator = memoryAllocator
                });
                var heap = new RowHeap(name, columnCount, options, memoryAllocator, client, window, index, state);
                await heap.RestoreAsync();
                return heap;
            }
            catch
            {
                window?.Dispose();
                if (created)
                {
                    // Removes the client and waits for it, the first failure is thrown
                    syncClient.RemoveImmutablePageClient($"{name}_pages", client);
                    await Task.WhenAny(client.DisposalTask).ConfigureAwait(false);
                }
                throw;
            }
        }

        private async Task RestoreAsync()
        {
            var stored = _state.Value;
            long nextPage = 0;
            if (stored == null)
            {
                _state.Value = new RowHeapState()
                {
                    FormatVersion = FormatVersion,
                    SlotBits = RowReference.SlotBits,
                    ColumnCount = _columnCount,
                    NextSequence = 1
                };
            }
            else
            {
                nextPage = stored.NextPage;
                if (nextPage < 0 || nextPage > ImmutablePageIds.RangeSize)
                {
                    throw new InvalidOperationException($"Row heap '{_name}' is stored with page counter {nextPage}, outside the ids of its pages.");
                }
                _nextSequence = stored.NextSequence;
                _persistedSequence = stored.NextSequence;
                _rowCount = stored.RowCount;
                _storedBytes = stored.StoredBytes;
                await ReadIndexAsync(nextPage);
            }
            long pageId = _client.FirstPageId + nextPage;
            // Pages stored after this state are swept, they have no gap
            while (pageId <= _client.LastReservedPageId && _client.IsStored(pageId))
            {
                _client.Delete(pageId);
                pageId++;
            }
            // Their ids are not used again
            _client.NextPageId = pageId;
            _persistedPage = pageId - _client.FirstPageId;
        }

        // Called by the store's own thread only
        public async ValueTask AppendAsync(EventBatchData payload, int start, int count, Column references)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            if (payload.Columns.Count != _columnCount)
            {
                throw new ArgumentException($"Row heap '{_name}' stores {_columnCount} columns, got {payload.Columns.Count}.", nameof(payload));
            }
            // Subtraction, a sum of the two can wrap around
            if (start < 0 || count < 0 || count > payload.Count - start)
            {
                throw new ArgumentOutOfRangeException(nameof(count), $"{count} rows from row {start} are not inside a batch of {payload.Count} rows.");
            }
            if (count == 0)
            {
                return;
            }
            MeasureRows(payload, start, count);
            int end = start + count;
            int row = start;
            while (row < end)
            {
                int pageEnd = NextPageEnd(start, row, end, out long pageBytes);
                if (pageBytes > int.MaxValue)
                {
                    throw new InvalidOperationException($"Row {row} of the batch is {pageBytes} bytes, more than a page of row heap '{_name}' can hold.");
                }
                int byteSize = (int)pageBytes;
                int rows = pageEnd - row;
                int maxRow = LargestRow(start, row, pageEnd);
                long sequence = _nextSequence;
                RowReference.ThrowIfExhausted(sequence, _name);
                var page = BuildPage(payload, row, rows, sequence);
                long pageId;
                try
                {
                    pageId = _client.NewPageId();
                    await _client.PublishAsync(pageId, page).ConfigureAwait(false);
                }
                catch
                {
                    // The client did not take the page
                    page.Return();
                    throw;
                }
                long number = pageId - _client.FirstPageId;
                AddPage(number, rows, byteSize, false, maxRow, 1);
                // The slot is written before a reference to it exists
                _directory.Set(sequence, Pack(number, rows));
                Volatile.Write(ref _nextSequence, sequence + 1);
                Volatile.Write(ref _rowCount, _rowCount + rows);
                for (int i = 0; i < rows; i++)
                {
                    references.Add(new RowRefValue(RowReference.Pack(sequence, i)));
                }
                await AfterPublishAsync(byteSize, _options.PageTargetBytes).ConfigureAwait(false);
                row = pageEnd;
            }
        }

        // A page above its limit counts as oversized for the writer
        private ValueTask AfterPublishAsync(int byteSize, long limit)
        {
            if (byteSize <= limit)
            {
                return ValueTask.CompletedTask;
            }
            _oversizedBytes += byteSize;
            if (_oversizedBytes <= (long)_options.PageTargetBytes * _options.WriteQueueCapacity)
            {
                return ValueTask.CompletedTask;
            }
            // Oversized pages wait only up to a full queue's bytes
            _oversizedBytes = 0;
            return new ValueTask(_client.DrainAsync());
        }

        private void MeasureRows(EventBatchData payload, int start, int count)
        {
            if (_rows.Length < count)
            {
                int size = Math.Max(count, _rows.Length * 2);
                _rows = new int[size];
                _columnSizes = new int[size];
                _sizes = new long[size];
            }
            for (int i = 0; i < count; i++)
            {
                _rows[i] = start + i;
            }
            SumRowBytes(payload.Columns, count, out _);
        }

        // Sums the listed rows' bytes, also past a column's 32-bit wrap
        private long SumRowBytes(IReadOnlyList<IColumn> columns, int count, out int maxRow)
        {
            maxRow = 0;
            if (count == 0)
            {
                return 0;
            }
            var sizes = _sizes.AsSpan(0, count);
            sizes.Clear();
            for (int c = 0; c < columns.Count; c++)
            {
                AddColumnBytes(columns[c], count, sizes);
            }
            long last = 0;
            long largest = 0;
            for (int i = 0; i < count; i++)
            {
                largest = Math.Max(largest, sizes[i] - last);
                last = sizes[i];
            }
            maxRow = (int)Math.Min(largest, int.MaxValue);
            return last;
        }

        private void AddColumnBytes(IColumn column, int count, Span<long> sizes)
        {
            if (column is Column plain && column.DataColumn is StructColumn fields)
            {
                // Field by field, a struct adds its fields up in 32 bits
                for (int i = 0; i < fields._columns.Length; i++)
                {
                    AddColumnBytes(fields._columns[i], count, sizes);
                }
                // The struct's own null bits
                var own = _columnSizes.AsSpan(0, count);
                own.Clear();
                plain.GetValidityPrefixSumByteSizes(_rows.AsSpan(0, count), own);
                for (int i = 0; i < count; i++)
                {
                    sizes[i] += own[i];
                }
                return;
            }
            int from = 0;
            long before = 0;
            while (from < count)
            {
                var part = _columnSizes.AsSpan(from, count - from);
                part.Clear();
                column.GetPrefixSumByteSizes(_rows.AsSpan(from, count - from), part);
                int good = 0;
                int previous = 0;
                while (good < part.Length && part[good] >= previous)
                {
                    previous = part[good];
                    sizes[from + good] += before + previous;
                    good++;
                }
                if (good == part.Length)
                {
                    break;
                }
                if (good == 0)
                {
                    throw new InvalidOperationException($"A row of row heap '{_name}' is more than {int.MaxValue} bytes in one column.");
                }
                before += previous;
                from += good;
            }
        }

        private int NextPageEnd(int start, int row, int end, out long byteSize)
        {
            int limit = Math.Min(end, row + RowReference.MaxRowsPerPage);
            long before = row == start ? 0 : _sizes[row - start - 1];
            byteSize = _sizes[limit - start - 1] - before;
            if (byteSize <= _options.PageTargetBytes)
            {
                return limit;
            }
            // Find the last end that fits, a page holds at least one row
            int low = row + 1;
            int high = limit;
            while (low < high)
            {
                int middle = low + (high - low + 1) / 2;
                if (_sizes[middle - start - 1] - before <= _options.PageTargetBytes)
                {
                    low = middle;
                }
                else
                {
                    high = middle - 1;
                }
            }
            byteSize = _sizes[low - start - 1] - before;
            return low;
        }

        // The largest row of a page that is being appended
        private int LargestRow(int start, int row, int pageEnd)
        {
            long previous = row == start ? 0 : _sizes[row - start - 1];
            long largest = 0;
            for (int i = row; i < pageEnd; i++)
            {
                largest = Math.Max(largest, _sizes[i - start] - previous);
                previous = _sizes[i - start];
            }
            return (int)largest;
        }

        private RowHeapPage BuildPage(EventBatchData payload, int row, int rows, long sequence)
        {
            var columns = new IColumn[_columnCount];
            try
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    var column = Column.Create(_memoryAllocator);
                    columns[i] = column;
                    column.InsertRangeFrom(0, payload.Columns[i], row, rows);
                }
            }
            catch
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i]?.Dispose();
                }
                throw;
            }
            return new RowHeapPage(new EventBatchData(columns), rows, sequence);
        }

        public RowHeapReader CreateReader()
        {
            PageWindow<RowHeapPage> window;
            try
            {
                window = _client.CreateWindow();
            }
            catch (InvalidOperationException e) when (e is not ObjectDisposedException)
            {
                throw new InvalidOperationException($"Row heap '{_name}' cannot take another reader, the cache has no room for a window of {_options.WindowSize} pages.", e);
            }
            var reader = new RowHeapReader(this, window);
            bool disposed;
            lock (_readers)
            {
                disposed = _disposed;
                if (!disposed)
                {
                    _readers.Add(reader);
                }
            }
            if (disposed)
            {
                window.Dispose();
                throw new ObjectDisposedException(nameof(RowHeap));
            }
            return reader;
        }

        internal void RemoveReader(RowHeapReader reader)
        {
            lock (_readers)
            {
                _readers.Remove(reader);
            }
        }

        // Called when every reader and the store have stopped using the heap
        public void Dispose()
        {
            RowHeapReader[] readers;
            lock (_readers)
            {
                if (_disposed)
                {
                    return;
                }
                Volatile.Write(ref _disposed, true);
                readers = _readers.ToArray();
            }
            Exception? failure = null;
            for (int i = 0; i < readers.Length; i++)
            {
                try
                {
                    readers[i].Dispose();
                }
                catch (Exception e)
                {
                    // The other readers still give their windows back
                    failure ??= e;
                }
            }
            try
            {
                _window.Dispose();
            }
            catch (Exception e)
            {
                failure ??= e;
            }
            if (failure != null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }
    }
}
