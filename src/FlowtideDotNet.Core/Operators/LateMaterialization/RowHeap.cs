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
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using System.Runtime.ExceptionServices;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal sealed class RowHeap : IDisposable
    {
        private const int FormatVersion = 1;

        private readonly string _name;
        private readonly int _columnCount;
        private readonly RowHeapOptions _options;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly ImmutablePageClient<RowHeapPage> _client;
        private readonly IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _directoryTree;
        private readonly IObjectState<RowHeapState> _state;
        private readonly RowHeapDirectory _directory;
        private readonly List<RowHeapReader> _readers;
        private long _nextSequence;
        private long _persistedSequence;
        private long _rowCount;
        private long _oversizedBytes;
        private int[] _rows;
        private int[] _sizes;
        private bool _disposed;

        private RowHeap(
            string name,
            int columnCount,
            RowHeapOptions options,
            IMemoryAllocator memoryAllocator,
            ImmutablePageClient<RowHeapPage> client,
            IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> directoryTree,
            IObjectState<RowHeapState> state)
        {
            _name = name;
            _columnCount = columnCount;
            _options = options;
            _memoryAllocator = memoryAllocator;
            _client = client;
            _directoryTree = directoryTree;
            _state = state;
            _directory = new RowHeapDirectory();
            _readers = new List<RowHeapReader>();
            _rows = Array.Empty<int>();
            _sizes = Array.Empty<int>();
            _nextSequence = 1;
            _persistedSequence = 1;
        }

        public string Name => _name;

        public int ColumnCount => _columnCount;

        public long PageCount => Volatile.Read(ref _nextSequence) - 1;

        public long RowCount => Volatile.Read(ref _rowCount);

        public long DirectoryBytes => _directory.ByteSize;

        internal RowHeapOptions Options => _options;

        internal IMemoryAllocator MemoryAllocator => _memoryAllocator;

        internal ImmutablePageClient<RowHeapPage> Client => _client;

        internal RowHeapDirectory Directory => _directory;

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
            var client = syncClient.GetOrCreateImmutablePageClient($"{name}_pages", pageOptions, memoryAllocator);
            var directoryTree = await stateManagerClient.GetOrCreateTree($"{name}_directory", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
            {
                Comparer = new PrimitiveListComparer<long>(),
                KeySerializer = new PrimitiveListKeyContainerSerializer<long>(memoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<long>(memoryAllocator),
                MemoryAllocator = memoryAllocator
            });
            var heap = new RowHeap(name, columnCount, options, memoryAllocator, client, directoryTree, state);
            await heap.RestoreAsync();
            return heap;
        }

        private async Task RestoreAsync()
        {
            var stored = _state.Value;
            if (stored == null)
            {
                _state.Value = new RowHeapState()
                {
                    FormatVersion = FormatVersion,
                    SlotBits = RowReference.SlotBits,
                    ColumnCount = _columnCount,
                    NextSequence = 1
                };
                return;
            }
            _nextSequence = stored.NextSequence;
            _persistedSequence = stored.NextSequence;
            _rowCount = stored.RowCount;

            using var iterator = _directoryTree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                page.EnterWriteLock();
                try
                {
                    foreach (var entry in page)
                    {
                        _directory.Set(entry.Key, entry.Value);
                    }
                }
                finally
                {
                    page.ExitWriteLock();
                }
            }
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
                int pageEnd = NextPageEnd(start, row, end, out int byteSize);
                int rows = pageEnd - row;
                long sequence = _nextSequence;
                RowReference.ThrowIfExhausted(sequence, _name);
                var page = BuildPage(payload, row, rows);
                long pageId = _client.NewPageId();
                try
                {
                    await _client.PublishAsync(pageId, page).ConfigureAwait(false);
                }
                catch
                {
                    // The client did not take the page
                    page.Return();
                    throw;
                }
                // The slot is written before a reference to it exists
                _directory.Set(sequence, pageId);
                Volatile.Write(ref _nextSequence, sequence + 1);
                Volatile.Write(ref _rowCount, _rowCount + rows);
                for (int i = 0; i < rows; i++)
                {
                    references.Add(new RowRefValue(RowReference.Pack(sequence, i)));
                }
                if (byteSize > _options.PageTargetBytes)
                {
                    _oversizedBytes += byteSize;
                    if (_oversizedBytes > (long)_options.PageTargetBytes * _options.WriteQueueCapacity)
                    {
                        // Oversized pages wait to be written only up to a full queue's bytes
                        await _client.DrainAsync().ConfigureAwait(false);
                        _oversizedBytes = 0;
                    }
                }
                row = pageEnd;
            }
        }

        // Fills the byte size of the rows up to and with each row, one pass per column
        private void MeasureRows(EventBatchData payload, int start, int count)
        {
            if (_rows.Length < count)
            {
                int size = Math.Max(count, _rows.Length * 2);
                _rows = new int[size];
                _sizes = new int[size];
            }
            for (int i = 0; i < count; i++)
            {
                _rows[i] = start + i;
            }
            var sizes = _sizes.AsSpan(0, count);
            sizes.Clear();
            ReadOnlySpan<int> rows = _rows.AsSpan(0, count);
            for (int i = 0; i < _columnCount; i++)
            {
                payload.Columns[i].GetPrefixSumByteSizes(rows, sizes);
            }
        }

        private int NextPageEnd(int start, int row, int end, out int byteSize)
        {
            int limit = Math.Min(end, row + RowReference.MaxRowsPerPage);
            int before = row == start ? 0 : _sizes[row - start - 1];
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

        private RowHeapPage BuildPage(EventBatchData payload, int row, int rows)
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
            return new RowHeapPage(new EventBatchData(columns), rows);
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

        // Called in the host's OnCheckpoint, before the storage checkpoint
        public async Task CheckpointAsync()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            // Every page appended so far is written and committed first
            await _client.CommitAsync();
            for (long sequence = _persistedSequence; sequence < _nextSequence; sequence++)
            {
                await _directoryTree.Upsert(sequence, _directory.Get(sequence));
            }
            _persistedSequence = _nextSequence;
            await _directoryTree.Commit();
            _state.Value!.NextSequence = _nextSequence;
            _state.Value.RowCount = _rowCount;
            await _state.Commit();
        }

        // Called when every reader and the store have stopped using the heap
        public void Dispose()
        {
            RowHeapReader[] readers;
            lock (_readers)
            {
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
            if (failure != null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }
    }
}
