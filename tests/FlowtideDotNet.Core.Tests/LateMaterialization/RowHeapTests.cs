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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.AppendTree.Internal;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowHeapTests
    {
        private static readonly string[] EveryKind = new string[] { "int", "double", "bool", "string", "binary", "decimal", "timestamp", "null", "list", "map", "struct", "mixed", "sparse" };

        private static readonly StructHeader StructColumns = StructHeader.Create("number", "text");

        internal sealed class Stream : IDisposable
        {
            public Stream(StateManagerSync<object> manager, ReservoirPersistentStorage storage, MemoryFileProvider provider)
            {
                Manager = manager;
                Storage = storage;
                Provider = provider;
                Client = manager.GetOrCreateClient("store");
            }

            public StateManagerSync<object> Manager { get; }

            public ReservoirPersistentStorage Storage { get; }

            public MemoryFileProvider Provider { get; }

            public IStateManagerClient Client { get; }

            public Task<RowHeap> Heap(int columns, int pageTargetBytes = 2000, int window = 1, double maxStoredRatio = 1.5)
            {
                var options = new RowHeapOptions() { PageTargetBytes = pageTargetBytes, WindowSize = window, MaxStoredRatio = maxStoredRatio };
                return RowHeap.CreateAsync(Client, "heap", columns, options, GlobalMemoryManager.Instance);
            }

            public Task<RowHeap> Heap(int columns, RowHeapOptions options)
            {
                return RowHeap.CreateAsync(Client, "heap", columns, options, GlobalMemoryManager.Instance);
            }

            public void Dispose()
            {
                Manager.Dispose();
            }
        }

        internal static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        internal sealed class Gate
        {
            private int _armed = 1;

            public ManualResetEventSlim Entered { get; } = new ManualResetEventSlim(false);

            public ManualResetEventSlim Release { get; } = new ManualResetEventSlim(false);

            public void Pass()
            {
                if (Interlocked.Exchange(ref _armed, 0) == 1)
                {
                    Entered.Set();
                    Release.Wait(TimeSpan.FromMinutes(2));
                }
            }
        }

        // Counts the rows that page sizing looks at
        private sealed class SizingColumn : Column, IColumn
        {
            public SizingColumn() : base(GlobalMemoryManager.Instance)
            {
            }

            public long RowsVisited { get; private set; }

            int IColumn.GetByteSize(int start, int end)
            {
                RowsVisited += end - start + 1;
                return base.GetByteSize(start, end);
            }

            void IColumn.GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
            {
                RowsVisited += indices.Length;
                base.GetPrefixSumByteSizes(indices, sizes);
            }
        }

        // Says its rows are as large as it is told
        private sealed class HeavyColumn : Column, IColumn
        {
            private readonly int _rowBytes;

            public HeavyColumn(int rowBytes) : base(GlobalMemoryManager.Instance)
            {
                _rowBytes = rowBytes;
            }

            void IColumn.GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    sizes[i] += (i + 1) * _rowBytes;
                }
            }
        }

        private static EventBatchData HeavyBatch(int columns, int rows, int rowBytes)
        {
            var heavy = new IColumn[columns];
            for (int c = 0; c < columns; c++)
            {
                heavy[c] = new HeavyColumn(rowBytes);
                for (int i = 0; i < rows; i++)
                {
                    heavy[c].Add(new Int64Value(c * 10 + i));
                }
            }
            return new EventBatchData(heavy);
        }

        private sealed class SizingProvider : MemoryFileProvider
        {
            private long _bytes;

            public long Bytes => Interlocked.Read(ref _bytes);

            public override Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
            {
                Interlocked.Add(ref _bytes, size);
                return base.WriteDataFileAsync(fileId, crc64, size, isBundle, data, cancellationToken);
            }
        }

        internal sealed class CountingAllocator : IMemoryAllocator
        {
            private long _outstandingBytes;
            private long _allocations;
            private long _bytesFreedByFinalizers;

            // Finds the finalizer thread, a leaked column is freed on it
            private static class FinalizerThread
            {
                public static int Id;
            }

            private sealed class Probe
            {
                ~Probe()
                {
                    Volatile.Write(ref FinalizerThread.Id, Environment.CurrentManagedThreadId);
                }
            }

            public CountingAllocator()
            {
                if (Volatile.Read(ref FinalizerThread.Id) == 0)
                {
                    MakeProbe();
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private static void MakeProbe()
            {
                GC.KeepAlive(new Probe());
            }

            public long OutstandingBytes => Interlocked.Read(ref _outstandingBytes);

            public long Allocations => Interlocked.Read(ref _allocations);

            // Collects first, so memory left to a finalizer is counted
            public long BytesFreedByFinalizers
            {
                get
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    return Interlocked.Read(ref _bytesFreedByFinalizers);
                }
            }

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                Interlocked.Increment(ref _allocations);
                return GlobalMemoryManager.Instance.Allocate(size, alignment);
            }

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                return GlobalMemoryManager.Instance.Realloc(memory, size, alignment);
            }

            public void RegisterAllocationToMetrics(int size)
            {
                Interlocked.Add(ref _outstandingBytes, size);
            }

            public void RegisterFreeToMetrics(int size)
            {
                if (Environment.CurrentManagedThreadId == Volatile.Read(ref FinalizerThread.Id))
                {
                    Interlocked.Add(ref _bytesFreedByFinalizers, size);
                }
                Interlocked.Add(ref _outstandingBytes, -size);
            }
        }

        private sealed class OtherClient : IStateManagerClient
        {
            public ValueTask<IBPlusTree<K, V, TKeyContainer, TValueContainer>> GetOrCreateTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
                where TKeyContainer : IKeyContainer<K>
                where TValueContainer : IValueContainer<V>
            {
                throw new NotSupportedException();
            }

            public ValueTask<IAppendTree<K, V, TKeyContainer, TValueContainer>> GetOrCreateAppendTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
                where TKeyContainer : IKeyContainer<K>
                where TValueContainer : IValueContainer<V>
            {
                throw new NotSupportedException();
            }

            public ValueTask<IFlowtideQueue<V, TValueContainer>> GetOrCreateQueue<V, TValueContainer>(string name, FlowtideQueueOptions<V, TValueContainer> options)
                where TValueContainer : IValueContainer<V>
            {
                throw new NotSupportedException();
            }

            public ValueTask<IObjectState<T>> GetOrCreateObjectStateAsync<T>(string name)
            {
                throw new NotSupportedException();
            }

            public bool StateExists(string name)
            {
                return false;
            }

            public IStateManagerClient GetChildManager(string name)
            {
                return this;
            }
        }

        internal static async Task<Stream> Open(MemoryFileProvider? provider = null, int cachePages = 10000, bool compress = false, int maxFileSize = 64 * 1024 * 1024, string meterName = "rowHeap")
        {
            provider ??= new MemoryFileProvider();
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider, MaxFileSize = maxFileSize });
            var manager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    CachePageCount = cachePages,
                    MinCachePageCount = Math.Min(1000, cachePages),
                    SerializeOptions = new StateSerializeOptions()
                    {
                        CompressionType = compress ? CompressionType.Zstd : CompressionType.None,
                        CompressionMethod = CompressionMethod.Page
                    },
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/rowHeap/{Guid.NewGuid():N}" }
                },
                NullLoggerFactory.Instance,
                new Meter(meterName),
                "rowHeap",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            return new Stream(manager, storage, provider);
        }

        internal static IDataValue Value(string kind, long row)
        {
            switch (kind)
            {
                case "int":
                    return new Int64Value(row * 7 - 3);
                case "double":
                    return new DoubleValue(row * 0.25);
                case "bool":
                    return new BoolValue(row % 3 == 0);
                case "string":
                    return new StringValue("row " + row + new string('x', (int)(row % 40)));
                case "binary":
                    return new BinaryValue(Encoding.UTF8.GetBytes("bytes " + row));
                case "decimal":
                    return new DecimalValue(row * 1.5m);
                case "timestamp":
                    return new TimestampTzValue(1_700_000_000_000_000 + row * 1000, 0);
                case "null":
                    return NullValue.Instance;
                case "list":
                    return new ListValue(new Int64Value(row), new StringValue("item " + row));
                case "map":
                    return new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("key " + row), new Int64Value(row)));
                case "struct":
                    return new StructValue(StructColumns, new Int64Value(row), new StringValue("text " + row));
                case "mixed":
                    return (row % 3) switch
                    {
                        0 => new Int64Value(row),
                        1 => new StringValue("mixed " + row),
                        _ => new DoubleValue(row + 0.5)
                    };
                case "sparse":
                    return row % 4 == 0 ? NullValue.Instance : new StringValue("sparse " + row);
                case "wide":
                    return new StringValue(new string((char)('a' + row % 26), 1000));
                default:
                    throw new InvalidOperationException(kind);
            }
        }

        internal static EventBatchData Batch(string[] kinds, long firstRow, int rows)
        {
            var columns = new IColumn[kinds.Length];
            for (int c = 0; c < kinds.Length; c++)
            {
                var column = Column.Create(GlobalMemoryManager.Instance);
                for (int i = 0; i < rows; i++)
                {
                    column.Add(Value(kinds[c], firstRow + i));
                }
                columns[c] = column;
            }
            return new EventBatchData(columns);
        }

        internal static long PageId(RowHeap heap, long sequence)
        {
            return heap.Client.FirstPageId + RowHeap.PageOf(heap.Directory.Get(sequence));
        }

        internal static async Task<ulong[]> Append(RowHeap heap, string[] kinds, long firstRow, int rows)
        {
            using var batch = Batch(kinds, firstRow, rows);
            using var references = Column.Create(GlobalMemoryManager.Instance);
            await heap.AppendAsync(batch, 0, rows, references);
            var result = new ulong[references.Count];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = (ulong)references.GetValueAt(i, default).AsLong;
            }
            return result;
        }

        internal static Column References(ulong?[] references)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < references.Length; i++)
            {
                if (references[i].HasValue)
                {
                    column.Add(new RowRefValue(references[i]!.Value));
                }
                else
                {
                    column.Add(NullValue.Instance);
                }
            }
            return column;
        }

        // Fetches and puts the rows back in the order they were asked for
        internal static async Task<Column[]> Gather(RowHeapReader reader, ulong?[] references, int[] columns)
        {
            using var referenceColumn = References(references);
            var fetched = new Column[columns.Length];
            for (int i = 0; i < fetched.Length; i++)
            {
                fetched[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            var rowOrder = new int[references.Length];
            await reader.GatherAsync(referenceColumn, references.Length, columns, fetched, rowOrder);
            Assert.Equal(Enumerable.Range(0, references.Length), rowOrder.OrderBy(x => x));
            var position = new int[references.Length];
            for (int i = 0; i < rowOrder.Length; i++)
            {
                position[rowOrder[i]] = i;
            }
            var output = new Column[columns.Length];
            for (int c = 0; c < output.Length; c++)
            {
                output[c] = Column.Create(GlobalMemoryManager.Instance);
                for (int i = 0; i < position.Length; i++)
                {
                    output[c].Add(fetched[c].GetValueAt(position[i], default));
                }
                fetched[c].Dispose();
            }
            return output;
        }

        internal static void AssertRows(string[] kinds, int[] columns, long?[] rows, Column[] fetched)
        {
            for (int c = 0; c < columns.Length; c++)
            {
                Assert.Equal(rows.Length, fetched[c].Count);
                for (int i = 0; i < rows.Length; i++)
                {
                    IDataValue expected = rows[i].HasValue ? Value(kinds[columns[c]], rows[i]!.Value) : NullValue.Instance;
                    var actual = fetched[c].GetValueAt(i, default);
                    Assert.True(DataValueComparer.Instance.Compare(expected, actual) == 0, $"Column {kinds[columns[c]]}, position {i}, row {rows[i]}: expected {expected}, got {actual}.");
                }
                fetched[c].Dispose();
            }
        }

        private static async Task WaitUntil(Func<bool> done)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            while (!done())
            {
                if (watch.Elapsed > Limit)
                {
                    throw new TimeoutException("The awaited state was not reached.");
                }
                await Task.Delay(10);
            }
        }

        private static (ulong?[] references, long?[] rows) Pick(ulong[] all, int count, int seed)
        {
            var random = new Random(seed);
            var references = new ulong?[count];
            var rows = new long?[count];
            for (int i = 0; i < count; i++)
            {
                int choice = random.Next(all.Length + all.Length / 10);
                if (choice < all.Length)
                {
                    references[i] = all[choice];
                    rows[i] = choice;
                }
            }
            return (references, rows);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RowsOfEveryTypeComeBackByTheirReference(bool afterRestart)
        {
            var provider = new MemoryFileProvider();
            var all = new List<ulong>();
            long pages;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(EveryKind.Length);
                for (int batch = 0; batch < 3; batch++)
                {
                    all.AddRange(await Append(heap, EveryKind, batch * 100, 100));
                }
                pages = heap.PageCount;
                var (references, rows) = Pick(all.ToArray(), 500, 123);
                using var reader = heap.CreateReader();
                var columns = new int[] { 3, 0, 8, 9, 10, 11, 12, 7, 1, 2, 4, 5, 6 };
                if (!afterRestart)
                {
                    AssertRows(EveryKind, columns, rows, await Gather(reader, references, columns));
                    Assert.Equal(0, reader.HeldPages);
                }
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }
            Assert.True(pages > 20);
            Assert.Equal(300, all.Distinct().Count());

            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(EveryKind.Length);
                var (references, rows) = Pick(all.ToArray(), 500, 456);
                using var reader = heap.CreateReader();
                var columns = new int[] { 5, 12, 0, 3, 10, 1, 2, 4, 6, 7, 8, 9, 11 };
                AssertRows(EveryKind, columns, rows, await Gather(reader, references, columns));
                long coldLoads = reader.ColdLoads;
                AssertRows(EveryKind, columns, rows, await Gather(reader, references, columns));
                Assert.Equal(pages, heap.PageCount);
                Assert.InRange(coldLoads, 1, pages);
                Assert.Equal(coldLoads, reader.ColdLoads);
            }
        }

        [Fact]
        public async Task APageEndsAtTheByteTargetOrAtTheRowCap()
        {
            using var stream = await Open();
            using var small = await RowHeap.CreateAsync(stream.Client, "small", 1, new RowHeapOptions() { PageTargetBytes = 100_000_000 }, GlobalMemoryManager.Instance);
            using var wide = await RowHeap.CreateAsync(stream.Client, "wide", 1, new RowHeapOptions() { PageTargetBytes = 10_000 }, GlobalMemoryManager.Instance);

            var smallReferences = await Append(small, new string[] { "int" }, 0, 70_000);
            var wideReferences = await Append(wide, new string[] { "wide" }, 0, 100);

            Assert.Equal(2, small.PageCount);
            Assert.Equal(RowReference.Pack(1, 65_535), smallReferences[65_535]);
            Assert.Equal(RowReference.Pack(2, 0), smallReferences[65_536]);
            Assert.Equal(RowReference.Pack(2, 4_463), smallReferences[69_999]);
            Assert.Equal(70_000, small.RowCount);
            var rowsPerPage = wideReferences.GroupBy(RowReference.Sequence).Select(x => x.Count()).ToList();
            Assert.Equal(wide.PageCount, rowsPerPage.Count);
            Assert.All(rowsPerPage.Take(rowsPerPage.Count - 1), rows => Assert.Equal(9, rows));
        }

        [Fact]
        public async Task ARowAboveTheTargetGetsAPageOfItsOwn()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue("before"));
            batch.Columns[0].Add(new StringValue(new string('y', 100_000)));
            batch.Columns[0].Add(new StringValue("after"));
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, 3, references);
            using var reader = heap.CreateReader();
            var fetched = new Column[] { Column.Create(GlobalMemoryManager.Instance) };
            var rowOrder = new int[3];
            await reader.GatherAsync(references, 3, new int[] { 0 }, fetched, rowOrder);

            Assert.Equal(3, heap.PageCount);
            Assert.Equal(new int[] { 0, 1, 2 }, rowOrder);
            Assert.Equal("before", fetched[0].GetValueAt(0, default).AsString.ToString());
            Assert.Equal(100_000, fetched[0].GetValueAt(1, default).AsString.ToString().Length);
            Assert.Equal("after", fetched[0].GetValueAt(2, default).AsString.ToString());
            fetched[0].Dispose();
        }

        [Fact]
        public async Task AReferenceToNoRowFailsWithItsOwnError()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1);
            var references = await Append(heap, new string[] { "int" }, 0, 10);
            using var reader = heap.CreateReader();
            var columns = new int[] { 0 };

            var noPage = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { references[0], RowReference.Pack(999, 0) }, columns));
            var noSlot = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { RowReference.Pack(1, 60_000) }, columns));
            var zero = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { 0 }, columns));
            using var numbers = Column.Create(GlobalMemoryManager.Instance);
            numbers.Add(new Int64Value(5));
            var output = new Column[] { Column.Create(GlobalMemoryManager.Instance) };
            var notAReference = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.GatherAsync(numbers, 1, columns, output, new int[1]));
            output[0].Dispose();

            Assert.Equal("Row reference to page 999 in heap 'heap' points at no page.", noPage.Message);
            Assert.Equal("Row reference to slot 60000 of page 1 in heap 'heap' is beyond the page's 10 rows.", noSlot.Message);
            Assert.Equal("Row heap 'heap' got the row reference 0, which no row has.", zero.Message);
            Assert.Equal("Row heap 'heap' got a value of type Int64 where a row reference was expected.", notAReference.Message);
            Assert.Equal(0, reader.HeldPages);
        }

        private static EventBatchData RandomRows(int rows, int bytes)
        {
            var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            var random = new Random(3);
            for (int i = 0; i < rows; i++)
            {
                var value = new byte[bytes];
                random.NextBytes(value);
                batch.Columns[0].Add(new BinaryValue(value));
            }
            return batch;
        }

        [Fact]
        public async Task AnAppendWaitsForTheWriterWhenOversizedPagesFillAQueuesBytes()
        {
            using var stream = await Open();
            var gate = new Gate();
            var options = new RowHeapOptions() { PageTargetBytes = 1000, WriteQueueCapacity = 16, PageWriteHookForTests = gate.Pass };
            using var heap = await RowHeap.CreateAsync(stream.Client, "heap", 1, options, GlobalMemoryManager.Instance);
            using var batch = RandomRows(5, 20_000);
            using var references = Column.Create(GlobalMemoryManager.Instance);
            try
            {
                var append = heap.AppendAsync(batch, 0, 5, references).AsTask();
                Assert.True(gate.Entered.Wait(Limit));
                await Task.WhenAny(append, Task.Delay(1000));
                long pagesWhileHeld = heap.PageCount;
                bool waited = !append.IsCompleted;
                gate.Release.Set();
                await append.WaitAsync(Limit);

                Assert.True(waited);
                Assert.Equal(1, pagesWhileHeld);
                Assert.Equal(5, heap.PageCount);
                Assert.Equal(5, references.Count);
                Assert.Equal(0, heap.Client.HeldPages);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task OversizedPagesWithinAQueuesBytesDoNotWaitForTheWriter()
        {
            using var stream = await Open();
            var gate = new Gate();
            var options = new RowHeapOptions() { PageTargetBytes = 1000, WriteQueueCapacity = 16, PageWriteHookForTests = gate.Pass };
            using var heap = await RowHeap.CreateAsync(stream.Client, "heap", 1, options, GlobalMemoryManager.Instance);
            using var batch = RandomRows(5, 3_000);
            using var references = Column.Create(GlobalMemoryManager.Instance);
            try
            {
                var append = heap.AppendAsync(batch, 0, 5, references).AsTask();
                Assert.True(gate.Entered.Wait(Limit));
                await append.WaitAsync(Limit);
                int queued = heap.Client.HeldPages;
                gate.Release.Set();
                await heap.Client.DrainAsync().WaitAsync(Limit);

                Assert.Equal(5, heap.PageCount);
                Assert.Equal(5, queued);
            }
            finally
            {
                gate.Release.Set();
            }
        }

        [Fact]
        public async Task ThePageSequenceLimitStopsAnAppend()
        {
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "int" };
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1);
                var state = await stream.Client.GetOrCreateObjectStateAsync<RowHeapState>("heap_state");
                state.Value!.NextSequence = RowReference.MaxSequence + 1;
                await state.Commit();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            using var full = await restarted.Heap(1);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Append(full, kinds, 5, 5));

            Assert.Equal("Row heap 'heap' has used all 281474976710655 page sequences.", error.Message);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EmptyListsAndMapsCountTowardsThePageTarget(bool maps)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            IDataValue empty = maps ? new MapValue(Array.Empty<KeyValuePair<IDataValue, IDataValue>>()) : new ListValue(Array.Empty<IDataValue>());
            for (int i = 0; i < RowReference.MaxRowsPerPage; i++)
            {
                batch.Columns[0].Add(empty);
            }
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, batch.Count, references);
            using var window = heap.Client.CreateWindow();
            bool cached = heap.Client.TryGet(PageId(heap, 1), window, out var page);

            Assert.True(cached);
            Assert.True(heap.PageCount > 1, $"The heap has {heap.PageCount} pages.");
            Assert.True(page!.Data.GetByteSize() <= 32 * 1024 + sizeof(int), $"The first page holds {page.Count} rows in {page.Data.GetByteSize()} bytes.");
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ARowOfManyEmptyListsOrMapsGetsAPageOfItsOwn(bool maps)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 1000);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            IDataValue empty = maps ? new MapValue(Array.Empty<KeyValuePair<IDataValue, IDataValue>>()) : new ListValue(Array.Empty<IDataValue>());
            var children = new IDataValue[1000];
            Array.Fill(children, empty);
            for (int i = 0; i < 3; i++)
            {
                batch.Columns[0].Add(new ListValue(children));
            }
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, batch.Count, references);
            using var window = heap.Client.CreateWindow();
            bool cached = heap.Client.TryGet(PageId(heap, 1), window, out var page);

            Assert.True(cached);
            Assert.True(page!.Data.GetByteSize() > 1000);
            Assert.True(page.Count == 1, $"The first page holds {page.Count} rows in {page.Data.GetByteSize()} bytes.");
            Assert.Equal(3, heap.PageCount);
        }

        [Theory]
        [InlineData(1000)]
        [InlineData(65536)]
        public async Task PageSizingReadsEveryRowOnce(int rows)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            var column = new SizingColumn();
            string text = new string('x', 1000);
            for (int i = 0; i < rows; i++)
            {
                if ((i & 1) == 0)
                {
                    column.Add(new StringValue(text));
                }
                else
                {
                    column.Add(new Int64Value(i));
                }
            }
            using var batch = new EventBatchData(new IColumn[] { column });
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, rows, references);

            Assert.Equal(ArrowTypeId.Union, column.Type);
            Assert.Equal(rows, references.Count);
            Assert.Equal(rows, column.RowsVisited);
        }

        [Fact]
        public async Task AppendGoesOnWithNewSequencesAfterARestart()
        {
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "string", "int" };
            ulong[] first;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                first = await Append(heap, kinds, 0, 200);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            long pagesBefore = again.PageCount;
            long rowsBefore = again.RowCount;
            var second = await Append(again, kinds, 200, 200);
            using var reader = again.CreateReader();
            var all = first.Concat(second).ToArray();
            var (references, rows) = Pick(all, 300, 7);

            Assert.Empty(first.Intersect(second));
            Assert.Equal(200, rowsBefore);
            Assert.Equal(400, again.RowCount);
            Assert.Equal(RowReference.Sequence(first[199]) + 1, RowReference.Sequence(second[0]));
            Assert.True(again.PageCount > pagesBefore);
            AssertRows(kinds, new int[] { 0, 1 }, rows, await Gather(reader, references, new int[] { 0, 1 }));
        }

        [Fact]
        public async Task TwoCheckpointsAndARestartKeepEveryPage()
        {
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "string", "list" };
            ulong[] first;
            ulong[] second;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                first = await Append(heap, kinds, 0, 150);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                second = await Append(heap, kinds, 150, 150);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            using var reader = again.CreateReader();
            var all = first.Concat(second).ToArray();
            var fetched = await Gather(reader, all.Select(x => (ulong?)x).ToArray(), new int[] { 1, 0 });

            Assert.Equal(RowReference.Sequence(second[149]), again.PageCount);
            Assert.Equal(300, again.RowCount);
            Assert.Equal(again.PageCount, reader.ColdLoads);
            AssertRows(kinds, new int[] { 1, 0 }, Enumerable.Range(0, 300).Select(x => (long?)x).ToArray(), fetched);
        }

        [Fact]
        public async Task ARestoreForgetsThePagesAppendedAfterTheCheckpoint()
        {
            using var stream = await Open();
            var kinds = new string[] { "string" };
            var heap = await stream.Heap(1);
            var kept = await Append(heap, kinds, 0, 100);
            await heap.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
            var lost = await Append(heap, kinds, 100, 100);
            long lostPage = PageId(heap, RowReference.Sequence(lost[0]));
            await heap.Client.DrainAsync();
            bool lostWritten = heap.Client.IsStored(lostPage);
            heap.Dispose();

            await stream.Manager.InitializeAsync();
            using var restored = await stream.Heap(1);
            using var reader = restored.CreateReader();
            var keptRows = await Gather(reader, kept.Select(x => (ulong?)x).ToArray(), new int[] { 0 });
            var missing = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { lost[99] }, new int[] { 0 }));
            var reused = await Append(restored, kinds, 500, 100);
            long reusedPage = PageId(restored, RowReference.Sequence(reused[0]));
            await restored.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
            for (long sequence = 1; sequence <= restored.SequenceCount; sequence++)
            {
                stream.Manager.DeleteFromCache(PageId(restored, sequence));
            }
            long coldBefore = reader.ColdLoads;
            var reusedRows = await Gather(reader, reused.Select(x => (ulong?)x).ToArray(), new int[] { 0 });

            AssertRows(kinds, new int[] { 0 }, Enumerable.Range(0, 100).Select(x => (long?)x).ToArray(), keptRows);
            Assert.EndsWith("points at no page.", missing.Message);
            Assert.True(lostWritten);
            Assert.Equal(lost, reused);
            Assert.Equal(lostPage, reusedPage);
            Assert.True(reader.ColdLoads > coldBefore);
            AssertRows(kinds, new int[] { 0 }, Enumerable.Range(500, 100).Select(x => (long?)x).ToArray(), reusedRows);
        }

        [Fact]
        public async Task AHeapStoredWithOtherColumnsFailsToOpen()
        {
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                await Append(heap, new string[] { "int", "string" }, 0, 10);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => restarted.Heap(3));

            Assert.Equal("Row heap 'heap' is stored with format 2, 16 slot bits and 2 columns, the stream expects format 2, 16 slot bits and 3 columns.", error.Message);
        }

        [Fact]
        public async Task AFailedOpenDoesNotLeakCacheReservationOrPoisonRetry()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, new string[] { "int", "string" }, 0, 10);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            Assert.Equal(0, restarted.Manager.CacheTable.ReservedHeldPages);
            await Assert.ThrowsAsync<InvalidOperationException>(() => restarted.Heap(3));
            Assert.Equal(0, restarted.Manager.CacheTable.ReservedHeldPages);

            using var recovered = await restarted.Heap(2);
            using var reader = recovered.CreateReader();
            AssertRows(new string[] { "int", "string" }, new int[] { 0, 1 }, new long?[] { 0 }, await Gather(reader, new ulong?[] { references[0] }, new int[] { 0, 1 }));
        }

        [Fact]
        public async Task AHeapNeedsReservoirStorage()
        {
            using var manager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"./data/rowHeap/{Guid.NewGuid():N}" }),
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/rowHeap/{Guid.NewGuid():N}" }
                },
                NullLoggerFactory.Instance,
                new Meter("rowHeap"),
                "rowHeap",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();

            var error = await Assert.ThrowsAsync<NotSupportedException>(() => RowHeap.CreateAsync(manager.GetOrCreateClient("store"), "heap", 1, new RowHeapOptions(), GlobalMemoryManager.Instance));

            Assert.Equal("Immutable pages need Reservoir storage.", error.Message);
        }

        [Fact]
        public async Task AppendRefusesABatchWithOtherColumns()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            using var batch = Batch(new string[] { "int" }, 0, 5);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            var error = await Assert.ThrowsAsync<ArgumentException>(async () => await heap.AppendAsync(batch, 0, 5, references));

            Assert.StartsWith("Row heap 'heap' stores 2 columns, got 1.", error.Message);
            Assert.Equal(0, heap.PageCount);
        }

        [Fact]
        public async Task AppendRefusesRowsOutsideTheBatchAndADisposedHeap()
        {
            using var stream = await Open();
            var heap = await stream.Heap(1);
            using var batch = Batch(new string[] { "int" }, 0, 5);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            var outside = new (int start, int count)[] { (3, 3), (-1, 2), (0, -1), (6, 0), (int.MaxValue, 1), (1, int.MaxValue) };
            foreach (var range in outside)
            {
                var refused = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await heap.AppendAsync(batch, range.start, range.count, references));
                Assert.Equal($"{range.count} rows from row {range.start} are not inside a batch of 5 rows. (Parameter 'count')", refused.Message);
            }
            await heap.AppendAsync(batch, 5, 0, references);
            long pages = heap.PageCount;
            heap.Dispose();
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await heap.AppendAsync(batch, 0, 5, references));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => heap.CheckpointAsync());

            Assert.Equal(0, pages);
            Assert.Equal(0, heap.PageCount);
            Assert.Equal(0, references.Count);
        }

        [Fact]
        public async Task ARangeOfABatchIsAppended()
        {
            using var stream = await Open();
            var kinds = new string[] { "string", "sparse" };
            using var heap = await stream.Heap(2);
            using var batch = Batch(kinds, 0, 100);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 40, 20, references);
            using var reader = heap.CreateReader();
            var output = new Column[] { Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance) };
            await reader.GatherAsync(references, 20, new int[] { 0, 1 }, output, new int[20]);

            Assert.Equal(20, references.Count);
            Assert.Equal(20, heap.RowCount);
            AssertRows(kinds, new int[] { 0, 1 }, Enumerable.Range(40, 20).Select(x => (long?)x).ToArray(), output);
        }

        [Fact]
        public async Task TheDirectoryCountsItsBytes()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 1);
            long empty = heap.DirectoryBytes;

            await Append(heap, new string[] { "int" }, 0, 5000);

            Assert.Equal(16 * 12, empty);
            Assert.Equal(5000, heap.PageCount);
            Assert.Equal(5000, heap.SequenceCount);
            Assert.Equal(2 * RowHeapDirectory.ChunkSize * 8 + 16 * 12, heap.DirectoryBytes);
            Assert.Equal(2 * (RowHeapPageTable.ChunkSize * RowHeapPageTable.EntrySize + RowHeapPageTable.ChunkOverhead), heap.PageTableBytes);
        }

        [Fact]
        public void TheDirectoryKeepsEverySlotWhileItGrows()
        {
            var directory = new RowHeapDirectory();

            for (long sequence = 1; sequence <= 200_000; sequence += 37)
            {
                directory.Set(sequence, sequence + 1000);
            }

            for (long sequence = 1; sequence <= 200_000; sequence++)
            {
                Assert.Equal((sequence - 1) % 37 == 0 ? sequence + 1000 : 0, directory.Get(sequence));
            }
            Assert.Equal(0, directory.Get(0));
            Assert.Equal(0, directory.Get(-5));
            Assert.Equal(0, directory.Get(10_000_000_000));
        }

        [Fact]
        public void AnOlderChunkWithFewSequencesLeftIsKeptAsAShortList()
        {
            var directory = new RowHeapDirectory();
            const int chunk = RowHeapDirectory.ChunkSize;
            const long table = 16 * 12;
            for (long sequence = 1; sequence < 3 * chunk; sequence++)
            {
                directory.Set(sequence, sequence + 1000);
            }
            long full = directory.ByteSize;

            for (long sequence = 66; sequence < chunk; sequence++)
            {
                directory.Set(sequence, 0);
            }
            long with65 = directory.ByteSize;
            directory.Set(65, 0);
            long with64 = directory.ByteSize;
            directory.Set(64, 77);
            directory.Set(63, 0);
            directory.Set(63, 0);
            long with63 = directory.ByteSize;
            var kept = new long[] { directory.Get(1), directory.Get(62), directory.Get(63), directory.Get(64), directory.Get(65), directory.Get(chunk - 1), directory.Get(chunk) };
            directory.Set(200, 9);
            long with64Again = directory.ByteSize;
            long added = directory.Get(200);
            for (long sequence = 1; sequence < chunk; sequence++)
            {
                directory.Set(sequence, 0);
            }
            long without = directory.ByteSize;
            for (long sequence = 2 * chunk + 1; sequence < 3 * chunk; sequence++)
            {
                directory.Set(sequence, 0);
            }
            long newestWithOne = directory.ByteSize;
            directory.Set(2 * chunk, 0);

            Assert.Equal(table + 3 * chunk * 8, full);
            Assert.Equal(table + 3 * chunk * 8, with65);
            Assert.Equal(table + 2 * chunk * 8 + 64 * 10 + 64, with64);
            Assert.Equal(table + 2 * chunk * 8 + 64 * 10 + 64, with63);
            Assert.Equal(new long[] { 1001, 1062, 0, 77, 0, 0, chunk + 1000 }, kept);
            Assert.Equal(table + 2 * chunk * 8 + 65 * 10 + 64, with64Again);
            Assert.Equal(9, added);
            Assert.Equal(table + 2 * chunk * 8, without);
            Assert.Equal(table + 2 * chunk * 8, newestWithOne);
            Assert.Equal(table + chunk * 8, directory.ByteSize);
            Assert.Equal(0, directory.Get(2 * chunk));
            Assert.Equal(chunk + 1005, directory.Get(chunk + 5));
        }

        [Fact]
        public void AnOlderChunkOfThePageTableWithFewPagesLeftIsMoved()
        {
            var pages = new RowHeapPageTable();
            const int chunk = RowHeapPageTable.ChunkSize;
            const long chunkBytes = chunk * RowHeapPageTable.EntrySize + RowHeapPageTable.ChunkOverhead;
            const long moved = RowHeapPageTable.MovedEntrySize;
            long empty = pages.ByteSize;
            for (long page = 0; page < 3 * chunk; page++)
            {
                pages.GetOrAdd(page).Rows = (int)page + 1;
            }
            long full = pages.ByteSize;

            for (long page = 65; page < chunk; page++)
            {
                pages.Remove(page);
            }
            long with65 = pages.ByteSize;
            pages.Remove(64);
            pages.Remove(64);
            long with64 = pages.ByteSize;
            pages.GetOrAdd(10).Live = 7;
            pages.GetOrAdd(500).Rows = 9;
            long with65Again = pages.ByteSize;
            var kept = new int[] { pages.Rows(0), pages.Rows(63), pages.Rows(64), pages.Rows(500), pages.Live(10), pages.Rows(chunk) };
            var states = new RowHeapPageState[] { pages.State(63), pages.State(64), pages.State(500), pages.State(chunk), pages.State(-1), pages.State(100L * chunk) };
            var unknown = Assert.Throws<InvalidOperationException>(() => pages.Get(64));
            int known = pages.Get(63).Rows;
            for (long page = 0; page < chunk; page++)
            {
                pages.Remove(page);
            }
            long without = pages.ByteSize;
            for (long page = 2 * chunk + 1; page < 3 * chunk; page++)
            {
                pages.Remove(page);
            }
            long newestWithOne = pages.ByteSize;

            Assert.Equal(0, empty);
            Assert.Equal(3 * chunkBytes, full);
            Assert.Equal(3 * chunkBytes, with65);
            Assert.Equal(2 * chunkBytes + 64 * moved, with64);
            Assert.Equal(2 * chunkBytes + 65 * moved, with65Again);
            Assert.Equal(new int[] { 1, 64, 0, 9, 7, chunk + 1 }, kept);
            Assert.Equal("The row heap knows no page 64.", unknown.Message);
            Assert.Equal(64, known);
            Assert.Equal(RowHeapPageState.None, pages.State(64));
            Assert.Equal(new RowHeapPageState[] { RowHeapPageState.Stored, RowHeapPageState.None, RowHeapPageState.Stored, RowHeapPageState.Stored, RowHeapPageState.None, RowHeapPageState.None }, states);
            Assert.Equal(2 * chunkBytes, without);
            Assert.Equal(2 * chunkBytes, newestWithOne);
            Assert.Equal(chunk + 1, pages.Count);
        }

        [Fact]
        public void APageTableThatIsLoadedMovesNoChunkUntilItIsDone()
        {
            var pages = new RowHeapPageTable();
            const int chunk = RowHeapPageTable.ChunkSize;
            pages.Loading = true;
            pages.GetOrAdd(3).Rows = 1;
            pages.GetOrAdd(2 * chunk).Rows = 2;
            pages.GetOrAdd(4).Rows = 3;
            pages.GetOrAdd(chunk + 1).Rows = 4;
            long loading = pages.ByteSize;

            pages.Loading = false;

            Assert.Equal(3 * (chunk * RowHeapPageTable.EntrySize + RowHeapPageTable.ChunkOverhead), loading);
            Assert.Equal(chunk * RowHeapPageTable.EntrySize + RowHeapPageTable.ChunkOverhead + 3 * RowHeapPageTable.MovedEntrySize, pages.ByteSize);
            Assert.Equal(new int[] { 1, 3, 4, 2 }, new int[] { pages.Rows(3), pages.Rows(4), pages.Rows(chunk + 1), pages.Rows(2 * chunk) });
            Assert.Equal(4, pages.Count);
        }

        [Fact]
        public void TheDirectoryRefusesASequenceItCannotHold()
        {
            var directory = new RowHeapDirectory();

            var zero = Assert.Throws<InvalidOperationException>(() => directory.Set(0, 5));
            var huge = Assert.Throws<InvalidOperationException>(() => directory.Set(RowReference.MaxSequence, 5));

            Assert.Equal("The row heap directory has no slot for page sequence 0.", zero.Message);
            Assert.Equal("The row heap directory has no slot for page sequence 281474976710655.", huge.Message);
            Assert.Equal(16 * 12, directory.ByteSize);
        }

        [Fact]
        public void AReferenceIsAPageSequenceAndASlot()
        {
            ulong reference = RowReference.Pack(RowReference.MaxSequence, 65_535);

            Assert.Equal(ulong.MaxValue, reference);
            Assert.Equal(RowReference.MaxSequence, RowReference.Sequence(reference));
            Assert.Equal(65_535, RowReference.Slot(reference));
            Assert.Equal((1UL << 16) + 7, RowReference.Pack(1, 7));
            Assert.Equal(281_474_976_710_655, RowReference.MaxSequence);
        }

        [Fact]
        public async Task PagesAreCompressedWhenTheStreamCompressesItsPages()
        {
            var kinds = new string[] { "wide", "int" };
            var sizes = new long[2];
            ulong[] references = Array.Empty<ulong>();
            var compressed = new SizingProvider();
            for (int run = 0; run < 2; run++)
            {
                var provider = run == 0 ? new SizingProvider() : compressed;
                using var stream = await Open(provider, compress: run == 1);
                using var heap = await stream.Heap(2, pageTargetBytes: 32 * 1024);
                references = await Append(heap, kinds, 0, 2000);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                sizes[run] = provider.Bytes;
            }

            using var restarted = await Open(compressed, compress: true);
            using var again = await restarted.Heap(2, pageTargetBytes: 32 * 1024);
            using var reader = again.CreateReader();
            var (picked, rows) = Pick(references, 300, 11);

            Assert.True(sizes[0] > 2_000_000, $"Stored {sizes[0]} bytes without compression.");
            Assert.True(sizes[1] * 5 < sizes[0], $"Stored {sizes[1]} bytes with compression and {sizes[0]} without.");
            AssertRows(kinds, new int[] { 0, 1 }, rows, await Gather(reader, picked, new int[] { 0, 1 }));
        }

        [Fact]
        public async Task ACrashForgetsWhatNoStorageCheckpointCovered()
        {
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "string" };
            ulong[] kept;
            ulong[] lost;
            long lostPage;
            using (var stream = await Open(provider))
            {
                var heap = await stream.Heap(1);
                kept = await Append(heap, kinds, 0, 100);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                lost = await Append(heap, kinds, 100, 100);
                lostPage = PageId(heap, RowReference.Sequence(lost[0]));
                await heap.CheckpointAsync();
            }

            long pages;
            long reusedPage;
            ulong[] reused;
            Column[] keptRows;
            InvalidOperationException missing;
            using (var restarted = await Open(provider))
            {
                using var again = await restarted.Heap(1);
                using var reader = again.CreateReader();
                pages = again.PageCount;
                keptRows = await Gather(reader, kept.Select(x => (ulong?)x).ToArray(), new int[] { 0 });
                missing = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { lost[0] }, new int[] { 0 }));
                reused = await Append(again, kinds, 300, 100);
                reusedPage = PageId(again, RowReference.Sequence(reused[0]));
                await again.CheckpointAsync();
                await restarted.Manager.CheckpointAsync();
            }

            using var third = await Open(provider);
            using var last = await third.Heap(1);
            using var lastReader = last.CreateReader();
            var reusedRows = await Gather(lastReader, reused.Select(x => (ulong?)x).ToArray(), new int[] { 0 });

            Assert.Equal(RowReference.Sequence(kept[99]), pages);
            AssertRows(kinds, new int[] { 0 }, Enumerable.Range(0, 100).Select(x => (long?)x).ToArray(), keptRows);
            Assert.EndsWith("points at no page.", missing.Message);
            Assert.Equal(lost, reused);
            Assert.Equal(lostPage, reusedPage);
            Assert.True(lastReader.ColdLoads > 0);
            AssertRows(kinds, new int[] { 0 }, Enumerable.Range(300, 100).Select(x => (long?)x).ToArray(), reusedRows);
        }

        [Fact]
        public async Task OnlyNullReferencesGiveOnlyNulls()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            await Append(heap, new string[] { "int", "string" }, 0, 10);
            using var reader = heap.CreateReader();

            var fetched = await Gather(reader, new ulong?[] { null, null, null }, new int[] { 1, 0 });

            AssertRows(new string[] { "int", "string" }, new int[] { 1, 0 }, new long?[] { null, null, null }, fetched);
        }

        [Fact]
        public async Task RowsAreAddedAfterWhatTheOutputAlreadyHolds()
        {
            using var stream = await Open();
            var kinds = new string[] { "string", "sparse" };
            using var heap = await stream.Heap(2);
            var references = await Append(heap, kinds, 0, 50);
            using var reader = heap.CreateReader();
            using var first = References(new ulong?[] { references[40], null, references[3] });
            using var second = References(new ulong?[] { references[7], references[41] });
            var output = new Column[] { Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance) };
            var firstOrder = new int[3];
            var secondOrder = new int[2];

            await reader.GatherAsync(first, 3, new int[] { 0, 0 }, output, firstOrder);
            await reader.GatherAsync(second, 2, new int[] { 0, 0 }, output, secondOrder);

            var firstRows = new long?[] { 40, null, 3 };
            var secondRows = new long?[] { 7, 41 };
            var expected = firstOrder.Select(x => firstRows[x]).Concat(secondOrder.Select(x => secondRows[x])).ToArray();
            AssertRows(kinds, new int[] { 0, 0 }, expected, output);
        }

        [Fact]
        public async Task AColumnMayHoldAnotherTypeOnEveryPage()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 100_000);
            var kinds = new string[] { "int", "string", "null", "list", "double", "mixed", "struct" };
            var references = new List<ulong>();
            var expected = new List<IDataValue>();
            for (int k = 0; k < kinds.Length; k++)
            {
                references.AddRange(await Append(heap, new string[] { kinds[k] }, k * 10, 10));
                for (int i = 0; i < 10; i++)
                {
                    expected.Add(Value(kinds[k], k * 10 + i));
                }
            }
            using var reader = heap.CreateReader();
            var random = new Random(5);
            var picks = Enumerable.Range(0, 200).Select(x => random.Next(references.Count)).ToArray();

            var fetched = await Gather(reader, picks.Select(x => (ulong?)references[x]).ToArray(), new int[] { 0 });

            Assert.Equal(kinds.Length, heap.PageCount);
            for (int i = 0; i < picks.Length; i++)
            {
                Assert.True(DataValueComparer.Instance.Compare(expected[picks[i]], fetched[0].GetValueAt(i, default)) == 0, $"Position {i}, row {picks[i]}.");
            }
            fetched[0].Dispose();
        }

        [Fact]
        public async Task EveryPageIsFreedWhenTheStreamIsGone()
        {
            var allocator = new CountingAllocator();
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "string", "list", "sparse" };
            long whileOpen;
            long pages;
            int cached;
            ulong[] references;
            using (var stream = await Open(provider, cachePages: 500, compress: true))
            {
                await stream.Manager.CacheTable.StopCleanupTask();
                using var heap = await RowHeap.CreateAsync(stream.Client, "heap", 3, new RowHeapOptions() { PageTargetBytes = 100 }, allocator);
                references = await Append(heap, kinds, 0, 3000);
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                pages = heap.PageCount;
                await stream.Manager.CacheTable.ForceCleanup();
                cached = stream.Manager.CacheTable.Count;
                whileOpen = allocator.OutstandingBytes;
            }
            await WaitUntil(() => allocator.OutstandingBytes == 0);
            long afterWriting = allocator.OutstandingBytes;

            using (var stream = await Open(provider, compress: true))
            {
                using var heap = await RowHeap.CreateAsync(stream.Client, "heap", 3, new RowHeapOptions() { PageTargetBytes = 100 }, allocator);
                using var reader = heap.CreateReader();
                using var column = References(references.Select(x => (ulong?)x).ToArray());
                var output = new Column[] { Column.Create(GlobalMemoryManager.Instance) };
                await reader.GatherAsync(column, references.Length, new int[] { 1 }, output, new int[references.Length]);
                output[0].Dispose();
            }
            await WaitUntil(() => allocator.OutstandingBytes == 0);

            Assert.True(pages > 1500, $"The heap has {pages} pages.");
            Assert.True(cached < pages / 2, $"{cached} of {pages} pages are still cached.");
            Assert.True(whileOpen > 50_000, $"{whileOpen} bytes were held while the heap was open.");
            Assert.Equal(0, afterWriting);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        [Fact]
        public async Task APageThatCannotBePublishedIsFreed()
        {
            var allocator = new CountingAllocator();
            var stream = await Open();
            var heap = await RowHeap.CreateAsync(stream.Client, "heap", 1, new RowHeapOptions(), allocator);
            await Append(heap, new string[] { "string" }, 0, 10);
            stream.Dispose();
            await WaitUntil(() => allocator.OutstandingBytes == 0);
            using var batch = Batch(new string[] { "string" }, 0, 50);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await heap.AppendAsync(batch, 0, 50, references));

            Assert.Equal(0, allocator.OutstandingBytes);
            Assert.Equal(0, references.Count);
        }

        [Fact]
        public async Task AReaderReservesItsWindowUntilItOrItsHeapIsDisposed()
        {
            using var stream = await Open(cachePages: 500);
            var table = stream.Manager.CacheTable;
            var heap = await stream.Heap(1, window: 4);
            int heapOnly = table.ReservedHeldPages;
            var first = heap.CreateReader();
            int withReader = table.ReservedHeldPages;
            var refused = Assert.Throws<InvalidOperationException>(() => heap.CreateReader());
            first.Dispose();
            first.Dispose();
            int afterOne = table.ReservedHeldPages;
            var second = heap.CreateReader();
            heap.Dispose();
            heap.Dispose();
            int afterHeap = table.ReservedHeldPages;
            second.Dispose();
            Assert.Throws<ObjectDisposedException>(() => heap.CreateReader());

            Assert.Equal(25, table.HeldPagesBound);
            Assert.Equal(20, heapOnly);
            Assert.Equal(24, withReader);
            Assert.Equal("Row heap 'heap' cannot take another reader, the cache has no room for a window of 4 pages.", refused.Message);
            Assert.Equal(20, afterOne);
            Assert.Equal(16, afterHeap);
            Assert.Equal(16, table.ReservedHeldPages);
        }

        [Fact]
        public async Task GatherChecksWhatItIsGiven()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, new string[] { "int", "string" }, 0, 10);
            using var reader = heap.CreateReader();
            using var column = References(new ulong?[] { references[0], references[1] });
            var output = new Column[] { Column.Create(GlobalMemoryManager.Instance) };

            using var nulls = References(new ulong?[] { null, null });
            var columns = await Assert.ThrowsAsync<ArgumentException>(async () => await reader.GatherAsync(column, 2, new int[] { 0, 1 }, output, new int[2]));
            var order = await Assert.ThrowsAsync<ArgumentException>(async () => await reader.GatherAsync(column, 2, new int[] { 0 }, output, new int[1]));
            var tooMany = await Assert.ThrowsAsync<ArgumentException>(async () => await reader.GatherAsync(column, 3, new int[] { 0 }, output, new int[3]));
            var noColumn = await Assert.ThrowsAsync<ArgumentException>(async () => await reader.GatherAsync(nulls, 2, new int[] { 2 }, output, new int[2]));
            await reader.GatherAsync(column, 0, new int[] { 0 }, output, new int[0]);

            Assert.StartsWith("Got 1 output columns for 2 asked columns.", columns.Message);
            Assert.StartsWith("The row order has room for 1 rows, 2 are asked for.", order.Message);
            Assert.StartsWith("3 references are asked for, the column holds 2.", tooMany.Message);
            Assert.StartsWith("Column 2 is asked for, row heap 'heap' stores 2 columns.", noColumn.Message);
            Assert.Equal(0, output[0].Count);
            reader.Dispose();
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await reader.GatherAsync(column, 2, new int[] { 0 }, output, new int[2]));
            output[0].Dispose();
        }

        [Fact]
        public async Task AHeapRefusesAClientThatIsNotTheStreamsOwn()
        {
            var error = await Assert.ThrowsAsync<NotSupportedException>(() => RowHeap.CreateAsync(new OtherClient(), "heap", 1, new RowHeapOptions(), GlobalMemoryManager.Instance));

            Assert.Equal("Row heap 'heap' needs the stream's own state manager client.", error.Message);
        }

        [Fact]
        public async Task AHeapChecksItsSettings()
        {
            using var stream = await Open();

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RowHeap.CreateAsync(stream.Client, "none", 0, new RowHeapOptions(), GlobalMemoryManager.Instance));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RowHeap.CreateAsync(stream.Client, "none", 1, new RowHeapOptions() { PageTargetBytes = 0 }, GlobalMemoryManager.Instance));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RowHeap.CreateAsync(stream.Client, "none", 1, new RowHeapOptions() { CompactPagesPerCall = 0 }, GlobalMemoryManager.Instance));
            using var opened = await RowHeap.CreateAsync(stream.Client, "some", 1, new RowHeapOptions(), GlobalMemoryManager.Instance);

            Assert.False(stream.Client.StateExists("none_state"));
            Assert.False(stream.Client.StateExists("none_pages"));
            Assert.False(stream.Client.StateExists("none_index"));
            Assert.True(stream.Client.StateExists("some_state"));
            Assert.True(stream.Client.StateExists("some_pages"));
            Assert.True(stream.Client.StateExists("some_index"));
        }

        [Fact]
        public async Task AColumnWhoseRowsPassTwoGigabytesIsCountedOnPastTheWrap()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            using var batch = HeavyBatch(1, 6, 600_000_000);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, 6, references);

            Assert.Equal(6, heap.SequenceCount);
            Assert.Equal(6, heap.PageCount);
            Assert.Equal(3_600_000_000, heap.StoredBytes);
        }

        [Fact]
        public async Task SmallPagesAreMergedIntoOneAtATargetOfNearlyTwoGigabytes()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 1_800_000_000);
            using var batch = HeavyBatch(1, 1, 600_000_000);
            for (int page = 0; page < 4; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 1, references);
            }

            await heap.CompactAsync();

            Assert.Equal(4, heap.PagesMerged);
            Assert.Equal(5, heap.PageCount);
        }

        [Fact]
        public async Task RowsWhoseColumnsAddUpPastTwoGigabytesAreCountedInFull()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, pageTargetBytes: 32 * 1024);
            using var batch = HeavyBatch(2, 3, 600_000_000);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, 3, references);
            using var reader = heap.CreateReader();
            var asked = Enumerable.Range(0, 3).Select(x => (ulong?)(ulong)references.GetValueAt(x, default).AsLong).ToArray();
            var fetched = await Gather(reader, asked, new int[] { 0, 1 });
            var values = new long[] { fetched[0].GetValueAt(2, default).AsLong, fetched[1].GetValueAt(0, default).AsLong };
            fetched[0].Dispose();
            fetched[1].Dispose();

            Assert.Equal(3, heap.SequenceCount);
            Assert.Equal(3, heap.PageCount);
            Assert.Equal(3_600_000_000, heap.StoredBytes);
            Assert.Equal(3_600_000_000, heap.LiveBytes);
            Assert.Equal(new long[] { 2, 10 }, values);
        }

        [Fact]
        public async Task ARowThatIsMoreThanAPageCanHoldIsRefused()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(4, pageTargetBytes: 32 * 1024);
            using var batch = HeavyBatch(4, 2, 600_000_000);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await heap.AppendAsync(batch, 0, 2, references));

            Assert.Equal("Row 0 of the batch is 2400000000 bytes, more than a page of row heap 'heap' can hold.", refused.Message);
            Assert.Equal(0, heap.RowCount);
            Assert.Equal(0, heap.PageCount);
            Assert.Equal(0, references.Count);
        }

        [Fact]
        public void TheLastPageSequenceIsCheckedBeforeItIsUsed()
        {
            RowReference.ThrowIfExhausted(RowReference.MaxSequence, "heap");
            var error = Assert.Throws<InvalidOperationException>(() => RowReference.ThrowIfExhausted(RowReference.MaxSequence + 1, "heap"));

            Assert.Equal("Row heap 'heap' has used all 281474976710655 page sequences.", error.Message);
        }

        [Fact]
        public void APageOfAnotherFormatOrShapeIsRefused()
        {
            using var batch = Batch(new string[] { "int", "string" }, 0, 5);
            var written = new ArrayBufferWriter<byte>();
            using var twoColumns = new RowHeapPageSerializer(2, GlobalMemoryManager.Instance);
            using var threeColumns = new RowHeapPageSerializer(3, GlobalMemoryManager.Instance);
            var page = new RowHeapPage(new EventBatchData(new IColumn[] { batch.Columns[0].Copy(GlobalMemoryManager.Instance), batch.Columns[1].Copy(GlobalMemoryManager.Instance) }), 5, 7);
            twoColumns.Serialize(written, page);
            page.Return();
            var bytes = written.WrittenSpan.ToArray();
            var otherFormat = (byte[])bytes.Clone();
            otherFormat[0] = 9;

            var read = twoColumns.Deserialize(new ReadOnlySequence<byte>(bytes), bytes.Length);
            int rows = read.Count;
            long sequence = read.Sequence;
            var map = read.Map;
            read.Return();
            var format = Assert.Throws<InvalidOperationException>(() => twoColumns.Deserialize(new ReadOnlySequence<byte>(otherFormat), otherFormat.Length));
            var shape = Assert.Throws<InvalidOperationException>(() => threeColumns.Deserialize(new ReadOnlySequence<byte>(bytes), bytes.Length));
            var cut = Assert.Throws<InvalidOperationException>(() => twoColumns.Deserialize(new ReadOnlySequence<byte>(bytes, 0, 3), 3));
            var cutSequence = Assert.Throws<InvalidOperationException>(() => twoColumns.Deserialize(new ReadOnlySequence<byte>(bytes, 0, 12), 12));

            Assert.Equal(5, rows);
            Assert.Equal(7, sequence);
            Assert.Null(map);
            Assert.Equal("Row heap page format 9 is not supported, expected 2.", format.Message);
            Assert.Equal("A row heap page holds 5 rows in 2 columns, expected 5 rows in 3 columns.", shape.Message);
            Assert.Equal("A row heap page is shorter than its header.", cut.Message);
            Assert.Equal("A row heap page is shorter than its header.", cutSequence.Message);
        }

        [Fact]
        public void AMergedPageIsStoredWithItsSlotMap()
        {
            using var batch = Batch(new string[] { "int" }, 0, 5);
            var written = new ArrayBufferWriter<byte>();
            using var serializer = new RowHeapPageSerializer(1, GlobalMemoryManager.Instance);
            var slotMap = new RowHeapSlotMap(new long[] { 3, 9 }, new int[] { 2, 0 }, new int[] { 3, 2 }, new ushort[]?[] { new ushort[] { 1, 40, 65535 }, null });
            var page = new RowHeapPage(new EventBatchData(new IColumn[] { batch.Columns[0].Copy(GlobalMemoryManager.Instance) }), 5, slotMap);
            serializer.Serialize(written, page);
            page.Return();
            var bytes = written.WrittenSpan.ToArray();

            var read = serializer.Deserialize(new ReadOnlySequence<byte>(bytes), bytes.Length);
            var map = read.Map!;
            int rows = read.Count;
            long sequence = read.Sequence;
            read.Return();
            var cut = Assert.Throws<InvalidOperationException>(() => serializer.Deserialize(new ReadOnlySequence<byte>(bytes, 0, 30), 30));
            var cutSlots = Assert.Throws<InvalidOperationException>(() => serializer.Deserialize(new ReadOnlySequence<byte>(bytes, 0, 28), 28));

            Assert.Equal(5, rows);
            Assert.Equal(0, sequence);
            Assert.Equal(2, map.Count);
            Assert.Equal(0, map.Find(3));
            Assert.Equal(1, map.Find(9));
            Assert.True(map.Find(4) < 0);
            Assert.Equal(new int[] { 2, 3, 4, -1, -1 }, new int[] { map.Row(0, 1), map.Row(0, 40), map.Row(0, 65535), map.Row(0, 0), map.Row(0, 41) });
            Assert.Equal(new int[] { 0, 1, -1 }, new int[] { map.Row(1, 0), map.Row(1, 1), map.Row(1, 2) });
            Assert.Equal(new int[] { 1, 40, 65535, 0, 1 }, new int[] { map.SlotAt(0, 0), map.SlotAt(0, 1), map.SlotAt(0, 2), map.SlotAt(1, 0), map.SlotAt(1, 1) });
            Assert.Equal(2 * 24 + 3 * 2, map.ByteSize);
            Assert.Equal("A row heap page is shorter than its slot map.", cut.Message);
            Assert.Equal("A row heap page is shorter than its slot map.", cutSlots.Message);
        }

        [Theory]
        [InlineData(26, 5L, "The slot map of a row heap page lists page sequence 5 after 5.")]
        [InlineData(26, 4L, "The slot map of a row heap page lists page sequence 4 after 5.")]
        [InlineData(21, 0, "The slot map of a row heap page puts 0 rows of page sequence 5 at row 0, the page has 4 rows.")]
        [InlineData(21, -2, "The slot map of a row heap page puts -2 rows of page sequence 5 at row 0, the page has 4 rows.")]
        [InlineData(34, 3, "The slot map of a row heap page puts 2 rows of page sequence 9 at row 3, the page has 4 rows.")]
        [InlineData(34, -1, "The slot map of a row heap page puts 2 rows of page sequence 9 at row -1, the page has 4 rows.")]
        [InlineData(38, int.MaxValue, "The slot map of a row heap page puts 2147483647 rows of page sequence 9 at row 2, the page has 4 rows.")]
        [InlineData(21, 1, "The slot map of a row heap page has rows from 2 where row 1 comes next, the page has 4 rows.")]
        [InlineData(34, 0, "The slot map of a row heap page has rows from 0 where row 2 comes next, the page has 4 rows.")]
        [InlineData(34, 1, "The slot map of a row heap page has rows from 1 where row 2 comes next, the page has 4 rows.")]
        [InlineData(42, (byte)2, "The slot map of a row heap page marks the slots of page sequence 9 with 2.")]
        [InlineData(38, 1, "The slot map of a row heap page places 3 rows, the page has 4.")]
        [InlineData(1, 0, "A row heap page says it holds 0 rows.")]
        [InlineData(1, 70_000, "A row heap page says it holds 70000 rows.")]
        [InlineData(43, (short)3, "The slot map of a row heap page lists the slots of page sequence 9 out of order.")]
        [InlineData(45, (short)1, "The slot map of a row heap page lists the slots of page sequence 9 out of order.")]
        public void ASlotMapThatDoesNotFitItsPageIsRefused(int offset, object value, string message)
        {
            using var batch = Batch(new string[] { "int" }, 0, 4);
            var written = new ArrayBufferWriter<byte>();
            using var serializer = new RowHeapPageSerializer(1, GlobalMemoryManager.Instance);
            var slotMap = new RowHeapSlotMap(new long[] { 5, 9 }, new int[] { 0, 2 }, new int[] { 2, 2 }, new ushort[]?[] { null, new ushort[] { 1, 3 } });
            var page = new RowHeapPage(new EventBatchData(new IColumn[] { batch.Columns[0].Copy(GlobalMemoryManager.Instance) }), 4, slotMap);
            serializer.Serialize(written, page);
            page.Return();
            var bytes = written.WrittenSpan.ToArray();
            var changed = (byte[])bytes.Clone();
            switch (value)
            {
                case long sequence:
                    BinaryPrimitives.WriteInt64LittleEndian(changed.AsSpan(offset), sequence);
                    break;
                case int number:
                    BinaryPrimitives.WriteInt32LittleEndian(changed.AsSpan(offset), number);
                    break;
                case byte flag:
                    changed[offset] = flag;
                    break;
                default:
                    BinaryPrimitives.WriteInt16LittleEndian(changed.AsSpan(offset), (short)value);
                    break;
            }

            var read = serializer.Deserialize(new ReadOnlySequence<byte>(bytes), bytes.Length);
            int rows = read.Count;
            read.Return();
            var refused = Assert.Throws<InvalidOperationException>(() => serializer.Deserialize(new ReadOnlySequence<byte>(changed), changed.Length));

            Assert.Equal(4, rows);
            Assert.Equal(message, refused.Message);
        }

        [Fact]
        public void AShortListCountsTheRoomItHasAndIsHalvedWhenHalfOfItIsFree()
        {
            var directory = new RowHeapDirectory();
            const int chunk = RowHeapDirectory.ChunkSize;
            for (long sequence = 1; sequence < 2 * chunk; sequence++)
            {
                directory.Set(sequence, 1);
            }
            for (long sequence = 1; sequence < chunk - 64; sequence++)
            {
                directory.Set(sequence, 0);
            }
            long with64 = directory.ByteSize;
            for (long sequence = chunk - 64; sequence < chunk - 33; sequence++)
            {
                directory.Set(sequence, 0);
            }
            long with33 = directory.ByteSize;
            directory.Set(chunk - 33, 0);
            long with32 = directory.ByteSize;
            for (long sequence = chunk - 32; sequence < chunk - 1; sequence++)
            {
                directory.Set(sequence, 0);
            }
            long withOne = directory.ByteSize;

            Assert.Equal(16 * 12 + chunk * 8 + 64 * 10 + 64, with64);
            Assert.Equal(with64, with33);
            Assert.Equal(with64 - 32 * 10, with32);
            Assert.Equal(with64 - 63 * 10, withOne);
            Assert.Equal(1, directory.Get(chunk - 1));
            Assert.Equal(0, directory.Get(chunk - 2));
        }

        [Fact]
        public void TheDirectoryCountsEveryArrayItKeeps()
        {
            var directory = new RowHeapDirectory();
            for (int sequence = 1; sequence <= RowHeapDirectory.SparseLimit; sequence++)
            {
                directory.Set(sequence, sequence);
            }
            directory.Set(RowHeapDirectory.ChunkSize, 1);
            for (int sequence = 1; sequence < RowHeapDirectory.SparseLimit - 20; sequence++)
            {
                directory.Set(sequence, 0);
            }
            var chunks = (object?[])typeof(RowHeapDirectory).GetField("_chunks", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(directory)!;
            long kept = chunks.Length * (sizeof(long) + sizeof(int));
            foreach (var chunk in chunks)
            {
                if (chunk is long[] dense)
                {
                    kept += dense.Length * sizeof(long);
                }
                else if (chunk != null)
                {
                    var slots = (ushort[])chunk.GetType().GetProperty("Slots")!.GetValue(chunk)!;
                    var values = (long[])chunk.GetType().GetProperty("Values")!.GetValue(chunk)!;
                    kept += slots.Length * sizeof(ushort) + values.Length * sizeof(long);
                }
            }

            Assert.True(directory.ByteSize >= kept, $"The directory says {directory.ByteSize} bytes, its arrays alone are {kept}.");
            Assert.True(directory.ByteSize <= kept + 64);
        }

        // Says each row is 600 MB, as a column in a struct
        private sealed class HeavyStringColumn : StringColumn, IDataColumn
        {
            public HeavyStringColumn() : base(GlobalMemoryManager.Instance)
            {
            }

            void IDataColumn.GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    sizes[i] += unchecked((i + 1) * 600_000_000);
                }
            }
        }

        [Fact]
        public async Task AStructColumnWhoseRowsPassTwoGigabytesIsCountedInFull()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            var children = new Column[2];
            for (int child = 0; child < children.Length; child++)
            {
                children[child] = Column.Create(0, new HeavyStringColumn(), default, ArrowTypeId.String, GlobalMemoryManager.Instance);
                for (int row = 0; row < 3; row++)
                {
                    children[child].Add(new StringValue("x"));
                }
            }
            var data = new StructColumn(StructHeader.Create("left", "right"), children, 3);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(0, data, default, ArrowTypeId.Struct, GlobalMemoryManager.Instance) });
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, 0, 3, references);

            Assert.Equal(3, heap.SequenceCount);
            Assert.InRange(heap.StoredBytes, 3_600_000_000L, 3_600_000_100L);
            Assert.Equal(heap.StoredBytes, heap.LiveBytes);
        }

        [Fact]
        public async Task AStructRowWhoseChildrenExceedFourGigabytesIsRefused()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            var children = new Column[8];
            for (int child = 0; child < children.Length; child++)
            {
                children[child] = Column.Create(0, new HeavyStringColumn(), default, ArrowTypeId.String, GlobalMemoryManager.Instance);
                children[child].Add(new StringValue("x"));
            }
            var data = new StructColumn(StructHeader.Create("a", "b", "c", "d", "e", "f", "g", "h"), children, 1);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(0, data, default, ArrowTypeId.Struct, GlobalMemoryManager.Instance) });
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await heap.AppendAsync(batch, 0, 1, references));
        }
    }
}
