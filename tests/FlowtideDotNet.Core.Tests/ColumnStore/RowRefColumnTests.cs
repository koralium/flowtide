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

using Apache.Arrow.Ipc;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Core.ColumnStore.Json;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using System.IO.Hashing;
using System.Text.Json;
using ArrowBuffer = Apache.Arrow.ArrowBuffer;
using FixedSizeBinaryType = Apache.Arrow.Types.FixedSizeBinaryType;
using TreeColumnComparer = FlowtideDotNet.Core.ColumnStore.TreeStorage.ColumnComparer;
using ArrayData = Apache.Arrow.ArrayData;
using Field = Apache.Arrow.Field;
using RecordBatch = Apache.Arrow.RecordBatch;
using Schema = Apache.Arrow.Schema;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class RowRefColumnTests
    {
        private const ulong High = 1UL << 63;

        private static Column Refs(params ulong?[] values)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                if (value.HasValue)
                {
                    column.Add(new RowRefValue(value.Value));
                }
                else
                {
                    column.Add(NullValue.Instance);
                }
            }
            return column;
        }

        private static ulong?[] Read(IColumn column)
        {
            var values = new ulong?[column.Count];
            for (int i = 0; i < values.Length; i++)
            {
                var value = column.GetValueAt(i, default);
                if (!value.IsNull)
                {
                    Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, value.Type);
                    values[i] = (ulong)value.AsLong;
                }
            }
            return values;
        }

        private static EventBatchData RoundTrip(EventBatchData batch, int count)
        {
            var writer = new ArrayBufferWriter<byte>();
            new EventBatchSerializer().SerializeEventBatch(writer, batch, count);
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray()));
            return new EventBatchDeserializer(GlobalMemoryManager.Instance).DeserializeBatch(ref reader).EventBatch;
        }

        [Fact]
        public void KeepsReferencesAndNulls()
        {
            using var column = Refs(0, 1, null, ulong.MaxValue, High);

            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, column.Type);
            Assert.Equal(1, column.NullCounter);
            Assert.Equal([0, 1, null, ulong.MaxValue, High], Read(column));
            Assert.Equal(ulong.MaxValue, Assert.IsType<RowRefValue>(column.GetValueAt(3, default)).Value);

            var container = new DataValueContainer();
            column.GetValueAt(4, container, default);
            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, container.Type);
            Assert.Equal(High, (ulong)container.AsLong);
        }

        [Fact]
        public void OrdersReferencesAsUnsignedNumbers()
        {
            using var column = Refs(High, 5, ulong.MaxValue, 0, null);

            Assert.True(column.CompareTo(column, 3, 1) < 0);
            Assert.True(column.CompareTo(column, 1, 0) < 0);
            Assert.True(column.CompareTo(column, 0, 2) < 0);
            Assert.Equal(0, column.CompareTo(column, 2, 2));
            Assert.True(column.CompareTo(column, 4, 3) < 0);
            Assert.True(column.CompareTo(0, new RowRefValue(High + 1), default) < 0);
            Assert.True(column.CompareTo(1, new RowRefValue(High), default) < 0);
            Assert.True(column.CompareTo(2, new RowRefValue(High), default) > 0);

            int[] order = [0, 1, 2, 3, 4];
            var span = order.AsSpan();
            new BatchSorter(1).SortData([column], ref span);
            Assert.Equal(new int[] { 4, 3, 1, 0, 2 }, order);
        }

        [Fact]
        public void ValuesCompareAndHashAsTheColumnDoes()
        {
            using var column = Refs(High, 5);
            var first = new DataValueContainer();
            var second = new DataValueContainer();
            column.GetValueAt(0, first, default);
            column.GetValueAt(1, second, default);
            var fromContainer = new XxHash32();
            var fromColumn = new XxHash32();
            first.AddToHash(fromContainer);
            column.AddToHash(0, default, fromColumn);

            Assert.True(DataValueComparer.CompareTo(first, second) > 0);
            Assert.True(DataValueComparer.CompareTo(new RowRefValue(5), new RowRefValue(High)) < 0);
            Assert.Equal(0, DataValueComparer.CompareTo(second, new RowRefValue(5)));
            Assert.True(DataValueComparer.CompareTo(new Int64Value(long.MaxValue), new RowRefValue(0)) < 0);
            Assert.Equal(fromColumn.GetCurrentHashAsUInt32(), fromContainer.GetCurrentHashAsUInt32());
        }

        [Fact]
        public void FindsARangeOfEqualReferences()
        {
            using var ascending = Refs(1, 5, 5, High, High, High, ulong.MaxValue);
            using var descending = Refs(ulong.MaxValue, High, High, 5, 1);

            Assert.Equal((3, 5), ascending.SearchBoundries(new RowRefValue(High), 0, 6, default));
            Assert.Equal((1, 2), ascending.SearchBoundries(new RowRefValue(5), 0, 6, default));
            Assert.True(ascending.SearchBoundries(new RowRefValue(6), 0, 6, default).Item1 < 0);
            Assert.Equal((1, 2), descending.SearchBoundries(new RowRefValue(High), 0, 4, default, true));
        }

        [Fact]
        public void CopiesRowsBetweenColumns()
        {
            using var source = Refs(10, null, High, 30);

            using var copy = source.Copy(GlobalMemoryManager.Instance);
            Assert.Equal([10, null, High, 30], Read(copy));

            using var ranged = Refs(1, 2);
            ranged.InsertRangeFrom(1, source, 1, 3);
            Assert.Equal([1, null, High, 30, 2], Read(ranged));

            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(3);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            offsets.Add(0);
            using var view = new ColumnWithOffset(source, offsets);
            using var fromView = Column.Create(GlobalMemoryManager.Instance);
            fromView.InsertRangeFrom(0, view, 0, 3);
            Assert.Equal([30, null, 10], Read(fromView));

            using var merged = Refs(1, 2);
            ReadOnlySpan<int> lookup = [2, 0];
            ReadOnlySpan<int> positions = [0, 2];
            merged.InsertFrom(source, in lookup, in positions, -1);
            Assert.Equal([High, 1, 2, 10], Read(merged));
        }

        [Fact]
        public void ChangesSingleRows()
        {
            using var column = Refs(1, 2, 3, 4, 5);

            column.InsertAt(1, new RowRefValue(High));
            column.UpdateAt(0, new RowRefValue(9));
            column.RemoveAt(2);
            Assert.Equal([9, High, 3, 4, 5], Read(column));

            column.InsertAt(0, NullValue.Instance);
            column.RemoveRange(2, 2);
            Assert.Equal([null, 9, 4, 5], Read(column));

            ReadOnlySpan<int> targets = [0, 2];
            column.DeleteBatch(targets);
            Assert.Equal([9, 5], Read(column));
        }

        [Fact]
        public void BecomesAUnionWithOtherTypes()
        {
            using var column = Refs(7);
            column.Add(new Int64Value(7));
            column.Add(new StringValue("a"));
            column.Add(new RowRefValue(High));

            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.Union, column.Type);
            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, column.GetValueAt(0, default).Type);
            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.Int64, column.GetValueAt(1, default).Type);
            Assert.Equal(High, (ulong)column.GetValueAt(3, default).AsLong);

            using var batch = new EventBatchData([column]);
            using var back = RoundTrip(batch, 4);
            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, back.Columns[0].GetValueAt(0, default).Type);
            Assert.Equal(7UL, (ulong)back.Columns[0].GetValueAt(0, default).AsLong);
            Assert.Equal("a", back.Columns[0].GetValueAt(2, default).AsString.ToString());
            Assert.Equal(High, (ulong)back.Columns[0].GetValueAt(3, default).AsLong);
        }

        [Fact]
        public void HashesTheSameWithEveryMethod()
        {
            using var column = Refs(0, 1, High, ulong.MaxValue, 123456789);
            int[] indices = [0, 1, 2, 3, 4];
            var expected = new uint[5];
            var hasher = new XxHash32();
            for (int i = 0; i < 5; i++)
            {
                hasher.Reset();
                column.AddToHash(i, default, hasher);
                expected[i] = hasher.GetCurrentHashAsUInt32();
            }

            var direct = new uint[5];
            column.ToXxHash32(indices, default, direct, new int[5]);
            var states = new Xxh32RowState[5];
            var appended = new uint[5];
            var single = new uint[5];
            for (int i = 0; i < 5; i++)
            {
                states[i].Init();
            }
            column.AppendToXxHash32(indices, default, states, new int[5]);
            for (int i = 0; i < 5; i++)
            {
                appended[i] = XxHash32Implementation.GetCurrentHashAsUInt32(ref states[i]);
                var state = new Xxh32RowState();
                state.Init();
                column.AppendXxHash32Single(i, default, ref state);
                single[i] = XxHash32Implementation.GetCurrentHashAsUInt32(ref state);
            }

            Assert.Equal(expected, direct);
            Assert.Equal(expected, appended);
            Assert.Equal(expected, single);
            Assert.Equal(5, expected.Distinct().Count());
        }

        [Fact]
        public void SurvivesTheStateSerializer()
        {
            using var column = Refs(0, null, High, ulong.MaxValue, 42);
            using var batch = new EventBatchData([column]);

            using var back = RoundTrip(batch, 5);

            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, back.Columns[0].Type);
            Assert.Equal([0, null, High, ulong.MaxValue, 42], Read(back.Columns[0]));
        }

        [Fact]
        public void IsWrittenAsItsOwnExtensionType()
        {
            using var column = Refs(1, High);
            using var batch = new EventBatchData([column]);
            var writer = new ArrayBufferWriter<byte>();
            new EventBatchSerializer().SerializeEventBatch(writer, batch, 2);

            using var stream = new MemoryStream(writer.WrittenSpan.ToArray());
            using var reader = new ArrowStreamReader(stream);
            using var recordBatch = reader.ReadNextRecordBatch();

            var field = recordBatch.Schema.FieldsList[0];
            Assert.True(field.Metadata.TryGetValue("ARROW:extension:name", out var name));
            Assert.Equal("flowtide.rowref", name);
            Assert.Equal(8, Assert.IsType<FixedSizeBinaryType>(field.DataType).ByteWidth);
        }

        [Fact]
        public void SurvivesInsideAStructAndAList()
        {
            var header = StructHeader.Create("ref", "name");
            using var structs = Column.Create(GlobalMemoryManager.Instance);
            structs.Add(new StructValue(header, new RowRefValue(High), new StringValue("a")));
            structs.Add(NullValue.Instance);
            structs.Add(new StructValue(header, NullValue.Instance, new StringValue("b")));
            using var lists = Column.Create(GlobalMemoryManager.Instance);
            lists.Add(new ListValue(new RowRefValue(1), new RowRefValue(ulong.MaxValue)));
            lists.Add(new ListValue(Array.Empty<IDataValue>()));
            lists.Add(new ListValue(new RowRefValue(3)));
            using var batch = new EventBatchData([structs, lists]);

            using var back = RoundTrip(batch, 3);

            var first = back.Columns[0].GetValueAt(0, default).AsStruct;
            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, first.GetAt(0).Type);
            Assert.Equal(High, (ulong)first.GetAt(0).AsLong);
            Assert.Equal("a", first.GetAt(1).AsString.ToString());
            Assert.True(back.Columns[0].GetValueAt(1, default).IsNull);
            Assert.True(back.Columns[0].GetValueAt(2, default).AsStruct.GetAt(0).IsNull);
            var list = back.Columns[1].GetValueAt(0, default).AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(ulong.MaxValue, (ulong)list.GetAt(1).AsLong);
            Assert.Equal(0, back.Columns[1].GetValueAt(1, default).AsList.Count);
            Assert.Equal(3UL, (ulong)back.Columns[1].GetValueAt(2, default).AsList.GetAt(0).AsLong);
        }

        [Fact]
        public void SurvivesTheArrowConversion()
        {
            using var column = Refs(5, null, High);
            using var batch = new EventBatchData([column]);

            using var recordBatch = EventArrowSerializer.BatchToArrow(batch, 3);
            using var back = EventArrowSerializer.ArrowToBatch(recordBatch, GlobalMemoryManager.Instance);

            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, back.Columns[0].Type);
            Assert.Equal([5, null, High], Read(back.Columns[0]));
        }

        [Fact]
        public void AnExtensionOfAnotherWidthIsRefused()
        {
            var metadata = new Dictionary<string, string>() { { "ARROW:extension:name", "flowtide.rowref" } };
            var field = new Field("c", new FixedSizeBinaryType(16), true, metadata);
            using var data = new ArrayData(field.DataType, 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[16])]);
            using var array = new Apache.Arrow.Arrays.FixedSizeBinaryArray(data);
            using var recordBatch = new RecordBatch(new Schema([field], null), [array], 1);
            using var stream = new MemoryStream();
            using (var arrowWriter = new ArrowStreamWriter(stream, recordBatch.Schema, true))
            {
                arrowWriter.WriteRecordBatch(recordBatch);
            }
            var bytes = stream.ToArray();

            var fromArrow = Assert.Throws<InvalidOperationException>(() => EventArrowSerializer.ArrowToBatch(recordBatch, GlobalMemoryManager.Instance));
            var fromBytes = Assert.Throws<InvalidOperationException>(() =>
            {
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
                new EventBatchDeserializer(GlobalMemoryManager.Instance).DeserializeBatch(ref reader);
            });

            Assert.Contains("8 bytes wide, this one is 16", fromArrow.Message);
            Assert.Contains("8 bytes wide, this one is 16", fromBytes.Message);
        }

        [Fact]
        public void ASlicedArrowArrayIsRefused()
        {
            var metadata = new Dictionary<string, string>() { { "ARROW:extension:name", "flowtide.rowref" } };
            var field = new Field("c", new FixedSizeBinaryType(8), true, metadata);
            using var data = new ArrayData(field.DataType, 1, 0, 1, [ArrowBuffer.Empty, new ArrowBuffer(new byte[16])]);
            using var array = new Apache.Arrow.Arrays.FixedSizeBinaryArray(data);
            using var recordBatch = new RecordBatch(new Schema([field], null), [array], 1);

            var error = Assert.Throws<NotSupportedException>(() => EventArrowSerializer.ArrowToBatch(recordBatch, GlobalMemoryManager.Instance));

            Assert.Contains("sliced flowtide.rowref array", error.Message);
        }

        [Fact]
        public void AColumnWithTooFewBytesIsRefused()
        {
            using var column = Refs(1, 2, 3, 4, 5, 6, 7, 8, 9);
            using var batch = new EventBatchData([column]);
            var writer = new ArrayBufferWriter<byte>();
            new EventBatchSerializer().SerializeEventBatch(writer, batch, 9);
            var bytes = writer.WrittenSpan.ToArray();
            int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(4));
            int headerStart = 8 + schemaLength + 8;
            int headerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(headerStart - 4));
            ReadOnlySpan<byte> header = bytes.AsSpan(headerStart, headerLength);
            var message = MessageStruct.GetRootAsMessage(ref header, 0);
            int valuesLength = headerStart + new RecordBatchStruct(header, message.HeaderPosition()).BuffersStartIndex + 16 + 8;
            Assert.Equal(72, BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(valuesLength)));
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(valuesLength), 71);

            var error = Assert.Throws<InvalidOperationException>(() =>
            {
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
                new EventBatchDeserializer(GlobalMemoryManager.Instance).DeserializeBatch(ref reader);
            });

            Assert.Contains("9 rows needs 72 bytes, this one has 71", error.Message);
        }

        [Fact]
        public void AShortArrowBufferIsRefused()
        {
            var metadata = new Dictionary<string, string>() { { "ARROW:extension:name", "flowtide.rowref" } };
            var field = new Field("c", new FixedSizeBinaryType(8), true, metadata);
            using var data = new ArrayData(field.DataType, 9, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[71])]);
            using var array = new Apache.Arrow.Arrays.FixedSizeBinaryArray(data);
            using var recordBatch = new RecordBatch(new Schema([field], null), [array], 9);

            var error = Assert.Throws<InvalidOperationException>(() => EventArrowSerializer.ArrowToBatch(recordBatch, GlobalMemoryManager.Instance));

            Assert.Contains("9 rows needs 72 bytes, this one has 71", error.Message);
        }

        private sealed class TestCompressor : IBatchCompressor, IDisposable
        {
            public readonly ZstdSharp.Compressor Zstd = new ZstdSharp.Compressor();

            public void Dispose()
            {
                Zstd.Dispose();
            }

            public void ColumnChange(int columnIndex)
            {
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return Zstd.Wrap(input, output);
            }
        }

        private sealed class TestDecompressor : IBatchDecompressor, IDisposable
        {
            private readonly ZstdSharp.Decompressor _zstd = new ZstdSharp.Decompressor();

            public void Dispose()
            {
                _zstd.Dispose();
            }

            public void ColumnChange(int columnIndex)
            {
            }

            public int Unwrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return _zstd.Unwrap(input, output);
            }
        }

        [Fact]
        public void ACompressedColumnWithTooFewBytesIsRefused()
        {
            using var column = Refs(1, 2, 3, 4, 5, 6, 7, 8, 9);
            using var batch = new EventBatchData([column]);
            using var compressor = new TestCompressor();
            using var decompressor = new TestDecompressor();
            var writer = new ArrayBufferWriter<byte>();
            new EventBatchSerializer().SerializeEventBatch(writer, batch, 9, compressor);
            var bytes = writer.WrittenSpan.ToArray();
            var intact = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
            using (var read = new EventBatchDeserializer(GlobalMemoryManager.Instance, decompressor).DeserializeBatch(ref intact).EventBatch)
            {
                Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], Read(read.Columns[0]));
            }
            int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(4));
            int headerStart = 8 + schemaLength + 8;
            int headerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(headerStart - 4));
            ReadOnlySpan<byte> header = bytes.AsSpan(headerStart, headerLength);
            var message = MessageStruct.GetRootAsMessage(ref header, 0);
            int values = headerStart + new RecordBatchStruct(header, message.HeaderPosition()).BuffersStartIndex + 16;
            int valuesStart = headerStart + headerLength + (int)BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(values));
            int valuesLength = (int)BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(values + 8));
            var shortFrame = compressor.Zstd.Wrap(new byte[8]).ToArray();
            Assert.Equal(72, BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(valuesStart)));
            Assert.True(shortFrame.Length + 8 <= valuesLength);
            shortFrame.CopyTo(bytes.AsSpan(valuesStart + 8));
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(values + 8), shortFrame.Length + 8);

            var error = Assert.Throws<InvalidOperationException>(() =>
            {
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
                new EventBatchDeserializer(GlobalMemoryManager.Instance, decompressor).DeserializeBatch(ref reader);
            });

            Assert.Contains("9 rows needs 72 bytes, this one has 8", error.Message);
        }

        [Fact]
        public void ASizeInfoBuildsAReferenceColumn()
        {
            using var column = Refs(1, 2, null);

            using var sized = new Column(GlobalMemoryManager.Instance, column.GetColumnSizeInfo());

            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, sized.Type);
            Assert.Empty(sized);
            sized.Add(new RowRefValue(High));
            Assert.Equal([High], Read(sized));
        }

        private static StateManagerSync<object> TreeState(FileCachePersistentStorage storage)
        {
            return new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = storage
            }, NullLoggerFactory.Instance, new Meter("rowref"), "rowref", GlobalMemoryManager.Instance);
        }

        private static ValueTask<IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>> Tree(StateManagerSync<object> stateManager)
        {
            return stateManager.GetOrCreateClient("node1").GetOrCreateTree("tree",
                new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>()
                {
                    BucketSize = 8,
                    Comparer = new TreeColumnComparer(2),
                    KeySerializer = new ColumnStoreSerializer(2, GlobalMemoryManager.Instance),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
        }

        [Fact]
        public async Task ATreeKeepsItsOrderOverARestart()
        {
            var directory = $"./data/rowRefTree/{Guid.NewGuid():N}";
            var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = directory }, true);
            var stateManager = TreeState(storage);
            await stateManager.InitializeAsync();
            var tree = await Tree(stateManager);
            var random = new Random(3);
            var keys = Column.Create(GlobalMemoryManager.Instance);
            var references = Column.Create(GlobalMemoryManager.Instance);
            var expected = new List<(long key, ulong? reference)>();
            for (int i = 0; i < 300; i++)
            {
                long key = random.Next(20);
                ulong? reference = (ulong)random.NextInt64() ^ (random.Next(2) == 0 ? High : 0);
                if (random.Next(10) == 0)
                {
                    reference = null;
                    references.Add(NullValue.Instance);
                }
                else
                {
                    references.Add(new RowRefValue(reference.Value));
                }
                keys.Add(new Int64Value(key));
                expected.Add((key, reference));
            }
            using var batch = new EventBatchData([keys, references]);
            for (int i = 0; i < 300; i++)
            {
                await tree.Upsert(new ColumnRowReference() { referenceBatch = batch, RowIndex = i }, i);
            }
            await tree.Commit();
            await stateManager.CheckpointAsync();
            stateManager.Dispose();

            var restarted = TreeState(storage);
            await restarted.InitializeAsync();
            var restored = await Tree(restarted);
            using var added = new EventBatchData([Column.Create(GlobalMemoryManager.Instance), Refs(High + 1)]);
            added.Columns[0].Add(new Int64Value(7));
            await restored.Delete(new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 });
            await restored.Upsert(new ColumnRowReference() { referenceBatch = added, RowIndex = 0 }, 300);
            var deleted = expected[0];
            expected.RemoveAll(x => x == deleted);
            expected.Add((7, High + 1));
            var actual = new List<(long key, ulong? reference)>();
            using (var iterator = restored.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    foreach (var row in page)
                    {
                        var reference = row.Key.referenceBatch.Columns[1].GetValueAt(row.Key.RowIndex, default);
                        actual.Add((row.Key.referenceBatch.Columns[0].GetValueAt(row.Key.RowIndex, default).AsLong, reference.IsNull ? null : (ulong)reference.AsLong));
                    }
                }
            }
            restarted.Dispose();
            storage.ForceDispose();
            Directory.Delete(directory, true);

            Assert.Equal(expected.Distinct().OrderBy(x => x.key).ThenBy(x => x.reference), actual);
        }

        [Fact]
        public void MultiJoinCopiesReferencesAsPlainValues()
        {
            using var source = Refs(10, null, High);
            using var target = Refs(1);

            Assert.True(MultiJoinTreeLookup.IsPlain(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef));
            Assert.False(MultiJoinTreeLookup.TakesRowsAsObjects(target, source));

            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(2);
            offsets.Add(1);
            offsets.Add(0);
            using var view = new ColumnWithOffset(source, offsets);
            int[] positions = [];
            MultiJoinTreeLookup.AppendRows(target, view, 3, ref positions);

            Assert.Equal([1, High, null, 10], Read(target));
        }

        [Fact]
        public void AFieldReferenceCopiesReferences()
        {
            using var column = Refs(4, null, High);
            using var batch = new EventBatchData([column]);
            var register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);
            var project = ColumnProjectCompiler.Compile(new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } }, register);
            using var result = Column.Create(GlobalMemoryManager.Instance);

            for (int i = 0; i < 3; i++)
            {
                project(batch, i, result);
            }

            Assert.Equal(FlowtideDotNet.Core.ColumnStore.ArrowTypeId.RowRef, result.Type);
            Assert.Equal([4, null, High], Read(result));
        }

        private static IDataValue Evaluate(Expression expression, EventBatchData batch)
        {
            var register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);
            return ColumnProjectCompiler.CompileToValue(expression, register)(batch, 0);
        }

        [Fact]
        public void ConversionsAreRefused()
        {
            using var column = Refs(4, 5);
            using var batch = new EventBatchData([column]);
            var field = new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } };
            var container = new DataValueContainer();
            column.GetValueAt(0, container, default);
            using var stream = new MemoryStream();
            using var json = new Utf8JsonWriter(stream);

            var castTypes = new SubstraitBaseType[]
            {
                new FlowtideDotNet.Substrait.Type.StringType(),
                new FlowtideDotNet.Substrait.Type.Int64Type(),
                new DecimalType(),
                new BoolType(),
                new Fp64Type(),
                new TimestampType()
            };
            foreach (var castType in castTypes)
            {
                var cast = Assert.Throws<NotSupportedException>(() => Evaluate(new CastExpression() { Expression = field, Type = castType }, batch));
                Assert.Contains("cannot be cast", cast.Message);
            }
            var columnJson = Assert.Throws<NotSupportedException>(() => column.WriteToJson(in json, 0));
            var valueJson = Assert.Throws<NotSupportedException>(() => new DataValueJsonWriter().Visit(ref container));
            var rowEvent = Assert.Throws<NotSupportedException>(() => RowEventToEventBatchData.RowReferenceToRowEvent(1, 0, new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 }));

            Assert.Contains("written as JSON", columnJson.Message);
            Assert.Contains("cannot be converted,", valueJson.Message);
            Assert.Contains("converted to a row event", rowEvent.Message);
        }
    }
}
