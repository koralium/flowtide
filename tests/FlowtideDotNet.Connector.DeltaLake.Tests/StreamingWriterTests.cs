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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using ParquetSharp;
using System.Collections;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class StreamingWriterTests : IDisposable
    {
        private static readonly Delta.StructType s_table = new Delta.StructType(new[]
        {
            Field("i8", new Delta.ByteType()),
            Field("i16", new Delta.ShortType()),
            Field("i32", new Delta.IntegerType()),
            Field("i64", new Delta.LongType()),
            Field("f32", new Delta.FloatType()),
            Field("f64", new Delta.DoubleType()),
            Field("bo", new Delta.BooleanType()),
            Field("da", new Delta.DateType()),
            Field("ts", new Delta.TimestampType()),
            Field("de", new Delta.DecimalType(10, 2)),
            Field("st", new Delta.StringType()),
            Field("bi", new Delta.BinaryType()),
            Field("s", new Delta.StructType(new[] { Field("x", new Delta.IntegerType()), Field("y", new Delta.StringType()) })),
            Field("li", new Delta.ArrayType() { ElementType = new Delta.IntegerType(), ContainsNull = true }),
            Field("mp", new Delta.MapType(new Delta.StringType(), new Delta.IntegerType(), true)),
        });

        private readonly string _directory;

        public StreamingWriterTests()
        {
            _directory = Path.Join(Path.GetTempPath(), "flowtide-streaming-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        [Fact]
        public async Task ThreeGroupsKeepTheRowsAndStatisticsOfOne()
        {
            var source = WriteSource(30, 10);
            var deleted = new long[] { 5, 17 };
            using var file = Open(source);
            var binding = FileBinding.Create(s_table, file, "t");
            var path = Path.Join(_directory, "out.parquet");
            using var streamed = Writer();
            streamed.OpenStreaming(path, Budget(new JobMemory()));
            using var whole = Writer();
            var start = whole.Measure().Length;
            var mask = new BatchDeleteMask();
            long index = 0;
            long appended = 0;

            for (int group = 0; group < 3; group++)
            {
                await foreach (var batch in file.ReadRowGroup(group, binding.LeafColumns))
                {
                    using (batch)
                    using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                    {
                        mask.Reset(index, rebound.Length);
                        foreach (var position in deleted.Where(x => x >= index && x < index + rebound.Length))
                        {
                            mask.Add(position);
                        }
                        streamed.CopyBatch(rebound, index, mask);
                        appended += whole.CopyBatch(rebound, index, mask);
                        index += rebound.Length;
                    }
                }
                Assert.Null(streamed.FlushRowGroup());
            }
            var length = streamed.CloseStreaming();

            var output = File.ReadAllBytes(path);
            using (var reader = new ParquetFileReader(new MemoryStream(output)))
            {
                Assert.Equal(3, reader.FileMetaData.NumRowGroups);
            }
            Assert.Equal(output.Length, length);
            Assert.Equal(28, streamed.FileRows);
            Assert.Equal(whole.Measure().Length - start, appended);
            Assert.Equal(Statistics(whole), Statistics(streamed));
            var expected = (await Rows(source)).Where((_, i) => !deleted.Contains(i));
            Assert.Equal(expected, await Rows(output));
        }

        [Fact]
        public async Task BuilderSizesCountEveryBufferAtCapacity()
        {
            using var file = Open(WriteSource(30, 30));
            var binding = FileBinding.Create(s_table, file, "t");
            using var writer = Writer();
            var mask = new BatchDeleteMask();
            long elements = 0;
            long entries = 0;
            await foreach (var batch in file.ReadRowGroup(0, binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    mask.Reset(0, rebound.Length);
                    writer.CopyBatch(rebound, 0, mask);
                    elements += ((ListArray)rebound.Column(13)).Values.Length;
                    entries += ((MapArray)rebound.Column(14)).KeyValues.Length;
                }
            }

            var sizes = writer.Measure();
            var buffers = new List<long>();
            Walk(writer, buffers, new HashSet<object>(ReferenceEqualityComparer.Instance));

            Assert.Equal(43, sizes.Buffers);
            Assert.Equal(buffers.Count, sizes.Buffers);
            Assert.Equal(buffers.Sum(), sizes.Capacity);
            Assert.Equal(buffers.Sum(x => (x + 63) / 64 * 64 + 64), sizes.Built);
            Assert.Equal(17, sizes.Leaves.Count);
            Assert.All(sizes.Leaves.Take(14), x => Assert.Equal((30, false), (x.Levels, x.Repeated)));
            Assert.Equal(new[] { (30 + elements, true), (30 + entries, true), (30 + entries, true) }, sizes.Leaves.Skip(14).Select(x => (x.Levels, x.Repeated)));
        }

        [Fact]
        public void ANewGroupStartsWithNoNulls()
        {
            using var writer = Writer();
            writer.OpenStreaming(Path.Join(_directory, "out.parquet"), Budget(new JobMemory()));
            var columns = s_table.Fields.Select(_ =>
            {
                var column = FlowtideDotNet.Core.ColumnStore.Column.Create(FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance);
                column.Add(FlowtideDotNet.Core.ColumnStore.DataValues.NullValue.Instance);
                return (FlowtideDotNet.Core.ColumnStore.IColumn)column;
            }).ToArray();
            writer.AddRow(new FlowtideDotNet.Core.ColumnStore.TreeStorage.ColumnRowReference() { referenceBatch = new FlowtideDotNet.Core.ColumnStore.EventBatchData(columns), RowIndex = 0 });
            Assert.Null(writer.FlushRowGroup());

            using var batch = writer.GetRecordBatch();

            Assert.All(Enumerable.Range(0, batch.ColumnCount), x => Assert.Equal(0, batch.Column(x).NullCount));
        }

        [Fact]
        public void AnExcessIsHeldOnlyWhileItsGroupIsWritten()
        {
            var memory = new JobMemory();
            using var writer = LongWriter();
            writer.OpenStreaming(Path.Join(_directory, "out.parquet"), new GroupBudget(memory, 0, 0, 1L << 30, (_, _) => 5_000));
            AddLongs(writer, 100);
            var built = writer.Measure().Built;

            Assert.Null(writer.FlushRowGroup());
            Assert.Equal((1, built + 5_000, 0L), (writer.RowGroups, memory.NativePeak, memory.Native));
        }

        [Fact]
        public void GroupsPastTheChargedOnesHoldTheirMetadataUpTo1024()
        {
            var memory = new JobMemory();
            using (var writer = LongWriter())
            {
                writer.OpenStreaming(Path.Join(_directory, "out.parquet"), new GroupBudget(memory, 1L << 40, 100, 1L << 30, (_, _) => 0) { ChargedGroups = 2 });
                for (int group = 0; group < ParquetSharpWriter.MaxRowGroups; group++)
                {
                    AddLongs(writer, 1);
                    Assert.Null(writer.FlushRowGroup());
                }
                Assert.Equal(1_022 * 100, memory.Native);

                AddLongs(writer, 1);
                Assert.Equal(CompactionReason.GroupCap, writer.FlushRowGroup());
                Assert.Equal(1_024, writer.RowGroups);
                writer.NewBatch();
                writer.CloseStreaming();
            }
            var path = Path.Join(_directory, "out.parquet");
            using (var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 7, new FileInfo(path).Length)))
            {
                Assert.Equal((1_024, 1_024L), (file.LowLevel.FileMetaData.NumRowGroups, file.NumRows));
            }
            Assert.Equal(0, memory.Native);
        }

        [Fact]
        public void AGrowthTheLedgerRefusesFlushesFirstAndRetriesOnce()
        {
            var memory = new JobMemory(managedCap: 2_000);
            using (var writer = LongWriter())
            {
                writer.OpenStreaming(Path.Join(_directory, "out.parquet"), new GroupBudget(memory, 1L << 40, 100, 1_000, (_, _) => 0) { ChargedGroups = 1 });
                AddLongs(writer, 100);

                Assert.Null(writer.ReserveGrowth(100));
                Assert.Null(writer.ReserveGrowth(100));
                Assert.Equal((0, 0L, 3 * (813 + 100) + 96 * 2L - 1_000), (writer.RowGroups, memory.Native, memory.Managed));

                AddLongs(writer, 100);
                Assert.Null(writer.ReserveGrowth(100));
                Assert.Equal((1, 0L, 0L), (writer.RowGroups, memory.Native, memory.Managed));

                AddLongs(writer, 300);
                Assert.Null(writer.ReserveGrowth(100));
                Assert.Equal((2, 100L, 0L), (writer.RowGroups, memory.Native, memory.Managed));

                Assert.Equal(CompactionReason.ManagedBound, writer.ReserveGrowth(10_000));
                Assert.Equal(2, writer.RowGroups);
            }
            Assert.Equal((0L, 0L), (memory.Native, memory.Managed));
        }

        [Fact]
        public void AFailedGroupWriteKeepsOnlyItsMetadataUntilDispose()
        {
            var memory = new JobMemory();
            using (var writer = LongWriter())
            {
                writer.OpenStreaming(Path.Join(_directory, "out.parquet"), new GroupBudget(memory, 0, 100, 1L << 30, (_, _) => 5_000));
                AddLongs(writer, 100);
                var native = (IDisposable)typeof(ParquetSharpWriter).GetField("_fileWriter", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(writer)!;
                native.Dispose();

                Assert.Throws<NullReferenceException>(() => writer.FlushRowGroup());
                Assert.Equal(100, memory.Native);
            }
            Assert.Equal((0L, 0L), (memory.Native, memory.Managed));
        }

        [Fact]
        public void TheLedgerCallsItsHookAndRefusesOverflow()
        {
            var memory = new JobMemory(managedCap: 1_000, nativeCap: 1_000);
            memory.Reserve(100);
            using var native = memory.ReserveNative(100);
            long hooked = 0;
            memory.BeforeReserveForTests = x => hooked = x;

            Assert.False(memory.TryReserve(long.MaxValue));
            Assert.False(memory.TryReserveNative(long.MaxValue));
            Assert.Throws<InvalidOperationException>(() => memory.ReleaseNative(101));
            Assert.Equal((100L, 100L, long.MaxValue), (memory.Managed, memory.Native, hooked));
        }

        [Fact]
        public void WriterTermsTrapOverflow()
        {
            Assert.Throws<OverflowException>(() => WriterTerms.Leaf(Compression.Zstd, 1L << 20, long.MaxValue, long.MaxValue, 1_024, long.MaxValue, 2, 1_000));
        }

        [Fact]
        public void BoolsAndDatesBuildValidityOnlyWithNulls()
        {
            var bools = new ParquetBoolWriter();
            var dates = new ParquetDateWriter();
            bools.NewBatch();
            dates.NewBatch();
            for (int i = 0; i < 100; i++)
            {
                bools.WriteValue(BoolValue.True);
                dates.WriteValue(new TimestampTzValue(new DateTime(2020, 1, 1)));
            }
            using (var b = bools.GetArray())
            using (var d = dates.GetArray())
            {
                Assert.Equal((0, 0), (b.Data.Buffers[0].Length, d.Data.Buffers[0].Length));
            }

            bools.WriteNull();
            dates.WriteNull();

            using (var b = bools.GetArray())
            using (var d = dates.GetArray())
            {
                Assert.Equal((1, 1, true, true), (b.NullCount, d.NullCount, b.IsNull(100), d.IsNull(100)));
            }
        }

        [Fact]
        public void StreamingNeedsOneGroupPerFlushAndEveryRowFlushed()
        {
            var table = new Delta.StructType(new[] { Field("v", new Delta.LongType()) });
            using var sinkWriter = new ParquetSharpWriter(table, new List<string>() { "v" }, writerProperties: DeltaParquetCompression.GetWriterProperties(Compression.Snappy));
            using var writer = LongWriter();
            writer.OpenStreaming(Path.Join(_directory, "out.parquet"), Budget(new JobMemory()));
            AddLongs(writer, 3);

            Assert.Equal("Streaming needs the output writer properties", Assert.Throws<InvalidOperationException>(() => sinkWriter.OpenStreaming(Path.Join(_directory, "sink.parquet"), Budget(new JobMemory()))).Message);
            Assert.Equal("The writer is already streaming", Assert.Throws<InvalidOperationException>(() => writer.OpenStreaming(Path.Join(_directory, "again.parquet"), Budget(new JobMemory()))).Message);
            Assert.Equal("3 rows are not flushed", Assert.Throws<InvalidOperationException>(() => writer.CloseStreaming()).Message);
        }

        [Fact]
        public void ANewBatchStartsWithNoGroups()
        {
            using var writer = LongWriter();
            writer.OpenStreaming(Path.Join(_directory, "out.parquet"), Budget(new JobMemory()));
            AddLongs(writer, 5);
            Assert.Null(writer.FlushRowGroup());

            writer.NewBatch();

            Assert.Equal((0, 0L), (writer.RowGroups, writer.FileRows));
        }

        [Fact]
        public void UnitsFoldToMillisecondsOnlyWhereEveryInputHasThem()
        {
            var units = ParquetSharpWriter.FoldUnits(new[]
            {
                new[] { TimeUnit.Millisecond, TimeUnit.Second, TimeUnit.Microsecond, TimeUnit.Millisecond },
                new[] { TimeUnit.Second, TimeUnit.Millisecond, TimeUnit.Millisecond, TimeUnit.Nanosecond },
            });

            Assert.Equal(new[] { TimeUnit.Millisecond, TimeUnit.Millisecond, TimeUnit.Microsecond, TimeUnit.Microsecond }, units);
        }

        private static ParquetSharpWriter Writer()
        {
            var writer = new ParquetSharpWriter(s_table, s_table.Fields.Select(x => x.Name).ToList(), writerProperties: DeltaParquetCompression.GetOutputWriterProperties(Compression.Zstd), units: new[] { TimeUnit.Microsecond });
            writer.NewBatch();
            return writer;
        }

        internal static ParquetSharpWriter LongWriter()
        {
            var table = new Delta.StructType(new[] { Field("v", new Delta.LongType()) });
            var writer = new ParquetSharpWriter(table, new List<string>() { "v" }, writerProperties: DeltaParquetCompression.GetOutputWriterProperties(Compression.Snappy));
            writer.NewBatch();
            return writer;
        }

        internal static void AddLongs(ParquetSharpWriter writer, int rows)
        {
            using var values = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => (long)x)).Build();
            using var rebound = new ReboundBatch(rows, new IArrowArray[] { values }, new List<ArrowBuffer>());
            var mask = new BatchDeleteMask();
            mask.Reset(0, rows);
            writer.CopyBatch(rebound, 0, mask);
        }

        private static GroupBudget Budget(JobMemory memory) => new GroupBudget(memory, 1L << 40, 0, 1L << 30, (_, _) => 0);

        private static string Statistics(ParquetSharpWriter writer)
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new DeltaStatisticsConverter(s_table));
            return JsonSerializer.Serialize(writer.GetStatistics(), options);
        }

        private ParquetDataFile Open(byte[] bytes)
        {
            var path = Path.Join(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            File.WriteAllBytes(path, bytes);
            return new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 1_024, bytes.Length));
        }

        // Every column with nulls, nested nulls, empty lists and maps
        private static byte[] WriteSource(int rows, int groupRows)
        {
            var i8 = new Int8Array.Builder();
            var i16 = new Int16Array.Builder();
            var i32 = new Int32Array.Builder();
            var i64 = new Int64Array.Builder();
            var f32 = new FloatArray.Builder();
            var f64 = new DoubleArray.Builder();
            var bo = new BooleanArray.Builder();
            var da = new Date32Array.Builder();
            var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
            var de = new Decimal128Array.Builder(new Decimal128Type(10, 2));
            var st = new StringArray.Builder();
            var bi = new BinaryArray.Builder();
            var sx = new Int32Array.Builder();
            var sy = new StringArray.Builder();
            var sValid = new List<bool>();
            var li = new ListArray.Builder(Int32Type.Default);
            var liValues = (Int32Array.Builder)li.ValueBuilder;
            var mp = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
            var mpKeys = (StringArray.Builder)mp.KeyBuilder;
            var mpValues = (Int32Array.Builder)mp.ValueBuilder;
            for (int i = 0; i < rows; i++)
            {
                if (i % 4 == 3)
                {
                    i8.AppendNull();
                    i16.AppendNull();
                    i32.AppendNull();
                    i64.AppendNull();
                    f32.AppendNull();
                    f64.AppendNull();
                    bo.AppendNull();
                    da.AppendNull();
                    ts.AppendNull();
                    de.AppendNull();
                    st.AppendNull();
                    bi.AppendNull();
                    sx.AppendNull();
                    sy.AppendNull();
                }
                else
                {
                    i8.Append((sbyte)(i - 15));
                    i16.Append((short)(i * 100 - 900));
                    i32.Append(i * 7 - 50);
                    i64.Append(i * 1_000_000_007L - 3);
                    f32.Append(i * 1.5f - 7);
                    f64.Append(i * -2.25 + 3);
                    bo.Append(i % 3 == 0);
                    da.Append(new DateTime(2020, 1, 1).AddDays(i * 3 - 40));
                    ts.Append(DateTimeOffset.UnixEpoch.AddTicks(i * 12_345_670L - 99_990L));
                    de.Append(i * 1.25m - 9);
                    st.Append($"s{i * 37 % 23}");
                    bi.Append(new byte[] { (byte)i, (byte)(255 - i) }.AsSpan());
                    sx.Append(i);
                    sy.Append($"y{i % 7}");
                }
                sValid.Add(i % 5 != 2);
                if (i % 6 == 1)
                {
                    li.AppendNull();
                    mp.AppendNull();
                    continue;
                }
                li.Append();
                mp.Append();
                for (int j = 0; j < (i % 6 == 4 ? 0 : i % 3 + 1); j++)
                {
                    liValues.Append(i + j);
                    mpKeys.Append($"k{j}");
                    if (j == 1)
                    {
                        mpValues.AppendNull();
                    }
                    else
                    {
                        mpValues.Append(i * j);
                    }
                }
            }

            var validity = new ArrowBuffer.BitmapBuilder();
            foreach (var valid in sValid)
            {
                validity.Append(valid);
            }
            var x = sx.Build();
            var y = sy.Build();
            var structType = new StructType(new List<Field>() { new Field("x", x.Data.DataType, true), new Field("y", y.Data.DataType, true) });
            var columns = new IArrowArray[]
            {
                i8.Build(), i16.Build(), i32.Build(), i64.Build(), f32.Build(), f64.Build(), bo.Build(), da.Build(), ts.Build(), de.Build(), st.Build(), bi.Build(),
                new StructArray(structType, rows, new IArrowArray[] { x, y }, validity.Build(), sValid.Count(v => !v)),
                li.Build(),
                mp.Build(),
            };
            var schema = new Schema.Builder();
            foreach (var (field, column) in s_table.Fields.Zip(columns))
            {
                schema.Field(new Field(field.Name, column.Data.DataType, true));
            }
            using var batch = new RecordBatch(schema.Build(), columns, rows);
            using var stream = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, batch.Schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch, groupRows);
                writer.Close();
            }
            return stream.ToArray();
        }

        private static Task<List<string>> Rows(byte[] file)
        {
            return RewriteCopyTests.ReadRows(file, batch => Enumerable.Range(0, batch.Length)
                .Select(i => string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => Render(batch.Column(c), i)))));
        }

        private static string Render(IArrowArray array, int i)
        {
            if (array.IsNull(i))
            {
                return "null";
            }
            return array switch
            {
                Int8Array a => $"{a.GetValue(i)}",
                Int16Array a => $"{a.GetValue(i)}",
                Int32Array a => $"{a.GetValue(i)}",
                Int64Array a => $"{a.GetValue(i)}",
                FloatArray a => $"{a.GetValue(i)}",
                DoubleArray a => $"{a.GetValue(i)}",
                BooleanArray a => $"{a.GetValue(i)}",
                Date32Array a => $"{a.GetValue(i)}",
                TimestampArray a => $"{a.GetValue(i)}",
                Decimal128Array a => $"{a.GetValue(i)}",
                StringArray a => a.GetString(i),
                BinaryArray a => Convert.ToHexString(a.GetBytes(i)),
                StructArray a => "{" + string.Join(",", a.Fields.Select(x => Render(x, i))) + "}",
                ListArray a => "[" + string.Join(",", Enumerable.Range(a.ValueOffsets[i], a.GetValueLength(i)).Select(j => Render(a.Values, j))) + "]",
                _ => throw new NotSupportedException(array.GetType().Name)
            };
        }

        // Every buffer the writer holds, found by reflection
        private static void Walk(object node, List<long> buffers, HashSet<object> seen)
        {
            if (!seen.Add(node))
            {
                return;
            }
            var type = node.GetType();
            if (node is ArrowBuffer.BitmapBuilder bitmap)
            {
                buffers.Add(bitmap.Capacity / 8);
                return;
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ArrowBuffer.Builder<>))
            {
                buffers.Add((int)type.GetProperty("Capacity")!.GetValue(node)! * (long)Marshal.SizeOf(type.GetGenericArguments()[0]));
                return;
            }
            if (node is IList list)
            {
                foreach (var item in list.Cast<object?>().Where(x => x != null))
                {
                    Walk(item!, buffers, seen);
                }
            }
            for (var t = type; t != null && t != typeof(object); t = t.BaseType)
            {
                foreach (var field in t.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    var value = field.GetValue(node);
                    var space = value?.GetType().Namespace ?? "";
                    if (value is IList || (space.StartsWith("Apache.Arrow") && space != "Apache.Arrow.Types") || space.StartsWith("FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat"))
                    {
                        Walk(value!, buffers, seen);
                    }
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        public void ARepeatedWindowCoversArrowsWriteBatch(int depth)
        {
            const int rows = 2_048, length = 4_096;
            FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type = new Delta.StringType();
            IArrowArray array = new StringArray.Builder().AppendRange(Enumerable.Repeat(new string('x', length), rows)).Build();
            for (int d = 0; d < depth; d++)
            {
                type = new Delta.ArrayType() { ElementType = type, ContainsNull = true };
                var offsets = new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, rows + 1)).Build();
                array = new ListArray(new ListType(new Field("element", array.Data.DataType, true)), rows, offsets, array, ArrowBuffer.Empty, 0);
            }
            using var writer = new ParquetSharpWriter(new Delta.StructType(new[] { Field("l", type) }), new List<string>() { "l" }, writerProperties: DeltaParquetCompression.GetOutputWriterProperties(Compression.Zstd));
            writer.NewBatch();
            using var rebound = new ReboundBatch(rows, new IArrowArray[] { array }, new List<ArrowBuffer>());
            var mask = new BatchDeleteMask();
            mask.Reset(0, rows);
            writer.CopyBatch(rebound, 0, mask);

            var leaf = writer.Measure(windows: true).Leaves.Single();

            Assert.True(leaf.Window >= 1_024L * length, $"{leaf.Window} bytes in the window");
        }

        private static Delta.StructField Field(string name, FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type)
        {
            return new Delta.StructField(name, type, true, new Dictionary<string, object>());
        }
    }

    [CollectionDefinition(nameof(ArrowAllocatorCollection), DisableParallelization = true)]
    public class ArrowAllocatorCollection
    {
    }

    // Arrow's default allocator counts every build, so nothing may run beside it
    [Collection(nameof(ArrowAllocatorCollection))]
    public class StreamingWriterAllocationTests
    {
        [Fact]
        public void ARefusedGroupBuildsNothing()
        {
            var path = Path.Join(Path.GetTempPath(), "refused-" + Guid.NewGuid().ToString("N") + ".parquet");
            var memory = new JobMemory(nativeCap: 1_000);
            try
            {
                using var writer = StreamingWriterTests.LongWriter();
                writer.OpenStreaming(path, new GroupBudget(memory, 0, 0, 1L << 30, (_, _) => 5_000));
                StreamingWriterTests.AddLongs(writer, 100);
                var allocations = MemoryAllocator.Default.Value.Statistics.Allocations;

                Assert.Equal(CompactionReason.WriterBound, writer.FlushRowGroup());
                Assert.Equal(allocations, MemoryAllocator.Default.Value.Statistics.Allocations);
                Assert.Equal((0, 100L, 0L), (writer.RowGroups, writer.FileRows, memory.NativePeak));
                Assert.Contains("Row group 0", writer.RefusalDetail);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void EveryFlushedGroupTrimsTheHeapWhereGlibcAllocates()
        {
            var glibc = OperatingSystem.IsLinux() && !RuntimeInformation.RuntimeIdentifier.Contains("musl", StringComparison.Ordinal);
            var path = Path.Join(Path.GetTempPath(), "trimmed-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                using var writer = StreamingWriterTests.LongWriter();
                writer.OpenStreaming(path, new GroupBudget(new JobMemory(), 1L << 40, 0, 1L << 30, (_, _) => 0));
                var before = NativeHeap.Trims;
                for (int group = 0; group < 3; group++)
                {
                    StreamingWriterTests.AddLongs(writer, 100);
                    Assert.Null(writer.FlushRowGroup());
                }
                writer.CloseStreaming();

                Assert.Equal(glibc, NativeHeap.Trim());
                Assert.Equal(glibc ? 4 : 0, NativeHeap.Trims - before);
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
