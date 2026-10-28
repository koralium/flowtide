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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using Stowage;
using System.Buffers;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class FileBindingTests
    {
        [Fact]
        public async Task ColumnsAndStructFieldsBindByName()
        {
            var s = new StructType(new List<Field>() { new Field("y", StringType.Default, true), new Field("x", Int32Type.Default, true) });
            var bytes = Write(new Schema.Builder()
                .Field(new Field("b", Int32Type.Default, true))
                .Field(new Field("extra", StringType.Default, true))
                .Field(new Field("a", Int32Type.Default, true))
                .Field(new Field("s", s, true))
                .Build(), Ints(10, 20), Strings("e1", "e2"), Ints(1, 2), Struct(s, Strings("y1", "y2"), Ints(5, 6)));
            var table = Table(
                Field("a", new Delta.IntegerType()),
                Field("b", new Delta.IntegerType()),
                Field("s", Table(Field("x", new Delta.IntegerType()), Field("y", new Delta.StringType()))),
                Field("missing", new Delta.StringType()));

            using var file = await Open(bytes);
            var binding = FileBinding.Create(table, file, "t");
            var rows = new List<string>();
            await foreach (var batch in file.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    // An unchanged column is the batch's own array
                    Assert.Same(batch.Column(1), rebound.Column(0));
                    var structs = (StructArray)rebound.Column(2);
                    for (int i = 0; i < rebound.Length; i++)
                    {
                        rows.Add($"{Int(rebound.Column(0), i)} {Int(rebound.Column(1), i)} {Int(structs.Fields[0], i)} {((StringArray)structs.Fields[1]).GetString(i)} {rebound.Column(3).IsNull(i)}");
                    }
                }
            }

            Assert.Equal(new[] { "1 10 5 y1 True", "2 20 6 y2 True" }, rows);
            // The unbound column is not read
            Assert.Equal(new[] { 0, 2, 3, 4 }, binding.LeafColumns);
        }

        [Theory]
        [InlineData("not nullable", "the file has no column gone, which is not nullable")]
        [InlineData("scale", "column a is decimal(10,2) in the file and decimal(10,3) in the table")]
        [InlineData("type", "column a is int32 in the file and string in the table")]
        public async Task ABindingErrorNamesTheColumn(string error, string message)
        {
            var bytes = Write(new Schema.Builder().Field(new Field("a", error == "scale" ? new Decimal128Type(10, 2) : Int32Type.Default, true)).Build(),
                error == "scale" ? Decimals(new Decimal128Type(10, 2)) : Ints(1));
            var table = error switch
            {
                "not nullable" => Table(Field("a", new Delta.IntegerType()), new Delta.StructField("gone", new Delta.IntegerType(), false, new Dictionary<string, object>())),
                "scale" => Table(Field("a", new Delta.DecimalType(10, 3))),
                _ => Table(Field("a", new Delta.StringType()))
            };

            using var file = await Open(bytes);
            var failure = Assert.Throws<InvalidOperationException>(() => FileBinding.Create(table, file, "t"));

            Assert.Contains("Delta table t cannot bind file data.parquet", failure.Message);
            Assert.Contains(message, failure.Message);
        }

        [Theory]
        [InlineData("x", null)]
        [InlineData("X", null)]
        [InlineData("COL-X", null)]
        [InlineData("x,col-x", "column s.x matches both col-x and x")]
        [InlineData("col-x,COL-X", "column s.x matches both col-x and COL-X")]
        [InlineData("x,X", "column s.x matches both x and X")]
        public async Task NestedFieldsFallBackToAUniqueLogicalName(string children, string? error)
        {
            var names = children.Split(',');
            var s = new StructType(names.Select(x => new Field(x, Int32Type.Default, true)).ToList());
            var bytes = Write(new Schema.Builder().Field(new Field("s", s, true)).Build(), Struct(s, names.Select((_, i) => Ints(i + 7)).ToArray()));
            var table = Table(Field("s", Table(Field("x", new Delta.IntegerType(), "col-x"))));

            using var file = await Open(bytes);
            if (error != null)
            {
                Assert.Contains(error, Assert.Throws<InvalidOperationException>(() => FileBinding.Create(table, file, "t")).Message);
                return;
            }
            Assert.Equal(new[] { "7" }, await ReadStructField(file, FileBinding.Create(table, file, "t"), 0));
        }

        [Theory]
        [InlineData("a")]
        [InlineData("A")]
        public async Task ASiblingsPhysicalNameIsNoFallback(string renamed)
        {
            // Field a was renamed b, then a was added again
            var table = Table(Field("s", Table(Field("b", new Delta.IntegerType(), renamed), Field("a", new Delta.IntegerType(), "col-new"))));
            var old = new StructType(new List<Field>() { new Field("a", Int32Type.Default, true) });
            var both = new StructType(new List<Field>() { new Field("a", Int32Type.Default, true), new Field("col-new", Int32Type.Default, true) });

            using var oldFile = await Open(Write(new Schema.Builder().Field(new Field("s", old, true)).Build(), Struct(old, Ints(3))));
            using var bothFile = await Open(Write(new Schema.Builder().Field(new Field("s", both, true)).Build(), Struct(both, Ints(3), Ints(4))));

            Assert.Equal(new[] { "3" }, await ReadStructField(oldFile, FileBinding.Create(table, oldFile, "t"), 0));
            Assert.Equal(new[] { "null" }, await ReadStructField(oldFile, FileBinding.Create(table, oldFile, "t"), 1));
            Assert.Equal(new[] { "3" }, await ReadStructField(bothFile, FileBinding.Create(table, bothFile, "t"), 0));
            Assert.Equal(new[] { "4" }, await ReadStructField(bothFile, FileBinding.Create(table, bothFile, "t"), 1));
        }

        [Fact]
        public async Task TopLevelColumnsBindOnlyTheirPhysicalName()
        {
            var table = Table(Field("a", new Delta.IntegerType(), "col-a"));
            using var both = await Open(Write(new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Field(new Field("col-a", Int32Type.Default, true)).Build(), Ints(1), Ints(2)));
            using var logical = await Open(Write(new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Build(), Ints(1)));

            Assert.Equal(new int?[] { 2 }, await ReadInts(both, FileBinding.Create(table, both, "t"), 0));
            Assert.Equal(new int?[] { null }, await ReadInts(logical, FileBinding.Create(table, logical, "t"), 0));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AFieldIdUnderAnotherNameIsAnIdModeBindingError(bool nested)
        {
            // Id mode resolves by field id, the file keeps a name from before a rename
            var a = new Delta.StructField("a", new Delta.IntegerType(), true, new Dictionary<string, object>()) { PhysicalName = "col-a", FieldId = 1 };
            var table = nested ? Table(Field("s", Table(a), "s")) : Table(a);
            var old = new Field("old_a", Int32Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "1" });
            var s = new StructType(new List<Field>() { old });
            using var file = await Open(nested
                ? Write(new Schema.Builder().Field(new Field("s", s, true)).Build(), Struct(s, Ints(7)))
                : Write(new Schema.Builder().Field(old).Build(), Ints(7)));

            // Read as null, a rewrite would store null for every survivor
            var error = Assert.Throws<InvalidOperationException>(() => FileBinding.Create(table, file, "t", byId: true));

            Assert.Contains($"column {(nested ? "s.a" : "a")} has field id 1, which the file stores as old_a", error.Message);
        }

        [Fact]
        public async Task NameModeDoesNotResolveByFieldId()
        {
            // Name mode reads a missing physical name as null, whatever the ids
            var table = Table(new Delta.StructField("a", new Delta.IntegerType(), true, new Dictionary<string, object>()) { PhysicalName = "col-a", FieldId = 1 });
            using var file = await Open(Write(new Schema.Builder().Field(new Field("old_a", Int32Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "1" })).Build(), Ints(7)));

            Assert.Equal(new int?[] { null }, await ReadInts(file, FileBinding.Create(table, file, "t"), 0));
        }

        [Fact]
        public async Task AFieldWithANewIdReadsAsNullBesideOtherIds()
        {
            // Column a was dropped and added again, so its id is new
            var b = new Delta.StructField("b", new Delta.IntegerType(), true, new Dictionary<string, object>()) { PhysicalName = "col-b", FieldId = 2 };
            var a = new Delta.StructField("a", new Delta.IntegerType(), true, new Dictionary<string, object>()) { PhysicalName = "col-new", FieldId = 3 };
            using var file = await Open(Write(new Schema.Builder()
                .Field(new Field("col-a", Int32Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "1" }))
                .Field(new Field("col-b", Int32Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "2" }))
                .Build(), Ints(7), Ints(8)));
            var binding = FileBinding.Create(Table(b, a), file, "t", byId: true);

            Assert.Equal(new int?[] { 8 }, await ReadInts(file, binding, 0));
            Assert.Equal(new int?[] { null }, await ReadInts(file, binding, 1));
        }

        [Fact]
        public async Task ListAndMapStructsFallBackToo()
        {
            var element = new StructType(new List<Field>() { new Field("x", Int32Type.Default, true) });
            var list = new ListType(new Field("element", element, true));
            var map = new MapType(new Field("key", StringType.Default, false), new Field("value", element, true));
            var nested = Table(Field("x", new Delta.IntegerType(), "col-x"));
            var table = Table(
                Field("l", new Delta.ArrayType() { ElementType = nested, ContainsNull = true }),
                Field("m", new Delta.MapType(new Delta.StringType(), nested, true)));
            using var file = await Open(Write(new Schema.Builder().Field(new Field("l", list, true)).Field(new Field("m", map, true)).Build(),
                List(list, Struct(element, Ints(1, 2)), new[] { 0, 2 }), Map(map, Strings("k"), Struct(element, Ints(3)))));
            var binding = FileBinding.Create(table, file, "t");

            await foreach (var batch in file.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    Assert.Equal(new int?[] { 1, 2 }, Elements(((ListArray)rebound.Column(0)).GetSlicedValues(0)));
                    Assert.Equal(new int?[] { 3 }, Elements(((MapArray)rebound.Column(1)).GetSlicedValues(0), 1));
                }
            }
        }

        [Fact]
        public async Task ADecimalBindsWhenOnlyThePrecisionDiffers()
        {
            var table = Table(Field("a", new Delta.DecimalType(18, 2)));
            using var file = await Open(Write(new Schema.Builder().Field(new Field("a", new Decimal128Type(10, 2), true)).Build(), Decimals(new Decimal128Type(10, 2))));
            var binding = FileBinding.Create(table, file, "t");

            await foreach (var batch in file.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    Assert.Equal(1.5m, ((Decimal128Array)rebound.Column(0)).GetValue(0));
                }
            }
        }

        [Fact]
        public async Task TimestampUnitsAndLeavesFollowTheTable()
        {
            var s = new StructType(new List<Field>() { new Field("t2", new TimestampType(TimeUnit.Nanosecond, "UTC"), true), new Field("t1", new TimestampType(TimeUnit.Microsecond, "UTC"), true) });
            var list = new ListType(new Field("element", new TimestampType(TimeUnit.Nanosecond, "UTC"), true));
            var map = new MapType(new Field("key", new TimestampType(TimeUnit.Microsecond, "UTC"), false), new Field("value", new TimestampType(TimeUnit.Nanosecond, "UTC"), true));
            var bytes = Write(new Schema.Builder()
                .Field(new Field("other", StringType.Default, true))
                .Field(new Field("l", list, true))
                .Field(new Field("s", s, true))
                .Field(new Field("a", Int32Type.Default, true))
                .Field(new Field("m", map, true))
                .Build(), Strings("o"), List(list, Timestamps(TimeUnit.Nanosecond), new[] { 0, 1 }), Struct(s, Timestamps(TimeUnit.Nanosecond), Timestamps(TimeUnit.Microsecond)), Ints(1),
                Map(map, Timestamps(TimeUnit.Microsecond), Timestamps(TimeUnit.Nanosecond)));
            var table = Table(
                Field("a", new Delta.IntegerType()),
                Field("s", Table(Field("t1", new Delta.TimestampType()), Field("t2", new Delta.TimestampType()))),
                Field("l", new Delta.ArrayType() { ElementType = new Delta.TimestampType(), ContainsNull = true }),
                Field("m", new Delta.MapType(new Delta.TimestampType(), new Delta.TimestampType(), true)),
                Field("gone", new Delta.TimestampType()));

            using var file = await Open(bytes);
            var binding = FileBinding.Create(table, file, "t");

            Assert.Equal(new[] { TimeUnit.Microsecond, TimeUnit.Nanosecond, TimeUnit.Nanosecond, TimeUnit.Microsecond, TimeUnit.Nanosecond, TimeUnit.Millisecond }, binding.TimestampUnits);
            Assert.Equal(new[] { new PrecisionLeaf(2, false, 2, 0), new PrecisionLeaf(1, false, 3, 1), new PrecisionLeaf(6, false, 3, 1) }, binding.PrecisionLeaves);
            Assert.Equal(new[] { 1, 2, 3, 4, 5, 6 }, binding.LeafColumns);
        }

        [Fact]
        public async Task AFileWithNoTableColumnKeepsItsRows()
        {
            var table = Table(Field("new", new Delta.IntegerType()));
            using var file = await Open(Write(new Schema.Builder().Field(new Field("old", Int32Type.Default, true)).Build(), Ints(11, 22, 33)));
            var binding = FileBinding.Create(table, file, "t");
            var rows = 0;

            await foreach (var batch in file.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    // An empty projection reads every column
                    Assert.Equal(1, batch.ColumnCount);
                    Assert.Equal(rebound.Length, rebound.Column(0).NullCount);
                    rows += rebound.Length;
                }
            }

            Assert.Empty(binding.LeafColumns);
            Assert.Equal(3, rows);
        }

        [Fact]
        public async Task MissingColumnsReadAsNullsOfTheirType()
        {
            var table = Table(
                Field("a", new Delta.IntegerType()),
                Field("st", Table(Field("t", new Delta.TimestampType()), Field("n", new Delta.IntegerType()))),
                Field("li", new Delta.ArrayType() { ElementType = new Delta.TimestampType(), ContainsNull = true }),
                Field("mp", new Delta.MapType(new Delta.StringType(), new Delta.TimestampType(), true)),
                Field("bo", new Delta.BooleanType()),
                Field("bi", new Delta.BinaryType()),
                Field("ts", new Delta.TimestampType()));
            var schema = new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Build();
            using var file = await Open(Write(schema, Ints(1, 2)));
            var binding = FileBinding.Create(table, file, "t");
            using var batch = new RecordBatch(schema, new[] { Ints(1, 2) }, 2);
            var allocator = new CountingAllocator();

            using (var rebound = binding.Rebind(batch, allocator))
            {
                var st = (StructArray)rebound.Column(1);
                var li = (ListArray)rebound.Column(2);
                var mp = (MapArray)rebound.Column(3);
                var columns = new IArrowArray[] { st, st.Fields[0], st.Fields[1], li, mp, (BooleanArray)rebound.Column(4), (BinaryArray)rebound.Column(5), (TimestampArray)rebound.Column(6) };
                Assert.All(columns, x => Assert.Equal((2, 2), (x.Length, x.NullCount)));
                Assert.All(columns, x => Assert.True(x.IsNull(0) && x.IsNull(1)));
                Assert.IsType<TimestampArray>(st.Fields[0]);
                Assert.Equal((0, 0), (li.Values.Length, mp.KeyValues.Length));
                Assert.Equal(new[] { 0, 0, 0 }, li.ValueOffsets.ToArray());
            }

            // Missing timestamps keep the default unit
            Assert.Equal(Enumerable.Repeat(TimeUnit.Millisecond, 4), binding.TimestampUnits);
            Assert.Equal(allocator.Allocated, allocator.Released);
            Assert.Equal(0, allocator.Twice);
        }

        [Fact]
        public async Task TimestampsKeepTheirUnitAndPrecisionLeaves()
        {
            var table = Table(Field("ts", new Delta.TimestampType()));
            using var micros = await Open(Write(new Schema.Builder().Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true)).Build(), Timestamps(TimeUnit.Microsecond)));
            using var int96 = await Open(TimestampPrecisionTests.WriteInt96(new ParquetSharp.Int96?[] { new ParquetSharp.Int96(0, 0, 2_440_588) }));
            using var nanos = await Open(TimestampPrecisionTests.WriteNanos(new long?[] { 1 }));

            var microBinding = FileBinding.Create(table, micros, "t");
            var int96Binding = FileBinding.Create(table, int96, "t");
            var nanoBinding = FileBinding.Create(table, nanos, "t");

            Assert.Equal(new[] { TimeUnit.Microsecond }, microBinding.TimestampUnits);
            Assert.Empty(microBinding.PrecisionLeaves);
            // INT96 reads as microseconds on the delete path
            Assert.Equal(new[] { TimeUnit.Microsecond }, int96Binding.TimestampUnits);
            Assert.Equal(new[] { new PrecisionLeaf(0, true, 1, 0) }, int96Binding.PrecisionLeaves);
            Assert.Equal(new[] { TimeUnit.Nanosecond }, nanoBinding.TimestampUnits);
            Assert.Equal(new[] { new PrecisionLeaf(0, false, 1, 0) }, nanoBinding.PrecisionLeaves);
        }

        [Fact]
        public async Task ReboundViewsKeepOffsetsAndReleaseOnce()
        {
            // Each nested shape lacks the table's gone field
            var element = new StructType(new List<Field>() { new Field("x", Int32Type.Default, true) });
            var list = new ListType(new Field("element", element, true));
            var map = new MapType(new Field("key", StringType.Default, false), new Field("value", element, true));
            var inner = new StructType(new List<Field>() { new Field("inner", element, true) });
            var schema = new Schema.Builder().Field(new Field("s", element, true)).Field(new Field("l", list, true)).Field(new Field("m", map, true)).Field(new Field("n", inner, true)).Build();
            var nested = Table(Field("x", new Delta.IntegerType()), Field("gone", new Delta.StringType()));
            var table = Table(
                Field("s", nested),
                Field("l", new Delta.ArrayType() { ElementType = nested, ContainsNull = true }),
                Field("m", new Delta.MapType(new Delta.StringType(), nested, true)),
                Field("n", Table(Field("inner", nested))));
            IArrowArray[] Columns(MemoryAllocator allocator) => new[]
            {
                Struct(element, Ints(allocator, 1, null, 3)),
                List(list, Struct(element, Ints(allocator, 10, 11, null, 13)), new[] { 0, 1, 2, 4 }, allocator),
                Map(map, Strings("k0", "k1", "k2"), Struct(element, Ints(allocator, 20, null, 22)), allocator),
                // The inner struct starts at its second row
                Struct(inner, ArrowArrayFactory.Slice(Struct(element, Ints(allocator, 30, 31, 32, 33)), 1, 3))
            };
            using var file = await Open(Write(schema, Columns(MemoryAllocator.Default.Value)));
            var binding = FileBinding.Create(table, file, "t");

            foreach (var viewFirst in new[] { true, false })
            {
                // Rows 1 and 2, so every column has an offset
                var sources = new CountingAllocator();
                var source = new RecordBatch(schema, Columns(sources).Select(x => ArrowArrayFactory.Slice(x, 1, 2)), 2);
                var allocator = new CountingAllocator();
                var view = binding.Rebind(source, allocator);
                var s = (StructArray)view.Column(0);
                var l = (ListArray)view.Column(1);
                var m = (MapArray)view.Column(2);
                var n = (StructArray)view.Column(3);

                // Fields come sliced by the parent's offset
                Assert.Equal(new int?[] { null, 3 }, new[] { Int(s.Fields[0], 0), Int(s.Fields[0], 1) });
                // The made-up child covers offset plus length
                Assert.Equal(1, s.Offset);
                Assert.Equal(s.Offset + s.Length, s.Data.Children[1].Length);
                Assert.True(s.Fields[1].IsNull(0) && s.Fields[1].IsNull(1));
                Assert.Equal(new int?[] { 11, null, 13 }, Elements(l.GetSlicedValues(0)).Concat(Elements(l.GetSlicedValues(1))).ToArray());
                Assert.Equal(new int?[] { null, 22 }, Elements(m.GetSlicedValues(0), 1).Concat(Elements(m.GetSlicedValues(1), 1)).ToArray());
                // Gone children under list and map values are all null
                var listGone = ((StructArray)l.Values).Fields[1];
                var mapGone = ((StructArray)m.Values).Fields[1];
                Assert.Equal((4, 4, 3, 3), (listGone.Length, listGone.NullCount, mapGone.Length, mapGone.NullCount));
                // A nested struct keeps its own offset
                var innerView = (StructArray)n.Fields[0];
                Assert.Equal(new int?[] { 32, 33 }, new[] { Int(innerView.Fields[0], 0), Int(innerView.Fields[0], 1) });
                Assert.Equal(4, n.Data.Children[0].Children[1].Length);
                Assert.True(innerView.Fields[1].IsNull(0) && innerView.Fields[1].IsNull(1));
                Assert.True(allocator.Allocated > 0);

                if (viewFirst)
                {
                    view.Dispose();
                    // The source still reads after the view is gone
                    Assert.Equal(0, sources.Released);
                    Assert.Equal(3, Int(((StructArray)source.Column(0)).Fields[0], 1));
                    source.Dispose();
                }
                else
                {
                    source.Dispose();
                    view.Dispose();
                }
                view.Dispose();
                Assert.Equal(allocator.Allocated, allocator.Released);
                Assert.Equal(0, allocator.Twice);
                Assert.Equal(sources.Allocated, sources.Released);
                Assert.Equal(0, sources.Twice);
            }
        }

        [Fact]
        public async Task ADroppedFieldWithAnIdIsNoFallback()
        {
            // Field x, physical name x, was dropped and added again
            var table = Table(Field("s", Table(Field("y", new Delta.IntegerType(), "y"), Field("x", new Delta.IntegerType(), "col-new"))));
            var s = new StructType(new List<Field>()
            {
                new Field("x", Int32Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "2" }),
                new Field("y", Int32Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "3" })
            });
            using var file = await Open(Write(new Schema.Builder().Field(new Field("s", s, true)).Build(), Struct(s, Ints(3), Ints(4))));

            // The dropped values must not reach the new field
            Assert.Equal(new[] { "4" }, await ReadStructField(file, FileBinding.Create(table, file, "t"), 0));
            Assert.Equal(new[] { "null" }, await ReadStructField(file, FileBinding.Create(table, file, "t"), 1));
        }

        [Fact]
        public async Task AFailedNullColumnReleasesWhatItAllocated()
        {
            var schema = new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Build();
            var table = Table(Field("a", new Delta.IntegerType()), Field("missing", Table(Field("x", new Delta.IntegerType()), Field("y", new Delta.StringType()))));
            using var file = await Open(Write(schema, Ints(1, 2)));
            var binding = FileBinding.Create(table, file, "t");
            using var batch = new RecordBatch(schema, new[] { Ints(1, 2) }, 2);
            // The third buffer fails, inside the missing struct
            var allocator = new CountingAllocator(failOn: 3);

            Assert.Throws<OutOfMemoryException>(() => binding.Rebind(batch, allocator));
            Assert.Equal(allocator.Allocated, allocator.Released);
            Assert.Equal(0, allocator.Twice);
        }

        [Fact]
        public async Task ClosingAFileReleasesItsFooterMetadata()
        {
            var table = Table(Field("ts", new Delta.TimestampType()));
            ParquetSharp.FileMetaData metadata;
            using (var file = await Open(Write(new Schema.Builder().Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true)).Build(), Timestamps(TimeUnit.Microsecond))))
            {
                metadata = file.LowLevel.FileMetaData;
                Assert.Same(file.LowLevel, file.LowLevel);
                FileBinding.Create(table, file, "t");
                Assert.Same(metadata, file.LowLevel.FileMetaData);
            }

            // A released native handle throws on use
            Assert.Throws<NullReferenceException>(() => metadata.NumRows);
        }

        private static async Task<List<string>> ReadStructField(ParquetDataFile file, FileBinding binding, int field)
        {
            var values = new List<string>();
            await foreach (var batch in file.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    var child = ((StructArray)rebound.Column(0)).Fields[field];
                    for (int i = 0; i < rebound.Length; i++)
                    {
                        values.Add(child.IsNull(i) ? "null" : Int(child, i).ToString()!);
                    }
                }
            }
            return values;
        }

        private static async Task<List<int?>> ReadInts(ParquetDataFile file, FileBinding binding, int column)
        {
            var values = new List<int?>();
            await foreach (var batch in file.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, MemoryAllocator.Default.Value))
                {
                    for (int i = 0; i < rebound.Length; i++)
                    {
                        values.Add(Int(rebound.Column(column), i));
                    }
                }
            }
            return values;
        }

        private static IEnumerable<int?> Elements(IArrowArray entries, int field = 0)
        {
            var structs = (StructArray)entries;
            var child = structs.Fields[field] is StructArray inner ? inner.Fields[0] : structs.Fields[field];
            return Enumerable.Range(0, structs.Length).Select(i => Int(child, i));
        }

        private static int? Int(IArrowArray array, int index) => ((Int32Array)array).GetValue(index);

        private static Delta.StructType Table(params Delta.StructField[] fields) => new Delta.StructType(fields);

        private static Delta.StructField Field(string name, FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type, string? physicalName = null)
        {
            return new Delta.StructField(name, type, true, new Dictionary<string, object>()) { PhysicalName = physicalName };
        }

        private static IArrowArray Ints(params int[] values) => new Int32Array.Builder().AppendRange(values).Build();

        private static IArrowArray Ints(MemoryAllocator allocator, params int?[] values)
        {
            var builder = new Int32Array.Builder();
            foreach (var value in values)
            {
                builder.Append(value);
            }
            return builder.Build(allocator);
        }

        private static IArrowArray Strings(params string[] values) => new StringArray.Builder().AppendRange(values).Build();

        private static IArrowArray Decimals(Decimal128Type type) => new Decimal128Array.Builder(type).Append(1.5m).Build();

        private static IArrowArray Timestamps(TimeUnit unit) => new TimestampArray.Builder(new TimestampType(unit, "UTC")).Append(DateTimeOffset.UnixEpoch).Build();

        private static IArrowArray Struct(StructType type, params IArrowArray[] children)
        {
            return new StructArray(type, children[0].Length, children, ArrowBuffer.Empty, 0);
        }

        private static IArrowArray List(ListType type, IArrowArray values, int[] offsets, MemoryAllocator? allocator = null)
        {
            var buffer = new ArrowBuffer.Builder<int>().AppendRange(offsets).Build(allocator);
            return new ListArray(type, offsets.Length - 1, buffer, values, ArrowBuffer.Empty, 0);
        }

        private static IArrowArray Map(MapType type, IArrowArray keys, IArrowArray values, MemoryAllocator? allocator = null)
        {
            var entries = new StructArray(type.KeyValueType, keys.Length, new[] { keys, values }, ArrowBuffer.Empty, 0);
            var offsets = new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, keys.Length + 1)).Build(allocator);
            return new MapArray(type, keys.Length, offsets, entries, ArrowBuffer.Empty, 0);
        }

        private static byte[] Write(Schema schema, params IArrowArray[] columns)
        {
            using var batch = new RecordBatch(schema, columns, columns[0].Length);
            using var stream = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            return stream.ToArray();
        }

        private static async Task<ParquetDataFile> Open(byte[] bytes)
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/data.parquet"))
            {
                await write.WriteAsync(bytes);
            }
            return await ParquetSharpReader.OpenDataFile(storage, "/t", "data.parquet", forDeletes: true);
        }

        // Counts allocations and releases, failing one when asked
        private sealed class CountingAllocator : MemoryAllocator
        {
            private readonly int _failOn;
            public int Allocated;
            public int Released;
            public int Twice;

            public CountingAllocator(int failOn = 0)
            {
                _failOn = failOn;
            }

            protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
            {
                if (Allocated + 1 == _failOn)
                {
                    throw new OutOfMemoryException("Allocation failed in the test");
                }
                bytesAllocated = length;
                Allocated++;
                return new Owner(this, length);
            }

            private sealed class Owner : IMemoryOwner<byte>
            {
                private readonly CountingAllocator _allocator;
                private readonly byte[] _bytes;
                private bool _released;

                public Owner(CountingAllocator allocator, int length)
                {
                    _allocator = allocator;
                    _bytes = new byte[length];
                }

                public Memory<byte> Memory => _bytes;

                public void Dispose()
                {
                    if (_released)
                    {
                        _allocator.Twice++;
                        return;
                    }
                    _released = true;
                    _allocator.Released++;
                }
            }
        }
    }
}
