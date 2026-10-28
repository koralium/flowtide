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
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using ParquetSharp;
using Stowage;
using System.Buffers.Binary;
using System.Text.Json;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RewriteCopyTests
    {
        [Fact]
        public async Task ReorderedColumnsAndStructFieldsSurvive()
        {
            var s = new StructType(new List<Field>() { new Field("y", Int32Type.Default, true), new Field("x", StringType.Default, true) });
            var source = Write(new Schema.Builder()
                .Field(new Field("b", Int32Type.Default, true))
                .Field(new Field("a", StringType.Default, true))
                .Field(new Field("s", s, true))
                .Build(), Ints(10, 11, 12), Strings("a0", "a1", "a2"), Struct(s, null, Ints(1, 2, 3), Strings("x0", "x1", "x2")));
            var table = Table(
                Field("a", new Delta.StringType()),
                Field("b", new Delta.IntegerType()),
                Field("s", Table(Field("x", new Delta.StringType()), Field("y", new Delta.IntegerType()))));

            var output = await Rewrite(source, table, 1);

            var rows = await ReadRows(output.File, batch =>
            {
                var structs = (StructArray)batch.Column(2);
                return Enumerable.Range(0, batch.Length).Select(i =>
                    $"{((StringArray)batch.Column(0)).GetString(i)} {((Int32Array)batch.Column(1)).GetValue(i)} {((StringArray)structs.Fields[0]).GetString(i)} {((Int32Array)structs.Fields[1]).GetValue(i)}");
            });
            Assert.Equal(new[] { "a0 10 x0 1", "a2 12 x2 3" }, rows);
        }

        [Fact]
        public async Task SurvivorsAcrossBatchesKeepTheirValues()
        {
            // Read in batches of 64k rows, the last one shorter
            const int rows = 140_000;
            var source = Write(new Schema.Builder().Field(new Field("v", Int32Type.Default, true)).Build(), Ints(Enumerable.Range(0, rows).ToArray()));
            var table = Table(Field("v", new Delta.IntegerType()));
            var deleted = Enumerable.Range(0, rows).Where(x => x % 3 == 0 || x == 65_535 || x == 65_536).Select(x => (long)x).ToArray();

            var output = await Rewrite(source, table, deleted);

            var gone = deleted.ToHashSet();
            var expected = Enumerable.Range(0, rows).Where(x => !gone.Contains(x)).Select(x => (int?)x);
            Assert.Equal(expected, await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((Int32Array)batch.Column(0)).GetValue(i))));
        }

        [Fact]
        public async Task MicrosecondsKeepTheirMicroseconds()
        {
            var values = new long?[] { 1_000_001, null, -1_500, 1_700_000_000_123_456 };
            var source = Write(new Schema.Builder().Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true)).Build(), Micros(values));
            var table = Table(Field("ts", new Delta.TimestampType()));

            var output = await Rewrite(source, table);

            Assert.Equal(values, await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((TimestampArray)batch.Column(0)).GetValue(i))));
            Assert.Equal((ParquetSharp.TimeUnit.Micros, true), TimestampLeaf(output.File));
            // Bounds are the stored values floored to milliseconds
            Assert.Equal(-2, Milliseconds(output.Statistics.GetProperty("minValues").GetProperty("ts")));
            Assert.Equal(1_700_000_000_123, Milliseconds(output.Statistics.GetProperty("maxValues").GetProperty("ts")));
        }

        [Fact]
        public async Task AMicrosecondPastDateTimeOffsetLeavesNoBound()
        {
            // The second value floors to milliseconds whose ticks overflow
            var values = new long?[] { 0, 1_844_674_407_370_954_000, 1_000 };
            var source = Write(new Schema.Builder().Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true)).Build(), Micros(values));
            var table = Table(Field("ts", new Delta.TimestampType()));

            var output = await Rewrite(source, table, 2);

            Assert.Equal(values.Take(2), await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((TimestampArray)batch.Column(0)).GetValue(i))));
            Assert.Equal(0, Milliseconds(output.Statistics.GetProperty("minValues").GetProperty("ts")));
            Assert.False(output.Statistics.GetProperty("maxValues").TryGetProperty("ts", out _));
        }

        [Fact]
        public async Task AMicrosecondBeforeDateTimeOffsetLeavesNoBound()
        {
            // Unchecked, the first value's ticks wrap to a day after epoch
            var values = new long?[] { -1_844_674_320_970_955_000, 0, 1_000 };
            var source = Write(new Schema.Builder().Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true)).Build(), Micros(values));
            var table = Table(Field("ts", new Delta.TimestampType()));

            var output = await Rewrite(source, table, 2);

            Assert.Equal(values.Take(2), await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((TimestampArray)batch.Column(0)).GetValue(i))));
            Assert.False(output.Statistics.GetProperty("minValues").TryGetProperty("ts", out _));
            Assert.Equal(0, Milliseconds(output.Statistics.GetProperty("maxValues").GetProperty("ts")));
        }

        [Fact]
        public async Task AnInt96DayOneSurvives()
        {
            // 0001-01-01 00:00:00.000001, Julian day 1,721,426
            var source = TimestampPrecisionTests.WriteInt96(new Int96?[] { new Int96(1_000, 0, 1_721_426), null });
            var table = Table(Field("ts", new Delta.TimestampType()));

            var output = await Rewrite(source, table);

            Assert.Equal(new long?[] { -62_135_596_799_999_999, null }, await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((TimestampArray)batch.Column(0)).GetValue(i))));
            Assert.Equal((ParquetSharp.TimeUnit.Micros, true), TimestampLeaf(output.File));
        }

        [Fact]
        public async Task ANullStructAndANullMapReadBackNull()
        {
            var s = new StructType(new List<Field>() { new Field("x", Int32Type.Default, true) });
            var m = new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));
            var source = Write(new Schema.Builder().Field(new Field("s", s, true)).Field(new Field("m", m, true)).Build(),
                Struct(s, new[] { true, false, true }, Ints(1, 0, 3)),
                Map(m, new[] { true, false, true }, new[] { 0, 1, 1, 2 }, Strings("a", "c"), Ints(1, 3)));
            var table = Table(
                Field("s", Table(Field("x", new Delta.IntegerType()))),
                Field("m", new Delta.MapType(new Delta.StringType(), new Delta.IntegerType(), true)));

            var output = await Rewrite(source, table);

            var rows = await ReadRows(output.File, batch =>
            {
                var structs = (StructArray)batch.Column(0);
                var maps = (MapArray)batch.Column(1);
                return Enumerable.Range(0, batch.Length).Select(i =>
                {
                    var map = maps.IsNull(i) ? "null" : string.Join(",", Enumerable.Range(maps.ValueOffsets[i], maps.GetValueLength(i)).Select(j => $"{((StringArray)maps.Keys).GetString(j)}={((Int32Array)maps.Values).GetValue(j)}"));
                    return $"{(structs.IsNull(i) ? "null" : ((Int32Array)structs.Fields[0]).GetValue(i).ToString())} {map}";
                });
            });
            Assert.Equal(new[] { "1 a=1", "null null", "3 c=3" }, rows);
        }

        [Fact]
        public async Task AStructOutOfAlphabeticalOrderAndAMapSurviveADelete()
        {
            var s = new StructType(new List<Field>() { new Field("x", StringType.Default, true), new Field("y", Int32Type.Default, true) });
            var m = new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));
            var source = Write(new Schema.Builder().Field(new Field("s", s, true)).Field(new Field("m", m, true)).Build(),
                Struct(s, null, Strings("x0", "x1", "x2"), Ints(1, 2, 3)),
                Map(m, new[] { true, true, true }, new[] { 0, 2, 3, 5 }, Strings("a", "b", "c", "d", "e"), Ints(1, 2, 3, 4, 5)));
            // The table's struct is not in key order, row 2 shares its position with entry c
            var table = Table(
                Field("s", Table(Field("y", new Delta.IntegerType()), Field("x", new Delta.StringType()))),
                Field("m", new Delta.MapType(new Delta.StringType(), new Delta.IntegerType(), true)));

            var output = await Rewrite(source, table, 2);

            var rows = await ReadRows(output.File, batch =>
            {
                var structs = (StructArray)batch.Column(0);
                var maps = (MapArray)batch.Column(1);
                return Enumerable.Range(0, batch.Length).Select(i =>
                    $"{((Int32Array)structs.Fields[0]).GetValue(i)} {((StringArray)structs.Fields[1]).GetString(i)} " +
                    string.Join(",", Enumerable.Range(maps.ValueOffsets[i], maps.GetValueLength(i)).Select(j => $"{((StringArray)maps.Keys).GetString(j)}={((Int32Array)maps.Values).GetValue(j)}")));
            });
            Assert.Equal(new[] { "1 x0 a=1,b=2", "2 x1 c=3" }, rows);
        }

        [Fact]
        public async Task ADecimalOfThirtyDigitsSurvives()
        {
            var type = new Decimal128Type(38, 0);
            var large = Int128.Parse("1000000000000000000000000000000");
            var source = Write(new Schema.Builder().Field(new Field("d", type, true)).Build(), Decimals(type, large, 7));
            var table = Table(Field("d", new Delta.DecimalType(38, 0)));

            var output = await Rewrite(source, table);

            Assert.Equal(new Int128?[] { large, 7 }, await ReadRows(output.File, batch =>
            {
                var decimals = (Decimal128Array)batch.Column(0);
                return Enumerable.Range(0, batch.Length).Select(i => (Int128?)BinaryPrimitives.ReadInt128LittleEndian(decimals.GetBytes(i)));
            }));
            // A value outside System.Decimal leaves the column without bounds
            Assert.False(output.Statistics.GetProperty("minValues").TryGetProperty("d", out _));
            Assert.False(output.Statistics.GetProperty("maxValues").TryGetProperty("d", out _));
            Assert.Equal(0, output.Statistics.GetProperty("nullCount").GetProperty("d").GetInt32());
        }

        [Fact]
        public async Task MappedNestedRewritePreservesPhysicalFieldsAndIds()
        {
            var s = new StructType(new List<Field>() { new Field("col-x", Int32Type.Default, true, Id(2)), new Field("col-y", StringType.Default, true, Id(3)) });
            var source = Write(new Schema.Builder().Field(new Field("col-s", s, true, Id(1))).Build(), Struct(s, null, Ints(1, 2, 3), Strings("a", "b", "c")));
            var nested = Table(Field("x", new Delta.IntegerType(), "col-x", 2), Field("y", new Delta.StringType(), "col-y", 3));
            var table = Table(Field("s", nested, "col-s", 1));

            var output = await Rewrite(source, table, 0);

            Assert.Equal("col-s:1(col-x:2,col-y:3)", Footer(output.File));
            Assert.Equal(new[] { "2 b", "3 c" }, await ReadRows(output.File, batch =>
            {
                var structs = (StructArray)batch.Column(0);
                return Enumerable.Range(0, batch.Length).Select(i => $"{((Int32Array)structs.Fields[0]).GetValue(i)} {((StringArray)structs.Fields[1]).GetString(i)}");
            }));
            // Every nested level of the statistics uses physical names
            Assert.Equal("{\"col-x\":2,\"col-y\":\"b\"}", output.Statistics.GetProperty("minValues").GetProperty("col-s").GetRawText());
            Assert.Equal("{\"col-x\":3,\"col-y\":\"c\"}", output.Statistics.GetProperty("maxValues").GetProperty("col-s").GetRawText());
            Assert.Equal("{\"col-x\":0,\"col-y\":0}", output.Statistics.GetProperty("nullCount").GetProperty("col-s").GetRawText());
        }

        [Fact]
        public async Task NanosecondsBecomeExactMicroseconds()
        {
            var source = TimestampPrecisionTests.WriteNanos(new long?[] { 1_000, null, -2_000 });
            var table = Table(Field("ts", new Delta.TimestampType()));

            var output = await Rewrite(source, table);

            Assert.Equal(new long?[] { 1, null, -2 }, await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((TimestampArray)batch.Column(0)).GetValue(i))));
            Assert.Equal((ParquetSharp.TimeUnit.Micros, true), TimestampLeaf(output.File));
        }

        [Fact]
        public async Task AnInexactNanosecondIsNotCopied()
        {
            var source = TimestampPrecisionTests.WriteNanos(new long?[] { 1_000, 2_001 });
            var table = Table(Field("ts", new Delta.TimestampType()));

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Rewrite(source, table));

            Assert.Equal("Timestamp 2001 in Nanosecond cannot be written exactly in Microsecond", error.Message);
        }

        [Fact]
        public async Task MillisecondsStayMilliseconds()
        {
            var source = Write(new Schema.Builder().Field(new Field("ts", new TimestampType(TimeUnit.Millisecond, "UTC"), true)).Build(),
                new TimestampArray.Builder(new TimestampType(TimeUnit.Millisecond, "UTC")).Append(DateTimeOffset.FromUnixTimeMilliseconds(-1)).Build());
            var table = Table(Field("ts", new Delta.TimestampType()));

            var output = await Rewrite(source, table);

            Assert.Equal(new long?[] { -1 }, await ReadRows(output.File, batch => Enumerable.Range(0, batch.Length).Select(i => ((TimestampArray)batch.Column(0)).GetValue(i))));
            Assert.Equal((ParquetSharp.TimeUnit.Millis, true), TimestampLeaf(output.File));
            Assert.Equal(-1, Milliseconds(output.Statistics.GetProperty("minValues").GetProperty("ts")));
        }

        [Fact]
        public async Task DecimalsInRangeKeepExactBounds()
        {
            var type = new Decimal128Type(38, 2);
            var source = Write(new Schema.Builder().Field(new Field("d", type, true)).Build(), Decimals(type, -150, 12345));
            var table = Table(Field("d", new Delta.DecimalType(38, 2)));

            var output = await Rewrite(source, table, 1);

            Assert.Equal("-1.50", output.Statistics.GetProperty("minValues").GetProperty("d").GetRawText());
            Assert.Equal("-1.50", output.Statistics.GetProperty("maxValues").GetProperty("d").GetRawText());
        }

        [Fact]
        public async Task AWideDecimalWithTrailingZerosKeepsItsBounds()
        {
            // 10^20 at scale 10 has 31 digits, but drops its zeros
            var type = new Decimal128Type(38, 10);
            var source = Write(new Schema.Builder().Field(new Field("d", type, true)).Build(), Decimals(type, Int128.Parse("1000000000000000000000000000000")));
            var table = Table(Field("d", new Delta.DecimalType(38, 10)));

            var output = await Rewrite(source, table);

            Assert.Equal(100_000_000_000_000_000_000m, output.Statistics.GetProperty("maxValues").GetProperty("d").GetDecimal());
        }

        [Fact]
        public async Task ExactDecimalExtremaKeepTheirBounds()
        {
            // At scale 29 the middle value has no System.Decimal, the extrema do
            var type = new Decimal128Type(38, 29);
            var source = Write(new Schema.Builder().Field(new Field("d", type, true)).Build(), Decimals(type, 0, 1, Int128.Parse("100000000000000000000000000000")));
            var table = Table(Field("d", new Delta.DecimalType(38, 29)));

            var output = await Rewrite(source, table);

            Assert.Equal(0m, output.Statistics.GetProperty("minValues").GetProperty("d").GetDecimal());
            Assert.Equal(1m, output.Statistics.GetProperty("maxValues").GetProperty("d").GetDecimal());
        }

        [Theory]
        [InlineData(10, "1234567890123456789012345678901")]
        [InlineData(30, "15")]
        public async Task AWideDecimalWithoutAnExactValueHasNoBounds(int scale, string unscaled)
        {
            // Dropping a non-zero digit would make a bound below the value
            var type = new Decimal128Type(38, scale);
            var source = Write(new Schema.Builder().Field(new Field("d", type, true)).Build(), Decimals(type, Int128.Parse(unscaled)));
            var table = Table(Field("d", new Delta.DecimalType(38, scale)));

            var output = await Rewrite(source, table);

            Assert.False(output.Statistics.GetProperty("minValues").TryGetProperty("d", out _));
            Assert.False(output.Statistics.GetProperty("maxValues").TryGetProperty("d", out _));
        }

        [Fact]
        public void UnitsMustCoverEveryTimestampColumn()
        {
            var table = Table(Field("ts", new Delta.TimestampType()));
            var units = new[] { TimeUnit.Microsecond, TimeUnit.Microsecond };

            var error = Assert.Throws<InvalidOperationException>(() => new ParquetSharpWriter(table, new List<string>() { "ts" }, units: units));

            Assert.Equal("The table has other timestamp columns than the 2 units given", error.Message);
        }

        [Fact]
        public void ANewBatchStartsWithNoNullStructs()
        {
            var table = Table(Field("s", Table(Field("x", new Delta.IntegerType()))));
            var writer = new ParquetSharpWriter(table, new List<string>() { "s" });
            writer.NewBatch();
            writer.AddRow(NullRow());
            writer.NewBatch();

            using var batch = writer.GetRecordBatch();

            Assert.Equal(0, batch.Column(0).NullCount);
        }

        [Fact]
        public void StructValuesMatchFieldsByName()
        {
            var writer = new ParquetSharpWriteVisitor().Visit(Table(Field("x", new Delta.IntegerType()), Field("y", new Delta.IntegerType())));
            writer.NewBatch();

            // Upper case, out of order, a field left out
            writer.WriteValue(new FlowtideDotNet.Core.ColumnStore.DataValues.StructValue(FlowtideDotNet.Core.ColumnStore.DataValues.StructHeader.Create("Y"), new FlowtideDotNet.Core.ColumnStore.Int64Value(1)));
            var structs = (StructArray)writer.GetArray();

            // Fields are clipped to the struct, so check the children
            Assert.Equal((1, 1), (structs.Data.Children[0].Length, structs.Data.Children[1].Length));
            Assert.True(structs.Fields[0].IsNull(0));
            Assert.Equal(1, ((Int32Array)structs.Fields[1]).GetValue(0));
        }

        [Fact]
        public void AMapLeavingAFieldOutWritesItNull()
        {
            var writer = new ParquetSharpWriteVisitor().Visit(Table(Field("x", new Delta.IntegerType()), Field("y", new Delta.IntegerType())));
            writer.NewBatch();

            writer.WriteValue(new FlowtideDotNet.Core.ColumnStore.MapValue(new KeyValuePair<FlowtideDotNet.Core.ColumnStore.IDataValue, FlowtideDotNet.Core.ColumnStore.IDataValue>(new FlowtideDotNet.Core.ColumnStore.StringValue("y"), new FlowtideDotNet.Core.ColumnStore.Int64Value(1))));
            var structs = (StructArray)writer.GetArray();

            Assert.Equal((1, 1), (structs.Data.Children[0].Length, structs.Data.Children[1].Length));
            Assert.True(structs.Fields[0].IsNull(0));
            Assert.Equal(1, ((Int32Array)structs.Fields[1]).GetValue(0));
        }

        [Theory]
        [InlineData("z", "Struct field z does not exist in the table.")]
        [InlineData("X", "Struct field X is written more than once.")]
        public void AStructFieldTheTableLacksOrGetsTwiceIsAnError(string second, string message)
        {
            var writer = new ParquetSharpWriteVisitor().Visit(Table(Field("x", new Delta.IntegerType())));
            writer.NewBatch();

            // Before name matching, the extra field threw out of range
            var value = new FlowtideDotNet.Core.ColumnStore.DataValues.StructValue(FlowtideDotNet.Core.ColumnStore.DataValues.StructHeader.Create("x", second), new FlowtideDotNet.Core.ColumnStore.Int64Value(1), new FlowtideDotNet.Core.ColumnStore.Int64Value(2));
            Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => writer.WriteValue(value)).Message);
        }

        [Fact]
        public void StructFieldsDifferingOnlyByCaseAreAnError()
        {
            var error = Assert.Throws<InvalidOperationException>(() => new ParquetSharpWriteVisitor().Visit(Table(Field("a", new Delta.IntegerType()), Field("A", new Delta.IntegerType()))));

            // Which one is named depends on the culture's sort
            Assert.EndsWith("is in the table more than once.", error.Message);
        }

        [Fact]
        public void MapEntriesHaveTheValueTypeAndNoKeyOrder()
        {
            var writer = new ParquetSharpWriteVisitor().Visit(new Delta.MapType(new Delta.StringType(), new Delta.IntegerType(), true));
            writer.NewBatch();

            var map = (MapArray)writer.GetArray();

            Assert.False(((MapType)map.Data.DataType).KeySorted);
            Assert.IsType<Int32Type>(((StructType)map.KeyValues.Data.DataType).Fields[1].DataType);
        }

        private static FlowtideDotNet.Core.ColumnStore.TreeStorage.ColumnRowReference NullRow()
        {
            var column = FlowtideDotNet.Core.ColumnStore.Column.Create(FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance);
            column.Add(FlowtideDotNet.Core.ColumnStore.DataValues.NullValue.Instance);
            return new FlowtideDotNet.Core.ColumnStore.TreeStorage.ColumnRowReference() { referenceBatch = new FlowtideDotNet.Core.ColumnStore.EventBatchData(new FlowtideDotNet.Core.ColumnStore.IColumn[] { column }), RowIndex = 0 };
        }

        internal sealed record Rewritten(byte[] File, JsonElement Statistics);

        // Copies a source file's survivors as the sink's rewrite does
        internal static async Task<Rewritten> Rewrite(byte[] source, Delta.StructType table, params long[] deleted)
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/source.parquet"))
            {
                await write.WriteAsync(source);
            }
            var deleteVector = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
            foreach (var position in deleted)
            {
                deleteVector.Add(position);
            }

            using var file = await ParquetSharpReader.OpenDataFile(storage, "/t", "source.parquet", forDeletes: true);
            var binding = FileBinding.Create(table, file, "t");
            var writer = ParquetSharpWriter.ForRewrite(table, binding, null);
            writer.NewBatch();
            await writer.CopyFrom(file, binding, deleteVector, _ => false, () => Task.CompletedTask);
            var options = new JsonSerializerOptions();
            options.Converters.Add(new DeltaStatisticsConverter(table));
            var statistics = JsonDocument.Parse(JsonSerializer.Serialize(writer.GetStatistics(), options)).RootElement;

            await writer.WriteData(storage, "/t", "output.parquet");
            using var read = await storage.OpenRead("/t/output.parquet");
            using var copy = new MemoryStream();
            await read!.CopyToAsync(copy);
            return new Rewritten(copy.ToArray(), statistics);
        }

        internal static async Task<List<T>> ReadRows<T>(byte[] file, Func<RecordBatch, IEnumerable<T>> read)
        {
            var rows = new List<T>();
            using var reader = new ParquetSharp.Arrow.FileReader(new MemoryStream(file));
            using var batches = reader.GetRecordBatchReader();
            RecordBatch? batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    rows.AddRange(read(batch));
                }
            }
            return rows;
        }

        // The first leaf's unit and UTC flag
        internal static (ParquetSharp.TimeUnit, bool) TimestampLeaf(byte[] file, int column = 0)
        {
            using var reader = new ParquetFileReader(new MemoryStream(file));
            using var logical = reader.FileMetaData.Schema.Column(column).LogicalType;
            var timestamp = Assert.IsType<TimestampLogicalType>(logical);
            return (timestamp.TimeUnit, timestamp.IsAdjustedToUtc);
        }

        // Names and field ids of the footer's fields, nested in parentheses
        internal static string Footer(byte[] file)
        {
            using var reader = new ParquetFileReader(new MemoryStream(file));
            using var root = reader.FileMetaData.Schema.GroupNode;
            return string.Join(",", root.Fields.Select(Describe));
        }

        private static string Describe(ParquetSharp.Schema.Node node)
        {
            using (node)
            {
                var name = $"{node.Name}:{node.FieldId}";
                return node is ParquetSharp.Schema.GroupNode group && group.LogicalType is not ListLogicalType and not MapLogicalType
                    ? $"{name}({string.Join(",", group.Fields.Select(Describe))})"
                    : name;
            }
        }

        private static long Milliseconds(JsonElement value) => value.GetDateTimeOffset().ToUnixTimeMilliseconds();

        private static Dictionary<string, string> Id(int id) => new Dictionary<string, string>() { ["PARQUET:field_id"] = id.ToString() };

        private static Delta.StructType Table(params Delta.StructField[] fields) => new Delta.StructType(fields);

        private static Delta.StructField Field(string name, FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type, string? physicalName = null, int? fieldId = null)
        {
            return new Delta.StructField(name, type, true, new Dictionary<string, object>()) { PhysicalName = physicalName, FieldId = fieldId };
        }

        private static IArrowArray Ints(params int[] values) => new Int32Array.Builder().AppendRange(values).Build();

        private static IArrowArray Strings(params string[] values) => new StringArray.Builder().AppendRange(values).Build();

        private static IArrowArray Micros(long?[] values)
        {
            var builder = new ArrowBuffer.Builder<long>();
            var validity = new ArrowBuffer.BitmapBuilder();
            foreach (var value in values)
            {
                builder.Append(value ?? 0);
                validity.Append(value != null);
            }
            return new TimestampArray(new TimestampType(TimeUnit.Microsecond, "UTC"), builder.Build(), validity.Build(), values.Length, values.Count(x => x == null), 0);
        }

        private static IArrowArray Decimals(Decimal128Type type, params Int128[] values)
        {
            var builder = new Decimal128Array.Builder(type);
            Span<byte> bytes = stackalloc byte[16];
            foreach (var value in values)
            {
                BinaryPrimitives.WriteInt128LittleEndian(bytes, value);
                builder.Append(bytes);
            }
            return builder.Build();
        }

        private static IArrowArray Struct(StructType type, bool[]? valid, params IArrowArray[] children)
        {
            var length = children[0].Length;
            return valid == null
                ? new StructArray(type, length, children, ArrowBuffer.Empty, 0)
                : new StructArray(type, length, children, Validity(valid), valid.Count(x => !x));
        }

        private static IArrowArray Map(MapType type, bool[] valid, int[] offsets, IArrowArray keys, IArrowArray values)
        {
            var entries = new StructArray(type.KeyValueType, keys.Length, new[] { keys, values }, ArrowBuffer.Empty, 0);
            return new MapArray(type, valid.Length, new ArrowBuffer.Builder<int>().AppendRange(offsets).Build(), entries, Validity(valid), valid.Count(x => !x));
        }

        private static ArrowBuffer Validity(bool[] valid)
        {
            var builder = new ArrowBuffer.BitmapBuilder();
            foreach (var value in valid)
            {
                builder.Append(value);
            }
            return builder.Build();
        }

        internal static byte[] Write(Schema schema, params IArrowArray[] columns)
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
    }
}
