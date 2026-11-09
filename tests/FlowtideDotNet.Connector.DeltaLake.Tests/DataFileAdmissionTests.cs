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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using ParquetSharp;
using Stowage;
using System.Text.Json;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DataFileAdmissionTests : IDisposable
    {
        private readonly string _directory;

        public DataFileAdmissionTests()
        {
            _directory = Path.Join(Path.GetTempPath(), "flowtide-admission-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        [Theory]
        [InlineData("all_primitive_types")]
        [InlineData("nested_types")]
        [InlineData("table_with_column_mapping")]
        [InlineData("table_with_deletion_logs")]
        [InlineData("simple_table_with_cdc")]
        public void PreflightBindsTestDataAsFileBindingDoes(string table)
        {
            var (schema, files) = TestData(table);

            Assert.NotEmpty(files);
            Assert.All(files, path => AssertBindsAlike(schema, path));
        }

        [Fact]
        public void PreflightBindsSyntheticShapesAsFileBindingDoes()
        {
            var inner = new StructType(new List<Field>() { new Field("b", Int32Type.Default, true), new Field("A", StringType.Default, true) });
            var path = Write("shapes.parquet", new Schema.Builder()
                .Field(new Field("s", inner, true))
                .Field(new Field("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"), true))
                .Field(new Field("l", new ListType(new Field("element", inner, true)), true))
                .Build(), 3);
            var innerTable = Table(Field("a", new Delta.StringType()), Field("missing", new Delta.LongType()), Field("b", new Delta.IntegerType()));

            AssertBindsAlike(Table(Field("s", innerTable), Field("ts", new Delta.TimestampType()), Field("l", new Delta.ArrayType() { ElementType = innerTable, ContainsNull = true }), Field("gone", new Delta.TimestampType())), path);
            AssertBindsAlike(Table(Field("S", new Delta.LongType())), path);
            AssertBindsAlike(Table(Field("ts", new Delta.LongType())), path);
        }

        [Fact]
        public void PreflightBindsNameCollisionsAsFileBindingDoes()
        {
            foreach (var children in new[] { "x", "X", "COL-X", "x,col-x", "col-x,COL-X", "x,X" })
            {
                var inner = new StructType(children.Split(',').Select(x => new Field(x, Int32Type.Default, true)).ToList());
                AssertBindsAlike(Table(Field("s", Table(Field("x", new Delta.IntegerType(), "col-x")))), Write(Name(), Schema(new Field("s", inner, true)), 1));
            }
            foreach (var renamed in new[] { "a", "A" })
            {
                var table = Table(Field("s", Table(Field("b", new Delta.IntegerType(), renamed), Field("a", new Delta.IntegerType(), "col-new"))));
                AssertBindsAlike(table, Write(Name(), Schema(new Field("s", Struct(new Field("a", Int32Type.Default, true)), true)), 1));
                AssertBindsAlike(table, Write(Name(), Schema(new Field("s", Struct(new Field("a", Int32Type.Default, true), new Field("col-new", Int32Type.Default, true)), true)), 1));
            }
            var top = Table(Field("a", new Delta.IntegerType(), "col-a"));
            AssertBindsAlike(top, Write(Name(), Schema(new Field("a", Int32Type.Default, true), new Field("col-a", Int32Type.Default, true)), 1));
            AssertBindsAlike(top, Write(Name(), Schema(new Field("a", Int32Type.Default, true)), 1));
            var old = new Field("old_a", Int32Type.Default, true, Id(1));
            foreach (var byId in new[] { false, true })
            {
                AssertBindsAlike(Table(Field("a", new Delta.IntegerType(), "col-a", 1)), Write(Name(), Schema(old), 1), byId);
                AssertBindsAlike(Table(Field("s", Table(Field("a", new Delta.IntegerType(), "col-a", 1)), "s")), Write(Name(), Schema(new Field("s", Struct(old), true)), 1), byId);
            }
            var dropped = Struct(new Field("x", Int32Type.Default, true, Id(2)), new Field("y", Int32Type.Default, true, Id(3)));
            AssertBindsAlike(Table(Field("s", Table(Field("y", new Delta.IntegerType(), "y"), Field("x", new Delta.IntegerType(), "col-new")))), Write(Name(), Schema(new Field("s", dropped, true)), 1));
            var element = Struct(new Field("x", Int32Type.Default, true));
            var mapped = Table(Field("x", new Delta.IntegerType(), "col-x"));
            AssertBindsAlike(Table(Field("l", new Delta.ArrayType() { ElementType = mapped, ContainsNull = true }), Field("m", new Delta.MapType(new Delta.StringType(), mapped, true))),
                Write(Name(), Schema(new Field("l", new ListType(new Field("element", element, true)), true), new Field("m", new MapType(new Field("key", StringType.Default, false), new Field("value", element, true)), true)), 2, elements: 2));
            var nanos = new TimestampType(TimeUnit.Nanosecond, "UTC");
            var micros = new TimestampType(TimeUnit.Microsecond, "UTC");
            AssertBindsAlike(Table(Field("s", Table(Field("t1", new Delta.TimestampType()), Field("t2", new Delta.TimestampType()))), Field("l", new Delta.ArrayType() { ElementType = new Delta.TimestampType(), ContainsNull = true }), Field("m", new Delta.MapType(new Delta.TimestampType(), new Delta.TimestampType(), true))),
                Write(Name(), Schema(new Field("l", new ListType(new Field("element", nanos, true)), true), new Field("s", Struct(new Field("t2", nanos, true), new Field("t1", micros, true)), true), new Field("m", new MapType(new Field("key", micros, false), new Field("value", nanos, true)), true)), 2, elements: 2));
        }

        [Theory]
        [InlineData("all_primitive_types")]
        [InlineData("nested_types")]
        [InlineData("table_with_column_mapping")]
        [InlineData("table_with_deletion_logs")]
        [InlineData("simple_table_with_cdc")]
        public void TestDataIsAdmittedAtEveryTarget(string table)
        {
            var (schema, files) = TestData(table);

            foreach (var target in new[] { 8L << 20, 16L << 20, 32L << 20, 128L << 20 })
            {
                var settings = new AdmissionSettings(CompactionBin.FlushBytesFor(target), Compression.Zstd);
                Assert.All(files, path => Assert.Null(Admit(path, schema, new JobMemory(), settings, new List<InputSummary>()).Reason));
            }
        }

        [Fact]
        public void ByteArraysWithoutSizeStatisticsFallBackToTheirPages()
        {
            var schema = Schema(new Field("s", StringType.Default, true));
            var table = Table(Field("s", new Delta.StringType()));
            var memory = new JobMemory();

            var (dictionary, _) = DataFileAdmission.Preflight(Write(Name(), schema, 10_000, properties: b => b.SetSizeStatisticsLevel(SizeStatisticsLevel.None)), table, false, memory);
            var (plain, _) = DataFileAdmission.Preflight(Write(Name(), schema, 10_000, properties: b => b.SetSizeStatisticsLevel(SizeStatisticsLevel.None).DisableDictionary()), table, false, memory);
            var (indices, values) = (dictionary.Groups[0][0], plain.Groups[0][0]);

            Assert.Equal((null, null), (indices.Unencoded, values.Unencoded));
            Assert.Equal((true, false), (indices.HasDictionary, values.HasDictionary));
            Assert.Equal(indices.Values * indices.MaxValue, DataFileScan.Bytes(indices));
            Assert.Equal(values.Page, values.MaxValue);
            Assert.Equal(values.Uncompressed, DataFileScan.Bytes(values));
            Assert.True(dictionary.ReadBatch(0, 10_000).Batch >= 80_000);
            memory.Release(dictionary.ChargedBytes + plain.ChargedBytes);
        }

        [Fact]
        public void SizeStatisticsCountOnDictionaryChunks()
        {
            var schema = Schema(new Field("s", StringType.Default, true));
            var table = Table(Field("s", new Delta.StringType()));
            var memory = new JobMemory();

            var (plain, _) = DataFileAdmission.Preflight(Write(Name(), schema, 10_000, properties: b => b.DisableDictionary()), table, false, memory);
            var (foreign, _) = DataFileAdmission.Preflight(Write(Name(), schema, 10_000, properties: b => b.CreatedBy("parquet-mr version 1.15.1")), table, false, memory);
            var (ours, _) = DataFileAdmission.Preflight(Write(Name(), schema, 10_000), table, false, memory);

            Assert.Null(plain.Groups[0][0].Unencoded);
            Assert.Equal(plain.Groups[0][0].Uncompressed, DataFileScan.Bytes(plain.Groups[0][0]));
            Assert.Equal((80_000L, 80_000L), (foreign.Groups[0][0].Unencoded, ours.Groups[0][0].Unencoded));
            memory.Release(plain.ChargedBytes + foreign.ChargedBytes + ours.ChargedBytes);
        }

        [Theory]
        [InlineData("byte", 1_753_152)]
        [InlineData("date", 2_146_368)]
        public void CopiedLeavesKeepTheReadersValuesAtCapacity(string type, long expected)
        {
            IArrowType arrow = type == "byte" ? Int8Type.Default : Date32Type.Default;
            SchemaBaseType delta = type == "byte" ? new Delta.ByteType() : new Delta.DateType();
            var path = Write(Name(), Schema(new Field("a", arrow, true), new Field("b", arrow, true)), 70_000);
            var memory = new JobMemory();

            var (scan, _) = DataFileAdmission.Preflight(path, Table(Field("a", delta), Field("b", delta)), false, memory);

            Assert.Equal(expected, scan.ReadBatch(0, 65_536).RecordReader);
            memory.Release(scan.ChargedBytes);
        }

        [Fact]
        public void ShortListsOfBooleansFitTheDefaultTarget()
        {
            var path = Write(Name(), Schema(new Field("id", Int64Type.Default, true), new Field("l", new ListType(new Field("element", BooleanType.Default, true)), true)), 2_000, elements: 5);
            var table = Table(Field("id", new Delta.LongType()), Field("l", new Delta.ArrayType() { ElementType = new Delta.BooleanType(), ContainsNull = true }));

            Assert.Null(Admit(path, table, new JobMemory(), new AdmissionSettings(8L << 20, Compression.Zstd), new List<InputSummary>()).Reason);
        }

        [Fact]
        public void ShortStringsFitTheDefaultTarget()
        {
            var path = Write(Name(), Schema(new Field("s", StringType.Default, true)), 100_000);

            var admission = Admit(path, Table(Field("s", new Delta.StringType())), new JobMemory(), new AdmissionSettings(8L << 20, Compression.Zstd), new List<InputSummary>());

            Assert.True(admission.Reason == null, admission.Detail);
        }

        [Fact]
        public void ShortListsOfLongsFitTheDefaultTarget()
        {
            var path = Write(Name(), Schema(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true)), 100_000, elements: 5);
            var table = Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true }));

            var admission = Admit(path, table, new JobMemory(), new AdmissionSettings(8L << 20, Compression.Zstd), new List<InputSummary>());

            Assert.True(admission.Reason == null, admission.Detail);
        }

        [Fact]
        public void DenseBooleanListsFitAtTheLevelCap()
        {
            var path = Write(Name(), Schema(new Field("l", new ListType(new Field("element", BooleanType.Default, true)), true)), 20_000, elements: 25);
            var table = Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.BooleanType(), ContainsNull = true }));

            Assert.Null(Admit(path, table, new JobMemory(), new AdmissionSettings(8L << 20, Compression.Zstd), new List<InputSummary>()).Reason);
        }

        [Fact]
        public void LongListRowsFitAtTheLevelCap()
        {
            var path = Write(Name(), Schema(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true)), 5, elements: 200_000);
            var table = Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true }));

            Assert.Null(Admit(path, table, new JobMemory(), new AdmissionSettings(8L << 20, Compression.Zstd), new List<InputSummary>()).Reason);
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(128)]
        public void TablesOfOnlyBooleanListsFitEveryTarget(int target)
        {
            var (path, table) = Fixture("narrow");

            Assert.Null(Admit(path, table, new JobMemory(), new AdmissionSettings(CompactionBin.FlushBytesFor((long)target << 20), Compression.Zstd), new List<InputSummary>()).Reason);
        }

        [Fact]
        public void TheWorkedExampleSynthesizesNinetyNineMebibytes()
        {
            var element = new StructType(new List<Field>() { new Field("a", Int32Type.Default, true) });
            var path = Write("example.parquet", new Schema.Builder().Field(new Field("l", new ListType(new Field("element", element, true)), true)).Build(), 1, elements: 800_000);
            var wide = Table(Enumerable.Range(0, 16).Select(x => Field($"w{x}", new Delta.LongType())).ToArray());
            var table = Table(Field("l", new Delta.ArrayType() { ElementType = Table(Field("a", new Delta.IntegerType()), Field("s", wide)), ContainsNull = true }));
            var memory = new JobMemory();

            var (scan, plan) = DataFileAdmission.Preflight(path, table, false, memory);
            var summary = DataFileAdmission.Summarize(scan, plan, new OutputShape(table));
            var (batch, _, _) = scan.ReadBatch(0, 1);

            Assert.InRange(batch, 3_200_000, 3_600_000);
            Assert.InRange(summary.OutBatch[InputSummary.Candidates - 1], 105_000_000, 112_000_000);
            Assert.InRange(summary.Floor.SynthOut, 99L << 20, 100L << 20);
            Assert.True(summary.ReadPeak[InputSummary.Candidates - 1] > 99L << 20);
            Assert.Equal(new[] { "l.element.s" }, plan.MissingPaths);
            memory.Release(scan.ChargedBytes);
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData("codec", "unsupported-codec")]
        [InlineData("encoding", "unsupported-encoding")]
        [InlineData("deep", "metadata-bound")]
        [InlineData("dictionary", "dictionary-bound")]
        [InlineData("value", "row-group-compressed")]
        [InlineData("repeated", "repeated-chunk")]
        [InlineData("subtree", "synthesis-bound")]
        [InlineData("output", "synthesis-bound")]
        public void DeclinesNameTheirReason(string fixture, string expected)
        {
            var (path, table) = Fixture(fixture);
            var memory = new JobMemory();
            var settings = new AdmissionSettings(1L << 20, Compression.Zstd) { NativeCap = 48L << 20 };

            var admission = Admit(path, table, memory, settings, new List<InputSummary>());

            Assert.True(expected == (admission.Reason == null ? null : CompactionReasons.Name(admission.Reason.Value)), admission.Detail);
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void TheRunningPrefixHalvesTheNewInputThenDropsIt()
        {
            var output = new OutputShape(Table(Field("v", new Delta.LongType())));
            var settings = new AdmissionSettings(1L << 20, Compression.Zstd) { NativeCap = 1L << 30 };
            var earlier = Summary(model: 30L << 20, outBatch: 1L << 20);
            earlier.Candidate = 0;
            var halved = Summary(model: 0, outBatch: 16L << 20, shrinking: true);
            var stuck = Summary(model: 0, outBatch: 16L << 20);

            var alone = DataFileAdmission.Admit(System.Array.Empty<InputSummary>(), halved, output, settings);
            var admitted = DataFileAdmission.Admit(new[] { earlier }, halved, output, settings);
            var dropped = DataFileAdmission.Admit(new[] { earlier }, stuck, output, settings);

            Assert.Equal((0, 1), (alone.Candidate, admitted.Candidate));
            Assert.Equal((-1, CompactionReason.ManagedBound, true), (dropped.Candidate, dropped.Reason, dropped.PrefixDrop));
            Assert.Equal(0, DataFileAdmission.Admit(System.Array.Empty<InputSummary>(), stuck, output, settings).Candidate);
        }

        [Fact]
        public void TheWriterCountsArrowsDictionaryMemo()
        {
            var flat = new LeafLoad(8 * 200_000L, 200_000, false, 1, 8, LeafShape.Fixed, 8, false);
            var oneRow = new LeafLoad(8 * 2_000_000L + 250_128, 2_000_000, true, 2_000_000, 8, LeafShape.Fixed, 8, false);

            Assert.Equal(24 * 1_310_720 + 1_056_760 + NativeConstants.Compressed(Compression.Zstd, 1_056_760) + 12 * 20_000 + 48_654, DataFileAdmission.Dictionary(Compression.Zstd, flat, 1_024, 8_192));
            Assert.True(DataFileAdmission.WriterBytes(Compression.Zstd, new[] { flat }, 200_000) >= 24 * 1_310_720);
            Assert.True(DataFileAdmission.WriterBytes(Compression.Zstd, new[] { oneRow }, 1) >= 125_829_120 + 32_161_024 + 24_000_000);
        }

        [Fact]
        public void ThePrefixChecksEveryEarlierInputAtItsOwnBatch()
        {
            var output = new OutputShape(Table(Field("v", new Delta.LongType())));
            var settings = new AdmissionSettings(1L << 20, Compression.Zstd);
            var light = Summary(model: 0, outBatch: 1L << 20);
            var heavy = Summary(model: 66L << 20, outBatch: 1L << 20);
            var varying = Summary(model: 0, outBatch: 1L << 20, readPeak: k => k == 0 ? 400L << 20 : 1L << 20);
            var wide = Summary(model: 0, outBatch: 1L << 20, metadata: 150L << 20);
            (light.Candidate, heavy.Candidate, varying.Candidate, wide.Candidate) = (0, 0, 16, 0);

            var dropped = DataFileAdmission.Admit(new[] { light, heavy }, Summary(model: 0, outBatch: 4L << 20), output, settings);
            var admitted = DataFileAdmission.Admit(new[] { varying }, Summary(model: 0, outBatch: 1L << 20), output, settings);

            var native = DataFileAdmission.Admit(new[] { wide }, Summary(model: 0, outBatch: 10L << 20), output, settings);

            Assert.Equal((CompactionReason.ManagedBound, true), (dropped.Reason, dropped.PrefixDrop));
            Assert.Equal((CompactionReason.WriterBound, true), (native.Reason, native.PrefixDrop));
            Assert.Equal(0, admitted.Candidate);
        }

        [Fact]
        public void TheFloorAndCapsNameTheirReason()
        {
            var output = new OutputShape(Table(Field("v", new Delta.LongType())));
            var settings = new AdmissionSettings(1L << 20, Compression.Zstd);
            var none = System.Array.Empty<InputSummary>();

            Assert.Equal(CompactionReason.ManagedBound, DataFileAdmission.Admit(none, Summary(model: 100L << 20, outBatch: 1L << 20), output, settings).Reason);
            Assert.Equal(CompactionReason.MetadataBound, DataFileAdmission.Admit(none, Summary(model: 0, outBatch: 1L << 20, metadata: 300L << 20), output, settings).Reason);
            Assert.Equal(CompactionReason.BoundOverflow, DataFileAdmission.Admit(none, Summary(model: 0, outBatch: long.MaxValue / 2), output, settings).Reason);
            Assert.Equal(1, DataFileAdmission.Admit(none, Summary(model: 0, outBatch: 1L << 20, readPeak: k => k == 0 ? 400L << 20 : 1L << 20), output, settings).Candidate);
        }

        [Fact]
        public void TheReadPeakIsTheLargestGroups()
        {
            var path = Write(Name(), Schema(new Field("v", Int64Type.Default, true)), 100_000, properties: b => b.MaxRowGroupLength(70_000));
            var memory = new JobMemory();

            var (scan, plan) = DataFileAdmission.Preflight(path, Table(Field("v", new Delta.LongType())), false, memory);
            var summary = DataFileAdmission.Summarize(scan, plan, new OutputShape(Table(Field("v", new Delta.LongType()))));
            var peaks = Enumerable.Range(0, 2).Select(g => scan.Decode(g).Decode + scan.ReadBatch(g, 65_536).RecordReader + scan.ReadBatch(g, 65_536).Batch).ToArray();

            Assert.True(peaks[0] > peaks[1]);
            Assert.Equal(peaks[0], summary.ReadPeak[0]);
            memory.Release(scan.ChargedBytes);
        }

        [Theory]
        [InlineData(1, 0)]
        [InlineData(1_000, NativeConstants.FooterReadSize)]
        public void ALongFooterCountsTheSpeculativeRead(int groups, long expected)
        {
            var path = Write(Name(), Schema(new Field("v", Int64Type.Default, true)), 2 * groups, properties: b => b.MaxRowGroupLength(2));
            var memory = new JobMemory();

            var (scan, _) = DataFileAdmission.Preflight(path, Table(Field("v", new Delta.LongType())), false, memory);
            var footer = scan.Footer;

            Assert.Equal(expected, scan.Metadata - footer.FooterLength - NativeConstants.ChunkMetadata * footer.TotalChunks - NativeConstants.SchemaElementMetadata * footer.Schema.Count
                - NativeConstants.RowGroupMetadata * footer.RowGroupRows.Count - NativeConstants.StringFactor * footer.StringBytes - NativeConstants.ListEntryBytes * footer.ListEntries);
            memory.Release(scan.ChargedBytes);
        }

        [Fact]
        public void SharesAreTheLargestGroups()
        {
            var schema = Schema(new Field("v", Int64Type.Default, true), new Field("s", StringType.Default, true));
            var table = Table(Field("v", new Delta.LongType()), Field("s", new Delta.StringType()));
            string Strings(int rows, int skewed)
            {
                var path = Path.Join(_directory, Name());
                var columns = new IArrowArray[] { Column(Int64Type.Default, rows, 0, 0), new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => x < skewed ? new string('x', 100) : "a")).Build() };
                using var batch = new RecordBatch(schema, columns, rows);
                using var properties = new WriterPropertiesBuilder().MaxRowGroupLength(10_000).Build();
                using var writer = new ParquetSharp.Arrow.FileWriter(path, schema, properties);
                writer.WriteRecordBatch(batch);
                writer.Close();
                return path;
            }
            var memory = new JobMemory();

            var (two, twoPlan) = DataFileAdmission.Preflight(Strings(20_000, 10_000), table, false, memory);
            var (one, onePlan) = DataFileAdmission.Preflight(Strings(10_000, 10_000), table, false, memory);
            var shares = new[] { DataFileAdmission.Summarize(two, twoPlan, new OutputShape(table)), DataFileAdmission.Summarize(one, onePlan, new OutputShape(table)) };

            Assert.Equal(2, two.Groups.Length);
            Assert.Equal(shares[1].LeafShare[1], shares[0].LeafShare[1]);
            memory.Release(two.ChargedBytes + one.ChargedBytes);
        }

        [Fact]
        public async Task TheAuditCountsDeclinedFiles()
        {
            var schema = Schema(new Field("v", Int64Type.Default, true));
            using (var batch = new RecordBatch(schema, new[] { Column(Int64Type.Default, 1, 0, 0) }, 1))
            using (var arrowProperties = new ParquetSharp.Arrow.ArrowWriterPropertiesBuilder().StoreSchema().Build())
            using (var writer = new ParquetSharp.Arrow.FileWriter(Path.Join(_directory, "stored.parquet"), schema, new WriterPropertiesBuilder().Build(), arrowProperties))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            using var output = new StringWriter();

            await AdmissionAudit.RunAsync(new Dictionary<string, string>() { ["path"] = _directory }, output);

            Assert.Equal(1, JsonDocument.Parse(output.ToString()).RootElement.GetProperty("declinedFiles").GetInt32());
        }

        [Fact]
        public void AStoredArrowSchemaIsDeclined()
        {
            var schema = Schema(new Field("v", Int64Type.Default, true));
            var path = Path.Join(_directory, Name());
            using (var batch = new RecordBatch(schema, new[] { Column(Int64Type.Default, 10, 0, 0) }, 10))
            using (var arrowProperties = new ParquetSharp.Arrow.ArrowWriterPropertiesBuilder().StoreSchema().Build())
            using (var writer = new ParquetSharp.Arrow.FileWriter(path, schema, new WriterPropertiesBuilder().Build(), arrowProperties))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            var memory = new JobMemory();

            var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(path, Table(Field("v", new Delta.LongType())), false, memory));

            Assert.Equal(CompactionReason.BindingPreflight, declined.Reason);
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData(Compression.Uncompressed)]
        [InlineData(Compression.Snappy)]
        [InlineData(Compression.Gzip)]
        [InlineData(Compression.Lz4Hadoop)]
        [InlineData(Compression.Lz4)]
        [InlineData(Compression.Zstd)]
        public async Task SinkFilesAreAdmittedAtFullBatches(Compression compression)
        {
            var table = Table(Field("id", new Delta.LongType()), Field("name", new Delta.StringType()), Field("at", new Delta.TimestampType()), Field("ok", new Delta.BooleanType()));
            var path = Path.Join(_directory, "sink.parquet");
            var writer = new ParquetSharpWriter(table, table.Fields.Select(x => x.Name).ToList(), writerProperties: DeltaParquetCompression.GetWriterProperties(compression));
            writer.NewBatch();
            using (var rebound = new ReboundBatch(2_000, new IArrowArray[]
            {
                new Int64Array.Builder().AppendRange(Enumerable.Range(0, 2_000).Select(x => (long)x)).Build(),
                new StringArray.Builder().AppendRange(Enumerable.Range(0, 2_000).Select(x => $"name {x % 97}")).Build(),
                new TimestampArray.Builder(new TimestampType(TimeUnit.Millisecond, "UTC")).AppendRange(Enumerable.Range(0, 2_000).Select(x => DateTimeOffset.UnixEpoch.AddSeconds(x))).Build(),
                new BooleanArray.Builder().AppendRange(Enumerable.Range(0, 2_000).Select(x => x % 3 == 0)).Build()
            }, new List<ArrowBuffer>()))
            {
                var mask = new Internal.Delta.DeletionVectors.BatchDeleteMask();
                mask.Reset(0, 2_000);
                writer.CopyBatch(rebound, 0, mask);
            }
            await writer.WriteData(Files.Of.LocalDisk(_directory), "/", "sink.parquet");
            var memory = new JobMemory();

            var (scan, plan) = DataFileAdmission.Preflight(path, table, false, memory);
            var admission = DataFileAdmission.Admit(System.Array.Empty<InputSummary>(), DataFileAdmission.Summarize(scan, plan, new OutputShape(table)), new OutputShape(table), new AdmissionSettings(8L << 20, compression));

            Assert.Null(admission.Reason);
            Assert.True(InputSummary.BatchRows(admission.Candidate) >= 1_024);
            Assert.All(scan.Groups.SelectMany(x => x).Where((_, i) => scan.Schema.Leaves[scan.ReadLeaves[i % scan.ReadLeaves.Length]].Shape == LeafShape.Bytes), x => Assert.NotNull(x.Unencoded));
            memory.Release(scan.ChargedBytes);
        }

        [Fact]
        public void ARepeatedChunkWithFewerValuesThanRowsIsDeclined()
        {
            var path = Write(Name(), Schema(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true)), 1, elements: 3);
            var bytes = File.ReadAllBytes(path);
            var length = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
            var footer = bytes.AsSpan(bytes.Length - 8 - length, length);
            var patched = 0;
            for (int i = 0; i + 1 < footer.Length; i++)
            {
                if (footer[i] == 0x16 && footer[i + 1] == 0x02)
                {
                    footer[i + 1] = 0x0A;
                    patched++;
                }
            }
            File.WriteAllBytes(path, bytes);
            var memory = new JobMemory();

            var admission = Admit(path, Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true })), memory, new AdmissionSettings(1L << 20, Compression.Zstd), new List<InputSummary>());

            Assert.Equal(2, patched);
            Assert.Equal(CompactionReason.Malformed, admission.Reason);
            Assert.Contains("holds 3 values in a row group of 5 rows", admission.Detail);
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void AnOversizedFixedLengthDecimalIsDeclinedInPreflight()
        {
            var path = Path.Join(_directory, "decimal-flba.parquet");
            using (var element = new ParquetSharp.Schema.PrimitiveNode("d", Repetition.Optional, LogicalType.None(), PhysicalType.FixedLenByteArray, 20))
            using (var root = new ParquetSharp.Schema.GroupNode("schema", Repetition.Required, new[] { element }))
            using (var builder = new WriterPropertiesBuilder())
            using (var properties = builder.DisableDictionary().Build())
            using (var writer = new ParquetFileWriter(path, root, properties))
            using (var group = writer.AppendRowGroup())
            using (var column = (ColumnWriter<FixedLenByteArray>)group.NextColumn())
            {
                var buffer = new byte[20];
                var handle = System.Runtime.InteropServices.GCHandle.Alloc(buffer, System.Runtime.InteropServices.GCHandleType.Pinned);
                try
                {
                    column.WriteBatch(1, new short[] { 1 }, null, new[] { new FixedLenByteArray(handle.AddrOfPinnedObject()) });
                }
                finally
                {
                    handle.Free();
                }
                writer.Close();
            }
            var bytes = File.ReadAllBytes(path);
            var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
            var footer = bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength).ToArray().ToList();
            for (int i = 0; i < footer.Count - 4; i++)
            {
                if (footer[i] == 0x15 && footer[i + 1] == 0x0E && footer[i + 2] == 0x15 && footer[i + 3] == 0x28 && footer[i + 4] == 0x15 && footer[i + 5] == 0x02)
                {
                    var nameLength = footer[i + 7];
                    footer.InsertRange(i + 8 + nameLength, new byte[] { 0x25, 0x0A, 0x15, 0x04, 0x15, 0x14 });
                    break;
                }
            }
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Write))
            {
                stream.Position = bytes.Length - 8 - footerLength;
                stream.Write(footer.ToArray());
                Span<byte> tail = stackalloc byte[8];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(tail, footer.Count);
                 "PAR1"u8.CopyTo(tail.Slice(4));
                stream.Write(tail);
                stream.SetLength(stream.Position);
            }
            var table = Table(Field("d", new Delta.DecimalType(10, 2)));
            var memory = new JobMemory();

            var admission = Admit(path, table, memory, new AdmissionSettings(1L << 20, Compression.Zstd), new List<InputSummary>());

            Assert.Equal(CompactionReason.BindingPreflight, admission.Reason);
            Assert.Equal(0, memory.Managed);
        }

        private static Admission Admit(string path, Delta.StructType table, JobMemory memory, AdmissionSettings settings, IReadOnlyList<InputSummary> prefix)
        {
            try
            {
                var (scan, plan) = DataFileAdmission.Preflight(path, table, false, memory);
                var output = new OutputShape(table);
                var summary = DataFileAdmission.Summarize(scan, plan, output);
                memory.Release(scan.ChargedBytes);
                return DataFileAdmission.Admit(prefix, summary, output, settings);
            }
            catch (DataFileDeclinedException e)
            {
                return new Admission(-1, e.Reason, e.Message);
            }
        }

        // Small files whose largest term passes a 48 MiB cap at B = 1
        private (string Path, Delta.StructType Table) Fixture(string name)
        {
            var longs = new Schema.Builder().Field(new Field("v", Int64Type.Default, true)).Build();
            var longTable = Table(Field("v", new Delta.LongType()));
            var strings = new Schema.Builder().Field(new Field("s", StringType.Default, true)).Build();
            var stringTable = Table(Field("s", new Delta.StringType()));
            var element = new StructType(new List<Field>() { new Field("a", Int32Type.Default, true) });
            var lists = new Schema.Builder().Field(new Field("l", new ListType(new Field("element", element, true)), true)).Build();
            var wide = Table(Field("a", new Delta.IntegerType()), Field("s", Table(Enumerable.Range(0, 8).Select(x => Field($"w{x}", new Delta.LongType())).ToArray())));
            var listTable = Table(Field("l", new Delta.ArrayType() { ElementType = wide, ContainsNull = true }));
            return name switch
            {
                "codec" => (Write("codec.parquet", longs, 10, properties: b => b.Compression(Compression.Brotli)), longTable),
                "encoding" => (Write("encoding.parquet", strings, 10, properties: b => b.DisableDictionary().Encoding(ParquetSharp.Encoding.DeltaByteArray)), stringTable),
                "deep" => Deep(),
                "dictionary" => (Write("dictionary.parquet", strings, 600_000, properties: b => b.DictionaryPagesizeLimit(64L << 20)), stringTable),
                "value" => (Write("value.parquet", strings, 1, giant: 12 << 20, properties: b => b.DisableDictionary()), stringTable),
                "repeated" => (Write("repeated.parquet", new Schema.Builder().Field(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true)).Build(), 1, elements: 1_500_000),
                    Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true }))),
                "subtree" => (Write("subtree.parquet", lists, 1, elements: 400_000), listTable),
                "narrow" => (Write("narrow.parquet", Schema(new Field("l", new ListType(new Field("element", BooleanType.Default, true)), true)), 2_000, elements: 5),
                    Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.BooleanType(), ContainsNull = true }))),
                _ => (Write("output.parquet", lists, 1, elements: 250_000), Table(Field("l", new Delta.ArrayType() { ElementType = Table(Field("a", new Delta.IntegerType()), Field("s", Table(Enumerable.Range(0, 16).Select(x => Field($"w{x}", new Delta.StringType())).ToArray()))), ContainsNull = true })))
            };
        }

        // 65 nested groups, deeper than we model; Arrow's writer refuses that
        private (string, Delta.StructType) Deep()
        {
            ParquetSharp.Schema.Node node = new ParquetSharp.Schema.PrimitiveNode("n", Repetition.Optional, LogicalType.None(), PhysicalType.Int32);
            for (int i = 0; i < 65; i++)
            {
                node = new ParquetSharp.Schema.GroupNode(i == 64 ? "d" : "n", Repetition.Optional, new[] { node });
            }
            var path = Path.Join(_directory, "deep.parquet");
            using (var root = new ParquetSharp.Schema.GroupNode("schema", Repetition.Required, new[] { node }))
            using (var builder = new WriterPropertiesBuilder())
            using (var properties = builder.Build())
            using (var writer = new ParquetFileWriter(path, root, properties))
            using (var group = writer.AppendRowGroup())
            using (var column = (ColumnWriter<int>)group.NextColumn())
            {
                column.WriteBatch(1, new short[] { 66 }, null, new[] { 1 });
                writer.Close();
            }
            return (path, Table(Field("d", new Delta.IntegerType())));
        }

        // Distinct values, lists in one row, or one giant string
        private string Write(string name, Schema schema, int rows, int elements = 0, int giant = 0, Func<WriterPropertiesBuilder, WriterPropertiesBuilder>? properties = null)
        {
            var columns = schema.FieldsList.Select(field => Column(field.DataType, rows, elements, giant)).ToArray();
            var path = Path.Join(_directory, name);
            using var builder = new WriterPropertiesBuilder();
            using var writerProperties = (properties?.Invoke(builder) ?? builder).Build();
            using var batch = new RecordBatch(schema, columns, rows);
            using var writer = new ParquetSharp.Arrow.FileWriter(path, schema, writerProperties);
            writer.WriteRecordBatch(batch);
            writer.Close();
            return path;
        }

        private static IArrowArray Column(IArrowType type, int rows, int elements, int giant)
        {
            switch (type)
            {
                case BooleanType:
                    return new BooleanArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => x % 2 == 0)).Build();
                case Int8Type:
                    return new Int8Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => (sbyte)x)).Build();
                case Date32Type:
                    return new Date32Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => DateTime.UnixEpoch.AddDays(x % 20_000))).Build();
                case Int32Type:
                    return new Int32Array.Builder().AppendRange(Enumerable.Range(0, rows)).Build();
                case Int64Type:
                    return new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => (long)x)).Build();
                case StringType:
                    return giant > 0 ? new StringArray.Builder().Append(new string('x', giant)).Build() : new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => $"v{x:D7}")).Build();
                case TimestampType timestamp:
                    return new TimestampArray.Builder(timestamp).AppendRange(Enumerable.Range(0, rows).Select(x => DateTimeOffset.UnixEpoch.AddSeconds(x))).Build();
                case StructType structType:
                    return new StructArray(structType, rows, structType.Fields.Select(x => Column(x.DataType, rows, elements, giant)).ToArray(), ArrowBuffer.Empty, 0);
                case ListType listType:
                    return new ListArray(listType, rows, Offsets(rows, elements), Column(listType.ValueDataType, rows * elements, 0, 0), ArrowBuffer.Empty, 0);
                case MapType mapType:
                    var entries = new StructArray(mapType.KeyValueType, rows * elements, new[] { Column(mapType.KeyField.DataType, rows * elements, 0, 0), Column(mapType.ValueField.DataType, rows * elements, 0, 0) }, ArrowBuffer.Empty, 0);
                    return new MapArray(mapType, rows, Offsets(rows, elements), entries, ArrowBuffer.Empty, 0);
                default:
                    throw new NotSupportedException(type.Name);
            }
        }

        private static ArrowBuffer Offsets(int rows, int elements) => new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, rows + 1).Select(x => x * elements)).Build();

        private static (Delta.StructType Schema, List<string> Files) TestData(string table)
        {
            var root = Path.Join("../../../testdata", table);
            var json = Directory.EnumerateFiles(Path.Join(root, "_delta_log"), "*.json").Order().SelectMany(File.ReadLines).First(x => x.Contains("\"metaData\""));
            var schemaString = JsonDocument.Parse(json).RootElement.GetProperty("metaData").GetProperty("schemaString").GetString()!;
            var options = new JsonSerializerOptions();
            options.Converters.Add(new TypeConverter());
            var schema = (Delta.StructType)JsonSerializer.Deserialize<SchemaBaseType>(schemaString, options)!;
            return (schema, Directory.EnumerateFiles(root, "*.parquet", SearchOption.AllDirectories).Where(x => !x.Contains("_delta_log") && !x.Contains("_change_data")).ToList());
        }

        // Preflight and FileBinding agree; a file with no read leaf is declined
        private static void AssertBindsAlike(Delta.StructType table, string path, bool byId = false)
        {
            var memory = new JobMemory();
            var binding = TryBind(table, path, byId);
            if (binding == null || binding.LeafColumns.Length == 0)
            {
                var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(path, table, byId, memory));
                Assert.Equal(CompactionReason.BindingPreflight, declined.Reason);
                Assert.Equal(0, memory.Managed);
                return;
            }
            var (scan, plan) = DataFileAdmission.Preflight(path, table, byId, memory);
            Assert.Equal(binding.LeafColumns, scan.ReadLeaves);
            Assert.Equal(binding.TimestampUnits, plan.Units);
            Assert.Equal(binding.PrecisionLeaves, plan.Precision);
            Assert.Equal(binding.MissingPaths, plan.MissingPaths);
            memory.Release(scan.ChargedBytes);
            Assert.Equal(0, memory.Managed);
        }

        private static FileBinding? TryBind(Delta.StructType table, string path, bool byId)
        {
            using var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 1_024, new FileInfo(path).Length));
            try
            {
                return FileBinding.Create(table, file, "t", byId);
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        // Output batch fixed or halving with B, model held as managed
        private static InputSummary Summary(long model, long outBatch, bool shrinking = false, long metadata = 1L << 20, Func<int, long>? readPeak = null)
        {
            var candidates = InputSummary.Candidates;
            var readerOnly = 1L << 20;
            return new InputSummary()
            {
                Metadata = metadata,
                Rows = 1_000,
                Model = model,
                Out = 1L << 20,
                LeafShare = new[] { 1.0 },
                LeafMaxValue = new[] { 8L },
                LeafMeanValue = new[] { 8L },
                LeafLevelRatio = new[] { 0.0 },
                LeafRowLevels = new[] { 1L },
                LeafGroupLevels = new[] { 1L },
                Groups = 1,
                ReadPeak = Enumerable.Range(0, candidates).Select(k => readPeak?.Invoke(k) ?? readerOnly).ToArray(),
                ReaderOnly = Enumerable.Repeat(readerOnly, candidates).ToArray(),
                OutBatch = Enumerable.Range(0, candidates).Select(k => shrinking ? outBatch >> k : outBatch).ToArray(),
                SynthStage = new long[candidates],
                PrecisionDecode = 0,
                PrecisionManaged = 0,
                Floor = (0, readerOnly, 0, 0, 0, 0, 0)
            };
        }

        private static Delta.StructType Table(params Delta.StructField[] fields) => new Delta.StructType(fields);

        private static Delta.StructField Field(string name, SchemaBaseType type, string? physicalName = null, int? fieldId = null) => new Delta.StructField(name, type, true, new Dictionary<string, object>()) { PhysicalName = physicalName, FieldId = fieldId };

        private static Schema Schema(params Field[] fields) => new Schema(fields, null);

        private static StructType Struct(params Field[] fields) => new StructType(fields);

        private static Dictionary<string, string> Id(int id) => new Dictionary<string, string>() { ["PARQUET:field_id"] = id.ToString() };

        private static string Name() => Guid.NewGuid().ToString("N") + ".parquet";
    }
}
