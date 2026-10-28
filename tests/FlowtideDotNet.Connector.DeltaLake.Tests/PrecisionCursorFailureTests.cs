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
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using ParquetSharp;
using ParquetSharp.Schema;
using Stowage;
using Xunit.Abstractions;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [CollectionDefinition("PrecisionCursorFailures", DisableParallelization = true)]
    public class PrecisionCursorFailureCollection { }

    [Collection("PrecisionCursorFailures")]
    public class PrecisionCursorFailureTests
    {
        private readonly ITestOutputHelper _output;

        public PrecisionCursorFailureTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task ACursorRejectsAMissingRootInAMalformedRepeatedFile()
        {
            const string path = "short-repeated-precision.parquet";
            var file = RepeatedFileWithAnExtraMetadataRow();
            var arrowRows = 0;
            var arrowError = await Record.ExceptionAsync(async () =>
            {
                using var arrow = new ParquetSharp.Arrow.FileReader(new MemoryStream(file));
                using var batches = arrow.GetRecordBatchReader();
                RecordBatch? batch;
                while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        arrowRows += batch.Length;
                    }
                }
            });
            _output.WriteLine($"Arrow rows: {arrowRows}; error: {arrowError}");
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var descriptor = reader.FileMetaData.Schema.Column(0);
            var leaves = new[] { new PrecisionLeaf(0, true, descriptor.MaxDefinitionLevel, descriptor.MaxRepetitionLevel) };
            Assert.Contains("ended after 40 of its 41 rows", Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, path, 0, leaves)).Message);
            using var cursor = new TimestampPrecision.RowGroupCursor(reader, path, 0, leaves);
            var error = Assert.Throws<InvalidDataException>(() => cursor.Flagged(39));
            Assert.Contains(path, error.Message);
        }

        [Fact]
        public void ACursorRejectsEndOfLeafBeforeTheRequestedRoot()
        {
            const string path = "short-precision-leaf.parquet";
            var file = TimestampPrecisionTests.WriteInt96(Enumerable.Range(0, 40).Select(_ => (Int96?)ForeignRewrite.Int96(0, 0)).ToArray());
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var descriptor = reader.FileMetaData.Schema.Column(0);
            var leaves = new[] { new PrecisionLeaf(0, true, descriptor.MaxDefinitionLevel, descriptor.MaxRepetitionLevel) };
            TimestampPrecision.LevelsHookForTests = (string hookPath, PrecisionLeaf leaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead) =>
            {
                if (hookPath == path)
                {
                    levels = 2;
                    valuesRead = 2;
                }
            };
            try
            {
                Assert.Contains("ended after 2 of its 40 rows", Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, path, 0, leaves)).Message);
                using var cursor = new TimestampPrecision.RowGroupCursor(reader, path, 0, leaves);
                var error = Assert.Throws<InvalidDataException>(() => cursor.Flagged(39));
                Assert.Contains(path, error.Message);
                Assert.Contains("timestamp leaf ts", error.Message);
            }
            finally
            {
                TimestampPrecision.LevelsHookForTests = null;
            }
        }

        [Fact]
        public async Task AShortRawLeafCannotAuthorizeAnInexactMatch()
        {
            const string path = "short-precision-match.parquet";
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            var bytes = ForeignRewrite.WriteInt96("a", "ts", Enumerable.Range(0, 40).Select(x => (long)x).ToArray(),
                Enumerable.Range(0, 40).Select(i => ForeignRewrite.Int96(0, i == 39 ? 500 : 0)).ToArray(), 40);
            using (var write = await storage.OpenWrite("/t/" + path))
            {
                await write.WriteAsync(bytes);
            }
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("a", new Delta.LongType(), true, new Dictionary<string, object>()),
                new Delta.StructField("ts", new Delta.TimestampType(), true, new Dictionary<string, object>())
            });
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("a", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Field("ts", new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, "UTC"), true)).Build();
            using var stored = new RecordBatch(schema, new IArrowArray[]
            {
                new Int64Array.Builder().Append(39).Build(),
                new TimestampArray(new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, "UTC"),
                    new ArrowBuffer.Builder<long>().Append(0).Build(), ArrowBuffer.Empty, 1, 0, 0)
            }, 1);
            var batch = new DeleteBatch(stored, new long[] { 1 }, new IArrowComparer[] { new ArrowInt64Comparer(), new ArrowTimestampComparer() });
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = new DeltaFileRecord { Path = path, PartitionValues = new Dictionary<string, string>() };
            TimestampPrecision.LevelsHookForTests = (string hookPath, PrecisionLeaf leaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead) =>
            {
                if (hookPath.EndsWith(path, StringComparison.Ordinal))
                {
                    levels = 2;
                    valuesRead = 2;
                }
            };
            try
            {
                var matcher = new FileMatcher(storage, "/t", "t", table, false);
                var error = await Assert.ThrowsAsync<InvalidDataException>(() => matcher.Scan(0, batch, run));
                Assert.Contains(path, error.Message);
                Assert.Empty(run.Claims);
                Assert.Equal(1, batch.Unresolved);
            }
            finally
            {
                TimestampPrecision.LevelsHookForTests = null;
            }
        }

        private static byte[] RepeatedFileWithAnExtraMetadataRow()
        {
            using var element = new PrimitiveNode("element", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var list = new GroupNode("list", Repetition.Repeated, new Node[] { element });
            using var ts = new GroupNode("ts", Repetition.Optional, new Node[] { list }, LogicalType.List());
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { ts });
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x.Compression(Compression.Uncompressed));
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                using (var group = writer.AppendRowGroup())
                using (var column = group.NextColumn())
                {
                    ((ColumnWriter<Int96>)column).WriteBatch(80, Enumerable.Repeat((short)3, 80).ToArray(),
                        Enumerable.Range(0, 80).Select(i => (short)(i % 2)).ToArray(),
                        Enumerable.Repeat(ForeignRewrite.Int96(0, 0), 80).ToArray());
                }
                writer.Close();
            }
            var file = stream.ToArray();
            var footerLength = BitConverter.ToInt32(file, file.Length - 8);
            var matches = new List<int>();
            for (int i = file.Length - 8 - footerLength; i + 1 < file.Length - 8; i++)
            {
                if (file[i] == 0x16 && file[i + 1] == 80)
                {
                    matches.Add(i + 1);
                }
            }
            Assert.Equal(2, matches.Count);
            foreach (var offset in matches)
            {
                file[offset] = 82;
            }
            return file;
        }

        [Fact]
        public async Task AMalformedPreviousGroupCannotAuthorizeAnInexactNextGroupRow()
        {
            const string path = "short-first-precision-group.parquet";
            var file = TwoGroupsWithAnExtraFirstGroupMetadataRow();
            using (var reader = new ParquetFileReader(new MemoryStream(file)))
            {
                var descriptor = reader.FileMetaData.Schema.Column(1);
                var leaves = new[] { new PrecisionLeaf(1, true, descriptor.MaxDefinitionLevel, descriptor.MaxRepetitionLevel) };
                Assert.Equal(new ulong[] { 1 }, TimestampPrecision.FlagRowGroup(reader, path, 1, leaves));
                Assert.Contains("ended after 40 of its 41 rows", Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, path, 0, leaves)).Message);
            }
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/" + path))
            {
                await write.WriteAsync(file);
            }
            var timestamp = new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, "UTC");
            var list = new Apache.Arrow.Types.ListType(timestamp);
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("a", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Field("ts", list, true)).Build();
            var values = new TimestampArray(timestamp, new ArrowBuffer.Builder<long>().AppendRange(new long[] { 0, 0 }).Build(), ArrowBuffer.Empty, 2, 0, 0);
            using var stored = new RecordBatch(schema, new IArrowArray[]
            {
                new Int64Array.Builder().Append(1000).Build(),
                new ListArray(list, 1, new ArrowBuffer.Builder<int>().AppendRange(new[] { 0, 2 }).Build(), values, ArrowBuffer.Empty, 0)
            }, 1);
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("a", new Delta.LongType(), true, new Dictionary<string, object>()),
                new Delta.StructField("ts", new Delta.ArrayType { ElementType = new Delta.TimestampType(), ContainsNull = true }, true, new Dictionary<string, object>())
            });
            var batch = new DeleteBatch(stored, new long[] { 1 }, new IArrowComparer[] { new ArrowInt64Comparer(), new ArrowArrayComparer(new ArrowTimestampComparer()) });
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = new DeltaFileRecord { Path = path, PartitionValues = new Dictionary<string, string>() };
            var matcher = new FileMatcher(storage, "/t", "t", table, false);
            var error = await Record.ExceptionAsync(() => matcher.Scan(0, batch, run));
            _output.WriteLine($"Matcher error: {error}; claims: {string.Join(",", run.Claims.SelectMany(x => x.Value))}; unresolved: {batch.Unresolved}");
            Assert.Contains("holds 40 values in row group 0 of 41 rows", Assert.IsType<InvalidDataException>(error).Message);
            Assert.Empty(run.Claims);
            Assert.Equal(1, batch.Unresolved);
        }

        [Fact]
        public async Task AnUnderstatedPreviousGroupCannotAuthorizeAnInexactNextGroupRow()
        {
            const string path = "long-first-precision-group.parquet";
            var file = TwoGroupsWithFirstGroupMetadataRows(39);
            using (var reader = new ParquetFileReader(new MemoryStream(file)))
            {
                var descriptor = reader.FileMetaData.Schema.Column(1);
                var leaves = new[] { new PrecisionLeaf(1, true, descriptor.MaxDefinitionLevel, descriptor.MaxRepetitionLevel) };
                Assert.Equal(new ulong[] { 1 }, TimestampPrecision.FlagRowGroup(reader, path, 1, leaves));
                Assert.Contains("holds more than its 39 rows", Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, path, 0, leaves)).Message);
            }
            // Arrow reads past group 0's footer count
            var keys = new List<long?>();
            using (var arrow = new ParquetSharp.Arrow.FileReader(new MemoryStream(file)))
            using (var batches = arrow.GetRecordBatchReader())
            {
                RecordBatch? read;
                while ((read = await batches.ReadNextRecordBatchAsync()) != null)
                {
                    using (read)
                    {
                        var column = (Int64Array)read.Column(0);
                        for (int i = 0; i < column.Length; i++)
                        {
                            keys.Add(column.GetValue(i));
                        }
                    }
                }
            }
            _output.WriteLine($"Arrow rows: {keys.Count}; keys 38-41: {string.Join(",", keys.Skip(38).Take(4))}");
            Assert.Equal((long?)40, keys[39]);
            Assert.Equal((long?)1000, keys[40]);
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/" + path))
            {
                await write.WriteAsync(file);
            }
            var timestamp = new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, "UTC");
            var list = new Apache.Arrow.Types.ListType(timestamp);
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("a", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Field("ts", list, true)).Build();
            var values = new TimestampArray(timestamp, new ArrowBuffer.Builder<long>().AppendRange(new long[] { 0, 0 }).Build(), ArrowBuffer.Empty, 2, 0, 0);
            using var stored = new RecordBatch(schema, new IArrowArray[]
            {
                new Int64Array.Builder().Append(1000).Build(),
                new ListArray(list, 1, new ArrowBuffer.Builder<int>().AppendRange(new[] { 0, 2 }).Build(), values, ArrowBuffer.Empty, 0)
            }, 1);
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("a", new Delta.LongType(), true, new Dictionary<string, object>()),
                new Delta.StructField("ts", new Delta.ArrayType { ElementType = new Delta.TimestampType(), ContainsNull = true }, true, new Dictionary<string, object>())
            });
            var batch = new DeleteBatch(stored, new long[] { 1 }, new IArrowComparer[] { new ArrowInt64Comparer(), new ArrowArrayComparer(new ArrowTimestampComparer()) });
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = new DeltaFileRecord { Path = path, PartitionValues = new Dictionary<string, string>() };
            var matcher = new FileMatcher(storage, "/t", "t", table, false);
            var error = await Record.ExceptionAsync(() => matcher.Scan(0, batch, run));
            _output.WriteLine($"Matcher error: {error}; claims: {string.Join(",", run.Claims.SelectMany(x => x.Value))}; unresolved: {batch.Unresolved}");
            Assert.Contains("holds 40 values in row group 0 of 39 rows", Assert.IsType<InvalidDataException>(error).Message);
            Assert.Empty(run.Claims);
            Assert.Equal(1, batch.Unresolved);
        }

        private static byte[] TwoGroupsWithAnExtraFirstGroupMetadataRow()
        {
            return TwoGroupsWithFirstGroupMetadataRows(41);
        }

        // Group 0 holds 40 rows, its footer says firstRows
        private static byte[] TwoGroupsWithFirstGroupMetadataRows(int firstRows)
        {
            using var key = new PrimitiveNode("a", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64);
            using var element = new PrimitiveNode("element", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var list = new GroupNode("list", Repetition.Repeated, new Node[] { element });
            using var ts = new GroupNode("ts", Repetition.Optional, new Node[] { list }, LogicalType.List());
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { key, ts });
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x.Compression(Compression.Uncompressed));
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                for (int g = 0; g < 2; g++)
                {
                    using var group = writer.AppendRowGroup();
                    using (var column = group.NextColumn())
                    {
                        var keys = Enumerable.Range(0, 40).Select(i => g == 0 ? i + 1L : i < 2 ? 1000L : 2000L + i).ToArray();
                        ((ColumnWriter<long>)column).WriteBatch(40, Enumerable.Repeat((short)1, 40).ToArray(), null, keys);
                    }
                    using (var column = group.NextColumn())
                    {
                        var timestamps = Enumerable.Range(0, 80).Select(i => ForeignRewrite.Int96(0, g == 1 && i < 2 ? 500 : 0)).ToArray();
                        ((ColumnWriter<Int96>)column).WriteBatch(80, Enumerable.Repeat((short)3, 80).ToArray(),
                            Enumerable.Range(0, 80).Select(i => (short)(i % 2)).ToArray(), timestamps);
                    }
                }
                writer.Close();
            }
            var file = stream.ToArray();
            var footerLength = BitConverter.ToInt32(file, file.Length - 8);
            var forty = new List<int>();
            var eighty = new List<int>();
            for (int i = file.Length - 8 - footerLength; i + 2 < file.Length - 8; i++)
            {
                if (file[i] == 0x16 && file[i + 1] == 80)
                {
                    forty.Add(i + 1);
                }
                if (file[i] == 0x16 && file[i + 1] == 0xA0 && file[i + 2] == 1)
                {
                    eighty.Add(i + 1);
                }
            }
            // Per group: the scalar chunk count, then root count
            Assert.Equal(4, forty.Count);
            // File count precedes both repeated chunk counts
            Assert.Equal(3, eighty.Count);
            // Zigzag varints; the file count keeps its second byte
            file[forty[1]] = (byte)(2 * firstRows);
            file[eighty[0]] = (byte)(0x80 | ((2 * (40 + firstRows)) & 0x7F));
            using var reader = new ParquetFileReader(new MemoryStream(file));
            Assert.Equal(40 + firstRows, reader.FileMetaData.NumRows);
            using var first = reader.RowGroup(0);
            using var second = reader.RowGroup(1);
            Assert.Equal(firstRows, first.MetaData.NumRows);
            Assert.Equal(40, second.MetaData.NumRows);
            return file;
        }

        [Theory]
        [InlineData(39, false, 2)]
        [InlineData(40, false, 2)]
        [InlineData(39, true, 2)]
        [InlineData(40, true, 2)]
        // Hits are in group 2, so groups 0 and 1 are passed
        [InlineData(39, false, 3)]
        [InlineData(40, false, 3)]
        public async Task AnAllRepeatedFileCannotAuthorizeAShiftedInexactRow(int firstRows, bool earlyHit, int groups)
        {
            const string path = "all-repeated-understated-group.parquet";
            var file = AllRepeatedGroupsWithFirstGroupMetadataRows(firstRows, groups);
            using (var reader = new ParquetFileReader(new MemoryStream(file)))
            {
                var descriptor = reader.FileMetaData.Schema.Column(0);
                Assert.True(descriptor.MaxRepetitionLevel > 0);
                var leaves = new[] { new PrecisionLeaf(0, true, descriptor.MaxDefinitionLevel, descriptor.MaxRepetitionLevel) };
                Assert.Equal(new ulong[] { 1 }, TimestampPrecision.FlagRowGroup(reader, path, groups - 1, leaves));
                if (firstRows == 39)
                {
                    Assert.Contains("holds more than its 39 rows", Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, path, 0, leaves)).Message);
                }
                else
                {
                    Assert.Null(TimestampPrecision.FlagRowGroup(reader, path, 0, leaves));
                }
            }

            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/" + path))
            {
                await write.WriteAsync(file);
            }
            var timestamp = new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, "UTC");
            var list = new Apache.Arrow.Types.ListType(timestamp);
            var schema = new Apache.Arrow.Schema.Builder().Field(new Field("ts", list, true)).Build();
            // With an early hit, group 0's first row is claimed before group 1 is reached
            var millis = earlyHit ? new long[] { 1, 1, 0, 0 } : new long[] { 0, 0 };
            var rows = millis.Length / 2;
            var values = new TimestampArray(timestamp, new ArrowBuffer.Builder<long>().AppendRange(millis).Build(), ArrowBuffer.Empty, millis.Length, 0, 0);
            using var stored = new RecordBatch(schema, new IArrowArray[]
            {
                new ListArray(list, rows, new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, rows + 1).Select(i => 2 * i)).Build(), values, ArrowBuffer.Empty, 0)
            }, rows);
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("ts", new Delta.ArrayType { ElementType = new Delta.TimestampType(), ContainsNull = true }, true, new Dictionary<string, object>())
            });
            var batch = new DeleteBatch(stored, Enumerable.Repeat(1L, rows).ToArray(), new IArrowComparer[] { new ArrowArrayComparer(new ArrowTimestampComparer()) });
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = new DeltaFileRecord { Path = path, PartitionValues = new Dictionary<string, string>() };
            var matcher = new FileMatcher(storage, "/t", "t", table, false);
            var error = await Record.ExceptionAsync(() => matcher.Scan(0, batch, run));
            _output.WriteLine($"Matcher error: {error}; claims: {string.Join(",", run.Claims.SelectMany(x => x.Value))}; unresolved: {batch.Unresolved}");
            var claims = run.Claims.SelectMany(x => x.Value).ToList();
            if (firstRows == 39)
            {
                Assert.Contains("holds more than its 39 rows", Assert.IsType<InvalidDataException>(error).Message);
                Assert.Equal(earlyHit ? new long[] { 0 } : new long[0], claims);
                Assert.Equal(rows, batch.Unresolved + (earlyHit ? 1 : 0));
            }
            else
            {
                Assert.Null(error);
                // The exact twin is the last group's second row
                var twin = 40 * (groups - 1) + 1;
                Assert.Equal(earlyHit ? new long[] { 0, twin } : new long[] { twin }, claims);
                Assert.Equal(0, batch.Unresolved);
            }
        }

        // Only a list of INT96, group 0 holds 40 rows, its footer says firstRows
        private static byte[] AllRepeatedGroupsWithFirstGroupMetadataRows(int firstRows, int groups)
        {
            using var element = new PrimitiveNode("element", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var list = new GroupNode("list", Repetition.Repeated, new Node[] { element });
            using var ts = new GroupNode("ts", Repetition.Optional, new Node[] { list }, LogicalType.List());
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { ts });
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x.Compression(Compression.Uncompressed));
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                for (int g = 0; g < groups; g++)
                {
                    using var group = writer.AppendRowGroup();
                    using var column = group.NextColumn();
                    // Only the last group holds the inexact row and its exact twin
                    var timestamps = Enumerable.Range(0, 80).Select(i => ForeignRewrite.Int96(0,
                        g < groups - 1 ? 1_000_000 : i < 2 ? 500 : i < 4 ? 0 : 2_000_000)).ToArray();
                    ((ColumnWriter<Int96>)column).WriteBatch(80, Enumerable.Repeat((short)3, 80).ToArray(),
                        Enumerable.Range(0, 80).Select(i => (short)(i % 2)).ToArray(), timestamps);
                }
                writer.Close();
            }
            var file = stream.ToArray();
            var footerLength = BitConverter.ToInt32(file, file.Length - 8);
            // Zigzag varints: each group's 40 rows, and the file's rows in two bytes
            var total = 2 * 40 * groups;
            var forty = new List<int>();
            var counts = new List<int>();
            for (int i = file.Length - 8 - footerLength; i + 2 < file.Length - 8; i++)
            {
                if (file[i] == 0x16 && file[i + 1] == 80)
                {
                    forty.Add(i + 1);
                }
                if (file[i] == 0x16 && file[i + 1] == (0x80 | (total & 0x7F)) && file[i + 2] == total >> 7)
                {
                    counts.Add(i + 1);
                }
            }
            Assert.Equal(groups, forty.Count);
            // The file count precedes any chunk count of the same bytes
            Assert.NotEmpty(counts);
            var patched = 2 * (40 * (groups - 1) + firstRows);
            Assert.Equal(total >> 7, patched >> 7);
            file[forty[0]] = (byte)(2 * firstRows);
            file[counts[0]] = (byte)(0x80 | (patched & 0x7F));
            using var reader = new ParquetFileReader(new MemoryStream(file));
            Assert.Equal(40 * (groups - 1) + firstRows, reader.FileMetaData.NumRows);
            for (int g = 0; g < groups; g++)
            {
                using var group = reader.RowGroup(g);
                Assert.Equal(g == 0 ? firstRows : 40, group.MetaData.NumRows);
            }
            return file;
        }
    }
}
