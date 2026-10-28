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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using ParquetSharp;
using ParquetSharp.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class TimestampPrecisionTests
    {
        private const int Day = 2_460_000;

        [Fact]
        public async Task RepeatedInt96MatchesTheNanosecondRead()
        {
            // Two row groups of over 4096 levels each
            var random = new Random(31);
            var groups = new[] { ListRows(random, 2000), ListRows(random, 1800) };
            var file = WriteList(groups);

            using var reader = new ParquetFileReader(new MemoryStream(file));
            var leaf = Leaf(reader, int96: true);
            var expected = await NanosecondFlags(file, groups.Select(x => x.Count).ToArray());
            Assert.True(expected.Any(x => x.Any(y => y)), "the fixture flags no row");
            for (int g = 0; g < groups.Length; g++)
            {
                using (var group = reader.RowGroup(g))
                using (var chunk = group.MetaData.GetColumnChunkMetaData(0))
                {
                    Assert.True(chunk.NumValues > 4096, $"{chunk.NumValues} levels");
                }
                var flags = TimestampPrecision.FlagRowGroup(reader, "list.parquet", g, new[] { leaf });
                Assert.Equal(expected[g], Rows(flags, groups[g].Count));
            }
        }

        [Fact]
        public async Task NestedListsMatchTheNanosecondRead()
        {
            var random = new Random(37);
            var rows = NestedRows(random, 1500);
            var file = WriteNestedList(rows);

            using var reader = new ParquetFileReader(new MemoryStream(file));
            var leaf = Leaf(reader, int96: true);
            Assert.Equal(5, leaf.MaxDefinitionLevel);
            Assert.Equal(2, leaf.MaxRepetitionLevel);
            using (var group = reader.RowGroup(0))
            using (var chunk = group.MetaData.GetColumnChunkMetaData(0))
            {
                Assert.True(chunk.NumValues > 4096, $"{chunk.NumValues} levels");
            }
            var expected = (await NanosecondFlags(file, new[] { rows.Count }))[0];
            Assert.Contains(true, expected);
            Assert.Contains(false, expected);

            var flags = TimestampPrecision.FlagRowGroup(reader, "nested.parquet", 0, new[] { leaf });

            Assert.Equal(expected, Rows(flags, rows.Count));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(500)]
        public void ACursorAgreesWithTheBitmapAtTheRowsItIsAsked(int step)
        {
            var rows = NestedRows(new Random(41), 1500);
            var file = WriteNestedList(rows);
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var leaf = Leaf(reader, int96: true);
            var expected = Rows(TimestampPrecision.FlagRowGroup(reader, "nested.parquet", 0, new[] { leaf }), rows.Count);
            Assert.Contains(true, expected);

            using var cursor = new TimestampPrecision.RowGroupCursor(reader, "nested.parquet", 0, new[] { leaf });
            for (int row = step / 2; row < rows.Count; row += step)
            {
                Assert.Equal(expected[row], cursor.Flagged(row));
            }
        }

        [Fact]
        public void ACursorReadsOnlyAsFarAsTheRowAsked()
        {
            var file = WriteInt96(Enumerable.Range(0, 100_000).Select(i => (Int96?)Int96(i % 2 == 0 ? 1_000 : 1_001, Day)).ToArray());
            using var reader = new ParquetFileReader(new MemoryStream(file));
            using var cursor = new TimestampPrecision.RowGroupCursor(reader, "flat.parquet", 0, new[] { Leaf(reader, int96: true) });

            Assert.False(cursor.Flagged(10));
            Assert.Equal(4096, cursor.LevelsRead);
            Assert.True(cursor.Flagged(5_001));
            Assert.Equal(8192, cursor.LevelsRead);
        }

        [Fact]
        public void ACursorAskedPastItsLastLevelIsADecodeError()
        {
            var path = "cursor-ends-early.parquet";
            var file = WriteList(new[] { ListRows(new Random(7), 40, nulls: false) });
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var leaf = Leaf(reader, int96: true);
            // A flagged row the cut-short read never reaches
            var expected = Rows(TimestampPrecision.FlagRowGroup(reader, path, 0, new[] { leaf }), 40);
            var row = System.Array.IndexOf(expected, true, 1);
            Assert.True(row > 0, "the fixture flags no row after the first");
            TimestampPrecision.LevelsHookForTests = (string hookPath, PrecisionLeaf hookLeaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead) =>
            {
                if (hookPath == path)
                {
                    // Stops at the second row's first level
                    levels = repetitions.Slice(1, (int)levels - 1).IndexOf((short)0) + 1;
                    valuesRead = levels;
                }
            };
            try
            {
                using var cursor = new TimestampPrecision.RowGroupCursor(reader, path, 0, new[] { leaf });

                var error = Assert.Throws<InvalidDataException>(() => cursor.Flagged(row));

                Assert.Contains("ended after", error.Message);
            }
            finally
            {
                TimestampPrecision.LevelsHookForTests = null;
            }
        }

        [Fact]
        public void ARowAcrossTheBatchBoundaryKeepsItsFlag()
        {
            // Row 4094 holds levels 4094 to 4097, the third one inexact
            var path = "boundary.parquet";
            var whole = Int96(1_000, Day);
            var rows = Enumerable.Range(0, 4094).Select(_ => (Int96?[]?)new Int96?[] { whole }).ToList();
            rows.Add(new Int96?[] { whole, whole, Int96(1_001, Day), whole });
            rows.Add(new Int96?[] { whole });
            rows.Add(new Int96?[] { Int96(2_002, Day) });
            var file = WriteList(new[] { rows });
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var batches = new List<(long Levels, short FirstRepetition)>();
            TimestampPrecision.LevelsHookForTests = (string hookPath, PrecisionLeaf leaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead) =>
            {
                if (hookPath == path)
                {
                    batches.Add((levels, repetitions[0]));
                }
            };
            try
            {
                var flags = TimestampPrecision.FlagRowGroup(reader, path, 0, new[] { Leaf(reader, int96: true) });

                // The second batch opens inside row 4094
                Assert.Equal(new (long, short)[] { (4096, 0), (4, 1) }, batches);
                var flagged = Rows(flags, rows.Count);
                Assert.Equal(new[] { 4094, 4096 }, Enumerable.Range(0, rows.Count).Where(r => flagged[r]));
            }
            finally
            {
                TimestampPrecision.LevelsHookForTests = null;
            }
        }

        [Fact]
        public void NanosecondInt64FlagsSubMicrosecondRows()
        {
            var file = WriteNanos(new long?[] { 1_000, 1_001, null, -2_000, -2_001, 5_000_000 });
            using var reader = new ParquetFileReader(new MemoryStream(file));

            var flags = TimestampPrecision.FlagRowGroup(reader, "nanos.parquet", 0, new[] { Leaf(reader, int96: false) });

            Assert.NotNull(flags);
            Assert.Equal(0b10010UL, flags[0]);
        }

        [Fact]
        public void RequiredLeafCountsEveryLevelAsDefined()
        {
            var path = "required.parquet";
            var file = WriteNanos(new long?[] { 1_000, 1_001, -3_000, -2_002, 5_000_000 }, required: true);
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var leaf = Leaf(reader, int96: false);
            Assert.Equal(0, leaf.MaxDefinitionLevel);
            // Unwritten pooled levels may hold an earlier read's levels
            TimestampPrecision.LevelsHookForTests = (string hookPath, PrecisionLeaf hookLeaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead) =>
            {
                if (hookPath == path)
                {
                    definitions.Fill(7);
                    repetitions.Fill(7);
                }
            };
            try
            {
                var flags = TimestampPrecision.FlagRowGroup(reader, path, 0, new[] { leaf });

                Assert.NotNull(flags);
                Assert.Equal(0b01010UL, flags[0]);
            }
            finally
            {
                TimestampPrecision.LevelsHookForTests = null;
            }
        }

        [Fact]
        public void SeveralLeavesJoinTheirFlags()
        {
            // List flags rows 1 and 3, nanos rows 3 and 4
            var whole = Int96(1_000, Day);
            var list = new List<Int96?[]?> { new Int96?[] { whole }, new Int96?[] { whole, Int96(1_001, Day) }, null, new Int96?[] { Int96(1, Day) }, new Int96?[0], new Int96?[] { whole, null } };
            var file = WriteListAndNanos(list, new long[] { 1_000, 2_000, 3_000, 4_001, -5_001, 6_000 });
            using var reader = new ParquetFileReader(new MemoryStream(file));
            var listLeaf = Leaf(reader, int96: true, column: 0);
            var nanosLeaf = Leaf(reader, int96: false, column: 1);

            Assert.Equal(0b01010UL, TimestampPrecision.FlagRowGroup(reader, "both.parquet", 0, new[] { listLeaf })![0]);
            Assert.Equal(0b11000UL, TimestampPrecision.FlagRowGroup(reader, "both.parquet", 0, new[] { nanosLeaf })![0]);
            Assert.Equal(0b11010UL, TimestampPrecision.FlagRowGroup(reader, "both.parquet", 0, new[] { listLeaf, nanosLeaf })![0]);
            Assert.Equal(0b11010UL, TimestampPrecision.FlagRowGroup(reader, "both.parquet", 0, new[] { nanosLeaf, listLeaf })![0]);
        }

        [Fact]
        public void MalformedInt96IsFlagged()
        {
            // A whole day of nanos, then days past the microsecond range
            var file = WriteInt96(new Int96?[] { Int96(0, Day), Int96(86_400_000_000_000, Day), Int96(1_000, int.MaxValue), null, Int96(1_000, -1) });
            using var reader = new ParquetFileReader(new MemoryStream(file));

            var flags = TimestampPrecision.FlagRowGroup(reader, "malformed.parquet", 0, new[] { Leaf(reader, int96: true) });

            Assert.NotNull(flags);
            Assert.Equal(0b10110UL, flags[0]);
        }

        [Fact]
        public void WholeMicrosecondsFlagNothing()
        {
            var file = WriteInt96(new Int96?[] { Int96(1_000, Day), null, Int96(86_399_999_999_000, Day - 1) });
            using var reader = new ParquetFileReader(new MemoryStream(file));

            Assert.Null(TimestampPrecision.FlagRowGroup(reader, "whole.parquet", 0, new[] { Leaf(reader, int96: true) }));
        }

        [Fact]
        public void ARowGroupClaimingTooManyRowsIsADecodeError()
        {
            // The first row is flagged, so a bitmap would be made
            var file = WriteInt96(Enumerable.Range(0, 1000).Select(i => (Int96?)Int96(i == 0 ? 1 : 0, Day)).ToArray());
            using var reader = new ParquetFileReader(new MemoryStream(WithRowGroupRows(file, 1L << 40)));

            var error = Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, "rows.parquet", 0, new[] { Leaf(reader, int96: true) }));

            Assert.Equal("Delta file rows.parquet cannot be decoded: timestamp leaf ts has 1000 levels for 1099511627776 rows", error.Message);
        }

        [Theory]
        [InlineData("definition above maximum", "definition level 4")]
        [InlineData("repetition above maximum", "repetition level 2")]
        [InlineData("first repetition not zero", "repetition level 1")]
        [InlineData("values not defined levels", "values for")]
        [InlineData("row group ends early", "ended after")]
        [InlineData("extra row", "more than its 40 rows")]
        [InlineData("no levels", "returned 0 levels and 0 values")]
        [InlineData("too many levels", "returned 4097 levels")]
        [InlineData("negative values", "and -1 values")]
        [InlineData("more values than levels", "levels and")]
        public void MalformedLevelsAreDecodeErrors(string corruption, string message)
        {
            var path = $"corrupt-{corruption}.parquet";
            var file = WriteList(new[] { ListRows(new Random(7), 40, nulls: false) });
            using var reader = new ParquetFileReader(new MemoryStream(file));
            TimestampPrecision.LevelsHookForTests = (string hookPath, PrecisionLeaf leaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead) =>
            {
                if (hookPath != path)
                {
                    return;
                }
                switch (corruption)
                {
                    case "definition above maximum": definitions[0] = 4; valuesRead--; break;
                    case "repetition above maximum": repetitions[1] = 2; break;
                    case "first repetition not zero": repetitions[0] = 1; break;
                    case "values not defined levels": valuesRead--; break;
                    // A continuation level starts a new row
                    case "extra row": repetitions[repetitions.Slice(1, (int)levels - 1).IndexOf((short)1) + 1] = 0; break;
                    case "no levels": levels = 0; valuesRead = 0; break;
                    case "too many levels": levels = 4097; break;
                    case "negative values": valuesRead = -1; break;
                    case "more values than levels": valuesRead = levels + 1; break;
                    // Stops at the second row's first level
                    default: levels = repetitions.Slice(1, (int)levels - 1).IndexOf((short)0) + 1; valuesRead = levels; break;
                }
            };
            try
            {
                var error = Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, path, 0, new[] { Leaf(reader, int96: true) }));

                Assert.Contains(path, error.Message);
                Assert.Contains("ts.list.element", error.Message);
                // The message formats numbers in the current culture
                Assert.Contains(message.Replace("-1", (-1).ToString(System.Globalization.CultureInfo.CurrentCulture)), error.Message);
            }
            finally
            {
                TimestampPrecision.LevelsHookForTests = null;
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ACorruptPageIsADecodeError(bool compressed)
        {
            var file = WriteInt96(Enumerable.Range(0, 2000).Select(i => (Int96?)Int96(i * 1000L, Day)).ToArray(), compressed ? Compression.Snappy : Compression.Uncompressed);
            if (compressed)
            {
                // Bad snappy data also reports as IOError
                file.AsSpan(64, 1000).Fill(0xFF);
            }
            else
            {
                // The first page header, the footer kept
                file[4] = 0xFF;
            }
            using var reader = new ParquetFileReader(new MemoryStream(file));

            var error = Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(reader, "corrupt-page.parquet", 0, new[] { Leaf(reader, int96: true) }));

            Assert.Contains("Delta file corrupt-page.parquet cannot be decoded: timestamp leaf ts fails in the native reader", error.Message);
            var native = Assert.IsType<ParquetException>(error.InnerException);
            Assert.Contains(compressed ? "IOError: Corrupt snappy compressed data" : "Deserializing page header failed", native.Message);
        }

        [Fact]
        public void ACorruptIndexIsADecodeErrorInTheRead()
        {
            // Two values alternate, so the indices pack as 0xAA
            var file = WriteNanos(Enumerable.Range(0, 2000).Select(i => (long?)(i % 2 * 1000)).ToArray(), required: true);
            // Bit width 1, a run header, then the packed indices
            var at = file.AsSpan().IndexOf(new byte[] { 0xAA, 0xAA, 0xAA, 0xAA }) - 2;
            Assert.Equal(1, file[at]);
            // Wider indices point past the dictionary
            file[at] = 8;
            using var corrupt = new ParquetFileReader(new MemoryStream(file));

            var error = Assert.Throws<InvalidDataException>(() => TimestampPrecision.FlagRowGroup(corrupt, "corrupt-index.parquet", 0, new[] { Leaf(corrupt, int96: false) }));

            Assert.Contains("Delta file corrupt-index.parquet cannot be decoded: timestamp leaf ts fails in the native reader", error.Message);
            Assert.IsType<ParquetException>(error.InnerException);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void AFailingStreamIsNoDecodeError(bool io)
        {
            var stream = new FailingStream(WriteInt96(new Int96?[] { Int96(1_000, Day) }));
            using var reader = new ParquetFileReader(stream);
            var leaf = Leaf(reader, int96: true);
            // The footer is read, the column chunk is not
            stream.Failure = io ? new IOException("disk gone") : new HttpRequestException("connection reset");

            var error = Assert.Throws<ParquetException>(() => TimestampPrecision.FlagRowGroup(reader, "stream.parquet", 0, new[] { leaf }));

            Assert.Contains(io ? "IOError: System.IO.IOException: disk gone" : "System.Net.Http.HttpRequestException: connection reset", error.Message);
        }

        private static PrecisionLeaf Leaf(ParquetFileReader reader, bool int96, int column = 0)
        {
            var descriptor = reader.FileMetaData.Schema.Column(column);
            return new PrecisionLeaf(column, int96, descriptor.MaxDefinitionLevel, descriptor.MaxRepetitionLevel);
        }

        // Rewrites the row group's row count in the footer
        private static byte[] WithRowGroupRows(byte[] file, long rows)
        {
            var footerLength = BitConverter.ToInt32(file, file.Length - 8);
            var footer = file.Length - 8 - footerLength;
            // 1000 as i64 fields: file rows, chunk values, group rows
            var matches = new List<int>();
            for (int i = footer; i + 2 < file.Length - 8; i++)
            {
                if (file[i] == 0x16 && file[i + 1] == 0xD0 && file[i + 2] == 0x0F)
                {
                    matches.Add(i + 1);
                }
            }
            Assert.Equal(3, matches.Count);
            var varint = new List<byte>();
            for (var zigzag = (ulong)rows << 1; ; zigzag >>= 7)
            {
                if (zigzag < 0x80)
                {
                    varint.Add((byte)zigzag);
                    break;
                }
                varint.Add((byte)(zigzag | 0x80));
            }
            var patched = file.Take(matches[2]).Concat(varint).Concat(file.Skip(matches[2] + 2)).ToArray();
            BitConverter.TryWriteBytes(patched.AsSpan(patched.Length - 8), footerLength + varint.Count - 2);
            return patched;
        }

        private static Int96 Int96(long nanos, int julianDay) => new Int96((int)nanos, (int)(nanos >> 32), julianDay);

        private static bool[] Rows(ulong[]? flags, int count)
        {
            return Enumerable.Range(0, count).Select(r => flags != null && (flags[r >> 6] & (1UL << (r & 63))) != 0).ToArray();
        }

        // Null lists, empty lists, null and present elements, some sub-microsecond
        private static List<Int96?[]?> ListRows(Random random, int count, bool nulls = true)
        {
            var rows = new List<Int96?[]?>();
            for (int i = 0; i < count; i++)
            {
                var kind = random.Next(10);
                if (nulls && kind == 0)
                {
                    rows.Add(null);
                    continue;
                }
                rows.Add(Enumerable.Range(0, nulls && kind == 1 ? 0 : random.Next(1, 6)).Select(_ => nulls && random.Next(6) == 0
                    ? (Int96?)null
                    : Int96(random.NextInt64(86_400_000_000) * 1000 + (random.Next(8) == 0 ? random.Next(1, 1000) : 0), Day + random.Next(100))).ToArray());
            }
            return rows;
        }

        // Null and empty outer lists around the rows above
        private static List<Int96?[]?[]?> NestedRows(Random random, int count)
        {
            var rows = new List<Int96?[]?[]?>();
            for (int i = 0; i < count; i++)
            {
                var kind = random.Next(10);
                rows.Add(kind == 0 ? null : kind == 1 ? new Int96?[]?[0] : ListRows(random, random.Next(1, 4)).ToArray());
            }
            return rows;
        }

        // Each row flagged when a present value is not whole microseconds
        private static async Task<List<bool[]>> NanosecondFlags(byte[] file, int[] groupRows)
        {
            var flags = new List<bool>();
            using var reader = ParquetFileReaders.Open(new MemoryStream(file));
            using var batches = reader.GetRecordBatchReader();
            RecordBatch? batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    var column = batch.Column(0);
                    for (int i = 0; i < column.Length; i++)
                    {
                        flags.Add(Inexact(column, i));
                    }
                }
            }
            return new List<bool[]> { flags.Take(groupRows[0]).ToArray(), flags.Skip(groupRows[0]).ToArray() }.Take(groupRows.Length).ToList();
        }

        private static bool Inexact(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return false;
            }
            if (array is ListArray list)
            {
                return Enumerable.Range(list.ValueOffsets[index], list.ValueOffsets[index + 1] - list.ValueOffsets[index]).Any(j => Inexact(list.Values, j));
            }
            return ((TimestampArray)array).GetValue(index)!.Value % 1000 != 0;
        }

        // An optional list of optional INT96, one row group per entry
        private static byte[] WriteList(IEnumerable<List<Int96?[]?>> groups)
        {
            using var element = new PrimitiveNode("element", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var list = new GroupNode("list", Repetition.Repeated, new Node[] { element });
            using var ts = new GroupNode("ts", Repetition.Optional, new Node[] { list }, LogicalType.List());
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { ts });
            return Write(schema, groups, WriteListColumn);
        }

        // The list above, then a required nanosecond column
        private static byte[] WriteListAndNanos(List<Int96?[]?> rows, long[] nanos)
        {
            using var element = new PrimitiveNode("element", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var list = new GroupNode("list", Repetition.Repeated, new Node[] { element });
            using var ts = new GroupNode("ts", Repetition.Optional, new Node[] { list }, LogicalType.List());
            using var required = new PrimitiveNode("nanos", Repetition.Required, LogicalType.Timestamp(true, TimeUnit.Nanos), PhysicalType.Int64);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { ts, required });
            return Write(schema, new[] { rows }, (writer, values) =>
            {
                if (writer.ColumnIndex == 0)
                {
                    WriteListColumn(writer, values);
                }
                else
                {
                    ((ColumnWriter<long>)writer).WriteBatch(nanos);
                }
            });
        }

        private static void WriteListColumn(ColumnWriter writer, List<Int96?[]?> rows)
        {
            var definitions = new List<short>();
            var repetitions = new List<short>();
            var values = new List<Int96>();
            foreach (var row in rows)
            {
                if (row == null || row.Length == 0)
                {
                    definitions.Add(row == null ? (short)0 : (short)1);
                    repetitions.Add(0);
                    continue;
                }
                for (int i = 0; i < row.Length; i++)
                {
                    definitions.Add(row[i] == null ? (short)2 : (short)3);
                    repetitions.Add(i == 0 ? (short)0 : (short)1);
                    if (row[i] != null)
                    {
                        values.Add(row[i]!.Value);
                    }
                }
            }
            ((ColumnWriter<Int96>)writer).WriteBatch(definitions.Count, definitions.ToArray(), repetitions.ToArray(), values.ToArray());
        }

        // An optional list of optional lists of optional INT96
        private static byte[] WriteNestedList(List<Int96?[]?[]?> rows)
        {
            using var element = new PrimitiveNode("element", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var innerRepeated = new GroupNode("list", Repetition.Repeated, new Node[] { element });
            using var innerList = new GroupNode("element", Repetition.Optional, new Node[] { innerRepeated }, LogicalType.List());
            using var outerRepeated = new GroupNode("list", Repetition.Repeated, new Node[] { innerList });
            using var ts = new GroupNode("ts", Repetition.Optional, new Node[] { outerRepeated }, LogicalType.List());
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { ts });
            return Write(schema, new[] { rows }, (writer, outer) =>
            {
                var definitions = new List<short>();
                var repetitions = new List<short>();
                var values = new List<Int96>();
                foreach (var row in outer)
                {
                    if (row == null || row.Length == 0)
                    {
                        definitions.Add(row == null ? (short)0 : (short)1);
                        repetitions.Add(0);
                        continue;
                    }
                    for (int i = 0; i < row.Length; i++)
                    {
                        var repetition = i == 0 ? (short)0 : (short)1;
                        var inner = row[i];
                        if (inner == null || inner.Length == 0)
                        {
                            definitions.Add(inner == null ? (short)2 : (short)3);
                            repetitions.Add(repetition);
                            continue;
                        }
                        for (int j = 0; j < inner.Length; j++)
                        {
                            definitions.Add(inner[j] == null ? (short)4 : (short)5);
                            repetitions.Add(j == 0 ? repetition : (short)2);
                            if (inner[j] != null)
                            {
                                values.Add(inner[j]!.Value);
                            }
                        }
                    }
                }
                ((ColumnWriter<Int96>)writer).WriteBatch(definitions.Count, definitions.ToArray(), repetitions.ToArray(), values.ToArray());
            });
        }

        internal static byte[] WriteInt96(Int96?[] rows, Compression compression = Compression.Uncompressed)
        {
            using var node = new PrimitiveNode("ts", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
            return Write(schema, new[] { rows }, (writer, values) =>
                ((ColumnWriter<Int96>)writer).WriteBatch(values.Length, values.Select(x => x == null ? (short)0 : (short)1).ToArray(), null, values.Where(x => x != null).Select(x => x!.Value).ToArray()), compression);
        }

        internal static byte[] WriteNanos(long?[] rows, bool required = false)
        {
            using var node = new PrimitiveNode("ts", required ? Repetition.Required : Repetition.Optional, LogicalType.Timestamp(true, TimeUnit.Nanos), PhysicalType.Int64);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
            return Write(schema, new[] { rows }, (writer, values) =>
                ((ColumnWriter<long>)writer).WriteBatch(values.Length, required ? null : values.Select(x => x == null ? (short)0 : (short)1).ToArray(), null, values.Where(x => x != null).Select(x => x!.Value).ToArray()));
        }

        // Writes every column of each row group
        private static byte[] Write<T>(GroupNode schema, IEnumerable<T> groups, Action<ColumnWriter, T> write, Compression compression = Compression.Uncompressed)
        {
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x.Compression(compression));
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                foreach (var group in groups)
                {
                    using var rowGroup = writer.AppendRowGroup();
                    for (int i = 0; i < writer.Schema.NumColumns; i++)
                    {
                        using var column = rowGroup.NextColumn();
                        write(column, group);
                    }
                }
                writer.Close();
            }
            return stream.ToArray();
        }

        // Fails every read once a failure is set
        private sealed class FailingStream : MemoryStream
        {
            public FailingStream(byte[] bytes)
                : base(bytes)
            {
            }

            public Exception? Failure { get; set; }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return Failure == null ? base.Read(buffer, offset, count) : throw Failure;
            }

            public override int Read(Span<byte> buffer)
            {
                return Failure == null ? base.Read(buffer) : throw Failure;
            }
        }
    }
}
