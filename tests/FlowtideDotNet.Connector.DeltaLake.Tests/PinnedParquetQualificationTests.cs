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
using ParquetSharp;
using ParquetSharp.Arrow;
using System.Buffers.Binary;
using ArrowTimeUnit = Apache.Arrow.Types.TimeUnit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class PinnedParquetQualificationTests
    {
        private const int UnixEpochJulianDay = 2440588;

        // Shared like the sink's, with INT96 read as microseconds
        private static readonly ArrowReaderProperties s_properties = CreateProperties();

        // Both range ends, a pre-epoch sub-µs value and today
        private static readonly DateTime[] s_instants = new[]
        {
            new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(1969, 12, 31, 23, 59, 59, DateTimeKind.Utc).AddTicks(9_999_995),
            new DateTime(2026, 10, 24, 12, 34, 56, DateTimeKind.Utc).AddTicks(7_890_123),
            DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc),
        };

        [Fact]
        public async Task Int96ReadsAsExactMicroseconds()
        {
            var values = s_instants.Select(ToInt96).ToArray();
            var file = WriteInt96(values, rowsPerGroup: values.Length);

            using var reader = new FileReader(new MemoryStream(file), null, s_properties);
            var rows = await ReadRows(reader);

            Assert.Equal(s_instants.Select(ExpectedMicros), rows.Select(x => x.Micros));
            Assert.Equal(values, ReadRaw(reader, 0));
        }

        [Fact]
        public async Task RawReadMidFileLeavesArrowBatchesUnchanged()
        {
            var values = Enumerable.Range(0, 12).Select(i => ToInt96(s_instants[2].AddDays(i).AddTicks(i))).ToArray();
            var file = WriteInt96(values, rowsPerGroup: 4);
            using var independent = new FileReader(new MemoryStream(file), null, s_properties);
            var expected = await ReadRows(independent);

            var rows = new List<(long Micros, int Id)>();
            using var reader = new FileReader(new MemoryStream(file), null, s_properties);
            using var batches = reader.GetRecordBatchReader();
            AddRows(await batches.ReadNextRecordBatchAsync(), rows);
            AddRows(await batches.ReadNextRecordBatchAsync(), rows);
            // The second batch crossed into row group 1
            Assert.Equal(6, rows.Count);
            Assert.Equal(values[..4], ReadRaw(reader, 0));
            Assert.Equal(values[4..8], ReadRaw(reader, 1));
            Assert.Equal(values[8..], ReadRaw(reader, 2));
            RecordBatch? batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                AddRows(batch, rows);
            }

            Assert.Equal(expected, rows);
        }

        [Fact]
        public async Task Decimal128RawValuesRoundTrip()
        {
            var value = Int128.Parse("1000000000000000000000000000000");
            var raw = new byte[48];
            BinaryPrimitives.WriteInt128LittleEndian(raw, value);
            BinaryPrimitives.WriteInt128LittleEndian(raw.AsSpan(32), -value);
            var wide = new Decimal128Type(38, 0);
            var narrow = new Decimal128Type(31, 0);
            var schema = new Schema.Builder()
                .Field(f => f.Name("d").DataType(wide).Nullable(false))
                .Field(f => f.Name("n").DataType(narrow).Nullable(true))
                .Build();
            using var wideData = new ArrayData(wide, 3, 0, 0, new[] { ArrowBuffer.Empty, new ArrowBuffer(raw) });
            // A nullable narrower column with a null in the middle
            using var narrowData = new ArrayData(narrow, 3, 1, 0, new[] { new ArrowBuffer(new byte[] { 0b101 }), new ArrowBuffer(raw) });
            using var wideArray = new Decimal128Array(wideData);
            using var narrowArray = new Decimal128Array(narrowData);
            using var written = new RecordBatch(schema, new IArrowArray[] { wideArray, narrowArray }, 3);
            var file = WriteArrow(written);

            using var reader = new FileReader(new MemoryStream(file), null, s_properties);
            using var batches = reader.GetRecordBatchReader();
            using var batch = await batches.ReadNextRecordBatchAsync();
            var read = (Decimal128Array)batch.Column(0);
            var nullable = (Decimal128Array)batch.Column(1);

            Assert.Equal(3, batch.Length);
            Assert.Equal(raw, read.ValueBuffer.Span.Slice(read.Offset * 16, 48).ToArray());
            Assert.True(nullable.IsNull(1));
            Assert.Equal(raw[..16], nullable.ValueBuffer.Span.Slice(nullable.Offset * 16, 16).ToArray());
            Assert.Equal(raw[32..], nullable.ValueBuffer.Span.Slice((nullable.Offset + 2) * 16, 16).ToArray());
        }

        [Fact]
        public void UtcTimestampsWriteAdjustedToUtc()
        {
            var types = new[] { new TimestampType(ArrowTimeUnit.Millisecond, "UTC"), new TimestampType(ArrowTimeUnit.Microsecond, "UTC") };
            var schema = new Schema.Builder().Field(f => f.Name("ms").DataType(types[0])).Field(f => f.Name("us").DataType(types[1])).Build();
            var arrays = types.Select(x => (IArrowArray)new TimestampArray.Builder(x).Append(s_instants[2]).Build()).ToArray();
            using var written = new RecordBatch(schema, arrays, 1);
            var file = WriteArrow(written);

            using var reader = new ParquetFileReader(new MemoryStream(file), false);
            using var metadata = reader.FileMetaData;
            for (int i = 0; i < types.Length; i++)
            {
                using var logicalType = metadata.Schema.Column(i).LogicalType;
                Assert.True(Assert.IsType<TimestampLogicalType>(logicalType).IsAdjustedToUtc, schema.FieldsList[i].Name);
            }
        }

        private static ArrowReaderProperties CreateProperties()
        {
            var properties = ArrowReaderProperties.GetDefault();
            properties.PreBuffer = false;
            properties.CoerceInt96TimestampUnit = ArrowTimeUnit.Microsecond;
            // Batches of 3 cross the 4 row groups of Q2
            properties.BatchSize = 3;
            return properties;
        }

        private static Int96 ToInt96(DateTime instant)
        {
            var nanos = instant.TimeOfDay.Ticks * 100;
            return new Int96((int)nanos, (int)(nanos >> 32), UnixEpochJulianDay + (instant.Date - DateTime.UnixEpoch).Days);
        }

        private static long ExpectedMicros(DateTime instant)
        {
            return (instant.Date - DateTime.UnixEpoch).Days * 86_400_000_000L + instant.TimeOfDay.Ticks / 10;
        }

        private static byte[] WriteInt96(Int96[] values, int rowsPerGroup)
        {
            using var stream = new MemoryStream();
            using (var writer = new ParquetFileWriter(stream, new ParquetSharp.Column[] { new Column<Int96>("ts"), new Column<int>("id") }, leaveOpen: true))
            {
                for (int start = 0; start < values.Length; start += rowsPerGroup)
                {
                    var count = Math.Min(rowsPerGroup, values.Length - start);
                    using var rowGroup = writer.AppendRowGroup();
                    using (var timestamps = (ColumnWriter<Int96>)rowGroup.NextColumn())
                    {
                        timestamps.WriteBatch(values.AsSpan(start, count));
                    }
                    using (var ids = rowGroup.NextColumn().LogicalWriter<int>())
                    {
                        ids.WriteBatch(Enumerable.Range(start, count).ToArray());
                    }
                }
                writer.Close();
            }
            return stream.ToArray();
        }

        private static byte[] WriteArrow(RecordBatch batch)
        {
            using var stream = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, batch.Schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            return stream.ToArray();
        }

        private static Int96[] ReadRaw(FileReader reader, int rowGroup)
        {
            using var group = reader.ParquetReader.RowGroup(rowGroup);
            using var column = (ColumnReader<Int96>)group.Column(0);
            var values = new Int96[group.MetaData.NumRows];
            var read = 0;
            while (read < values.Length)
            {
                var count = column.ReadBatch(values.Length - read, values.AsSpan(read), out _);
                Assert.True(count > 0, $"Row group {rowGroup} ended after {read} of {values.Length} values");
                read += (int)count;
            }
            return values;
        }

        private static async Task<List<(long Micros, int Id)>> ReadRows(FileReader reader)
        {
            var rows = new List<(long Micros, int Id)>();
            using var batches = reader.GetRecordBatchReader();
            RecordBatch? batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                AddRows(batch, rows);
            }
            return rows;
        }

        private static void AddRows(RecordBatch batch, List<(long Micros, int Id)> rows)
        {
            using (batch)
            {
                var timestamps = (TimestampArray)batch.Column(0);
                Assert.Equal(ArrowTimeUnit.Microsecond, ((TimestampType)timestamps.Data.DataType).Unit);
                var ids = (Int32Array)batch.Column(1);
                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add((timestamps.GetValue(i)!.Value, ids.GetValue(i)!.Value));
                }
            }
        }
    }
}
