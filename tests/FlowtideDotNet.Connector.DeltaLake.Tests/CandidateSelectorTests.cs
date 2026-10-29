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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.Text;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CandidateSelectorTests
    {
        // Each kind a stored batch can hold, the integer widths share a kernel type
        private static readonly SchemaBaseType[] s_types =
        {
            new Delta.ByteType(), new Delta.ShortType(), new Delta.IntegerType(), new Delta.LongType(), new Delta.FloatType(), new Delta.DoubleType(),
            new Delta.DecimalType(38, 0), new Delta.DecimalType(38, 9), new Delta.StringType(), new Delta.BinaryType(), new Delta.BooleanType(),
            new Delta.TimestampType(), new Delta.DateType()
        };
        private static readonly double[] s_doubles = { -0.0, 0.0, -1.5, 1.5, 2.0, 1e-9, double.NaN, double.PositiveInfinity, double.NegativeInfinity };
        private static readonly string[] s_strings = { "", "a", "ab", "b", "abcdefghijklmnop", "abcdefghijklmnopq", "abcdefghijklmnopz", "abcdefghijklmnoq", "\u00e9", "\u00e9a" };
        // Bytes past 0x7f order unsigned
        private static readonly byte[][] s_binaries = { new byte[0], new byte[] { 0x01 }, new byte[] { 0x7f }, new byte[] { 0x80 }, new byte[] { 0xff }, new byte[] { 0x80, 0x00 }, Enumerable.Repeat((byte)0x90, 17).ToArray() };

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void TheSelectorKeepsEveryFileAnEntryMatches(int seed)
        {
            var random = new Random(seed);
            var described = 0;
            for (int round = 0; round < 150; round++)
            {
                var types = Enumerable.Range(0, random.Next(1, 4)).Select(_ => s_types[random.Next(s_types.Length)]).ToArray();
                var table = new Delta.StructType(types.Select((x, c) => new Delta.StructField($"c{c}", x, true, new Dictionary<string, object>())).ToList());
                var full = PruningLayout.Create(table, table.Fields.Select(x => x.Name).ToList(), types.Length);
                // A shrunk layout keeps the first cells at their offsets
                var layout = random.Next(4) == 0 ? full.Take(1) : full;
                var rows = Enumerable.Range(0, random.Next(0, 90)).Select(_ => types.Select(x => Value(random, x)).ToArray()).ToList();
                using var stored = StoredProbes.Store(table, rows);
                var probes = StoredProbes.Build(layout, stored);
                for (int file = 0; file < 25; file++)
                {
                    var row = Row(random, full, types);
                    var exact = Enumerable.Range(0, probes.Count).Any(entry => probes.Matches(row, entry));
                    AssertRanges(row, probes);

                    Assert.Equal(exact, CandidateSelector.Admits(row, probes, int.MaxValue, out var tested));
                    AssertSurvivors(row, probes, tested);
                    foreach (var threshold in new[] { 0, 16, 64 })
                    {
                        var admitted = CandidateSelector.Admits(row, probes, threshold, out var descriptor);
                        Assert.True(admitted || !exact, $"round {round} file {file} at T = {threshold} drops a file an entry matches");
                        AssertSurvivors(row, probes, descriptor);
                        // A tested file within the mask's size is described
                        if (admitted && (row[0] & PruningLayout.HasStatistics) != 0 && SmallestCount(row, probes) <= Math.Min(threshold, CandidateSelector.MaxDescribed))
                        {
                            Assert.NotNull(descriptor);
                            described++;
                        }
                    }
                }
            }
            Assert.True(described > 0, "no file was described");
        }

        [Fact]
        public void AFileOnlyAnticorrelatedEntriesFitIsRejected()
        {
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("a", new Delta.LongType(), true, new Dictionary<string, object>()),
                new Delta.StructField("b", new Delta.LongType(), true, new Dictionary<string, object>())
            });
            var layout = PruningLayout.Create(table, new[] { "a", "b" }, 2);
            using var stored = StoredProbes.Store(table, new[] { new IDataValue[] { new Int64Value(0), new Int64Value(1) }, new IDataValue[] { new Int64Value(1), new Int64Value(0) } });
            var probes = StoredProbes.Build(layout, stored);
            var row = new byte[layout.RowSize];
            row[0] = PruningLayout.HasStatistics;
            foreach (var column in layout.Columns)
            {
                PruningCell.WriteInt64(row.AsSpan(column.Offset, PruningCell.CellSize(column.Type)), column.Type, 0, 0, 0);
            }

            // Each column admits one entry, neither entry fits both
            Assert.False(CandidateSelector.Admits(row, probes, 1, out _));
            Assert.True(CandidateSelector.Admits(row, probes, 0, out var descriptor));
            Assert.Null(descriptor);
        }

        [Fact]
        public void DecimalLimitsAtScaleNineRemainOrdinaryOrderedProbes()
        {
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("c", new Delta.DecimalType(38, 9), true, new Dictionary<string, object>())
            });
            var layout = PruningLayout.Create(table, new[] { "c" }, 1);
            var values = new[] { decimal.MaxValue, decimal.MinValue };
            using var stored = StoredProbes.Store(table, values.Select(value => new IDataValue[] { new DecimalValue(value) }).ToArray());
            var probes = StoredProbes.Build(layout, stored);

            Assert.Empty(probes.Column(0).Special);
            Assert.Equal(new[] { 1, 0 }, probes.Column(0).Sorted);
            for (int entry = 0; entry < values.Length; entry++)
            {
                var row = new byte[layout.RowSize];
                row[0] = PruningLayout.HasStatistics;
                PruningCell.WriteDecimal(row.AsSpan(1), values[entry], values[entry], 0);
                Assert.True(probes.Matches(row, entry));
                Assert.False(probes.Matches(row, 1 - entry));
                Assert.True(CandidateSelector.Admits(row, probes, 1, out var descriptor));
                Assert.NotNull(descriptor);
                Assert.Equal(1UL, descriptor.Value.Survivors);
            }
        }

        [Fact]
        public void NumericColumnsCountTheirKeysAgainstTheCap()
        {
            // Eight bytes for each column that keeps sorted keys, none for decimals and bytes
            var table = new Delta.StructType(s_types.Select((x, c) => new Delta.StructField($"c{c}", x, true, new Dictionary<string, object>())).ToList());
            var layout = PruningLayout.Create(table, table.Fields.Select(x => x.Name).ToList(), s_types.Length);
            Assert.Equal(s_types.Length, layout.Columns.Count);
            Assert.Equal(8 * (s_types.Length - 4), ProbeBatch.KeyBytes(layout));
        }

        [Fact]
        public void EntriesReadTheirOwnStoredRow()
        {
            // Rows 0 and 1 store the same rounded value, so the batch keeps rows 0 and 2
            var table = new Delta.StructType(new[] { new Delta.StructField("d", new Delta.DecimalType(10, 2), true, new Dictionary<string, object>()) });
            using var stored = StoredProbes.Store(table, new[] { 1.004m, 1.0m, 7m }.Select(x => new IDataValue[] { new DecimalValue(x) }).ToList());
            var comparers = table.Fields.Select(x => x.Type.Accept(new Internal.Delta.ParquetFormat.Comparers.ArrowComparerVisitor())).ToArray();
            var batch = new Internal.Deletes.DeleteBatch(stored, new long[] { 1, 1, 1 }, comparers);
            Assert.Equal(new[] { 0, 2 }, batch.EntryRows.ToArray());
            var layout = PruningLayout.Create(table, new[] { "d" }, 1);
            var probes = ProbeBatch.Build(layout, stored, batch.EntryRows);

            Assert.Equal(2, probes.Count);
            Assert.Equal((true, false), (probes.Matches(DecimalRow(layout, 1m), 0), probes.Matches(DecimalRow(layout, 1m), 1)));
            Assert.Equal((false, true), (probes.Matches(DecimalRow(layout, 7m), 0), probes.Matches(DecimalRow(layout, 7m), 1)));
        }

        [Fact]
        public void ADecimalBeyondTheDecimalRangeIsTestedAsSpecial()
        {
            // An ordinary 1 and another engine's 2^100, which every cell admits
            var layout = new PruningLayout(new[] { new PruningColumn(0, 0, "c", PruningType.Decimal, 1) });
            var bytes = new byte[32];
            System.Buffers.Binary.BinaryPrimitives.WriteInt128LittleEndian(bytes.AsSpan(0, 16), Int128.One);
            System.Buffers.Binary.BinaryPrimitives.WriteInt128LittleEndian(bytes.AsSpan(16, 16), Int128.One << 100);
            var type = new Apache.Arrow.Types.Decimal128Type(38, 0);
            var decimals = new Apache.Arrow.Decimal128Array(new Apache.Arrow.ArrayData(type, 2, 0, 0, new[] { Apache.Arrow.ArrowBuffer.Empty, new Apache.Arrow.ArrowBuffer(bytes) }));
            using var stored = new Apache.Arrow.RecordBatch(new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("c", type, true)).Build(), new Apache.Arrow.IArrowArray[] { decimals }, 2);
            var probes = StoredProbes.Build(layout, stored);

            Assert.Equal(new[] { 0 }, probes.Column(0).Sorted);
            Assert.Equal(new[] { 1 }, probes.Column(0).Special);
            Assert.True(CandidateSelector.Admits(DecimalRow(layout, 5m), probes, 1, out var descriptor));
            Assert.Equal(new CandidateDescriptor(0, 1, 1, false, 1UL), descriptor);
        }

        private static byte[] DecimalRow(PruningLayout layout, decimal value)
        {
            var row = new byte[layout.RowSize];
            row[0] = PruningLayout.HasStatistics;
            PruningCell.WriteDecimal(row.AsSpan(1, PruningCell.CellSize(PruningType.Decimal)), value, value, 0);
            return row;
        }

        // As the selector counts each column
        private static int SmallestCount(ReadOnlySpan<byte> row, ProbeBatch probes)
        {
            var smallest = int.MaxValue;
            for (int c = 0; c < probes.Layout.Columns.Count; c++)
            {
                var column = probes.Layout.Columns[c];
                var cell = row.Slice(column.Offset, PruningCell.CellSize(column.Type));
                var values = probes.Column(c);
                var lower = values.Lower(cell);
                var upper = values.Upper(cell, lower);
                smallest = Math.Min(smallest, upper - lower + (PruningCell.MayHoldNull(cell) ? values.Nulls.Length : 0) + values.Special.Length);
            }
            return smallest;
        }

        // Each searched range holds exactly the ordinary entries its cell admits
        private static void AssertRanges(ReadOnlySpan<byte> row, ProbeBatch probes)
        {
            if ((row[0] & PruningLayout.HasStatistics) == 0)
            {
                return;
            }
            for (int c = 0; c < probes.Layout.Columns.Count; c++)
            {
                var column = probes.Layout.Columns[c];
                var cell = row.Slice(column.Offset, PruningCell.CellSize(column.Type));
                var values = probes.Column(c);
                var admitted = 0;
                foreach (var entry in values.Sorted)
                {
                    admitted += values.Admits(cell, entry) ? 1 : 0;
                }
                var lower = values.Lower(cell);
                Assert.Equal(admitted, values.Upper(cell, lower) - lower);
            }
        }

        // A described file tested exactly its enumerated entries
        private static void AssertSurvivors(ReadOnlySpan<byte> row, ProbeBatch probes, CandidateDescriptor? descriptor)
        {
            if (descriptor == null)
            {
                return;
            }
            var column = probes.Column(descriptor.Value.Column);
            var tested = column.Sorted[descriptor.Value.Lower..descriptor.Value.Upper].AsEnumerable();
            if (descriptor.Value.WithNulls)
            {
                tested = tested.Concat(column.Nulls);
            }
            var entries = tested.Concat(column.Special).ToList();
            Assert.InRange(entries.Count, 1, CandidateSelector.MaxDescribed);
            ulong expected = 0;
            for (int i = 0; i < entries.Count; i++)
            {
                if (probes.Matches(row, entries[i]))
                {
                    expected |= 1UL << i;
                }
            }
            Assert.Equal(expected, descriptor.Value.Survivors);
        }

        private static IDataValue Value(Random random, SchemaBaseType type)
        {
            if (random.Next(8) == 0)
            {
                return NullValue.Instance;
            }
            return type switch
            {
                Delta.ByteType or Delta.ShortType or Delta.IntegerType or Delta.LongType => new Int64Value(random.Next(-3, 4)),
                Delta.FloatType or Delta.DoubleType => new DoubleValue(s_doubles[random.Next(s_doubles.Length)]),
                Delta.DecimalType decimalType => new DecimalValue(Decimals(decimalType.Scale)[random.Next(Decimals(decimalType.Scale).Length)]),
                Delta.StringType => new StringValue(s_strings[random.Next(s_strings.Length)]),
                Delta.BinaryType => new BinaryValue(s_binaries[random.Next(s_binaries.Length)]),
                Delta.BooleanType => new BoolValue(random.Next(2) == 0),
                Delta.DateType => new TimestampTzValue(DateTime.UnixEpoch.AddDays(random.Next(-3, 4)).Ticks, 0),
                _ => new TimestampTzValue(DateTime.UnixEpoch.Ticks + random.Next(-3, 4) * TimeSpan.TicksPerMillisecond, 0)
            };
        }

        // The limits fit DECIMAL(38, 9) only after their trailing zeros go
        private static decimal[] Decimals(int scale)
        {
            return scale == 0 ? new[] { decimal.MinValue, -2m, 0m, 1m, 2m, decimal.MaxValue } : new[] { decimal.MinValue, -1.5m, -0.000000001m, 0m, 1.5m, 2.25m, decimal.MaxValue };
        }

        // Cells with and without bounds, null counts, inverted and truncated bounds, NaN and infinite bounds
        private static byte[] Row(Random random, PruningLayout layout, SchemaBaseType[] types)
        {
            var row = new byte[layout.RowSize];
            if (random.Next(10) == 0)
            {
                return row;
            }
            row[0] = PruningLayout.HasStatistics;
            for (int c = 0; c < layout.Columns.Count; c++)
            {
                var column = layout.Columns[c];
                var cell = row.AsSpan(column.Offset, PruningCell.CellSize(column.Type));
                if (random.Next(6) == 0)
                {
                    PruningCell.WriteUnknown(cell);
                    continue;
                }
                int? nulls = random.Next(3) switch { 0 => null, 1 => 0, _ => 2 };
                var hasMin = random.Next(5) != 0;
                var hasMax = random.Next(5) != 0;
                var inverted = random.Next(10) == 0;
                switch (column.Type)
                {
                    case PruningType.Int64:
                    case PruningType.Timestamp:
                        Bounds(random, () => (long)random.Next(-3, 4), hasMin, hasMax, inverted, out long? lowLong, out long? highLong);
                        PruningCell.WriteInt64(cell, column.Type, lowLong, highLong, nulls);
                        break;
                    case PruningType.Date:
                        Bounds(random, () => DateTime.UnixEpoch.AddDays(random.Next(-3, 4)).Ticks, hasMin, hasMax, inverted, out long? lowDate, out long? highDate);
                        PruningCell.WriteInt64(cell, column.Type, lowDate, highDate, nulls);
                        break;
                    case PruningType.Double:
                    case PruningType.Float32:
                        Bounds(random, () => s_doubles[random.Next(s_doubles.Length)], hasMin, hasMax, inverted, out double? lowDouble, out double? highDouble);
                        PruningCell.WriteDouble(cell, column.Type, lowDouble, highDouble, nulls);
                        break;
                    case PruningType.Decimal:
                        var decimals = Decimals(((Delta.DecimalType)types[c]).Scale);
                        Bounds(random, () => decimals[random.Next(decimals.Length)], hasMin, hasMax, inverted, out decimal? lowDecimal, out decimal? highDecimal);
                        PruningCell.WriteDecimal(cell, lowDecimal, highDecimal, nulls);
                        break;
                    case PruningType.Bool:
                        Bounds(random, () => random.Next(2) == 0, hasMin, hasMax, inverted, out bool? lowBool, out bool? highBool);
                        PruningCell.WriteBool(cell, lowBool, highBool, nulls);
                        break;
                    default:
                        var pool = column.Type == PruningType.String ? s_strings.Select(x => Encoding.UTF8.GetBytes(x)).ToArray() : s_binaries;
                        var low = hasMin ? pool[random.Next(pool.Length)] : null;
                        var high = hasMax ? pool[random.Next(pool.Length)] : null;
                        if (low != null && high != null && (low.AsSpan().SequenceCompareTo(high) > 0) != inverted)
                        {
                            (low, high) = (high, low);
                        }
                        PruningCell.WriteBytes(cell, low, high, nulls);
                        break;
                }
            }
            return row;
        }

        // Ordered bounds, sometimes missing or inverted
        private static void Bounds<T>(Random random, Func<T> next, bool hasMin, bool hasMax, bool inverted, out T? low, out T? high)
            where T : struct, IComparable<T>
        {
            var a = next();
            var b = next();
            if ((a.CompareTo(b) > 0) != inverted)
            {
                (a, b) = (b, a);
            }
            low = hasMin ? a : null;
            high = hasMax ? b : null;
        }
    }
}
