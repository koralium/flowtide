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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class PruningKernelTests
    {
        private const int Cases = 4000;

        [Fact]
        public void Int64MatchesTheComparer()
        {
            var random = new Random(1);
            for (int i = 0; i < Cases; i++)
            {
                var (min, max) = Bounds(random, () => (long)random.Next(-5, 6));
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(8) == 0 ? NullValue.Instance : new Int64Value(random.Next(-7, 8));
                AssertSame(new Int64StatisticsComparer(min, max, nullCount), PruningType.Int64, probe, exact: true);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void FloatingPointMatchesTheComparer(bool isFloat32)
        {
            var random = new Random(isFloat32 ? 3 : 2);
            double[] values = { -0.0, 0.0, 1e-9, -1e-9, 0.1, 1.0 / 3, 0.5, 1.5, 1e40, -1e40, 2.0000000099, 2.0, 16777217, 0.30000001192092896 };
            for (int i = 0; i < Cases; i++)
            {
                var (min, max) = Bounds(random, () => values[random.Next(values.Length)]);
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(10) switch
                {
                    0 => NullValue.Instance,
                    1 => new DoubleValue(double.NaN),
                    2 => new Int64Value(random.Next(-2, 3)),
                    _ => new DoubleValue(values[random.Next(values.Length)] + (random.Next(3) - 1) * 5e-9)
                };
                AssertSame(new FloatStatisticsComparer(min, max, nullCount, isFloat32), isFloat32 ? PruningType.Float32 : PruningType.Double, probe, exact: true);
            }
        }

        [Fact]
        public void DecimalMatchesTheComparer()
        {
            var random = new Random(4);
            for (int i = 0; i < Cases; i++)
            {
                var (min, max) = Bounds(random, () => random.Next(-300, 300) / 100m);
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(8) == 0 ? NullValue.Instance : new DecimalValue(random.Next(-3100, 3100) / 1000m);
                AssertSame(new DecimalStatisticsComparer(min, max, nullCount), PruningType.Decimal, probe, exact: true);
            }
            // Probes inside and outside the epsilon, so the cell's copy of the epsilon follows the comparer's
            foreach (var bound in new[] { -1.5m, 0m, 2.25m })
            {
                foreach (var offset in new[] { -0.000000002m, -0.0000000005m, 0m, 0.0000000005m, 0.000000002m })
                {
                    AssertSame(new DecimalStatisticsComparer(bound, bound, 0), PruningType.Decimal, new DecimalValue(bound + offset), exact: true);
                }
            }
        }

        [Fact]
        public void DateMatchesTheComparer()
        {
            var random = new Random(5);
            var start = new DateTime(2024, 1, 1);
            for (int i = 0; i < Cases; i++)
            {
                var (min, max) = Bounds(random, () => start.AddDays(random.Next(0, 6)).AddHours(random.Next(0, 24)));
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(8) == 0 ? NullValue.Instance : new TimestampTzValue(start.AddDays(random.Next(-1, 7)).AddMinutes(random.Next(0, 1440)));
                AssertSame(new DateStatisticsComparer(min, max, nullCount), PruningType.Date, probe, exact: true);
            }
        }

        [Fact]
        public void TimestampMatchesTheComparer()
        {
            var random = new Random(6);
            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            for (int i = 0; i < Cases; i++)
            {
                // Sub millisecond ticks and offsets
                var (min, max) = Bounds(random, () => start.AddTicks(random.Next(0, 50_000)));
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(8) == 0
                    ? NullValue.Instance
                    : new TimestampTzValue(start.AddTicks(random.Next(-10_000, 60_000)).ToOffset(TimeSpan.FromMinutes(random.Next(-2, 3) * 60)));
                AssertSame(new TimestampStatisticsComparer(min, max, nullCount), PruningType.Timestamp, probe, exact: true);
            }
        }

        [Fact]
        public void BoolMatchesTheComparer()
        {
            var random = new Random(7);
            for (int i = 0; i < Cases; i++)
            {
                var (min, max) = Bounds(random, () => random.Next(2) == 1);
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(6) == 0 ? NullValue.Instance : new BoolValue(random.Next(2) == 1);
                AssertSame(new BoolStatisticsComparer(min, max, nullCount), PruningType.Bool, probe, exact: true);
            }
        }

        [Fact]
        public void StringMatchesTheComparerExactlyForShortBoundsAndNeverPrunesMore()
        {
            var random = new Random(8);
            string[] alphabet = { "a", "b", "z", "\0", "é", "€", "𝄞" };
            string Text(int maxLength)
            {
                var builder = new StringBuilder();
                var length = random.Next(0, maxLength + 1);
                while (Encoding.UTF8.GetByteCount(builder.ToString()) < length)
                {
                    builder.Append(alphabet[random.Next(alphabet.Length)]);
                }
                return builder.ToString();
            }
            for (int i = 0; i < Cases * 2; i++)
            {
                // Lengths around the 16 byte prefix, a shared start makes ties at the prefix likely
                var shared = random.Next(2) == 0 ? "aaaaaaaaaaaaaaa" : "";
                var (min, max) = RefBounds(random, () => Encoding.UTF8.GetBytes(shared + Text(5)));
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(8) == 0 ? NullValue.Instance : new StringValue(shared + Text(6));
                var exact = (min == null || min.Length <= PruningCell.PrefixLength) && (max == null || max.Length <= PruningCell.PrefixLength);
                AssertSame(new StringStatisticsComparer(min, max, nullCount), PruningType.String, probe, exact);
            }
        }

        [Fact]
        public void BinaryMatchesTheComparerExactlyForShortBoundsAndNeverPrunesMore()
        {
            var random = new Random(9);
            byte[] Bytes(int maxLength)
            {
                var bytes = new byte[random.Next(0, maxLength + 1)];
                for (int b = 0; b < bytes.Length; b++)
                {
                    bytes[b] = (byte)random.Next(0, 4);
                }
                return bytes;
            }
            for (int i = 0; i < Cases * 2; i++)
            {
                var (min, max) = RefBounds(random, () => Bytes(20));
                var nullCount = NullCount(random);
                IDataValue probe = random.Next(8) == 0 ? NullValue.Instance : new BinaryValue(Bytes(20));
                var exact = (min == null || min.Length <= PruningCell.PrefixLength) && (max == null || max.Length <= PruningCell.PrefixLength);
                AssertSame(new BinaryStatisticsComparer(min, max, nullCount), PruningType.Binary, probe, exact);
            }
        }

        [Theory]
        [InlineData("", "", "", true)]
        [InlineData("abcdefghijklmnop", "abcdefghijklmnop", "abcdefghijklmnopq", false)]
        [InlineData("abcdefghijklmnopq", "abcdefghijklmnopz", "abcdefghijklmnopa", true)]
        [InlineData("abcdefghijklmnopq", "abcdefghijklmnopz", "abcdefghijklmnoa", false)]
        [InlineData("a\0b", "a\0b", "a\0", false)]
        [InlineData("a\0", "a\0b", "a", false)]
        public void StringEdgeCases(string min, string max, string probe, bool expected)
        {
            var layout = new PruningLayout(new[] { new PruningColumn(0, "c", PruningType.String, 1) });
            var row = Row(layout, new StringStatisticsComparer(Encoding.UTF8.GetBytes(min), Encoding.UTF8.GetBytes(max), 0));
            var batch = new ProbeBatch(layout);
            batch.Add(new IDataValue[] { new StringValue(probe) });
            Assert.Equal(expected, batch.Matches(row, 0));
        }

        [Fact]
        public void FilesWithoutStatisticsMatchEveryProbe()
        {
            var layout = new PruningLayout(new[] { new PruningColumn(0, "c", PruningType.Int64, 1) });
            var row = new byte[layout.RowSize];
            layout.WriteRow(null, row);
            var batch = new ProbeBatch(layout);
            batch.Add(new IDataValue[] { NullValue.Instance });
            batch.Add(new IDataValue[] { new Int64Value(5) });

            Assert.True(batch.Matches(row, 0));
            Assert.True(batch.Matches(row, 1));
        }

        [Fact]
        public void AProbeTypeNoKernelAcceptsPassesTheColumn()
        {
            // The comparer threw on such a value, the projection lets it reach the file scan
            var layout = new PruningLayout(new[] { new PruningColumn(0, "c", PruningType.Double, 1) });
            var row = Row(layout, new FloatStatisticsComparer(1, 2, 0));
            var batch = new ProbeBatch(layout);
            batch.Add(new IDataValue[] { new StringValue("text") });

            Assert.True(batch.Matches(row, 0));
            Assert.Throws<InvalidOperationException>(() => new FloatStatisticsComparer(1, 2, 0).IsInBetween(new StringValue("text")));
        }

        private static (T? Min, T? Max) RefBounds<T>(Random random, Func<T> next)
            where T : class
        {
            var min = random.Next(5) == 0 ? null : next();
            var max = random.Next(5) == 0 ? null : next();
            return (min, max);
        }

        private static (T? Min, T? Max) Bounds<T>(Random random, Func<T> next)
            where T : struct
        {
            T? min = random.Next(5) == 0 ? null : next();
            T? max = random.Next(5) == 0 ? null : next();
            return (min, max);
        }

        private static int? NullCount(Random random)
        {
            return random.Next(3) switch { 0 => null, 1 => 0, _ => random.Next(1, 4) };
        }

        private static byte[] Row(PruningLayout layout, IStatisticsComparer comparer)
        {
            var row = new byte[layout.RowSize];
            var statistics = new DeltaStatistics() { ValueComparers = new Dictionary<string, IStatisticsComparer>(StringComparer.OrdinalIgnoreCase) { ["c"] = comparer } };
            layout.WriteRow(statistics, row);
            return row;
        }

        private static void AssertSame(IStatisticsComparer comparer, PruningType type, IDataValue probe, bool exact)
        {
            var expected = comparer.IsInBetween(probe);
            var layout = new PruningLayout(new[] { new PruningColumn(0, "c", type, 1) });
            var row = Row(layout, comparer);
            var batch = new ProbeBatch(layout);
            batch.Add(new[] { probe });
            var projected = batch.Matches(row, 0);
            if (exact)
            {
                Assert.Equal(expected, projected);
            }
            else if (expected)
            {
                // A truncated bound may keep more files, never fewer
                Assert.True(projected);
            }
        }
    }
}
