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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class StringStatisticsTruncationTests
    {
        // The min and max the comparer writes, null when absent
        private static (string? Min, string? Max) Written(string? min, string? max)
        {
            return Written(new StringStatisticsComparer(min == null ? null : Encoding.UTF8.GetBytes(min), max == null ? null : Encoding.UTF8.GetBytes(max), 0));
        }

        private static (string? Min, string? Max) Written(IStatisticsComparer comparer)
        {
            using var buffer = new MemoryStream();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                comparer.WriteMinValue(writer, "min");
                comparer.WriteMaxValue(writer, "max");
                writer.WriteEndObject();
            }
            using var document = JsonDocument.Parse(buffer.ToArray());
            var root = document.RootElement;
            return (root.TryGetProperty("min", out var a) ? a.GetString() : null, root.TryGetProperty("max", out var b) ? b.GetString() : null);
        }

        private static bool Bounds(string? min, string? max, string value)
        {
            var comparer = new StringStatisticsComparer(min == null ? null : Encoding.UTF8.GetBytes(min), max == null ? null : Encoding.UTF8.GetBytes(max), 0);
            return comparer.IsInBetween(new StringValue(value));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ExtremesOutliveTheirSourceAndTheWritersGrowth(bool binary)
        {
            IParquetWriter writer = binary ? new ParquetBinaryWriter() : new ParquetStringWriter();
            writer.NewBatch();
            var sources = new List<byte[]>();
            void Write(string value)
            {
                var bytes = Encoding.UTF8.GetBytes(value);
                sources.Add(bytes);
                _ = binary ? writer.WriteValue(new BinaryValue(bytes)) : writer.WriteValue(new StringValue(bytes));
            }
            // Each extreme grows, then shrinks, past many appends
            Write("m");
            Write(new string('a', 30));
            Write(new string('z', 30));
            for (int i = 0; i < 10_000; i++)
            {
                Write($"k{i:D5}");
            }
            Write("a");
            Write("{");
            // The source pages are released and reused
            foreach (var bytes in sources)
            {
                Array.Fill(bytes, (byte)'~');
            }

            Assert.Equal(("a", "{"), Written(writer.GetStatisticsComparer()!));
        }

        [Fact]
        public void ShortValuesAreWrittenWhole()
        {
            var value = new string('a', StringStatisticsComparer.StatisticsPrefixLength);
            Assert.Equal((value, value), Written(value, value));
        }

        [Fact]
        public void LongValuesBecomePrefixBounds()
        {
            var min = "apple" + new string('x', 3_000_000);
            var max = "pear" + new string('y', 3_000_000);
            var (writtenMin, writtenMax) = Written(min, max);

            Assert.Equal(min.Substring(0, 32), writtenMin);
            Assert.Equal(max.Substring(0, 32) + char.ConvertFromUtf32(0x10FFFF), writtenMax);
            // Every value between the originals stays within the written bounds
            static int Compare(string x, string y) => Encoding.UTF8.GetBytes(x).AsSpan().SequenceCompareTo(Encoding.UTF8.GetBytes(y));
            foreach (var value in new[] { min, max, "apple", "banana", max.Substring(0, 32), max.Substring(0, 32) + "a", min + "a", max.Substring(0, 33) })
            {
                if (Compare(value, min) >= 0 && Compare(value, max) <= 0)
                {
                    Assert.True(Bounds(writtenMin, writtenMax, value), value.Substring(0, Math.Min(40, value.Length)));
                }
            }
            Assert.False(Bounds(writtenMin, writtenMax, "zebra"));
        }

        [Fact]
        public void TruncationIsIdempotent()
        {
            var first = Written("m" + new string('é', 100), "n" + new string('ü', 100));
            Assert.NotNull(first.Min);
            Assert.NotNull(first.Max);
            Assert.Equal(first, Written(first.Min, first.Max));
        }

        [Fact]
        public void PrefixNeverSplitsACodePoint()
        {
            // 31 ASCII characters, then four-byte and two-byte characters across the cut
            var value = new string('a', 31) + "\U0001F600" + "\U0001F600" + "é";
            var (min, max) = Written(value, value);
            Assert.Equal(new string('a', 31) + "\U0001F600", min);
            Assert.Equal(new string('a', 31) + "\U0001F600" + char.ConvertFromUtf32(0x10FFFF), max);
        }

        [Fact]
        public void LongBinaryStatisticsDoNotCarryTheWholeValue()
        {
            // A binary value that is valid UTF-8, such as a JSON document, would otherwise put its own add line over the action cap
            var value = Encoding.UTF8.GetBytes(new string('j', 5_000_000));
            var comparer = new BinaryStatisticsComparer(value, value, 0);
            using var buffer = new MemoryStream();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                comparer.WriteMinValue(writer, "min");
                comparer.WriteMaxValue(writer, "max");
                writer.WriteEndObject();
            }
            Assert.True(buffer.Length < 1024, $"{buffer.Length} bytes of statistics");
        }

        [Fact]
        public void MaximumIsOmittedWhenTheTieBreakerFollowsThePrefix()
        {
            // No bound of the form prefix + U+10FFFF covers a value that continues past it
            var max = new string('a', 32) + char.ConvertFromUtf32(0x10FFFF) + "b";
            var (writtenMin, writtenMax) = Written("a", max);
            Assert.Equal("a", writtenMin);
            Assert.Null(writtenMax);
        }
    }
}
