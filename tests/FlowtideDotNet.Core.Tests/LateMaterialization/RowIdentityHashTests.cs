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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowIdentityHashTests
    {
        private static ulong Hash(IDataValue value)
        {
            return RowIdentityHash.Add(RowIdentityHash.Seed, value);
        }

        private static StructValue Struct(string first, string second, IDataValue firstValue, IDataValue secondValue)
        {
            return new StructValue(StructHeader.Create(first, second), firstValue, secondValue);
        }

        private static MapValue Map(IDataValue key, IDataValue value)
        {
            return new MapValue(new KeyValuePair<IDataValue, IDataValue>(key, value));
        }

        // A value of a random kind, some of them nested
        private static IDataValue RandomValue(Random random, int depth)
        {
            switch (random.Next(depth > 1 ? 9 : 12))
            {
                case 0:
                    return NullValue.Instance;
                case 1:
                    return new BoolValue(random.Next(2) == 0);
                case 2:
                    return new Int64Value(random.Next(4));
                case 3:
                    return new DoubleValue(random.Next(5) switch { 0 => 0.0, 1 => -0.0, 2 => double.NaN, 3 => BitConverter.UInt64BitsToDouble(0xFFF8000000000123), _ => random.Next(3) });
                case 4:
                    return new StringValue(new string('a', random.Next(3)));
                case 5:
                    return new BinaryValue(new byte[random.Next(3)]);
                case 6:
                    return new DecimalValue(new decimal(random.Next(3) * (int)Math.Pow(10, random.Next(4)), 0, 0, random.Next(2) == 0, (byte)random.Next(4)));
                case 7:
                    return new TimestampTzValue(random.Next(3), random.Next(3) * 60);
                case 8:
                    return new RowRefValue((ulong)random.Next(3));
                case 9:
                    return new ListValue(Enumerable.Range(0, random.Next(3)).Select(x => RandomValue(random, depth + 1)).ToArray());
                case 10:
                    return Map(RandomValue(random, depth + 1), RandomValue(random, depth + 1));
                default:
                    return Struct(random.Next(2) == 0 ? "a" : "ab", random.Next(2) == 0 ? "c" : "bc", RandomValue(random, depth + 1), RandomValue(random, depth + 1));
            }
        }

        [Fact]
        public void DecimalsOfAnotherScaleHashAlike()
        {
            ulong one = Hash(new DecimalValue(1m));
            ulong zero = Hash(new DecimalValue(0m));

            Assert.Equal(one, Hash(new DecimalValue(1.0m)));
            Assert.Equal(one, Hash(new DecimalValue(1.0000000000000000000000000000m)));
            Assert.Equal(zero, Hash(new DecimalValue(0.000m)));
            Assert.Equal(zero, Hash(new DecimalValue(new decimal(0, 0, 0, true, 5))));
            Assert.Equal(Hash(new DecimalValue(123.45m)), Hash(new DecimalValue(123.4500m)));
            Assert.NotEqual(Hash(new DecimalValue(decimal.MaxValue)), Hash(new DecimalValue(decimal.MinValue)));
            Assert.Equal(Hash(new DecimalValue(7922816251426433759354395033m)), Hash(new DecimalValue(7922816251426433759354395033.0m)));
            Assert.NotEqual(one, Hash(new DecimalValue(10m)));
            Assert.NotEqual(one, Hash(new DecimalValue(-1m)));
            Assert.NotEqual(one, Hash(new DecimalValue(0.1m)));
            Assert.NotEqual(Hash(new DecimalValue(decimal.MaxValue)), Hash(new DecimalValue(decimal.MaxValue - 1)));
            Assert.NotEqual(Hash(new DecimalValue(new decimal(1, 0, 1, false, 0))), Hash(new DecimalValue(new decimal(1, 0, 2, false, 0))));
            Assert.NotEqual(Hash(new DecimalValue(new decimal(1, 0, 1, false, 0))), Hash(new DecimalValue(new decimal(1, 0, 0, false, 0))));
            Assert.Equal(Hash(new DecimalValue(new decimal(10, 0, 1, false, 3))), Hash(new DecimalValue(new decimal(100, 0, 10, false, 4))));
            Assert.Equal(Hash(new DecimalValue(1.5m)), Hash(new DecimalValue(1.5000000000000000000000000000m)));
            Assert.NotEqual(Hash(new DecimalValue(new decimal(1, 0, 1, true, 0))), Hash(new DecimalValue(new decimal(1, 0, 1, false, 0))));
            Assert.NotEqual(Hash(new DecimalValue(new decimal(1, 0, 1, false, 1))), Hash(new DecimalValue(new decimal(1, 0, 1, false, 0))));
        }

        [Fact]
        public void BothZerosAndEveryNaNHashAlike()
        {
            ulong zero = Hash(new DoubleValue(0.0));
            ulong nan = Hash(new DoubleValue(double.NaN));

            Assert.Equal(zero, Hash(new DoubleValue(-0.0)));
            Assert.Equal(nan, Hash(new DoubleValue(BitConverter.UInt64BitsToDouble(0x7FF8000000000001))));
            Assert.Equal(nan, Hash(new DoubleValue(BitConverter.UInt64BitsToDouble(0xFFF0000000000123))));
            Assert.NotEqual(zero, nan);
            Assert.NotEqual(zero, Hash(new DoubleValue(double.Epsilon)));
            Assert.NotEqual(Hash(new DoubleValue(1.5)), Hash(new DoubleValue(-1.5)));
            Assert.NotEqual(nan, Hash(new DoubleValue(double.PositiveInfinity)));
        }

        [Fact]
        public void TimestampsHashByTheirTicks()
        {
            ulong noon = Hash(new TimestampTzValue(1000, 0));

            Assert.Equal(noon, Hash(new TimestampTzValue(1000, 120)));
            Assert.NotEqual(noon, Hash(new TimestampTzValue(1001, 0)));
        }

        [Fact]
        public void TheSameBitsInAnotherTypeHashApart()
        {
            var hashes = new HashSet<ulong>()
            {
                Hash(NullValue.Instance),
                Hash(new BoolValue(true)),
                Hash(new Int64Value(1)),
                Hash(new DoubleValue(BitConverter.Int64BitsToDouble(1))),
                Hash(new DecimalValue(1m)),
                Hash(new TimestampTzValue(1, 0)),
                Hash(new RowRefValue(1)),
                Hash(new StringValue("\u0001")),
                Hash(new BinaryValue(new byte[] { 1 })),
                Hash(new ListValue(new Int64Value(1))),
                Hash(Map(new Int64Value(1), NullValue.Instance)),
                Hash(new BoolValue(false)),
                Hash(new Int64Value(0)),
                Hash(new StringValue("")),
                Hash(new BinaryValue(Array.Empty<byte>())),
                Hash(new ListValue(Array.Empty<IDataValue>()))
            };

            Assert.Equal(16, hashes.Count);
        }

        [Fact]
        public void LengthsKeepNeighboursApart()
        {
            Assert.NotEqual(
                Hash(new ListValue(new StringValue("ab"), new StringValue("c"))),
                Hash(new ListValue(new StringValue("a"), new StringValue("bc"))));
            Assert.NotEqual(
                Hash(new ListValue(new Int64Value(1), new Int64Value(2))),
                Hash(new ListValue(new ListValue(new Int64Value(1)), new Int64Value(2))));
            Assert.NotEqual(
                Hash(new ListValue(new ListValue(Array.Empty<IDataValue>()), new ListValue(new Int64Value(1)))),
                Hash(new ListValue(new ListValue(new Int64Value(1)), new ListValue(Array.Empty<IDataValue>()))));
            Assert.NotEqual(
                Hash(new ListValue(new ListValue(new Int64Value(1), new Int64Value(2)))),
                Hash(new ListValue(new ListValue(new Int64Value(1)), new Int64Value(2))));
            Assert.NotEqual(
                Hash(Map(new Int64Value(1), new Int64Value(2))),
                Hash(Map(new Int64Value(2), new Int64Value(1))));
            Assert.NotEqual(
                Hash(new BinaryValue(new byte[1])),
                Hash(new BinaryValue(new byte[2])));
            Assert.NotEqual(
                Hash(Struct("ab", "c", new Int64Value(1), new Int64Value(2))),
                Hash(Struct("a", "bc", new Int64Value(1), new Int64Value(2))));
            Assert.NotEqual(
                Hash(Struct("a", "b", new Int64Value(1), new Int64Value(2))),
                Hash(Struct("a", "b", new Int64Value(2), new Int64Value(1))));
        }

        [Fact]
        public void MapsOfAnotherLengthHashApart()
        {
            var one = new Int64Value(1);
            var two = new Int64Value(2);
            var three = new Int64Value(3);
            var four = new Int64Value(4);
            var five = new Int64Value(5);
            var oneToTwo = Map(one, two);
            var twoEntries = new MapValue(new KeyValuePair<IDataValue, IDataValue>(one, three), new KeyValuePair<IDataValue, IDataValue>(two, four));
            var twoLater = new MapValue(new KeyValuePair<IDataValue, IDataValue>(one, four), new KeyValuePair<IDataValue, IDataValue>(two, five));

            Assert.NotEqual(
                Hash(Map(twoEntries, five)),
                Hash(new MapValue(new KeyValuePair<IDataValue, IDataValue>(oneToTwo, four), new KeyValuePair<IDataValue, IDataValue>(three, five))));
            Assert.NotEqual(
                Hash(Map(three, twoLater)),
                Hash(new MapValue(new KeyValuePair<IDataValue, IDataValue>(three, four), new KeyValuePair<IDataValue, IDataValue>(oneToTwo, five))));
            Assert.NotEqual(Hash(new MapValue(Array.Empty<KeyValuePair<IDataValue, IDataValue>>())), Hash(oneToTwo));
            Assert.NotEqual(
                Hash(twoEntries),
                Hash(new MapValue(new KeyValuePair<IDataValue, IDataValue>(one, three), new KeyValuePair<IDataValue, IDataValue>(two, five))));
            Assert.NotEqual(
                Hash(twoEntries),
                Hash(new MapValue(new KeyValuePair<IDataValue, IDataValue>(one, three), new KeyValuePair<IDataValue, IDataValue>(three, four))));
        }

        [Fact]
        public void NestedValuesHashByWhatTheyHold()
        {
            Assert.Equal(
                Hash(new ListValue(new DecimalValue(1.0m), new DoubleValue(0.0), new TimestampTzValue(5, 0))),
                Hash(new ListValue(new DecimalValue(1.000m), new DoubleValue(-0.0), new TimestampTzValue(5, 60))));
            Assert.Equal(
                Hash(Map(new DecimalValue(2.50m), new DoubleValue(double.NaN))),
                Hash(Map(new DecimalValue(2.5m), new DoubleValue(BitConverter.UInt64BitsToDouble(0x7FF8000000000007)))));
            Assert.Equal(
                Hash(Struct("a", "b", new DecimalValue(3m), new ListValue(new DecimalValue(0m)))),
                Hash(Struct("a", "b", new DecimalValue(3.0m), new ListValue(new DecimalValue(0.00m)))));
        }

        [Fact]
        public void ValuesThatCompareEqualHashAlike()
        {
            var random = new Random(7);
            var values = Enumerable.Range(0, 3000).Select(x => RandomValue(random, 0)).ToArray();
            var hashes = values.Select(Hash).ToArray();
            int equal = 0;
            int unequalWithOneHash = 0;
            for (int i = 0; i < values.Length; i++)
            {
                for (int k = i + 1; k < values.Length; k++)
                {
                    if (DataValueComparer.CompareTo(values[i], values[k]) == 0)
                    {
                        equal++;
                        Assert.Equal(hashes[i], hashes[k]);
                    }
                    else if (hashes[i] == hashes[k])
                    {
                        unequalWithOneHash++;
                    }
                }
            }

            Assert.True(equal > 10_000, $"Only {equal} pairs compared equal.");
            Assert.Equal(0, unequalWithOneHash);
        }

        [Fact]
        public void ManyDistinctValuesGetHashesOfTheirOwn()
        {
            var hashes = new HashSet<ulong>();
            var low = new HashSet<ulong>();
            for (long i = 0; i < 200_000; i++)
            {
                hashes.Add(Hash(new Int64Value(i)));
                hashes.Add(Hash(new StringValue(i.ToString())));
                low.Add(Hash(new Int64Value(i)) & 0xFFFF);
            }

            Assert.Equal(400_000, hashes.Count);
            Assert.InRange(low.Count, 62_000, 63_000);
        }

        [Fact]
        public void ARowIsHashedOverItsPickedColumnsInTheirOrder()
        {
            using var numbers = Column.Create(GlobalMemoryManager.Instance);
            using var texts = Column.Create(GlobalMemoryManager.Instance);
            using var skipped = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 10; i++)
            {
                numbers.Add(new Int64Value(i));
                if (i % 3 == 0)
                {
                    texts.Add(NullValue.Instance);
                }
                else
                {
                    texts.Add(new StringValue("row " + i));
                }
                skipped.Add(new Int64Value(1000 + i));
            }
            var columns = new IColumn[] { numbers, skipped, texts };
            var picked = new int[] { 0, 2 };
            var hashes = new ulong[4];
            var swapped = new ulong[4];

            RowIdentityHash.HashRows(columns, picked, 3, hashes);
            RowIdentityHash.HashRows(columns, new int[] { 2, 0 }, 3, swapped);

            for (int i = 0; i < 4; i++)
            {
                ulong byHand = RowIdentityHash.Add(RowIdentityHash.Seed, numbers.GetValueAt(3 + i, default));
                byHand = RowIdentityHash.Add(byHand, texts.GetValueAt(3 + i, default));
                Assert.Equal(byHand, hashes[i]);
                Assert.Equal(byHand, RowIdentityHash.HashRow(columns, picked, 3 + i));
                Assert.NotEqual(hashes[i], swapped[i]);
            }
            Assert.Equal(4, hashes.Distinct().Count());
        }

        public static IEnumerable<object[]> KindsAndNulls()
        {
            var kinds = new string[] { "int8", "int16", "int32", "int64", "bool", "double", "string", "binary", "decimal", "timestamp", "reference", "list", "map", "struct", "mixed", "nothing" };
            foreach (var kind in kinds)
            {
                yield return new object[] { kind, 0 };
                yield return new object[] { kind, 3 };
                yield return new object[] { kind, 8 };
            }
        }

        private static IDataValue ValueOf(string kind, Random random)
        {
            switch (kind)
            {
                case "int8":
                    return new Int64Value(random.Next(-100, 100));
                case "int16":
                    return new Int64Value(random.Next(-30_000, 30_000));
                case "int32":
                    return new Int64Value(random.Next(int.MinValue, int.MaxValue));
                case "int64":
                    return new Int64Value(random.NextInt64(long.MinValue, long.MaxValue));
                case "bool":
                    return new BoolValue(random.Next(2) == 0);
                case "double":
                    return new DoubleValue(random.Next(6) switch { 0 => 0.0, 1 => -0.0, 2 => double.NaN, 3 => BitConverter.UInt64BitsToDouble(0xFFF8000000000123), _ => random.NextDouble() - 0.5 });
                case "string":
                    return new StringValue(new string('a', random.Next(40)) + random.Next(100));
                case "binary":
                    var bytes = new byte[random.Next(20)];
                    random.NextBytes(bytes);
                    return new BinaryValue(bytes);
                case "decimal":
                    return new DecimalValue(random.Next(4) switch
                    {
                        0 => new decimal(0, 0, 0, random.Next(2) == 0, (byte)random.Next(6)),
                        1 => new decimal(random.Next(), random.Next(), random.Next(), random.Next(2) == 0, (byte)random.Next(29)),
                        _ => new decimal(random.Next(1000) * 100, 0, 0, random.Next(2) == 0, (byte)random.Next(6))
                    });
                case "timestamp":
                    return new TimestampTzValue(random.NextInt64(1, long.MaxValue / 2), random.Next(3) * 60);
                case "reference":
                    return new RowRefValue((ulong)random.NextInt64());
                case "list":
                    return new ListValue(new Int64Value(random.Next(5)), new StringValue("x" + random.Next(5)));
                case "map":
                    return Map(new StringValue("k" + random.Next(3)), new Int64Value(random.Next(5)));
                case "struct":
                    return Struct("a", "b", new Int64Value(random.Next(5)), new DoubleValue(random.Next(5)));
                case "mixed":
                    return RandomValue(random, 0);
                default:
                    return NullValue.Instance;
            }
        }

        [Theory]
        [MemberData(nameof(KindsAndNulls))]
        public void AColumnHashesItsRowsAsTheirValuesHash(string kind, int nullsInTen)
        {
            var random = new Random(kind.Length * 31 + nullsInTen);
            var values = new IDataValue[300];
            var before = new IDataValue[300];
            using var first = Column.Create(GlobalMemoryManager.Instance);
            using var second = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < values.Length; i++)
            {
                before[i] = new Int64Value(random.Next(1000));
                values[i] = random.Next(10) < nullsInTen ? NullValue.Instance : ValueOf(kind, random);
                first.Add(before[i]);
                second.Add(values[i]);
            }
            var columns = new IColumn[] { first, second };
            var picked = new int[] { 0, 1 };
            var ranges = new (int start, int length)[] { (0, 300), (1, 299), (17, 100), (299, 1), (150, 0), (64, 64) };

            foreach (var (start, length) in ranges)
            {
                var hashes = new ulong[length];
                RowIdentityHash.HashRows(columns, picked, start, hashes);
                for (int i = 0; i < length; i++)
                {
                    ulong byHand = RowIdentityHash.Add(RowIdentityHash.Add(RowIdentityHash.Seed, before[start + i]), values[start + i]);
                    Assert.True(byHand == hashes[i], $"Row {start + i} of the range from {start} hashes apart from its value.");
                    Assert.Equal(byHand, RowIdentityHash.HashRow(columns, picked, start + i));
                }
            }
        }

        [Theory]
        [MemberData(nameof(KindsAndNulls))]
        public void HashingABatchAllocatesNothing(string kind, int nullsInTen)
        {
            var random = new Random(kind.Length * 17 + nullsInTen);
            using var column = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 1024; i++)
            {
                column.Add(random.Next(10) < nullsInTen ? NullValue.Instance : ValueOf(kind, random));
            }
            var columns = new IColumn[] { column };
            var picked = new int[] { 0 };
            var hashes = new ulong[1024];
            for (int i = 0; i < 32; i++)
            {
                RowIdentityHash.HashRows(columns, picked, 0, hashes);
                RowIdentityHash.HashRow(columns, picked, i);
            }

            long least = long.MaxValue;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < 16; i++)
                {
                    RowIdentityHash.HashRows(columns, picked, 0, hashes);
                    RowIdentityHash.HashRow(columns, picked, i);
                }
                least = Math.Min(least, GC.GetAllocatedBytesForCurrentThread() - before);
            }

            Assert.True(least == 0, $"{least} bytes were allocated for 16,384 rows of {kind}, at the least of five runs.");
        }

        [Fact]
        public void AValueOfAnUnknownTypeIsRefused()
        {
            var value = new DataValueContainer();
            value._type = ArrowTypeId.Union;

            var refused = Assert.Throws<NotSupportedException>(() => RowIdentityHash.Add(RowIdentityHash.Seed, value));

            Assert.Equal("A value of type Union has no identity hash.", refused.Message);
        }

        [Fact]
        public void RowsOutsideAColumnAreNotHashed()
        {
            using var column = Column.Create(GlobalMemoryManager.Instance);
            using var empty = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 5; i++)
            {
                column.Add(new Int64Value(i));
            }
            var columns = new IColumn[] { column, empty };
            var none = Array.Empty<ulong>();

            RowIdentityHash.HashRows(columns, new int[] { 0 }, 5, none);
            RowIdentityHash.HashRows(columns, new int[] { 1 }, 0, none);

            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRows(columns, new int[] { 0 }, 6, Array.Empty<ulong>()));
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRows(columns, new int[] { 0 }, 3, new ulong[3]));
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRows(columns, new int[] { 0 }, -1, new ulong[1]));
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRows(columns, new int[] { 1 }, 0, new ulong[1]));
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRow(columns, new int[] { 0 }, 5));
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRow(columns, new int[] { 1 }, 0));
        }

        [Fact]
        public void AColumnBehindOffsetsHashesItsRowsAsTheirValuesHash()
        {
            using var inner = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 20; i++)
            {
                inner.Add(new StringValue("row " + i));
            }
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var order = new int[] { 19, 3, ColumnWithOffset.NullValueIndex, 7, 7, 0 };
            foreach (int offset in order)
            {
                offsets.Add(offset);
            }
            var columns = new IColumn[] { new ColumnWithOffset(inner, offsets) };
            var hashes = new ulong[4];

            RowIdentityHash.HashRows(columns, new int[] { 0 }, 1, hashes);

            Assert.Equal(Hash(new StringValue("row 3")), hashes[0]);
            Assert.Equal(Hash(NullValue.Instance), hashes[1]);
            Assert.Equal(Hash(new StringValue("row 7")), hashes[2]);
            Assert.Equal(hashes[2], hashes[3]);
        }

        [Fact]
        public void ARowWithNoPickedColumnHasTheSeed()
        {
            using var numbers = Column.Create(GlobalMemoryManager.Instance);
            numbers.Add(new Int64Value(1));
            var hashes = new ulong[1];

            RowIdentityHash.HashRows(new IColumn[] { numbers }, Array.Empty<int>(), 0, hashes);

            Assert.Equal(RowIdentityHash.Seed, hashes[0]);
        }
    }
}
