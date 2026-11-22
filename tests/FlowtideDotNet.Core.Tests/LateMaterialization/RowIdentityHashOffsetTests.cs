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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowIdentityHashOffsetTests
    {
        private static IDataValue ValueOf(string kind, int row)
        {
            var values = new IDataValue[8];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = new Int64Value(row + i);
            }
            return kind switch
            {
                "list" => new ListValue(values),
                "map" => new MapValue(
                    new KeyValuePair<IDataValue, IDataValue>(values[0], values[1]),
                    new KeyValuePair<IDataValue, IDataValue>(values[2], values[3]),
                    new KeyValuePair<IDataValue, IDataValue>(values[4], values[5]),
                    new KeyValuePair<IDataValue, IDataValue>(values[6], values[7])),
                "struct" => new StructValue(StructHeader.Create("a", "b", "c", "d", "e", "f", "g", "h"), values),
                _ => values[0]
            };
        }

        [Theory]
        [InlineData("scalar", false)]
        [InlineData("list", false)]
        [InlineData("map", false)]
        [InlineData("struct", false)]
        [InlineData("scalar", true)]
        [InlineData("list", true)]
        [InlineData("map", true)]
        [InlineData("struct", true)]
        public void WarmedOffsetHashingDoesNotAllocatePerRow(string kind, bool oneAtATime)
        {
            using var column = Column.Create(GlobalMemoryManager.Instance);
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 1024; i++)
            {
                column.Add(ValueOf(kind, i));
                offsets.Add(1023 - i);
            }
            var columns = new IColumn[] { new ColumnWithOffset(column, offsets) };
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
                for (int repeat = 0; repeat < 16; repeat++)
                {
                    if (oneAtATime)
                    {
                        for (int i = 0; i < hashes.Length; i++)
                        {
                            hashes[i] = RowIdentityHash.HashRow(columns, picked, i);
                        }
                    }
                    else
                    {
                        RowIdentityHash.HashRows(columns, picked, 0, hashes);
                    }
                }
                least = Math.Min(least, GC.GetAllocatedBytesForCurrentThread() - before);
            }

            Assert.True(least == 0, $"{least} bytes allocated over 16,384 offset rows of {kind}; oneAtATime={oneAtATime}.");
        }

        private static void CheckHashes(Column column, IReadOnlyList<IDataValue> values)
        {
            var columns = new IColumn[] { column };
            var picked = new int[] { 0 };
            var hashes = new ulong[values.Count];
            RowIdentityHash.HashRows(columns, picked, 0, hashes);
            for (int i = 0; i < values.Count; i++)
            {
                ulong expected = RowIdentityHash.Add(RowIdentityHash.Seed, values[i]);
                Assert.Equal(expected, hashes[i]);
                Assert.Equal(expected, RowIdentityHash.HashRow(columns, picked, i));
            }
        }

        [Fact]
        public void MutatedNestedAndMixedColumnsKeepScalarHashEquivalence()
        {
            using var column = Column.Create(GlobalMemoryManager.Instance);
            var values = new List<IDataValue>();
            var kinds = new string[] { "list", "map", "struct", "scalar" };
            for (int pass = 0; pass < 4; pass++)
            {
                for (int i = 0; i < 60; i++)
                {
                    IDataValue value = i % 9 == 0 ? NullValue.Instance : ValueOf(kinds[(pass + i) % kinds.Length], i);
                    values.Add(value);
                    column.Add(value);
                }
                CheckHashes(column, values);
                for (int i = 0; i < values.Count; i += 3)
                {
                    IDataValue value = i % 2 == 0 ? NullValue.Instance : new ListValue(ValueOf("map", i), ValueOf("struct", i));
                    values[i] = value;
                    column.UpdateAt(i, value);
                }
                for (int i = values.Count - 1; i >= 0; i -= 4)
                {
                    values.RemoveAt(i);
                    column.RemoveAt(i);
                }
                for (int i = 0; i < 10; i++)
                {
                    var value = ValueOf(kinds[i % kinds.Length], i + 1000);
                    values.Insert(i, value);
                    column.InsertAt(i, value);
                }
                CheckHashes(column, values);
                column.Clear();
                values.Clear();
                CheckHashes(column, values);
            }
        }

        [Theory]
        [InlineData("scalar")]
        [InlineData("list")]
        [InlineData("map")]
        [InlineData("struct")]
        public void NestedRowsBehindOffsetsHashAsTheirValuesOneRowAtATime(string kind)
        {
            using var column = Column.Create(GlobalMemoryManager.Instance);
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var expected = new List<ulong>();
            for (int i = 0; i < 64; i++)
            {
                column.Add(ValueOf(kind, i));
            }
            for (int i = 0; i < 64; i++)
            {
                int offset = i % 5 == 0 ? ColumnWithOffset.NullValueIndex : 63 - i;
                offsets.Add(offset);
                IDataValue value = offset == ColumnWithOffset.NullValueIndex ? NullValue.Instance : ValueOf(kind, offset);
                expected.Add(RowIdentityHash.Add(RowIdentityHash.Seed, value));
            }
            var columns = new IColumn[] { new ColumnWithOffset(column, offsets) };
            var picked = new int[] { 0 };
            var hashes = new ulong[63];

            RowIdentityHash.HashRows(columns, picked, 1, hashes);

            for (int i = 0; i < hashes.Length; i++)
            {
                Assert.Equal(expected[i + 1], hashes[i]);
                Assert.Equal(expected[i + 1], RowIdentityHash.HashRow(columns, picked, i + 1));
            }
        }


        [Fact]
        public void ChainedOffsetsPreserveNestedValuesNullsAndHashAccumulators()
        {
            var values = new IDataValue[]
            {
                new ListValue(new Int64Value(1), NullValue.Instance, new StringValue("a")),
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k"), new ListValue(new Int64Value(2)))),
                new StructValue(StructHeader.Create("s"), new IDataValue[] { new ListValue(new Int64Value(3), NullValue.Instance) }),
                NullValue.Instance,
                new DecimalValue(1.2300m),
                new ListValue(new StructValue(StructHeader.Create("s"), new IDataValue[] { new Int64Value(4) }))
            };
            using var inner = Column.Create(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                inner.Add(value);
            }
            var firstOrder = new int[] { 5, 3, -1, 2, 1, 0, 4 };
            var secondOrder = new int[] { 6, 0, 2, -1, 1, 4, 3, 5, 0 };
            using var firstOffsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            using var secondOffsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (int offset in firstOrder)
            {
                firstOffsets.Add(offset);
            }
            foreach (int offset in secondOrder)
            {
                secondOffsets.Add(offset);
            }
            using var prefix = Column.Create(GlobalMemoryManager.Instance);
            for (int row = 0; row < secondOrder.Length; row++)
            {
                prefix.Add(new Int64Value(100 + row));
            }
            var wrapped = new ColumnWithOffset(new ColumnWithOffset(inner, firstOffsets), secondOffsets);
            var columns = new IColumn[] { prefix, wrapped };
            var picked = new int[] { 0, 1 };
            var ranges = new (int start, int count)[] { (0, 9), (1, 7), (8, 1), (9, 0) };
            foreach (var range in ranges)
            {
                var hashes = new ulong[range.count];
                RowIdentityHash.HashRows(columns, picked, range.start, hashes);
                for (int i = 0; i < hashes.Length; i++)
                {
                    int row = range.start + i;
                    int first = secondOrder[row];
                    int second = first < 0 ? -1 : firstOrder[first];
                    IDataValue expectedValue = second < 0 ? NullValue.Instance : values[second];
                    ulong expected = RowIdentityHash.Add(RowIdentityHash.Seed, new Int64Value(100 + row));
                    expected = RowIdentityHash.Add(expected, expectedValue);
                    Assert.Equal(expected, hashes[i]);
                    Assert.Equal(expected, RowIdentityHash.HashRow(columns, picked, row));
                }
            }
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRows(columns, picked, 10, Array.Empty<ulong>()));
            Assert.Throws<ArgumentOutOfRangeException>(() => RowIdentityHash.HashRow(columns, picked, 9));
        }

    }
}
