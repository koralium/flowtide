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
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Hash
{
    public class AppendColumnHashTests
    {
        
        private Column CreateColumn(params IDataValue[] values)
        {
            Column col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            for (int i = 0; i < values.Length; i++)
            {
                col.Add(values[i]);
            }
            return col;
        }

        private int[] CreateIndices(int length)
        {
            var r = new int[length];
            for (int i = 0; i <  length; i++)
            {
                r[i] = i;
            }
            return r;
        }

        private Xxh32RowState[] CreateHashStates(int length)
        {
            var r = new Xxh32RowState[length];
            for (int i = 0; i < length; i++)
            {
                r[i].Init();
            }
            return r;
        }

        private void CompareHashes(params IDataValue[] values)
        {
            var col = CreateColumn(values);
            CheckAppendHash(col, values.Length);
            CheckToHash(col, values.Length);
            CheckAppendToSingle(col, values.Length);
        }

        private void CheckAppendHash(IColumn column, int length)
        {
            var indices = CreateIndices(length);
            var states = CreateHashStates(length);
            var scratch = new int[length];
            var destination = new uint[length];

            column.AppendToXxHash32(indices, default, states, scratch);

            for (int i = 0; i < length; i++)
            {
                destination[i] = XxHash32Implementation.GetCurrentHashAsUInt32(ref states[i]);
            }

            XxHash32 expectedHasher = new XxHash32();
            for (int i = 0; i < length; i++)
            {
                expectedHasher.Reset();
                column.AddToHash(i, default, expectedHasher);
                var expected = expectedHasher.GetCurrentHashAsUInt32();
                Assert.Equal(expected, destination[i]);
            }
        }

        private void CheckToHash(IColumn column, int length)
        {
            var indices = CreateIndices(length);
            var scratch = new int[length];
            var destination = new uint[length];

            column.ToXxHash32(indices, default, destination, scratch);

            XxHash32 expectedHasher = new XxHash32();
            for (int i = 0; i < length; i++)
            {
                expectedHasher.Reset();
                column.AddToHash(i, default, expectedHasher);
                var expected = expectedHasher.GetCurrentHashAsUInt32();
                Assert.Equal(expected, destination[i]);
            }
        }

        private void CheckAppendToSingle(IColumn column, int length)
        {
            XxHash32 expectedHasher = new XxHash32();
            Xxh32RowState state = new Xxh32RowState();
            for (int i = 0; i < length; i++)
            {
                state.Init();
                expectedHasher.Reset();
                column.AddToHash(i, default, expectedHasher);
                column.AppendXxHash32Single(i, default, ref state);
                var actual = XxHash32Implementation.GetCurrentHashAsUInt32(ref state);
                var expected = expectedHasher.GetCurrentHashAsUInt32();
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void IntegerHashes()
        {
            CompareHashes(
                new Int64Value(3),
                new Int64Value(17),
                new Int64Value(8),
                new Int64Value(1093829),
                NullValue.Instance,
                new Int64Value(9999999999));
        }

        [Fact]
        public void StringHashes()
        {
            CompareHashes(
               new StringValue("abc"),
               new StringValue("qqweq"),
               NullValue.Instance,
               new StringValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
               );
        }

        [Fact]
        public void BinaryHashes()
        {
            CompareHashes(
               new BinaryValue(new byte[] {1,2,3}),
               new BinaryValue(new byte[] { 6, 5, 4 }),
               NullValue.Instance,
               new BinaryValue(new byte[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 })
               );
        }

        [Fact]
        public void BoolHashes()
        {
            CompareHashes(
                new BoolValue(true),
                NullValue.Instance,
                new BoolValue(false)
               );
        }

        [Fact]
        public void DecimalHashes()
        {
            CompareHashes(
                new DecimalValue(123.4m),
                new DecimalValue(123213131231m),
                NullValue.Instance,
                new DecimalValue(Decimal.MaxValue),
                new DecimalValue(Decimal.MinValue)
               );
        }

        // Indexed, theory serialization drops the sign of negative zero
        private static readonly (decimal canonical, decimal[] equalValues)[] EqualDecimals =
        [
            (1m, [1.0m, 1.00m, 1.0000000000000000000000000000m]),
            (0m, [0.00m, new decimal(0, 0, 0, true, 0), new decimal(0, 0, 0, true, 5)]),
            (-12.5m, [-12.50m, -12.5000m]),
            (100m, [100.0m, 100.000m]),
            (7922816251426433759354395033m, [decimal.Parse("7922816251426433759354395033.0", CultureInfo.InvariantCulture)])
        ];

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void EqualDecimalsHashEqual(int caseIndex)
        {
            var (canonical, equalValues) = EqualDecimals[caseIndex];
            var values = new IDataValue[equalValues.Length + 1];
            values[0] = new DecimalValue(canonical);
            for (int i = 0; i < equalValues.Length; i++)
            {
                // Different representation, same value
                Assert.Equal(canonical, equalValues[i]);
                Assert.NotEqual(decimal.GetBits(canonical), decimal.GetBits(equalValues[i]));
                values[i + 1] = new DecimalValue(equalValues[i]);
            }

            // Canonical value keeps the hash it had before
            XxHash32 expectedHasher = new XxHash32();
            new DecimalValue(canonical).AddToHash(expectedHasher);
            var expected = expectedHasher.GetCurrentHashAsUInt32();

            var col = CreateColumn(values);
            var length = values.Length;

            var destination = new uint[length];
            col.ToXxHash32(CreateIndices(length), default, destination, new int[length]);

            var states = CreateHashStates(length);
            col.AppendToXxHash32(CreateIndices(length), default, states, new int[length]);

            // Union column hashes through AppendXxHash32Single
            var unionCol = CreateColumn(values.Append(new StringValue("x")).ToArray());

            Xxh32RowState state = new Xxh32RowState();
            for (int i = 0; i < length; i++)
            {
                Assert.Equal(expected, destination[i]);
                Assert.Equal(expected, XxHash32Implementation.GetCurrentHashAsUInt32(ref states[i]));

                state.Init();
                unionCol.AppendXxHash32Single(i, default, ref state);
                Assert.Equal(expected, XxHash32Implementation.GetCurrentHashAsUInt32(ref state));
            }
        }

        [Fact]
        public void DoubleHashes()
        {
            CompareHashes(
                new DoubleValue(123.4),
                new DoubleValue(123213131231),
                NullValue.Instance,
                new DoubleValue(Double.MaxValue),
                new DoubleValue(Double.MinValue),
                new DoubleValue(Double.NegativeInfinity),
                new DoubleValue(Double.PositiveInfinity),
                new DoubleValue(Double.NegativeZero)
               );
        }

        [Fact]
        public void ListWithIntegersHashes()
        {
            CompareHashes(
                new ListValue(new Int64Value(1), new Int64Value(2), new Int64Value(3)),
                new ListValue(Array.Empty<IDataValue>()),
                new ListValue(new Int64Value(321)),
                NullValue.Instance
               );
        }

        [Fact]
        public void MapWithIntegersHashes()
        {
            CompareHashes(
                new MapValue(
                    new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3)),
                    new KeyValuePair<IDataValue, IDataValue>(new Int64Value(9), new Int64Value(7)),
                    new KeyValuePair<IDataValue, IDataValue>(NullValue.Instance, new Int64Value(3)),
                    new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), NullValue.Instance)
                ),
                NullValue.Instance
            );
        }

        [Fact]
        public void StructWithIntegersHashes()
        {
            var header = StructHeader.Create("col1", "col2");
            CompareHashes(
                new StructValue(header, new Int64Value(3), new Int64Value(7)),
                NullValue.Instance,
                new StructValue(header, new Int64Value(13), new Int64Value(17)),
                new StructValue(header, NullValue.Instance, new Int64Value(1))
            );
        }

        [Fact]
        public void TimestampHashes()
        {
            CompareHashes(
                new TimestampTzValue(DateTime.MinValue),
                new TimestampTzValue(DateTime.MaxValue),
                NullValue.Instance,
                new TimestampTzValue(DateTime.UtcNow)
            );
        }

        [Fact]
        public void UnionWithIntegersStringsHashes()
        {
            CompareHashes(
                new Int64Value(17),
                new StringValue("hello"),
                NullValue.Instance,
                new Int64Value(3),
                new StringValue("world")
            );
        }

        [Fact]
        public void AlwaysNullColumnHashes()
        {
            var col = new AlwaysNullColumn();
            CheckAppendHash(col, 5);
            CheckToHash(col, 5);
            CheckAppendToSingle(col, 5);
        }

        [Fact]
        public void ColumnWithOffsetHashes()
        {
            var inner = new Column(GlobalMemoryManager.Instance);
            inner.Add(new Int64Value(10));
            inner.Add(new Int64Value(20));
            inner.Add(new Int64Value(30));

            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(1);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            offsets.Add(0);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            offsets.Add(2);

            var col = new ColumnWithOffset(inner, offsets);
            CheckAppendHash(col, offsets.Count);
            CheckToHash(col, offsets.Count);
            CheckAppendToSingle(col, offsets.Count);
        }
    }
}
