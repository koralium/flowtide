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

using FlowtideDotNet.Core.ColumnStore.Hash;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Hash
{
    public class XxHash32Tests
    {
        [Theory]
        [InlineData(new int[] { 37 })]
        [InlineData(new int[] { 37, 42 })]
        [InlineData(new int[] { 37, 42, 99 })]
        [InlineData(new int[] { 37, 42, 99, 123 })]
        [InlineData(new int[] { 37, 42, 99, 123, 456 })]
        [InlineData(new int[] { 37, 42, 99, 123, 456, 789 })]
        public void HashMultipleInt32(int[] values)
        {
            Xxh32RowState state = new Xxh32RowState();
            state.Init();

            XxHash32 xxHash32 = new XxHash32();

            Span<byte> buffer = stackalloc byte[8];
            for (int i = 0; i < values.Length; i++) 
            {
                buffer.Clear();
                BinaryPrimitives.WriteInt64LittleEndian(buffer, values[i]);
                XxHash32Implementation.Append(buffer, ref state);
                xxHash32.Append(buffer);
            }

            var hashValue = XxHash32Implementation.GetCurrentHashAsUInt32(ref state);
            var expectedHashValue = xxHash32.GetCurrentHashAsUInt32();

            Assert.Equal(expectedHashValue, hashValue);
        }

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 37 })]
        [InlineData(new byte[] { 37, 42 })]
        [InlineData(new byte[] { 37, 42, 99 })]
        [InlineData(new byte[] { 37, 42, 99, 123 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20, 21 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20, 21, 22 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25 })]
        [InlineData(new byte[] { 37, 42, 99, 123, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26 })]
        public void HashByteArray(byte[] values)
        {
            Xxh32RowState state = new Xxh32RowState();
            state.Init();
            XxHash32 xxHash32 = new XxHash32();

            XxHash32Implementation.Append(values, ref state);
            xxHash32.Append(values);

            var hashValue = XxHash32Implementation.GetCurrentHashAsUInt32(ref state);
            var expectedHashValue = xxHash32.GetCurrentHashAsUInt32();

            Assert.Equal(expectedHashValue, hashValue);
        }

        [Theory]
        [InlineData(357)]
        [InlineData(10234)]
        [InlineData(102132)]
        public void HashLargeByteArray(int size)
        {
            byte[] arr = new byte[size];

            Random random = new Random(42);
            random.NextBytes(arr);

            HashByteArray(arr);
        }


        [Fact]
        public void HashOneShotMatchesForAllLengths()
        {
            byte[] arr = new byte[64];
            new Random(42).NextBytes(arr);

            for (int length = 0; length <= arr.Length; length++)
            {
                var span = arr.AsSpan(0, length);
                Assert.Equal(XxHash32.HashToUInt32(span), XxHash32Implementation.Hash(span));
            }
        }

        [Fact]
        public unsafe void HashOneShotEmptySpanWithNullPointer()
        {
            var expected = XxHash32.HashToUInt32(Array.Empty<byte>());

            Assert.Equal(expected, XxHash32Implementation.Hash(ReadOnlySpan<byte>.Empty));
            // Same shape as BinaryList.Get on unallocated memory
            Assert.Equal(expected, XxHash32Implementation.Hash(new Span<byte>(null, 0).Slice(0, 0)));
        }

        [Theory]
        [InlineData("1.00", "1")]
        [InlineData("12.50", "12.5")]
        [InlineData("-12.500", "-12.5")]
        [InlineData("0.000", "0")]
        [InlineData("-0.0", "0")]
        [InlineData("100", "100")]
        [InlineData("100.00", "100")]
        [InlineData("1.0000000000000000000000000000", "1")]
        [InlineData("0.0000000000000000000000000010", "0.000000000000000000000000001")]
        [InlineData("7922816251426433759354395033.0", "7922816251426433759354395033")]
        [InlineData("79228162514264337593543950335", "79228162514264337593543950335")]
        [InlineData("-79228162514264337593543950335", "-79228162514264337593543950335")]
        [InlineData("123.4", "123.4")]
        public void CanonicalizeDecimal(string input, string expected)
        {
            var value = decimal.Parse(input, CultureInfo.InvariantCulture);
            var expectedValue = decimal.Parse(expected, CultureInfo.InvariantCulture);

            var actual = XxHash32Implementation.CanonicalizeDecimal(value);

            Assert.Equal(decimal.GetBits(expectedValue), decimal.GetBits(actual));
        }

        [Fact]
        public void CanonicalizeNegativeZeroScaleZero()
        {
            var negativeZero = new decimal(0, 0, 0, true, 0);
            Assert.Equal(decimal.GetBits(0m), decimal.GetBits(XxHash32Implementation.CanonicalizeDecimal(negativeZero)));
        }

        [Fact]
        public void HashDecimalMatchesAppendDecimalAtAllOffsets()
        {
            var value = 12.50m;
            var canonicalBytes = new byte[16];
            MemoryMarshal.Write(canonicalBytes, 12.5m);

            Span<byte> prefix = stackalloc byte[15];
            prefix.Fill(7);

            for (int offset = 0; offset < 16; offset++)
            {
                Xxh32RowState state = new Xxh32RowState();
                state.Init();
                XxHash32Implementation.Append(prefix.Slice(0, offset), ref state);
                XxHash32Implementation.AppendDecimal(value, ref state);

                XxHash32 expected = new XxHash32();
                expected.Append(prefix.Slice(0, offset));
                expected.Append(canonicalBytes);

                Assert.Equal(expected.GetCurrentHashAsUInt32(), XxHash32Implementation.GetCurrentHashAsUInt32(ref state));
            }

            Assert.Equal(XxHash32.HashToUInt32(canonicalBytes), XxHash32Implementation.HashDecimal(value));
        }

        [Theory]
        [InlineData(3)]
        [InlineData(37)]
        public void IntToHash(long val)
        {
            XxHash32 xxHash32 = new XxHash32();

            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buffer, val);
            xxHash32.Append(buffer);
            var expected = xxHash32.GetCurrentHashAsUInt32();
            var actual = XxHash32Implementation.HashSingleLong(val);

            Assert.Equal(expected, actual);
        }
    }
}
