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

using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Utils
{
    public class IntegerListTests
    {
        /// <summary>
        /// The list views one storage as different widths, that only works when every
        /// NativeList instantiation has the same layout.
        /// </summary>
        [Fact]
        public void NativeListLayoutIsTheSameForAllWidths()
        {
            Assert.Equal(Unsafe.SizeOf<NativeList<byte>>(), Unsafe.SizeOf<NativeList<sbyte>>());
            Assert.Equal(Unsafe.SizeOf<NativeList<byte>>(), Unsafe.SizeOf<NativeList<short>>());
            Assert.Equal(Unsafe.SizeOf<NativeList<byte>>(), Unsafe.SizeOf<NativeList<int>>());
            Assert.Equal(Unsafe.SizeOf<NativeList<byte>>(), Unsafe.SizeOf<NativeList<long>>());
        }

        [Fact]
        public void ValuesWrittenThroughOneWidthReadBackCorrectly()
        {
            IntegerList list = new IntegerList(4);
            for (int i = 0; i < 100; i++)
            {
                list.Add(i * 1000, GlobalMemoryManager.Instance);
            }

            Assert.Equal(100, list.Count);
            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(i * 1000, list.Get(i));
            }
            // 100 int32 values are 400 bytes.
            Assert.Equal(400, list.SlicedSpan.Length);

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void WideningPreservesValuesIncludingNegatives()
        {
            IntegerList list = new IntegerList(1);
            list.Add(-5, GlobalMemoryManager.Instance);
            list.Add(100, GlobalMemoryManager.Instance);
            list.Add(0, GlobalMemoryManager.Instance);

            list.Widen(2, GlobalMemoryManager.Instance);
            Assert.Equal(-5, list.Get(0));
            Assert.Equal(100, list.Get(1));
            Assert.Equal(16, list.BitWidth);

            list.Widen(8, GlobalMemoryManager.Instance);
            Assert.Equal(-5, list.Get(0));
            Assert.Equal(100, list.Get(1));
            Assert.Equal(0, list.Get(2));
            Assert.Equal(64, list.BitWidth);
            Assert.Equal(3 * 8, list.SlicedSpan.Length);

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void WidenDirectlyFromInt8ToInt64()
        {
            IntegerList list = new IntegerList(1);
            for (int i = -50; i < 50; i++)
            {
                list.Add(i, GlobalMemoryManager.Instance);
            }
            list.Widen(8, GlobalMemoryManager.Instance);

            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(i - 50, list.Get(i));
            }

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void AdoptionRoundTrip()
        {
            IntegerList source = new IntegerList(2);
            source.Add(short.MinValue, GlobalMemoryManager.Instance);
            source.Add(short.MaxValue, GlobalMemoryManager.Instance);
            var bytes = source.SlicedSpan.ToArray();

            var memory = ((IMemoryAllocator)GlobalMemoryManager.Instance).AllocateMemory(bytes.Length);
            bytes.CopyTo(memory.Span);
            IntegerList adopted = new IntegerList(memory, 2, 2);

            Assert.Equal(short.MinValue, adopted.Get(0));
            Assert.Equal(short.MaxValue, adopted.Get(1));

            source.Dispose(GlobalMemoryManager.Instance);
            adopted.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void CopyIsIndependent()
        {
            IntegerList list = new IntegerList(4);
            list.Add(42, GlobalMemoryManager.Instance);

            IntegerList copy = list.Copy(GlobalMemoryManager.Instance);
            list.Update(0, 43);

            Assert.Equal(43, list.Get(0));
            Assert.Equal(42, copy.Get(0));

            list.Dispose(GlobalMemoryManager.Instance);
            copy.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void ClearKeepsTheWidth()
        {
            IntegerList list = new IntegerList(4);
            list.Add(1, GlobalMemoryManager.Instance);
            list.Clear();

            Assert.Equal(0, list.Count);
            Assert.Equal(32, list.BitWidth);

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void DisposeTwiceIsSafe()
        {
            IntegerList list = new IntegerList(8);
            list.Add(1, GlobalMemoryManager.Instance);

            list.Dispose(GlobalMemoryManager.Instance);
            list.Dispose(GlobalMemoryManager.Instance);

            Assert.Equal(0, list.Count);
            Assert.Equal(0, list.ElementSize);
        }

        [Fact]
        public void DisposeOfDefaultIsSafe()
        {
            IntegerList list = default;
            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Theory]
        [InlineData(sbyte.MaxValue, 8)]
        [InlineData(sbyte.MinValue, 8)]
        [InlineData(sbyte.MaxValue + 1, 16)]
        [InlineData(short.MaxValue, 16)]
        [InlineData(short.MinValue, 16)]
        [InlineData(short.MaxValue + 1, 32)]
        [InlineData(int.MaxValue, 32)]
        [InlineData(int.MinValue, 32)]
        [InlineData((long)int.MaxValue + 1, 64)]
        [InlineData(long.MaxValue, 64)]
        [InlineData(long.MinValue, 64)]
        public void BoundaryValuesKeepTheSmallestWidth(long value, int expectedBitWidth)
        {
            using var column = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            column.Add(new FlowtideDotNet.Core.ColumnStore.Int64Value(value));

            Assert.Equal(expectedBitWidth, column.GetColumnSizeInfo().BitWidth);
            Assert.Equal(value, column.GetValueAt(0, default).AsLong);
        }

        [Fact]
        public void WidthDoesNotDependOnInsertionOrder()
        {
            using var first = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            first.Add(new FlowtideDotNet.Core.ColumnStore.Int64Value(sbyte.MaxValue));
            first.Add(new FlowtideDotNet.Core.ColumnStore.Int64Value(0));

            using var second = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            second.Add(new FlowtideDotNet.Core.ColumnStore.Int64Value(0));
            second.Add(new FlowtideDotNet.Core.ColumnStore.Int64Value(sbyte.MaxValue));

            Assert.Equal(first.GetColumnSizeInfo().BitWidth, second.GetColumnSizeInfo().BitWidth);
            Assert.Equal(8, first.GetColumnSizeInfo().BitWidth);
        }
    }
}
