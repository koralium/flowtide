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
using FlowtideDotNet.Storage.Memory;
using Xunit;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ColumnPrefixSumByteSizesTests
    {
        [Fact]
        public void NullOnlyColumnDoesNotModifySizes()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.Equal(0, sizes[0]);
            Assert.Equal(0, sizes[1]);
            Assert.Equal(0, sizes[2]);
        }

        [Fact]
        public void Int64ColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(10));
            column.Add(new Int64Value(20));
            column.Add(new Int64Value(30));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void Int64ColumnPrefixSumHasUniformSteps()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));
            column.Add(new Int64Value(4));

            var indices = new int[] { 0, 1, 2, 3 };
            var sizes = new int[4];

            column.GetPrefixSumByteSizes(indices, sizes);

            var step = sizes[1] - sizes[0];
            for (int i = 2; i < sizes.Length; i++)
            {
                Assert.Equal(step, sizes[i] - sizes[i - 1]);
            }
        }

        [Fact]
        public void StringColumnPrefixSumGrowsByVariableAmounts()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("a"));
            column.Add(new StringValue("hello"));
            column.Add(new StringValue("world!!!!"));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void BoolColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] >= sizes[0]);
            Assert.True(sizes[2] >= sizes[1]);
        }

        [Fact]
        public void DoubleColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(1.5));
            column.Add(new DoubleValue(2.5));
            column.Add(new DoubleValue(3.5));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void DecimalColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DecimalValue(1.23m));
            column.Add(new DecimalValue(4.56m));
            column.Add(new DecimalValue(7.89m));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void TimestampColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new TimestampTzValue(100L, 0));
            column.Add(new TimestampTzValue(200L, 0));
            column.Add(new TimestampTzValue(300L, 0));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void BinaryColumnPrefixSumGrowsByVariableAmounts()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BinaryValue(new byte[] { 1 }));
            column.Add(new BinaryValue(new byte[] { 2, 3, 4, 5 }));
            column.Add(new BinaryValue(new byte[] { 6, 7 }));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void ListColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));
            column.Add(new ListValue(new IDataValue[] { new Int64Value(3) }));
            column.Add(new ListValue(new IDataValue[] { new Int64Value(4), new Int64Value(5), new Int64Value(6) }));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void MapColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k1"), new Int64Value(1))
                }
            ));
            column.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k2"), new Int64Value(2)),
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k3"), new Int64Value(3))
                }
            ));

            var indices = new int[] { 0, 1 };
            var sizes = new int[2];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
        }

        [Fact]
        public void StructColumnPrefixSumIsMonotonicallyIncreasing()
        {
            var header = StructHeader.Create("name", "age");
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StructValue(header, new IDataValue[] { new StringValue("Alice"), new Int64Value(30) }));
            column.Add(new StructValue(header, new IDataValue[] { new StringValue("Bob"), new Int64Value(25) }));

            var indices = new int[] { 0, 1 };
            var sizes = new int[2];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
        }

        [Fact]
        public void UnionColumnPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new Int64Value(2));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void SingleElementColumnHasNonZeroSize()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(42));

            var indices = new int[] { 0 };
            var sizes = new int[1];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
        }

        [Fact]
        public void EmptyIndicesDoesNotModifySizes()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));

            var indices = Array.Empty<int>();
            var sizes = Array.Empty<int>();

            column.GetPrefixSumByteSizes(indices, sizes);
        }

        [Fact]
        public void SubsetIndicesProducesCorrectPrefixSum()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));
            column.Add(new Int64Value(4));
            column.Add(new Int64Value(5));

            var allIndices = new int[] { 0, 1, 2, 3, 4 };
            var allSizes = new int[5];
            column.GetPrefixSumByteSizes(allIndices, allSizes);

            var subsetIndices = new int[] { 0, 2, 4 };
            var subsetSizes = new int[3];
            column.GetPrefixSumByteSizes(subsetIndices, subsetSizes);

            Assert.True(subsetSizes[0] > 0);
            Assert.True(subsetSizes[1] > subsetSizes[0]);
            Assert.True(subsetSizes[2] > subsetSizes[1]);
        }

        [Fact]
        public void PrefixSumAddsToExistingValues()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            var indices = new int[] { 0, 1 };
            var sizes = new int[] { 100, 200 };

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 100);
            Assert.True(sizes[1] > 200);
        }

        [Fact]
        public void Int64WithNullsPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(3));
            column.Add(NullValue.Instance);

            var indices = new int[] { 0, 1, 2, 3 };
            var sizes = new int[4];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] >= sizes[0]);
            Assert.True(sizes[2] >= sizes[1]);
            Assert.True(sizes[3] >= sizes[2]);
        }

        [Fact]
        public void StringWithNullsPrefixSumIsMonotonicallyIncreasing()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("hello"));
            column.Add(NullValue.Instance);
            column.Add(new StringValue("world"));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] >= sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void LargeColumnPrefixSumNeverDecreases()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 1000; i++)
            {
                column.Add(new Int64Value(i));
            }

            var indices = new int[1000];
            for (int i = 0; i < 1000; i++)
            {
                indices[i] = i;
            }
            var sizes = new int[1000];

            column.GetPrefixSumByteSizes(indices, sizes);

            for (int i = 1; i < 1000; i++)
            {
                Assert.True(sizes[i] > sizes[i - 1]);
            }
        }

        [Fact]
        public void LargeStringColumnPrefixSumNeverDecreases()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 200; i++)
            {
                column.Add(new StringValue($"value_{i}"));
            }

            var indices = new int[200];
            for (int i = 0; i < 200; i++)
            {
                indices[i] = i;
            }
            var sizes = new int[200];

            column.GetPrefixSumByteSizes(indices, sizes);

            for (int i = 1; i < 200; i++)
            {
                Assert.True(sizes[i] > sizes[i - 1]);
            }
        }

        [Fact]
        public void MultipleColumnsAccumulateIntoSameSizesSpan()
        {
            using var col1 = new Column(GlobalMemoryManager.Instance);
            col1.Add(new Int64Value(1));
            col1.Add(new Int64Value(2));

            using var col2 = new Column(GlobalMemoryManager.Instance);
            col2.Add(new StringValue("hello"));
            col2.Add(new StringValue("world"));

            var indices = new int[] { 0, 1 };
            var sizes = new int[2];

            col1.GetPrefixSumByteSizes(indices, sizes);

            var afterCol1 = new int[] { sizes[0], sizes[1] };

            col2.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > afterCol1[0]);
            Assert.True(sizes[1] > afterCol1[1]);
        }

        [Fact]
        public void EmptyStringPrefixSumStillIncreases()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue(""));
            column.Add(new StringValue(""));
            column.Add(new StringValue(""));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void EmptyBinaryPrefixSumStillIncreases()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BinaryValue(Array.Empty<byte>()));
            column.Add(new BinaryValue(Array.Empty<byte>()));

            var indices = new int[] { 0, 1 };
            var sizes = new int[2];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
        }

        [Fact]
        public void EmptyListPrefixSumStillIncreases()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new ListValue(Array.Empty<IDataValue>()));
            column.Add(new ListValue(Array.Empty<IDataValue>()));

            var indices = new int[] { 0, 1 };
            var sizes = new int[2];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
        }

        [Fact]
        public void PrefixSumGrowsProportionallyToElementCount()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(42));

            var indices = new int[] { 0 };
            var sizes = new int[1];
            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
        }

        [Fact]
        public void PrefixSumLastElementIsLargerThanPrevious()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];
            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[2] > sizes[1]);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[0] > 0);
        }

        [Fact]
        public void CallingTwiceDoublesContribution()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("a"));
            column.Add(new StringValue("bb"));
            column.Add(new StringValue("ccc"));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];
            column.GetPrefixSumByteSizes(indices, sizes);

            var firstPass = new int[] { sizes[0], sizes[1], sizes[2] };

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.Equal(firstPass[0] * 2, sizes[0]);
            Assert.Equal(firstPass[1] * 2, sizes[1]);
            Assert.Equal(firstPass[2] * 2, sizes[2]);
        }

        [Fact]
        public void AlwaysNullColumnDoesNotModifySizes()
        {
            var column = AlwaysNullColumn.Instance;

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.Equal(0, sizes[0]);
            Assert.Equal(0, sizes[1]);
            Assert.Equal(0, sizes[2]);
        }

        [Fact]
        public void StructWithMixedChildTypesPrefixSumIsMonotonicallyIncreasing()
        {
            var header = StructHeader.Create("id", "name", "score");
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StructValue(header, new IDataValue[] { new Int64Value(1), new StringValue("Alice"), new DoubleValue(95.5) }));
            column.Add(new StructValue(header, new IDataValue[] { new Int64Value(2), new StringValue("Bob"), new DoubleValue(87.3) }));
            column.Add(new StructValue(header, new IDataValue[] { new Int64Value(3), new StringValue("Charlie"), new DoubleValue(92.1) }));

            var indices = new int[] { 0, 1, 2 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }

        [Fact]
        public void RepeatedIndicesProducesIncreasingPrefixSum()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            var indices = new int[] { 0, 0, 0 };
            var sizes = new int[3];

            column.GetPrefixSumByteSizes(indices, sizes);

            Assert.True(sizes[0] > 0);
            Assert.True(sizes[1] > sizes[0]);
            Assert.True(sizes[2] > sizes[1]);
        }
    }
}
