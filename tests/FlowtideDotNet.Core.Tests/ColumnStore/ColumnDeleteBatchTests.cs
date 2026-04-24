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
    public class ColumnDeleteBatchTests
    {
        private static Column CreateIntColumn(params long[] values)
        {
            var col = new Column(GlobalMemoryManager.Instance);
            foreach (var v in values)
            {
                col.Add(new Int64Value(v));
            }
            return col;
        }

        private static Column CreateStringColumn(params string?[] values)
        {
            var col = new Column(GlobalMemoryManager.Instance);
            foreach (var v in values)
            {
                if (v == null)
                    col.Add(NullValue.Instance);
                else
                    col.Add(new StringValue(v));
            }
            return col;
        }

        private static Column CreateNullColumn(int count)
        {
            var col = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < count; i++)
            {
                col.Add(NullValue.Instance);
            }
            return col;
        }

        private static void AssertIntValue(Column col, int index, long expected)
        {
            var val = col.GetValueAt(index, default);
            Assert.Equal(ArrowTypeId.Int64, val.Type);
            Assert.Equal(expected, val.AsLong);
        }

        private static void AssertStringValue(Column col, int index, string expected)
        {
            var val = col.GetValueAt(index, default);
            Assert.Equal(ArrowTypeId.String, val.Type);
            Assert.Equal(expected, val.AsString.ToString());
        }

        private static void AssertDoubleValue(Column col, int index, double expected)
        {
            var val = col.GetValueAt(index, default);
            Assert.Equal(ArrowTypeId.Double, val.Type);
            Assert.Equal(expected, val.AsDouble);
        }

        private static void AssertBoolValue(Column col, int index, bool expected)
        {
            var val = col.GetValueAt(index, default);
            Assert.Equal(ArrowTypeId.Boolean, val.Type);
            Assert.Equal(expected, val.AsBool);
        }

        private static void AssertNullValue(Column col, int index)
        {
            var val = col.GetValueAt(index, default);
            Assert.Equal(ArrowTypeId.Null, val.Type);
        }

        [Fact]
        public void DeleteBatchEmptyTargets()
        {
            using var col = CreateIntColumn(10, 20, 30);

            Span<int> targets = stackalloc int[0];

            col.DeleteBatch(targets);

            Assert.Equal(3, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 20);
            AssertIntValue(col, 2, 30);
        }

        [Fact]
        public void DeleteBatchSingleIntAtBeginning()
        {
            using var col = CreateIntColumn(10, 20, 30);

            Span<int> targets = stackalloc int[] { 0 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 20);
            AssertIntValue(col, 1, 30);
        }

        [Fact]
        public void DeleteBatchSingleIntInMiddle()
        {
            using var col = CreateIntColumn(10, 20, 30);

            Span<int> targets = stackalloc int[] { 1 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 30);
        }

        [Fact]
        public void DeleteBatchSingleIntAtEnd()
        {
            using var col = CreateIntColumn(10, 20, 30);

            Span<int> targets = stackalloc int[] { 2 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 20);
        }

        [Fact]
        public void DeleteBatchMultiplePositions()
        {
            using var col = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> targets = stackalloc int[] { 0, 2, 4 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 20);
            AssertIntValue(col, 1, 40);
        }

        [Fact]
        public void DeleteBatchConsecutive()
        {
            using var col = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> targets = stackalloc int[] { 1, 2, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 50);
        }

        [Fact]
        public void DeleteBatchAllElements()
        {
            using var col = CreateIntColumn(10, 20, 30);

            Span<int> targets = stackalloc int[] { 0, 1, 2 };

            col.DeleteBatch(targets);

            Assert.Empty(col);
        }

        [Fact]
        public void DeleteBatchAllButOne()
        {
            using var col = CreateIntColumn(10, 20, 30, 40);

            Span<int> targets = stackalloc int[] { 0, 1, 3 };

            col.DeleteBatch(targets);

            Assert.Single(col);
            AssertIntValue(col, 0, 30);
        }

        [Fact]
        public void DeleteBatchSingleElementColumn()
        {
            using var col = CreateIntColumn(42);

            Span<int> targets = stackalloc int[] { 0 };

            col.DeleteBatch(targets);

            Assert.Empty(col);
        }

        [Fact]
        public void DeleteBatchNullOnlyColumn()
        {
            using var col = CreateNullColumn(5);

            Span<int> targets = stackalloc int[] { 0, 2, 4 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertNullValue(col, 0);
            AssertNullValue(col, 1);
        }

        [Fact]
        public void DeleteBatchNullOnlyColumnAll()
        {
            using var col = CreateNullColumn(3);

            Span<int> targets = stackalloc int[] { 0, 1, 2 };

            col.DeleteBatch(targets);

            Assert.Empty(col);
        }

        [Fact]
        public void DeleteBatchTypedColumnWithNullsDeleteNulls()
        {
            // Column with mixed values and nulls, delete the null positions
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(30));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(50));

            Span<int> targets = stackalloc int[] { 1, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(3, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 30);
            AssertIntValue(col, 2, 50);
        }

        [Fact]
        public void DeleteBatchTypedColumnWithNullsDeleteValues()
        {
            // Column with mixed values and nulls, delete the non-null positions
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(30));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(50));

            Span<int> targets = stackalloc int[] { 0, 2, 4 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertNullValue(col, 0);
            AssertNullValue(col, 1);
        }

        [Fact]
        public void DeleteBatchTypedColumnWithNullsDeleteMixed()
        {
            // Column with mixed values and nulls, delete both null and non-null positions
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(30));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(50));

            Span<int> targets = stackalloc int[] { 0, 1 };

            col.DeleteBatch(targets);

            Assert.Equal(3, col.Count);
            AssertIntValue(col, 0, 30);
            AssertNullValue(col, 1);
            AssertIntValue(col, 2, 50);
        }

        [Fact]
        public void DeleteBatchTypedColumnDeleteAllNullsLeavesCleanColumn()
        {
            // Column with exactly one null, delete it to verify null counter goes to 0
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(30));

            Span<int> targets = stackalloc int[] { 1 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 30);

            // Verify column still works correctly after null removal by adding more values
            col.Add(new Int64Value(40));
            Assert.Equal(3, col.Count);
            AssertIntValue(col, 2, 40);
        }

        [Fact]
        public void DeleteBatchStringColumn()
        {
            using var col = CreateStringColumn("a", "b", "c", "d", "e");

            Span<int> targets = stackalloc int[] { 1, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(3, col.Count);
            AssertStringValue(col, 0, "a");
            AssertStringValue(col, 1, "c");
            AssertStringValue(col, 2, "e");
        }

        [Fact]
        public void DeleteBatchBoolColumn()
        {
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new BoolValue(true));
            col.Add(new BoolValue(false));
            col.Add(new BoolValue(true));
            col.Add(new BoolValue(false));

            Span<int> targets = stackalloc int[] { 0, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertBoolValue(col, 0, false);
            AssertBoolValue(col, 1, true);
        }

        [Fact]
        public void DeleteBatchDoubleColumn()
        {
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new DoubleValue(1.1));
            col.Add(new DoubleValue(2.2));
            col.Add(new DoubleValue(3.3));

            Span<int> targets = stackalloc int[] { 1 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertDoubleValue(col, 0, 1.1);
            AssertDoubleValue(col, 1, 3.3);
        }

        [Fact]
        public void DeleteBatchUnionColumn()
        {
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(new StringValue("hello"));
            col.Add(new Int64Value(30));
            col.Add(new StringValue("world"));

            Span<int> targets = stackalloc int[] { 1, 2 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertStringValue(col, 1, "world");
        }

        [Fact]
        public void DeleteBatchUnionColumnAllOfOneType()
        {
            // Delete all elements of one type from a union, leaving only the other type
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(new StringValue("a"));
            col.Add(new Int64Value(20));
            col.Add(new StringValue("b"));

            // Delete both strings at indices 1, 3
            Span<int> targets = stackalloc int[] { 1, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 20);
        }

        [Fact]
        public void DeleteBatchListColumn()
        {
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));
            col.Add(new ListValue(new IDataValue[] { new Int64Value(3), new Int64Value(4) }));
            col.Add(new ListValue(new IDataValue[] { new Int64Value(5), new Int64Value(6) }));

            Span<int> targets = stackalloc int[] { 1 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);

            var list0 = col.GetValueAt(0, default).AsList;
            Assert.Equal(1, list0.GetAt(0).AsLong);
            Assert.Equal(2, list0.GetAt(1).AsLong);

            var list1 = col.GetValueAt(1, default).AsList;
            Assert.Equal(5, list1.GetAt(0).AsLong);
            Assert.Equal(6, list1.GetAt(1).AsLong);
        }

        [Fact]
        public void DeleteBatchMapColumn()
        {
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>
            {
                new(new StringValue("a"), new Int64Value(1))
            }));
            col.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>
            {
                new(new StringValue("b"), new Int64Value(2))
            }));
            col.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>
            {
                new(new StringValue("c"), new Int64Value(3))
            }));

            Span<int> targets = stackalloc int[] { 0, 2 };

            col.DeleteBatch(targets);

            Assert.Single(col);
            Assert.Equal(2, col.GetValueAt(0, default).AsMap.GetValueAt(0).AsLong);
        }

        [Fact]
        public void DeleteBatchStructColumn()
        {
            var header = StructHeader.Create("x", "y");

            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new StructValue(header, new Int64Value(1), new Int64Value(2)));
            col.Add(new StructValue(header, new Int64Value(3), new Int64Value(4)));
            col.Add(new StructValue(header, new Int64Value(5), new Int64Value(6)));

            Span<int> targets = stackalloc int[] { 0 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            var s0 = col.GetValueAt(0, default).AsStruct;
            Assert.Equal(3, s0.GetAt(0).AsLong);
            Assert.Equal(4, s0.GetAt(1).AsLong);

            var s1 = col.GetValueAt(1, default).AsStruct;
            Assert.Equal(5, s1.GetAt(0).AsLong);
            Assert.Equal(6, s1.GetAt(1).AsLong);
        }

        [Fact]
        public void DeleteBatchThenAdd()
        {
            using var col = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> targets = stackalloc int[] { 1, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(3, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 30);
            AssertIntValue(col, 2, 50);

            col.Add(new Int64Value(60));
            col.Add(new Int64Value(70));

            Assert.Equal(5, col.Count);
            AssertIntValue(col, 3, 60);
            AssertIntValue(col, 4, 70);
        }

        [Fact]
        public void DeleteBatchThenInsertFrom()
        {
            // Verify DeleteBatch followed by InsertFrom works correctly
            using var col = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> deleteTargets = stackalloc int[] { 1, 3 };
            col.DeleteBatch(deleteTargets);

            // After delete: [10, 30, 50]
            Assert.Equal(3, col.Count);

            using var source = CreateIntColumn(99, 88);
            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 1, 2 };

            col.InsertFrom(source, lookup, positions);

            // After insert: [10, 99, 30, 88, 50]
            Assert.Equal(5, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 99);
            AssertIntValue(col, 2, 30);
            AssertIntValue(col, 3, 88);
            AssertIntValue(col, 4, 50);
        }

        [Fact]
        public void DeleteBatchTwoConsecutiveCalls()
        {
            using var col = CreateIntColumn(10, 20, 30, 40, 50, 60, 70);

            Span<int> targets1 = stackalloc int[] { 1, 3, 5 };
            col.DeleteBatch(targets1);

            // After first: [10, 30, 50, 70]
            Assert.Equal(4, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 30);
            AssertIntValue(col, 2, 50);
            AssertIntValue(col, 3, 70);

            Span<int> targets2 = stackalloc int[] { 0, 2 };
            col.DeleteBatch(targets2);

            // After second: [30, 70]
            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 30);
            AssertIntValue(col, 1, 70);
        }

        [Fact]
        public void DeleteBatchNullOnlyColumnSingle()
        {
            // Null-only column with exactly one element
            using var col = CreateNullColumn(1);

            Span<int> targets = stackalloc int[] { 0 };

            col.DeleteBatch(targets);

            Assert.Empty(col);
        }

        [Fact]
        public void DeleteBatchTypedColumnWithSingleNull()
        {
            // Typed column with exactly one null among many values
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(new Int64Value(20));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(40));
            col.Add(new Int64Value(50));

            // Delete only the null element
            Span<int> targets = stackalloc int[] { 2 };

            col.DeleteBatch(targets);

            Assert.Equal(4, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 20);
            AssertIntValue(col, 2, 40);
            AssertIntValue(col, 3, 50);
        }

        [Fact]
        public void DeleteBatchTypedColumnWithNullsDeleteAllValues()
        {
            // Delete all non-null values, leaving only nulls
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(20));
            col.Add(NullValue.Instance);

            Span<int> targets = stackalloc int[] { 1 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertNullValue(col, 0);
            AssertNullValue(col, 1);
        }

        [Fact]
        public void DeleteBatchStringColumnWithNulls()
        {
            using var col = CreateStringColumn("a", null, "c", null, "e");

            Span<int> targets = stackalloc int[] { 0, 2, 4 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertNullValue(col, 0);
            AssertNullValue(col, 1);
        }

        [Fact]
        public void DeleteBatchLargeScale()
        {
            int size = 1000;
            using var col = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < size; i++)
            {
                col.Add(new Int64Value(i));
            }

            // Delete all even-indexed elements
            var targetList = new List<int>();
            for (int i = 0; i < size; i += 2)
            {
                targetList.Add(i);
            }

            col.DeleteBatch(targetList.ToArray().AsSpan());

            Assert.Equal(size / 2, col.Count);
            for (int i = 0; i < col.Count; i++)
            {
                AssertIntValue(col, i, (i * 2) + 1);
            }
        }

        [Fact]
        public void DeleteBatchLargeScaleWithNulls()
        {
            var rng = new Random(42);
            int size = 500;
            using var col = new Column(GlobalMemoryManager.Instance);
            var expected = new List<long?>();
            for (int i = 0; i < size; i++)
            {
                if (rng.Next(5) == 0)
                {
                    col.Add(NullValue.Instance);
                    expected.Add(null);
                }
                else
                {
                    var v = (long)i;
                    col.Add(new Int64Value(v));
                    expected.Add(v);
                }
            }

            // Delete every 3rd element
            var targetList = new List<int>();
            for (int i = 0; i < size; i += 3)
            {
                targetList.Add(i);
            }

            // Apply deletions to expected list (reverse order to maintain indices)
            for (int i = targetList.Count - 1; i >= 0; i--)
            {
                expected.RemoveAt(targetList[i]);
            }

            col.DeleteBatch(targetList.ToArray().AsSpan());

            Assert.Equal(expected.Count, col.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i] == null)
                {
                    AssertNullValue(col, i);
                }
                else
                {
                    AssertIntValue(col, i, expected[i]!.Value);
                }
            }
        }

        [Fact]
        public void DeleteBatchLeadingConsecutive()
        {
            using var col = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> targets = stackalloc int[] { 0, 1, 2 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 40);
            AssertIntValue(col, 1, 50);
        }

        [Fact]
        public void DeleteBatchTrailingConsecutive()
        {
            using var col = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> targets = stackalloc int[] { 2, 3, 4 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            AssertIntValue(col, 0, 10);
            AssertIntValue(col, 1, 20);
        }

        [Fact]
        public void DeleteBatchListColumnWithNulls()
        {
            // List column with null entries interspersed
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new ListValue(new IDataValue[] { new Int64Value(1) }));
            col.Add(NullValue.Instance);
            col.Add(new ListValue(new IDataValue[] { new Int64Value(3) }));

            Span<int> targets = stackalloc int[] { 1 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);
            Assert.Equal(1, col.GetValueAt(0, default).AsList.GetAt(0).AsLong);
            Assert.Equal(3, col.GetValueAt(1, default).AsList.GetAt(0).AsLong);
        }

        [Fact]
        public void DeleteBatchListColumnVariableLengths()
        {
            // Lists of different sizes to stress offset management
            using var col = new Column(GlobalMemoryManager.Instance);
            col.Add(new ListValue(new IDataValue[] { new Int64Value(1) }));
            col.Add(new ListValue(new IDataValue[] { new Int64Value(2), new Int64Value(3), new Int64Value(4) }));
            col.Add(new ListValue(new IDataValue[] { new Int64Value(5), new Int64Value(6) }));
            col.Add(new ListValue(Array.Empty<IDataValue>()));

            // Delete the large list and the empty list
            Span<int> targets = stackalloc int[] { 1, 3 };

            col.DeleteBatch(targets);

            Assert.Equal(2, col.Count);

            var list0 = col.GetValueAt(0, default).AsList;
            Assert.Equal(1, list0.Count);
            Assert.Equal(1, list0.GetAt(0).AsLong);

            var list1 = col.GetValueAt(1, default).AsList;
            Assert.Equal(2, list1.Count);
            Assert.Equal(5, list1.GetAt(0).AsLong);
            Assert.Equal(6, list1.GetAt(1).AsLong);
        }
    }
}
