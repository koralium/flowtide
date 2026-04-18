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
    public class ColumnInsertFromTests
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

        private static void AssertNullValue(Column col, int index)
        {
            var val = col.GetValueAt(index, default);
            Assert.Equal(ArrowTypeId.Null, val.Type);
        }

        [Fact]
        public void InsertFromSingleIntAtMiddle()
        {
            using var target = CreateIntColumn(10, 20, 30);
            using var source = CreateIntColumn(99);

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 99);
            AssertIntValue(target, 2, 20);
            AssertIntValue(target, 3, 30);
        }

        [Fact]
        public void InsertFromIntAtStart()
        {
            using var target = CreateIntColumn(10, 20);
            using var source = CreateIntColumn(99);

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 0 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(3, target.Count);
            AssertIntValue(target, 0, 99);
            AssertIntValue(target, 1, 10);
            AssertIntValue(target, 2, 20);
        }

        [Fact]
        public void InsertFromIntAtEnd()
        {
            using var target = CreateIntColumn(10, 20);
            using var source = CreateIntColumn(99);

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 2 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(3, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 20);
            AssertIntValue(target, 2, 99);
        }

        [Fact]
        public void InsertFromTwoIntsAtDifferentPositions()
        {
            using var target = CreateIntColumn(10, 20, 30);
            using var source = CreateIntColumn(100, 200);

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 0, 2 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(5, target.Count);
            AssertIntValue(target, 0, 100);
            AssertIntValue(target, 1, 10);
            AssertIntValue(target, 2, 20);
            AssertIntValue(target, 3, 200);
            AssertIntValue(target, 4, 30);
        }

        [Fact]
        public void InsertFromEmptyLookup()
        {
            using var target = CreateIntColumn(10, 20);
            using var source = CreateIntColumn(99);

            Span<int> lookup = stackalloc int[0];
            Span<int> positions = stackalloc int[0];

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(2, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 20);
        }

        [Fact]
        public void InsertFromStringsAtSamePosition()
        {
            using var target = CreateStringColumn("a", "d");
            using var source = CreateStringColumn("b", "c");

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 1, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertStringValue(target, 0, "a");
            AssertStringValue(target, 1, "b");
            AssertStringValue(target, 2, "c");
            AssertStringValue(target, 3, "d");
        }

        [Fact]
        public void InsertFromBothNullColumns()
        {
            using var target = CreateNullColumn(3);
            using var source = CreateNullColumn(5);

            Span<int> lookup = stackalloc int[] { 0, 2, 4 };
            Span<int> positions = stackalloc int[] { 0, 1, 2 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(6, target.Count);
            for (int i = 0; i < 6; i++)
            {
                AssertNullValue(target, i);
            }
        }

        [Fact]
        public void InsertFromBothColumnsHaveNulls()
        {
            using var target = new Column(GlobalMemoryManager.Instance);
            target.Add(new Int64Value(10));
            target.Add(NullValue.Instance);
            target.Add(new Int64Value(30));

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(NullValue.Instance);
            source.Add(new Int64Value(200));

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 0, 2 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(5, target.Count);
            AssertNullValue(target, 0);
            AssertIntValue(target, 1, 10);
            AssertNullValue(target, 2);
            AssertIntValue(target, 3, 200);
            AssertIntValue(target, 4, 30);
        }

        [Fact]
        public void InsertFromOnlyTargetHasNulls()
        {
            using var target = new Column(GlobalMemoryManager.Instance);
            target.Add(new Int64Value(10));
            target.Add(NullValue.Instance);
            target.Add(new Int64Value(30));

            using var source = CreateIntColumn(200);

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 200);
            AssertNullValue(target, 2);
            AssertIntValue(target, 3, 30);
        }

        [Fact]
        public void InsertFromOnlySourceHasNulls()
        {
            using var target = CreateIntColumn(10, 20);

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(NullValue.Instance);
            source.Add(new Int64Value(200));

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 0, 2 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertNullValue(target, 0);
            AssertIntValue(target, 1, 10);
            AssertIntValue(target, 2, 20);
            AssertIntValue(target, 3, 200);
        }

        [Fact]
        public void InsertFromNullTargetTypedSource()
        {
            using var target = CreateNullColumn(2);
            using var source = CreateIntColumn(100, 200);

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 0, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 100);
            AssertNullValue(target, 1);
            AssertIntValue(target, 2, 200);
            AssertNullValue(target, 3);
        }

        [Fact]
        public void InsertFromTypedTargetNullSource()
        {
            using var target = CreateIntColumn(10, 20);
            using var source = CreateNullColumn(3);

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 1, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 10);
            AssertNullValue(target, 1);
            AssertNullValue(target, 2);
            AssertIntValue(target, 3, 20);
        }

        [Fact]
        public void InsertFromTypeMismatchPromotesToUnion()
        {
            using var target = CreateIntColumn(10, 20);
            using var source = CreateStringColumn("hello");

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(3, target.Count);
            AssertIntValue(target, 0, 10);
            AssertStringValue(target, 1, "hello");
            AssertIntValue(target, 2, 20);
        }

        [Fact]
        public void InsertFromUnionTarget()
        {
            using var target = new Column(GlobalMemoryManager.Instance);
            target.Add(new Int64Value(10));
            target.Add(new StringValue("a"));

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(new Int64Value(20));
            source.Add(new StringValue("b"));

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 1, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 20);
            AssertStringValue(target, 2, "b");
            AssertStringValue(target, 3, "a");
        }

        [Fact]
        public void InsertFromListColumn()
        {
            using var target = new Column(GlobalMemoryManager.Instance);
            target.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));
            target.Add(new ListValue(new IDataValue[] { new Int64Value(5), new Int64Value(6) }));

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(new ListValue(new IDataValue[] { new Int64Value(3), new Int64Value(4) }));

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(3, target.Count);

            var list0 = target.GetValueAt(0, default).AsList;
            Assert.Equal(1, list0.GetAt(0).AsLong);
            Assert.Equal(2, list0.GetAt(1).AsLong);

            var list1 = target.GetValueAt(1, default).AsList;
            Assert.Equal(3, list1.GetAt(0).AsLong);
            Assert.Equal(4, list1.GetAt(1).AsLong);

            var list2 = target.GetValueAt(2, default).AsList;
            Assert.Equal(5, list2.GetAt(0).AsLong);
            Assert.Equal(6, list2.GetAt(1).AsLong);
        }

        [Fact]
        public void InsertFromMapColumn()
        {
            using var target = new Column(GlobalMemoryManager.Instance);
            target.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>
            {
                new(new StringValue("a"), new Int64Value(1))
            }));

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>
            {
                new(new StringValue("b"), new Int64Value(2))
            }));

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 0 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(2, target.Count);
            Assert.Equal(2, target.GetValueAt(0, default).AsMap.GetValueAt(0).AsLong);
            Assert.Equal(1, target.GetValueAt(1, default).AsMap.GetValueAt(0).AsLong);
        }

        [Fact]
        public void InsertFromStructColumn()
        {
            var header = StructHeader.Create("x", "y");

            using var target = new Column(GlobalMemoryManager.Instance);
            target.Add(new StructValue(header, new Int64Value(1), new Int64Value(2)));

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(new StructValue(header, new Int64Value(3), new Int64Value(4)));

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 0 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(2, target.Count);
            var s0 = target.GetValueAt(0, default).AsStruct;
            Assert.Equal(3, s0.GetAt(0).AsLong);
            Assert.Equal(4, s0.GetAt(1).AsLong);

            var s1 = target.GetValueAt(1, default).AsStruct;
            Assert.Equal(1, s1.GetAt(0).AsLong);
            Assert.Equal(2, s1.GetAt(1).AsLong);
        }

        [Fact]
        public void InsertFromMultipleInterleaved()
        {
            using var target = CreateIntColumn(0, 10, 20, 30, 40);
            using var source = CreateIntColumn(100, 200, 300, 400, 500);

            Span<int> lookup = stackalloc int[] { 0, 2, 4 };
            Span<int> positions = stackalloc int[] { 0, 2, 4 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(8, target.Count);
            AssertIntValue(target, 0, 100);
            AssertIntValue(target, 1, 0);
            AssertIntValue(target, 2, 10);
            AssertIntValue(target, 3, 300);
            AssertIntValue(target, 4, 20);
            AssertIntValue(target, 5, 30);
            AssertIntValue(target, 6, 500);
            AssertIntValue(target, 7, 40);
        }

        [Fact]
        public void InsertFromConsecutiveSamePosition()
        {
            using var target = CreateIntColumn(10, 20);
            using var source = CreateIntColumn(100, 200, 300);

            Span<int> lookup = stackalloc int[] { 0, 1, 2 };
            Span<int> positions = stackalloc int[] { 1, 1, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(5, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 100);
            AssertIntValue(target, 2, 200);
            AssertIntValue(target, 3, 300);
            AssertIntValue(target, 4, 20);
        }

        [Fact]
        public void InsertFromIntoEmptyColumn()
        {
            using var target = CreateIntColumn();
            using var source = CreateIntColumn(100, 200, 300);

            Span<int> lookup = stackalloc int[] { 0, 1, 2 };
            Span<int> positions = stackalloc int[] { 0, 0, 0 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(3, target.Count);
            AssertIntValue(target, 0, 100);
            AssertIntValue(target, 1, 200);
            AssertIntValue(target, 2, 300);
        }

        [Fact]
        public void InsertFromIntoSingleElementColumn()
        {
            using var target = CreateIntColumn(42);
            using var source = CreateIntColumn(10, 20);

            Span<int> lookup = stackalloc int[] { 0, 1 };
            Span<int> positions = stackalloc int[] { 0, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(3, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 42);
            AssertIntValue(target, 2, 20);
        }

        [Fact]
        public void InsertFromAllPrepend()
        {
            using var target = CreateIntColumn(50, 60, 70);
            using var source = CreateIntColumn(10, 20, 30);

            Span<int> lookup = stackalloc int[] { 0, 1, 2 };
            Span<int> positions = stackalloc int[] { 0, 0, 0 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(6, target.Count);
            AssertIntValue(target, 0, 10);
            AssertIntValue(target, 1, 20);
            AssertIntValue(target, 2, 30);
            AssertIntValue(target, 3, 50);
            AssertIntValue(target, 4, 60);
            AssertIntValue(target, 5, 70);
        }

        [Fact]
        public void InsertFromAllAppend()
        {
            using var target = CreateIntColumn(50, 60, 70);
            using var source = CreateIntColumn(80, 90, 100);

            Span<int> lookup = stackalloc int[] { 0, 1, 2 };
            Span<int> positions = stackalloc int[] { 3, 3, 3 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(6, target.Count);
            AssertIntValue(target, 0, 50);
            AssertIntValue(target, 1, 60);
            AssertIntValue(target, 2, 70);
            AssertIntValue(target, 3, 80);
            AssertIntValue(target, 4, 90);
            AssertIntValue(target, 5, 100);
        }

        [Fact]
        public void InsertFromNonContiguousLookups()
        {
            using var target = CreateIntColumn(0);
            using var source = CreateIntColumn(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);

            Span<int> lookup = stackalloc int[] { 1, 5, 9 };
            Span<int> positions = stackalloc int[] { 0, 0, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 11);
            AssertIntValue(target, 1, 15);
            AssertIntValue(target, 2, 0);
            AssertIntValue(target, 3, 19);
        }

        [Fact]
        public void InsertFromSingleInsertIntoLargeColumn()
        {
            using var target = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 1000; i++)
            {
                target.Add(new Int64Value(i));
            }

            using var source = CreateIntColumn(9999);

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 500 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(1001, target.Count);
            AssertIntValue(target, 499, 499);
            AssertIntValue(target, 500, 9999);
            AssertIntValue(target, 501, 500);
        }

        [Fact]
        public void InsertFromAlternatingNullsInSource()
        {
            using var target = CreateIntColumn(1, 2, 3, 4, 5);

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(NullValue.Instance);
            source.Add(new Int64Value(100));
            source.Add(NullValue.Instance);
            source.Add(new Int64Value(200));
            source.Add(NullValue.Instance);

            Span<int> lookup = stackalloc int[] { 1, 3 };
            Span<int> positions = stackalloc int[] { 2, 4 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(7, target.Count);
            AssertIntValue(target, 0, 1);
            AssertIntValue(target, 1, 2);
            AssertIntValue(target, 2, 100);
            AssertIntValue(target, 3, 3);
            AssertIntValue(target, 4, 4);
            AssertIntValue(target, 5, 200);
            AssertIntValue(target, 6, 5);
        }

        [Fact]
        public void InsertFromPickOnlyNullsFromSource()
        {
            using var target = CreateIntColumn(1, 2, 3);

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(NullValue.Instance);
            source.Add(new Int64Value(100));
            source.Add(NullValue.Instance);

            Span<int> lookup = stackalloc int[] { 0, 2 };
            Span<int> positions = stackalloc int[] { 1, 2 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(5, target.Count);
            AssertIntValue(target, 0, 1);
            AssertNullValue(target, 1);
            AssertIntValue(target, 2, 2);
            AssertNullValue(target, 3);
            AssertIntValue(target, 4, 3);
        }

        [Fact]
        public void InsertFromIntegerBitwidthMismatch()
        {
            using var target = CreateIntColumn(1, 2, 3);

            using var source = new Column(GlobalMemoryManager.Instance);
            source.Add(new Int64Value(1000));

            Span<int> lookup = stackalloc int[] { 0 };
            Span<int> positions = stackalloc int[] { 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(4, target.Count);
            AssertIntValue(target, 0, 1);
            AssertIntValue(target, 1, 1000);
            AssertIntValue(target, 2, 2);
            AssertIntValue(target, 3, 3);
        }

        [Fact]
        public void InsertFromEntireSourceAtSamePosition()
        {
            using var target = CreateIntColumn(0, 100);
            using var source = CreateIntColumn(10, 20, 30, 40, 50);

            Span<int> lookup = stackalloc int[] { 0, 1, 2, 3, 4 };
            Span<int> positions = stackalloc int[] { 1, 1, 1, 1, 1 };

            target.InsertFrom(source, lookup, positions);

            Assert.Equal(7, target.Count);
            AssertIntValue(target, 0, 0);
            AssertIntValue(target, 1, 10);
            AssertIntValue(target, 2, 20);
            AssertIntValue(target, 3, 30);
            AssertIntValue(target, 4, 40);
            AssertIntValue(target, 5, 50);
            AssertIntValue(target, 6, 100);
        }

        [Fact]
        public void InsertFromLargeRandomStress()
        {
            var rng = new Random(42);
            int targetSize = 200;
            int sourceSize = 100;
            int insertCount = 50;

            using var target = new Column(GlobalMemoryManager.Instance);
            var expected = new List<long>();
            for (int i = 0; i < targetSize; i++)
            {
                var v = rng.Next(0, 10000);
                target.Add(new Int64Value(v));
                expected.Add(v);
            }

            using var source = new Column(GlobalMemoryManager.Instance);
            var sourceValues = new long[sourceSize];
            for (int i = 0; i < sourceSize; i++)
            {
                sourceValues[i] = rng.Next(10000, 20000);
                source.Add(new Int64Value(sourceValues[i]));
            }

            var posSet = new SortedSet<int>();
            while (posSet.Count < insertCount)
            {
                posSet.Add(rng.Next(0, targetSize + 1));
            }
            var positions = posSet.ToArray();
            var lookups = new int[insertCount];
            for (int i = 0; i < insertCount; i++)
            {
                lookups[i] = rng.Next(0, sourceSize);
            }
            Array.Sort(lookups);

            for (int i = insertCount - 1; i >= 0; i--)
            {
                expected.Insert(positions[i], sourceValues[lookups[i]]);
            }

            target.InsertFrom(source, lookups.AsSpan(), positions.AsSpan());

            Assert.Equal(expected.Count, target.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                AssertIntValue(target, i, expected[i]);
            }
        }

        [Fact]
        public void InsertFromStressWithMixedNulls()
        {
            var rng = new Random(123);
            int targetSize = 100;
            int sourceSize = 50;
            int insertCount = 30;

            using var target = new Column(GlobalMemoryManager.Instance);
            var expected = new List<long?>();
            for (int i = 0; i < targetSize; i++)
            {
                if (rng.Next(5) == 0)
                {
                    target.Add(NullValue.Instance);
                    expected.Add(null);
                }
                else
                {
                    var v = (long)rng.Next(0, 10000);
                    target.Add(new Int64Value(v));
                    expected.Add(v);
                }
            }

            using var source = new Column(GlobalMemoryManager.Instance);
            var sourceValues = new long?[sourceSize];
            for (int i = 0; i < sourceSize; i++)
            {
                if (rng.Next(5) == 0)
                {
                    sourceValues[i] = null;
                    source.Add(NullValue.Instance);
                }
                else
                {
                    sourceValues[i] = rng.Next(10000, 20000);
                    source.Add(new Int64Value(sourceValues[i]!.Value));
                }
            }

            var posSet = new SortedSet<int>();
            while (posSet.Count < insertCount)
            {
                posSet.Add(rng.Next(0, targetSize + 1));
            }
            var positions = posSet.ToArray();
            var lookups = new int[insertCount];
            for (int i = 0; i < insertCount; i++)
            {
                lookups[i] = rng.Next(0, sourceSize);
            }
            Array.Sort(lookups);

            for (int i = insertCount - 1; i >= 0; i--)
            {
                expected.Insert(positions[i], sourceValues[lookups[i]]);
            }

            target.InsertFrom(source, lookups.AsSpan(), positions.AsSpan());

            Assert.Equal(expected.Count, target.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i] == null)
                {
                    AssertNullValue(target, i);
                }
                else
                {
                    AssertIntValue(target, i, expected[i]!.Value);
                }
            }
        }
    }
}
