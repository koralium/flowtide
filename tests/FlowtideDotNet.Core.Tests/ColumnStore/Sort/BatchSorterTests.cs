using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.Memory;
using System;
using Xunit;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Sort
{
    public class BatchSorterTests
    {
        [Fact]
        public void TestSortAscending()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(456));
            column.Add(new DoubleValue(123));
            column.Add(new DoubleValue(789));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(0, indirect[1]);
            Assert.Equal(2, indirect[2]);
        }

        [Fact]
        public void TestSortMultipleColumns()
        {
            var column1 = new Column(GlobalMemoryManager.Instance);
            column1.Add(new DoubleValue(1));
            column1.Add(new DoubleValue(1));
            column1.Add(new DoubleValue(2));

            var column2 = new Column(GlobalMemoryManager.Instance);
            column2.Add(new DoubleValue(456));
            column2.Add(new DoubleValue(123));
            column2.Add(new DoubleValue(100));

            IColumn[] columns = new IColumn[] { column1, column2 };
            var batchSorter = new BatchSorter(2);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(0, indirect[1]);
            Assert.Equal(2, indirect[2]);
        }

        [Fact]
        public void TestSortMultipleBatchesCacheHit()
        {
            var column1 = new Column(GlobalMemoryManager.Instance);
            column1.Add(new DoubleValue(3));
            column1.Add(new DoubleValue(1));
            column1.Add(new DoubleValue(2));

            IColumn[] columns1 = new IColumn[] { column1 };
            var batchSorter = new BatchSorter(1);

            int[] indirect1 = new int[] { 0, 1, 2 };
            var span1 = indirect1.AsSpan();

            batchSorter.SortData(columns1, ref span1);

            Assert.Equal(1, indirect1[0]);
            Assert.Equal(2, indirect1[1]);
            Assert.Equal(0, indirect1[2]);

            var column2 = new Column(GlobalMemoryManager.Instance);
            column2.Add(new DoubleValue(5));
            column2.Add(new DoubleValue(6));
            column2.Add(new DoubleValue(4));

            IColumn[] columns2 = new IColumn[] { column2 };

            int[] indirect2 = new int[] { 0, 1, 2 };
            var span2 = indirect2.AsSpan();

            batchSorter.SortData(columns2, ref span2);

            Assert.Equal(2, indirect2[0]);
            Assert.Equal(0, indirect2[1]);
            Assert.Equal(1, indirect2[2]);
        }

        [Fact]
        public void TestSortChangeSchemaCacheMiss()
        {
            var column1 = new Column(GlobalMemoryManager.Instance);
            column1.Add(new DoubleValue(3));
            column1.Add(new DoubleValue(1));
            column1.Add(new DoubleValue(2));

            IColumn[] columns1 = new IColumn[] { column1 };
            var batchSorter = new BatchSorter(1);

            int[] indirect1 = new int[] { 0, 1, 2 };
            var span1 = indirect1.AsSpan();

            batchSorter.SortData(columns1, ref span1);

            Assert.Equal(1, indirect1[0]);
            Assert.Equal(2, indirect1[1]);
            Assert.Equal(0, indirect1[2]);

            var column2 = new Column(GlobalMemoryManager.Instance);
            column2.Add(new StringValue("C"));
            column2.Add(new StringValue("A"));
            column2.Add(new StringValue("B"));

            IColumn[] columns2 = new IColumn[] { column2 };

            int[] indirect2 = new int[] { 0, 1, 2 };
            var span2 = indirect2.AsSpan();

            batchSorter.SortData(columns2, ref span2);

            Assert.Equal(1, indirect2[0]);
            Assert.Equal(2, indirect2[1]);
            Assert.Equal(0, indirect2[2]);
        }

        [Fact]
        public void TestSortManyColumns()
        {
            int numColumns = 8;
            IColumn[] columns = new IColumn[numColumns];
            for (int i = 0; i < numColumns; i++)
            {
                var col = new Column(GlobalMemoryManager.Instance);
                if (i < numColumns - 1)
                {
                    col.Add(new DoubleValue(1));
                    col.Add(new DoubleValue(1));
                    col.Add(new DoubleValue(1));
                }
                else
                {
                    col.Add(new DoubleValue(2));
                    col.Add(new DoubleValue(0));
                    col.Add(new DoubleValue(1));
                }
                columns[i] = col;
            }

            var batchSorter = new BatchSorter(numColumns);
            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortListAndIntColumns()
        {
            var listColumn = new Column(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1)
            }));

            var intColumn = new Column(GlobalMemoryManager.Instance);
            intColumn.Add(new Int64Value(5));
            intColumn.Add(new Int64Value(3));
            intColumn.Add(new Int64Value(9));

            IColumn[] columns = new IColumn[] { listColumn, intColumn };
            var batchSorter = new BatchSorter(2);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(2, indirect[0]);
            Assert.Equal(1, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortInt64Column()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(300));
            column.Add(new Int64Value(100));
            column.Add(new Int64Value(200));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortStringColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("banana"));
            column.Add(new StringValue("apple"));
            column.Add(new StringValue("cherry"));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(0, indirect[1]);
            Assert.Equal(2, indirect[2]);
        }

        [Fact]
        public void TestSortBoolColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(BoolValue.True);
            column.Add(BoolValue.False);
            column.Add(BoolValue.True);
            column.Add(BoolValue.False);

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.False(column.GetValueAt(indirect[0], default).AsBool);
            Assert.False(column.GetValueAt(indirect[1], default).AsBool);
            Assert.True(column.GetValueAt(indirect[2], default).AsBool);
            Assert.True(column.GetValueAt(indirect[3], default).AsBool);
        }

        [Fact]
        public void TestSortDecimalColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DecimalValue(3.14m));
            column.Add(new DecimalValue(1.01m));
            column.Add(new DecimalValue(2.72m));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortBinaryColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BinaryValue(new byte[] { 3, 0 }));
            column.Add(new BinaryValue(new byte[] { 1, 0 }));
            column.Add(new BinaryValue(new byte[] { 2, 0 }));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortTimestampTzColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new TimestampTzValue(new DateTime(2025, 3, 1)));
            column.Add(new TimestampTzValue(new DateTime(2025, 1, 1)));
            column.Add(new TimestampTzValue(new DateTime(2025, 2, 1)));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortMapColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new MapValue(
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(3))));
            column.Add(new MapValue(
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(1))));
            column.Add(new MapValue(
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(2))));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortWithNullValues()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(3));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            var val0 = column.GetValueAt(indirect[0], default);
            var val2 = column.GetValueAt(indirect[2], default);
            Assert.True(val0.IsNull || val2.IsNull);
        }

        [Fact]
        public void TestSortAllNullColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.True(column.GetValueAt(indirect[0], default).IsNull);
            Assert.True(column.GetValueAt(indirect[1], default).IsNull);
            Assert.True(column.GetValueAt(indirect[2], default).IsNull);
        }

        [Fact]
        public void TestSortSingleElement()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(42));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(0, indirect[0]);
        }

        [Fact]
        public void TestSortAlreadySorted()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(0, indirect[0]);
            Assert.Equal(1, indirect[1]);
            Assert.Equal(2, indirect[2]);
        }

        [Fact]
        public void TestSortReverseSorted()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(3));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(1));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(2, indirect[0]);
            Assert.Equal(1, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortDuplicateValues()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(1));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, column.GetValueAt(indirect[0], default).AsLong);
            Assert.Equal(1, column.GetValueAt(indirect[1], default).AsLong);
            Assert.Equal(2, column.GetValueAt(indirect[2], default).AsLong);
            Assert.Equal(2, column.GetValueAt(indirect[3], default).AsLong);
        }

        [Fact]
        public void TestSortLargerDataset()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            var random = new Random(42);
            int count = 100;

            for (int i = 0; i < count; i++)
            {
                column.Add(new Int64Value(random.Next(0, 1000)));
            }

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[count];
            for (int i = 0; i < count; i++) indirect[i] = i;
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            for (int i = 1; i < count; i++)
            {
                var prev = column.GetValueAt(indirect[i - 1], default).AsLong;
                var curr = column.GetValueAt(indirect[i], default).AsLong;
                Assert.True(prev <= curr, $"Not sorted at position {i}: {prev} > {curr}");
            }
        }

        [Fact]
        public void TestSortMultipleColumnsStringAndInt()
        {
            var stringCol = new Column(GlobalMemoryManager.Instance);
            stringCol.Add(new StringValue("a"));
            stringCol.Add(new StringValue("a"));
            stringCol.Add(new StringValue("b"));

            var intCol = new Column(GlobalMemoryManager.Instance);
            intCol.Add(new Int64Value(2));
            intCol.Add(new Int64Value(1));
            intCol.Add(new Int64Value(1));

            IColumn[] columns = new IColumn[] { stringCol, intCol };
            var batchSorter = new BatchSorter(2);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(0, indirect[1]);
            Assert.Equal(2, indirect[2]);
        }

        [Fact]
        public void TestSortMultipleColumnsDecimalAndTimestamp()
        {
            var decCol = new Column(GlobalMemoryManager.Instance);
            decCol.Add(new DecimalValue(1.0m));
            decCol.Add(new DecimalValue(1.0m));
            decCol.Add(new DecimalValue(2.0m));

            var tsCol = new Column(GlobalMemoryManager.Instance);
            tsCol.Add(new TimestampTzValue(new DateTime(2025, 2, 1)));
            tsCol.Add(new TimestampTzValue(new DateTime(2025, 1, 1)));
            tsCol.Add(new TimestampTzValue(new DateTime(2025, 1, 1)));

            IColumn[] columns = new IColumn[] { decCol, tsCol };
            var batchSorter = new BatchSorter(2);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(0, indirect[1]);
            Assert.Equal(2, indirect[2]);
        }

        [Fact]
        public void TestSortStructColumn()
        {
            var header = StructHeader.Create("name", "age");
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StructValue(header, new StringValue("bob"), new Int64Value(30)));
            column.Add(new StructValue(header, new StringValue("alice"), new Int64Value(25)));
            column.Add(new StructValue(header, new StringValue("bob"), new Int64Value(20)));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(1, indirect[0]);
            Assert.Equal(2, indirect[1]);
            Assert.Equal(0, indirect[2]);
        }

        [Fact]
        public void TestSortUnionColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("z"));
            column.Add(new Int64Value(1));
            column.Add(new StringValue("a"));
            column.Add(new Int64Value(5));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            for (int i = 1; i < 4; i++)
            {
                var cmp = column.CompareTo(column, indirect[i - 1], indirect[i]);
                Assert.True(cmp <= 0, $"Not sorted at position {i}: index {indirect[i - 1]} > index {indirect[i]}");
            }
        }

        [Fact]
        public void TestSortIntegerInt8BitWidth()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(50));
            column.Add(new Int64Value(10));
            column.Add(new Int64Value(127));
            column.Add(new Int64Value(-5));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(-5, column.GetValueAt(indirect[0], default).AsLong);
            Assert.Equal(10, column.GetValueAt(indirect[1], default).AsLong);
            Assert.Equal(50, column.GetValueAt(indirect[2], default).AsLong);
            Assert.Equal(127, column.GetValueAt(indirect[3], default).AsLong);
        }

        [Fact]
        public void TestSortIntegerInt16BitWidth()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1000));
            column.Add(new Int64Value(128));
            column.Add(new Int64Value(32000));
            column.Add(new Int64Value(500));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(128, column.GetValueAt(indirect[0], default).AsLong);
            Assert.Equal(500, column.GetValueAt(indirect[1], default).AsLong);
            Assert.Equal(1000, column.GetValueAt(indirect[2], default).AsLong);
            Assert.Equal(32000, column.GetValueAt(indirect[3], default).AsLong);
        }

        [Fact]
        public void TestSortIntegerInt32BitWidth()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(100000));
            column.Add(new Int64Value(33000));
            column.Add(new Int64Value(2000000000));
            column.Add(new Int64Value(50000));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(33000, column.GetValueAt(indirect[0], default).AsLong);
            Assert.Equal(50000, column.GetValueAt(indirect[1], default).AsLong);
            Assert.Equal(100000, column.GetValueAt(indirect[2], default).AsLong);
            Assert.Equal(2000000000, column.GetValueAt(indirect[3], default).AsLong);
        }

        [Fact]
        public void TestSortIntegerInt64BitWidth()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(3000000000L));
            column.Add(new Int64Value(1000000000000L));
            column.Add(new Int64Value(5000000000L));
            column.Add(new Int64Value(2500000000L));

            IColumn[] columns = new IColumn[] { column };
            var batchSorter = new BatchSorter(1);

            int[] indirect = new int[] { 0, 1, 2, 3 };
            var span = indirect.AsSpan();

            batchSorter.SortData(columns, ref span);

            Assert.Equal(2500000000L, column.GetValueAt(indirect[0], default).AsLong);
            Assert.Equal(3000000000L, column.GetValueAt(indirect[1], default).AsLong);
            Assert.Equal(5000000000L, column.GetValueAt(indirect[2], default).AsLong);
            Assert.Equal(1000000000000L, column.GetValueAt(indirect[3], default).AsLong);
        }
    }
}
