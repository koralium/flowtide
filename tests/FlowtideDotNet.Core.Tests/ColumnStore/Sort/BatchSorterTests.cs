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
    }
}
