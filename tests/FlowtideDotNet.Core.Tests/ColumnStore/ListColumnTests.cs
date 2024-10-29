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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ListColumnTests
    {
        [Fact]
        public void TestUpdateSmallerList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));

            listColumn.Update(0, new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2)
            }));

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
        }

        [Fact]
        public void TestUpdateLargerList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2)
            }));

            listColumn.Update(0, new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);
        }

        [Fact]
        public void UpdateValueToEmptyList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2)
            }));

            listColumn.Update(0, new ListValue(new List<IDataValue>()));

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void UpdateValueToNull()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(3),
                new Int64Value(4)
            }));

            listColumn.Update(0, NullValue.Instance);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(0, list.Count);

            currentValue = listColumn.GetValueAt(1, default);
            list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(3, list.GetAt(0).AsLong);
            Assert.Equal(4, list.GetAt(1).AsLong);
        }

        [Fact]
        public void RemoveFirstElement()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));

            listColumn.RemoveAt(0);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(4, list.GetAt(0).AsLong);
            Assert.Equal(5, list.GetAt(1).AsLong);
        }

        [Fact]
        public void RemoveSingleElementResultsEmptyList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));

            listColumn.RemoveAt(0);

            Assert.Equal(0, listColumn.Count);
        }

        [Fact]
        public void RemoveLastElement()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));

            listColumn.RemoveAt(1);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);
        }

        [Fact]
        public void RemoveMiddleElement()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(6),
                new Int64Value(7)
            }));

            listColumn.RemoveAt(1);

            Assert.Equal(2, listColumn.Count);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);

            currentValue = listColumn.GetValueAt(1, default);
            list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(6, list.GetAt(0).AsLong);
            Assert.Equal(7, list.GetAt(1).AsLong);
        }

        [Fact]
        public void InsertAtInEmptyList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.InsertAt(0, new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);
        }

        [Fact]
        public void InsertAtInMiddleOfList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));

            listColumn.InsertAt(1, new ListValue(new List<IDataValue>()
            {
                new Int64Value(6),
                new Int64Value(7)
            }));

            Assert.Equal(3, listColumn.Count);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);

            currentValue = listColumn.GetValueAt(1, default);
            list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(6, list.GetAt(0).AsLong);
            Assert.Equal(7, list.GetAt(1).AsLong);

            currentValue = listColumn.GetValueAt(2, default);
            list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(4, list.GetAt(0).AsLong);
            Assert.Equal(5, list.GetAt(1).AsLong);
        }

        [Fact]
        public void InsertNullInMiddleOfList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));

            listColumn.InsertAt(1, NullValue.Instance);

            Assert.Equal(3, listColumn.Count);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);

            currentValue = listColumn.GetValueAt(1, default);
            list = currentValue.AsList;
            Assert.Equal(0, list.Count);

            currentValue = listColumn.GetValueAt(2, default);
            list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(4, list.GetAt(0).AsLong);
            Assert.Equal(5, list.GetAt(1).AsLong);
        }

        [Fact]
        public void InsertAtEndOfList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));

            listColumn.InsertAt(1, new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));

            Assert.Equal(2, listColumn.Count);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);

            currentValue = listColumn.GetValueAt(1, default);
            list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(4, list.GetAt(0).AsLong);
            Assert.Equal(5, list.GetAt(1).AsLong);
        }

        [Fact]
        public void InsertAtStartOfList()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));

            listColumn.InsertAt(0, new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5)
            }));

            Assert.Equal(2, listColumn.Count);

            var currentValue = listColumn.GetValueAt(0, default);
            var list = currentValue.AsList;
            Assert.Equal(2, list.Count);
            Assert.Equal(4, list.GetAt(0).AsLong);
            Assert.Equal(5, list.GetAt(1).AsLong);

            currentValue = listColumn.GetValueAt(1, default);
            list = currentValue.AsList;
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list.GetAt(0).AsLong);
            Assert.Equal(2, list.GetAt(1).AsLong);
            Assert.Equal(3, list.GetAt(2).AsLong);
        }

        [Fact]
        public void SearchBoundries()
        {
            ListColumn listColumn = new ListColumn(GlobalMemoryManager.Instance);
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(1),
                new Int64Value(2),
                new Int64Value(3)
            }));
            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5),
                new Int64Value(6)
            }));

            listColumn.Add(new ListValue(new List<IDataValue>()
            {
                new Int64Value(7),
                new Int64Value(8),
                new Int64Value(9)
            }));

            var (start, end) = listColumn.SearchBoundries(new ListValue(new List<IDataValue>()
            {
                new Int64Value(4),
                new Int64Value(5),
                new Int64Value(6)
            }), 0, 2, default, false);

            Assert.Equal(1, start);
            Assert.Equal(1, end);
        }

        [Fact]
        public void RemoveRangeNonNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                expected.Add(data);
                column.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());
            Assert.Equal(900, column.Count);

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i] == null)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    List<byte> actualByteList = new List<byte>();
                    for (int k = 0; k < actual.AsList.Count; k++)
                    {
                        actualByteList.Add((byte)actual.AsList.GetAt(k).AsLong);
                    }
                    Assert.Equal(expected[i], actualByteList.ToArray());
                }
            }
        }

        [Fact]
        public void RemoveRangeWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    column.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    expected.Add(data);
                    column.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }

            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());
            Assert.Equal(900, column.Count);

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i] == null)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    List<byte> actualByteList = new List<byte>();
                    for (int k = 0; k < actual.AsList.Count; k++)
                    {
                        actualByteList.Add((byte)actual.AsList.GetAt(k).AsLong);
                    }
                    Assert.Equal(expected[i], actualByteList.ToArray());
                }
            }
        }
    }
}
