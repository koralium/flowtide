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
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Text.Json;

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

        [Fact]
        public void InsertRangeNoNullIntoEmptyColumn()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]> expected = new List<byte[]>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                expected.Add(data);
                other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
            }

            column.InsertRangeFrom(0, other, 5, 50);

            Assert.Equal(50, column.Count);

            for (int i = 0; i < 50; i++)
            {
                var actual = column.GetValueAt(i, default);
                List<byte> actualByteList = new List<byte>();
                for (int k = 0; k < actual.AsList.Count; k++)
                {
                    actualByteList.Add((byte)actual.AsList.GetAt(k).AsLong);
                }
                Assert.Equal(expected[i + 5], actualByteList.ToArray());
            }
        }

        [Fact]
        public void InsertRangeNoNullIntoNonEmptyColumn()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]> existing = new List<byte[]>();
            List<byte[]> expected = new List<byte[]>();
            Random r = new Random(123);

            // Was 50
            for (int i = 0; i < 50; i++)
            {
                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                existing.Add(data);
                column.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
            }

            for (int i = 0; i < 1000; i++)
            {
                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                expected.Add(data);
                other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
            }

            // Was 13 and 50
            column.InsertRangeFrom(13, other, 5, 50);

            Assert.Equal(100, column.Count);

            for (int i = 0; i < 13; i++)
            {
                var actual = column.GetValueAt(i, default);
                List<byte> actualByteList = new List<byte>();
                for (int k = 0; k < actual.AsList.Count; k++)
                {
                    actualByteList.Add((byte)actual.AsList.GetAt(k).AsLong);
                }
                Assert.Equal(existing[i], actualByteList.ToArray());
            }

            for (int i = 0; i < 50; i++)
            {
                var actual = column.GetValueAt(i + 13, default);
                List<byte> actualByteList = new List<byte>();
                for (int k = 0; k < actual.AsList.Count; k++)
                {
                    actualByteList.Add((byte)actual.AsList.GetAt(k).AsLong);
                }
                Assert.Equal(expected[i + 5], actualByteList.ToArray());
            }

            for (int i = 0; i < 37; i++)
            {
                var actual = column.GetValueAt(i + 63, default);
                List<byte> actualByteList = new List<byte>();
                for (int k = 0; k < actual.AsList.Count; k++)
                {
                    actualByteList.Add((byte)actual.AsList.GetAt(k).AsLong);
                }
                Assert.Equal(existing[i + 13], actualByteList.ToArray());
            }
        }

        [Fact]
        public void InsertRangeWithNullIntoEmptyColumn()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    other.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    expected.Add(data);
                    other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }
            }

            column.InsertRangeFrom(0, other, 5, 50);

            Assert.Equal(50, column.Count);

            for (int i = 0; i < 50; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i + 5] == null)
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
                    Assert.Equal(expected[i + 5], actualByteList.ToArray());
                }
            }
        }

        [Fact]
        public void InsertRangeWithNullIntoNullColumnWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    other.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    expected.Add(data);
                    other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }
            }

            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);

            column.InsertRangeFrom(5, other, 5, 50);

            Assert.Equal(58, column.Count);

            for (int i = 0; i < 5; i++)
            {
                var actual = column.GetValueAt(i, default);
                Assert.True(actual.IsNull);
            }

            for (int i = 5; i < 55; i++)
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

            for (int i = 55; i < 58; i++)
            {
                var actual = column.GetValueAt(i, default);
                Assert.True(actual.IsNull);
            }
        }

        [Fact]
        public void InsertRangeWithNullIntoNonEmptyColumn()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> existing = new List<byte[]?>();
            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);

            // Was 50
            for (int i = 0; i < 50; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    existing.Add(null);
                    column.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    existing.Add(data);
                    column.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }
            }

            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    other.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    expected.Add(data);
                    other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }
            }

            column.InsertRangeFrom(13, other, 5, 50);

            Assert.Equal(100, column.Count);

            for (int i = 0; i < 13; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (existing[i] == null)
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
                    Assert.Equal(existing[i], actualByteList.ToArray());
                }

            }

            for (int i = 0; i < 50; i++)
            {
                var actual = column.GetValueAt(i + 13, default);
                if (expected[i + 5] == null)
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
                    Assert.Equal(expected[i + 5], actualByteList.ToArray());
                }
            }

            for (int i = 0; i < 37; i++)
            {
                var actual = column.GetValueAt(i + 63, default);
                if (existing[i + 13] == null)
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
                    Assert.Equal(existing[i + 13], actualByteList.ToArray());
                }
            }
        }

        [Fact]
        public void InsertRangeWitNonNullIntoNonEmptyColumnWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> existing = new List<byte[]?>();
            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);

            // Was 50
            for (int i = 0; i < 50; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    existing.Add(null);
                    column.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    existing.Add(data);
                    column.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }
            }

            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                expected.Add(data);
                other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
            }

            column.InsertRangeFrom(13, other, 5, 50);

            Assert.Equal(100, column.Count);

            for (int i = 0; i < 13; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (existing[i] == null)
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
                    Assert.Equal(existing[i], actualByteList.ToArray());
                }

            }

            for (int i = 0; i < 50; i++)
            {
                var actual = column.GetValueAt(i + 13, default);
                if (expected[i + 5] == null)
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
                    Assert.Equal(expected[i + 5], actualByteList.ToArray());
                }
            }

            for (int i = 0; i < 37; i++)
            {
                var actual = column.GetValueAt(i + 63, default);
                if (existing[i + 13] == null)
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
                    Assert.Equal(existing[i + 13], actualByteList.ToArray());
                }
            }
        }

        [Fact]
        public void InsertRangeWithNullIntoNonEmptyColumnWithNoNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> existing = new List<byte[]?>();
            List<byte[]?> expected = new List<byte[]?>();
            Random r = new Random(123);

            // Was 50
            for (int i = 0; i < 50; i++)
            {
                var isNull = r.Next(2) == 0;

                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                existing.Add(data);
                column.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
            }

            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    other.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    expected.Add(data);
                    other.Add(new ListValue(data.Select(x => (IDataValue)new Int64Value(x)).ToList()));
                }
            }

            column.InsertRangeFrom(13, other, 5, 50);

            Assert.Equal(100, column.Count);

            for (int i = 0; i < 13; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (existing[i] == null)
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
                    Assert.Equal(existing[i], actualByteList.ToArray());
                }

            }

            for (int i = 0; i < 50; i++)
            {
                var actual = column.GetValueAt(i + 13, default);
                if (expected[i + 5] == null)
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
                    Assert.Equal(expected[i + 5], actualByteList.ToArray());
                }
            }

            for (int i = 0; i < 37; i++)
            {
                var actual = column.GetValueAt(i + 63, default);
                if (existing[i + 13] == null)
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
                    Assert.Equal(existing[i + 13], actualByteList.ToArray());
                }
            }
        }

        [Fact]
        public void TestRemoveRangeNullLastBitmapSegment()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            for (int i = 0; i < 63; i++)
            {
                column.Add(new ListValue(new Int64Value(i)));
            }
            column.Add(new ListValue(NullValue.Instance));
            column.Add(NullValue.Instance);

            column.RemoveRange(64, 1);

            Assert.Equal(64, column.Count);
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new ListValue(new Int64Value(1), new Int64Value(3))
            };

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("[1,3]", json);
        }

        [Fact]
        public void TestCopy()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new ListValue(new Int64Value(1), new Int64Value(3))
            };

            Column copy = column.Copy(GlobalMemoryManager.Instance);

            Assert.Single(copy);
            Assert.Equal(2, copy.GetValueAt(0, default).AsList.Count);
            Assert.Equal(1, copy.GetValueAt(0, default).AsList.GetAt(0).AsLong);
            Assert.Equal(3, copy.GetValueAt(0, default).AsList.GetAt(1).AsLong);
        }

        [Fact]
        public void TestAddToHash()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new ListValue(new Int64Value(1), new Int64Value(3))
            };

            var hash = new XxHash32();
            column.AddToHash(0, default, hash);
            var columnHash = hash.GetHashAndReset();

            column.GetValueAt(0, default).AddToHash(hash);
            var valueHash = hash.GetHashAndReset();

            Assert.Equal(columnHash, valueHash);
        }
    }
}
