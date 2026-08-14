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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Buffers;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Core.ColumnStore.Comparers;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class UnionColumnTests
    {
        [Fact]
        public void TestGetTypeAt()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);

            unionColumn.Add(new Int64Value(1), GlobalMemoryManager.Instance);
            unionColumn.Add(new StringValue("hello"), GlobalMemoryManager.Instance);

            Assert.Equal(ArrowTypeId.Int64, unionColumn.GetTypeAt(0, default));
            Assert.Equal(ArrowTypeId.String, unionColumn.GetTypeAt(1, default));
            unionColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestUpdateToNull()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);

            column.UpdateAt(0, NullValue.Instance);

            Assert.True(column.GetValueAt(0, default).IsNull);
            Assert.Equal("hello", column.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void TestUpdateToIntToStringColumnAreadyExists()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);

            column.UpdateAt(0, new StringValue("world"));

            Assert.Equal("world", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal("hello", column.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void TestInsertNull()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new DecimalValue(123));

            column.InsertAt(0, NullValue.Instance);

            column.InsertAt(2, NullValue.Instance);

            Assert.True(column.GetValueAt(0, default).IsNull);
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.True(column.GetValueAt(2, default).IsNull);
            Assert.Equal("hello", column.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestInsertStrings()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new DecimalValue(123));

            column.InsertAt(0, new StringValue("world"));

            column.InsertAt(2, new StringValue("foo"));

            Assert.Equal("world", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.Equal("foo", column.GetValueAt(2, default).AsString.ToString());
            Assert.Equal("hello", column.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestDelete()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new DecimalValue(123));

            column.InsertAt(0, new StringValue("world"));

            column.InsertAt(2, new StringValue("foo"));

            column.RemoveAt(2);

            Assert.Equal("world", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.Equal("hello", column.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(3, default).AsDecimal);
        }

        /// <summary>
        /// Checks bug that occured that after conversion to the union column, the value was not inserted in the correct position.
        /// </summary>
        [Fact]
        public void ConvertToUnionInsertInMiddle()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new StringValue("1"));
            column.Add(new StringValue("2"));
            column.Add(new StringValue("3"));

            column.InsertAt(1, new Int64Value(123));

            Assert.Equal("1", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(1, default).AsLong);
            Assert.Equal("2", column.GetValueAt(2, default).AsString.ToString());
            Assert.Equal("3", column.GetValueAt(3, default).AsString.ToString());
        }

        [Fact]
        public void RemoveRangeNonNull()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            List<object?> expected = new List<object?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var type = r.Next(3);

                switch (type)
                {
                    case 0:
                        column.Add(new Int64Value(i));
                        expected.Add((long)i);
                        break;
                    case 1:
                        column.Add(new DecimalValue((i)));
                        expected.Add((decimal)i);
                        break;
                    case 2:
                        var byteSize = r.Next(1, 20);
                        string data = new string(Enumerable.Range(0, byteSize).Select(x => (char)r.Next(32, 127)).ToArray());
                        expected.Add(data);
                        column.Add(new StringValue(data));
                        break;
                }
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);

            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i] == null)
                {
                    Assert.True(column.GetValueAt(i, default).IsNull);
                }
                else
                {
                    if (expected[i] is long)
                    {
                        Assert.Equal((long)expected[i]!, column.GetValueAt(i, default).AsLong);
                    }
                    else if (expected[i] is decimal)
                    {
                        Assert.Equal((decimal)expected[i]!, column.GetValueAt(i, default).AsDecimal);
                    }
                    else
                    {
                        Assert.Equal(expected[i], column.GetValueAt(i, default).AsString.ToString());
                    }
                }
            }
        }

        [Fact]
        public void RemoveRangeWithNull()
        {
            using Column column = new Column(GlobalMemoryManager.Instance);

            List<object?> expected = new List<object?>();
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
                    var type = r.Next(3);

                    switch (type)
                    {
                        case 0:
                            column.Add(new Int64Value(i));
                            expected.Add((long)i);
                            break;
                        case 1:
                            column.Add(new DecimalValue((i)));
                            expected.Add((decimal)i);
                            break;
                        case 2:
                            var byteSize = r.Next(1, 20);
                            string data = new string(Enumerable.Range(0, byteSize).Select(x => (char)r.Next(32, 127)).ToArray());
                            expected.Add(data);
                            column.Add(new StringValue(data));
                            break;
                    }
                }
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);

            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i] == null)
                {
                    Assert.True(column.GetValueAt(i, default).IsNull);
                }
                else
                {
                    if (expected[i] is long)
                    {
                        Assert.Equal((long)expected[i]!, column.GetValueAt(i, default).AsLong);
                    }
                    else if (expected[i] is decimal)
                    {
                        Assert.Equal((decimal)expected[i]!, column.GetValueAt(i, default).AsDecimal);
                    }
                    else
                    {
                        Assert.Equal(expected[i], column.GetValueAt(i, default).AsString.ToString());
                    }
                }
            }
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNoNulls()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance }
            };

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 2, default, GlobalMemoryManager.Instance);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        // No nulls but with validity list
        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNoNullsWithValidityList()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance }
            };

            BitmapList validityList = default;
            validityList.Set(0, GlobalMemoryManager.Instance);
            validityList.Set(1, GlobalMemoryManager.Instance);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 2, validityList, GlobalMemoryManager.Instance);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
            validityList.Dispose(GlobalMemoryManager.Instance);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNullInMiddle()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance }
            };

            BitmapList validityList = default;
            validityList.Set(0, GlobalMemoryManager.Instance);
            validityList.Set(2, GlobalMemoryManager.Instance);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList, GlobalMemoryManager.Instance);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.Equal("world", unionColumn.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
            validityList.Dispose(GlobalMemoryManager.Instance);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNullInStart()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { NullValue.Instance, GlobalMemoryManager.Instance },
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance }
            };

            BitmapList validityList = default;
            validityList.Set(1, GlobalMemoryManager.Instance);
            validityList.Set(2, GlobalMemoryManager.Instance);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList, GlobalMemoryManager.Instance);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.True(unionColumn.GetValueAt(1, default).IsNull);
            Assert.Equal("hello", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
            validityList.Dispose(GlobalMemoryManager.Instance);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNullInEnd()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance }
            };

            BitmapList validityList = default;
            validityList.Set(0, GlobalMemoryManager.Instance);
            validityList.Set(1, GlobalMemoryManager.Instance);
            validityList.Unset(2, GlobalMemoryManager.Instance);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList, GlobalMemoryManager.Instance);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(3, default).IsNull);
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
            validityList.Dispose(GlobalMemoryManager.Instance);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        // Test with all values set to null in the range
        [Fact]
        public void TestInsertRangeFromInsertBasicColumnAllNulls()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { NullValue.Instance, GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance }
            };

            BitmapList validityList = default;
            validityList.Unset(0, GlobalMemoryManager.Instance);
            validityList.Unset(1, GlobalMemoryManager.Instance);
            validityList.Unset(2, GlobalMemoryManager.Instance);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList, GlobalMemoryManager.Instance);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.True(unionColumn.GetValueAt(1, default).IsNull);
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.True(unionColumn.GetValueAt(3, default).IsNull);
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
            validityList.Dispose(GlobalMemoryManager.Instance);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNulSubrange()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance }
            };

            BitmapList validityList = default;
            validityList.Set(0, GlobalMemoryManager.Instance);
            validityList.Set(1, GlobalMemoryManager.Instance);
            validityList.Unset(2, GlobalMemoryManager.Instance);

            unionColumn.InsertRangeFrom(1, stringColumn, 1, 2, validityList, GlobalMemoryManager.Instance);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("world", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
            validityList.Dispose(GlobalMemoryManager.Instance);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            stringColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnWithEmptyValidityList()
        {
            using Column unionColumn = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            using Column stringColumn = new Column(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                new StringValue("world")
            };

            unionColumn.InsertRangeFrom(1, stringColumn, 1, 1);

            Assert.Equal(3, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("world", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(2, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumn()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("hello"), GlobalMemoryManager.Instance },
                { new StringValue("world"), GlobalMemoryManager.Instance }
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 0, 2, default, GlobalMemoryManager.Instance);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            otherUnionColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvx()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("1"), GlobalMemoryManager.Instance },
                { new StringValue("2"), GlobalMemoryManager.Instance },
                { new StringValue("3"), GlobalMemoryManager.Instance },
                { new StringValue("4"), GlobalMemoryManager.Instance },
                { new StringValue("5"), GlobalMemoryManager.Instance },
                { new StringValue("6"), GlobalMemoryManager.Instance },
                { new StringValue("7"), GlobalMemoryManager.Instance },
                { new StringValue("8"), GlobalMemoryManager.Instance },
                { new StringValue("9"), GlobalMemoryManager.Instance },
                { new StringValue("10"), GlobalMemoryManager.Instance },
                { new StringValue("11"), GlobalMemoryManager.Instance },
                { new StringValue("12"), GlobalMemoryManager.Instance },
                { new StringValue("13"), GlobalMemoryManager.Instance },
                { new StringValue("14"), GlobalMemoryManager.Instance },
                { new StringValue("15"), GlobalMemoryManager.Instance },
                { new StringValue("16"), GlobalMemoryManager.Instance },
                { new StringValue("17"), GlobalMemoryManager.Instance },
                { new StringValue("18"), GlobalMemoryManager.Instance },
                { new StringValue("19"), GlobalMemoryManager.Instance },
                { new StringValue("20"), GlobalMemoryManager.Instance },
                { new StringValue("21"), GlobalMemoryManager.Instance },
                { new StringValue("22"), GlobalMemoryManager.Instance },
                { new StringValue("23"), GlobalMemoryManager.Instance },
                { new StringValue("24"), GlobalMemoryManager.Instance },
                { new StringValue("25"), GlobalMemoryManager.Instance },
                { new StringValue("26"), GlobalMemoryManager.Instance },
                { new StringValue("27"), GlobalMemoryManager.Instance },
                { new StringValue("28"), GlobalMemoryManager.Instance },
                { new StringValue("29"), GlobalMemoryManager.Instance },
                { new StringValue("30"), GlobalMemoryManager.Instance },
                { new StringValue("31"), GlobalMemoryManager.Instance },
                { new StringValue("32"), GlobalMemoryManager.Instance },
                { new StringValue("33"), GlobalMemoryManager.Instance },
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 0, 33, default, GlobalMemoryManager.Instance);

            Assert.Equal(35, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 1; i <= 33; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(34, default).AsDecimal);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            otherUnionColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvxSubrange()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("1"), GlobalMemoryManager.Instance },
                { new StringValue("2"), GlobalMemoryManager.Instance },
                { new StringValue("3"), GlobalMemoryManager.Instance },
                { new StringValue("4"), GlobalMemoryManager.Instance },
                { new StringValue("5"), GlobalMemoryManager.Instance },
                { new StringValue("6"), GlobalMemoryManager.Instance },
                { new StringValue("7"), GlobalMemoryManager.Instance },
                { new StringValue("8"), GlobalMemoryManager.Instance },
                { new StringValue("9"), GlobalMemoryManager.Instance },
                { new StringValue("10"), GlobalMemoryManager.Instance },
                { new StringValue("11"), GlobalMemoryManager.Instance },
                { new StringValue("12"), GlobalMemoryManager.Instance },
                { new StringValue("13"), GlobalMemoryManager.Instance },
                { new StringValue("14"), GlobalMemoryManager.Instance },
                { new StringValue("15"), GlobalMemoryManager.Instance },
                { new StringValue("16"), GlobalMemoryManager.Instance },
                { new StringValue("17"), GlobalMemoryManager.Instance },
                { new StringValue("18"), GlobalMemoryManager.Instance },
                { new StringValue("19"), GlobalMemoryManager.Instance },
                { new StringValue("20"), GlobalMemoryManager.Instance },
                { new StringValue("21"), GlobalMemoryManager.Instance },
                { new StringValue("22"), GlobalMemoryManager.Instance },
                { new StringValue("23"), GlobalMemoryManager.Instance },
                { new StringValue("24"), GlobalMemoryManager.Instance },
                { new StringValue("25"), GlobalMemoryManager.Instance },
                { new StringValue("26"), GlobalMemoryManager.Instance },
                { new StringValue("27"), GlobalMemoryManager.Instance },
                { new StringValue("28"), GlobalMemoryManager.Instance },
                { new StringValue("29"), GlobalMemoryManager.Instance },
                { new StringValue("30"), GlobalMemoryManager.Instance },
                { new StringValue("31"), GlobalMemoryManager.Instance },
                { new StringValue("32"), GlobalMemoryManager.Instance },
                { new StringValue("33"), GlobalMemoryManager.Instance },
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 1, 31, default, GlobalMemoryManager.Instance);

            Assert.Equal(33, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 2; i <= 32; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i - 1, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(32, default).AsDecimal);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            otherUnionColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvxExistingDataInType()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance },
                { new StringValue("1a"), GlobalMemoryManager.Instance },
                { new StringValue("2a"), GlobalMemoryManager.Instance },
                { new StringValue("3a"), GlobalMemoryManager.Instance },
                { new StringValue("4a"), GlobalMemoryManager.Instance },
                { new StringValue("5a"), GlobalMemoryManager.Instance },
                { new StringValue("6a"), GlobalMemoryManager.Instance },
                { new StringValue("7a"), GlobalMemoryManager.Instance },
                { new StringValue("8a"), GlobalMemoryManager.Instance },
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("1"), GlobalMemoryManager.Instance },
                { new StringValue("2"), GlobalMemoryManager.Instance },
                { new StringValue("3"), GlobalMemoryManager.Instance },
                { new StringValue("4"), GlobalMemoryManager.Instance },
                { new StringValue("5"), GlobalMemoryManager.Instance },
                { new StringValue("6"), GlobalMemoryManager.Instance },
                { new StringValue("7"), GlobalMemoryManager.Instance },
                { new StringValue("8"), GlobalMemoryManager.Instance },
                { new StringValue("9"), GlobalMemoryManager.Instance },
                { new StringValue("10"), GlobalMemoryManager.Instance },
                { new StringValue("11"), GlobalMemoryManager.Instance },
                { new StringValue("12"), GlobalMemoryManager.Instance },
                { new StringValue("13"), GlobalMemoryManager.Instance },
                { new StringValue("14"), GlobalMemoryManager.Instance },
                { new StringValue("15"), GlobalMemoryManager.Instance },
                { new StringValue("16"), GlobalMemoryManager.Instance },
                { new StringValue("17"), GlobalMemoryManager.Instance },
                { new StringValue("18"), GlobalMemoryManager.Instance },
                { new StringValue("19"), GlobalMemoryManager.Instance },
                { new StringValue("20"), GlobalMemoryManager.Instance },
                { new StringValue("21"), GlobalMemoryManager.Instance },
                { new StringValue("22"), GlobalMemoryManager.Instance },
                { new StringValue("23"), GlobalMemoryManager.Instance },
                { new StringValue("24"), GlobalMemoryManager.Instance },
                { new StringValue("25"), GlobalMemoryManager.Instance },
                { new StringValue("26"), GlobalMemoryManager.Instance },
                { new StringValue("27"), GlobalMemoryManager.Instance },
                { new StringValue("28"), GlobalMemoryManager.Instance },
                { new StringValue("29"), GlobalMemoryManager.Instance },
                { new StringValue("30"), GlobalMemoryManager.Instance },
                { new StringValue("31"), GlobalMemoryManager.Instance },
                { new StringValue("32"), GlobalMemoryManager.Instance },
                { new StringValue("33"), GlobalMemoryManager.Instance },
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 1, 31, default, GlobalMemoryManager.Instance);

            Assert.Equal(41, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 2; i <= 32; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i - 1, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(32, default).AsDecimal);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            otherUnionColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvxExistingDataInTypeInMiddle()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance },
                { new StringValue("1a"), GlobalMemoryManager.Instance },
                { new StringValue("2a"), GlobalMemoryManager.Instance },
                { new StringValue("3a"), GlobalMemoryManager.Instance },
                { new StringValue("4a"), GlobalMemoryManager.Instance },
                { new StringValue("5a"), GlobalMemoryManager.Instance },
                { new StringValue("6a"), GlobalMemoryManager.Instance },
                { new StringValue("7a"), GlobalMemoryManager.Instance },
                { new StringValue("8a"), GlobalMemoryManager.Instance },
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new StringValue("1"), GlobalMemoryManager.Instance },
                { new StringValue("2"), GlobalMemoryManager.Instance },
                { new StringValue("3"), GlobalMemoryManager.Instance },
                { new StringValue("4"), GlobalMemoryManager.Instance },
                { new StringValue("5"), GlobalMemoryManager.Instance },
                { new StringValue("6"), GlobalMemoryManager.Instance },
                { new StringValue("7"), GlobalMemoryManager.Instance },
                { new StringValue("8"), GlobalMemoryManager.Instance },
                { new StringValue("9"), GlobalMemoryManager.Instance },
                { new StringValue("10"), GlobalMemoryManager.Instance },
                { new StringValue("11"), GlobalMemoryManager.Instance },
                { new StringValue("12"), GlobalMemoryManager.Instance },
                { new StringValue("13"), GlobalMemoryManager.Instance },
                { new StringValue("14"), GlobalMemoryManager.Instance },
                { new StringValue("15"), GlobalMemoryManager.Instance },
                { new StringValue("16"), GlobalMemoryManager.Instance },
                { new StringValue("17"), GlobalMemoryManager.Instance },
                { new StringValue("18"), GlobalMemoryManager.Instance },
                { new StringValue("19"), GlobalMemoryManager.Instance },
                { new StringValue("20"), GlobalMemoryManager.Instance },
                { new StringValue("21"), GlobalMemoryManager.Instance },
                { new StringValue("22"), GlobalMemoryManager.Instance },
                { new StringValue("23"), GlobalMemoryManager.Instance },
                { new StringValue("24"), GlobalMemoryManager.Instance },
                { new StringValue("25"), GlobalMemoryManager.Instance },
                { new StringValue("26"), GlobalMemoryManager.Instance },
                { new StringValue("27"), GlobalMemoryManager.Instance },
                { new StringValue("28"), GlobalMemoryManager.Instance },
                { new StringValue("29"), GlobalMemoryManager.Instance },
                { new StringValue("30"), GlobalMemoryManager.Instance },
                { new StringValue("31"), GlobalMemoryManager.Instance },
                { new StringValue("32"), GlobalMemoryManager.Instance },
                { new StringValue("33"), GlobalMemoryManager.Instance },
            };

            unionColumn.InsertRangeFrom(5, otherUnionColumn, 1, 31, default, GlobalMemoryManager.Instance);

            Assert.Equal(41, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal(3, unionColumn.GetValueAt(1, default).AsDecimal);
            Assert.Equal("1a", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal("2a", unionColumn.GetValueAt(3, default).AsString.ToString());
            Assert.Equal("3a", unionColumn.GetValueAt(4, default).AsString.ToString());
            for (int i = 2; i <= 32; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i + 3, default).AsString.ToString());
            }
            Assert.Equal("4a", Assert.IsType<StringValue>(unionColumn.GetValueAt(36, default)).ToString());
            Assert.Equal("5a", Assert.IsType<StringValue>(unionColumn.GetValueAt(37, default)).ToString());
            Assert.Equal("6a", Assert.IsType<StringValue>(unionColumn.GetValueAt(38, default)).ToString());
            Assert.Equal("7a", Assert.IsType<StringValue>(unionColumn.GetValueAt(39, default)).ToString());
            Assert.Equal("8a", Assert.IsType<StringValue>(unionColumn.GetValueAt(40, default)).ToString());
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            otherUnionColumn.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestInsertNullUnionColumn()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            UnionColumn other = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { NullValue.Instance, GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance }
            };

            unionColumn.InsertRangeFrom(2, other, 0, 2, default, GlobalMemoryManager.Instance);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(2, unionColumn.GetDataColumn(0).Count);
            unionColumn.Dispose(GlobalMemoryManager.Instance);
            other.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestRemoveRangeWitNull()
        {
            UnionColumn column = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { NullValue.Instance, GlobalMemoryManager.Instance },
                { NullValue.Instance, GlobalMemoryManager.Instance }
            };

            column.RemoveRange(0, 2, GlobalMemoryManager.Instance);
            Assert.Empty(column);
            Assert.Equal(0, column.GetDataColumn(0).Count);
            column.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void TestCopy()
        {
            using Column column = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            using var copy = column.Copy(GlobalMemoryManager.Instance);

            Assert.Equal(2, copy.Count);

            Assert.Equal(1, copy.GetValueAt(0, default).AsLong);
            Assert.Equal(3, copy.GetValueAt(1, default).AsDecimal);
        }

        [Fact]
        public void TestAddToHash()
        {
            using Column column = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            var hash = new XxHash32();
            column.AddToHash(0, default, hash);
            var columnHash = hash.GetHashAndReset();

            column.GetValueAt(0, default).AddToHash(hash);
            var valueHash = hash.GetHashAndReset();

            Assert.Equal(columnHash, valueHash);

            column.AddToHash(1, default, hash);
            columnHash = hash.GetHashAndReset();

            column.GetValueAt(1, default).AddToHash(hash);
            valueHash = hash.GetHashAndReset();

            Assert.Equal(columnHash, valueHash);
        }

        [Fact]
        public void InsertRangeFromBasicNullInColumn()
        {
            using Column unionColumn = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3),
                new StringValue("1a")
            };

            using Column otherUnionColumn = new Column(GlobalMemoryManager.Instance)
            {
                new StringValue("1"),
                new StringValue("2"),
                new StringValue("3"),
                new StringValue("4"),
                new NullValue()
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 1, 2);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 2; i <= 3; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i - 1, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromUnionWithTypeIdsAbove15IntoNarrowUnion()
        {
            UnionColumn source = new UnionColumn(GlobalMemoryManager.Instance);

            // Claim type ids 1..17 with distinct struct headers so the copied range uses an id above 15.
            var headers = new StructHeader[17];
            for (int i = 0; i < 17; i++)
            {
                headers[i] = StructHeader.Create("col" + i);
                source.Add(new StructValue(headers[i], new Int64Value(i)), GlobalMemoryManager.Instance);
            }
            var wideHeader = headers[16];
            for (int i = 0; i < 16; i++)
            {
                source.Add(new StructValue(wideHeader, new Int64Value(100 + i)), GlobalMemoryManager.Instance);
            }

            UnionColumn target = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            target.InsertRangeFrom(1, source, 17, 16, default, GlobalMemoryManager.Instance);

            Assert.Equal(18, target.Count);
            Assert.Equal(1, target.GetValueAt(0, default).AsLong);
            for (int i = 0; i < 16; i++)
            {
                var expected = new StructValue(wideHeader, new Int64Value(100 + i));
                Assert.Equal(0, DataValueComparer.CompareTo(expected, target.GetValueAt(1 + i, default)));
            }
            Assert.Equal(3, target.GetValueAt(17, default).AsDecimal);
            target.Dispose(GlobalMemoryManager.Instance);
            source.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void InsertRangeFromUnionWithTypeIdsAbove7IntoNarrowUnion()
        {
            UnionColumn source = new UnionColumn(GlobalMemoryManager.Instance);

            // Claim type ids 1..9 so the copied range uses an id above 7 with a non-zero start offset.
            var headers = new StructHeader[9];
            for (int i = 0; i < 9; i++)
            {
                headers[i] = StructHeader.Create("col" + i);
                source.Add(new StructValue(headers[i], new Int64Value(i)), GlobalMemoryManager.Instance);
            }
            var wideHeader = headers[8];
            for (int i = 0; i < 8; i++)
            {
                source.Add(new StructValue(wideHeader, new Int64Value(100 + i)), GlobalMemoryManager.Instance);
            }

            UnionColumn target = new UnionColumn(GlobalMemoryManager.Instance)
            {
                { new Int64Value(1), GlobalMemoryManager.Instance },
                { new DecimalValue(3), GlobalMemoryManager.Instance }
            };

            target.InsertRangeFrom(1, source, 9, 8, default, GlobalMemoryManager.Instance);

            Assert.Equal(10, target.Count);
            Assert.Equal(1, target.GetValueAt(0, default).AsLong);
            for (int i = 0; i < 8; i++)
            {
                var expected = new StructValue(wideHeader, new Int64Value(100 + i));
                Assert.Equal(0, DataValueComparer.CompareTo(expected, target.GetValueAt(1 + i, default)));
            }
            Assert.Equal(3, target.GetValueAt(9, default).AsDecimal);
            target.Dispose(GlobalMemoryManager.Instance);
            source.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void InsertRangeFromBasicNullInRange()
        {
            using Column unionColumn = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            using Column otherUnionColumn = new Column(GlobalMemoryManager.Instance)
            {
                new StringValue("1"),
                new StringValue("2"),
                new NullValue(),
                new NullValue(),
                new StringValue("3"),
                new StringValue("4"),
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 1, 4);

            Assert.Equal(6, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("2", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.True(unionColumn.GetValueAt(3, default).IsNull);
            Assert.Equal("3", unionColumn.GetValueAt(4, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(5, default).AsDecimal);
        }
    }
}
