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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class UnionColumnTests
    {
        [Fact]
        public void TestGetTypeAt()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);

            unionColumn.Add(new Int64Value(1));
            unionColumn.Add(new StringValue("hello"));

            Assert.Equal(ArrowTypeId.Int64, unionColumn.GetTypeAt(0, default));
            Assert.Equal(ArrowTypeId.String, unionColumn.GetTypeAt(1, default));
        }

        [Fact]
        public void TestUpdateToNull()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

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
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

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
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

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
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

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
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

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
            Column column = new Column(GlobalMemoryManager.Instance);

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
            Column column = new Column(GlobalMemoryManager.Instance);

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
            Column column = new Column(GlobalMemoryManager.Instance);

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
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                new StringValue("world")
            };

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 2, default);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
        }

        // No nulls but with validity list
        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNoNullsWithValidityList()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                new StringValue("world")
            };

            BitmapList validityList = new BitmapList(GlobalMemoryManager.Instance);
            validityList.Set(0);
            validityList.Set(1);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 2, validityList);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNullInMiddle()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                NullValue.Instance,
                new StringValue("world")
            };

            BitmapList validityList = new BitmapList(GlobalMemoryManager.Instance);
            validityList.Set(0);
            validityList.Set(2);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.Equal("world", unionColumn.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNullInStart()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                new StringValue("hello"),
                new StringValue("world")
            };

            BitmapList validityList = new BitmapList(GlobalMemoryManager.Instance);
            validityList.Set(1);
            validityList.Set(2);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.True(unionColumn.GetValueAt(1, default).IsNull);
            Assert.Equal("hello", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNullInEnd()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                new StringValue("world"),
                NullValue.Instance
            };

            BitmapList validityList = new BitmapList(GlobalMemoryManager.Instance);
            validityList.Set(0);
            validityList.Set(1);
            validityList.Unset(2);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(3, default).IsNull);
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
        }

        // Test with all values set to null in the range
        [Fact]
        public void TestInsertRangeFromInsertBasicColumnAllNulls()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                NullValue.Instance,
                NullValue.Instance
            };

            BitmapList validityList = new BitmapList(GlobalMemoryManager.Instance);
            validityList.Unset(0);
            validityList.Unset(1);
            validityList.Unset(2);

            unionColumn.InsertRangeFrom(1, stringColumn, 0, 3, validityList);

            Assert.Equal(5, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.True(unionColumn.GetValueAt(1, default).IsNull);
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.True(unionColumn.GetValueAt(3, default).IsNull);
            Assert.Equal(3, unionColumn.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestInsertRangeFromInsertBasicColumnNulSubrange()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            StringColumn stringColumn = new StringColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                new StringValue("world"),
                NullValue.Instance
            };

            BitmapList validityList = new BitmapList(GlobalMemoryManager.Instance);
            validityList.Set(0);
            validityList.Set(1);
            validityList.Unset(2);

            unionColumn.InsertRangeFrom(1, stringColumn, 1, 2, validityList);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("world", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.True(unionColumn.GetValueAt(2, default).IsNull);
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumn()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("hello"),
                new StringValue("world")
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 0, 2, default);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", unionColumn.GetValueAt(1, default).AsString.ToString());
            Assert.Equal("world", unionColumn.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(3, unionColumn.GetValueAt(3, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvx()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("1"),
                new StringValue("2"),
                new StringValue("3"),
                new StringValue("4"),
                new StringValue("5"),
                new StringValue("6"),
                new StringValue("7"),
                new StringValue("8"),
                new StringValue("9"),
                new StringValue("10"),
                new StringValue("11"),
                new StringValue("12"),
                new StringValue("13"),
                new StringValue("14"),
                new StringValue("15"),
                new StringValue("16"),
                new StringValue("17"),
                new StringValue("18"),
                new StringValue("19"),
                new StringValue("20"),
                new StringValue("21"),
                new StringValue("22"),
                new StringValue("23"),
                new StringValue("24"),
                new StringValue("25"),
                new StringValue("26"),
                new StringValue("27"),
                new StringValue("28"),
                new StringValue("29"),
                new StringValue("30"),
                new StringValue("31"),
                new StringValue("32"),
                new StringValue("33"),
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 0, 33, default);

            Assert.Equal(35, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 1; i <= 33; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(34, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvxSubrange()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("1"),
                new StringValue("2"),
                new StringValue("3"),
                new StringValue("4"),
                new StringValue("5"),
                new StringValue("6"),
                new StringValue("7"),
                new StringValue("8"),
                new StringValue("9"),
                new StringValue("10"),
                new StringValue("11"),
                new StringValue("12"),
                new StringValue("13"),
                new StringValue("14"),
                new StringValue("15"),
                new StringValue("16"),
                new StringValue("17"),
                new StringValue("18"),
                new StringValue("19"),
                new StringValue("20"),
                new StringValue("21"),
                new StringValue("22"),
                new StringValue("23"),
                new StringValue("24"),
                new StringValue("25"),
                new StringValue("26"),
                new StringValue("27"),
                new StringValue("28"),
                new StringValue("29"),
                new StringValue("30"),
                new StringValue("31"),
                new StringValue("32"),
                new StringValue("33"),
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 1, 31, default);

            Assert.Equal(33, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 2; i <= 32; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i - 1, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(32, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvxExistingDataInType()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3),
                new StringValue("1a"),
                new StringValue("2a"),
                new StringValue("3a"),
                new StringValue("4a"),
                new StringValue("5a"),
                new StringValue("6a"),
                new StringValue("7a"),
                new StringValue("8a"),
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("1"),
                new StringValue("2"),
                new StringValue("3"),
                new StringValue("4"),
                new StringValue("5"),
                new StringValue("6"),
                new StringValue("7"),
                new StringValue("8"),
                new StringValue("9"),
                new StringValue("10"),
                new StringValue("11"),
                new StringValue("12"),
                new StringValue("13"),
                new StringValue("14"),
                new StringValue("15"),
                new StringValue("16"),
                new StringValue("17"),
                new StringValue("18"),
                new StringValue("19"),
                new StringValue("20"),
                new StringValue("21"),
                new StringValue("22"),
                new StringValue("23"),
                new StringValue("24"),
                new StringValue("25"),
                new StringValue("26"),
                new StringValue("27"),
                new StringValue("28"),
                new StringValue("29"),
                new StringValue("30"),
                new StringValue("31"),
                new StringValue("32"),
                new StringValue("33"),
            };

            unionColumn.InsertRangeFrom(1, otherUnionColumn, 1, 31, default);

            Assert.Equal(41, unionColumn.Count);
            Assert.Equal(1, unionColumn.GetValueAt(0, default).AsLong);
            for (int i = 2; i <= 32; i++)
            {
                Assert.Equal(i.ToString(), unionColumn.GetValueAt(i - 1, default).AsString.ToString());
            }
            Assert.Equal(3, unionColumn.GetValueAt(32, default).AsDecimal);
        }

        [Fact]
        public void InsertRangeFromOtherUnionColumnWithAvxExistingDataInTypeInMiddle()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3),
                new StringValue("1a"),
                new StringValue("2a"),
                new StringValue("3a"),
                new StringValue("4a"),
                new StringValue("5a"),
                new StringValue("6a"),
                new StringValue("7a"),
                new StringValue("8a"),
            };

            UnionColumn otherUnionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new StringValue("1"),
                new StringValue("2"),
                new StringValue("3"),
                new StringValue("4"),
                new StringValue("5"),
                new StringValue("6"),
                new StringValue("7"),
                new StringValue("8"),
                new StringValue("9"),
                new StringValue("10"),
                new StringValue("11"),
                new StringValue("12"),
                new StringValue("13"),
                new StringValue("14"),
                new StringValue("15"),
                new StringValue("16"),
                new StringValue("17"),
                new StringValue("18"),
                new StringValue("19"),
                new StringValue("20"),
                new StringValue("21"),
                new StringValue("22"),
                new StringValue("23"),
                new StringValue("24"),
                new StringValue("25"),
                new StringValue("26"),
                new StringValue("27"),
                new StringValue("28"),
                new StringValue("29"),
                new StringValue("30"),
                new StringValue("31"),
                new StringValue("32"),
                new StringValue("33"),
            };

            unionColumn.InsertRangeFrom(5, otherUnionColumn, 1, 31, default);

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
        }

        [Fact]
        public void TestInsertNullUnionColumn()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new DecimalValue(3)
            };

            UnionColumn other = new UnionColumn(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                NullValue.Instance
            };

            unionColumn.InsertRangeFrom(2, other, 0, 2, default);

            Assert.Equal(4, unionColumn.Count);
            Assert.Equal(2, unionColumn.GetDataColumn(0).Count);
        }

        [Fact]
        public void TestRemoveRangeWitNull()
        {
            UnionColumn column = new UnionColumn(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                NullValue.Instance
            };

            column.RemoveRange(0, 2);
            Assert.Empty(column);
            Assert.Equal(0, column.GetDataColumn(0).Count);
        }
    }
}
