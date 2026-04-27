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
    public class ColumnSizeInfoTests
    {
        [Fact]
        public void MergeSameTypeAccumulatesRowsAndVariableBytes()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10, TotalVariableBytes = 200 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 5, TotalVariableBytes = 100 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.String, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Equal(300, a.TotalVariableBytes);
        }

        [Fact]
        public void MergeSameIntTypePicksLargerBitWidth()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 8 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5, BitWidth = 32 };

            a.Merge(b);

            Assert.Equal(32, a.BitWidth);
            Assert.Equal(15, a.TotalRows);
        }

        [Fact]
        public void MergeSameIntTypeBitWidthDoesNotDecrease()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 64 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5, BitWidth = 16 };

            a.Merge(b);

            Assert.Equal(64, a.BitWidth);
        }

        [Fact]
        public void MergeDifferentTypesPromotesToUnion()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10, TotalVariableBytes = 100 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5, BitWidth = 32 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.NotNull(a.Children);
            Assert.Equal(2, a.Children!.Count);
            Assert.Equal(ArrowTypeId.String, a.Children[0].DataType);
            Assert.Equal(10, a.Children[0].TotalRows);
            Assert.Equal(100, a.Children[0].TotalVariableBytes);
            Assert.Equal(ArrowTypeId.Int64, a.Children[1].DataType);
            Assert.Equal(5, a.Children[1].TotalRows);
            Assert.Equal(32, a.Children[1].BitWidth);
        }

        [Fact]
        public void MergeUnionWithNewTypeAddsChild()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5 };
            var c = new ColumnSizeInfo { DataType = ArrowTypeId.Double, TotalRows = 3 };

            a.Merge(b);
            a.Merge(c);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(18, a.TotalRows);
            Assert.Equal(3, a.Children!.Count);
            Assert.Equal(ArrowTypeId.Double, a.Children[2].DataType);
            Assert.Equal(3, a.Children[2].TotalRows);
        }

        [Fact]
        public void MergeUnionWithExistingTypeMergesIntoChild()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10, TotalVariableBytes = 100 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5 };
            var c = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 3, TotalVariableBytes = 50 };

            a.Merge(b);
            a.Merge(c);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(18, a.TotalRows);
            Assert.Equal(2, a.Children!.Count);
            Assert.Equal(ArrowTypeId.String, a.Children[0].DataType);
            Assert.Equal(13, a.Children[0].TotalRows);
            Assert.Equal(150, a.Children[0].TotalVariableBytes);
        }

        [Fact]
        public void MergeTwoUnionsMergesChildren()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5 };
            a.Merge(b);

            var c = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 3 };
            var d = new ColumnSizeInfo { DataType = ArrowTypeId.Double, TotalRows = 7 };
            c.Merge(d);

            a.Merge(c);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(25, a.TotalRows);
            Assert.Equal(3, a.Children!.Count);

            var stringChild = a.Children.First(x => x.DataType == ArrowTypeId.String);
            Assert.Equal(10, stringChild.TotalRows);

            var intChild = a.Children.First(x => x.DataType == ArrowTypeId.Int64);
            Assert.Equal(8, intChild.TotalRows);

            var doubleChild = a.Children.First(x => x.DataType == ArrowTypeId.Double);
            Assert.Equal(7, doubleChild.TotalRows);
        }

        [Fact]
        public void MergeSameTypeWithChildrenMergesRecursively()
        {
            var a = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 10,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 30, TotalVariableBytes = 500 }
                }
            };
            var b = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 5,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 20, TotalVariableBytes = 300 }
                }
            };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.List, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Single(a.Children!);
            Assert.Equal(50, a.Children[0].TotalRows);
            Assert.Equal(800, a.Children[0].TotalVariableBytes);
        }

        [Fact]
        public void MergeSameStructWithSameHeaderMergesChildren()
        {
            var header = StructHeader.Create("a", "b");

            var a = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Struct,
                StructHeader = header,
                TotalRows = 10,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 8 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10, TotalVariableBytes = 100 },
                }
            };
            var b = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Struct,
                StructHeader = header,
                TotalRows = 5,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5, BitWidth = 32 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 5, TotalVariableBytes = 50 },
                }
            };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Struct, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Equal(2, a.Children!.Count);
            Assert.Equal(15, a.Children[0].TotalRows);
            Assert.Equal(32, a.Children[0].BitWidth);
            Assert.Equal(15, a.Children[1].TotalRows);
            Assert.Equal(150, a.Children[1].TotalVariableBytes);
        }

        [Fact]
        public void MergeDifferentStructHeadersPromotesToUnion()
        {
            var headerA = StructHeader.Create("x", "y");
            var headerB = StructHeader.Create("a", "b");

            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Struct, StructHeader = headerA, TotalRows = 10 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Struct, StructHeader = headerB, TotalRows = 5 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Equal(2, a.Children!.Count);
        }

        [Fact]
        public void MergeNullIntoNullAccumulatesRows()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Null, TotalRows = 5 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Null, TotalRows = 3 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Null, a.DataType);
            Assert.Equal(8, a.TotalRows);
        }

        [Fact]
        public void MergeTypeIntoNullChangesToIncomingType()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Null, TotalRows = 5 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 3 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Int64, a.DataType);
            Assert.Equal(8, a.TotalRows);
            Assert.Null(a.Children);
        }

        [Fact]
        public void MergeNullIntoTypePromotesToUnion()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Null, TotalRows = 3 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Int64, a.DataType);
            Assert.Equal(8, a.TotalRows);
        }

        [Fact]
        public void MergeZeroBitWidthDoesNotAffectResult()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Double, TotalRows = 10, BitWidth = 0 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Double, TotalRows = 5, BitWidth = 0 };

            a.Merge(b);

            Assert.Equal(0, a.BitWidth);
            Assert.Equal(15, a.TotalRows);
        }

        [Fact]
        public void CloneCopiesAllFields()
        {
            var original = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.String,
                TotalRows = 42,
                TotalVariableBytes = 500,
                BitWidth = 0,
            };

            var clone = original.Clone();

            Assert.Equal(original.DataType, clone.DataType);
            Assert.Equal(original.TotalRows, clone.TotalRows);
            Assert.Equal(original.TotalVariableBytes, clone.TotalVariableBytes);
        }

        [Fact]
        public void CloneDeepCopiesChildren()
        {
            var original = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 10,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 30, TotalVariableBytes = 300 }
                }
            };

            var clone = original.Clone();

            original.Children[0].TotalRows = 999;

            Assert.Single(clone.Children!);
            Assert.Equal(30, clone.Children[0].TotalRows);
        }

        [Fact]
        public void ClonePreservesBitWidth()
        {
            var original = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Int64,
                TotalRows = 10,
                BitWidth = 32
            };

            var clone = original.Clone();

            Assert.Equal(32, clone.BitWidth);
        }

        [Fact]
        public void ClonePreservesStructHeader()
        {
            var header = StructHeader.Create("col1", "col2");
            var original = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Struct,
                StructHeader = header,
                TotalRows = 5
            };

            var clone = original.Clone();

            Assert.Equal(header, clone.StructHeader);
        }

        [Fact]
        public void CloneWithNullChildren()
        {
            var original = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Int64,
                TotalRows = 10,
                Children = null
            };

            var clone = original.Clone();

            Assert.Null(clone.Children);
        }

        [Fact]
        public void GetSizeInfoFromNullOnlyColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Null, info.DataType);
            Assert.Equal(2, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromInt64Column()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Int64, info.DataType);
            Assert.Equal(3, info.TotalRows);
            Assert.True(info.BitWidth > 0);
        }

        [Fact]
        public void GetSizeInfoFromStringColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("hello"));
            column.Add(new StringValue("world"));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.String, info.DataType);
            Assert.Equal(2, info.TotalRows);
            Assert.True(info.TotalVariableBytes > 0);
        }

        [Fact]
        public void GetSizeInfoFromBoolColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(false));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Boolean, info.DataType);
            Assert.Equal(2, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromDoubleColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(1.5));
            column.Add(new DoubleValue(2.5));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Double, info.DataType);
            Assert.Equal(2, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromDecimalColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DecimalValue(1.23m));
            column.Add(new DecimalValue(4.56m));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Decimal128, info.DataType);
            Assert.Equal(2, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromTimestampColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new TimestampTzValue(DateTime.UtcNow.Ticks, 0));
            column.Add(new TimestampTzValue(DateTime.UtcNow.Ticks + 1000, 0));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Timestamp, info.DataType);
            Assert.Equal(2, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromBinaryColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BinaryValue(new byte[] { 1, 2, 3 }));
            column.Add(new BinaryValue(new byte[] { 4, 5, 6, 7 }));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Binary, info.DataType);
            Assert.Equal(2, info.TotalRows);
            Assert.True(info.TotalVariableBytes > 0);
        }

        [Fact]
        public void GetSizeInfoFromListColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));
            column.Add(new ListValue(new IDataValue[] { new Int64Value(3) }));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.List, info.DataType);
            Assert.Equal(2, info.TotalRows);
            Assert.NotNull(info.Children);
            Assert.Single(info.Children!);
            Assert.Equal(3, info.Children[0].TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromMapColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k1"), new Int64Value(1))
                }
            ));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Map, info.DataType);
            Assert.Equal(1, info.TotalRows);
            Assert.NotNull(info.Children);
            Assert.Equal(2, info.Children!.Count);
        }

        [Fact]
        public void GetSizeInfoFromStructColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            var header = StructHeader.Create("name", "age");
            column.Add(new StructValue(header, new IDataValue[] { new StringValue("Alice"), new Int64Value(30) }));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Struct, info.DataType);
            Assert.Equal(1, info.TotalRows);
            Assert.NotNull(info.StructHeader);
            Assert.Equal(header, info.StructHeader!.Value);
            Assert.NotNull(info.Children);
            Assert.Equal(2, info.Children!.Count);
        }

        [Fact]
        public void GetSizeInfoFromUnionColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Union, info.DataType);
            Assert.Equal(2, info.TotalRows);
            Assert.NotNull(info.Children);
            Assert.True(info.Children!.Count >= 2);
        }

        [Fact]
        public void GetSizeInfoFromEmptyColumn()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Null, info.DataType);
            Assert.Equal(0, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromIntWithNullsMatchesCount()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(3));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Int64, info.DataType);
            Assert.Equal(3, info.TotalRows);
        }

        [Fact]
        public void GetSizeInfoFromAlwaysNullColumn()
        {
            var info = AlwaysNullColumn.Instance.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Null, info.DataType);
        }

        [Fact]
        public void CreateFromSizeInfoInt64()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 100, BitWidth = 64 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(42));
            column.Add(new Int64Value(99));

            Assert.Equal(2, column.Count);
            Assert.Equal(42, column.GetValueAt(0, default).AsLong);
            Assert.Equal(99, column.GetValueAt(1, default).AsLong);
        }

        [Fact]
        public void CreateFromSizeInfoString()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 100, TotalVariableBytes = 5000 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new StringValue("hello"));
            column.Add(new StringValue("world"));

            Assert.Equal(2, column.Count);
            Assert.Equal("hello", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal("world", column.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void CreateFromSizeInfoBool()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Boolean, TotalRows = 50 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new BoolValue(true));
            column.Add(new BoolValue(false));

            Assert.Equal(2, column.Count);
            Assert.True(column.GetValueAt(0, default).AsBool);
            Assert.False(column.GetValueAt(1, default).AsBool);
        }

        [Fact]
        public void CreateFromSizeInfoDouble()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Double, TotalRows = 50 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new DoubleValue(3.14));

            Assert.Single(column);
            Assert.Equal(3.14, column.GetValueAt(0, default).AsDouble);
        }

        [Fact]
        public void CreateFromSizeInfoDecimal()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Decimal128, TotalRows = 50 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new DecimalValue(1.23m));

            Assert.Single(column);
            Assert.Equal(1.23m, column.GetValueAt(0, default).AsDecimal);
        }

        [Fact]
        public void CreateFromSizeInfoTimestamp()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Timestamp, TotalRows = 50 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            var ts = new TimestampTzValue(123456789L, 60);
            column.Add(ts);

            Assert.Single(column);
            Assert.Equal(123456789L, column.GetValueAt(0, default).AsTimestamp.ticks);
        }

        [Fact]
        public void CreateFromSizeInfoBinary()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Binary, TotalRows = 50, TotalVariableBytes = 1000 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new BinaryValue(new byte[] { 1, 2, 3 }));

            Assert.Single(column);
        }

        [Fact]
        public void CreateFromSizeInfoNull()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Null, TotalRows = 0 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(NullValue.Instance);
            Assert.Single(column);
        }

        [Fact]
        public void CreateFromSizeInfoList()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 50,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 200, BitWidth = 64 }
                }
            };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));

            Assert.Single(column);
        }

        [Fact]
        public void CreateFromSizeInfoMap()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Map,
                TotalRows = 50,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 100, TotalVariableBytes = 2000 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 100, BitWidth = 64 },
                }
            };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("key"), new Int64Value(42))
                }
            ));

            Assert.Single(column);
        }

        [Fact]
        public void CreateFromSizeInfoStruct()
        {
            var header = StructHeader.Create("name", "age");
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Struct,
                StructHeader = header,
                TotalRows = 50,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 50, TotalVariableBytes = 1000 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 50, BitWidth = 8 },
                }
            };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new StructValue(header, new IDataValue[] { new StringValue("Bob"), new Int64Value(25) }));

            Assert.Single(column);
        }

        [Fact]
        public void CreateFromSizeInfoStructThrowsWithoutHeader()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Struct,
                TotalRows = 50,
            };

            Assert.Throws<ArgumentException>(() => new Column(GlobalMemoryManager.Instance, sizeInfo));
        }

        [Fact]
        public void CreateFromSizeInfoUnion()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Union,
                TotalRows = 100,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.Null, TotalRows = 0 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 50, BitWidth = 32 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 50, TotalVariableBytes = 1000 },
                }
            };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            Assert.Equal(2, column.Count);
        }

        [Fact]
        public void CreateFromSizeInfoUnionThrowsWithoutChildren()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Union,
                TotalRows = 100,
            };

            Assert.Throws<ArgumentException>(() => new Column(GlobalMemoryManager.Instance, sizeInfo));
        }

        [Fact]
        public void CreateFromSizeInfoListThrowsWithoutChildren()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 50,
            };

            Assert.Throws<ArgumentException>(() => new Column(GlobalMemoryManager.Instance, sizeInfo));
        }

        [Fact]
        public void CreateFromSizeInfoMapThrowsWithWrongChildCount()
        {
            var sizeInfo = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Map,
                TotalRows = 50,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 50 }
                }
            };

            Assert.Throws<ArgumentException>(() => new Column(GlobalMemoryManager.Instance, sizeInfo));
        }

        [Fact]
        public void CreateFromSizeInfoInt8()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 100, BitWidth = 8 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(5));
            Assert.Equal(1, column.Count);
            Assert.Equal(5, column.GetValueAt(0, default).AsLong);
        }

        [Fact]
        public void CreateFromSizeInfoInt16()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 100, BitWidth = 16 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(1000));
            Assert.Single(column);
            Assert.Equal(1000, column.GetValueAt(0, default).AsLong);
        }

        [Fact]
        public void CreateFromSizeInfoInt32()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 100, BitWidth = 32 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(100_000));
            Assert.Single(column);
            Assert.Equal(100_000, column.GetValueAt(0, default).AsLong);
        }

        [Fact]
        public void RoundTripInt64Column()
        {
            using var original = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                original.Add(new Int64Value(i));
            }

            var sizeInfo = original.GetColumnSizeInfo();

            using var preallocated = new Column(GlobalMemoryManager.Instance, sizeInfo);
            for (int i = 0; i < 100; i++)
            {
                preallocated.Add(new Int64Value(i));
            }

            Assert.Equal(original.Count, preallocated.Count);
            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(original.GetValueAt(i, default).AsLong, preallocated.GetValueAt(i, default).AsLong);
            }
        }

        [Fact]
        public void RoundTripStringColumn()
        {
            using var original = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 50; i++)
            {
                original.Add(new StringValue($"value_{i}"));
            }

            var sizeInfo = original.GetColumnSizeInfo();

            using var preallocated = new Column(GlobalMemoryManager.Instance, sizeInfo);
            for (int i = 0; i < 50; i++)
            {
                preallocated.Add(new StringValue($"value_{i}"));
            }

            Assert.Equal(original.Count, preallocated.Count);
            for (int i = 0; i < 50; i++)
            {
                Assert.Equal(
                    original.GetValueAt(i, default).AsString.ToString(),
                    preallocated.GetValueAt(i, default).AsString.ToString());
            }
        }

        [Fact]
        public void RoundTripListColumn()
        {
            using var original = new Column(GlobalMemoryManager.Instance);
            original.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));
            original.Add(new ListValue(new IDataValue[] { new Int64Value(3) }));

            var sizeInfo = original.GetColumnSizeInfo();

            using var preallocated = new Column(GlobalMemoryManager.Instance, sizeInfo);
            preallocated.Add(new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }));
            preallocated.Add(new ListValue(new IDataValue[] { new Int64Value(3) }));

            Assert.Equal(2, preallocated.Count);
        }

        [Fact]
        public void RoundTripMapColumn()
        {
            using var original = new Column(GlobalMemoryManager.Instance);
            original.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k1"), new Int64Value(1)),
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k2"), new Int64Value(2)),
                }
            ));

            var sizeInfo = original.GetColumnSizeInfo();

            using var preallocated = new Column(GlobalMemoryManager.Instance, sizeInfo);
            preallocated.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k1"), new Int64Value(1)),
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("k2"), new Int64Value(2)),
                }
            ));

            Assert.Single(preallocated);
        }

        [Fact]
        public void RoundTripStructColumn()
        {
            var header = StructHeader.Create("x", "y");
            using var original = new Column(GlobalMemoryManager.Instance);
            original.Add(new StructValue(header, new IDataValue[] { new Int64Value(1), new StringValue("a") }));
            original.Add(new StructValue(header, new IDataValue[] { new Int64Value(2), new StringValue("b") }));

            var sizeInfo = original.GetColumnSizeInfo();

            using var preallocated = new Column(GlobalMemoryManager.Instance, sizeInfo);
            preallocated.Add(new StructValue(header, new IDataValue[] { new Int64Value(1), new StringValue("a") }));
            preallocated.Add(new StructValue(header, new IDataValue[] { new Int64Value(2), new StringValue("b") }));

            Assert.Equal(2, preallocated.Count);
        }

        [Fact]
        public void MergeSizeInfosThenCreateColumn()
        {
            using var batch1 = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 50; i++)
            {
                batch1.Add(new StringValue($"batch1_{i}"));
            }

            using var batch2 = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 30; i++)
            {
                batch2.Add(new StringValue($"batch2_{i}"));
            }

            var info1 = batch1.GetColumnSizeInfo();
            var info2 = batch2.GetColumnSizeInfo();

            info1.Merge(info2);

            Assert.Equal(80, info1.TotalRows);
            Assert.True(info1.TotalVariableBytes > 0);

            using var merged = new Column(GlobalMemoryManager.Instance, info1);

            for (int i = 0; i < 50; i++)
            {
                merged.Add(new StringValue($"batch1_{i}"));
            }
            for (int i = 0; i < 30; i++)
            {
                merged.Add(new StringValue($"batch2_{i}"));
            }

            Assert.Equal(80, merged.Count);
        }

        [Fact]
        public void MergeSizeInfosDifferentTypesThenCreateColumn()
        {
            using var batch1 = new Column(GlobalMemoryManager.Instance);
            batch1.Add(new Int64Value(1));
            batch1.Add(new Int64Value(2));

            using var batch2 = new Column(GlobalMemoryManager.Instance);
            batch2.Add(new StringValue("hello"));

            var info1 = batch1.GetColumnSizeInfo();
            var info2 = batch2.GetColumnSizeInfo();

            info1.Merge(info2);

            Assert.Equal(ArrowTypeId.Union, info1.DataType);
            Assert.Equal(3, info1.TotalRows);

            using var merged = new Column(GlobalMemoryManager.Instance, info1);
            merged.Add(new Int64Value(1));
            merged.Add(new StringValue("hello"));

            Assert.Equal(2, merged.Count);
        }

        [Fact]
        public void MergeMultipleBatchSizeInfosBitWidthGrows()
        {
            using var batch1 = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 10; i++)
            {
                batch1.Add(new Int64Value(i));
            }

            using var batch2 = new Column(GlobalMemoryManager.Instance);
            batch2.Add(new Int64Value(100_000));

            var info1 = batch1.GetColumnSizeInfo();
            var info2 = batch2.GetColumnSizeInfo();

            info1.Merge(info2);

            Assert.Equal(ArrowTypeId.Int64, info1.DataType);
            Assert.Equal(11, info1.TotalRows);
            Assert.True(info1.BitWidth >= info2.BitWidth);
        }

        [Fact]
        public void CreateFromSizeInfoAddNullsToTypedColumn()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 50, BitWidth = 8 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(3));

            Assert.Equal(3, column.Count);
            Assert.Equal(1, column.GetValueAt(0, default).AsLong);
            Assert.Equal(ArrowTypeId.Null, column.GetValueAt(1, default).Type);
            Assert.Equal(3, column.GetValueAt(2, default).AsLong);
        }

        [Fact]
        public void CreateFromSizeInfoTypePromotionToUnion()
        {
            var sizeInfo = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 50, BitWidth = 8 };
            using var column = new Column(GlobalMemoryManager.Instance, sizeInfo);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            Assert.Equal(2, column.Count);
            Assert.Equal(1, column.GetValueAt(0, default).AsLong);
            Assert.Equal("hello", column.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void MergeListWithDifferentChildTypes()
        {
            var a = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 10,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 30, TotalVariableBytes = 500 }
                }
            };
            var b = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.List,
                TotalRows = 5,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 20, BitWidth = 32 }
                }
            };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.List, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Single(a.Children!);
            Assert.Equal(ArrowTypeId.Union, a.Children[0].DataType);
            Assert.Equal(50, a.Children[0].TotalRows);
        }

        [Fact]
        public void MergePromotesToUnionPreservesNestedChildren()
        {
            var header = StructHeader.Create("x", "y");
            var a = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Struct,
                StructHeader = header,
                TotalRows = 10,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 8 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10, TotalVariableBytes = 200 },
                }
            };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Double, TotalRows = 5 };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Equal(2, a.Children!.Count);

            var structChild = a.Children.First(c => c.DataType == ArrowTypeId.Struct);
            Assert.Equal(header, structChild.StructHeader);
            Assert.Equal(2, structChild.Children!.Count);
            Assert.Equal(ArrowTypeId.Int64, structChild.Children[0].DataType);
            Assert.Equal(ArrowTypeId.String, structChild.Children[1].DataType);
        }

        [Fact]
        public void MergeChainOfThreeBatches()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 10, TotalVariableBytes = 100 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 20, TotalVariableBytes = 200 };
            var c = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 30, TotalVariableBytes = 300 };

            a.Merge(b);
            a.Merge(c);

            Assert.Equal(ArrowTypeId.String, a.DataType);
            Assert.Equal(60, a.TotalRows);
            Assert.Equal(600, a.TotalVariableBytes);
        }

        [Fact]
        public void MergeChainWithTypeChange()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 8 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 20, BitWidth = 32 };
            var c = new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 5, TotalVariableBytes = 100 };
            var d = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 15, BitWidth = 16 };

            a.Merge(b);
            a.Merge(c);
            a.Merge(d);

            Assert.Equal(ArrowTypeId.Union, a.DataType);
            Assert.Equal(50, a.TotalRows);
            Assert.Equal(2, a.Children!.Count);

            var intChild = a.Children.First(x => x.DataType == ArrowTypeId.Int64);
            Assert.Equal(45, intChild.TotalRows);
            Assert.Equal(32, intChild.BitWidth);
        }

        [Fact]
        public void GetSizeInfoFromListOfStructs()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            var header = StructHeader.Create("name", "value");
            column.Add(new ListValue(new IDataValue[]
            {
                new StructValue(header, new IDataValue[] { new StringValue("a"), new Int64Value(1) }),
                new StructValue(header, new IDataValue[] { new StringValue("b"), new Int64Value(2) }),
            }));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.List, info.DataType);
            Assert.Equal(1, info.TotalRows);
            Assert.Single(info.Children!);
            Assert.Equal(ArrowTypeId.Struct, info.Children[0].DataType);
            Assert.Equal(header, info.Children[0].StructHeader);
            Assert.Equal(2, info.Children[0].Children!.Count);
        }

        [Fact]
        public void GetSizeInfoFromMapWithListValues()
        {
            using var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new MapValue(
                new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(
                        new StringValue("k1"),
                        new ListValue(new IDataValue[] { new Int64Value(1), new Int64Value(2) }))
                }
            ));

            var info = column.GetColumnSizeInfo();

            Assert.Equal(ArrowTypeId.Map, info.DataType);
            Assert.Equal(2, info.Children!.Count);
            Assert.Equal(ArrowTypeId.String, info.Children[0].DataType);
            Assert.Equal(ArrowTypeId.List, info.Children[1].DataType);
            Assert.Single(info.Children[1].Children!);
        }

        [Fact]
        public void RoundTripUnionColumn()
        {
            using var original = new Column(GlobalMemoryManager.Instance);
            original.Add(new Int64Value(1));
            original.Add(new StringValue("hello"));
            original.Add(NullValue.Instance);
            original.Add(new Int64Value(2));

            var sizeInfo = original.GetColumnSizeInfo();

            using var preallocated = new Column(GlobalMemoryManager.Instance, sizeInfo);
            preallocated.Add(new Int64Value(10));
            preallocated.Add(new StringValue("world"));

            Assert.Equal(2, preallocated.Count);
            Assert.Equal(10, preallocated.GetValueAt(0, default).AsLong);
            Assert.Equal("world", preallocated.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void MergeMapWithSameChildTypes()
        {
            var a = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Map,
                TotalRows = 10,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 30, TotalVariableBytes = 500 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 30, BitWidth = 8 },
                }
            };
            var b = new ColumnSizeInfo
            {
                DataType = ArrowTypeId.Map,
                TotalRows = 5,
                Children = new List<ColumnSizeInfo>
                {
                    new ColumnSizeInfo { DataType = ArrowTypeId.String, TotalRows = 15, TotalVariableBytes = 200 },
                    new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 15, BitWidth = 32 },
                }
            };

            a.Merge(b);

            Assert.Equal(ArrowTypeId.Map, a.DataType);
            Assert.Equal(15, a.TotalRows);
            Assert.Equal(2, a.Children!.Count);
            Assert.Equal(45, a.Children[0].TotalRows);
            Assert.Equal(700, a.Children[0].TotalVariableBytes);
            Assert.Equal(45, a.Children[1].TotalRows);
            Assert.Equal(32, a.Children[1].BitWidth);
        }

        [Fact]
        public void MergeBitWidthNotAppliedWhenOneSideIsZero()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 32 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5, BitWidth = 0 };

            a.Merge(b);

            Assert.Equal(32, a.BitWidth);
        }

        [Fact]
        public void MergeBitWidthNotAppliedWhenBothZero()
        {
            var a = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 10, BitWidth = 0 };
            var b = new ColumnSizeInfo { DataType = ArrowTypeId.Int64, TotalRows = 5, BitWidth = 0 };

            a.Merge(b);

            Assert.Equal(0, a.BitWidth);
        }
    }
}
