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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using DataValueComparer = FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ColumnOrderTests
    {
        [Fact]
        public void TestSearchOtherTypeAfterNulls()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                new StringValue("a")
            };

            Assert.Equal((~1, ~1), column.SearchBoundries(new Int64Value(5), 0, 1, default));
            Assert.True(column.CompareTo(0, new Int64Value(5), default) < 0);
        }

        [Fact]
        public async Task TestTreeOrderWithMixedTypesAndNulls()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new StringValue("b"),
                NullValue.Instance,
                new Int64Value(5),
                new StringValue("a"),
                new Int64Value(3)
            };

            var actual = await InsertAndRead(column, value => value.ToString()!);

            Assert.Equal(["null", "3", "5", "a", "b"], actual);
        }

        [Fact]
        public async Task TestTreeOrderWithMixedTypesAndNullsInStruct()
        {
            var header = StructHeader.Create("x");
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new StructValue(header, new StringValue("b")),
                new StructValue(header, NullValue.Instance),
                new StructValue(header, new Int64Value(5)),
                new StructValue(header, new StringValue("a")),
                new StructValue(header, new Int64Value(3))
            };

            var actual = await InsertAndRead(column, value => value.AsStruct.GetAt(0).ToString()!);

            Assert.Equal(["null", "3", "5", "a", "b"], actual);
        }

        private static async Task<List<string>> InsertAndRead(Column column, Func<IDataValue, string> read)
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter($"storage"), "storage", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<ColumnRowReference, string, ColumnKeyStorageContainer, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<ColumnRowReference, string, ColumnKeyStorageContainer, ListValueContainer<string>>()
                {
                    BucketSize = 4,
                    Comparer = new ColumnComparer(1),
                    KeySerializer = new ColumnStoreSerializer(1, GlobalMemoryManager.Instance),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            var batch = new EventBatchData([column]);
            for (int i = 0; i < column.Count; i++)
            {
                await tree.Upsert(new ColumnRowReference() { referenceBatch = batch, RowIndex = i }, i.ToString());
            }

            var result = new List<string>();
            var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    result.Add(read(kv.Key.referenceBatch.Columns[0].GetValueAt(kv.Key.RowIndex, default)));
                }
            }
            return result;
        }

        [Fact]
        public void TestColumnWithOffsetNullCompare()
        {
            Column inner = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1)
            };
            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(0);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            var column = new ColumnWithOffset(inner, offsets);

            Assert.True(column.CompareTo(column, 1, 0) < 0);
            Assert.Equal(0, column.CompareTo(column, 1, 1));
            Assert.True(column.CompareTo(1, new Int64Value(1), default) < 0);
            Assert.Equal(0, column.CompareTo(1, NullValue.Instance, default));
        }

        [Fact]
        public void TestAlwaysNullColumnCompare()
        {
            IColumn nullColumn = AlwaysNullColumn.Instance;
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1)
            };

            Assert.Equal(ArrowTypeId.Null, nullColumn.Type);
            Assert.True(nullColumn.CompareTo(0, new Int64Value(1), default) < 0);
            Assert.Equal(0, nullColumn.CompareTo(0, NullValue.Instance, default));
            Assert.True(nullColumn.CompareTo(column, 0, 0) < 0);
            Assert.True(column.CompareTo(nullColumn, 0, 0) > 0);
        }

        [Fact]
        public void TestMapOrderMatchesDataValueComparer()
        {
            var first = new MapValue(
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(2)),
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("b"), new Int64Value(1)));
            var second = new MapValue(
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(1)),
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("c"), new Int64Value(0)));
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                first,
                second
            };

            Assert.True(DataValueComparer.CompareTo(first, second) < 0);
            Assert.True(column.CompareTo(column, 0, 1) < 0);
            Assert.True(column.CompareTo(0, column.GetValueAt(1, default), default) < 0);
            Assert.True(column.CompareTo(0, second, default) < 0);
        }

        [Fact]
        public void TestStructInUnionCompare()
        {
            var header = StructHeader.Create("x");
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(1),
                new StructValue(header, new Int64Value(10)),
                new StructValue(header, new Int64Value(20))
            };

            Assert.True(column.CompareTo(1, new StructValue(header, new Int64Value(20)), default) < 0);
            Assert.Equal((2, 2), column.SearchBoundries(new StructValue(header, new Int64Value(20)), 0, 2, default));
        }

        [Fact]
        public void TestUnionNullAgainstFilteredNull()
        {
            Column union = new Column(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                new Int64Value(1),
                new StringValue("a")
            };
            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            var filtered = new ColumnWithOffset(union, offsets);

            Assert.Equal(ArrowTypeId.Union, union.Type);
            Assert.Equal(0, union.CompareTo(filtered, 0, 0));
            Assert.Equal(0, filtered.CompareTo(union, 0, 0));
            Assert.True(union.CompareTo(filtered, 1, 0) > 0);
        }

        [Fact]
        public void TestFieldCompareWithNulls()
        {
            var header = StructHeader.Create("x");
            Column structs = new Column(GlobalMemoryManager.Instance)
            {
                NullValue.Instance,
                new StructValue(header, NullValue.Instance),
                new StructValue(header, new Int64Value(5))
            };
            var field = new StructReferenceSegment() { Field = 0 };
            Column maps = new Column(GlobalMemoryManager.Instance)
            {
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(1)))
            };

            Assert.Equal((0, 1), structs.SearchBoundries(NullValue.Instance, 0, 2, field));
            Assert.True(structs.CompareTo(1, new Int64Value(5), field) < 0);
            Assert.Equal(0, maps.CompareTo(0, NullValue.Instance, new MapKeyReferenceSegment() { Key = "b" }));
        }

        [Fact]
        public void TestFieldCompareInUnionAndList()
        {
            var header = StructHeader.Create("x");
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(0),
                new StructValue(header, new Int64Value(10)),
                new StructValue(header, new Int64Value(20))
            };
            var field = new StructReferenceSegment() { Field = 0 };
            Column lists = new Column(GlobalMemoryManager.Instance)
            {
                new ListValue(new Int64Value(1))
            };

            Assert.Equal(0, column.CompareTo(2, new Int64Value(20), field));
            Assert.Equal((2, 2), column.SearchBoundries(new Int64Value(20), 1, 2, field));
            Assert.Equal(0, column.CompareTo(0, NullValue.Instance, field));
            Assert.True(lists.CompareTo(0, new Int64Value(1), new MapKeyReferenceSegment() { Key = "x" }) < 0);
        }

        [Fact]
        public void TestFieldOnTypesWithoutFields()
        {
            Column ints = new Column(GlobalMemoryManager.Instance)
            {
                new Int64Value(10)
            };
            Column lists = new Column(GlobalMemoryManager.Instance)
            {
                new ListValue(new Int64Value(1))
            };
            var field = new MapKeyReferenceSegment() { Key = "x" };

            Assert.True(ints.CompareTo(0, new Int64Value(10), field) < 0);
            Assert.Equal((~1, ~1), ints.SearchBoundries(new Int64Value(10), 0, 0, field));
            Assert.Equal((0, 0), ints.SearchBoundries(NullValue.Instance, 0, 0, field));
            Assert.True(lists.CompareTo(0, new ListValue(new Int64Value(1)), field) < 0);
            Assert.Equal((~1, ~1), lists.SearchBoundries(new Int64Value(1), 0, 0, field));
        }
    }
}
