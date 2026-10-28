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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeleteWriterNullSlotTests
    {
        [Theory]
        [InlineData(false, true)]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public void SuppliedStructNullSlotsCountAgainstTheDeleteByteCap(bool partial, bool supplied)
        {
            var children = Enumerable.Range(0, 100)
                .Select(i => new Delta.StructField($"v{i}", new Delta.LongType(), true, new Dictionary<string, object>())).ToArray();
            var schema = new Delta.StructType(new[]
            {
                new Delta.StructField("key", new Delta.LongType(), true, new Dictionary<string, object>()),
                new Delta.StructField("s", new Delta.StructType(children), true, new Dictionary<string, object>())
            });
            var key = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            var value = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            for (int row = 0; row < 10; row++)
            {
                key.Add(new Int64Value(row));
                if (partial)
                {
                    value.Add(new StructValue(StructHeader.Create("v0"), new Int64Value(1)));
                }
                else
                {
                    value.Add(NullValue.Instance);
                }
            }
            using var input = new EventBatchData(new IColumn[] { key, value });
            // As the sink makes its delete writer
            var writer = new ParquetSharpWriter(schema, supplied ? new List<string> { "key", "s" } : new List<string> { "key" }, countNulls: true);
            writer.NewBatch();
            var limits = new DeleteLimits { ByteCap = 4_000 };
            const int pruningColumns = 1;
            var rowBytes = 32 + 4 * pruningColumns + writer.NullBytesPerRow * limits.WrittenSlack;
            long estimate = 0;
            for (int row = 0; row < 10; row++)
            {
                writer.AddRow(new ColumnRowReference { referenceBatch = input, RowIndex = row });
                estimate = writer.WrittenBytes * limits.WrittenSlack + (long)writer.WrittenCount * rowBytes;
                if (estimate >= limits.ByteCap)
                {
                    break;
                }
            }
            using var stored = writer.GetRecordBatch();
            var structs = Assert.IsType<StructArray>(stored.Column(1));
            // Child payload excludes padding, validity and builders
            var childValueBytes = structs.Fields.Cast<Int64Array>().Sum(x => (long)x.Length * sizeof(long));
            Assert.True(childValueBytes <= limits.ByteCap,
                $"{writer.WrittenCount} rows leave {childValueBytes} stored child bytes above the {limits.ByteCap} cap; estimate {estimate}");
        }

        [Theory]
        [InlineData(false, true)]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public void BooleanNullSlotsCountAgainstTheDeleteByteCap(bool partial, bool supplied)
        {
            var children = Enumerable.Range(0, 400).Select(i => Field($"v{i}", new Delta.BooleanType())).ToArray();
            var schema = new Delta.StructType(new[] { Field("key", new Delta.LongType()), Field("s", new Delta.StructType(children)) });
            var key = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            var value = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                key.Add(new Int64Value(i));
                IDataValue rowValue = partial ? new StructValue(StructHeader.Create("v0"), new BoolValue(true)) : NullValue.Instance;
                value.Add(rowValue);
            }
            using var input = new EventBatchData(new IColumn[] { key, value });
            var writer = new ParquetSharpWriter(schema, supplied ? new List<string> { "key", "s" } : new List<string> { "key" }, countNulls: true);
            writer.NewBatch();
            var limits = new DeleteLimits { ByteCap = 4_000 };
            long estimate = 0;
            for (int i = 0; i < 100; i++)
            {
                writer.AddRow(new ColumnRowReference { referenceBatch = input, RowIndex = i });
                // As the sink counts a row, one pruning column
                estimate = writer.WrittenBytes * limits.WrittenSlack + (long)writer.WrittenCount * (36 + writer.NullBytesPerRow * limits.WrittenSlack);
                if (estimate >= limits.ByteCap)
                {
                    break;
                }
            }
            using var stored = writer.GetRecordBatch();
            var structs = Assert.IsType<StructArray>(stored.Column(1));
            // Count bitmap payload, excluding allocated padding and builders
            var childBytes = structs.Fields.Cast<BooleanArray>().Sum(x =>
                x.Data.Buffers.Sum(buffer => (long)Math.Min(buffer.Length, (x.Length + 7) / 8)));
            Assert.True(childBytes <= limits.ByteCap,
                $"{writer.WrittenCount} rows hold {childBytes} child bitmap bytes against cap {limits.ByteCap}; estimate {estimate}");
        }

        [Theory]
        [InlineData("list", false)]
        [InlineData("list", true)]
        [InlineData("map", false)]
        [InlineData("map", true)]
        [InlineData("struct", false)]
        [InlineData("struct", true)]
        public void NestedNullSlotsAreCountedOnlyForDeleteBatches(string container, bool partial)
        {
            var childType = new Delta.StructType(new[] { Field("a", new Delta.LongType()), Field("b", new Delta.LongType()) });
            IDataValue child = partial ? new StructValue(StructHeader.Create("a"), new Int64Value(7)) : NullValue.Instance;
            (SchemaBaseType, IDataValue, int, int) nested = container switch
            {
                "list" => (new Delta.ArrayType { ElementType = childType, ContainsNull = true }, new ListValue(child), 4, 4),
                "map" => (new Delta.MapType(new Delta.LongType(), childType, true), new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(3), child)), 12, 12),
                _ => (new Delta.StructType(new[] { Field("inner", childType), Field("omitted", new Delta.LongType()) }), new StructValue(StructHeader.Create("inner"), child), 0, 8)
            };
            var (type, value, ordinaryContainerBytes, countedContainerBytes) = nested;
            var schema = new Delta.StructType(new[] { Field("value", type), Field("unwritten", new Delta.LongType()) });
            var column = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            column.Add(value);
            using var input = new EventBatchData(new IColumn[] { column });
            var row = new ColumnRowReference { referenceBatch = input, RowIndex = 0 };

            foreach (var countNulls in new[] { false, true })
            {
                foreach (var cdc in new[] { false, true })
                {
                    var writer = new ParquetSharpWriter(schema, new List<string> { "value" }, isCdcWriter: cdc, countNulls: countNulls);
                    for (int batch = 0; batch < 2; batch++)
                    {
                        writer.NewBatch();
                        writer.AddRow(row, isDelete: cdc && batch == 1);
                        // The unwritten column is in the per-row term, not WrittenBytes
                        var expected = countNulls
                            ? countedContainerBytes + (partial ? 16 : 17)
                            : ordinaryContainerBytes + (partial ? 8 : 1);
                        Assert.Equal(expected + (cdc ? 10 : 0), writer.WrittenBytes);
                        Assert.Equal(countNulls ? 8 : 0, writer.NullBytesPerRow);
                        Assert.Equal(1, writer.WrittenCount);
                        using var stored = writer.GetRecordBatch();
                        var structs = container switch
                        {
                            "list" => Assert.IsType<StructArray>(Assert.IsType<ListArray>(stored.Column(0)).Values),
                            "map" => Assert.IsType<StructArray>(Assert.IsType<MapArray>(stored.Column(0)).Values),
                            _ => Assert.IsType<StructArray>(Assert.IsType<StructArray>(stored.Column(0)).Fields[0])
                        };
                        Assert.Equal(!partial, structs.IsNull(0));
                        Assert.Equal(partial ? 7L : null, Assert.IsType<Int64Array>(structs.Fields[0]).GetValue(0));
                        Assert.Null(Assert.IsType<Int64Array>(structs.Fields[1]).GetValue(0));
                        Assert.Null(Assert.IsType<Int64Array>(stored.Column(1)).GetValue(0));
                        if (cdc)
                        {
                            Assert.Equal(batch == 0 ? "insert" : "delete", Assert.IsType<StringArray>(stored.Column(2)).GetString(0));
                        }
                    }
                }
            }
        }

        private static Delta.StructField Field(string name, SchemaBaseType type) => new(name, type, true, new Dictionary<string, object>());
    }
}
