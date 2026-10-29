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
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using ParquetSharp;
using ParquetSharp.Schema;
using Stowage;
using System.Globalization;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Rewrites the sink's file as another engine would, committed like OPTIMIZE
    internal static class ForeignRewrite
    {
        public const int UnixEpochJulianDay = 2_440_588;

        // The table's only file, read as stored
        public static async Task<RecordBatch> ReadOnlyFile(IFileStorage storage, string table)
        {
            var head = await DeltaTransactionReader.ReadTable(storage, table);
            var add = Assert.Single(head!.AddFiles);
            var bytes = await DeltaTestKit.ReadBytes(storage, $"/{table}/{add.Path}");
            using var reader = new ParquetSharp.Arrow.FileReader(new MemoryStream(bytes));
            using var batches = reader.GetRecordBatchReader();
            var batch = await batches.ReadNextRecordBatchAsync();
            Assert.Null(await batches.ReadNextRecordBatchAsync());
            return batch;
        }

        // The batch disposes the columns it is given
        public static byte[] Write(Apache.Arrow.Schema schema, IArrowArray[] columns)
        {
            using var batch = new RecordBatch(schema, columns, columns[0].Length);
            using var stream = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            return stream.ToArray();
        }

        // An INT64 key and an INT96 timestamp per row, in row groups of the given size
        public static byte[] WriteInt96(string keyName, string timestampName, long[] keys, Int96[] timestamps, int rowGroupRows)
        {
            using var key = new PrimitiveNode(keyName, Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64);
            using var timestamp = new PrimitiveNode(timestampName, Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] { key, timestamp });
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x);
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                for (int start = 0; start < keys.Length; start += rowGroupRows)
                {
                    var count = Math.Min(rowGroupRows, keys.Length - start);
                    var definitions = Enumerable.Repeat((short)1, count).ToArray();
                    using var rowGroup = writer.AppendRowGroup();
                    using (var column = rowGroup.NextColumn())
                    {
                        ((ColumnWriter<long>)column).WriteBatch(count, definitions, null, keys.AsSpan(start, count).ToArray());
                    }
                    using (var column = rowGroup.NextColumn())
                    {
                        ((ColumnWriter<Int96>)column).WriteBatch(count, definitions, null, timestamps.AsSpan(start, count).ToArray());
                    }
                }
                writer.Close();
            }
            return stream.ToArray();
        }

        // Milliseconds since 1970 plus nanoseconds, as Spark writes INT96
        public static Int96 Int96(long millis, long extraNanos)
        {
            var days = Math.DivRem(millis, 86_400_000L, out var ofDay);
            if (ofDay < 0)
            {
                days--;
                ofDay += 86_400_000L;
            }
            var nanos = ofDay * 1_000_000 + extraNanos;
            return new Int96((int)nanos, (int)(nanos >> 32), (int)(UnixEpochJulianDay + days));
        }

        // Replaces every live file, without a data change
        public static async Task Commit(IFileStorage storage, string table, byte[] file)
        {
            var head = await DeltaTransactionReader.ReadTable(storage, table);
            var path = $"part-foreign-{ScratchOwners.ShortId()}.parquet";
            using (var write = await storage.OpenWrite($"/{table}/{path}"))
            {
                await write.WriteAsync(file);
            }
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "OPTIMIZE" } } }
            };
            foreach (var add in head!.AddFiles)
            {
                actions.Add(new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = add.Path, DeletionVector = add.DeletionVector, DataChange = false, DeletionTimestamp = 1, Size = add.Size } });
            }
            actions.Add(new DeltaAction() { Add = new DeltaAddAction() { Path = path, Size = file.Length, DataChange = false, ModificationTime = 1, PartitionValues = new Dictionary<string, string>() } });
            await DeltaTransactionWriter.WriteCommit(storage, table, head.Version + 1, actions);
        }

        // Each live row at the head, bound to the table's columns
        public static async Task<List<(string Path, long Position, string Row)>> LiveRows(IFileStorage storage, string table)
        {
            var head = await DeltaTransactionReader.ReadTable(storage, table);
            var rows = new List<(string, long, string)>();
            foreach (var add in head!.AddFiles)
            {
                IDeleteVector deleted = add.DeletionVector == null ? EmptyDeleteVector.Instance : await DeletionVectorReader.ReadDeletionVector(storage, table, add.DeletionVector);
                using var file = await ParquetSharpReader.OpenDataFile(storage, table, add.Path!, forDeletes: true);
                var binding = FileBinding.Create(head.Schema, file, table, head.ColumnMappingById);
                long position = 0;
                await foreach (var batch in file.ReadBatches(binding.LeafColumns))
                {
                    using (batch)
                    using (var rebound = binding.Rebind(batch, Apache.Arrow.Memory.MemoryAllocator.Default.Value))
                    {
                        for (int i = 0; i < rebound.Length; i++, position++)
                        {
                            if (!deleted.Contains(position))
                            {
                                rows.Add((add.Path!, position, string.Join(" ", Enumerable.Range(0, head.Schema.Fields.Count).Select(c => Describe(rebound.Column(c), i)))));
                            }
                        }
                    }
                }
            }
            return rows;
        }

        // Nested names as the table's logical names and no time zone, as files before physical nested names
        public static IArrowArray Logical(IArrowArray array, Delta.StructType table, int column)
        {
            return ArrowArrayFactory.BuildArray(Logical(array.Data, table.Fields[column].Type));
        }

        private static ArrayData Logical(ArrayData data, FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type)
        {
            switch (type)
            {
                case Delta.StructType structType:
                    var children = data.Children.Select((x, i) => Logical(x, structType.Fields[i].Type)).ToArray();
                    var fields = structType.Fields.Select((x, i) => new Field(x.Name, children[i].DataType, true)).ToList();
                    return new ArrayData(new StructType(fields), data.Length, data.NullCount, data.Offset, data.Buffers, children);
                case Delta.ArrayType listType:
                    var element = Logical(data.Children[0], listType.ElementType!);
                    return new ArrayData(new ListType(new Field("element", element.DataType, true)), data.Length, data.NullCount, data.Offset, data.Buffers, new[] { element });
                case Delta.MapType mapType:
                    var entries = data.Children[0];
                    var key = Logical(entries.Children[0], mapType.KeyType);
                    var value = Logical(entries.Children[1], mapType.ValueType);
                    var map = new MapType(new Field("key", key.DataType, false), new Field("value", value.DataType, true));
                    var entryData = new ArrayData(map.KeyValueType, entries.Length, entries.NullCount, entries.Offset, entries.Buffers, new[] { key, value });
                    return new ArrayData(map, data.Length, data.NullCount, data.Offset, data.Buffers, new[] { entryData });
                case Delta.TimestampType:
                    return new ArrayData(new TimestampType(((TimestampType)data.DataType).Unit, (string?)null), data.Length, data.NullCount, data.Offset, data.Buffers, data.Children);
                default:
                    return data;
            }
        }

        // Every map's entries in reverse order, in new buffers
        public static MapArray Reversed(MapArray maps)
        {
            var order = new List<int>();
            for (int i = 0; i < maps.Length; i++)
            {
                order.AddRange(Enumerable.Range(maps.ValueOffsets[i], maps.GetValueLength(i)).Reverse());
            }
            var entries = maps.KeyValues;
            var children = entries.Fields.Select(x => ArrowArrayConcatenator.Concatenate(order.Select(i => ArrowArrayFactory.Slice(x, i, 1)).ToList())).ToArray();
            var offsets = new ArrowBuffer.Builder<int>();
            for (int i = 0; i <= maps.Length; i++)
            {
                offsets.Append(maps.ValueOffsets[i] - maps.ValueOffsets[0]);
            }
            var validity = new ArrowBuffer.BitmapBuilder();
            for (int i = 0; i < maps.Length; i++)
            {
                validity.Append(maps.IsValid(i));
            }
            return new MapArray(maps.Data.DataType, maps.Length, offsets.Build(), new StructArray(entries.Data.DataType, order.Count, children, ArrowBuffer.Empty, 0), validity.Build(), maps.NullCount, 0);
        }

        // A copy with each millisecond leaf in microseconds
        public static IArrowArray Micros(IArrowArray array)
        {
            return ArrowArrayFactory.BuildArray(Micros(array.Data));
        }

        private static ArrayData Micros(ArrayData data)
        {
            var buffers = data.Buffers.Select(x => new ArrowBuffer(x.Span.ToArray())).ToArray();
            var children = data.Children?.Select(Micros).ToArray();
            var type = data.DataType;
            switch (data.DataType)
            {
                case TimestampType timestamp:
                    var values = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span).ToArray().Select(x => x * 1_000).ToArray();
                    buffers[1] = new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray());
                    type = new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, timestamp.Timezone);
                    break;
                case StructType structType:
                    type = new StructType(structType.Fields.Select((x, i) => new Field(x.Name, children![i].DataType, x.IsNullable, x.Metadata)).ToList());
                    break;
                case MapType:
                    var entries = (StructType)children![0].DataType;
                    type = new MapType(entries.Fields[0], entries.Fields[1]);
                    break;
                case ListType list:
                    type = new ListType(new Field(list.ValueField.Name, children![0].DataType, list.ValueField.IsNullable, list.ValueField.Metadata));
                    break;
            }
            return new ArrayData(type, data.Length, data.NullCount, data.Offset, buffers, children);
        }

        private static string Describe(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return "null";
            }
            switch (array)
            {
                case Int8Array bytes:
                    return bytes.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture);
                case Int16Array shorts:
                    return shorts.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture);
                case Int32Array ints:
                    return ints.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture);
                case Decimal128Array decimals:
                    return decimals.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture);
                case Int64Array longs:
                    return longs.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture);
                case StringArray strings:
                    return strings.GetString(index);
                case TimestampArray timestamps:
                    return $"{timestamps.GetValue(index)}{((TimestampType)timestamps.Data.DataType).Unit}";
                case StructArray structs:
                    return "{" + string.Join(",", structs.Fields.Select(x => Describe(x, index))) + "}";
                case MapArray maps:
                    var offsets = maps.ValueOffsets;
                    return "{" + string.Join(",", Enumerable.Range(offsets[index], offsets[index + 1] - offsets[index]).Select(x => $"{Describe(maps.Keys, x)}:{Describe(maps.Values, x)}")) + "}";
                case ListArray lists:
                    var starts = lists.ValueOffsets;
                    return "[" + string.Join(",", Enumerable.Range(starts[index], starts[index + 1] - starts[index]).Select(x => Describe(lists.Values, x))) + "]";
                default:
                    throw new NotSupportedException($"No description for {array.Data.DataType.Name}");
            }
        }
    }
}
