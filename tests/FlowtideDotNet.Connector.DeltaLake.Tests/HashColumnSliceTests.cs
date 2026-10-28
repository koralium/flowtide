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
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class HashColumnSliceTests
    {
        [Theory]
        [InlineData("byte")]
        [InlineData("short")]
        [InlineData("int")]
        [InlineData("long")]
        [InlineData("date")]
        [InlineData("float")]
        [InlineData("double")]
        [InlineData("decimal")]
        [InlineData("string")]
        [InlineData("binary")]
        [InlineData("second")]
        [InlineData("millisecond")]
        [InlineData("microsecond")]
        [InlineData("nanosecond")]
        public void SlicedColumnsHashLikeTheOriginalRows(string kind)
        {
            var (comparer, array) = Create(kind);
            using (array)
            using (var slice = ArrowArrayFactory.Slice(array, 7, 10))
            {
                Assert.NotEqual(0, slice.Offset);
                Assert.True(slice.NullCount > 0);
                var states = Enumerable.Range(0, slice.Length).Select(i => (ulong)i * 0x9E3779B97F4A7C15UL).ToArray();
                var expected = (ulong[])states.Clone();
                var rejected = Enumerable.Range(0, slice.Length).Select(i => i % 4 == 3).ToArray();

                comparer.HashColumn(slice, states, rejected);

                for (int row = 0; row < slice.Length; row++)
                {
                    if (row % 4 == 3)
                    {
                        Assert.True(rejected[row]);
                        continue;
                    }
                    var hashable = comparer.TryHash(array, 7 + row, ref expected[row]);
                    Assert.Equal(!hashable, rejected[row]);
                    if (hashable)
                    {
                        Assert.Equal(expected[row], states[row]);
                    }
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void MixedBatchHashesMatchRowHashesAfterTimestampRejections(bool collide)
        {
            const int length = 19;
            var milliseconds = Enumerable.Range(0, length).Select(i => (long)(i - 11)).ToArray();
            var timestamps = Primitive(new TimestampType(TimeUnit.Millisecond, "UTC"), milliseconds);
            var booleans = new BooleanArray.Builder();
            for (int i = 0; i < length; i++)
            {
                if (i % 4 == 0) booleans.AppendNull();
                else booleans.Append(i % 2 == 0);
            }
            var columns = new[]
            {
                timestamps,
                booleans.Build(),
                Create("int").Item2,
                Create("string").Item2
            };
            var schema = new Schema(columns.Select((column, i) => new Field($"c{i}", column.Data.DataType, true)), null);
            using var stored = new RecordBatch(schema, columns, length);
            var comparers = new IArrowComparer[] { new ArrowTimestampComparer(), new ArrowBoolComparer(), new ArrowInt32Comparer(), new ArrowStringComparer() };
            var batch = new DeleteBatch(stored, Enumerable.Repeat(1L, length).ToArray(), comparers, collide: collide);
            using var foreign = Primitive(new TimestampType(TimeUnit.Microsecond, "UTC"),
                milliseconds.Select((value, i) => value * 1_000 + (i % 5 == 2 ? 1 : 0)).ToArray());
            var fileColumns = new[] { foreign, columns[1], columns[2], columns[3] };
            var hashes = Enumerable.Repeat(ulong.MaxValue, length).ToArray();
            var rejected = Enumerable.Repeat(true, length).ToArray();

            batch.HashBatch(fileColumns, hashes, rejected);

            var rejectedCount = 0;
            for (int row = 0; row < length; row++)
            {
                var hashable = batch.TryHash(fileColumns, row, out var expected);
                Assert.Equal(!hashable, rejected[row]);
                if (hashable)
                {
                    Assert.Equal(expected, hashes[row]);
                    Assert.True(batch.Find(hashes[row], fileColumns, row) >= 0);
                }
                else rejectedCount++;
            }
            Assert.True(rejectedCount > 0);
            Assert.True(rejectedCount < length);
        }

        private static (IArrowComparer, IArrowArray) Create(string kind)
        {
            var values = Enumerable.Range(0, 19).Select(i => i - 11).ToArray();
            switch (kind)
            {
                case "byte": return (new ArrowInt8Comparer(), Primitive(Int8Type.Default, values.Select(i => (sbyte)i).ToArray()));
                case "short": return (new ArrowInt16Comparer(), Primitive(Int16Type.Default, values.Select(i => (short)i).ToArray()));
                case "int": return (new ArrowInt32Comparer(), Primitive(Int32Type.Default, values));
                case "long": return (new ArrowInt64Comparer(), Primitive(Int64Type.Default, values.Select(i => (long)i).ToArray()));
                case "date": return (new ArrowDateComparer(), Primitive(Date32Type.Default, values));
                case "float": return (new ArrowFloat32Comparer(), Primitive(FloatType.Default, values.Select(i => i == 0 ? -0f : i == 1 ? float.NaN : (float)i).ToArray()));
                case "double": return (new ArrowFloat64Comparer(), Primitive(DoubleType.Default, values.Select(i => i == 0 ? -0.0 : i == 1 ? double.NaN : (double)i).ToArray()));
                case "decimal":
                    var decimals = new Decimal128Array.Builder(new Decimal128Type(38, 0));
                    Span<byte> bytes = stackalloc byte[16];
                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i % 3 == 1) decimals.AppendNull();
                        else
                        {
                            BinaryPrimitives.WriteInt128LittleEndian(bytes, ((Int128)values[i] << 64) + values[i]);
                            decimals.Append(bytes);
                        }
                    }
                    return (new ArrowDecimalComparer(), decimals.Build());
                case "string":
                    var strings = new StringArray.Builder();
                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i % 3 == 1) strings.AppendNull();
                        else strings.Append(i % 4 == 0 ? "" : $"é-prefix-longer-than-sixteen-{values[i]}");
                    }
                    return (new ArrowStringComparer(), strings.Build());
                case "binary":
                    var binary = new BinaryArray.Builder();
                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i % 3 == 1) binary.AppendNull();
                        else binary.Append(Enumerable.Repeat((byte)i, i % 7).ToArray().AsSpan());
                    }
                    return (new ArrowBinaryComparer(), binary.Build());
                default:
                    var unit = kind switch
                    {
                        "second" => TimeUnit.Second,
                        "millisecond" => TimeUnit.Millisecond,
                        "microsecond" => TimeUnit.Microsecond,
                        _ => TimeUnit.Nanosecond
                    };
                    var scale = unit == TimeUnit.Microsecond ? 1_000 : unit == TimeUnit.Nanosecond ? 1_000_000 : 1;
                    var timestamps = values.Select((value, i) => (long)value * scale + (scale > 1 && i % 5 == 0 ? 1 : 0)).ToArray();
                    return (new ArrowTimestampComparer(), Primitive(new TimestampType(unit, "UTC"), timestamps));
            }
        }

        private static IArrowArray Primitive<T>(IArrowType type, T[] values) where T : unmanaged
        {
            var validity = new ArrowBuffer.BitmapBuilder();
            for (int i = 0; i < values.Length; i++) validity.Append(i % 3 != 1);
            return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, 6, 0,
                new[] { validity.Build(), new ArrowBuffer.Builder<T>().AppendRange(values).Build() }));
        }
    }
}
