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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using System.Runtime.CompilerServices;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using SchemaBaseType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ComparerPropertyTests
    {
        // One hash seed for the whole test run, so a failure repeats
        [ModuleInitializer]
        internal static void FixTheHashSeed()
        {
            ArrowHash.SetSeedForTests(5);
        }

        public static TheoryData<string> Types => new TheoryData<string>()
        {
            "byte", "short", "int", "long", "float", "double", "decimal", "timestamp", "date", "bool", "string", "binary",
            "list", "struct", "map", "repeated map", "nested map", "struct keys", "list keys", "map keys", "repeated map keys", "permuted map"
        };

        [Theory]
        [MemberData(nameof(Types))]
        public void StoredRowsAgreeWithTheEngine(string type)
        {
            var (deltaType, values) = Generate(type, new Random(type.Sum(x => x)));
            using var stored = Store(deltaType, values);
            IArrowArray column = stored.Column(0);
            using var reversed = type == "permuted map" ? Reversed((MapArray)column) : null;
            using var permuted = reversed != null ? ArrowArrayConcatenator.Concatenate(new[] { column, reversed }) : null;
            if (permuted != null)
            {
                // Each map again with its entries reversed
                column = permuted;
                values = values.Concat(values).ToList();
            }
            var comparer = new ArrowComparerVisitor().Visit(deltaType);
            AssertColumnHash(comparer, column);
            AssertPrefilterKeeps(stored, comparer, column);

            for (int i = 0; i < values.Count; i++)
            {
                HashOf(comparer, column, i);
                Assert.True(comparer.IsEqual(i, column, i, column), $"row {i} is not equal to itself");
                Assert.Equal(0, comparer.CompareCanonical(column, i, column, i));
                for (int j = 0; j < values.Count; j++)
                {
                    var equal = comparer.IsEqual(i, column, j, column);
                    var order = comparer.CompareCanonical(column, i, column, j);
                    // Rows the temporary tree merges are one stored row
                    if (DataValueComparer.Instance.Compare(values[i], values[j]) == 0)
                    {
                        Assert.True(equal, $"engine-equal rows {i} and {j} differ");
                    }
                    if (equal)
                    {
                        Assert.Equal(HashOf(comparer, column, i), HashOf(comparer, column, j));
                    }
                    Assert.True((order == 0) == equal, $"rows {i} and {j}: order {order}, equal {equal}");
                    Assert.Equal(Math.Sign(order), -Math.Sign(comparer.CompareCanonical(column, j, column, i)));
                }
            }
            var signs = new int[values.Count, values.Count];
            for (int i = 0; i < values.Count; i++)
            {
                for (int j = 0; j < values.Count; j++)
                {
                    signs[i, j] = Math.Sign(comparer.CompareCanonical(column, i, column, j));
                }
            }
            for (int a = 0; a < values.Count; a++)
            {
                for (int b = 0; b < values.Count; b++)
                {
                    if (values[a].IsNull && !values[b].IsNull)
                    {
                        Assert.True(signs[a, b] < 0, $"null row {a} does not sort before row {b}");
                    }
                    for (int c = 0; c < values.Count; c++)
                    {
                        if (signs[a, b] <= 0 && signs[b, c] <= 0)
                        {
                            Assert.True(signs[a, c] <= 0, $"rows {a} <= {b} <= {c} but not {a} <= {c}");
                        }
                    }
                }
            }
        }

        [Fact]
        public void OtherUnitsOfTheSameInstantAreEqual()
        {
            var milliseconds = new long[] { 0, 1, -1, 1_700_000_000_123, -62_135_596_800_000, 253_402_300_799_999, 1_700_000_000_000 };
            using var stored = Store(new Delta.TimestampType(), milliseconds.Select(x => (IDataValue)new TimestampTzValue(DateTime.UnixEpoch.Ticks + x * TimeSpan.TicksPerMillisecond, 0)).ToList());
            var column = stored.Column(0);
            var comparer = new ArrowTimestampComparer();

            foreach (var unit in new[] { TimeUnit.Second, TimeUnit.Microsecond, TimeUnit.Nanosecond })
            {
                // Nanoseconds since 1970 reach only from 1677 to 2262
                bool Fits(long x) => unit == TimeUnit.Second ? x % 1_000 == 0 : Math.Abs(x) < long.MaxValue / (unit == TimeUnit.Microsecond ? 1_000 : 1_000_000);
                long In(long x) => !Fits(x) ? 0 : unit switch { TimeUnit.Second => x / 1_000, TimeUnit.Microsecond => x * 1_000, _ => x * 1_000_000 };
                using var foreign = Timestamps(unit, milliseconds.Select(In));
                using var inexact = Timestamps(unit, milliseconds.Select(x => In(x) + 1));
                AssertPrefilterKeeps(stored, comparer, foreign);
                AssertColumnHash(comparer, foreign);
                AssertColumnHash(comparer, inexact);
                if (unit == TimeUnit.Second)
                {
                    // Seconds past the millisecond range equal no stored row
                    using var outside = Timestamps(unit, new[] { long.MaxValue / 1_000 + 1, long.MinValue / 1_000 - 1 });
                    Assert.Null(Hash(comparer, outside, 0));
                    Assert.Null(Hash(comparer, outside, 1));
                    Assert.Equal(1, Math.Sign(comparer.CompareCanonical(outside, 0, column, 5)));
                    Assert.Equal(-1, Math.Sign(comparer.CompareCanonical(outside, 1, column, 4)));
                }
                for (int i = 0; i < milliseconds.Length; i++)
                {
                    if (!Fits(milliseconds[i]))
                    {
                        continue;
                    }
                    Assert.True(comparer.IsEqual(i, column, i, foreign), $"{milliseconds[i]} ms in {unit}");
                    Assert.Equal(HashOf(comparer, column, i), HashOf(comparer, foreign, i));
                    Assert.Equal(0, comparer.CompareCanonical(column, i, foreign, i));
                    if (unit != TimeUnit.Second)
                    {
                        // A value between two milliseconds equals no stored row
                        Assert.Null(Hash(comparer, inexact, i));
                        Assert.All(Enumerable.Range(0, milliseconds.Length), j => Assert.False(comparer.IsEqual(j, column, i, inexact)));
                    }
                }
                // The first day sorts before the epoch in every unit
                Assert.Equal(-1, Math.Sign(comparer.CompareCanonical(column, 4, foreign, 0)));
                // The last day sorts after it, a long product wraps
                Assert.Equal(1, Math.Sign(comparer.CompareCanonical(column, 5, foreign, 0)));
            }
        }

        [Fact]
        public async Task AnInt96CopyIsEqualToTheStoredRow()
        {
            // 0001-01-01, the epoch, and a millisecond into a later day
            var rows = new (int Day, long Nanos)[] { (1_721_426, 0), (2_440_588, 0), (2_460_000, 86_399_999_000_000) };
            using var stored = Store(new Delta.TimestampType(), rows.Select(x => (IDataValue)new TimestampTzValue(DateTime.UnixEpoch.Ticks + (x.Day - 2_440_588L) * TimeSpan.TicksPerDay + x.Nanos / 100, 0)).ToList());
            var file = TimestampPrecisionTests.WriteInt96(rows.Select(x => (ParquetSharp.Int96?)new ParquetSharp.Int96((int)x.Nanos, (int)(x.Nanos >> 32), x.Day)).ToArray());
            var comparer = new ArrowTimestampComparer();

            using var batch = Assert.Single(await ReadForDeletes(file));
            for (int i = 0; i < rows.Length; i++)
            {
                Assert.True(comparer.IsEqual(i, stored.Column(0), i, batch.Column(0)), $"row {i}");
                Assert.Equal(HashOf(comparer, stored.Column(0), i), HashOf(comparer, batch.Column(0), i));
            }
        }

        [Fact]
        public async Task ReorderedColumnsAndFieldsAreEqualAfterBinding()
        {
            var s = new Delta.StructType(new[] { Field("x", new Delta.LongType()), Field("y", new Delta.StringType()) });
            var table = new Delta.StructType(new[] { Field("a", new Delta.LongType()), Field("s", s) });
            var header = StructHeader.Create("x", "y");
            var rows = Enumerable.Range(0, 20).Select(i => new IDataValue[]
            {
                i % 7 == 0 ? NullValue.Instance : new Int64Value(i),
                i % 5 == 0 ? NullValue.Instance : new StructValue(header, new Int64Value(i * 10), i % 3 == 0 ? NullValue.Instance : new StringValue($"value {i}"))
            }).ToList();
            using var stored = StoreRows(table, rows);

            // Another engine wrote s(y, x) before a
            var foreignStruct = new StructType(new List<Field>() { new Field("y", StringType.Default, true), new Field("x", Int64Type.Default, true) });
            var schema = new Schema.Builder().Field(new Field("s", foreignStruct, true)).Field(new Field("a", Int64Type.Default, true)).Build();
            var validity = new ArrowBuffer.BitmapBuilder();
            var ys = new StringArray.Builder();
            var xs = new Int64Array.Builder();
            var keys = new Int64Array.Builder();
            for (int i = 0; i < rows.Count; i++)
            {
                validity.Append(i % 5 != 0);
                ys.Append(i % 3 == 0 ? null : $"value {i}");
                xs.Append(i * 10);
                keys.Append(i % 7 == 0 ? null : i);
            }
            var foreign = RewriteCopyTests.Write(schema, new StructArray(foreignStruct, rows.Count, new IArrowArray[] { ys.Build(), xs.Build() }, validity.Build(), rows.Count(x => x[1].IsNull)), keys.Build());

            var visitor = new ArrowComparerVisitor();
            var comparers = table.Fields.Select(x => visitor.Visit(x.Type)).ToArray();
            using var dataFile = await Open(foreign);
            var binding = FileBinding.Create(table, dataFile, "t");
            var read = 0;
            await foreach (var batch in dataFile.ReadBatches(binding.LeafColumns))
            {
                using (batch)
                using (var rebound = binding.Rebind(batch, Apache.Arrow.Memory.MemoryAllocator.Default.Value))
                {
                    read += rebound.Length;
                    for (int i = 0; i < rows.Count; i++)
                    {
                        for (int c = 0; c < comparers.Length; c++)
                        {
                            Assert.True(comparers[c].IsEqual(i, stored.Column(c), i, rebound.Column(c)), $"row {i} column {c}");
                            Assert.Equal(HashOf(comparers[c], stored.Column(c), i), HashOf(comparers[c], rebound.Column(c), i));
                        }
                    }
                }
            }
            Assert.Equal(rows.Count, read);
        }

        [Fact]
        public void PermutedUniqueKeyMapsAreEqual()
        {
            var keyTypes = new (SchemaBaseType Type, Func<int, IDataValue> Key)[]
            {
                (new Delta.StringType(), i => new StringValue($"key {i}")),
                (new Delta.StructType(new[] { Field("k", new Delta.LongType()) }), i => new StructValue(StructHeader.Create("k"), new Int64Value(i))),
                (new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true), i => new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k"), new Int64Value(i)))),
                (new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true), i => new MapValue(Enumerable.Range(0, 3).Select(j => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{j}"), new Int64Value(i * 10 + j))).ToArray()))
            };
            foreach (var (keyType, key) in keyTypes)
            {
                var type = new Delta.MapType(keyType, new Delta.LongType(), true);
                var values = Enumerable.Range(1, 6).Select(n => (IDataValue)new MapValue(Enumerable.Range(0, n).Select(i => new KeyValuePair<IDataValue, IDataValue>(key(i), new Int64Value(i * 3))))).ToList();
                using var stored = Store(type, values);
                var maps = (MapArray)stored.Column(0);
                var comparer = new ArrowComparerVisitor().Visit(type);

                // The same entries reversed, and map keys reversed on their own
                using var permuted = Reversed(maps);
                using var innerKeys = keyType is Delta.MapType ? InnerReversed(maps) : null;
                using var both = innerKeys != null ? Reversed(innerKeys) : null;
                foreach (var copy in new[] { permuted, innerKeys, both }.Where(x => x != null))
                {
                    for (int i = 0; i < values.Count; i++)
                    {
                        Assert.True(comparer.IsEqual(i, maps, i, copy!), $"{keyType.Type} map {i}");
                        Assert.Equal(HashOf(comparer, maps, i), HashOf(comparer, copy, i));
                        Assert.Equal(0, comparer.CompareCanonical(maps, i, copy, i));
                    }
                }
            }
        }

        [Fact]
        public void APermutedRepeatedKeyMapIsAnotherMap()
        {
            var type = new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true);
            var comparer = new ArrowComparerVisitor().Visit(type);
            // Both sides built from one input sequence
            foreach (var count in new[] { 2, 16, 17, 20 })
            {
                var entries = Enumerable.Range(0, count).Select(i => new KeyValuePair<IDataValue, IDataValue>(new StringValue(i < 2 ? "k" : $"k{i}"), new Int64Value(i))).ToArray();
                using var stored = Store(type, new List<IDataValue>() { new MapValue(entries.ToArray()), new MapValue(entries.ToArray()) });
                var maps = (MapArray)stored.Column(0);
                using var permuted = Reversed(maps);

                Assert.True(comparer.IsEqual(0, maps, 1, maps), $"{count} entries");
                Assert.False(comparer.IsEqual(0, maps, 0, permuted), $"{count} entries reversed");
                Assert.NotEqual(0, comparer.CompareCanonical(maps, 0, permuted, 0));
            }
        }

        [Theory]
        [InlineData("struct")]
        [InlineData("list")]
        [InlineData("map")]
        public void ValuesUnderNullParentsAreSkipped(string kind)
        {
            // Another engine left two values under the null parent
            var validity = new ArrowBuffer.BitmapBuilder().Append(false).Build();
            var values = new Int64Array.Builder().Append(42).Append(43).Build();
            var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(2).Build();
            var (type, foreign) = kind switch
            {
                "struct" => ((SchemaBaseType)new Delta.StructType(new[] { Field("x", new Delta.LongType()) }),
                    (IArrowArray)new StructArray(new StructType(new List<Field>() { new Field("x", Int64Type.Default, true) }), 1, new IArrowArray[] { values }, validity, 1)),
                "list" => (new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true },
                    new ListArray(new ListType(Int64Type.Default), 1, offsets, values, validity, 1)),
                _ => (new Delta.MapType(new Delta.LongType(), new Delta.LongType(), true),
                    new MapArray(new MapType(Int64Type.Default, Int64Type.Default), 1, offsets,
                        new StructArray(new MapType(Int64Type.Default, Int64Type.Default).KeyValueType, 2, new IArrowArray[] { values, new Int64Array.Builder().Append(1).Append(2).Build() }, ArrowBuffer.Empty, 0), validity, 1))
            };
            using var stored = Store(type, new List<IDataValue>() { NullValue.Instance });
            using (foreign)
            {
                var comparer = new ArrowComparerVisitor().Visit(type);

                Assert.True(comparer.IsEqual(0, stored.Column(0), 0, foreign));
                Assert.Equal(HashOf(comparer, stored.Column(0), 0), HashOf(comparer, foreign, 0));
                Assert.Equal(0, comparer.CompareCanonical(stored.Column(0), 0, foreign, 0));
                // A missing column is all nulls
                Assert.Equal(HashOf(comparer, stored.Column(0), 0), HashOf(comparer, null, 0));
                AssertColumnHash(comparer, foreign);
                AssertColumnHash(comparer, null, 3);
                Assert.Equal(0, comparer.CompareCanonical(null, 0, foreign, 0));
            }
        }

        [Fact]
        public void ARepeatedKeyMapKeepsItsStoredOrder()
        {
            var type = new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true);
            var comparer = new ArrowComparerVisitor().Visit(type);
            KeyValuePair<IDataValue, IDataValue> Entry(string key, long value) => new KeyValuePair<IDataValue, IDataValue>(new StringValue(key), new Int64Value(value));
            using var stored = Store(type, new List<IDataValue>() { new MapValue(Entry("a", 0), Entry("k", 1), Entry("k", 2)) });
            var maps = (MapArray)stored.Column(0);
            // (k,1) moved in front of (a,0)
            using var moved = Permuted(maps, entries => new[] { entries[1], entries[0], entries[2] });

            Assert.False(comparer.IsEqual(0, maps, 0, moved));
            var order = comparer.CompareCanonical(maps, 0, moved, 0);
            Assert.NotEqual(0, order);
            Assert.Equal(-Math.Sign(order), Math.Sign(comparer.CompareCanonical(moved, 0, maps, 0)));
        }

        [Fact]
        public void NestedTimestampsInOtherUnitsAreEqual()
        {
            var header = StructHeader.Create("ts", "l");
            IDataValue At(long milliseconds) => new TimestampTzValue(DateTime.UnixEpoch.Ticks + milliseconds * TimeSpan.TicksPerMillisecond, 0);
            var types = new (SchemaBaseType Type, Func<int, IDataValue> Value)[]
            {
                (new Delta.StructType(new[] { Field("ts", new Delta.TimestampType()), Field("l", new Delta.ArrayType() { ElementType = new Delta.TimestampType(), ContainsNull = true }) }),
                    i => new StructValue(header, At(i * 7 - 20), new ListValue(At(i), At(-i * 1_000)))),
                (new Delta.MapType(new Delta.StringType(), new Delta.TimestampType(), true),
                    i => new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), At(i)), new KeyValuePair<IDataValue, IDataValue>(new StringValue("b"), At(i * 3 - 40)))),
                (new Delta.MapType(new Delta.TimestampType(), new Delta.LongType(), true),
                    i => new MapValue(new KeyValuePair<IDataValue, IDataValue>(At(i * 5), new Int64Value(i))))
            };
            foreach (var (type, value) in types)
            {
                var values = Enumerable.Range(0, 12).Select(value).ToList();
                using var stored = Store(type, values);
                var column = stored.Column(0);
                var comparer = new ArrowComparerVisitor().Visit(type);
                foreach (var unit in new[] { TimeUnit.Microsecond, TimeUnit.Nanosecond })
                {
                    using var foreign = InUnit(column, unit, 0);
                    using var inexact = InUnit(column, unit, 1);
                    AssertColumnHash(comparer, foreign);
                    AssertColumnHash(comparer, inexact);
                    for (int i = 0; i < values.Count; i++)
                    {
                        Assert.True(comparer.IsEqual(i, column, i, foreign), $"{type} row {i} in {unit}");
                        Assert.Equal(HashOf(comparer, column, i), HashOf(comparer, foreign, i));
                        Assert.Null(Hash(comparer, inexact, i));
                        Assert.False(comparer.IsEqual(i, column, i, inexact));
                    }
                }
            }
        }

        [Fact]
        public void NaNSortsLastAndMinusZeroEqualsZero()
        {
            using var doubles = (DoubleArray)new DoubleArray.Builder().Append(double.NaN).Append(double.PositiveInfinity).Append(-0.0).Append(0.0).Build();
            using var floats = (FloatArray)new FloatArray.Builder().Append(float.NaN).Append(float.PositiveInfinity).Append(-0f).Append(0f).Build();

            foreach (var (comparer, array) in new (IArrowComparer, IArrowArray)[] { (new ArrowFloat64Comparer(), doubles), (new ArrowFloat32Comparer(), floats) })
            {
                Assert.Equal(1, comparer.CompareCanonical(array, 0, array, 1));
                Assert.Equal(0, comparer.CompareCanonical(array, 2, array, 3));
            }
        }

        [Fact]
        public void AUniqueKeyMapSortsBeforeARepeatedKeyMap()
        {
            var type = new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true);
            var pair = new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(1));
            // By entries alone the repeated map would sort first
            using var stored = Store(type, new List<IDataValue>()
            {
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k"), new Int64Value(5))),
                new MapValue(pair, pair)
            });
            var comparer = new ArrowComparerVisitor().Visit(type);

            Assert.Equal(-1, Math.Sign(comparer.CompareCanonical(stored.Column(0), 0, stored.Column(0), 1)));
        }

        [Fact]
        public void DecimalsCompareAllSixteenBytes()
        {
            // Equal low words, the high words differ
            var type = new Decimal128Type(38, 0);
            var builder = new Decimal128Array.Builder(type);
            Span<byte> bytes = stackalloc byte[16];
            foreach (var value in new[] { Int128.One, (Int128.One << 64) + 1 })
            {
                System.Buffers.Binary.BinaryPrimitives.WriteInt128LittleEndian(bytes, value);
                builder.Append(bytes);
            }
            using var decimals = builder.Build();
            var comparer = new ArrowDecimalComparer();

            Assert.False(comparer.IsEqual(0, decimals, 1, decimals));
            Assert.Equal(-1, Math.Sign(comparer.CompareCanonical(decimals, 0, decimals, 1)));
            Assert.NotEqual(HashOf(comparer, decimals, 0), HashOf(comparer, decimals, 1));
        }

        [Fact]
        public void TheMapFallbackSortsInMLogMComparisons()
        {
            const int entries = 1_024;
            var keys = new CountingComparer(new ArrowStringComparer());
            var comparer = new ArrowMapComparer(keys, new ArrowInt64Comparer());
            var type = new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true);
            var random = new Random(3);
            using var stored = Store(type, new List<IDataValue>()
            {
                new MapValue(Enumerable.Range(0, entries).OrderBy(_ => random.Next()).Select(i => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"key {i:D5}"), new Int64Value(i))))
            });
            var maps = (MapArray)stored.Column(0);
            using var permuted = Reversed(maps);

            Assert.True(comparer.IsEqual(0, maps, 0, permuted));
            // Two sorts and two uniqueness walks
            var bound = 4 * entries * (int)Math.Ceiling(Math.Log2(entries)) + 4 * entries;
            Assert.True(keys.Calls <= bound, $"{keys.Calls} key comparisons, bound {bound}");
        }

        private static ulong? Hash(IArrowComparer comparer, IArrowArray? array, int index)
        {
            ulong state = 0;
            return comparer.TryHash(array, index, ref state) ? state : null;
        }

        // A batch over the stored rows lets every row of the column through
        private static void AssertPrefilterKeeps(RecordBatch stored, IArrowComparer comparer, IArrowArray column)
        {
            var batch = new Internal.Deletes.DeleteBatch(stored, Enumerable.Repeat(1L, stored.Length).ToList(), new[] { comparer }, prefilterColumn: 0);
            var hashes = new ulong[column.Length];
            var rejected = new bool[column.Length];
            batch.HashPrefilter(column, hashes, rejected);
            for (int i = 0; i < column.Length; i++)
            {
                Assert.True(rejected[i] || batch.MayMatch(hashes[i]), $"row {i} is rejected");
            }
        }

        // The column path gives each row TryHash's result, sliced or not
        private static void AssertColumnHash(IArrowComparer comparer, IArrowArray? array, int length = -1)
        {
            AssertColumnHashOnce(comparer, array, length);
            // A slice whose offset is not a whole byte of the bitmap
            if (array != null && array.Length > 5)
            {
                AssertColumnHashOnce(comparer, ArrowArrayFactory.Slice(array, 3, array.Length - 5));
            }
        }

        private static void AssertColumnHashOnce(IArrowComparer comparer, IArrowArray? array, int length = -1)
        {
            length = array?.Length ?? length;
            var states = Enumerable.Range(0, length).Select(i => (ulong)i * 0x9E3779B1UL).ToArray();
            var rejected = Enumerable.Range(0, length).Select(i => i % 5 == 4).ToArray();
            var expected = (ulong[])states.Clone();
            comparer.HashColumn(array, states, rejected);
            for (int i = 0; i < length; i++)
            {
                if (i % 5 == 4)
                {
                    Assert.True(rejected[i], $"row {i} was rejected before");
                    continue;
                }
                var hashed = comparer.TryHash(array, i, ref expected[i]);
                Assert.Equal(!hashed, rejected[i]);
                if (hashed)
                {
                    Assert.Equal(expected[i], states[i]);
                }
            }
        }

        // A value that must hash
        private static ulong HashOf(IArrowComparer comparer, IArrowArray? array, int index)
        {
            var hash = Hash(comparer, array, index);
            Assert.True(hash.HasValue, $"row {index} has no hash");
            return hash.Value;
        }

        private static (SchemaBaseType, List<IDataValue>) Generate(string type, Random random)
        {
            List<IDataValue> Many(Func<IDataValue> next, params IDataValue[] edges) => edges.Concat(Enumerable.Range(0, 30).Select(_ => next())).Append(NullValue.Instance).Append(NullValue.Instance).ToList();
            IDataValue Long(long max) => new Int64Value(random.NextInt64(-max, max));
            IDataValue Text() => new StringValue(random.Next(4) switch
            {
                0 => "",
                1 => "a prefix shared by long strings " + random.Next(3),
                2 => "åäö ✓ " + random.Next(3),
                _ => "s" + random.Next(5)
            });
            IDataValue Pair(IDataValue key, IDataValue value) => new MapValue(new KeyValuePair<IDataValue, IDataValue>(key, value));
            IDataValue Pairs(Func<IDataValue> key) => new MapValue(Enumerable.Range(0, random.Next(1, 5)).Select(_ => new KeyValuePair<IDataValue, IDataValue>(key(), Long(2))).ToArray());
            var header = StructHeader.Create("a", "b");
            switch (type)
            {
                case "byte": return (new Delta.ByteType(), Many(() => Long(128), new Int64Value(sbyte.MinValue), new Int64Value(sbyte.MaxValue), new Int64Value(0)));
                case "short": return (new Delta.ShortType(), Many(() => Long(32_768), new Int64Value(short.MinValue), new Int64Value(short.MaxValue)));
                case "int": return (new Delta.IntegerType(), Many(() => Long(4), new Int64Value(int.MinValue), new Int64Value(int.MaxValue)));
                case "long": return (new Delta.LongType(), Many(() => Long(4), new Int64Value(long.MinValue), new Int64Value(long.MaxValue)));
                case "float":
                case "double":
                    var doubles = new IDataValue[]
                    {
                        new DoubleValue(-0.0), new DoubleValue(0.0), new DoubleValue(double.NaN), new DoubleValue(BitConverter.Int64BitsToDouble(0x7FF8000000000001)),
                        new DoubleValue(double.PositiveInfinity), new DoubleValue(double.NegativeInfinity), new DoubleValue(9_007_199_254_740_992), new DoubleValue(long.MaxValue),
                        new Int64Value(9_007_199_254_740_993), new Int64Value(9_007_199_254_740_992), new Int64Value(long.MaxValue), new Int64Value(long.MinValue), new Int64Value(-3), new Int64Value(0)
                    };
                    return (type == "float" ? new Delta.FloatType() : new Delta.DoubleType(), Many(() => new DoubleValue(random.Next(-3, 3) * 0.5), doubles));
                case "decimal":
                    return (new Delta.DecimalType(10, 2), Many(() => new DecimalValue(random.Next(-300, 300) / 100m), new DecimalValue(1.005m), new DecimalValue(1.0050m), new DecimalValue(1.5m), new DecimalValue(1.50m), new DecimalValue(-1.005m)));
                case "timestamp":
                    // Sub-millisecond ticks, before 1970, and other offsets of one instant
                    return (new Delta.TimestampType(), Many(() => new TimestampTzValue(DateTime.UnixEpoch.Ticks + random.NextInt64(-30, 30) * 5_000, random.Next(3) * 60),
                        new TimestampTzValue(DateTime.UnixEpoch.Ticks - 1, 0), new TimestampTzValue(new DateTime(1, 1, 1).Ticks, 0), new TimestampTzValue(DateTime.UnixEpoch.Ticks + 1, 120)));
                case "date": return (new Delta.DateType(), Many(() => new TimestampTzValue(DateTime.UnixEpoch.Ticks + random.Next(-5, 5) * TimeSpan.TicksPerDay, 0)));
                case "bool": return (new Delta.BooleanType(), Many(() => new BoolValue(random.Next(2) == 0)));
                case "string": return (new Delta.StringType(), Many(Text));
                case "binary": return (new Delta.BinaryType(), Many(() => new BinaryValue(Enumerable.Range(0, random.Next(4)).Select(_ => (byte)random.Next(250, 256)).ToArray())));
                case "list": return (new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true }, Many(() => new ListValue(Enumerable.Range(0, random.Next(3)).Select(_ => random.Next(4) == 0 ? NullValue.Instance : Long(2)).ToArray())));
                case "struct":
                    return (new Delta.StructType(new[] { Field("a", new Delta.LongType()), Field("b", new Delta.StringType()) }),
                        Many(() => new StructValue(header, random.Next(3) == 0 ? NullValue.Instance : Long(2), random.Next(3) == 0 ? NullValue.Instance : Text()), new StructValue(header, NullValue.Instance, NullValue.Instance)));
                case "map":
                    return (new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true),
                        Many(() => new MapValue(Enumerable.Range(0, random.Next(4)).Select(_ => random.Next(6)).Distinct().Select(i => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{i}"), random.Next(4) == 0 ? NullValue.Instance : Long(2))))));
                case "repeated map":
                    return (new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true),
                        Many(() => new MapValue(Enumerable.Range(0, random.Next(5)).Select(_ => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{random.Next(2)}"), Long(2))))));
                case "nested map":
                    return (new Delta.MapType(new Delta.StringType(), new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true), true),
                        Many(() => Pair(new StringValue($"k{random.Next(2)}"), Pair(new StringValue($"i{random.Next(2)}"), Long(2)))));
                case "struct keys":
                    return (new Delta.MapType(new Delta.StructType(new[] { Field("a", new Delta.LongType()), Field("b", new Delta.StringType()) }), new Delta.LongType(), true),
                        Many(() => Pairs(() => new StructValue(header, Long(2), Text()))));
                case "list keys":
                    return (new Delta.MapType(new Delta.ArrayType() { ElementType = new Delta.LongType(), ContainsNull = true }, new Delta.LongType(), true),
                        Many(() => Pairs(() => new ListValue(Enumerable.Range(0, random.Next(3)).Select(_ => Long(2)).ToArray()))));
                case "map keys":
                    return (new Delta.MapType(new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true), new Delta.LongType(), true),
                        Many(() => Pairs(() => new MapValue(Enumerable.Range(0, random.Next(3)).Select(i => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{i}"), Long(2)))))));
                case "permuted map":
                    return (new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true),
                        Many(() => new MapValue(Enumerable.Range(0, random.Next(6)).Select(i => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{i}"), Long(2))).ToArray())));
                default:
                    // Map keys that are maps with a repeated key
                    return (new Delta.MapType(new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true), new Delta.LongType(), true),
                        Many(() => Pairs(() => new MapValue(Enumerable.Range(0, random.Next(4)).Select(_ => new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{random.Next(2)}"), Long(2)))))));
            }
        }

        // The values as the delete writer stores them, in one column
        private static RecordBatch Store(SchemaBaseType type, List<IDataValue> values)
        {
            return StoreRows(new Delta.StructType(new[] { Field("v", type) }), values.Select(x => new[] { x }).ToList());
        }

        private static RecordBatch StoreRows(Delta.StructType table, List<IDataValue[]> rows)
        {
            var columns = table.Fields.Select(_ => FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance)).ToArray();
            foreach (var row in rows)
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c].Add(row[c]);
                }
            }
            using var data = new EventBatchData(columns.Cast<IColumn>().ToArray());
            var writer = new ParquetSharpWriter(table, table.Fields.Select(x => x.Name).ToList());
            writer.NewBatch();
            for (int i = 0; i < rows.Count; i++)
            {
                writer.AddRow(new ColumnRowReference() { referenceBatch = data, RowIndex = i });
            }
            return writer.GetRecordBatch();
        }

        private static IArrowArray Timestamps(TimeUnit unit, IEnumerable<long> values)
        {
            var list = values.ToArray();
            return new TimestampArray(new TimestampType(unit, "UTC"), new ArrowBuffer.Builder<long>().AppendRange(list).Build(), ArrowBuffer.Empty, list.Length, 0, 0);
        }

        // A copy with every millisecond leaf in another unit, plus an offset
        private static IArrowArray InUnit(IArrowArray array, TimeUnit unit, long offset)
        {
            return ArrowArrayFactory.BuildArray(InUnit(array.Data, unit, offset));
        }

        private static ArrayData InUnit(ArrayData data, TimeUnit unit, long offset)
        {
            var buffers = data.Buffers.Select(x => new ArrowBuffer(x.Span.ToArray())).ToArray();
            var children = data.Children?.Select(x => InUnit(x, unit, offset)).ToArray();
            var type = data.DataType;
            switch (data.DataType)
            {
                case TimestampType timestamp:
                    var values = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span).ToArray();
                    var scale = unit == TimeUnit.Microsecond ? 1_000 : 1_000_000;
                    buffers[1] = new ArrowBuffer.Builder<long>().AppendRange(values.Select(x => x * scale + offset)).Build();
                    type = new TimestampType(unit, timestamp.Timezone);
                    break;
                case MapType map:
                    var entries = (StructType)children![0].DataType;
                    type = new MapType(entries.Fields[0], entries.Fields[1]);
                    break;
                case StructType structType:
                    type = new StructType(structType.Fields.Select((x, i) => new Field(x.Name, children![i].DataType, x.IsNullable)).ToList());
                    break;
                case ListType list:
                    type = new ListType(new Field(list.ValueField.Name, children![0].DataType, list.ValueField.IsNullable));
                    break;
            }
            return new ArrayData(type, data.Length, data.NullCount, data.Offset, buffers, children);
        }

        // The same maps with every map's entries in reverse order
        private static MapArray Reversed(MapArray maps)
        {
            return Permuted(maps, entries => entries.Reverse().ToArray());
        }

        private static MapArray Permuted(MapArray maps, Func<int[], int[]> permute)
        {
            var order = new List<int>();
            for (int i = 0; i < maps.Length; i++)
            {
                var offset = maps.ValueOffsets[i];
                order.AddRange(permute(Enumerable.Range(offset, maps.GetValueLength(i)).ToArray()));
            }
            var entries = (StructArray)maps.KeyValues;
            var children = entries.Fields.Select(x => (IArrowArray)Take(x, order)).ToArray();
            var permuted = new StructArray(entries.Data.DataType, order.Count, children, ArrowBuffer.Empty, 0);
            var offsets = new ArrowBuffer.Builder<int>();
            for (int i = 0; i <= maps.Length; i++)
            {
                offsets.Append(maps.ValueOffsets[i] - maps.ValueOffsets[0]);
            }
            return new MapArray(maps.Data.DataType, maps.Length, offsets.Build(), permuted, new ArrowBuffer(maps.NullBitmapBuffer.Span.ToArray()), maps.NullCount, 0);
        }

        // The same maps with each key map's entries reversed
        private static MapArray InnerReversed(MapArray maps)
        {
            var entries = maps.KeyValues;
            var keys = Reversed((MapArray)entries.Fields[0]);
            var values = Take(entries.Fields[1], Enumerable.Range(0, entries.Length).ToList());
            var offsets = new ArrowBuffer.Builder<int>();
            for (int i = 0; i <= maps.Length; i++)
            {
                offsets.Append(maps.ValueOffsets[i] - maps.ValueOffsets[0]);
            }
            var children = new StructArray(entries.Data.DataType, entries.Length, new IArrowArray[] { keys, values }, ArrowBuffer.Empty, 0);
            return new MapArray(maps.Data.DataType, maps.Length, offsets.Build(), children, new ArrowBuffer(maps.NullBitmapBuffer.Span.ToArray()), maps.NullCount, 0);
        }

        // The rows of an array in the given order, through the Arrow C data path
        private static IArrowArray Take(IArrowArray array, List<int> order)
        {
            var slices = order.Select(i => ArrowArrayFactory.Slice(array, i, 1)).ToList();
            return ArrowArrayConcatenator.Concatenate(slices);
        }

        private static async Task<List<RecordBatch>> ReadForDeletes(byte[] file)
        {
            var batches = new List<RecordBatch>();
            using var dataFile = await Open(file);
            await foreach (var batch in dataFile.ReadBatches(null))
            {
                batches.Add(batch);
            }
            return batches;
        }

        private static async Task<ParquetDataFile> Open(byte[] bytes)
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/data.parquet"))
            {
                await write.WriteAsync(bytes);
            }
            return await ParquetSharpReader.OpenDataFile(storage, "/t", "data.parquet", forDeletes: true);
        }

        private static Delta.StructField Field(string name, SchemaBaseType type)
        {
            return new Delta.StructField(name, type, true, new Dictionary<string, object>());
        }

        // Counts the key comparisons of the map fallback
        private sealed class CountingComparer : IArrowComparer
        {
            private readonly IArrowComparer _inner;

            public CountingComparer(IArrowComparer inner)
            {
                _inner = inner;
            }

            public int Calls { get; private set; }

            public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
            {
                Calls++;
                return _inner.IsEqual(leftIndex, array, rightIndex, otherArray);
            }

            public bool TryHash(IArrowArray? array, int index, ref ulong state) => _inner.TryHash(array, index, ref state);

            public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
            {
                Calls++;
                return _inner.CompareCanonical(left, leftIndex, right, rightIndex);
            }

        }
    }
}
