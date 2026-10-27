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
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Check;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using TemporaryTree = FlowtideDotNet.Storage.Tree.IBPlusTree<FlowtideDotNet.Core.ColumnStore.TreeStorage.ColumnRowReference, int, FlowtideDotNet.Core.ColumnStore.TreeStorage.ColumnKeyStorageContainer, FlowtideDotNet.Storage.Tree.PrimitiveListValueContainer<int>>;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class BulkInsertMatchesPerRowTests
    {
        private static readonly StructHeader s_x = StructHeader.Create("x");
        private static readonly StructHeader s_xy = StructHeader.Create("x", "y");
        private static readonly string s_prefix = new string('p', 40);
        private static readonly MapValue s_mapA = new MapValue(Pair("a", new Int64Value(2)), Pair("b", new Int64Value(1)));
        private static readonly MapValue s_mapB = new MapValue(Pair("a", new Int64Value(1)), Pair("c", new Int64Value(0)));

        // Each shape builds fresh batches from a seeded random
        private static readonly Dictionary<string, Func<Random, List<Batch>>> s_shapes = CreateShapes();

        public static IEnumerable<object[]> Shapes() => s_shapes.Keys.Select(x => new object[] { x });

        [Theory]
        [MemberData(nameof(Shapes))]
        public async Task BulkInsertMatchesPerRow(string shape)
        {
            var batches = s_shapes[shape](new Random(17));
            var columnCount = batches[0].Columns.Length;
            using var storage = new FileCachePersistentStorage(new FileCacheOptions());
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 4,
                MinCachePageCount = 1,
                PersistentStorage = storage
            }, NullLoggerFactory.Instance, new Meter("bulk_insert"), "bulk_insert", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var client = stateManager.GetOrCreateClient("node");
            var perRow = await CreateTree(client, "per_row", columnCount);
            var bulk = await CreateTree(client, "bulk", columnCount);
            var presorted = await CreateTree(client, "presorted", columnCount);
            // One inserter per tree, each uses one overload
            var bulkInserter = bulk.CreateBulkInserter();
            var presortedInserter = presorted.CreateBulkInserter();
            var sorter = new BatchSorter(columnCount);

            foreach (var batch in batches)
            {
                var count = batch.Weights.Length;
                if (count == 0)
                {
                    continue;
                }
                var data = new EventBatchData(batch.Columns);
                for (int i = 0; i < count; i++)
                {
                    await perRow.RMWNoResult(new ColumnRowReference() { referenceBatch = data, RowIndex = i }, batch.Weights[i], (input, current, exists) =>
                    {
                        if (exists)
                        {
                            var newWeight = current + input;
                            if (newWeight == 0)
                            {
                                return (0, GenericWriteOperation.Delete);
                            }
                            return (newWeight, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });
                }
                var bytes = data.GetByteSize() + count * sizeof(int);
                await bulkInserter.ApplyBatch(Keys(data, count), (int[])batch.Weights.Clone(), count, new WeightMutator(), bytes);
                var (order, tags) = Sort(sorter, batch.Columns, count);
                await presortedInserter.ApplyBatch(Keys(data, count), (int[])batch.Weights.Clone(), count, order, tags, new WeightMutator(), bytes);
            }

            var expected = await Read(perRow, columnCount);
            AssertSameEntries(expected, await Read(bulk, columnCount), columnCount);
            AssertSameEntries(expected, await Read(presorted, columnCount), columnCount);
        }

        [Theory]
        [MemberData(nameof(Shapes))]
        public void BatchSorterMatchesTreeOrder(string shape)
        {
            foreach (var batch in s_shapes[shape](new Random(29)))
            {
                AssertSortMatchesComparer(batch.Columns, batch.Weights.Length);
            }
        }

        [Fact]
        public void BatchSorterMatchesTreeOrderAfterAnEmptyIntegerLayout()
        {
            // An integer column with no values has no width yet
            var empty = new Column(0, new IntegerColumn(GlobalMemoryManager.Instance), default(BitmapList), ArrowTypeId.Int64, GlobalMemoryManager.Instance);
            var none = Span<int>.Empty;
            var noTags = Span<int>.Empty;
            new BatchSorter(1).SortDataWithTags(new IColumn[] { empty }, ref none, ref noTags);
            AssertSortMatchesComparer(new IColumn[] { Plain(new Int64Value(3), new Int64Value(1), new Int64Value(2)) }, 3);

            Sort(new BatchSorter(1), new IColumn[] { Wrap(empty, -1, -1) }, 2);
            AssertSortMatchesComparer(new IColumn[] { Wrap(Plain(new Int64Value(30), new Int64Value(10), new Int64Value(20)), 0, -1, 1, 2) }, 4);
        }

        [Fact]
        public void BatchSorterMatchesTreeOrderOnAMergedUnionChild()
        {
            var merged = MergedUnionStruct();
            foreach (var column in new IColumn[] { merged, Wrap(merged, 0, 1, 2, 3, 4, 5, -1) })
            {
                Assert.Equal(2, column.GetValueAt(4, default).AsStruct.GetAt(0).AsLong);
                Assert.Equal("b", column.GetValueAt(5, default).AsStruct.GetAt(0).AsString.ToString());
                for (int a = 0; a < column.Count; a++)
                {
                    for (int b = 0; b < column.Count; b++)
                    {
                        Assert.Equal(Math.Sign(column.CompareTo(column, a, b)), -Math.Sign(column.CompareTo(column, b, a)));
                    }
                }
                AssertSortMatchesComparer(new[] { column }, column.Count);
            }
        }

        private static Dictionary<string, Func<Random, List<Batch>>> CreateShapes()
        {
            var shapes = new Dictionary<string, Func<Random, List<Batch>>>()
            {
                ["flat"] = Generated(3, (r, b, c) => c switch
                {
                    0 => Maybe(r, new Int64Value(r.Next(20))),
                    1 => Maybe(r, new StringValue(new[] { "", "a", "b" }[r.Next(3)])),
                    _ => Maybe(r, new DoubleValue(r.Next(10) / 2.0)),
                }),
                ["mixed types"] = Generated(2, (r, b, c) => r.Next(3) switch
                {
                    0 => NullValue.Instance,
                    1 => new Int64Value(r.Next(5)),
                    _ => c == 0 ? new DoubleValue(r.Next(5) + 0.5) : new StringValue("s" + r.Next(5)),
                }),
                // Batches move across the integer widths
                ["integer widths"] = Generated(2, (r, b, c) => c == 0
                    ? new Int64Value(new[] { 0L, 100, 32_700, 2_147_483_600, -40_000, long.MaxValue - 200 }[b] + r.Next(100))
                    : Maybe(r, new Int64Value(r.Next(3)))),
                ["nested"] = Generated(3, (r, b, c) => Maybe(r, c switch
                {
                    0 => new StructValue(s_xy, Mixed(r), Maybe(r, new DoubleValue(r.Next(3)))),
                    1 => new ListValue(Enumerable.Range(0, r.Next(3)).Select(_ => Mixed(r)).ToArray()),
                    _ => new MapValue(Pair("a", Mixed(r)), Pair(r.Next(2) == 0 ? "b" : "c", Maybe(r, new DoubleValue(r.Next(3))))),
                })),
                ["struct child switches type"] = Generated(1, (r, b, c) => Maybe(r, new StructValue(s_x, b == 0 ? Maybe(r, new Int64Value(r.Next(5))) : Maybe(r, new StringValue("s" + r.Next(5)))))),
                ["struct header changes"] = Generated(1, (r, b, c) => b % 2 == 0
                    ? new StructValue(s_x, new Int64Value(r.Next(5)))
                    : new StructValue(s_xy, new Int64Value(r.Next(5)), new Int64Value(r.Next(2)))),
                ["list element switches type"] = Generated(1, (r, b, c) => b % 2 == 0
                    ? new ListValue(new Int64Value(r.Next(5)))
                    : new ListValue(new StringValue("s" + r.Next(5)))),
                ["maps"] = Generated(1, (r, b, c) => new IDataValue[] { s_mapA, s_mapB, NullValue.Instance }[r.Next(3)]),
                ["doubles"] = Generated(1, (r, b, c) => Pick(r, new DoubleValue(-0.0), new DoubleValue(0.0), new DoubleValue(double.NaN), new DoubleValue(double.PositiveInfinity), new DoubleValue(double.NegativeInfinity), new DoubleValue(1.5), NullValue.Instance)),
                ["decimals"] = Generated(1, (r, b, c) => Pick(r, new DecimalValue(1.0m), new DecimalValue(1.00m), new DecimalValue(2.5m), new DecimalValue(-1.0m), NullValue.Instance)),
                ["timestamps with offsets"] = Generated(1, (r, b, c) => Maybe(r, new TimestampTzValue(r.Next(3) * 10, new[] { 0, 60, -120 }[r.Next(3)]))),
                ["strings"] = Generated(1, (r, b, c) => Pick(r, new StringValue(s_prefix + "a"), new StringValue(s_prefix + "b"), new StringValue(s_prefix), new StringValue("é"), new StringValue("日本"), new StringValue(""), NullValue.Instance)),
                ["binary and bools"] = Generated(2, (r, b, c) => Maybe(r, c == 0 ? new BinaryValue(new byte[] { (byte)r.Next(3), 7 }) : new BoolValue(r.Next(2) == 0))),
                ["null typed"] = Generated(2, (r, b, c) => c == 0 ? NullValue.Instance : new Int64Value(r.Next(5))),
                // Integers only, tags come from radix keys
                ["radix integers"] = Generated(2, (r, b, c) => c == 0 ? Maybe(r, new Int64Value(r.Next(-3, 4))) : new Int64Value(r.Next(3))),
                // Ties reach the columns past the seventh
                ["wide"] = Generated(9, (r, b, c) => (c % 3) switch
                {
                    0 => Maybe(r, new Int64Value(r.Next(2))),
                    1 => Maybe(r, new StringValue(r.Next(2) == 0 ? "a" : "b")),
                    _ => Maybe(r, new DoubleValue(r.Next(2))),
                }),
                ["merged union child"] = r => new List<Batch>()
                {
                    new Batch(new[] { MergedUnionStruct() }, new[] { 1, 1, 1, 1, 1, 1 }),
                    new Batch(new IColumn[] { Plain(new StructValue(s_x, new Int64Value(2)), new StructValue(s_x, new StringValue("b"))) }, new[] { -1, -1 }),
                },
                ["large equal groups"] = EqualGroups,
            };
            foreach (var inStruct in new[] { false, true })
            {
                var name = inStruct ? " in a struct" : "";
                // Null-padded wrappers into doubles, nulls and wide integers
                shapes["padded into doubles" + name] = r => WithIds(Plain(Field(inStruct, new DoubleValue(1.5)), Field(inStruct, new DoubleValue(2.5))),
                    Wrap(Plain(Field(inStruct, new Int64Value(5)), Field(inStruct, NullValue.Instance), Field(inStruct, new Int64Value(7))), -1, 0, 1, -1, 2));
                shapes["padded into nulls" + name] = r => WithIds(Plain(Field(inStruct, NullValue.Instance), Field(inStruct, NullValue.Instance)),
                    Wrap(Plain(Field(inStruct, new Int64Value(10)), Field(inStruct, new DoubleValue(20.5)), Field(inStruct, NullValue.Instance)), 0, -1, 1, 2),
                    Plain(Field(inStruct, new Int64Value(5)), Field(inStruct, new DoubleValue(1.5))));
                shapes["padded into wider integers" + name] = r => WithIds(Plain(Field(inStruct, new Int64Value(1000)), Field(inStruct, new Int64Value(2000))),
                    Wrap(Plain(Field(inStruct, new Int64Value(5)), Field(inStruct, new Int64Value(6))), -1, 0, 1));
            }
            return shapes;
        }

        // Random rows over small domains, some retract earlier rows
        private static Func<Random, List<Batch>> Generated(int columnCount, Func<Random, int, int, IDataValue> value)
        {
            return r =>
            {
                var batches = new List<Batch>();
                var seen = new List<IDataValue[]>();
                for (int b = 0; b < 7; b++)
                {
                    var rows = new List<IDataValue[]>();
                    var weights = new List<int>();
                    // The last batch is empty
                    var count = b == 6 ? 0 : r.Next(1, 120);
                    for (int i = 0; i < count; i++)
                    {
                        if (seen.Count > 0 && r.Next(5) == 0)
                        {
                            rows.Add(seen[r.Next(seen.Count)]);
                            weights.Add(-r.Next(1, 3));
                            continue;
                        }
                        var row = new IDataValue[columnCount];
                        for (int c = 0; c < columnCount; c++)
                        {
                            row[c] = value(r, b, c);
                        }
                        rows.Add(row);
                        seen.Add(row);
                        weights.Add(r.Next(1, 4));
                    }
                    batches.Add(Build(r, rows, weights.ToArray(), columnCount));
                }
                return batches;
            };
        }

        // Over 16 equal rows per key, netting to 0
        private static List<Batch> EqualGroups(Random r)
        {
            var rows = new List<(long Key, int Weight)>();
            foreach (var (key, plus, minus) in new[] { (1L, 8, 9), (2L, 10, 10), (3L, 9, 9) })
            {
                rows.AddRange(Enumerable.Repeat((key, 1), plus).Concat(Enumerable.Repeat((key, -1), minus)));
            }
            var shuffled = rows.OrderBy(_ => r.Next()).ToList();
            return new List<Batch>()
            {
                Build(r, new List<IDataValue[]>() { new IDataValue[] { new Int64Value(1) }, new IDataValue[] { new Int64Value(3) } }, new[] { 1, 1 }, 1),
                Build(r, shuffled.Select(x => new IDataValue[] { new Int64Value(x.Key) }).ToList(), shuffled.Select(x => x.Weight).ToArray(), 1),
            };
        }

        // Rows to columns, each column plain or wrapped at random
        private static Batch Build(Random r, List<IDataValue[]> rows, int[] weights, int columnCount)
        {
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var values = rows.Select(x => x[c]).ToArray();
                columns[c] = r.Next(2) == 0 ? Plain(values) : WrapAtRandom(r, values);
            }
            return new Batch(columns, weights);
        }

        // Nulls become sentinels or inner nulls, extra rows shift offsets
        private static ColumnWithOffset WrapAtRandom(Random r, IDataValue[] values)
        {
            var inner = new Column(GlobalMemoryManager.Instance);
            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                if (value.IsNull && r.Next(2) == 0)
                {
                    offsets.Add(ColumnWithOffset.NullValueIndex);
                    continue;
                }
                if (r.Next(3) == 0)
                {
                    inner.Add(value);
                }
                offsets.Add(inner.Count);
                inner.Add(value);
            }
            return new ColumnWithOffset(inner, offsets);
        }

        // Three batches merged like the deferred insert buffer
        private static Column MergedUnionStruct()
        {
            var merged = Column.Create(GlobalMemoryManager.Instance);
            var parts = new[]
            {
                Plain(new StructValue(s_x, new Int64Value(1)), new StructValue(s_x, new StringValue("a")), new StructValue(s_x, NullValue.Instance)),
                Plain(NullValue.Instance),
                Plain(new StructValue(s_x, new Int64Value(2)), new StructValue(s_x, new StringValue("b"))),
            };
            foreach (var part in parts)
            {
                merged.InsertRangeFrom(merged.Count, part, 0, part.Count);
            }
            return merged;
        }

        private static void AssertSortMatchesComparer(IColumn[] columns, int count)
        {
            var (order, tags) = Sort(new BatchSorter(columns.Length), columns, count);
            var data = new EventBatchData(columns);
            var comparer = new ColumnComparer(columns.Length);
            for (int i = 1; i < count; i++)
            {
                var previous = new ColumnRowReference() { referenceBatch = data, RowIndex = order[i - 1] };
                var current = new ColumnRowReference() { referenceBatch = data, RowIndex = order[i] };
                var forward = comparer.CompareTo(previous, current);
                Assert.True(forward <= 0, $"Rows {order[i - 1]} and {order[i]} are out of order");
                Assert.Equal(Math.Sign(forward), -Math.Sign(comparer.CompareTo(current, previous)));
                Assert.Equal(forward == 0, tags[i] == tags[i - 1]);
            }
        }

        // Entries match in order, weight and tree compare
        private static void AssertSameEntries((EventBatchData Data, List<int> Weights) expected, (EventBatchData Data, List<int> Weights) actual, int columnCount)
        {
            Assert.Equal(expected.Weights, actual.Weights);
            var comparer = new ColumnComparer(columnCount);
            for (int i = 0; i < expected.Weights.Count; i++)
            {
                Assert.Equal(0, comparer.CompareTo(new ColumnRowReference() { referenceBatch = expected.Data, RowIndex = i }, new ColumnRowReference() { referenceBatch = actual.Data, RowIndex = i }));
            }
        }

        // Copies every entry out, zero weights are left out
        private static async Task<(EventBatchData Data, List<int> Weights)> Read(TemporaryTree tree, int columnCount)
        {
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                columns[c] = new Column(GlobalMemoryManager.Instance);
            }
            var weights = new List<int>();
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    if (kv.Value == 0)
                    {
                        continue;
                    }
                    for (int c = 0; c < columnCount; c++)
                    {
                        columns[c].Add(kv.Key.referenceBatch.Columns[c].GetValueAt(kv.Key.RowIndex, default));
                    }
                    weights.Add(kv.Value);
                }
            }
            return (new EventBatchData(columns), weights);
        }

        // Built like the sink's temporary tree, with small pages
        private static async Task<TemporaryTree> CreateTree(IStateManagerClient client, string name, int columnCount)
        {
            return await client.GetOrCreateTree(name, new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(columnCount),
                KeySerializer = new ColumnStoreSerializer(columnCount, GlobalMemoryManager.Instance),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance),
                MemoryAllocator = GlobalMemoryManager.Instance,
                UseByteBasedPageSizes = true,
                PageSizeBytes = 1024,
            });
        }

        private static (int[] Order, int[] Tags) Sort(BatchSorter sorter, IColumn[] columns, int count)
        {
            var order = Enumerable.Range(0, count).ToArray();
            var tags = new int[count];
            var orderSpan = order.AsSpan();
            var tagSpan = tags.AsSpan();
            sorter.SortDataWithTags(columns, ref orderSpan, ref tagSpan);
            return (order, tags);
        }

        // Every key on the same batch, at its own row
        private static ColumnRowReference[] Keys(EventBatchData data, int count)
        {
            return Enumerable.Range(0, count).Select(i => new ColumnRowReference() { referenceBatch = data, RowIndex = i }).ToArray();
        }

        // An id per row, so padded nulls are new keys
        private static List<Batch> WithIds(params IColumn[] columns)
        {
            var id = 0;
            return columns.Select(x => new Batch(new IColumn[] { x, Plain(Enumerable.Range(0, x.Count).Select(_ => (IDataValue)new Int64Value(id++)).ToArray()) }, Enumerable.Repeat(1, x.Count).ToArray())).ToList();
        }

        private static Column Plain(params IDataValue[] values)
        {
            var column = new Column(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                column.Add(value);
            }
            return column;
        }

        private static ColumnWithOffset Wrap(IColumn inner, params int[] offsets)
        {
            var list = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var offset in offsets)
            {
                list.Add(offset);
            }
            return new ColumnWithOffset(inner, list);
        }

        private static IDataValue Field(bool inStruct, IDataValue value) => inStruct ? new StructValue(s_x, value) : value;

        private static IDataValue Maybe(Random r, IDataValue value) => r.Next(8) == 0 ? NullValue.Instance : value;

        private static IDataValue Mixed(Random r) => Pick(r, new Int64Value(r.Next(3)), new StringValue("s" + r.Next(3)), NullValue.Instance);

        private static IDataValue Pick(Random r, params IDataValue[] values) => values[r.Next(values.Length)];

        private static KeyValuePair<IDataValue, IDataValue> Pair(string key, IDataValue value) => new KeyValuePair<IDataValue, IDataValue>(new StringValue(key), value);

        // Sums weights, zero deletes or skips the key
        private readonly struct WeightMutator : IRowMutator<ColumnRowReference, int>
        {
            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                CheckKeySizes.GetSizePrefixSum(keys, indices, sizes);
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in int existingData, ref int incomingData, int sortedIndex)
            {
                if (exists)
                {
                    incomingData += existingData;
                    return incomingData == 0 ? GenericWriteOperation.Delete : GenericWriteOperation.Upsert;
                }
                return incomingData == 0 ? GenericWriteOperation.None : GenericWriteOperation.Upsert;
            }
        }

        private sealed record Batch(IColumn[] Columns, int[] Weights);
    }
}
