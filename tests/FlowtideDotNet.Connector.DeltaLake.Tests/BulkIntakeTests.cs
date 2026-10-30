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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Diagnostics.Metrics;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class BulkIntakeTests
    {
        private static readonly IDataValue Null = NullValue.Instance;
        private static readonly StructHeader s_header = StructHeader.Create("a", "b");

        [Theory]
        [InlineData("flat")]
        [InlineData("mixed")]
        [InlineData("nested")]
        [InlineData("merged")]
        public async Task BulkIntakeMatchesPerRow(string shape)
        {
            var (width, batches) = Batches(shape, new Random(shape.Length));
            // Declared first, so the storage is disposed last
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                // Four pages of 1 KiB, so pages split, merge and spill
                CachePageCount = 4,
                MinCachePageCount = 1,
                PersistentStorage = storage
            }, NullLoggerFactory.Instance, new Meter(nameof(BulkIntakeMatchesPerRow)), nameof(BulkIntakeMatchesPerRow), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var client = stateManager.GetOrCreateClient("intake");
            var perRowTree = await Tree(client, "per_row", width, 1024);
            var bulkTree = await Tree(client, "bulk", width, 1024);
            var perRow = new TemporaryTreeIntake(perRowTree, width, new DeleteLimits() { BulkIntake = false });
            var bulk = new TemporaryTreeIntake(bulkTree, width, DeleteLimits.Default);

            foreach (var (batch, weights) in batches)
            {
                await perRow.Apply(batch, weights);
                await bulk.Apply(batch, weights);
            }

            // Equal under the tree comparer, the stored row may differ
            var (expected, _, _) = await Read(perRowTree);
            var (actual, pages, zeros) = await Read(bulkTree);
            Assert.True(pages > 1, $"{pages} page");
            // The bulk path stores no zero weight
            Assert.Equal(0, zeros);
            Assert.Equal(expected.Count, actual.Count);
            var comparer = new ColumnComparer(width);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.True(comparer.CompareTo(expected[i].Key, actual[i].Key) == 0, $"Row {i}: {expected[i].Key} against {actual[i].Key}");
                Assert.Equal(expected[i].Weight, actual[i].Weight);
            }
            // Only the batch with an extra column goes per-row
            var extra = batches.Where(x => x.Batch.Columns.Count != width).Sum(x => x.Weights.Count);
            Assert.Equal(extra, bulk.PerRowRows);
            Assert.Equal(batches.Sum(x => x.Weights.Count) - extra, bulk.BulkRows);
            Assert.Equal(0, bulk.WrapperRows);
            foreach (var (batch, weights) in batches)
            {
                batch.Dispose();
                weights.Dispose();
            }
            foreach (var key in expected.Concat(actual).Select(x => x.Key.referenceBatch).Distinct())
            {
                key.Dispose();
            }
        }

        [Theory]
        [InlineData("filter")]
        [InlineData("join")]
        [InlineData("union")]
        public async Task PipelineInputMatchesWithBulkOnAndOff(string pipeline)
        {
            var name = $"{nameof(PipelineInputMatchesWithBulkOnAndOff)}_{pipeline}";
            var off = await RunPipeline(name + "_off", pipeline, new DeleteLimits() { BulkIntake = false });
            var on = await RunPipeline(name + "_on", pipeline, DeleteLimits.Default);

            Assert.Equal(off.History, on.History);
            // Commits with retractions write change data
            Assert.All(on.History.Skip(1), x => Assert.Contains("cdc ", x));
            // Wrapper input went through the bulk path
            Assert.True(on.Intakes.Sum(x => x.WrapperRows) > 0);
            Assert.Equal(0, on.Intakes.Sum(x => x.PerRowRows));
            Assert.True(on.Intakes.Sum(x => x.BulkRows) > 0);
            Assert.Equal(0, off.Intakes.Sum(x => x.BulkRows));
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task DeleteCdfOnEveryIntakePath(bool bulk, bool wrapper)
        {
            var testName = $"{nameof(DeleteCdfOnEveryIntakePath)}_{bulk}_{wrapper}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // A delete rewrites its file, whose survivors are no changes
                options.EnableDeletionVectorsOnNewTables = false;
                options.DeleteLimitsOverride = new DeleteLimits() { BulkIntake = bulk };
            });
            // Computed columns arrive new, filtered source columns as wrappers
            var select = wrapper ? "userKey AS userkey, firstName AS name FROM users WHERE visits > 0" : "userKey + 0 AS userkey, firstName || '' AS name FROM users";
            var users = Enumerable.Range(1, 22).Select(x => new User() { UserKey = x, FirstName = $"First{x}", Visits = x % 4 }).ToList();
            stream.AddUsers(users.Take(20).ToList());
            await stream.StartStream($"CREATE TABLE t (userkey INT, name STRING); INSERT INTO t SELECT {select}");
            await WaitForVersion(storage, "t", stream, 0);

            // One fetch, one checkpoint for the retraction and inserts
            await stream.EnterDataWriteLockAsync();
            try
            {
                stream.AddUsers(users.Skip(20).ToList());
                stream.DeleteUsers(new[] { users[1] });
            }
            finally
            {
                stream.ExitDataWriteLock();
            }
            await WaitForVersion(storage, "t", stream, 1);

            // The file was rewritten, no deletion vector hides a survivor
            var actions = await ReadCommitActions(storage, "t", 1);
            Assert.Contains(actions, x => x.Remove != null);
            Assert.All(actions.Where(x => x.Add != null), x => Assert.Null(x.Add!.DeletionVector));
            Assert.Equal(new[] { "delete 2", "insert 21", "insert 22" }, await ChangeKeys(storage, "t", 1));
            var intake = stream.Sinks[^1].Intake!;
            Assert.Equal(bulk, intake.BulkRows > 0);
            Assert.Equal(!bulk, intake.PerRowRows > 0);
            // Every batch wraps its columns, or none does
            Assert.Equal(wrapper ? intake.BulkRows + intake.PerRowRows : 0, intake.WrapperRows);
        }

        private static async Task<(List<string> History, List<TemporaryTreeIntake> Intakes)> RunPipeline(string testName, string pipeline, DeleteLimits limits)
        {
            var sql = pipeline switch
            {
                "filter" => "CREATE TABLE t (userkey INT, name STRING); INSERT INTO t SELECT userKey AS userkey, firstName AS name FROM users WHERE visits > 1",
                // Users without orders are null-padded, user 1 fans out
                "join" => "CREATE TABLE t (userkey INT, name STRING, orderkey INT); INSERT INTO t SELECT u.userKey AS userkey, u.firstName AS name, o.orderKey AS orderkey FROM users u LEFT JOIN orders o ON u.userKey = o.userKey",
                // Both branches read one view, so one source
                _ => "CREATE TABLE t (userkey INT, name STRING); CREATE VIEW u AS SELECT userKey AS userkey, firstName AS name, visits FROM users; INSERT INTO t SELECT userkey, name FROM u WHERE visits > 1 UNION ALL SELECT userkey, name FROM u"
            };
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Mapped tables get random physical names, so files would differ
                options.EnableColumnMappingOnNewTables = false;
                options.DeleteLimitsOverride = limits;
            });
            // Fixed rows, the generator's seed is shared by every test
            var users = Enumerable.Range(1, 30).Select(x => new User() { UserKey = x, FirstName = x == 1 ? new string('w', 2000) : $"First{x}", Visits = x % 4 }).ToList();
            stream.AddUsers(users);
            foreach (var user in users.Take(20))
            {
                for (int i = 0; i < (user.UserKey == 1 ? 8 : user.UserKey % 3); i++)
                {
                    stream.AddOrder(new Order() { OrderKey = user.UserKey * 100 + i, UserKey = user.UserKey });
                }
            }
            await stream.StartStream(sql);
            if (pipeline == "union")
            {
                // Two sources could see one change in different versions
                Assert.Single(stream.GetDiagnosticsGraph().Nodes.Values, x => x.DisplayName == "Mock data source");
            }
            await WaitForVersion(storage, "t", stream, 0);

            // Retractions, including fanned-out and unmatched rows
            stream.EnterDataWriteLock();
            stream.DeleteUsers(users.Where(x => x.UserKey % 6 == 1).ToList());
            stream.ExitDataWriteLock();
            var version = 1;
            await WaitForVersion(storage, "t", stream, version);

            // Orders feed their own source, so their own version
            if (pipeline == "join")
            {
                stream.DeleteOrder(stream.Orders.First(x => x.UserKey == 2));
                await WaitForVersion(storage, "t", stream, ++version);
            }

            // One source fetch sees both changes
            stream.EnterDataWriteLock();
            stream.AddOrUpdateUser(new User() { UserKey = 2, FirstName = "Renamed", Visits = 3 });
            stream.AddUsers(new[] { new User() { UserKey = 31, FirstName = "First31", Visits = 2 } });
            stream.ExitDataWriteLock();
            await WaitForVersion(storage, "t", stream, ++version);

            return (await TableHistory(storage, "t", version), stream.Sinks.Select(x => x.Intake!).ToList());
        }

        // As the sink builds its temporary tree, smaller pages force splits
        internal static ValueTask<IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>> Tree(IStateManagerClient client, string name, int width, int? pageSizeBytes = null)
        {
            return client.GetOrCreateTree(name, new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(width),
                KeySerializer = new ColumnStoreSerializer(width, GlobalMemoryManager.Instance),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance),
                MemoryAllocator = GlobalMemoryManager.Instance,
                UseByteBasedPageSizes = true,
                PageSizeBytes = pageSizeBytes,
            });
        }

        // Keys copied out of their pages, zero weights skipped and counted
        private static async Task<(List<(ColumnRowReference Key, int Weight)> Rows, int Pages, int Zeros)> Read(IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> tree)
        {
            var rows = new List<(ColumnRowReference, int)>();
            var pages = 0;
            var zeros = 0;
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                pages++;
                var copy = new EventBatchData(page.Keys.Data.Columns.Select(x => (IColumn)x.Copy(GlobalMemoryManager.Instance)).ToArray());
                var kept = rows.Count;
                for (int i = 0; i < page.Values.Data.Count; i++)
                {
                    if (page.Values.Data[i] != 0)
                    {
                        rows.Add((new ColumnRowReference() { referenceBatch = copy, RowIndex = i }, page.Values.Data[i]));
                    }
                    else
                    {
                        zeros++;
                    }
                }
                if (rows.Count == kept)
                {
                    copy.Dispose();
                }
            }
            return (rows, pages, zeros);
        }

        // Seeded batches with repeats, retractions and edge batches
        private static (int Width, List<(EventBatchData Batch, PrimitiveList<int> Weights)> Batches) Batches(string shape, Random random)
        {
            var width = shape switch { "flat" => 5, "mixed" => 2, "nested" => 3, _ => 1 };
            var batches = new List<(EventBatchData, PrimitiveList<int>)>();
            var written = new List<IDataValue[]>();
            if (shape == "merged")
            {
                batches.Add((new EventBatchData(new IColumn[] { MergedStructColumn() }), Weights(1, 1, 1, 1, 1, 1)));
                written.Add(new[] { Struct(new Int64Value(2), Null) });
                written.Add(new[] { Struct(new StringValue("b"), Null) });
                batches.Add((Batch(width, written), Weights(-1, -1)));
            }
            // Net weight per row object, in first-seen order
            var sums = new Dictionary<IDataValue[], int>(ReferenceEqualityComparer.Instance);
            var seen = new List<IDataValue[]>();
            for (int b = 0; b < 12; b++)
            {
                var rows = new List<IDataValue[]>();
                var weights = new List<int>();
                if (b == 4 || b == 8)
                {
                    // Most rows retracted, so pages shrink and merge
                    for (int i = 0; i < seen.Count; i++)
                    {
                        if (i % 5 != 0 && sums[seen[i]] != 0)
                        {
                            rows.Add(seen[i]);
                            weights.Add(-sums[seen[i]]);
                        }
                    }
                }
                for (int i = random.Next(120); i > 0; i--)
                {
                    // Earlier rows come back, as updates and retractions
                    var row = written.Count > 0 && random.Next(3) == 0 ? written[random.Next(written.Count)] : Row(shape, width, random);
                    written.Add(row);
                    rows.Add(row);
                    weights.Add(random.Next(2) == 0 ? -random.Next(1, 4) : random.Next(1, 4));
                }
                if (b == 5)
                {
                    // Over 16 equal rows netting to zero, new and existing
                    var fresh = Row(shape, width, random);
                    var existing = written.Count > 0 ? written[0] : Row(shape, width, random);
                    for (int i = 0; i < 20; i++)
                    {
                        rows.Add(fresh);
                        rows.Add(existing);
                        weights.Add(i % 2 == 0 ? 1 : -1);
                        weights.Add(i % 2 == 0 ? 2 : -2);
                    }
                    // A new key with weight 0 is not stored in bulk
                    rows.Add(Row(shape, width, random));
                    weights.Add(0);
                }
                for (int i = 0; i < rows.Count; i++)
                {
                    if (sums.TryAdd(rows[i], weights[i]))
                    {
                        seen.Add(rows[i]);
                    }
                    else
                    {
                        sums[rows[i]] += weights[i];
                    }
                }
                batches.Add((Batch(width, rows), Weights(weights.ToArray())));
            }
            batches.Add((Batch(width, new List<IDataValue[]>()), Weights()));
            // An extra column goes per-row
            var extra = written.Take(10).Select(x => x.Append(new Int64Value(7)).ToArray()).ToList();
            batches.Add((Batch(width + 1, extra), Weights(Enumerable.Repeat(1, extra.Count).ToArray())));
            return (width, batches);
        }

        private static IDataValue[] Row(string shape, int width, Random random)
        {
            var row = new IDataValue[width];
            for (int c = 0; c < width; c++)
            {
                row[c] = (shape, c) switch
                {
                    // Integer widths change across 127, 32767 and 2^31
                    ("flat", 0) => Pick(random, Null, new Int64Value(-1), new Int64Value(5), new Int64Value(127), new Int64Value(128), new Int64Value(32767), new Int64Value(32768), new Int64Value(2147483648L), new Int64Value(long.MaxValue)),
                    ("flat", 1) => Pick(random, Null, new StringValue(""), new StringValue("a"), new StringValue("shared prefix of some length x"), new StringValue("shared prefix of some length y"), new StringValue("ö")),
                    ("flat", 2) => Pick(random, new DoubleValue(-0.0), new DoubleValue(0.0), new DoubleValue(double.NaN), new DoubleValue(double.PositiveInfinity), new DoubleValue(1.5), new Int64Value(3)),
                    ("flat", 3) => Pick(random, Null, new DecimalValue(1.0m), new DecimalValue(1.00m), new DecimalValue(2.5m)),
                    ("flat", _) => Pick(random, new TimestampTzValue(1000, 0), new TimestampTzValue(1000, 60), new TimestampTzValue(2000, 0)),
                    // Mixed types with nulls in one column
                    ("mixed", 0) => Pick(random, Null, new Int64Value(1), new Int64Value(2), new StringValue("1"), new StringValue("a"), new DoubleValue(1.5)),
                    ("mixed", _) => new Int64Value(random.Next(100)),
                    ("nested", 0) => Pick(random, Null, new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k"), Pick(random, Null, new Int64Value(1), new StringValue("v"))))),
                    ("nested", 1) => Pick(random, Null, new ListValue(Pick(random, new Int64Value(1), new StringValue("x")), Pick(random, Null, new Int64Value(2)))),
                    ("nested", _) => Pick(random, Null, Struct(Pick(random, Null, new Int64Value(1), new StringValue("s")), Pick(random, new Int64Value(2), new DoubleValue(2.5)))),
                    // Wide enough keys that pages split
                    _ => Pick(random, Null, Struct(new Int64Value(random.Next(1000)), new StringValue("a wider second field")), Struct(new Int64Value(random.Next(1000)), Null), Struct(new StringValue("a"), Null), Struct(Null, Null))
                };
            }
            return row;
        }

        // A struct with a union child, merged across a null batch
        private static Column MergedStructColumn()
        {
            var parts = new[]
            {
                ColumnOf(Struct(new Int64Value(1), Null), Struct(new StringValue("a"), Null), Struct(Null, Null)),
                ColumnOf(Null),
                ColumnOf(Struct(new Int64Value(2), Null), Struct(new StringValue("b"), Null))
            };
            var merged = Column.Create(GlobalMemoryManager.Instance);
            foreach (var part in parts)
            {
                merged.InsertRangeFrom(merged.Count, part, 0, part.Count);
            }
            return merged;
        }

        private static IDataValue Struct(IDataValue a, IDataValue b) => new StructValue(s_header, a, b);

        private static IDataValue Pick(Random random, params IDataValue[] values) => values[random.Next(values.Length)];

        private static Column ColumnOf(params IDataValue[] values)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                column.Add(value);
            }
            return column;
        }

        private static EventBatchData Batch(int width, List<IDataValue[]> rows)
        {
            var columns = new IColumn[width];
            for (int c = 0; c < width; c++)
            {
                columns[c] = ColumnOf(rows.Select(x => x[c]).ToArray());
            }
            return new EventBatchData(columns);
        }

        private static PrimitiveList<int> Weights(params int[] weights)
        {
            var list = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var weight in weights)
            {
                list.Add(weight);
            }
            return list;
        }
    }
}
