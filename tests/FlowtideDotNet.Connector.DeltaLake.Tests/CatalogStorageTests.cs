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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CatalogStorageTests
    {
        [Fact]
        public void EveryRecordFieldSurvivesSerialization()
        {
            var records = new List<DeltaFileRecord>()
            {
                new DeltaFileRecord() { Path = "a.parquet" },
                new DeltaFileRecord()
                {
                    Path = "dir/part-é€𝄞.zstd.parquet",
                    PartitionValues = new Dictionary<string, string>() { ["p"] = "1", ["q"] = null! },
                    Size = long.MaxValue,
                    ModificationTime = -1,
                    Statistics = "{\"numRecords\":3,\"minValues\":{\"col-x\":\"" + new string('s', 5000) + "\"}}",
                    Tags = new Dictionary<string, string>(),
                    DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^", Offset = 1, SizeInBytes = 34, Cardinality = 3_000_000_000 },
                    BaseRowId = 42,
                    DefaultRowCommitVersion = 7,
                    ClusteringProvider = "liquid",
                    NumRecords = 3
                },
                new DeltaFileRecord()
                {
                    Path = "b.parquet",
                    DeletionVector = new DeletionVector() { StorageType = "i", PathOrInlineDv = "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", SizeInBytes = 40, Cardinality = 6 },
                    NumRecords = null
                }
            };
            var serializer = new DeltaFileRecordSerializer();
            var container = serializer.CreateEmpty();
            foreach (var record in records)
            {
                container.Insert(container.Count, record);
            }

            var buffer = new ArrayBufferWriter<byte>();
            serializer.Serialize(buffer, container);
            // Trailing bytes must stay unread
            buffer.Write(new byte[] { 9, 9, 9 });
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            var read = serializer.Deserialize(ref reader);

            Assert.Equal(3, reader.Remaining);
            Assert.Equal(records.Count, read.Count);
            for (int i = 0; i < records.Count; i++)
            {
                Assert.Equal(JsonSerializer.Serialize(records[i]), JsonSerializer.Serialize(read.Get(i)));
            }
        }

        [Fact]
        public async Task RotationKeepsTheLiveRecordsUnderChurnAndSpill()
        {
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 16,
                MinCachePageCount = 4,
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            }, NullLoggerFactory.Instance, new Meter(nameof(RotationKeepsTheLiveRecordsUnderChurnAndSpill)), nameof(RotationKeepsTheLiveRecordsUnderChurnAndSpill), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var client = stateManager.GetOrCreateClient("catalog");
            var tree = await RotatingTree<DeltaFileRecord>.Open(client, "cold", new DeltaFileRecordSerializer(), GlobalMemoryManager.Instance);

            var model = new Dictionary<int, DeltaFileRecord>();
            var random = new Random(11);
            var nextId = 0;
            DeltaFileRecord Record(int id) => new DeltaFileRecord() { Path = $"part-{id}-{random.Next()}.parquet", Statistics = new string('x', random.Next(100, 900)), NumRecords = id };

            for (int i = 0; i < 2000; i++)
            {
                var record = Record(nextId);
                model[nextId] = record;
                await tree.Upsert(nextId++, record);
            }
            await AssertSame(tree, model);

            var rotations = 0;
            for (int round = 0; round < 40; round++)
            {
                // Constant live count: remove some, replace some deletion vectors, add new files
                foreach (var id in model.Keys.OrderBy(_ => random.Next()).Take(150).ToList())
                {
                    model.Remove(id);
                    await tree.Delete(id);
                }
                foreach (var id in model.Keys.OrderBy(_ => random.Next()).Take(50).ToList())
                {
                    var replaced = Record(id);
                    model[id] = replaced;
                    await tree.Upsert(id, replaced);
                }
                for (int i = 0; i < 150; i++)
                {
                    var record = Record(nextId);
                    model[nextId] = record;
                    await tree.Upsert(nextId++, record);
                }

                tree.RotateIfDue(model.Count, floor: 1000);
                if (tree.Migrating)
                {
                    rotations++;
                }
                await tree.MigrateSlice(maxRecords: 150, maxBytes: long.MaxValue);
                await AssertSame(tree, model);
            }

            Assert.True(tree.Rotations >= 2, $"rotations {tree.Rotations}");
            Assert.True(tree.RecordsMigrated > 0);
        }

        [Fact]
        public async Task AMigrationSliceStopsAtItsByteBudget()
        {
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            }, NullLoggerFactory.Instance, new Meter(nameof(AMigrationSliceStopsAtItsByteBudget)), nameof(AMigrationSliceStopsAtItsByteBudget), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var client = stateManager.GetOrCreateClient("catalog");
            var tree = await RotatingTree<DeltaFileRecord>.Open(client, "cold", new DeltaFileRecordSerializer(), GlobalMemoryManager.Instance);

            var model = new Dictionary<int, DeltaFileRecord>();
            for (int round = 0; round < 3; round++)
            {
                for (int id = 0; id < 200; id++)
                {
                    model[id] = new DeltaFileRecord() { Path = $"part-{id}-{round}.parquet", Statistics = new string('x', 1000), NumRecords = id };
                    await tree.Upsert(id, model[id]);
                }
            }
            tree.RotateIfDue(model.Count, floor: 100);
            Assert.True(tree.Migrating);

            var budget = 10 * model[0].ByteSize();
            await tree.MigrateSlice(maxRecords: 1000, maxBytes: budget);
            Assert.InRange(tree.RecordsMigrated, 1, 10);
            Assert.True(tree.Migrating);

            for (int slice = 0; slice < 100 && tree.Migrating; slice++)
            {
                await tree.MigrateSlice(maxRecords: 1000, maxBytes: budget);
            }
            Assert.False(tree.Migrating);
            Assert.Equal(model.Count, tree.RecordsMigrated);
            await AssertSame(tree, model);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task GrowingChargesTheOldAndTheNewArrayDuringTheCopy(bool bothFit)
        {
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            }, NullLoggerFactory.Instance, new Meter(nameof(GrowingChargesTheOldAndTheNewArrayDuringTheCopy)), nameof(GrowingChargesTheOldAndTheNewArrayDuringTheCopy), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var tree = await RotatingTree<byte[]>.Open(stateManager.GetOrCreateClient("catalog"), "bounds", new BoundsRowSerializer(), GlobalMemoryManager.Instance);
            var layout = new PruningLayout(new[] { new PruningColumn(0, "a", PruningType.Int64, 1) });
            var reservation = new PruningReservation();
            // 64 rows fit, doubling holds 64 and 128 rows at once
            var capacity = (64 + 128) * layout.RowSize - (bothFit ? 0 : 1);
            using var projection = PruningProjection.Create(GlobalMemoryManager.Instance, reservation, capacity, tree, layout, expectedFiles: 1, treeMode: false, out _);
            Assert.Equal(64 * layout.RowSize, reservation.Charged);

            for (int id = 0; id <= 64; id++)
            {
                await projection.Set(id, new byte[layout.RowSize]);
            }

            Assert.Equal(!bothFit, projection.TreeMode);
            Assert.Equal(bothFit ? 128 * layout.RowSize : 0, reservation.Charged);
            var ids = new List<int>();
            await projection.Scan((id, _) => ids.Add(id));
            Assert.Equal(Enumerable.Range(0, 65), ids);
        }

        [Fact]
        public async Task ARevocationKeepsTheFirstColumnOfEveryFile()
        {
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            }, NullLoggerFactory.Instance, new Meter(nameof(ARevocationKeepsTheFirstColumnOfEveryFile)), nameof(ARevocationKeepsTheFirstColumnOfEveryFile), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var tree = await RotatingTree<byte[]>.Open(stateManager.GetOrCreateClient("catalog"), "bounds", new BoundsRowSerializer(), GlobalMemoryManager.Instance);
            var cell = PruningCell.CellSize(PruningType.Int64);
            var layout = new PruningLayout(new[] { new PruningColumn(0, "a", PruningType.Int64, 1), new PruningColumn(1, "b", PruningType.Int64, 1 + cell) });
            var reservation = new PruningReservation();
            using var projection = PruningProjection.Create(GlobalMemoryManager.Instance, reservation, 64 * layout.RowSize, tree, layout, expectedFiles: 1, treeMode: false, out _);
            Assert.Equal(2, projection.Layout.Columns.Count);

            var random = new Random(5);
            var rows = new Dictionary<int, byte[]>();
            for (int id = 0; id < 64; id++)
            {
                var row = new byte[layout.RowSize];
                random.NextBytes(row);
                row[0] = PruningLayout.HasStatistics;
                rows[id] = row;
                await projection.Set(id, row);
            }

            // Another table's first column does not fit
            Assert.False(reservation.TryCharge(new PruningGrant(), 1, firstColumn: true));
            projection.ApplyRevocation();

            Assert.Single(projection.Layout.Columns);
            Assert.Equal(64 * projection.Layout.RowSize, reservation.Charged);
            var seen = new List<int>();
            await projection.Scan((id, row) =>
            {
                Assert.Equal(rows[id].AsSpan(1, cell).ToArray(), row.Span.Slice(1, cell).ToArray());
                seen.Add(id);
            });
            Assert.Equal(Enumerable.Range(0, 64), seen);
        }

        private static async Task AssertSame(RotatingTree<DeltaFileRecord> tree, Dictionary<int, DeltaFileRecord> model)
        {
            var scanned = new List<(int Id, DeltaFileRecord Value)>();
            await foreach (var entry in tree.ScanAll())
            {
                scanned.Add(entry);
            }
            Assert.Equal(model.Keys.Order(), scanned.Select(x => x.Id));
            foreach (var (id, value) in scanned)
            {
                Assert.Equal(model[id].Path, value.Path);
                Assert.Equal(model[id].Statistics, value.Statistics);
            }

            var probe = model.Keys.Where(x => x % 7 == 0).Append(-5).Append(int.MaxValue).ToList();
            var found = await tree.Get(probe);
            Assert.Equal(probe.Where(model.ContainsKey).Order(), found.Keys.Order());
            foreach (var (id, value) in found)
            {
                Assert.Equal(model[id].Path, value.Path);
            }
        }
    }
}
