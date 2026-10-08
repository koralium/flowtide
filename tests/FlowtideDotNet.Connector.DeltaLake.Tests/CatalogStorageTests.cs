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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

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

        [Theory]
        [InlineData(10_000)]
        [InlineData(1)]
        public async Task AMigrationSliceStaysWithinItsByteBudget(long budget)
        {
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            }, NullLoggerFactory.Instance, new Meter(nameof(AMigrationSliceStaysWithinItsByteBudget)), nameof(AMigrationSliceStaysWithinItsByteBudget), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var client = stateManager.GetOrCreateClient("catalog");
            var tree = await RotatingTree<DeltaFileRecord>.Open(client, "cold", new DeltaFileRecordSerializer(), GlobalMemoryManager.Instance);

            // Uneven records, some alone take most of the budget
            var random = new Random(9);
            var model = new Dictionary<int, DeltaFileRecord>();
            for (int round = 0; round < 3; round++)
            {
                for (int id = 0; id < 200; id++)
                {
                    model[id] = new DeltaFileRecord() { Path = $"part-{id}-{round}.parquet", Statistics = new string('x', random.Next(100, 4500)), NumRecords = id };
                    await tree.Upsert(id, model[id]);
                }
            }
            tree.RotateIfDue(model.Count, floor: 100);
            Assert.True(tree.Migrating);

            var none = await tree.MigrateSlice(maxRecords: 0, maxBytes: budget);
            Assert.Equal(0, none.Records);
            Assert.True(tree.Migrating);
            // After another tree used part of the budget, a record that does not fit waits
            var waiting = await tree.MigrateSlice(maxRecords: 1000, maxBytes: 1, first: false);
            Assert.Equal(0, waiting.Records);
            Assert.True(tree.Migrating);

            var slices = 0;
            while (tree.Migrating && slices < 1000)
            {
                var work = await tree.MigrateSlice(maxRecords: 1000, maxBytes: budget);
                // A record larger than the whole budget still moves, alone
                Assert.True(work.Records == 1 || work.Bytes <= budget, $"{work.Records} records, {work.Bytes} bytes");
                Assert.True(work.Records >= 1, "a slice made no progress");
                slices++;
            }
            Assert.False(tree.Migrating);
            Assert.Equal(model.Count, tree.RecordsMigrated);
            Assert.True(slices > 1);
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
        public void AFirstColumnThatDoesNotFitRevokesOnlyWhatItLacks()
        {
            var reservation = new PruningReservation();
            var grants = new[] { (Total: 300L, Extra: 200L), (Total: 250L, Extra: 150L), (Total: 200L, Extra: 100L) }.Select(x =>
            {
                var grant = new PruningGrant();
                reservation.Register(grant, 1000);
                Assert.True(reservation.TryCharge(grant, x.Total, firstColumn: true));
                reservation.SetExtraBytes(grant, x.Extra);
                return grant;
            }).ToList();

            // 50 bytes short, the largest extra columns cover it
            var first = new PruningGrant();
            reservation.Register(first, 1000);
            Assert.False(reservation.TryCharge(first, 300, firstColumn: true));
            Assert.Equal(new[] { true, false, false }, grants.Select(x => x.RevokeRequested));

            // Still covered by the revocation already asked for
            Assert.False(reservation.TryCharge(first, 400, firstColumn: true));
            Assert.Equal(new[] { true, false, false }, grants.Select(x => x.RevokeRequested));

            // 350 short, 200 are already coming, the next largest covers the rest
            var second = new PruningGrant();
            reservation.Register(second, 1000);
            Assert.False(reservation.TryCharge(second, 600, firstColumn: true));
            Assert.Equal(new[] { true, true, false }, grants.Select(x => x.RevokeRequested));
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

        [Fact]
        public async Task TreeModeFromARefusedGrowthSurvivesARebootstrap()
        {
            // One 18 byte column: 64 rows fit, growing to 128 holds both arrays at once, a fresh 128 row array fits
            const string table = "catalog_growth_sticky";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(64));
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), PruningMemoryBytes = 3000, StatsPruningColumns = 1 };
            var (stateManager, catalog) = await OpenCatalog(table, options);
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            var readOptions = new DeltaReadOptions() { SkipTombstones = true };
            await catalog.Validate(readOptions);
            Assert.False(catalog.TreeMode);

            // Publishing the 65th file needs the 65th slot
            var add = FileAdd(64);
            await WriteCommit(storage, table, 1, new DeltaAction() { Add = add });
            var overlay = catalog.BeginCommit(1, catalog.AdoptedAt, null);
            overlay.Add(catalog.AllocateId(), add);
            catalog.MarkPublished(overlay);
            await catalog.ApplyPublished();
            Assert.True(catalog.TreeMode);

            // Another writer commits before the same SaveData checks the head
            await WriteCommit(storage, table, 2, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
            await catalog.Validate(readOptions);

            Assert.Equal(2, catalog.Head);
            Assert.Equal(65, catalog.LiveFiles);
            Assert.True(catalog.TreeMode);
        }

        [Theory]
        [InlineData(7, 8 * 1024 * 1024)]
        [InlineData(1000, 1500)]
        public async Task BothCatalogTreesShareOneMigrationBudget(int sliceRecords, long sliceBytes)
        {
            // No room for any column, cold records and bounds both live in trees and rotate together
            var table = $"catalog_shared_budget_{sliceRecords}";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(40));
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), PruningMemoryBytes = 1, CatalogRotationFloor = 10, CatalogMigrationSlice = sliceRecords, CatalogMigrationBytes = sliceBytes };
            var (stateManager, catalog) = await OpenCatalog(table, options);
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            Assert.True(catalog.TreeMode);

            var live = Enumerable.Range(0, 40).ToDictionary(i => i, i => $"part-{i}.parquet");
            var next = 40;
            long largestStep = 0;
            var overBudget = new List<string>();
            for (int version = 1; version <= 40; version++)
            {
                // Constant live count, removed ids come back from the free list
                var overlay = catalog.BeginCommit(version, catalog.AdoptedAt, null);
                foreach (var id in live.Keys.Take(5).ToList())
                {
                    overlay.Removes.Add(id);
                    live.Remove(id);
                }
                for (int i = 0; i < 5; i++)
                {
                    var add = FileAdd(next++);
                    var id = catalog.AllocateId();
                    overlay.Add(id, add);
                    live[id] = add.Path!;
                }
                catalog.MarkPublished(overlay);
                await catalog.ApplyPublished();

                var before = catalog.RecordsMigrated;
                var bytesBefore = catalog.BytesMigrated;
                await catalog.Maintain();
                var step = catalog.RecordsMigrated - before;
                var bytes = catalog.BytesMigrated - bytesBefore;
                largestStep = Math.Max(largestStep, step);
                // Only a single record larger than the whole budget may exceed it
                if (bytes > sliceBytes && step != 1)
                {
                    overBudget.Add($"version {version}: {step} records, {bytes} bytes");
                }
            }

            Assert.True(catalog.Rotations >= 2, $"rotations {catalog.Rotations}");
            Assert.InRange(largestStep, 1, sliceRecords);
            Assert.Empty(overBudget);
            var scanned = new List<string>();
            await foreach (var (_, record) in catalog.ScanFiles())
            {
                scanned.Add(record.Path);
            }
            Assert.Equal(live.Values.Order(), scanned.Order());
        }

        [Fact]
        public async Task BoundsMigrationFinishesWhileTheColdTreeKeepsRotating()
        {
            // 15 files never change and the other 25 are replaced every commit, so a finished cold migration is due again at once
            const string table = "catalog_no_starvation";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(40));
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), PruningMemoryBytes = 1, CatalogRotationFloor = 10, CatalogMigrationSlice = 1 };
            var (stateManager, catalog) = await OpenCatalog(table, options);
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            Assert.True(catalog.TreeMode);

            var live = Enumerable.Range(0, 40).ToDictionary(i => i, i => $"part-{i}.parquet");
            var next = 40;
            for (int version = 1; version <= 120; version++)
            {
                var overlay = catalog.BeginCommit(version, catalog.AdoptedAt, null);
                foreach (var id in live.Keys.Where(x => x >= 15).ToList())
                {
                    overlay.Removes.Add(id);
                    live.Remove(id);
                }
                for (int i = 0; i < 25; i++)
                {
                    var add = FileAdd(next++);
                    var id = catalog.AllocateId();
                    overlay.Add(id, add);
                    live[id] = add.Path!;
                }
                catalog.MarkPublished(overlay);
                await catalog.ApplyPublished();
                await catalog.Maintain();
            }

            Assert.True(catalog.ColdTree.Completed >= 2, $"cold migrations {catalog.ColdTree.Completed}");
            Assert.True(catalog.BoundsTree.Completed >= 2, $"bounds migrations {catalog.BoundsTree.Completed}");
            var scanned = new List<string>();
            await foreach (var (_, record) in catalog.ScanFiles())
            {
                scanned.Add(record.Path);
            }
            Assert.Equal(live.Values.Order(), scanned.Order());
        }

        [Fact]
        public void StatisticsBeyondInt32AndUnreadableBoundsReachTheCatalog()
        {
            var schema = new StructType(new List<StructField>() { new StructField("userkey", new LongType(), true, new Dictionary<string, object>()) });
            var statisticsOptions = new JsonSerializerOptions();
            statisticsOptions.Converters.Add(new DeltaStatisticsConverter(schema));
            var layout = PruningLayout.Create(schema, new[] { "userkey" }, 8);
            var overlay = new CatalogOverlay(1, 1, layout, statisticsOptions, null);

            overlay.Add(0, new DeltaAddAction() { Path = "large.parquet", Statistics = "{\"numRecords\":3000000000,\"minValues\":{\"userkey\":1},\"maxValues\":{\"userkey\":2},\"nullCount\":{\"userkey\":0}}" });
            // A bound outside the column's range prunes nothing instead of failing the commit
            overlay.Add(1, new DeltaAddAction() { Path = "foreign.parquet", Statistics = "{\"numRecords\":5,\"minValues\":{\"userkey\":1e30},\"maxValues\":{\"userkey\":2e30},\"nullCount\":{\"userkey\":0}}" });

            Assert.Equal(3_000_000_000L, overlay.Adds[0].Record.NumRecords);
            Assert.Equal(PruningLayout.HasStatistics, overlay.Adds[0].Row[0]);
            Assert.Equal(0, overlay.Adds[1].Row[0]);
        }

        private static async Task<(StateManagerSync<object> StateManager, DeltaSinkCatalog Catalog)> OpenCatalog(string name, DeltaLakeOptions options)
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, name, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);
            return (stateManager, catalog);
        }

        private static DeltaAction[] TableWithFiles(int count)
        {
            return new[] { Protocol(), Metadata(UserSchema) }.Concat(Enumerable.Range(0, count).Select(i => new DeltaAction() { Add = FileAdd(i) })).ToArray();
        }

        private static DeltaAddAction FileAdd(int key)
        {
            return new DeltaAddAction()
            {
                Path = $"part-{key}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100,
                ModificationTime = 1,
                DataChange = true,
                Statistics = $"{{\"numRecords\":1,\"minValues\":{{\"userkey\":{key}}},\"maxValues\":{{\"userkey\":{key}}},\"nullCount\":{{\"userkey\":0}}}}"
            };
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
