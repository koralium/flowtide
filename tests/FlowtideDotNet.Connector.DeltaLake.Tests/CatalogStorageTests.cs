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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
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
            var tree = await stateManager.GetOrCreateClient("catalog").GetOrCreateEphemeralTree("bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance));
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
            var tree = await stateManager.GetOrCreateClient("catalog").GetOrCreateEphemeralTree("bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance));
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
                Assert.Equal(rows[id].AsSpan(1, cell).ToArray(), row.Slice(1, cell).ToArray());
                seen.Add(id);
            });
            Assert.Equal(Enumerable.Range(0, 64), seen);
        }

        [Fact]
        public async Task ArrayScanAllocatesNoManagedMemory()
        {
            using var persistence = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                PersistentStorage = persistence
            }, NullLoggerFactory.Instance, new Meter(nameof(ArrayScanAllocatesNoManagedMemory)), nameof(ArrayScanAllocatesNoManagedMemory), GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var tree = await stateManager.GetOrCreateClient("catalog").GetOrCreateEphemeralTree("bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance));
            var layout = new PruningLayout(new[] { new PruningColumn(0, "a", PruningType.Int64, 1) });
            using var projection = PruningProjection.Create(GlobalMemoryManager.Instance, new PruningReservation(), 1L << 20, tree, layout, expectedFiles: 1000, treeMode: false, out _);
            for (int id = 0; id < 1000; id++)
            {
                await projection.Set(id, new byte[layout.RowSize]);
            }
            Assert.False(projection.TreeMode);
            var counter = new VisitCounter();
            for (int i = 0; i < 10; i++)
            {
                Assert.True(projection.Scan(counter.Visit).IsCompletedSuccessfully);
            }

            counter.Count = 0;
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100; i++)
            {
                // Never awaited, an array scan completes synchronously
                Assert.True(projection.Scan(counter.Visit).IsCompletedSuccessfully);
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.Equal(0, allocated);
            Assert.Equal(100_000, counter.Count);
        }

        // The visitor is created once, so the scan loop allocates nothing of its own
        private sealed class VisitCounter
        {
            public int Count;

            public VisitCounter()
            {
                Visit = (_, _) => Count++;
            }

            public BoundsVisitor Visit { get; }
        }

        [Fact]
        public async Task TreeModeFromARefusedGrowthSurvivesARebootstrap()
        {
            // One 18 byte column: 64 rows fit, growing to 128 holds both arrays at once, a fresh 128 row array fits
            const string table = "catalog_growth_sticky";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(64));
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), PruningMemoryBytes = 3000, StatsPruningColumns = 1 };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            var readOptions = new DeltaReadOptions() { SkipTombstones = true };
            await catalog.Validate(readOptions);
            Assert.False(catalog.TreeMode);

            // Publishing the 65th file needs the 65th slot
            var add = FileAdd(64);
            await WriteCommit(storage, table, 1, new DeltaAction() { Add = add });
            var overlay = catalog.BeginCommit(1, null);
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

        [Fact]
        public async Task CatalogTreesNeverWritePersistentState()
        {
            const string table = "catalog_never_persisted";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(40));
            // The manager does not dispose a storage it was given, declared first so it is disposed last
            using var state = new CountingPersistentStorage(new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() }));
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 4,
                MinCachePageCount = 1,
                PersistentStorage = state
            }, NullLoggerFactory.Instance, new Meter(table), table, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            // No room for any column, the bounds also live in a tree
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), PruningMemoryBytes = 1 };

            var s0 = state.Sessions;
            using var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, table, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);
            var catalogSessions = Enumerable.Range(s0 + 1, state.Sessions - s0).ToList();
            Assert.Equal(2, catalogSessions.Count);
            Assert.All(catalogSessions, x => Assert.True(CountingPersistentStorage.IsCatalogSession(state.OriginOf(x)), state.OriginOf(x)));

            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            Assert.True(catalog.TreeMode);
            var live = Enumerable.Range(0, 40).ToList();
            var next = 40;
            for (int version = 1; version <= 40; version++)
            {
                // Constant live count, removed ids come back from the free list
                var overlay = catalog.BeginCommit(version, null);
                foreach (var id in live.Take(5).ToList())
                {
                    overlay.Removes.Add(id);
                    live.Remove(id);
                }
                for (int i = 0; i < 5; i++)
                {
                    var id = catalog.AllocateId();
                    overlay.Add(id, FileAdd(next++));
                    live.Add(id);
                }
                catalog.MarkPublished(overlay);
                await catalog.ApplyPublished();
            }

            // A committed tree on the same manager writes, so a counter that sees nothing fails here
            var c0 = state.Sessions;
            var control = await stateManager.GetOrCreateClient("control").GetOrCreateTree("control", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance));
            var controlSessions = Enumerable.Range(c0 + 1, state.Sessions - c0).ToList();
            await control.Upsert(1, new byte[] { 1, 2, 3 });
            await control.Commit();
            await stateManager.CheckpointAsync();

            Assert.Equal(0, catalogSessions.Sum(state.WritesOfSession));
            Assert.True(controlSessions.Sum(state.WritesOfSession) > 0, "the control tree wrote nothing");
        }

        [Fact]
        public async Task CatalogTreesAreEphemeral()
        {
            const string table = "catalog_ephemeral";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(4));
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation() };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            var client = stateManager.GetOrCreateClient("catalog");

            // Each name, opened again as an ordinary tree, collides with the catalog's ephemeral tree
            var cold = await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.GetOrCreateTree("catalog_cold", DeltaSinkCatalog.TreeOptions(new DeltaFileRecordSerializer(), GlobalMemoryManager.Instance)));
            Assert.Contains("other lifecycle", cold.Message);
            var bounds = await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.GetOrCreateTree("catalog_bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance)));
            Assert.Contains("other lifecycle", bounds.Message);
        }

        [Fact]
        public void CandidateOracleMatchesTheProjectionOnPartialStatistics()
        {
            var schema = new StructType(new List<StructField>()
            {
                new StructField("userkey", new LongType(), true, new Dictionary<string, object>()),
                new StructField("name", new StringType(), true, new Dictionary<string, object>())
            });
            var layout = PruningLayout.Create(schema, new[] { "userkey", "name" }, 8);
            Assert.Equal(2, layout.Columns.Count);
            // Bounds for userkey only, the name cell is written as unknown
            var partial = new DeltaStatistics() { NumRecords = 2, ValueComparers = new Dictionary<string, IStatisticsComparer>() { ["userkey"] = new Int64StatisticsComparer(1, 2, 0) } };
            var cases = new (DeltaStatistics? Statistics, long Key, bool Expected)[]
            {
                (null, 10, true),
                (partial, 10, false),
                (partial, 1, true)
            };
            var probes = new ProbeBatch(layout);
            var row = new byte[layout.RowSize];
            foreach (var (statistics, key, expected) in cases)
            {
                // Every probe has a name, so a row with statistics reads its unknown name cell
                var probe = new IDataValue[] { new Int64Value(key), new StringValue("m") };
                probes.Clear();
                probes.Add(probe);
                layout.WriteRow(statistics, row);
                Assert.Equal(expected, CatalogOracle.MayHold(statistics, layout, probe));
                Assert.Equal(expected, probes.Matches(row, 0));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CatalogMatchesAModelUnderChurnAndSpill(bool treeMode)
        {
            var table = $"catalog_model_{(treeMode ? "tree" : "array")}";
            var storage = Files.Of.InternalMemory($"./{table}");
            var random = new Random(11);
            DeltaAddAction Add(int key) => new DeltaAddAction()
            {
                Path = $"part-{key}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100,
                ModificationTime = 1,
                DataChange = true,
                Statistics = $"{{\"numRecords\":1,\"minValues\":{{\"userkey\":{key},\"name\":\"{new string('a', random.Next(100, 900))}\"}},\"maxValues\":{{\"userkey\":{key},\"name\":\"{new string('z', random.Next(100, 900))}\"}},\"nullCount\":{{\"userkey\":0,\"name\":0}}}}"
            };
            var initial = Enumerable.Range(0, 2000).Select(Add).ToList();
            await WriteCommit(storage, table, 0, new[] { Protocol(), Metadata(UserSchema) }.Concat(initial.Select(x => new DeltaAction() { Add = x })).ToArray());
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation() };
            if (treeMode)
            {
                options.PruningMemoryBytes = 1;
            }
            // Spill is counted through the factory in a directory of its own
            var spill = new WriteCountingFileCacheFactory(new DefaultFileCacheFactory(new FileCacheOptions() { DirectoryPath = $"./data/tempFiles/{table}" }));
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options, cachePages: 16, minCachePages: 4, fileCacheFactory: spill);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            Assert.Equal(treeMode, catalog.TreeMode);
            var statisticsOptions = new JsonSerializerOptions();
            statisticsOptions.Converters.Add(new DeltaStatisticsConverter(catalog.Header!.Schema));
            var expectedRows = new Dictionary<string, byte[]>();

            var model = initial.Select((add, id) => (id, add)).ToDictionary(x => x.id, x => (x.add.Path!, x.add.Statistics!, (string?)null));
            var freed = new HashSet<int>();
            var reused = false;
            var next = 2000;
            for (int version = 1; version <= 40; version++)
            {
                var overlay = catalog.BeginCommit(version, null);
                var removed = model.Keys.OrderBy(_ => random.Next()).Take(150).ToList();
                foreach (var id in removed)
                {
                    overlay.Removes.Add(id);
                    model.Remove(id);
                }
                // A new deletion vector keeps path and statistics, only the tags change here
                var current = await catalog.GetFiles(model.Keys.OrderBy(_ => random.Next()).Take(50).ToList());
                foreach (var (id, record) in current)
                {
                    var add = record.ToAdd();
                    add.Tags = new Dictionary<string, string>() { ["v"] = version.ToString() };
                    overlay.Updates.Add((id, DeltaFileRecord.FromAdd(add, record.NumRecords)));
                    model[id] = (model[id].Item1, model[id].Item2, version.ToString());
                }
                for (int i = 0; i < 150; i++)
                {
                    var id = catalog.AllocateId();
                    reused |= freed.Contains(id);
                    var add = Add(next++);
                    overlay.Add(id, add);
                    model[id] = (add.Path!, add.Statistics!, null);
                }
                catalog.MarkPublished(overlay);
                await catalog.ApplyPublished();
                foreach (var id in removed)
                {
                    freed.Add(id);
                }

                var scanned = new List<(int Id, DeltaFileRecord Record)>();
                await foreach (var entry in catalog.ScanFiles())
                {
                    scanned.Add(entry);
                }
                Assert.Equal(model.Keys.Order(), scanned.Select(x => x.Id));
                foreach (var (id, record) in scanned)
                {
                    Assert.Equal(model[id].Item1, record.Path);
                    Assert.Equal(model[id].Item2, record.Statistics);
                    Assert.Equal(model[id].Item3, record.Tags?.GetValueOrDefault("v"));
                }
                var probe = model.Keys.Where(x => x % 7 == 0).Concat(current.Keys).Concat(removed).Append(-5).Append(int.MaxValue).Distinct().ToList();
                var found = await catalog.GetFiles(probe);
                Assert.Equal(probe.Where(model.ContainsKey).Order(), found.Keys.Order());
                foreach (var (id, record) in found)
                {
                    Assert.Equal(model[id].Item1, record.Path);
                    Assert.Equal(model[id].Item2, record.Statistics);
                    Assert.Equal(model[id].Item3, record.Tags?.GetValueOrDefault("v"));
                }
                Assert.Contains(found.Values, record => record.Tags?.GetValueOrDefault("v") == version.ToString());
                var bounds = new List<(int Id, byte[] Row)>();
                await catalog.ScanBounds((id, row) => bounds.Add((id, row.ToArray())));
                Assert.Equal(model.Keys.Order(), bounds.Select(x => x.Id));
                // Rows read back after a spill equal the rows the statistics give, apart from the storage flag
                foreach (var (id, row) in bounds)
                {
                    if (!expectedRows.TryGetValue(model[id].Item2, out var expected))
                    {
                        expected = new byte[catalog.ScanLayout.RowSize];
                        catalog.ScanLayout.WriteRow(CatalogOverlay.ParseStatistics(model[id].Item2, statisticsOptions), expected);
                        expectedRows[model[id].Item2] = expected;
                    }
                    if ((row[0] & ~PruningProjection.Live) != expected[0] || !row.AsSpan(1).SequenceEqual(expected.AsSpan(1)))
                    {
                        Assert.Fail($"version {version}: bounds row of id {id} is {Convert.ToHexString(row)}, the statistics give {Convert.ToHexString(expected)}");
                    }
                }
            }
            Assert.True(reused, "no freed id was handed out again");
            Assert.True(spill.WritesOf(x => x.Contains("catalog_")) > 0, "the catalog never spilled");
            if (treeMode)
            {
                Assert.True(spill.WritesOf(x => x.EndsWith("catalog_bounds")) > 0, "the bounds tree never spilled");
            }
        }

        [Fact]
        public async Task ReopeningTheCatalogEmptiesBothTrees()
        {
            const string table = "catalog_reopen";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, TableWithFiles(40));
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), PruningMemoryBytes = 1 };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            var client = stateManager.GetOrCreateClient("catalog");
            var readOptions = new DeltaReadOptions() { SkipTombstones = true };
            using (catalog)
            {
                await catalog.Validate(readOptions);
                Assert.True(catalog.TreeMode);
                // A second open of an ephemeral name sees the same tree
                Assert.Equal(40, await CountEntries(await client.GetOrCreateEphemeralTree("catalog_cold", DeltaSinkCatalog.TreeOptions(new DeltaFileRecordSerializer(), GlobalMemoryManager.Instance))));
                Assert.Equal(40, await CountEntries(await client.GetOrCreateEphemeralTree("catalog_bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance))));
            }

            using var reopened = await DeltaSinkCatalog.Open(client, options, table, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);

            Assert.Equal(0, await CountEntries(await client.GetOrCreateEphemeralTree("catalog_cold", DeltaSinkCatalog.TreeOptions(new DeltaFileRecordSerializer(), GlobalMemoryManager.Instance))));
            Assert.Equal(0, await CountEntries(await client.GetOrCreateEphemeralTree("catalog_bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance))));
            await reopened.Validate(readOptions);
            Assert.Equal(40, reopened.LiveFiles);
            var paths = new List<string>();
            await foreach (var (_, record) in reopened.ScanFiles())
            {
                paths.Add(record.Path);
            }
            Assert.Equal(Enumerable.Range(0, 40).Select(x => $"part-{x}.parquet").Order(), paths.Order());
        }

        private static async Task<int> CountEntries<V>(IBPlusTree<int, V, PrimitiveListKeyContainer<int>, ManagedValueContainer<V>> tree)
        {
            var count = 0;
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                foreach (var _ in page)
                {
                    count++;
                }
            }
            return count;
        }

        [Fact]
        public void StatisticsBeyondInt32AndUnreadableBoundsReachTheCatalog()
        {
            var schema = new StructType(new List<StructField>() { new StructField("userkey", new LongType(), true, new Dictionary<string, object>()) });
            var statisticsOptions = new JsonSerializerOptions();
            statisticsOptions.Converters.Add(new DeltaStatisticsConverter(schema));
            var layout = PruningLayout.Create(schema, new[] { "userkey" }, 8);
            var overlay = new CatalogOverlay(1, layout, statisticsOptions, null);

            overlay.Add(0, new DeltaAddAction() { Path = "large.parquet", Statistics = "{\"numRecords\":3000000000,\"minValues\":{\"userkey\":1},\"maxValues\":{\"userkey\":2},\"nullCount\":{\"userkey\":0}}" });
            // A bound outside the column's range prunes nothing instead of failing the commit
            overlay.Add(1, new DeltaAddAction() { Path = "foreign.parquet", Statistics = "{\"numRecords\":5,\"minValues\":{\"userkey\":1e30},\"maxValues\":{\"userkey\":2e30},\"nullCount\":{\"userkey\":0}}" });

            Assert.Equal(3_000_000_000L, overlay.Adds[0].Record.NumRecords);
            Assert.Equal(PruningLayout.HasStatistics, overlay.Adds[0].Row[0]);
            Assert.Equal(0, overlay.Adds[1].Row[0]);
        }

        // The manager does not dispose a storage it was given, the caller disposes it last
        private static async Task<(StateManagerSync<object> StateManager, DeltaSinkCatalog Catalog, ReservoirPersistentStorage Persistence)> OpenCatalog(string name, DeltaLakeOptions options, int cachePages = 64, int minCachePages = 1000, IFileCacheFactory? fileCacheFactory = null)
        {
            var persistence = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = cachePages,
                MinCachePageCount = minCachePages,
                FileCacheFactory = fileCacheFactory,
                PersistentStorage = persistence
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, name, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);
            return (stateManager, catalog, persistence);
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

    }
}
