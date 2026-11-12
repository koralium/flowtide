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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics.Metrics;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CompactionCatalogTests
    {
        private static readonly long[] s_sizes = new long[] { 0, 100, 8191, 8192, 20_000, 1L << 20, (32L << 20) - 1, 32L << 20, 64L << 20, 200L << 20 };
        // Bits 6 and 7 of a row stay free
        private const byte Reserved = 0xC0;

        [Theory]
        [InlineData(0L, 0)]
        [InlineData(8191L, 0)]
        [InlineData(8192L, 1)]
        [InlineData(16383L, 1)]
        [InlineData(16384L, 2)]
        [InlineData(4194303L, 9)]
        [InlineData(4194304L, 10)]
        [InlineData(33554431L, 12)]
        [InlineData(33554432L, 13)]
        [InlineData(67108863L, 13)]
        [InlineData(67108864L, 14)]
        [InlineData(1099511627776L, 14)]
        [InlineData(-1L, 15)]
        public void SizeClassesDoubleFromEightKiB(long size, int expected)
        {
            Assert.Equal(expected, SizeClass.Of(Record("part-1.parquet", size)));
        }

        [Theory]
        [InlineData("part-1.parquet", 2)]
        [InlineData("sub/part-1.parquet", 2)]
        [InlineData("", 15)]
        [InlineData("/data/part-1.parquet", 15)]
        [InlineData("s3://bucket/part-1.parquet", 15)]
        [InlineData("abfss://c@a.dfs.core.windows.net/part-1.parquet", 15)]
        [InlineData("C:/data/part-1.parquet", 15)]
        [InlineData(@"\\server\share\part-1.parquet", 15)]
        [InlineData(@"\data\part-1.parquet", 15)]
        [InlineData("../other_table/part-1.parquet", 15)]
        [InlineData("sub/../part-1.parquet", 15)]
        [InlineData("./part-1.parquet", 15)]
        [InlineData("sub//part-1.parquet", 15)]
        [InlineData("%2Fdata/part-1.parquet", 15)]
        [InlineData("part%201.parquet", 15)]
        [InlineData("part#1.parquet", 15)]
        [InlineData("part?1.parquet", 15)]
        public void OnlyPlainRelativePathsAreEligible(string path, int expected)
        {
            Assert.Equal(expected, SizeClass.Of(Record(path, 20_000)));
        }

        [Fact]
        public void AnEmptyPathHasNoScheme()
        {
            Assert.Equal(0, PathCanonicalizer.SchemeLength(""));
        }

        [Fact]
        public void RowTrackingTagsClusteringAndPartitionsAreIneligible()
        {
            var empty = Record("part-1.parquet", 20_000, partitionValues: new Dictionary<string, string>(), tags: new Dictionary<string, string>());

            Assert.Equal(2, SizeClass.Of(empty));
            Assert.Equal(15, SizeClass.Of(Record("part-1.parquet", 20_000, partitionValues: new Dictionary<string, string>() { ["p"] = "1" })));
            Assert.Equal(15, SizeClass.Of(Record("part-1.parquet", 20_000, tags: new Dictionary<string, string>() { ["t"] = "1" })));
            Assert.Equal(15, SizeClass.Of(Record("part-1.parquet", 20_000, baseRowId: 5)));
            Assert.Equal(15, SizeClass.Of(Record("part-1.parquet", 20_000, defaultRowCommitVersion: 5)));
            Assert.Equal(15, SizeClass.Of(Record("part-1.parquet", 20_000, clusteringProvider: "liquid")));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CountersEqualAFullScanAfterEveryOperation(bool treeMode)
        {
            var table = $"compaction_counters_{(treeMode ? "tree" : "array")}";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var random = new Random(23);
            var next = 0;
            DeltaAddAction NewAdd() => FileAdd(next++, random);
            var initial = Enumerable.Range(0, 120).Select(_ => NewAdd()).ToList();
            await WriteCommit(storage, table, 0, new[] { Protocol(), Metadata(UserSchema) }.Concat(initial.Select(x => new DeltaAction() { Add = x })).ToArray());
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true };
            if (treeMode)
            {
                options.PruningMemoryBytes = 1;
            }
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            Assert.Equal(treeMode, catalog.TreeMode);
            Assert.True(catalog.CompactionEffective);
            await AssertCountersMatchScan(catalog, "bootstrap");

            var live = Enumerable.Range(0, initial.Count).ToList();
            var version = 0L;
            for (int operation = 0; operation < 1000; operation++)
            {
                var kind = random.Next(100);
                if (kind < 5)
                {
                    await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
                    live = Enumerable.Range(0, initial.Count).ToList();
                    version = 0;
                    await AssertCountersMatchScan(catalog, $"operation {operation}: bootstrap");
                    continue;
                }
                var overlay = catalog.BeginCommit(++version, null);
                if (kind < 10)
                {
                    overlay.RemoveAll = true;
                    catalog.RestartIds();
                    live.Clear();
                    var adds = random.Next(1, 40);
                    for (int i = 0; i < adds; i++)
                    {
                        var id = catalog.AllocateId();
                        overlay.Add(id, NewAdd());
                        live.Add(id);
                    }
                }
                else
                {
                    var touched = live.OrderBy(_ => random.Next()).Take(random.Next(0, 12)).ToList();
                    var records = await catalog.GetFiles(touched);
                    foreach (var id in touched)
                    {
                        if (random.Next(2) == 0)
                        {
                            overlay.Remove(id, records[id]);
                            live.Remove(id);
                        }
                        else
                        {
                            overlay.Updates.Add((id, WithDeletionVector(records[id], operation)));
                        }
                    }
                    var adds = random.Next(0, 12);
                    for (int i = 0; i < adds; i++)
                    {
                        var id = catalog.AllocateId();
                        overlay.Add(id, NewAdd());
                        live.Add(id);
                    }
                }
                catalog.MarkPublished(overlay);
                await catalog.ApplyPublished();
                await AssertCountersMatchScan(catalog, $"operation {operation}");
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TheCreateCommitDecidesLikeABootstrap(bool enabled)
        {
            var table = $"compaction_create_{enabled}";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var random = new Random(3);
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = enabled };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            var before = catalog.CatalogEpoch;

            var overlay = catalog.BeginCommit(0, NewTable());
            for (int i = 0; i < 20; i++)
            {
                overlay.Add(catalog.AllocateId(), FileAdd(i, random));
            }
            catalog.MarkPublished(overlay);
            await catalog.ApplyPublished();

            Assert.True(catalog.CatalogEpoch > before);
            Assert.Equal(enabled, catalog.CompactionEffective);
            await AssertCountersMatchScan(catalog, "create");
            var flags = new List<byte>();
            await catalog.ScanBounds((id, row) => flags.Add(row[0]));
            Assert.Equal(20, flags.Count);
            Assert.Equal(enabled, flags.Any(x => SizeClass.FromFlags(x) != 0));
        }

        [Fact]
        public async Task RowsAndCountersStayAsTodayWithCompactionOff()
        {
            var table = "compaction_off";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var random = new Random(5);
            await WriteCommit(storage, table, 0, new[] { Protocol(), Metadata(UserSchema) }.Concat(Enumerable.Range(0, 30).Select(i => new DeltaAction() { Add = FileAdd(i, random) })).ToArray());
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation() };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            var overlay = catalog.BeginCommit(1, null);
            overlay.Add(catalog.AllocateId(), FileAdd(30, random));
            var removed = (await catalog.GetFiles(new[] { 0 }))[0];
            overlay.Remove(0, removed);
            catalog.MarkPublished(overlay);
            await catalog.ApplyPublished();

            Assert.False(catalog.CompactionEffective);
            Assert.Null(catalog.Declines);
            Assert.Null(catalog.Cursor(DueRule.Count));
            Assert.Equal(CursorStop.Off, await catalog.Discover(DueRule.Count, 1 << 2, null, 0));
            Assert.All(Enumerable.Range(0, SizeClass.Counted), c => Assert.Equal(0, catalog.ClassFiles(c)));
            Assert.Equal(0, overlay.Removes.Single());
            Assert.Null(overlay.RemovedClasses);
            var flags = new List<byte>();
            await catalog.ScanBounds((id, row) => flags.Add(row[0]));
            Assert.All(flags, x => Assert.Equal(0, x & ~(PruningLayout.HasStatistics | PruningProjection.Live)));
            var counterBytes = typeof(DeltaSinkCatalog)
                .GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
                .Select(field => field.GetValue(catalog))
                .OfType<long[]>()
                .Sum(array => array.LongLength * sizeof(long));
            Assert.Equal(0L, counterBytes);
        }

        [Fact]
        public async Task AFailedBootstrapLeavesNothingCounted()
        {
            var table = "compaction_failed_bootstrap";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var valid = new DeltaAddAction() { Path = "part-0.parquet", Size = 20_000, ModificationTime = 1, DataChange = true };
            var broken = new DeltaAddAction() { Path = null, Size = 20_000, ModificationTime = 1, DataChange = true };
            await WriteCommit(storage, table, 0, Protocol(), Metadata(UserSchema), new DeltaAction() { Add = valid }, new DeltaAction() { Add = broken });
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;

            await Assert.ThrowsAsync<InvalidOperationException>(() => catalog.Validate(new DeltaReadOptions() { SkipTombstones = true }));

            Assert.False(catalog.IsReady);
            Assert.False(catalog.CompactionEffective);
            Assert.Equal(0, catalog.LiveFiles);
            Assert.Equal(0, catalog.ClassFiles(2));
            Assert.Equal(0, catalog.ClassBytes(2));
        }

        [Fact]
        public async Task TheEpochRisesWhenIdsOrTheFileSetRestart()
        {
            var table = "compaction_epoch";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var random = new Random(9);
            await WriteCommit(storage, table, 0, new[] { Protocol(), Metadata(UserSchema) }.Concat(Enumerable.Range(0, 5).Select(i => new DeltaAction() { Add = FileAdd(i, random) })).ToArray());
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            var epochs = new List<long>() { catalog.CatalogEpoch };

            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            epochs.Add(catalog.CatalogEpoch);
            var plain = catalog.BeginCommit(1, null);
            plain.Add(catalog.AllocateId(), FileAdd(5, random));
            catalog.MarkPublished(plain);
            await catalog.ApplyPublished();
            var unchanged = catalog.CatalogEpoch;
            var overwrite = catalog.BeginCommit(2, null);
            overwrite.RemoveAll = true;
            catalog.RestartIds();
            epochs.Add(catalog.CatalogEpoch);
            overwrite.Add(catalog.AllocateId(), FileAdd(6, random));
            catalog.MarkPublished(overwrite);
            await catalog.ApplyPublished();
            epochs.Add(catalog.CatalogEpoch);
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            epochs.Add(catalog.CatalogEpoch);
            using var reopened = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("reopened"), options, table, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);
            epochs.Add(reopened.CatalogEpoch);

            Assert.Equal(epochs[1], unchanged);
            Assert.True(epochs.Zip(epochs.Skip(1)).All(x => x.First < x.Second), string.Join(",", epochs));
        }

        [Fact]
        public async Task TheGuardFollowsCatalogChanges()
        {
            var table = "compaction_guard_catalog";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.Zero));
            await WriteCommit(storage, table, 0, Protocol(), Metadata(UserSchema), new DeltaAction() { Add = PlainAdd(0) }, new DeltaAction() { Add = PlainAdd(1) });
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true, TimeProvider = clock };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            var guard = catalog.Guard!;
            guard.AddOutput("part-0.parquet");
            guard.AddOutput("part-1.parquet");

            var removeOne = catalog.BeginCommit(1, null);
            removeOne.Remove(0, (await catalog.GetFiles(new[] { 0 }))[0]);
            catalog.MarkPublished(removeOne);
            await catalog.ApplyPublished();
            Assert.False(guard.IsOutput("part-0.parquet"));
            Assert.True(guard.IsOutput("part-1.parquet"));

            MakeQuiet(guard, clock);
            var overwrite = catalog.BeginCommit(2, null);
            overwrite.RemoveAll = true;
            catalog.RestartIds();
            overwrite.Add(catalog.AllocateId(), PlainAdd(2));
            overwrite.Add(catalog.AllocateId(), PlainAdd(3));
            catalog.MarkPublished(overwrite);
            await catalog.ApplyPublished();
            Assert.False(guard.IsOutput("part-1.parquet"));
            Assert.Equal(clock.GetUtcNow().ToUnixTimeMilliseconds(), guard.QuietSince);
            Assert.Equal(0, guard.Commits);
            Assert.False(guard.IsQuiet(clock.GetUtcNow().ToUnixTimeMilliseconds()));

            guard.AddOutput("part-2.parquet");
            guard.AddOutput("part-3.parquet");
            MakeQuiet(guard, clock);
            await WriteCommit(storage, table, 1, new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "part-0.parquet", DataChange = true, DeletionTimestamp = 1 } });
            await WriteCommit(storage, table, 2, new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "part-1.parquet", DataChange = true, DeletionTimestamp = 2 } }, new DeltaAction() { Add = PlainAdd(2) }, new DeltaAction() { Add = PlainAdd(3) });
            await WriteCommit(storage, table, 3, new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "part-2.parquet", DataChange = true, DeletionTimestamp = 3 } });
            clock.Advance(TimeSpan.FromMinutes(1));
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });

            Assert.Same(guard, catalog.Guard);
            Assert.Equal(clock.GetUtcNow().ToUnixTimeMilliseconds(), guard.QuietSince);
            Assert.Equal(0, guard.Commits);
            Assert.False(guard.IsQuiet(clock.GetUtcNow().ToUnixTimeMilliseconds()));
            Assert.True(guard.IsOutput("part-3.parquet"));
            Assert.False(guard.IsOutput("part-2.parquet"));
        }

        [Fact]
        public async Task ARecreatedTableStartsWithNoOutputs()
        {
            var table = "compaction_guard_recreate";
            using var storage = Files.Of.InternalMemory($"./{table}");
            var random = new Random(17);
            await WriteCommit(storage, table, 0, Protocol(), Metadata(UserSchema), new DeltaAction() { Add = FileAdd(1, random) });
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true };
            var (stateManager, catalog, persistence) = await OpenCatalog(table, options);
            using var persistenceScope = persistence;
            using var stateManagerScope = stateManager;
            using var catalogScope = catalog;
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            catalog.Guard!.AddOutput("part-1.parquet");

            await storage.Rm(CommitPath(table, 0));
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            Assert.Null(catalog.Guard);
            var create = catalog.BeginCommit(0, NewTable());
            create.Add(catalog.AllocateId(), FileAdd(2, random));
            catalog.MarkPublished(create);
            await catalog.ApplyPublished();

            Assert.False(catalog.Guard!.IsOutput("part-1.parquet"));
        }

        // Enough clean commits and time for the default window
        private static void MakeQuiet(CompactionGuard guard, FakeTimeProvider clock)
        {
            for (int i = 0; i < 500; i++)
            {
                guard.CommitStaged(true, guard.Sequence);
            }
            clock.Advance(TimeSpan.FromMinutes(30));
            Assert.True(guard.IsQuiet(clock.GetUtcNow().ToUnixTimeMilliseconds()));
        }

        private static DeltaAddAction PlainAdd(int key)
        {
            return new DeltaAddAction()
            {
                Path = $"part-{key}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 20_000,
                ModificationTime = 1,
                DataChange = true,
                Statistics = $"{{\"numRecords\":1,\"minValues\":{{\"userkey\":{key}}},\"maxValues\":{{\"userkey\":{key}}},\"nullCount\":{{\"userkey\":0}}}}"
            };
        }

        private static async Task AssertCountersMatchScan(DeltaSinkCatalog catalog, string when)
        {
            var mismatch = await CountersMismatch(catalog);
            if (mismatch != null)
            {
                Assert.Fail($"{when}: {mismatch}");
            }
        }

        // Returns null when counters and rows match the records
        private static async Task<string?> CountersMismatch(DeltaSinkCatalog catalog)
        {
            var files = new long[SizeClass.Counted];
            var bytes = new long[SizeClass.Counted];
            var records = new Dictionary<int, DeltaFileRecord>();
            await foreach (var (id, record) in catalog.ScanFiles())
            {
                records[id] = record;
                var sizeClass = SizeClass.Of(record);
                if (catalog.CompactionEffective && sizeClass != SizeClass.Ineligible)
                {
                    files[sizeClass]++;
                    bytes[sizeClass] += record.Size;
                }
            }
            for (int c = 0; c < SizeClass.Counted; c++)
            {
                if (files[c] != catalog.ClassFiles(c) || bytes[c] != catalog.ClassBytes(c))
                {
                    return $"class {c} counts {catalog.ClassFiles(c)} files and {catalog.ClassBytes(c)} bytes, the scan {files[c]} and {bytes[c]}";
                }
            }
            var rows = new Dictionary<int, byte>();
            await catalog.ScanBounds((id, row) => rows[id] = row[0]);
            if (!rows.Keys.Order().SequenceEqual(records.Keys.Order()))
            {
                return $"row ids [{string.Join(",", rows.Keys.Order())}] record ids [{string.Join(",", records.Keys.Order())}]";
            }
            foreach (var (id, record) in records)
            {
                var flags = rows[id];
                var expectedClass = catalog.CompactionEffective ? SizeClass.Of(record) : 0;
                if (SizeClass.FromFlags(flags) != expectedClass || (flags & Reserved) != 0 || ((flags & PruningLayout.HasStatistics) != 0) != (record.Statistics != null))
                {
                    return $"row of id {id} has flags {flags:X2}, its record class {expectedClass} and statistics {record.Statistics != null}";
                }
            }
            return null;
        }

        internal static async Task<(StateManagerSync<object> StateManager, DeltaSinkCatalog Catalog, ReservoirPersistentStorage Persistence)> OpenCatalog(string name, DeltaLakeOptions options, int pageBytes = 32 * 1024)
        {
            var persistence = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 64,
                MinCachePageCount = 1000,
                DefaultBPlusTreePageSizeBytes = pageBytes,
                PersistentStorage = persistence
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
            try
            {
                await stateManager.InitializeAsync();
                var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, name, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);
                return (stateManager, catalog, persistence);
            }
            catch
            {
                stateManager.Dispose();
                persistence.Dispose();
                throw;
            }
        }

        // The header a create commit carries
        internal static DeltaTable NewTable()
        {
            var schemaOptions = new JsonSerializerOptions();
            schemaOptions.Converters.Add(new TypeConverter());
            var schema = (StructType)JsonSerializer.Deserialize<SchemaBaseType>(UserSchema, schemaOptions)!;
            return new DeltaTable(Metadata(UserSchema).MetaData!, Protocol().Protocol!, new List<DeltaAddAction>(), schema, 0, Array.Empty<DeltaRemoveFileAction>(), new Dictionary<string, DeltaTransactionAction>(), new Dictionary<string, DeltaDomainMetadataAction>(), null);
        }

        // Sizes across the classes, and some files never compacted
        private static DeltaAddAction FileAdd(int key, Random random)
        {
            var kind = random.Next(10);
            return new DeltaAddAction()
            {
                Path = kind == 0 ? $"s3://bucket/part-{key}.parquet" : $"part-{key}.parquet",
                PartitionValues = kind == 1 ? new Dictionary<string, string>() { ["p"] = "1" } : new Dictionary<string, string>(),
                Tags = kind == 2 ? new Dictionary<string, string>() { ["t"] = "1" } : null,
                Size = s_sizes[random.Next(s_sizes.Length)],
                ModificationTime = 1,
                DataChange = true,
                Statistics = kind == 3 ? null : $"{{\"numRecords\":1,\"minValues\":{{\"userkey\":{key}}},\"maxValues\":{{\"userkey\":{key}}},\"nullCount\":{{\"userkey\":0}}}}"
            };
        }

        private static DeltaFileRecord WithDeletionVector(DeltaFileRecord record, int operation)
        {
            var add = record.ToAdd();
            add.DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = $"vector{operation}", Offset = 1, SizeInBytes = 10, Cardinality = 1 };
            return DeltaFileRecord.FromAdd(add, record.NumRecords);
        }

        private static DeltaFileRecord Record(string path, long size, Dictionary<string, string>? partitionValues = null, Dictionary<string, string>? tags = null, long? baseRowId = null, long? defaultRowCommitVersion = null, string? clusteringProvider = null)
        {
            return new DeltaFileRecord()
            {
                Path = path,
                Size = size,
                PartitionValues = partitionValues,
                Tags = tags,
                BaseRowId = baseRowId,
                DefaultRowCommitVersion = defaultRowCommitVersion,
                ClusteringProvider = clusteringProvider
            };
        }
    }
}
