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
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection("Probes")]
    public class DeleteProbes
    {
        private const int RowsPerFile = 50_000;
        private const int SelectionFiles = 10_000;
        // Per request in the latency probe
        private static readonly TimeSpan s_latency = TimeSpan.FromMilliseconds(20);

        private static readonly int[] s_deletes = new[] { 1, 16, 100, 1_000, 10_000, 100_000 };
        private static readonly int[] s_probes = new[] { 1, 100, 10_000, 100_000, 1_000_000 };
        private static readonly string[] s_selections = new[] { "spread", "skewed", "lowcardinality", "anticorrelated", "overlap" };

        private const string StringHeavyInsert = @"
            CREATE TABLE test (
                userkey INT,
                firstname STRING,
                lastname STRING,
                nullablestring STRING,
                companyid STRING,
                trimmable STRING
            );

            INSERT INTO test
            SELECT userKey AS userkey, firstName AS firstname, lastName AS lastname, nullableString AS nullablestring,
                companyId AS companyid, trimmableNullableString AS trimmable FROM users
        ";

        private readonly ITestOutputHelper _output;

        public DeleteProbes(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData(50, "spread")]
        [InlineData(50, "newest")]
        [InlineData(200, "spread")]
        [InlineData(200, "newest")]
        public async Task B2bEndToEnd(int files, string distribution)
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B2b {files} files, {distribution}");
            using var fixture = await ProbeFixture.Build($"b2b_{files}_{distribution}", files, RowsPerFile, kit);
            // Newest is the last 5 % of the files
            var pool = (distribution == "newest" ? fixture.Files.TakeLast((files + 19) / 20) : fixture.Files).SelectMany(x => x).ToList();
            // A shorter list lets old and new alternate
            var list = Environment.GetEnvironmentVariable("FLOWTIDE_PROBE_DELETES")?.Split(',').Select(int.Parse).ToArray() ?? s_deletes;
            foreach (var deletes in list)
            {
                if (!await RunDeletes(kit, fixture, pool, deletes, latency: null))
                {
                    break;
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(8)]
        public async Task B2aSelection(int columns)
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B2a selection at T = {DeleteLimits.Default.SelectorThreshold}, {SelectionFiles} files, {columns} columns");
            await WarmSelection(columns);
            foreach (var selection in s_selections)
            {
                // With one column anticorrelated is the same work as spread
                if (columns == 1 && selection == "anticorrelated")
                {
                    continue;
                }
                using var catalog = await SelectionCatalog.Create(SelectionFiles, columns, selection);
                var random = new Random(columns);
                foreach (var probes in s_probes)
                {
                    var name = $"P={columns} {selection} B={probes}";
                    using var stored = catalog.Probes(probes, random);
                    var batch = StoredProbes.Build(catalog.Catalog.ScanLayout, stored);
                    var samples = new List<ProbeSample>();
                    var candidateFiles = 0;
                    var finished = true;
                    for (int run = 0; run <= ProbeKit.Runs && finished; run++)
                    {
                        var allocated = GC.GetTotalAllocatedBytes(true);
                        var watch = Stopwatch.StartNew();
                        var candidates = await CandidateSelector.Select(catalog.Catalog, batch, DeleteLimits.Default.SelectorThreshold);
                        watch.Stop();
                        candidateFiles = candidates.Count;
                        finished = watch.Elapsed <= ProbeKit.DidNotFinish;
                        if (run > 0)
                        {
                            samples.Add(new ProbeSample(watch.Elapsed.TotalMilliseconds, 0, 0, 0, 0, 0, GC.GetTotalAllocatedBytes(true) - allocated, 0));
                        }
                    }
                    if (!finished)
                    {
                        kit.Write($"{name} | did not finish within {ProbeKit.DidNotFinish.TotalSeconds:0} s, larger B not run");
                        break;
                    }
                    kit.Write($"{name} | wall {ProbeKit.Spread(samples, x => x.WallMs)} ms | allocated {ProbeKit.Spread(samples, x => x.Allocated / 1048576.0)} MiB" +
                        $" | candidate files {candidateFiles}");
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(8)]
        public async Task B2aSelectionAtScale(int columns)
        {
            // One column count per invocation keeps each inside its time limit
            if (!ProbeKit.Enabled || (int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_PROBE_COLUMNS"), out var only) && only != columns))
            {
                return;
            }
            // The whole selection over an array projection, list included
            var kit = new ProbeKit(_output, $"B2a synthetic selection, {columns} columns, build and select over the projection");
            var names = Enumerable.Range(0, columns).Select(x => $"c{x}").ToList();
            var table = new Internal.Delta.Schema.Types.StructType(names.Select(x => new Internal.Delta.Schema.Types.StructField(x, new Internal.Delta.Schema.Types.LongType(), true, new Dictionary<string, object>())).ToList());
            var layout = PruningLayout.Create(table, names, columns);
            foreach (var files in new[] { 10_000, 100_000, 1_000_000 })
            {
                foreach (var selection in s_selections)
                {
                    if (columns == 1 && selection == "anticorrelated")
                    {
                        continue;
                    }
                    using var projection = await SyntheticProjection.Create(layout, files, selection);
                    var random = new Random(columns);
                    foreach (var probes in s_probes)
                    {
                        var name = $"F={files} P={columns} {selection} B={probes}";
                        using var stored = StoredProbes.Store(table, SyntheticProbes(files, columns, selection, probes, random));
                        var entries = Enumerable.Range(0, probes).ToArray();
                        // Every threshold at 10k files picks T, the default elsewhere
                        var thresholds = files == 10_000 ? new[] { 8, 16, 32, 64, int.MaxValue } : new[] { DeleteLimits.Default.SelectorThreshold };
                        var finished = true;
                        foreach (var threshold in thresholds)
                        {
                            var build = new List<double>();
                            var select = new List<double>();
                            var candidates = 0;
                            for (int run = 0; run <= ProbeKit.Runs && finished; run++)
                            {
                                var watch = Stopwatch.StartNew();
                                var batch = ProbeBatch.Build(layout, stored, entries);
                                var built = watch.Elapsed.TotalMilliseconds;
                                var selected = await CandidateSelector.Select(projection.Projection.Scan, batch, threshold);
                                watch.Stop();
                                candidates = selected.Count;
                                finished = watch.Elapsed <= ProbeKit.DidNotFinish;
                                if (run > 0)
                                {
                                    build.Add(built);
                                    select.Add(watch.Elapsed.TotalMilliseconds - built);
                                }
                            }
                            if (!finished)
                            {
                                kit.Write($"{name} T={(threshold == int.MaxValue ? "exact" : threshold)} | did not finish within {ProbeKit.DidNotFinish.TotalSeconds:0} s, larger B not run");
                                break;
                            }
                            kit.Write($"{name} T={(threshold == int.MaxValue ? "exact" : threshold)} | build {ProbeKit.Spread(build)} ms | select {ProbeKit.Spread(select)} ms" +
                                $" | total {ProbeKit.Median(build.Zip(select, (x, y) => x + y)):0.0} ms | candidates {candidates}");
                        }
                        if (!finished)
                        {
                            break;
                        }
                    }
                }
            }
        }

        // An array-mode projection holding the synthetic rows
        private sealed class SyntheticProjection : IDisposable
        {
            private readonly ReservoirPersistentStorage _state;
            private readonly StateManagerSync _stateManager;

            private SyntheticProjection(ReservoirPersistentStorage state, StateManagerSync stateManager, PruningProjection projection)
            {
                _state = state;
                _stateManager = stateManager;
                Projection = projection;
            }

            public PruningProjection Projection { get; }

            public static async Task<SyntheticProjection> Create(PruningLayout layout, int files, string selection)
            {
                var state = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
                var stateManager = new StateManagerSync<object>(new StateManagerOptions() { CachePageCount = 1000, PersistentStorage = state },
                    NullLoggerFactory.Instance, new Meter("b2a_synthetic"), "b2a_synthetic", GlobalMemoryManager.Instance);
                await stateManager.InitializeAsync();
                var tree = await stateManager.GetOrCreateClient("catalog").GetOrCreateEphemeralTree("bounds", DeltaSinkCatalog.TreeOptions(new BoundsRowSerializer(), GlobalMemoryManager.Instance));
                // Room for every column once the array doubles
                var projection = PruningProjection.Create(GlobalMemoryManager.Instance, new PruningReservation(), 2L * files * layout.RowSize, tree, layout, files, treeMode: false, out _);
                var rows = SyntheticRows(layout, files, selection);
                for (int id = 0; id < files; id++)
                {
                    await projection.Set(id, rows.AsMemory(id * layout.RowSize, layout.RowSize));
                }
                if (projection.TreeMode || projection.Layout.Columns.Count != layout.Columns.Count)
                {
                    throw new InvalidOperationException("The synthetic projection dropped columns or spilled to its tree");
                }
                return new SyntheticProjection(state, stateManager, projection);
            }

            public void Dispose()
            {
                Projection.Dispose();
                _stateManager.Dispose();
                _state.Dispose();
            }
        }

        // The selection catalog's bounds, one row per file
        private static byte[] SyntheticRows(PruningLayout layout, int files, string selection)
        {
            var rows = new byte[(long)files * layout.RowSize];
            for (int file = 0; file < files; file++)
            {
                var row = rows.AsSpan(file * layout.RowSize, layout.RowSize);
                row[0] = PruningLayout.HasStatistics;
                for (int c = 0; c < layout.Columns.Count; c++)
                {
                    var (min, max) = SyntheticBounds(file, c, files, selection);
                    PruningCell.WriteInt64(row.Slice(layout.Columns[c].Offset, PruningCell.CellSize(PruningType.Int64)), PruningType.Int64, min, max, 0);
                }
            }
            return rows;
        }

        // Spread hits one file, skewed the newest 5 %, anticorrelated another file's second column
        private static List<IDataValue[]> SyntheticProbes(int files, int columns, string selection, int count, Random random)
        {
            var probes = new List<IDataValue[]>(count);
            for (int i = 0; i < count; i++)
            {
                var file = selection == "skewed" ? files - 1 - random.Next(files / 20) : random.Next(files);
                var values = new IDataValue[columns];
                for (int c = 0; c < columns; c++)
                {
                    var target = selection == "anticorrelated" && c == 1 ? (file + 1 + random.Next(files - 1)) % files : file;
                    var (min, max) = SyntheticBounds(target, c, files, selection);
                    values[c] = new Int64Value(min + random.NextInt64(max - min + 1));
                }
                probes.Add(values);
            }
            return probes;
        }

        private static (long Min, long Max) SyntheticBounds(int file, int column, int files, string selection)
        {
            const long width = 1_000;
            return selection switch
            {
                "lowcardinality" when column == 0 => (file % 4, file % 4),
                "anticorrelated" when column == 1 => ((files - 1 - file) * width, (files - 1 - file) * width + width - 1),
                "overlap" => (0, files * width),
                _ => (file * width, file * width + width - 1)
            };
        }

        // Discarded selections, so the first measured one is not cold
        private static async Task WarmSelection(int columns)
        {
            using var catalog = await SelectionCatalog.Create(SelectionFiles, columns, "spread");
            using var stored = catalog.Probes(100, new Random(0));
            var batch = StoredProbes.Build(catalog.Catalog.ScanLayout, stored);
            for (int i = 0; i < 50; i++)
            {
                await CandidateSelector.Select(catalog.Catalog, batch, DeleteLimits.Default.SelectorThreshold);
            }
        }

        [Fact]
        public async Task B3RequestLatency()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B3 50 files, spread, {s_latency.TotalMilliseconds:0} ms per request");
            using var fixture = await ProbeFixture.Build("b3_50_spread", 50, RowsPerFile, kit);
            var pool = fixture.Files.SelectMany(x => x).ToList();
            foreach (var deletes in new[] { 16, 1_000 })
            {
                if (!await RunDeletes(kit, fixture, pool, deletes, s_latency))
                {
                    break;
                }
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task B4InsertOnly(bool stringHeavy, bool changeData)
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var rows = stringHeavy ? "string-heavy" : "narrow";
            var kit = new ProbeKit(_output, $"B4 1M {rows} rows, change data {(changeData ? "on" : "off")}");
            var samples = new List<ProbeSample>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                // A new table each run, so every run starts alike
                await using var table = await ProbeTable.Start($"b4_{(stringHeavy ? "strings" : "narrow")}_{(changeData ? "cdf" : "plain")}",
                    stringHeavy ? StringHeavyInsert : UserInsert, options => options.WriteChangeDataOnNewTables = changeData);
                var sample = await table.Measure(() => table.Stream.GenerateUsers(1_000_000));
                if (sample == null)
                {
                    kit.Write($"insert 1M | did not finish within {ProbeKit.DidNotFinish.TotalSeconds:0} s");
                    return;
                }
                if (run > 0)
                {
                    samples.Add(sample.Value);
                }
            }
            kit.Report("insert 1M", samples);
        }

        // Each run deletes from its own copy, then inserts back
        private static async Task<bool> RunDeletes(ProbeKit kit, ProbeFixture fixture, List<User> pool, int deletes, TimeSpan? latency)
        {
            var users = Sample(pool, deletes, new Random(deletes));
            var removed = new List<ProbeSample>();
            var inserted = new List<ProbeSample>();
            var matching = new List<DeleteStatistics>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                await using var table = await fixture.Open(latency);
                var delete = await table.Measure(() => table.Stream.DeleteUsers(users));
                ProbeSample? insert = null;
                if (delete != null)
                {
                    if (run > 0)
                    {
                        matching.Add(table.Stream.Sinks[0].DeleteStatisticsForTests!);
                    }
                    // Every delete matched, and the insert starts on an idle stream
                    await ExpectRows(table, fixture.Rows - deletes);
                    await table.Settle();
                    insert = await table.Measure(() => table.Stream.AddUsers(users));
                }
                if (delete == null || insert == null)
                {
                    kit.Write($"D={deletes} | did not finish within {ProbeKit.DidNotFinish.TotalSeconds:0} s, larger D not run");
                    return false;
                }
                await ExpectRows(table, fixture.Rows);
                if (run > 0)
                {
                    removed.Add(delete.Value);
                    inserted.Add(insert.Value);
                }
            }
            var shares = latency is { } l
                ? $" | latency share {ProbeKit.Spread(removed, x => x.Requests * l.TotalMilliseconds * 100 / x.WallMs)} % | dv request latency share {ProbeKit.Spread(removed, x => x.DvWrites * l.TotalMilliseconds * 100 / x.WallMs)} %"
                : "";
            kit.Report($"D={deletes} delete", removed, shares);
            ReportMatching(kit, $"D={deletes}", matching);
            kit.Report($"D={deletes} insert", inserted);
            var pairs = Enumerable.Range(0, removed.Count).ToList();
            kit.Write($"D={deletes} | delete minus insert, per run | wall {ProbeKit.Spread(pairs.Select(i => removed[i].WallMs - inserted[i].WallMs).ToList())} ms" +
                $" | sink {ProbeKit.Spread(pairs.Select(i => removed[i].SinkMs - inserted[i].SinkMs).ToList())} ms" +
                $" | cpu {ProbeKit.Spread(pairs.Select(i => removed[i].CpuMs - inserted[i].CpuMs).ToList())} ms");
            return true;
        }

        // Counts from the last run, tick shares over the runs
        internal static void ReportMatching(ProbeKit kit, string name, List<DeleteStatistics> runs)
        {
            static double Ms(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;
            static long Scan(DeleteStatistics x) => x.OpenTicks + x.ReadTicks + x.PrecisionTicks + x.HashTicks;
            var last = runs[^1];
            string Share(Func<DeleteStatistics, long> part) => ProbeKit.Spread(runs.Select(x => Scan(x) == 0 ? 0 : part(x) * 100.0 / Scan(x)).ToList());
            kit.Write($"{name} matching | passes {last.Passes} | files {last.FilesScanned}, claimed {last.FilesClaimed} | rows scanned {last.RowsScanned}, hashed {last.RowsHashed}, skipped {last.RowsSkipped}" +
                $" | hits {last.Hits} | claims {last.Claims} | vectors read {last.VectorsRead} | row groups raw-read {last.RowGroupsRawRead}, raw levels {last.RawLevelsRead} | claim-free vector bytes {last.PeakClaimFreeVectorBytes}");
            kit.Write($"{name} scan split | scan {ProbeKit.Spread(runs.Select(x => Ms(Scan(x))).ToList())} ms | open {Share(x => x.OpenTicks)} % | read and decode {Share(x => x.ReadTicks)} %" +
                $" | precision {Share(x => x.PrecisionTicks)} % | hash and probe {Share(x => x.HashTicks)} %" +
                $" | per candidate {ProbeKit.Spread(runs.Select(x => x.FilesScanned == 0 ? 0 : Ms(Scan(x)) / x.FilesScanned).ToList())} ms");
        }

        private static async Task ExpectRows(ProbeTable table, long expected)
        {
            var rows = await table.LiveRows();
            if (rows != expected)
            {
                throw new InvalidOperationException($"The table holds {rows} live rows, expected {expected}");
            }
        }

        private static List<User> Sample(List<User> pool, int count, Random random)
        {
            var picked = new HashSet<int>();
            while (picked.Count < count)
            {
                picked.Add(random.Next(pool.Count));
            }
            return picked.Select(x => pool[x]).ToList();
        }

        // A catalog over synthetic files with patterned bounds
        private sealed class SelectionCatalog : IDisposable
        {
            private const long Width = 1_000;

            private readonly IFileStorage _storage;
            private readonly ReservoirPersistentStorage _state;
            private readonly StateManagerSync _stateManager;
            private readonly int _files;
            private readonly int _columns;
            private readonly string _selection;

            private SelectionCatalog(IFileStorage storage, ReservoirPersistentStorage state, StateManagerSync stateManager, DeltaSinkCatalog catalog, int files, int columns, string selection)
            {
                _storage = storage;
                _state = state;
                _stateManager = stateManager;
                Catalog = catalog;
                _files = files;
                _columns = columns;
                _selection = selection;
            }

            public DeltaSinkCatalog Catalog { get; }

            public static async Task<SelectionCatalog> Create(int files, int columns, string selection)
            {
                var table = $"b2a_{columns}_{selection}";
                var names = Enumerable.Range(0, columns).Select(x => $"c{x}").ToList();
                var schema = "{\"type\":\"struct\",\"fields\":[" + string.Join(",", names.Select(x => $"{{\"name\":\"{x}\",\"type\":\"long\",\"nullable\":true,\"metadata\":{{}}}}")) + "]}";
                var actions = new List<DeltaAction>() { Protocol(), Metadata(schema) };
                for (int f = 0; f < files; f++)
                {
                    var bounds = names.Select((_, c) => Bounds(f, c, files, selection)).ToList();
                    var min = string.Join(",", names.Select((x, c) => $"\"{x}\":{bounds[c].Min}"));
                    var max = string.Join(",", names.Select((x, c) => $"\"{x}\":{bounds[c].Max}"));
                    var nulls = string.Join(",", names.Select(x => $"\"{x}\":0"));
                    actions.Add(new DeltaAction()
                    {
                        Add = new DeltaAddAction()
                        {
                            Path = $"part-{f}.parquet",
                            PartitionValues = new Dictionary<string, string>(),
                            Size = 1_000_000,
                            ModificationTime = 1,
                            DataChange = true,
                            Statistics = $"{{\"numRecords\":{RowsPerFile},\"minValues\":{{{min}}},\"maxValues\":{{{max}}},\"nullCount\":{{{nulls}}}}}"
                        }
                    });
                }
                var storage = Files.Of.InternalMemory($"./{table}");
                await WriteCommit(storage, table, 0, actions.ToArray());
                var state = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
                var stateManager = new StateManagerSync<object>(new StateManagerOptions() { CachePageCount = 100_000, PersistentStorage = state },
                    NullLoggerFactory.Instance, new Meter(table), table, GlobalMemoryManager.Instance);
                await stateManager.InitializeAsync();
                var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation() };
                var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, table, names, GlobalMemoryManager.Instance, NullLogger.Instance);
                await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
                return new SelectionCatalog(storage, state, stateManager, catalog, files, columns, selection);
            }

            private static (long Min, long Max) Bounds(int file, int column, int files, string selection)
            {
                return selection switch
                {
                    "lowcardinality" when column == 0 => (file % 4, file % 4),
                    "anticorrelated" when column == 1 => ((files - 1 - file) * Width, (files - 1 - file) * Width + Width - 1),
                    "overlap" => (0, files * Width),
                    _ => (file * Width, file * Width + Width - 1)
                };
            }

            // Spread hits one file, skewed the newest 5 %, as the delete writer stores them
            public Apache.Arrow.RecordBatch Probes(int count, Random random)
            {
                var rows = new List<IDataValue[]>(count);
                for (int i = 0; i < count; i++)
                {
                    var values = new IDataValue[_columns];
                    var file = _selection == "skewed" ? _files - 1 - random.Next(_files / 20) : random.Next(_files);
                    for (int c = 0; c < _columns; c++)
                    {
                        var target = _selection == "anticorrelated" && c == 1 ? (file + 1 + random.Next(_files - 1)) % _files : file;
                        var (min, max) = Bounds(target, c, _files, _selection);
                        values[c] = new Int64Value(min + random.NextInt64(max - min + 1));
                    }
                    rows.Add(values);
                }
                var table = new Internal.Delta.Schema.Types.StructType(Enumerable.Range(0, _columns)
                    .Select(c => new Internal.Delta.Schema.Types.StructField($"c{c}", new Internal.Delta.Schema.Types.LongType(), true, new Dictionary<string, object>())).ToList());
                return StoredProbes.Store(table, rows);
            }

            public void Dispose()
            {
                Catalog.Dispose();
                _stateManager.Dispose();
                _state.Dispose();
                _storage.Dispose();
            }
        }
    }
}
