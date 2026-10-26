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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
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
        // Probe-file pairs today's candidate lists may hold
        private const long MaxCandidatePairs = 200_000_000;
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
            foreach (var deletes in s_deletes)
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
            // Today's selection, beyond 10k files it is not run
            var kit = new ProbeKit(_output, $"B2a FindCandidates, {SelectionFiles} files, {columns} columns");
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
                double pairsPerProbe = 0;
                double msPerProbe = 0;
                foreach (var probes in s_probes)
                {
                    var name = $"P={columns} {selection} B={probes}";
                    if (pairsPerProbe * probes > MaxCandidatePairs)
                    {
                        kit.Write($"{name} | not run: about {pairsPerProbe * probes:0} probe-file pairs");
                        break;
                    }
                    // The scan tests every probe on every file
                    if (msPerProbe * probes > ProbeKit.DidNotFinish.TotalMilliseconds)
                    {
                        kit.Write($"{name} | not run: predicted over {ProbeKit.DidNotFinish.TotalSeconds:0} s, linear in B from the previous B");
                        break;
                    }
                    var batch = catalog.Probes(probes, random);
                    var samples = new List<ProbeSample>();
                    long pairs = 0;
                    var candidateFiles = 0;
                    var finished = true;
                    for (int run = 0; run <= ProbeKit.Runs && finished; run++)
                    {
                        var allocated = GC.GetTotalAllocatedBytes(true);
                        var watch = Stopwatch.StartNew();
                        var candidates = await catalog.Catalog.FindCandidates(batch);
                        watch.Stop();
                        candidateFiles = candidates.Count;
                        pairs = candidates.Values.Sum(x => (long)x.Count);
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
                        $" | candidate files {candidateFiles} | probe-file pairs {pairs}");
                    pairsPerProbe = (double)pairs / probes;
                    msPerProbe = ProbeKit.Median(samples.Select(x => x.WallMs)) / probes;
                }
            }
        }

        // Discarded selections, so the first measured one is not cold
        private static async Task WarmSelection(int columns)
        {
            using var catalog = await SelectionCatalog.Create(SelectionFiles, columns, "spread");
            var batch = catalog.Probes(100, new Random(0));
            for (int i = 0; i < 50; i++)
            {
                await catalog.Catalog.FindCandidates(batch);
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
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                await using var table = await fixture.Open(latency);
                var delete = await table.Measure(() => table.Stream.DeleteUsers(users));
                ProbeSample? insert = null;
                if (delete != null)
                {
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
            // Today's commit walk resolves 100k deletes per pass
            kit.Report($"D={deletes} delete", removed, $" | passes {(deletes + 99_999) / 100_000} (derived){shares}");
            kit.Report($"D={deletes} insert", inserted);
            var pairs = Enumerable.Range(0, removed.Count).ToList();
            kit.Write($"D={deletes} | delete minus insert, per run | wall {ProbeKit.Spread(pairs.Select(i => removed[i].WallMs - inserted[i].WallMs).ToList())} ms" +
                $" | sink {ProbeKit.Spread(pairs.Select(i => removed[i].SinkMs - inserted[i].SinkMs).ToList())} ms" +
                $" | cpu {ProbeKit.Spread(pairs.Select(i => removed[i].CpuMs - inserted[i].CpuMs).ToList())} ms");
            return true;
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

            // Spread hits one file, skewed the newest 5 %
            public ProbeBatch Probes(int count, Random random)
            {
                var batch = new ProbeBatch(Catalog.ScanLayout);
                var values = new IDataValue[_columns];
                for (int i = 0; i < count; i++)
                {
                    var file = _selection == "skewed" ? _files - 1 - random.Next(_files / 20) : random.Next(_files);
                    for (int c = 0; c < _columns; c++)
                    {
                        var target = _selection == "anticorrelated" && c == 1 ? (file + 1 + random.Next(_files - 1)) % _files : file;
                        var (min, max) = Bounds(target, c, _files, _selection);
                        values[c] = new Int64Value(min + random.NextInt64(max - min + 1));
                    }
                    batch.Add(values);
                }
                return batch;
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
