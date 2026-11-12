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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // P-track: discovery slices over a catalog of a million files
    internal static class CursorTrackProbe
    {
        private const long Hour = 3_600_000;
        // Files are written relative to a fixed probe time
        private const long T = 1_800_000_000_000;
        private const int RoundFiles = 10_000;
        private const double SliceBarMs = 17.9;
        private const int YoungFiles = 10_000;
        private const int DeclinedFiles = 1_000;
        private const int TaggedFiles = 9_000;
        private const int ColdFiles = 50;

        private enum Kind
        {
            Large,
            Fodder,
            Young,
            Declined,
            Tagged,
            Cold
        }

        private sealed record Settings(bool Tree, int Files, int Laps, int Seed);

        public static async Task<int> RunAsync(IReadOnlyDictionary<string, string> options, TextWriter output)
        {
            var error = Validate(options, out var settings);
            if (error != null)
            {
                output.WriteLine(error);
                return ManualRun.HarnessError;
            }
            var scratch = options["scratch"];
            // Sweeps a killed run's spill before free space is counted
            var runs = new ScratchDirectory(Path.Join(scratch, "ptrack-runs"));
            try
            {
                var free = CompactionProbe.FreeBytes(scratch);
                if (free < 3L << 30)
                {
                    output.WriteLine($"Scratch has {free / 1e9:F1} GB free, the run needs 3 GB");
                    return ManualRun.HarnessError;
                }
                return await Run(settings!, runs.ProcessPath, output);
            }
            finally
            {
                try
                {
                    Directory.Delete(runs.ProcessPath, true);
                }
                finally
                {
                    runs.ReleaseLockForTests();
                }
            }
        }

        private static string? Validate(IReadOnlyDictionary<string, string> options, out Settings? settings)
        {
            settings = null;
            var mode = options.GetValueOrDefault("mode", "");
            if (!options.ContainsKey("scratch"))
            {
                return "--scratch is required";
            }
            if (mode is not ("array" or "tree"))
            {
                return "--mode must be array or tree";
            }
            if (!int.TryParse(options.GetValueOrDefault("files", "1000000"), CultureInfo.InvariantCulture, out var files) || files < 100_000 || files > 1 << 20)
            {
                return "--files must be from 100000 to 1048576";
            }
            if (!int.TryParse(options.GetValueOrDefault("laps", mode == "tree" ? "10" : "3"), CultureInfo.InvariantCulture, out var laps) || laps < 1)
            {
                return "--laps must be a positive number";
            }
            if (!int.TryParse(options.GetValueOrDefault("seed", "1"), CultureInfo.InvariantCulture, out var seed))
            {
                return "--seed must be a number";
            }
            settings = new Settings(mode == "tree", files, laps, seed);
            return null;
        }

        private static async Task<int> Run(Settings settings, string root, TextWriter output)
        {
            using var meter = new Meter("ptrack");
            using var persistence = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            // Ephemeral trees spill through the file cache
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                PersistentStorage = persistence,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = Path.Join(root, "spill") }
            }, NullLoggerFactory.Instance, meter, "ptrack", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            using var storage = Files.Of.InternalMemory("./ptrack");
            var limits = new CompactionLimits();
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true, CompactionLimitsOverride = limits };
            if (settings.Tree)
            {
                options.PruningMemoryBytes = 1;
            }
            using var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, "ptrack", CompactionFixture.ColumnNames.ToList(), GlobalMemoryManager.Instance, NullLogger.Instance);
            await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
            var create = catalog.BeginCommit(0, Header());
            catalog.MarkPublished(create);
            await catalog.ApplyPublished();
            var table = new Table(catalog, new Random(settings.Seed));

            var started = Stopwatch.GetTimestamp();
            await table.Build(settings.Files);
            Write(output, new { record = "build", files = settings.Files, seconds = Stopwatch.GetElapsedTime(started).TotalSeconds, treeMode = catalog.TreeMode });
            if (catalog.TreeMode != settings.Tree)
            {
                output.WriteLine($"The catalog is in {(catalog.TreeMode ? "tree" : "array")} mode");
                return ManualRun.HarnessError;
            }

            // (a) Laps over a class no file has
            var remainder = catalog.Cursor(DueRule.Remainder)!;
            var maxCalls = 0;
            var scanMs = new List<double>();
            long lapsInspected = 0;
            long lapsLeaves = 0;
            for (int lap = 0; lap < settings.Laps; lap++)
            {
                var now = Math.Max(T, remainder.PausedUntil);
                var inspected = remainder.Inspected;
                var leaves = remainder.Leaves;
                var lapMs = new List<double>();
                CursorStop stop;
                do
                {
                    var start = Stopwatch.GetTimestamp();
                    stop = await catalog.Discover(DueRule.Remainder, 1 << 1, null, now);
                    lapMs.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                }
                while (stop == CursorStop.Budget && lapMs.Count < 10_000);
                if (stop != CursorStop.LapEnd)
                {
                    output.WriteLine($"Lap {lap} stopped with {stop}");
                    return ManualRun.HarnessError;
                }
                maxCalls = Math.Max(maxCalls, lapMs.Count);
                scanMs.AddRange(lapMs);
                lapsInspected += remainder.Inspected - inspected;
                lapsLeaves += remainder.Leaves - leaves;
                Write(output, new { record = "lap", lap, calls = lapMs.Count, inspected = remainder.Inspected - inspected, leaves = remainder.Leaves - leaves, p50Ms = ProbeKit.Median(lapMs), maxMs = lapMs.Max() });
            }
            var fill = lapsLeaves > 0 ? (double)lapsInspected / lapsLeaves : 0;
            var aPass = maxCalls <= 64;

            // Spaced so 64 reads span about 240 leaves
            var spacing = settings.Tree ? Math.Clamp((int)Math.Floor(3.75 * fill), 1, 512) : 512;
            var kinds = await table.Layout(spacing);
            var declined = kinds.Where(x => x.Value == Kind.Declined).Select(x => x.Key).ToList();
            var declinedRecords = await catalog.GetFiles(declined);
            foreach (var id in declined)
            {
                catalog.Declines!.Decline(id, declinedRecords[id].Path, declinedRecords[id], CompactionReason.Precision);
            }
            if (catalog.Declines!.Count != DeclinedFiles || catalog.Declines.Suspended)
            {
                output.WriteLine($"The memo holds {catalog.Declines.Count} declines, suspended {catalog.Declines.Suspended}");
                return ManualRun.HarnessError;
            }
            Write(output, new { record = "layout", spacing, fill, fodder = kinds.Count(x => x.Value == Kind.Fodder), young = YoungFiles, declined = DeclinedFiles, tagged = TaggedFiles, cold = ColdFiles });

            // (c) The first bin behind young, declined and ineligible files
            var count = catalog.Cursor(DueRule.Count)!;
            var cCalls = 0;
            CursorStop cStop;
            do
            {
                cStop = await catalog.Discover(DueRule.Count, 0b1111, null, T);
                cCalls++;
            }
            while (cStop == CursorStop.Budget && cCalls < 10_000);
            var firstReads = count.Reads;
            var hitIds = count.Hits.Select(x => x.Id).ToHashSet();
            var lastHit = hitIds.Count > 0 ? hitIds.Max() : -1;
            var readBefore = kinds.Count(x => x.Key <= lastHit && x.Value is Kind.Young or Kind.Cold);
            var declinedHits = hitIds.Count(x => kinds.GetValueOrDefault(x) == Kind.Declined);
            var coldHits = hitIds.All(x => kinds.GetValueOrDefault(x) == Kind.Cold);
            var bound = 64 + (YoungFiles + ColdFiles + 63) / 64;
            catalog.Consume(DueRule.Count, hitIds);
            var lapStop = CursorStop.Budget;
            for (int calls = 0; lapStop == CursorStop.Budget && calls < 10_000; calls++)
            {
                lapStop = await catalog.Discover(DueRule.Count, 0b1111, null, T);
            }
            var cChecks = cStop == CursorStop.Full && lapStop == CursorStop.LapEnd && cCalls <= bound && coldHits && declinedHits == 0 && firstReads - readBefore is >= 0 and <= 63 && count.Reads is >= YoungFiles + ColdFiles and <= YoungFiles + ColdFiles + 63;
            Write(output, new { record = "c", stop = cStop.ToString(), calls = cCalls, bound, firstReads, readBefore, declinedHits, lapReads = count.Reads, lapStop = lapStop.ToString(), checks = cChecks });

            // (b) Time the slices that read 64 fresh records
            limits.Hits = 128;
            var fodder = catalog.Cursor(DueRule.Fill)!;
            var samples = new List<double>();
            var sampleLeaves = new List<double>();
            var sampleInspected = new List<double>();
            using var cache = new CacheCounters(meter);
            var gcBefore = Enumerable.Range(0, 3).Select(GC.CollectionCount).ToArray();
            for (int lap = 0; lap <= settings.Laps; lap++)
            {
                // The first lap warms the cache
                if (lap == 1)
                {
                    cache.Mark();
                    gcBefore = Enumerable.Range(0, 3).Select(GC.CollectionCount).ToArray();
                }
                CursorStop stop;
                var calls = 0;
                do
                {
                    var reads = fodder.Reads;
                    var inspected = fodder.Inspected;
                    var leaves = fodder.Leaves;
                    var start = Stopwatch.GetTimestamp();
                    stop = await catalog.Discover(DueRule.Fill, 1 << 4, null, T);
                    var ms = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    if (fodder.Hits.Count > 0)
                    {
                        catalog.Consume(DueRule.Fill, fodder.Hits.Select(x => x.Id).ToHashSet());
                    }
                    if (lap > 0 && fodder.Reads - reads == 64)
                    {
                        samples.Add(ms);
                        sampleLeaves.Add(fodder.Leaves - leaves);
                        sampleInspected.Add(fodder.Inspected - inspected);
                    }
                }
                while (stop == CursorStop.Budget && ++calls < 10_000);
                if (stop != CursorStop.LapEnd)
                {
                    output.WriteLine($"Fodder lap {lap} stopped with {stop}");
                    return ManualRun.HarnessError;
                }
            }
            var (hits, misses) = cache.Read();
            if (samples.Count == 0)
            {
                output.WriteLine("No slice read 64 records");
                return ManualRun.HarnessError;
            }
            var p50 = ProbeKit.Percentile(samples, 0.5);
            var bPass = p50 <= SliceBarMs;
            Write(output, new
            {
                record = "b",
                n = samples.Count,
                p50Ms = p50,
                p50Interval = Interval(samples, 0.5, settings.Seed),
                p95Ms = ProbeKit.Percentile(samples, 0.95),
                p95Interval = Interval(samples, 0.95, settings.Seed),
                p99Ms = ProbeKit.Percentile(samples, 0.99),
                p99Interval = Interval(samples, 0.99, settings.Seed),
                maxMs = samples.Max(),
                leaves = ProbeKit.Median(sampleLeaves),
                inspected = ProbeKit.Median(sampleInspected),
                scanOnlyP50Ms = ProbeKit.Median(scanMs),
                gc = Enumerable.Range(0, 3).Select(x => GC.CollectionCount(x) - gcBefore[x]).ToArray(),
                hits = limits.Hits
            });
            Write(output, new { record = "cache", hits, misses });
            Write(output, new { record = "summary", a_pass = aPass, max_calls = maxCalls, b_pass = settings.Tree ? (object)bPass : "reported", c_calls = cCalls, c_checks = cChecks });
            return !aPass || (settings.Tree && !bPass) ? ManualRun.BarFailed : ManualRun.Passed;
        }

        private static double[] Interval(IReadOnlyList<double> samples, double p, int seed)
        {
            var (low, high) = ProbeKit.Bootstrap(samples, p, seed);
            return new[] { low, high };
        }

        private static void Write(TextWriter output, object record)
        {
            output.WriteLine(JsonSerializer.Serialize(record));
        }

        // The create commit's header with the twenty fixture columns
        private static DeltaTable Header()
        {
            var fields = CompactionFixture.Columns.Select(x => $"{{\"name\":\"{x.Name}\",\"type\":\"{DeltaType(x.Type)}\",\"nullable\":true,\"metadata\":{{}}}}");
            var schemaJson = $"{{\"type\":\"struct\",\"fields\":[{string.Join(",", fields)}]}}";
            var schemaOptions = new JsonSerializerOptions();
            schemaOptions.Converters.Add(new TypeConverter());
            var schema = (StructType)JsonSerializer.Deserialize<SchemaBaseType>(schemaJson, schemaOptions)!;
            return new DeltaTable(Metadata(schemaJson).MetaData!, Protocol().Protocol!, new List<DeltaAddAction>(), schema, 0, Array.Empty<DeltaRemoveFileAction>(), new Dictionary<string, DeltaTransactionAction>(), new Dictionary<string, DeltaDomainMetadataAction>(), null);
        }

        private static string DeltaType(string sqlType)
        {
            return sqlType switch
            {
                "INT" => "integer",
                "DOUBLE" => "double",
                "STRING" => "string",
                "BOOLEAN" => "boolean",
                "TIMESTAMP" => "timestamp",
                _ => sqlType.ToLowerInvariant()
            };
        }

        // Commits go through overlays and never touch the log
        private sealed class Table
        {
            private readonly DeltaSinkCatalog _catalog;
            private readonly Random _random;
            private readonly List<int> _live = new List<int>();
            private long _version;
            private long _serial;

            public Table(DeltaSinkCatalog catalog, Random random)
            {
                _catalog = catalog;
                _random = random;
            }

            // Large files with 30 % churn that reuses freed ids
            public async Task Build(int files)
            {
                while (_live.Count < files)
                {
                    await Commit(Array.Empty<int>(), Math.Min(RoundFiles, files - _live.Count), _ => Kind.Large);
                }
                var removed = 0;
                for (int round = 0; round < files * 3 / 10 / RoundFiles; round++)
                {
                    var removes = new List<int>();
                    for (int i = 0; i < RoundFiles; i++)
                    {
                        var index = _random.Next(_live.Count);
                        removes.Add(_live[index]);
                        _live[index] = _live[^1];
                        _live.RemoveAt(_live.Count - 1);
                    }
                    await Commit(removes, removed, _ => Kind.Large);
                    removed = RoundFiles;
                }
                await Commit(Array.Empty<int>(), removed, _ => Kind.Large);
            }

            // Turns chosen ids into fodder and the (c) mix
            public async Task<Dictionary<int, Kind>> Layout(int spacing)
            {
                var sorted = _live.Order().ToList();
                var kinds = new Dictionary<int, Kind>();
                for (int i = 0; i < sorted.Count; i += spacing)
                {
                    kinds[sorted[i]] = Kind.Fodder;
                }
                var rest = sorted.Where(x => !kinds.ContainsKey(x)).ToArray();
                var mix = Enumerable.Repeat(Kind.Young, YoungFiles).Concat(Enumerable.Repeat(Kind.Declined, DeclinedFiles))
                    .Concat(Enumerable.Repeat(Kind.Tagged, TaggedFiles)).Concat(Enumerable.Repeat(Kind.Cold, ColdFiles)).ToList();
                for (int i = 0; i < mix.Count; i++)
                {
                    var j = i + _random.Next(rest.Length - i);
                    (rest[i], rest[j]) = (rest[j], rest[i]);
                    kinds[rest[i]] = mix[i];
                }
                var chosen = kinds.Keys.ToList();
                _live.RemoveAll(kinds.ContainsKey);
                await Commit(chosen, 0, _ => Kind.Large);
                await Commit(Array.Empty<int>(), chosen.Count, id => kinds[id]);
                return kinds;
            }

            private async Task Commit(IReadOnlyList<int> removes, int adds, Func<int, Kind> kindOf)
            {
                var overlay = _catalog.BeginCommit(++_version, null);
                var records = await _catalog.GetFiles(removes.ToList());
                foreach (var id in removes)
                {
                    overlay.Remove(id, records[id]);
                }
                for (int i = 0; i < adds; i++)
                {
                    var id = _catalog.AllocateId();
                    overlay.Add(id, File(kindOf(id)));
                    _live.Add(id);
                }
                _catalog.MarkPublished(overlay);
                await _catalog.ApplyPublished();
            }

            private DeltaAddAction File(Kind kind)
            {
                var serial = _serial++;
                return new DeltaAddAction()
                {
                    Path = $"part-{serial:D8}.parquet",
                    PartitionValues = new Dictionary<string, string>(),
                    Tags = kind == Kind.Tagged ? new Dictionary<string, string>() { ["t"] = "1" } : null,
                    Size = kind switch { Kind.Large => 32L << 20, Kind.Fodder => 100_000, _ => 20_000 },
                    ModificationTime = kind == Kind.Young ? T - 60_000 : T - 3 * Hour,
                    DataChange = true,
                    Statistics = Statistics(serial * 1_000)
                };
            }
        }

        // Statistics for all twenty columns, about 1.2 KB each
        private static string Statistics(long key)
        {
            var builder = new StringBuilder(1_400);
            builder.Append("{\"numRecords\":1000,\"minValues\":{");
            Values(builder, key);
            builder.Append("},\"maxValues\":{");
            Values(builder, key + 999);
            builder.Append("},\"nullCount\":{");
            var first = true;
            foreach (var (name, _) in CompactionFixture.Columns)
            {
                builder.Append(first ? "" : ",").Append('"').Append(name).Append("\":0");
                first = false;
            }
            return builder.Append("}}").ToString();
        }

        private static void Values(StringBuilder builder, long key)
        {
            var first = true;
            foreach (var (name, type) in CompactionFixture.Columns)
            {
                builder.Append(first ? "" : ",").Append('"').Append(name).Append("\":");
                first = false;
                var value = type switch
                {
                    "INT" => (key % int.MaxValue).ToString(CultureInfo.InvariantCulture),
                    "DOUBLE" or "DECIMAL(18,4)" => (key * 0.25).ToString("0.0###", CultureInfo.InvariantCulture),
                    "BOOLEAN" => key % 2 == 0 ? "false" : "true",
                    "TIMESTAMP" => $"\"{DateTimeOffset.FromUnixTimeMilliseconds(key).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)}\"",
                    _ => $"\"{name}-{key:D12}\""
                };
                builder.Append(value);
            }
        }

        // The state cache's hits and misses since the last mark
        private sealed class CacheCounters : IDisposable
        {
            private readonly MeterListener _listener = new MeterListener();
            private long _hitsAtStart;
            private long _missesAtStart;
            private long _hits;
            private long _misses;

            public CacheCounters(Meter meter)
            {
                _listener.InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter == meter && instrument.Name is "flowtide_lru_table_cache_hits" or "flowtide_lru_table_cache_misses")
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                };
                _listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
                {
                    if (instrument.Name == "flowtide_lru_table_cache_hits")
                    {
                        _hits = value;
                    }
                    else
                    {
                        _misses = value;
                    }
                });
                _listener.Start();
                Mark();
            }

            public void Mark()
            {
                _listener.RecordObservableInstruments();
                _hitsAtStart = _hits;
                _missesAtStart = _misses;
            }

            public (long Hits, long Misses) Read()
            {
                _listener.RecordObservableInstruments();
                return (_hits - _hitsAtStart, _misses - _missesAtStart);
            }

            public void Dispose()
            {
                _listener.Dispose();
            }
        }
    }
}
