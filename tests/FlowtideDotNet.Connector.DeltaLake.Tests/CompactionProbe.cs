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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using Stowage.Impl;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Fixture builds and measured runs, one per process
    internal static class CompactionProbe
    {
        private const string Manifest = "manifest.json";
        private static readonly DateTimeOffset s_start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_commitLimit = TimeSpan.FromMinutes(5);

        // Workload commits after the base, the table time, and what built it
        internal sealed record FixtureManifest(string Fixture, int Commits, long ClockTicks, string Shape);

        internal sealed record RunSettings(int Commits, int Discard, double Pace, bool AzureLike, bool Source, string Variant = "off", int Rep = 1);

        // One commit's numbers; the last five stay null until later PRs
        private sealed record CommitRecord(int Index, long Version, double CheckpointMs, double CommitVersionMs, double SinkMs, double IterationMs,
            int Requests, int MaintenanceRequests, long BytesRead, int DvWrites, int Passes, long Candidates, long FilesScanned, long RowsScanned,
            double SourceMs, int Gen2, int AddedFiles, int RemovedFiles, int RewriteFiles, long RewriteRows,
            long? SettledCandidates, long? CompactionRequests, long? Wakes, string? GuardState, long? CursorSlices);

        public static async Task<int> BuildAsync(IReadOnlyDictionary<string, string> options, TextWriter output)
        {
            if (!CompactionFixture.All.TryGetValue(options.GetValueOrDefault("fixture", ""), out var fixture) || !options.TryGetValue("root", out var root))
            {
                output.WriteLine("A known --fixture and --root are required");
                return ManualRun.HarnessError;
            }
            return await BuildAsync(fixture, root, output);
        }

        public static async Task<int> BuildAsync(CompactionFixture fixture, string root, TextWriter output)
        {
            var baseDir = Path.Combine(root, "base-" + fixture.Base);
            if (!Complete(baseDir, fixture.Base, 0, Shape(fixture, false)))
            {
                await Publish(baseDir, partial => BuildBase(fixture, partial, output));
            }
            var dir = Path.Combine(root, fixture.Name);
            if (fixture.PreAge > 0 && !Complete(dir, fixture.Name, fixture.PreAge, Shape(fixture, true)))
            {
                var start = ReadManifest(baseDir);
                await Publish(dir, async partial =>
                {
                    CopyFixture(baseDir, partial);
                    await RunCommits(fixture, partial, start, fixture.PreAge, output, null);
                });
            }
            return ManualRun.Passed;
        }

        public static async Task<int> RunAsync(IReadOnlyDictionary<string, string> options, TextWriter output)
        {
            var error = Validate(options, out var fixture, out var run);
            if (error != null)
            {
                output.WriteLine(error);
                return ManualRun.HarnessError;
            }
            return await RunAsync(fixture!, options["root"], options.GetValueOrDefault("scratch", Path.GetTempPath()), run!, output);
        }

        public static async Task<int> RunAsync(CompactionFixture fixture, string root, string scratch, RunSettings run, TextWriter output)
        {
            var source = Path.Combine(root, fixture.PreAge > 0 ? fixture.Name : "base-" + fixture.Base);
            if (!Complete(source, fixture.PreAge > 0 ? fixture.Name : fixture.Base, fixture.PreAge, Shape(fixture, fixture.PreAge > 0)))
            {
                output.WriteLine($"{source} is missing or was built by other code, build it again");
                return ManualRun.HarnessError;
            }
            // Sweeps a killed run's copy before free space is counted
            var runs = new ScratchDirectory(Path.Combine(scratch, "compaction-runs"));
            var dir = Path.Combine(runs.ProcessPath, "run");
            try
            {
                var linked = Linkable(source, runs.ProcessPath);
                var needed = ProjectedBytes(source, fixture, run.Commits, linked) + (1L << 30);
                var free = FreeBytes(scratch);
                if (free < needed)
                {
                    output.WriteLine($"Scratch has {free / 1e9:F1} GB free, the run needs {needed / 1e9:F1} GB");
                    return ManualRun.HarnessError;
                }
                CopyFixture(source, dir);
                Write(output, new { record = "copy", linked });
                await RunCommits(fixture, dir, ReadManifest(source), run.Commits, output, run);
            }
            finally
            {
                Directory.Delete(runs.ProcessPath, true);
                runs.ReleaseLockForTests();
            }
            return ManualRun.Passed;
        }

        // Every option is checked before a fixture is touched
        private static string? Validate(IReadOnlyDictionary<string, string> options, out CompactionFixture? fixture, out RunSettings? run)
        {
            fixture = null;
            run = null;
            var name = options.GetValueOrDefault("fixture", "");
            if (!CompactionFixture.All.TryGetValue(name, out fixture))
            {
                return $"Unknown fixture '{name}'";
            }
            if (!options.ContainsKey("root"))
            {
                return "--root is required";
            }
            var store = options.GetValueOrDefault("store", "local");
            var source = options.GetValueOrDefault("source", "on");
            var variant = options.GetValueOrDefault("variant", "off");
            if (store is not ("local" or "azure-like"))
            {
                return $"Unknown store '{store}', use local or azure-like";
            }
            if (source is not ("on" or "off"))
            {
                return $"Unknown source mode '{source}', use on or off";
            }
            if (variant is not ("off" or "on" or "placebo"))
            {
                return $"Unknown variant '{variant}', use off, on or placebo";
            }
            if (variant != "off" || options.ContainsKey("cold-after"))
            {
                return $"Variant {variant} and cold-after need compaction, which later PRs build";
            }
            if (!int.TryParse(options.GetValueOrDefault("commits", "600"), CultureInfo.InvariantCulture, out var commits) ||
                !int.TryParse(options.GetValueOrDefault("discard", "60"), CultureInfo.InvariantCulture, out var discard) ||
                !double.TryParse(options.GetValueOrDefault("pace", "1"), CultureInfo.InvariantCulture, out var pace) ||
                !int.TryParse(options.GetValueOrDefault("rep", "1"), CultureInfo.InvariantCulture, out var rep))
            {
                return "commits, discard, pace and rep must be numbers";
            }
            if (discard < 0 || commits <= discard || !double.IsFinite(pace) || pace < 0 || pace > 3_600 || rep < 1)
            {
                return $"Need 0 <= discard < commits, 0 <= pace <= 3600 and rep >= 1, got {discard}, {commits}, {pace} and {rep}";
            }
            run = new RunSettings(commits, discard, pace, store == "azure-like", source == "on", variant, rep);
            return null;
        }

        private static async Task BuildBase(CompactionFixture fixture, string dir, TextWriter output)
        {
            var clock = new ProbeClock(s_start);
            long version = -1;
            await using var table = new ProbeTable(CompactionFixture.Table, dir, x => Configure(x, fixture, clock, v => Interlocked.Exchange(ref version, v)), keepFiles: true);
            var users = Users(table);
            var workload = new CompactionWorkload(fixture);
            var started = Stopwatch.GetTimestamp();
            var commit = 0;
            foreach (var rows in workload.Base())
            {
                clock.Advance();
                if (commit == 0)
                {
                    users.AddOrUpdate(rows);
                    await table.Stream.StartStream(fixture.Sql());
                    await WaitForVersion(table.Storage, CompactionFixture.Table, table.Stream, 0, s_commitLimit);
                }
                else
                {
                    await Commit(table, () => users.AddOrUpdate(rows), () => Interlocked.Read(ref version));
                }
                await table.Stream.Maintenance.TickOnceAsync(s_commitLimit);
                if (++commit % 500 == 0)
                {
                    Write(output, new { record = "build", fixture = fixture.Base, commit, seconds = Stopwatch.GetElapsedTime(started).TotalSeconds });
                }
            }
            await table.Settle();
            WriteManifest(dir, new FixtureManifest(fixture.Base, 0, clock.GetUtcNow().UtcTicks, Shape(fixture, false)));
        }

        // Pre-ages the fixture when run is null, else measures
        private static async Task RunCommits(CompactionFixture fixture, string dir, FixtureManifest start, int commits, TextWriter output, RunSettings? run)
        {
            var clock = new ProbeClock(new DateTimeOffset(start.ClockTicks, TimeSpan.Zero));
            var timings = new ConcurrentDictionary<long, (TimeSpan Checkpoint, TimeSpan CommitVersion)>();
            long version = -1;
            long timed = -1;
            await using var table = new ProbeTable(CompactionFixture.Table, dir, x =>
            {
                Configure(x, fixture, clock, v => Interlocked.Exchange(ref version, v));
                if (run?.AzureLike == true)
                {
                    x.DeleteLimitsOverride!.ReadAhead = 8;
                }
            }, keepFiles: true, objectStore: run?.AzureLike == true, timed: (checkpoint, commitVersion) =>
            {
                // A new version means this checkpoint published it
                var published = Interlocked.Read(ref version);
                if (published != timed)
                {
                    timed = published;
                    timings[published] = (checkpoint, commitVersion);
                }
            });
            var users = Users(table);
            var workload = new CompactionWorkload(fixture);
            workload.Skip(start.Commits);
            using var log = Files.Of.LocalDisk(dir);
            var head = (await DeltaTransactionReader.ReadTable(log, CompactionFixture.Table))!;
            var files = new LiveFiles(head.AddFiles);
            var applied = head.Version;
            var pruned = 0;
            await Open(table, fixture);
            await using var reader = run?.Source == true ? await SourceReader.Start(dir) : null;
            if (run?.AzureLike == true)
            {
                table.Latency = TimeSpan.FromMilliseconds(20);
                table.Storage.BytesPerSecond = 100L * 1000 * 1000;
            }
            var records = new List<CommitRecord>();
            var started = Stopwatch.GetTimestamp();
            var window = started;
            var overruns = 0;
            for (int i = 0; i < commits; i++)
            {
                var iteration = Stopwatch.GetTimestamp();
                if (i == run?.Discard)
                {
                    window = iteration;
                }
                clock.Advance();
                var (inserts, deletes) = workload.Next();
                var gen2 = GC.CollectionCount(2);
                void Change()
                {
                    users.Delete(deletes);
                    users.AddOrUpdate(inserts);
                }
                if (run == null)
                {
                    await Commit(table, Change, () => Interlocked.Read(ref version));
                    await table.Stream.Maintenance.TickOnceAsync(s_commitLimit);
                    if ((i + 1) % 100 == 0)
                    {
                        Write(output, new { record = "pre-age", fixture = fixture.Name, commit = i + 1, seconds = Stopwatch.GetElapsedTime(started).TotalSeconds });
                    }
                    continue;
                }
                var sample = await table.Measure(Change, s_commitLimit) ?? throw new TimeoutException($"Commit {i} of {fixture.Name} did not finish in {s_commitLimit}");
                var published = Interlocked.Read(ref version);
                var statistics = table.Stream.Sinks[0].DeleteStatisticsForTests;
                var sourceMs = reader != null ? (await reader.WaitFor(published)).TotalMilliseconds : 0;
                table.Storage.ClearRequests();
                await table.Stream.Maintenance.TickOnceAsync(s_commitLimit);
                var maintenanceRequests = table.Storage.Requests.Count;
                var (checkpoint, commitVersion) = await Timing(timings, published);
                var iterationMs = Stopwatch.GetElapsedTime(iteration).TotalMilliseconds;
                // Every new version, read before pruning and outside the timed parts
                var added = 0;
                var removedFiles = 0;
                var writtenFiles = 0;
                long writtenRows = 0;
                for (var v = applied + 1; v <= published; v++)
                {
                    var commit = (await DeltaTransactionReader.ReadVersionCommit(log, CompactionFixture.Table, v))!;
                    var removed = commit.RemovedFiles.Select(x => x.Path).ToHashSet(StringComparer.Ordinal);
                    var written = commit.AddedFiles.Where(x => x.DataChange && !removed.Contains(x.Path)).ToList();
                    files.Apply(commit);
                    added += commit.AddedFiles.Count;
                    removedFiles += commit.RemovedFiles.Count;
                    writtenFiles += written.Count;
                    writtenRows += written.Sum(NumRecords);
                }
                records.Add(new CommitRecord(i, published, checkpoint.TotalMilliseconds, commitVersion.TotalMilliseconds, sample.SinkMs, iterationMs,
                    sample.Requests, maintenanceRequests, sample.BytesRead, sample.DvWrites, statistics?.Passes ?? 0, statistics?.Candidates ?? 0, statistics?.FilesScanned ?? 0, statistics?.RowsScanned ?? 0,
                    sourceMs, GC.CollectionCount(2) - gen2, added, removedFiles, Math.Max(0, writtenFiles - 1), writtenRows - fixture.Inserts,
                    null, null, null, null, null));
                if (published / 20 > applied / 20)
                {
                    PruneLog(dir, published + 1);
                    pruned += files.Prune(dir, clock.GetUtcNow().ToUnixTimeMilliseconds() - 100_000);
                }
                applied = published;
                Write(output, new { record = "commit", value = records[^1] });
                var wait = TimeSpan.FromSeconds(run.Pace) - Stopwatch.GetElapsedTime(iteration);
                if (wait > TimeSpan.Zero)
                {
                    await Task.Delay(wait);
                }
                else if (i >= run.Discard)
                {
                    overruns++;
                }
            }
            var elapsed = Stopwatch.GetElapsedTime(window);
            table.Latency = null;
            table.Storage.BytesPerSecond = 0;
            await table.Settle();
            if (run == null)
            {
                WriteManifest(dir, new FixtureManifest(fixture.Name, start.Commits + commits, clock.GetUtcNow().UtcTicks, Shape(fixture, true)));
                return;
            }
            // Named by the storage the sink used, not by the option
            var store = table.Storage is ILocalDiskFileStorage ? "local" : "azure-like";
            Write(output, await Summary(fixture, run, store, dir, records.Skip(run.Discard).ToList(), elapsed, overruns, pruned, workload.LiveRows, reader?.Rows()));
        }

        private static void Configure(DeltaLakeOptions options, CompactionFixture fixture, ProbeClock clock, Action<long> published)
        {
            options.TimeProvider = clock;
            options.EnableDeletionVectorsOnNewTables = fixture.DeletionVectors;
            // The hook store lists completely, like a real object store
            options.AssumeCompleteStorageListing = true;
            options.CommitVersionHookForTests = (version, _, _) =>
            {
                if (version != null)
                {
                    published(version.Value);
                }
            };
        }

        private static MockTable Users(ProbeTable table)
        {
            return table.Stream.MockDatabaseForTests.GetOrCreateTable<User>("users");
        }

        // An unmeasured commit, checkpointed as soon as the rows are in
        private static async Task Commit(ProbeTable table, Action change, Func<long> version)
        {
            var target = version() + 1;
            change();
            var watch = Stopwatch.StartNew();
            while (version() < target)
            {
                if (watch.Elapsed > s_commitLimit)
                {
                    throw new TimeoutException($"Version {target} was not published within {s_commitLimit}");
                }
                _ = table.Stream.TriggerCheckpoint();
                await table.Stream.SchedulerTick();
                await Task.Delay(5);
            }
        }

        // Starts the stream on the built table, with no rows
        private static async Task Open(ProbeTable table, CompactionFixture fixture)
        {
            await table.Stream.StartStream(fixture.Sql());
            var watch = Stopwatch.StartNew();
            while (!table.Ready)
            {
                if (watch.Elapsed > s_commitLimit)
                {
                    throw new TimeoutException($"The catalog of {fixture.Name} was not ready within {s_commitLimit}");
                }
                _ = table.Stream.TriggerCheckpoint();
                await table.Stream.SchedulerTick();
                await Task.Delay(10);
            }
            await table.Settle();
        }

        private static async Task<object> Summary(CompactionFixture fixture, RunSettings run, string store, string dir, List<CommitRecord> records, TimeSpan elapsed, int overruns, int pruned, long expectedRows, long? sourceRows)
        {
            using var storage = Files.Of.LocalDisk(dir);
            var table = await DeltaTransactionReader.ReadTable(storage, CompactionFixture.Table);
            long physicalRows = 0;
            long deletedRows = 0;
            long bytes = 0;
            var debt = new int[6];
            foreach (var add in table!.AddFiles)
            {
                var physical = NumRecords(add);
                var deleted = add.DeletionVector?.Cardinality ?? 0;
                physicalRows += physical;
                deletedRows += deleted;
                bytes += add.Size;
                var share = physical == 0 ? 0 : (double)deleted / physical;
                debt[share == 0 ? 0 : share <= 0.025 ? 1 : share <= 0.05 ? 2 : share <= 0.075 ? 3 : share < 0.10 ? 4 : 5]++;
            }
            var rewrites = records.Select(x => ((double)x.RewriteRows, x.CheckpointMs)).ToList();
            long netFiles = records.Sum(x => x.AddedFiles - x.RemovedFiles);
            long rewriteFiles = records.Sum(x => x.RewriteFiles);
            var (scanMs, scanRows) = DuckDbScan(dir);
            var missing = new LiveFiles(table.AddFiles).Used().Count(x => !File.Exists(Path.Combine(dir, CompactionFixture.Table, x)));
            var log = new DirectoryInfo(Path.Combine(dir, CompactionFixture.Table, "_delta_log"));
            var checkpoint = log.GetFiles("*.checkpoint.parquet").OrderByDescending(x => x.Name, StringComparer.Ordinal).FirstOrDefault();
            var perHour = 3_600.0 / records.Count;
            return new
            {
                record = "summary",
                fixture = fixture.Name,
                variant = run.Variant,
                rep = run.Rep,
                store,
                sourceStore = "local",
                pace = run.Pace,
                discard = run.Discard,
                commits = records.Count,
                achievedCommitsPerSecond = records.Count / elapsed.TotalSeconds,
                overruns,
                checkpoint = Spread(records.Select(x => x.CheckpointMs).ToList()),
                commitVersion = Spread(records.Select(x => x.CommitVersionMs).ToList()),
                sink = Spread(records.Select(x => x.SinkMs).ToList()),
                iteration = Spread(records.Select(x => x.IterationMs).ToList()),
                source = Spread(records.Select(x => x.SourceMs).ToList()),
                keepsUpShare = records.Count(x => x.IterationMs <= Math.Max(run.Pace, 1) * 1_000) / (double)records.Count,
                rowsScanned = ProbeKit.Median(records.Select(x => (double)x.RowsScanned)),
                filesScanned = ProbeKit.Median(records.Select(x => (double)x.FilesScanned)),
                candidates = ProbeKit.Median(records.Select(x => (double)x.Candidates)),
                candidatesPerPass = ProbeKit.Median(records.Select(x => x.Passes == 0 ? 0 : (double)x.Candidates / x.Passes)),
                requests = ProbeKit.Median(records.Select(x => (double)x.Requests)),
                maintenanceRequests = records.Sum(x => x.MaintenanceRequests),
                gen2 = records.Sum(x => x.Gen2),
                filesPerHour = netFiles * perHour,
                rewriteFilesPerHour = rewriteFiles * perHour,
                debtBuckets = new { none = debt[0], upTo2_5 = debt[1], upTo5 = debt[2], upTo7_5 = debt[3], under10 = debt[4], atLeast10 = debt[5] },
                liveFiles = table.AddFiles.Count,
                bytesPerRow = (double)bytes / physicalRows,
                cRowMicroseconds = Slope(rewrites) * 1_000,
                checkpointBytes = checkpoint?.Length,
                duckDbScanMs = scanMs,
                liveRows = physicalRows - deletedRows,
                expectedLiveRows = expectedRows,
                duckDbRows = scanRows,
                sourceRows,
                missingFiles = missing,
                prunedFiles = pruned
            };
        }

        private static long NumRecords(Internal.Delta.Actions.DeltaAddAction add)
        {
            using var statistics = JsonDocument.Parse(add.Statistics!);
            return statistics.RootElement.GetProperty("numRecords").GetInt64();
        }

        private static object Spread(IReadOnlyList<double> values)
        {
            var (low, high) = ProbeKit.Bootstrap(values, 0.99, 123);
            return new { p50 = ProbeKit.Percentile(values, 0.50), p99 = ProbeKit.Percentile(values, 0.99), p99Low = low, p99High = high };
        }

        // Milliseconds per rewritten row, null when rows never vary
        internal static double? Slope(List<(double Rows, double Ms)> points)
        {
            var meanRows = points.Average(x => x.Rows);
            var meanMs = points.Average(x => x.Ms);
            var spread = points.Sum(x => (x.Rows - meanRows) * (x.Rows - meanRows));
            return spread == 0 ? null : points.Sum(x => (x.Rows - meanRows) * (x.Ms - meanMs)) / spread;
        }

        // Only the cached extension, a run never downloads one
        private static (double? Ms, long? Rows) DuckDbScan(string dir)
        {
            if (!File.Exists(DuckDbTimeTravelProbe.CachedExtension))
            {
                return (null, null);
            }
            using var connection = new DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:");
            connection.Open();
            using var load = connection.CreateCommand();
            load.CommandText = $"SET autoinstall_known_extensions = false; SET autoload_known_extensions = false; SET extension_directory = '{DuckDbTimeTravelProbe.ExtensionDirectory.Replace("\\", "/")}'; LOAD delta;";
            load.ExecuteNonQuery();
            using var scan = connection.CreateCommand();
            scan.CommandText = $"SELECT count(*), sum(length(fullname)), sum(score) FROM delta_scan('{Path.Combine(dir, CompactionFixture.Table).Replace("\\", "/")}')";
            var watch = Stopwatch.StartNew();
            var rows = Convert.ToInt64(scan.ExecuteScalar(), CultureInfo.InvariantCulture);
            return (watch.Elapsed.TotalMilliseconds, rows);
        }

        private static void Write(TextWriter output, object record)
        {
            output.WriteLine(JsonSerializer.Serialize(record));
        }

        private static FixtureManifest ReadManifest(string dir)
        {
            return JsonSerializer.Deserialize<FixtureManifest>(File.ReadAllText(Path.Combine(dir, Manifest)))!;
        }

        private static void WriteManifest(string dir, FixtureManifest manifest)
        {
            File.WriteAllText(Path.Combine(dir, Manifest), JsonSerializer.Serialize(manifest));
        }

        private static bool Complete(string dir, string fixture, int commits, string shape)
        {
            if (!File.Exists(Path.Combine(dir, Manifest)))
            {
                return false;
            }
            var manifest = ReadManifest(dir);
            return manifest.Fixture == fixture && manifest.Commits == commits && manifest.Shape == shape;
        }

        // Changes with the columns, rows and sizes, or the pre-age workload
        internal static string Shape(CompactionFixture fixture, bool preAged)
        {
            var text = new StringBuilder(fixture.Sql()).Append($"{fixture.BaseRows},{fixture.Inserts},{fixture.DeletionVectors}");
            foreach (var key in Enumerable.Range(1, 16))
            {
                text.Append(JsonSerializer.Serialize(ProbeRows.Create(key)));
            }
            if (preAged)
            {
                var workload = new CompactionWorkload(fixture);
                workload.Skip(0);
                text.AppendJoin(',', workload.Next().Deletes.Select(x => x.UserKey));
            }
            return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())));
        }

        // The sink reports a commit's times just after publishing it
        private static async Task<(TimeSpan Checkpoint, TimeSpan CommitVersion)> Timing(ConcurrentDictionary<long, (TimeSpan Checkpoint, TimeSpan CommitVersion)> timings, long version)
        {
            var watch = Stopwatch.StartNew();
            (TimeSpan, TimeSpan) timing;
            while (!timings.TryGetValue(version, out timing))
            {
                if (watch.Elapsed > s_commitLimit)
                {
                    throw new TimeoutException($"The sink never timed version {version}");
                }
                await Task.Delay(1);
            }
            return timing;
        }

        // Drops read versions behind the last checkpoint
        internal static void PruneLog(string dir, long readBefore = long.MaxValue)
        {
            var log = Path.Combine(dir, CompactionFixture.Table, "_delta_log");
            var last = Path.Combine(log, "_last_checkpoint");
            if (!File.Exists(last))
            {
                return;
            }
            using var checkpoint = JsonDocument.Parse(File.ReadAllText(last));
            var kept = Math.Min(checkpoint.RootElement.GetProperty("version").GetInt64(), readBefore);
            foreach (var file in Directory.EnumerateFiles(log))
            {
                var name = Path.GetFileName(file);
                if (name.Length > 20 && long.TryParse(name.AsSpan(0, 20), NumberStyles.None, CultureInfo.InvariantCulture, out var version) && version < kept)
                {
                    File.Delete(file);
                }
            }
        }

        // Builds in a fresh folder, moved into place after the manifest
        private static async Task Publish(string dir, Func<string, Task> build)
        {
            var partial = dir + ".partial";
            foreach (var stale in new[] { partial, dir })
            {
                if (Directory.Exists(stale))
                {
                    Directory.Delete(stale, true);
                }
            }
            Directory.CreateDirectory(partial);
            await build(partial);
            PruneLog(partial);
            await Vacuum(partial);
            Directory.Move(partial, dir);
        }

        // Data files are linked, the log is copied, the manifest skipped
        private static void CopyFixture(string from, string to)
        {
            Directory.CreateDirectory(to);
            foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(from, file);
                if (relative == Manifest)
                {
                    continue;
                }
                var target = Path.Combine(to, relative);
                Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                if (DataFile(Path.GetRelativePath(Path.Combine(from, CompactionFixture.Table), file)))
                {
                    LinkOrCopy(file, target);
                }
                else
                {
                    File.Copy(file, target);
                }
            }
        }

        // Delta never rewrites a data file, so runs can share them
        internal static void LinkOrCopy(string from, string to)
        {
            if (!TryLink(from, to))
            {
                File.Copy(from, to);
            }
        }

        private static bool TryLink(string from, string to)
        {
            return OperatingSystem.IsWindows() ? CreateHardLink(to, from, IntPtr.Zero) : link(from, to) == 0;
        }

        // One trial link tells whether a run copy shares the fixture's volume
        private static bool Linkable(string source, string scratch)
        {
            var file = Directory.EnumerateFiles(Path.Combine(source, CompactionFixture.Table)).FirstOrDefault(x => DataFile(Path.GetFileName(x)));
            if (file == null)
            {
                return true;
            }
            var probe = Path.Combine(scratch, "link-probe");
            var linked = TryLink(file, probe);
            File.Delete(probe);
            return linked;
        }

        [DllImport("libc", SetLastError = true)]
        private static extern int link(string oldpath, string newpath);

        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool CreateHardLink(string newName, string existing, IntPtr security);

        // Data, deletion vector and change files, by their path in the table
        private static bool DataFile(string relative)
        {
            relative = relative.Replace('\\', '/');
            return relative.StartsWith("part-", StringComparison.Ordinal) || relative.StartsWith("deletion_vector_", StringComparison.Ordinal) || relative.StartsWith("_change_data/", StringComparison.Ordinal);
        }

        // Deletes the data files the head version doesn't use, as vacuum would
        internal static async Task Vacuum(string dir)
        {
            using var storage = Files.Of.LocalDisk(dir);
            var used = new LiveFiles((await DeltaTransactionReader.ReadTable(storage, CompactionFixture.Table))!.AddFiles).Used();
            var root = Path.Combine(dir, CompactionFixture.Table);
            foreach (var file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories).ToList())
            {
                var relative = Path.GetRelativePath(root, file).Replace('\\', '/');
                if (DataFile(relative) && !used.Contains(relative))
                {
                    File.Delete(file);
                }
            }
        }

        // Log, checkpoints, inserts, ~120 unpruned versions and any copied data
        internal static long ProjectedBytes(string source, CompactionFixture fixture, int commits, bool linked)
        {
            var table = Path.Combine(source, CompactionFixture.Table);
            var log = new DirectoryInfo(Path.Combine(table, "_delta_log")).GetFiles();
            var json = log.Where(x => x.Name.EndsWith(".json", StringComparison.Ordinal)).ToList();
            var commit = json.Select(x => x.Length).DefaultIfEmpty().Max();
            var checkpoint = log.Where(x => x.Name.EndsWith(".checkpoint.parquet", StringComparison.Ordinal)).Select(x => x.Length).DefaultIfEmpty().Max();
            var rows = fixture.Inserts * 100L;
            // The fixture's own recent versions say how much each one adds
            var adds = json.Where(x => x.Length < 64 << 20).Select(x => AddedBytes(x.FullName)).DefaultIfEmpty(7 * rows).Average();
            var data = linked ? 0 : new DirectoryInfo(table).EnumerateFiles("*", SearchOption.AllDirectories).Where(x => DataFile(Path.GetRelativePath(table, x.FullName))).Sum(x => x.Length);
            return log.Sum(x => x.Length) + 2 * checkpoint + commits * rows + 120 * (commit + (long)adds) + data;
        }

        // Bytes of new files and new vectors only
        private static long AddedBytes(string commit)
        {
            var actions = File.ReadLines(commit).Select(x => JsonNode.Parse(x)).ToList();
            var removed = actions.Select(x => x?["remove"]?["path"]?.GetValue<string>()).OfType<string>().ToHashSet(StringComparer.Ordinal);
            long bytes = 0;
            foreach (var add in actions.Select(x => x?["add"]).OfType<JsonNode>())
            {
                if (!removed.Contains(add["path"]!.GetValue<string>()))
                {
                    bytes += add["size"]?.GetValue<long>() ?? 0;
                }
                bytes += add["deletionVector"]?["sizeInBytes"]?.GetValue<long>() ?? 0;
            }
            return bytes;
        }

        // Free space on the mount that holds the path
        private static long FreeBytes(string path)
        {
            var full = Path.GetFullPath(path) + Path.DirectorySeparatorChar;
            return DriveInfo.GetDrives()
                .Where(x => x.IsReady && full.StartsWith(Folder(x.RootDirectory.FullName), StringComparison.Ordinal))
                .MaxBy(x => x.RootDirectory.FullName.Length)!.AvailableFreeSpace;
        }

        private static string Folder(string path)
        {
            return path.EndsWith(Path.DirectorySeparatorChar) ? path : path + Path.DirectorySeparatorChar;
        }

        // The files the head version references, and when the others were removed
        private sealed class LiveFiles
        {
            private readonly Dictionary<string, string?> _live = new Dictionary<string, string?>(StringComparer.Ordinal);
            private readonly Dictionary<string, long> _removed = new Dictionary<string, long>(StringComparer.Ordinal);

            public LiveFiles(IEnumerable<Internal.Delta.Actions.DeltaAddAction> adds)
            {
                foreach (var add in adds)
                {
                    _live[Name(add.Path)] = add.DeletionVector?.AbsolutePath;
                }
            }

            // Removes come first, so a vector update keeps its file live
            public void Apply(Internal.Delta.Actions.DeltaCommit commit)
            {
                foreach (var remove in commit.RemovedFiles)
                {
                    var path = Name(remove.Path);
                    _live.Remove(path);
                    _removed[path] = remove.DeletionTimestamp ?? 0;
                    if (remove.DeletionVector?.AbsolutePath is string vector)
                    {
                        _removed[vector] = remove.DeletionTimestamp ?? 0;
                    }
                }
                foreach (var add in commit.AddedFiles)
                {
                    _live[Name(add.Path)] = add.DeletionVector?.AbsolutePath;
                }
            }

            public HashSet<string> Used()
            {
                var used = new HashSet<string>(_live.Keys, StringComparer.Ordinal);
                used.UnionWith(_live.Values.OfType<string>());
                return used;
            }

            // Deletes unused files removed at or before the cutoff
            public int Prune(string dir, long cutoff)
            {
                var used = Used();
                var pruned = 0;
                foreach (var (path, removedAt) in _removed.ToList())
                {
                    if (removedAt <= cutoff && !used.Contains(path) && DataFile(path))
                    {
                        File.Delete(Path.Combine(dir, CompactionFixture.Table, path));
                        _removed.Remove(path);
                        pruned++;
                    }
                }
                return pruned;
            }

            private static string Name(string path)
            {
                return Uri.UnescapeDataString(path);
            }
        }

        // Every column is read, so the source decodes the whole row
        internal static string SourceQuery()
        {
            var filter = string.Join(" OR ", CompactionFixture.ColumnNames.Select(x => $"{x} IS NOT NULL"));
            return $"INSERT INTO output SELECT count(*) FROM {CompactionFixture.Table} WHERE {filter}";
        }

        // A source stream on the table, read while the sink waits
        private sealed class SourceReader : IAsyncDisposable
        {
            private readonly DeltaLakeTestStream _stream;
            private readonly ConcurrentDictionary<long, long> _reads = new ConcurrentDictionary<long, long>();
            private readonly ConcurrentDictionary<long, long> _asks = new ConcurrentDictionary<long, long>();

            private SourceReader(string dir)
            {
                var storage = new HookFileStorage(Files.Of.LocalDisk(dir));
                // A version is read once it exists, asked for before that
                storage.Before = (verb, path) =>
                {
                    if (Version(path) is long version)
                    {
                        (verb == "OpenRead" ? _reads : _asks).TryAdd(version, Stopwatch.GetTimestamp());
                    }
                    return Task.CompletedTask;
                };
                _stream = new DeltaLakeTestStream("source", storage, oneVersionPerCheckpoint: false, configure: x => x.DeltaCheckInterval = TimeSpan.FromMilliseconds(1));
            }

            public static async Task<SourceReader> Start(string dir)
            {
                var reader = new SourceReader(dir);
                await reader._stream.StartStream(SourceQuery());
                await reader._stream.WaitForUpdate();
                return reader;
            }

            // From reading a version until asking for the next one
            public async Task<TimeSpan> WaitFor(long version)
            {
                var watch = Stopwatch.StartNew();
                long start;
                long end;
                while (!_reads.TryGetValue(version, out start) || !_asks.TryGetValue(version + 1, out end))
                {
                    if (watch.Elapsed > s_commitLimit)
                    {
                        throw new TimeoutException($"The source did not read version {version}");
                    }
                    await _stream.SchedulerTick();
                    await Task.Delay(1);
                }
                await _stream.WaitForUpdate();
                return Stopwatch.GetElapsedTime(start, end);
            }

            private static long? Version(IOPath path)
            {
                var name = Path.GetFileName(path.Full);
                return path.Full.Contains("/_delta_log/", StringComparison.Ordinal) && name.Length == 25 && name.EndsWith(".json", StringComparison.Ordinal) &&
                    long.TryParse(name.AsSpan(0, 20), NumberStyles.None, CultureInfo.InvariantCulture, out var version) ? version : null;
            }

            // The row count the source last returned
            public long Rows()
            {
                return _stream.GetActualRowsAsVectors().Columns[0].GetValueAt(0, default).AsLong;
            }

            public async ValueTask DisposeAsync()
            {
                await _stream.DisposeAsync();
            }
        }
    }
}
