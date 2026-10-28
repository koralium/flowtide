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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using Stowage;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Probes and their checks never run beside each other
    [CollectionDefinition("Probes", DisableParallelization = true)]
    public sealed class ProbesCollection
    {
    }

    // One measured run, sink time ends at the staged commit
    internal readonly record struct ProbeSample(double WallMs, double SinkMs, double CpuMs, int Requests, long BytesRead, int DvWrites, long Allocated, long PeakWorkingSet);

    internal sealed class ProbeKit
    {
        // Measured runs per case, after one warm-up
        public const int Runs = 5;

        public static readonly TimeSpan DidNotFinish = TimeSpan.FromSeconds(30);

#if DEBUG
        private const string Configuration = "Debug, not evidence";
#else
        private const string Configuration = "Release";
#endif

        private readonly ITestOutputHelper _output;
        private readonly string? _file;

        public ProbeKit(ITestOutputHelper output, string probe)
        {
            _output = output;
            _file = Environment.GetEnvironmentVariable("FLOWTIDE_PROBE_OUT");
            Write($"# {probe} | {RuntimeInformation.OSDescription}, {Environment.ProcessorCount} cores, {RuntimeInformation.FrameworkDescription}, {Configuration}");
        }

        public static bool Enabled => Environment.GetEnvironmentVariable("FLOWTIDE_RUN_PROBES") == "1";

        public void Write(string line)
        {
            _output.WriteLine(line);
            // Also kept on disk, a run may not end cleanly
            if (_file != null)
            {
                File.AppendAllText(_file, line + Environment.NewLine);
            }
        }

        // Median and min-max of every metric over the measured runs
        public void Report(string name, IReadOnlyList<ProbeSample> samples, string? extra = null)
        {
            Write($"{name} | wall {Spread(samples, x => x.WallMs)} ms | sink {Spread(samples, x => x.SinkMs)} ms | cpu {Spread(samples, x => x.CpuMs)} ms" +
                $" | requests {Spread(samples, x => x.Requests)} | read {Spread(samples, x => x.BytesRead / 1024.0)} KiB | dv writes {Spread(samples, x => x.DvWrites)}" +
                $" | allocated {Spread(samples, x => x.Allocated / 1048576.0)} MiB | peak working set {Spread(samples, x => x.PeakWorkingSet / 1048576.0)} MiB{extra}");
        }

        public static string Spread(IReadOnlyList<ProbeSample> samples, Func<ProbeSample, double> metric)
        {
            return Spread(samples.Select(metric).ToList());
        }

        public static string Spread(IReadOnlyList<double> values)
        {
            return $"{Median(values):0.#} [{values.Min():0.#}-{values.Max():0.#}]";
        }

        public static double Median(IEnumerable<double> values)
        {
            var sorted = values.Order().ToArray();
            var middle = sorted.Length / 2;
            return sorted.Length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
        }
    }

    // Peak working set above the start, sampled during a run
    internal sealed class PeakWorkingSet : IAsyncDisposable
    {
        private static int s_running;
        private readonly Process _process = Process.GetCurrentProcess();
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private readonly long _start;
        private readonly Task _sampler;
        private long _peak;
        private bool _disposed;

        public PeakWorkingSet()
        {
            Interlocked.Increment(ref s_running);
            _process.Refresh();
            _start = _process.WorkingSet64;
            _peak = _start;
            _sampler = Sample();
        }

        // Samplers not yet stopped, for the harness checks
        public static int Running => Volatile.Read(ref s_running);

        // Valid once disposed
        public long Peak => _peak - _start;

        private async Task Sample()
        {
            while (!_stop.IsCancellationRequested)
            {
                _process.Refresh();
                _peak = Math.Max(_peak, _process.WorkingSet64);
                try
                {
                    await Task.Delay(5, _stop.Token);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            _stop.Cancel();
            await _sampler;
            _process.Refresh();
            _peak = Math.Max(_peak, _process.WorkingSet64);
            _process.Dispose();
            _stop.Dispose();
            Interlocked.Decrement(ref s_running);
        }
    }

    // A built table, every run gets its own copy
    internal sealed class ProbeFixture : IDisposable
    {
        private readonly string _root;

        private ProbeFixture(string name, string root, List<List<User>> files)
        {
            Name = name;
            _root = root;
            Files = files;
        }

        public string Name { get; }

        // The users of each file, in commit order
        public List<List<User>> Files { get; }

        public long Rows => Files.Sum(x => (long)x.Count);

        // One commit of rowsPerFile users per file
        public static async Task<ProbeFixture> Build(string name, int files, int rowsPerFile, ProbeKit? kit)
        {
            var watch = Stopwatch.StartNew();
            var table = new ProbeTable(name, ProbeTable.NewRoot(), null, keepFiles: true);
            var written = new List<List<User>>();
            try
            {
                table.Stream.GenerateUsers(rowsPerFile);
                written.Add(table.Stream.Users.ToList());
                await table.Stream.StartStream(UserInsert.Replace("test", name));
                await WaitForVersion(table.Storage, name, table.Stream, 0, TimeSpan.FromMinutes(5));
                for (int file = 1; file < files; file++)
                {
                    var users = table.Stream.Users;
                    var start = users.Count;
                    _ = await table.Measure(() => table.Stream.GenerateUsers(rowsPerFile), TimeSpan.FromMinutes(5))
                        ?? throw new TimeoutException($"Build commit {file} of {name} did not commit in 5 minutes");
                    written.Add(Enumerable.Range(start, rowsPerFile).Select(x => users[x]).ToList());
                }
                var delta = await DeltaTransactionReader.ReadTable(table.Storage, name);
                kit?.Write($"built {delta!.AddFiles.Count} files of {rowsPerFile} rows in {watch.Elapsed.TotalSeconds:0} s");
            }
            catch
            {
                await table.DisposeAsync();
                Directory.Delete(table.Root, true);
                throw;
            }
            await table.DisposeAsync();
            return new ProbeFixture(name, table.Root, written);
        }

        // A copy of the table with a ready stream
        public async Task<ProbeTable> Open(TimeSpan? latency)
        {
            var root = ProbeTable.NewRoot();
            foreach (var file in Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories))
            {
                var target = Path.Combine(root, Path.GetRelativePath(_root, file));
                Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                File.Copy(file, target);
            }
            var table = new ProbeTable(Name, root, null, keepFiles: false);
            try
            {
                await table.Stream.StartStream(UserInsert.Replace("test", Name));
                // Empty checkpoints read the log before the measured one
                var watch = Stopwatch.StartNew();
                while (!table.Ready)
                {
                    if (watch.Elapsed > ProbeKit.DidNotFinish)
                    {
                        throw new TimeoutException($"The catalog of {Name} was not ready within {ProbeKit.DidNotFinish.TotalSeconds:0} s");
                    }
                    _ = table.Stream.TriggerCheckpoint();
                    await table.Stream.SchedulerTick();
                    await Task.Delay(10);
                }
                await table.Settle();
                table.Latency = latency;
                return table;
            }
            catch
            {
                await table.DisposeAsync();
                throw;
            }
        }

        public void Dispose()
        {
            Directory.Delete(_root, true);
        }
    }

    // A table on local disk with its stream
    internal sealed class ProbeTable : IAsyncDisposable
    {
        private readonly string _name;
        private readonly bool _keepFiles;
        private TaskCompletionSource _published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _target = long.MaxValue;
        private long _sinkStart;
        private double _sinkMs;
        private volatile bool _aborted;
        private volatile bool _ready;
        private bool _disposed;

        public ProbeTable(string name, string root, Action<DeltaLakeOptions>? configure, bool keepFiles)
        {
            _name = name;
            _keepFiles = keepFiles;
            Root = root;
            Storage = HookFileStorage.Wrap(Files.Of.LocalDisk(root));
            Stream = new DeltaLakeSinkStream(name, Storage, options =>
            {
                // The prefilter's measurement runs it off as well
                if (Environment.GetEnvironmentVariable("FLOWTIDE_PROBE_PREFILTER") == "0")
                {
                    options.DeleteLimitsOverride = new Internal.Deletes.DeleteLimits() { Prefilter = false };
                }
                configure?.Invoke(options);
                var previous = options.CommitVersionHookForTests;
                // After the commit file is written and the stage removed
                options.CommitVersionHookForTests = (version, outcome, target) =>
                {
                    previous?.Invoke(version, outcome, target);
                    if (version >= Volatile.Read(ref _target) && outcome is PublishOutcome.Published or PublishOutcome.AlreadyPublished or PublishOutcome.Repaired)
                    {
                        _published.TrySetResult();
                    }
                };
            });
            Stream.WaitForUpdateDoesNotRequireDataChange();
            // Without normalization deletes reach the sink on a new stream
            Stream.SourceImmutable();
            // Users pick their company from these
            Stream.GenerateCompanies(1);

            // The sink calls it only once the catalog is read
            DeltaLakeSink.CatalogHooksForTests[name] = _ =>
            {
                _ready = true;
                Interlocked.Exchange(ref _sinkStart, Stopwatch.GetTimestamp());
                return Task.CompletedTask;
            };
            Storage.Before = (verb, path) =>
            {
                if (_aborted)
                {
                    return Task.FromException(new IOException("The probe stopped this checkpoint"));
                }
                if (verb == "OpenWrite" && path.Full.Contains("/_delta_log/.", StringComparison.Ordinal) && path.Full.EndsWith(".tmp", StringComparison.Ordinal))
                {
                    var start = Interlocked.Exchange(ref _sinkStart, 0);
                    if (start != 0)
                    {
                        Volatile.Write(ref _sinkMs, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                    }
                }
                return Latency is { } latency ? Task.Delay(latency) : Task.CompletedTask;
            };
        }

        public string Root { get; }

        public HookFileStorage Storage { get; }

        public DeltaLakeSinkStream Stream { get; }

        // Added to every storage request
        public TimeSpan? Latency { get; set; }

        public bool Ready => _ready;

        public static string NewRoot()
        {
            var root = Path.Combine(Path.GetTempPath(), "probe-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            return root;
        }

        // A new table with one row already committed
        public static async Task<ProbeTable> Start(string name, string sql, Action<DeltaLakeOptions>? configure)
        {
            var table = new ProbeTable(name, NewRoot(), configure, keepFiles: false);
            try
            {
                table.Stream.GenerateUsers(1);
                await table.Stream.StartStream(sql.Replace("test", name));
                await WaitForVersion(table.Storage, name, table.Stream, 0, TimeSpan.FromMinutes(5));
                await table.Settle();
                return table;
            }
            catch
            {
                await table.DisposeAsync();
                throw;
            }
        }

        // Waits for the change's commit, null when too slow
        public async Task<ProbeSample?> Measure(Action change, TimeSpan? timeout = null)
        {
            _published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _target, Head() + 1);
            change();
            Storage.ClearRequests();
            Volatile.Write(ref _sinkMs, 0);
            var allocated = GC.GetTotalAllocatedBytes(true);
            var cpu = CpuTime();
            var limit = timeout ?? ProbeKit.DidNotFinish;
            double wall;
            long peak;
            await using (var sampler = new PeakWorkingSet())
            {
                var watch = Stopwatch.StartNew();
                while (!_published.Task.IsCompleted)
                {
                    if (watch.Elapsed > limit)
                    {
                        // Fails the sink's next request, so the checkpoint stops
                        _aborted = true;
                        return null;
                    }
                    await Stream.SchedulerTick();
                    await Task.Delay(1);
                }
                wall = watch.Elapsed.TotalMilliseconds;
                await sampler.DisposeAsync();
                peak = sampler.Peak;
            }
            var sinkMs = Volatile.Read(ref _sinkMs);
            if (sinkMs <= 0)
            {
                throw new InvalidOperationException($"The sink's checkpoint hooks did not fire for {_name}");
            }
            var requests = Storage.Requests;
            var dvWrites = requests.Count(x => x.StartsWith("OpenWrite ", StringComparison.Ordinal) && x.Contains("deletion_vector", StringComparison.Ordinal));
            return new ProbeSample(wall, sinkMs, (CpuTime() - cpu).TotalMilliseconds, requests.Count, BytesRead(requests), dvWrites, GC.GetTotalAllocatedBytes(true) - allocated, peak);
        }

        // Finishes the checkpoints in flight, so a clock starts on an idle stream
        public Task Settle()
        {
            return RunCheckpoints(Stream, 2);
        }

        // Rows minus deletion vectors, over every file in the table
        public async Task<long> LiveRows()
        {
            var table = await DeltaTransactionReader.ReadTable(Storage, _name);
            long rows = 0;
            foreach (var add in table!.AddFiles)
            {
                using var statistics = JsonDocument.Parse(add.Statistics!);
                rows += statistics.RootElement.GetProperty("numRecords").GetInt64() - (add.DeletionVector?.Cardinality ?? 0);
            }
            return rows;
        }

        private static TimeSpan CpuTime()
        {
            using var process = Process.GetCurrentProcess();
            return process.TotalProcessorTime;
        }

        private long Head()
        {
            var log = Path.Combine(Root, _name, "_delta_log");
            if (!Directory.Exists(log))
            {
                return -1;
            }
            return Directory.EnumerateFiles(log, "*.json")
                .Select(x => long.TryParse(Path.GetFileNameWithoutExtension(x), out var version) ? version : -1)
                .DefaultIfEmpty(-1)
                .Max();
        }

        // Data and deletion vector files, not the log
        private long BytesRead(IReadOnlyList<string> requests)
        {
            long bytes = 0;
            foreach (var request in requests)
            {
                if (request.StartsWith("OpenRead ", StringComparison.Ordinal) && !request.Contains("/_delta_log/", StringComparison.Ordinal))
                {
                    var file = new FileInfo(Path.Combine(Root, request.Substring("OpenRead ".Length).TrimStart('/')));
                    bytes += file.Exists ? file.Length : 0;
                }
            }
            return bytes;
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            DeltaLakeSink.CatalogHooksForTests.TryRemove(_name, out _);
            try
            {
                await Stream.DisposeAsync();
            }
            catch (Exception) when (_aborted)
            {
                // The stopped checkpoint failed the stream
            }
            if (!_keepFiles)
            {
                try
                {
                    Directory.Delete(Root, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }
    }
}
