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

using FlowtideDotNet.Base.Engine;
using EngineStream = FlowtideDotNet.Base.Engine.DataflowStream;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    internal sealed class LmRunOptions
    {
        public required string Workload { get; init; }

        public double Scale { get; init; } = 0.001;

        public int Rounds { get; init; } = 3;

        public bool Wide { get; init; } = true;

        public bool Shuffled { get; init; }

        public long Seed { get; init; } = 123;

        public int CachePages { get; init; } = 10000;

        /// <summary>
        /// Local disk folder for state, memory when null.
        /// </summary>
        public string? StateDirectory { get; init; }

        public bool Census { get; init; }

        public bool TrackRows { get; init; }

        /// <summary>
        /// Only true when this is the process's only run.
        /// </summary>
        public bool ReportPeakMemory { get; init; }

        public double TimeoutSeconds { get; init; } = 3600;

        public string Arm { get; init; } = "Off";
    }

    internal sealed class LmRunResult
    {
        public required string Workload { get; init; }

        public required string Arm { get; init; }

        public required bool Wide { get; init; }

        public required double Scale { get; init; }

        public required long InputRows { get; init; }

        public required double LoadSeconds { get; init; }

        public required double LoadRowsPerSecond { get; init; }

        public required double LoadCheckpointMs { get; init; }

        /// <summary>
        /// Changed rows the sources sent, the same for every run of a seed.
        /// </summary>
        public required long ChurnRows { get; init; }

        public required double ChurnRowsPerSecond { get; init; }

        public required List<double> RoundSeconds { get; init; }

        /// <summary>
        /// One sample per update round, one checkpoint each.
        /// </summary>
        public required List<double> CheckpointMs { get; init; }

        public required double CheckpointMeanMs { get; init; }

        public required double CheckpointMaxMs { get; init; }

        public required double GenerationSeconds { get; init; }

        public required double PeakWorkingSetMb { get; init; }

        public required long SinkUpserts { get; init; }

        public required long SinkDeletes { get; init; }

        /// <summary>
        /// Engine counters read once at the end.
        /// </summary>
        public required Dictionary<string, double> Counters { get; init; }

        public required List<string> Nodes { get; init; }

        public required List<string> Edges { get; init; }

        public long TotalStateBytes { get; init; }

        public List<LmCensusRow>? Census { get; init; }

        public List<string>? CensusProblems { get; init; }
    }

    internal static class LmRun
    {
        public const string StreamPrefix = "lm-";

        private static int _runs;

        // Cache and storage counters that show eviction and reads
        private static readonly string[] CounterNames =
        [
            "flowtide_s3fifo_small_queue_evictions",
            "flowtide_cache_read_hits",
            "flowtide_cache_read_misses",
            MetricNames.PersistentStorageNumberOfReads,
            MetricNames.PersistentStorageNumberOfWrites
        ];

        private sealed class Checkpoints : ICheckpointListener
        {
            private readonly List<long> _ticks = new List<long>();
            private TaskCompletionSource _next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            /// <summary>
            /// Held by the run while it asks for a checkpoint.
            /// </summary>
            public object Lock { get; } = new object();

            public int Count
            {
                get
                {
                    lock (Lock)
                    {
                        return _ticks.Count;
                    }
                }
            }

            public void OnCheckpointComplete(StreamCheckpointNotification notification)
            {
                TaskCompletionSource reached;
                lock (Lock)
                {
                    _ticks.Add(Stopwatch.GetTimestamp());
                    reached = _next;
                    _next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                reached.SetResult();
            }

            /// <summary>
            /// The timestamp of a commit, by its position.
            /// </summary>
            public long TicksOf(int index)
            {
                lock (Lock)
                {
                    return _ticks[index];
                }
            }

            /// <summary>
            /// Done when more commits than the given count have happened.
            /// </summary>
            public Task After(int count)
            {
                lock (Lock)
                {
                    return _ticks.Count > count ? Task.CompletedTask : _next.Task;
                }
            }
        }

        public static async Task<LmRunResult> Run(LmRunOptions options, LmControl? control = null)
        {
            var workload = LmWorkload.Create(options.Workload, options.Scale);
            control ??= new LmControl(options.Seed, options.Shuffled, options.TrackRows, workload.Tables);
            var failures = new ConcurrentQueue<string>();
            var checkpoints = new Checkpoints();
            var deadline = DateTime.UtcNow.AddSeconds(options.TimeoutSeconds);

            var plan = BuildPlan(workload, options.Wide);
            var connectors = new ConnectorManager();
            connectors.AddSource(new LmSourceFactory(workload, control));
            connectors.AddSink(new LmSinkFactory(control, options.Wide ? workload.WideKeyColumns : workload.NarrowKeyColumns));

            string runDirectory = Path.Join(options.StateDirectory ?? Path.GetTempPath(), $"lm-{Guid.NewGuid():N}");
            var reservoir = new ReservoirPersistentStorage(new ReservoirStorageOptions()
            {
                FileProvider = options.StateDirectory != null ? new LocalDiskProvider(Path.Join(runDirectory, "state")) : new MemoryFileProvider()
            });
            IPersistentStorage storage = reservoir;
            LmCensusStorage? census = null;
            try
            {
                if (options.Census)
                {
                    census = new LmCensusStorage(reservoir);
                    storage = census;
                }
                // Its own name keeps this run's counters apart
                string streamName = StreamPrefix + Interlocked.Increment(ref _runs);
                var stream = new FlowtideBuilder(streamName)
                    .AddPlan(plan, false)
                    .AddConnectorManager(connectors)
                    .WithCheckpointListener(checkpoints)
                    .WithFailureListener(error => failures.Enqueue(error?.ToString() ?? "stream failure"))
                    // After the load, only asked-for checkpoints run
                    .SetMinimumTimeBetweenCheckpoint(TimeSpan.FromDays(1))
                    .WithStateOptions(new StateManagerOptions()
                    {
                        CachePageCount = options.CachePages,
                        MinCachePageCount = Math.Min(1000, options.CachePages),
                        PersistentStorage = storage,
                        TemporaryStorageOptions = new FileCacheOptions()
                        {
                            DirectoryPath = Path.Join(runDirectory, "tmp")
                        }
                    })
                    .Build();
                try
                {
                    return await Measure(stream, streamName, workload, control, options, census, checkpoints, failures, deadline);
                }
                finally
                {
                    await DisposeStream(stream);
                }
            }
            finally
            {
                try
                {
                    storage.Dispose();
                }
                finally
                {
                    if (Directory.Exists(runDirectory))
                    {
                        Directory.Delete(runDirectory, true);
                    }
                }
            }
        }

        private static async Task<LmRunResult> Measure(EngineStream stream, string streamName, LmWorkload workload, LmControl control, LmRunOptions options, LmCensusStorage? census, Checkpoints checkpoints, ConcurrentQueue<string> failures, DateTime deadline)
        {
            var start = Stopwatch.GetTimestamp();
            await stream.StartAsync();
            // The load is done when the sink has uploaded every table's rows
            await Wait(control.WaitForSink(0), stream, failures, deadline, "the initial load");
            long loaded = control.SinkTicks(0);
            double loadSeconds = Stopwatch.GetElapsedTime(start, loaded).TotalSeconds;
            // The sources hold the first checkpoint until then
            await Wait(checkpoints.After(0), stream, failures, deadline, "the load checkpoint");
            if (checkpoints.TicksOf(0) < loaded)
            {
                throw new InvalidOperationException("A checkpoint was committed before the sink had the load.");
            }
            double loadCheckpointMs = Stopwatch.GetElapsedTime(loaded, checkpoints.TicksOf(0)).TotalMilliseconds;

            var roundSeconds = new List<double>();
            var checkpointMs = new List<double>();
            for (int round = 1; round <= options.Rounds; round++)
            {
                int seen = checkpoints.Count;
                var roundStart = Stopwatch.GetTimestamp();
                control.RequestRound(round);
                await Wait(control.WaitForSink(round), stream, failures, deadline, $"round {round}");
                long atSink = control.SinkTicks(round);
                roundSeconds.Add(Stopwatch.GetElapsedTime(roundStart, atSink).TotalSeconds);
                if (checkpoints.Count != seen)
                {
                    throw new InvalidOperationException($"A checkpoint was committed during round {round}, before the run asked for one.");
                }
                checkpointMs.Add(await Checkpoint(stream, control, round, checkpoints, seen, atSink, failures, deadline));
            }

            // Read once, at the end of the run
            var counters = ReadCounters(streamName);
            var graph = stream.GetDiagnosticsGraph();
            var nodes = new List<string>();
            foreach (var node in graph.Nodes)
            {
                nodes.Add($"{node.Key}|{node.Value.DisplayName}");
            }
            var edges = new List<string>();
            for (int i = 0; i < graph.Edges.Count; i++)
            {
                edges.Add($"{graph.Edges[i].Source}>{graph.Edges[i].Target}");
            }
            double churnSeconds = roundSeconds.Sum();
            return new LmRunResult()
            {
                Workload = workload.Name,
                Arm = options.Arm,
                Wide = options.Wide,
                Scale = options.Scale,
                InputRows = workload.InputRows,
                LoadSeconds = loadSeconds,
                LoadRowsPerSecond = workload.InputRows / loadSeconds,
                LoadCheckpointMs = loadCheckpointMs,
                ChurnRows = control.ChurnRows,
                ChurnRowsPerSecond = churnSeconds > 0 ? control.ChurnRows / churnSeconds : 0,
                RoundSeconds = roundSeconds,
                CheckpointMs = checkpointMs,
                CheckpointMeanMs = checkpointMs.Count > 0 ? checkpointMs.Average() : 0,
                CheckpointMaxMs = checkpointMs.Count > 0 ? checkpointMs.Max() : 0,
                GenerationSeconds = control.GenerationSeconds,
                PeakWorkingSetMb = options.ReportPeakMemory ? Process.GetCurrentProcess().PeakWorkingSet64 / (1024.0 * 1024.0) : 0,
                SinkUpserts = control.Upserts,
                SinkDeletes = control.Deletes,
                Counters = counters,
                Nodes = nodes,
                Edges = edges,
                TotalStateBytes = census?.TotalLiveBytes() ?? 0,
                Census = census?.Snapshot(graph),
                CensusProblems = census?.Verify()
            };
        }

        /// <summary>
        /// Reads the observable counters once, so timed runs carry no observer.
        /// </summary>
        private static Dictionary<string, double> ReadCounters(string streamName)
        {
            string meter = $"flowtide.{streamName}";
            var values = new Dictionary<string, double>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                string name = instrument.Meter.Name;
                bool own = name == meter || name.StartsWith(meter + ".", StringComparison.Ordinal);
                if (own && Array.IndexOf(CounterNames, instrument.Name) >= 0)
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => Add(values, instrument.Name, value));
            listener.SetMeasurementEventCallback<int>((instrument, value, tags, state) => Add(values, instrument.Name, value));
            listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) => Add(values, instrument.Name, value));
            listener.Start();
            listener.RecordObservableInstruments();
            return values;
        }

        private static void Add(Dictionary<string, double> values, string name, double value)
        {
            lock (values)
            {
                values[name] = values.TryGetValue(name, out var current) ? current + value : value;
            }
        }

        private static async Task DisposeStream(EngineStream stream)
        {
            try
            {
                await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromMinutes(2));
            }
            catch (Exception e)
            {
                // A failed stop must not hide the run's own error
                Console.Error.WriteLine($"The stream did not stop cleanly: {e.Message}");
            }
        }

        private static FlowtideDotNet.Substrait.Plan BuildPlan(LmWorkload workload, bool wide)
        {
            var builder = new SqlPlanBuilder();
            for (int i = 0; i < workload.Tables.Count; i++)
            {
                var table = workload.Tables[i];
                var names = new List<string>();
                var types = new List<SubstraitBaseType>();
                for (int c = 0; c < table.Columns.Count; c++)
                {
                    names.Add(table.Columns[c].Name);
                    switch (table.Columns[c].Kind)
                    {
                        case LmColumnKind.Text:
                        case LmColumnKind.GroupText:
                            types.Add(new StringType());
                            break;
                        case LmColumnKind.Items:
                            types.Add(new AnyType());
                            break;
                        default:
                            types.Add(new Int64Type());
                            break;
                    }
                }
                builder.AddTableDefinition(table.Name, new NamedStruct() { Names = names, Struct = new Struct() { Types = types } });
            }
            builder.Sql(wide ? workload.WideSql : workload.NarrowSql);
            return PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings() { Parallelization = workload.Parallelization });
        }

        private static async Task Wait(Task task, EngineStream stream, ConcurrentQueue<string> failures, DateTime deadline, string what)
        {
            while (!task.IsCompleted)
            {
                ThrowIfStuck(stream, failures, deadline, what);
                await Task.WhenAny(task, Task.Delay(50));
            }
            await task;
        }

        private static void ThrowIfStuck(EngineStream stream, ConcurrentQueue<string> failures, DateTime deadline, string what)
        {
            if (!failures.IsEmpty)
            {
                throw new InvalidOperationException(string.Join(Environment.NewLine, failures));
            }
            if (DateTime.UtcNow <= deadline)
            {
                return;
            }
            // Say where the stream stands, a bare timeout explains nothing
            var state = new System.Text.StringBuilder($"Timed out waiting for {what}. Stream state {stream.State}.");
            foreach (var node in stream.GetDiagnosticsGraph().Nodes)
            {
                state.AppendLine().Append($"  {node.Key} {node.Value.DisplayName}:");
                for (int i = 0; i < node.Value.Gauges.Count; i++)
                {
                    state.Append($" {node.Value.Gauges[i].Name}={node.Value.Gauges[i].Dimensions.Values.Sum(d => d.Value)}");
                }
            }
            throw new TimeoutException(state.ToString());
        }

        /// <summary>
        /// One checkpoint, timed from the sink to the commit.
        /// </summary>
        private static async Task<double> Checkpoint(EngineStream stream, LmControl control, int round, Checkpoints checkpoints, int seen, long from, ConcurrentQueue<string> failures, DateTime deadline)
        {
            var committed = checkpoints.After(seen);
            while (!committed.IsCompleted)
            {
                ThrowIfStuck(stream, failures, deadline, "a checkpoint");
                // A request during compaction is put off a day, so ask again.
                // Under the lock no request follows the commit.
                Task? request = null;
                lock (checkpoints.Lock)
                {
                    if (checkpoints.Count == seen)
                    {
                        request = stream.TriggerCheckpoint();
                    }
                }
                if (request != null)
                {
                    if (request.IsFaulted)
                    {
                        await request;
                    }
                    control.CheckpointAsked(round);
                }
                await Task.WhenAny(committed, Task.Delay(2));
            }
            return Stopwatch.GetElapsedTime(from, checkpoints.TicksOf(seen)).TotalMilliseconds;
        }
    }
}
