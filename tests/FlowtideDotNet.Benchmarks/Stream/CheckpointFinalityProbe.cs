#define FINALITY
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Sql;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using EngineStream = FlowtideDotNet.Base.Engine.DataflowStream;

namespace FlowtideDotNet.Benchmarks.Stream;

// Explicit diagnostic runner, independent of BenchmarkDotNet's process launcher. Copy
// unchanged to comparison checkouts; remove only FINALITY for the pre-finality API.
internal static class CheckpointFinalityProbe
{
    private record Scenario(string Shape, int Exchanges, int LinkDelayMs = 0, int SlowCommitMs = 0);

    public static async Task Run(string[] args)
    {
        var scenarios = new[]
        {
            new Scenario("chain", 1), new Scenario("chain", 10), new Scenario("chain", 100),
            new Scenario("fan", 1), new Scenario("fan", 10), new Scenario("fan", 100),
            new Scenario("chain", 1, 2), new Scenario("fan", 1, 2),
            new Scenario("chain", 1, 2, 50)
        };
        int repetitions = args.Length > 1 ? int.Parse(args[1]) : 6;
        var selected = args.Length > 2 ? args[2].Split(',').Select(int.Parse).ToHashSet() : null;
        int cycles = args.Length > 3 ? int.Parse(args[3]) : 5;
        foreach (var (scenario, index) in scenarios.Select((scenario, index) => (scenario, index)))
        {
            if (selected != null && !selected.Contains(index)) continue;
            for (int repetition = 0; repetition < repetitions; repetition++)
            {
                var result = await Measure(scenario, repetition, cycles);
                Console.WriteLine(JsonSerializer.Serialize(result));
            }
        }
    }

    private static async Task<object> Measure(Scenario scenario, int repetition, int cycles)
    {
        const int rowsPerCycle = 500;
        var db = new MockDatabase();
        var table = db.GetOrCreateTable<User>("users");
        var hub = new LocalSubstreamCommunicationHub();
        var control = new ConcurrentDictionary<string, long>();
        var streams = new List<EngineStream>();
        var stores = new List<Store>();
        var checkpoints = new Checkpoints();
        long received = 0;
        var failures = new ConcurrentQueue<string>();
        var sql = Sql(scenario);
        try
        {
            foreach (var member in new[] { "a", "b", "c" })
            {
                var plan = new SqlPlanBuilder();
                plan.AddTableProvider(new DatasetTableProvider(db));
                plan.Sql(sql);
                var connectors = new ConnectorManager();
                connectors.AddSource(new MockSourceFactory("*", db, false));
                connectors.AddSink(new MockSinkFactory("*", _ => { }, 0, _ => { },
                    onChangeRowsReceived: count => Interlocked.Add(ref received, count)));
                var store = new Store(member == "a" ? scenario.SlowCommitMs : 0);
                stores.Add(store);
                var stream = new FlowtideBuilder("measure-" + member)
                    .AddPlan(plan.GetPlan(), false).AddConnectorManager(connectors)
                    .WithStateOptions(new StateManagerOptions { PersistentStorage = store, CachePageCount = 100_000 })
                    .SetDistributedOptions(new DistributedOptions(member, null,
                        new HandlerFactory(hub.CreateFactory(member), control, scenario.LinkDelayMs)))
                    .WithCheckpointListener(checkpoints)
                    .WithFailureListener(error => failures.Enqueue(error?.ToString() ?? "recovery"))
                    .Build();
                streams.Add(stream);
            }
            await Task.WhenAll(streams.Select(s => s.StartAsync()));
            await Wait(() => streams.All(s => s.State == StreamStateValue.Running), failures);
            control.Clear();
            checkpoints.Clear();
            GC.Collect();
            var allocated = GC.GetTotalAllocatedBytes(true);
            var start = Stopwatch.GetTimestamp();
            var checkpointMs = new List<double>();
            for (int cycle = 0; cycle < cycles; cycle++)
            {
                table.AddOrUpdate(Enumerable.Range(cycle * rowsPerCycle, rowsPerCycle).Select(i => new User { UserKey = i }));
                await streams[0].CallTrigger("changes", null);
                long expected = (cycle + 1L) * rowsPerCycle * scenario.Exchanges;
                await Wait(() => Interlocked.Read(ref received) >= expected, failures);
                var versions = stores.Select(s => s.CurrentVersion).ToArray();
                var checkpointStart = Stopwatch.GetTimestamp();
                await streams[0].TriggerCheckpoint();
                await Wait(() => stores.Select((s, i) => s.CurrentVersion > versions[i]).All(x => x), failures);
                checkpointMs.Add(Stopwatch.GetElapsedTime(checkpointStart).TotalMilliseconds);
            }
            var elapsed = Stopwatch.GetElapsedTime(start).TotalSeconds;
            var bytes = GC.GetTotalAllocatedBytes(true) - allocated;
            var inputRows = (long)cycles * rowsPerCycle;
            return new
            {
                scenario.Shape, scenario.Exchanges, scenario.LinkDelayMs, scenario.SlowCommitMs,
                repetition, warmup = repetition == 0, inputRows,
                rowsPerSecond = inputRows / elapsed,
                bytesPerInputRow = bytes / (double)inputRows,
                checkpointMeanMs = checkpointMs.Average(), checkpointMaxMs = checkpointMs.Max(),
                checkpointCadenceMs = checkpoints.CadenceMs(), controlRequests = control.OrderBy(p => p.Key).ToDictionary(p => p.Key, p => p.Value),
                failures = failures.ToArray()
            };
        }
        finally
        {
            foreach (var stream in streams) await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));
            foreach (var store in stores) store.Dispose();
        }
    }

    private static async Task Wait(Func<bool> ready, ConcurrentQueue<string> failures)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        while (!ready())
        {
            if (!failures.IsEmpty) throw new InvalidOperationException(string.Join("\n", failures));
            await Task.Delay(1, timeout.Token);
        }
    }

    private static string Sql(Scenario scenario)
    {
        var sql = new StringBuilder("SUBSTREAM a;\n");
        int partitions = scenario.Shape == "fan" ? 2 : 1;
        for (int i = 0; i < scenario.Exchanges; i++)
            sql.AppendLine($"CREATE VIEW v{i} WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = {partitions}) AS SELECT userkey FROM users;");
        sql.AppendLine("SUBSTREAM b;");
        for (int i = 0; i < scenario.Exchanges; i++)
            sql.AppendLine(scenario.Shape == "chain"
                ? $"CREATE VIEW w{i} WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS SELECT userkey FROM v{i} WITH (PARTITION_ID = 0);"
                : $"INSERT INTO output_b_{i} SELECT userkey FROM v{i} WITH (PARTITION_ID = 0);");
        sql.AppendLine("SUBSTREAM c;");
        for (int i = 0; i < scenario.Exchanges; i++)
            sql.AppendLine(scenario.Shape == "chain"
                ? $"INSERT INTO output_c_{i} SELECT userkey FROM w{i} WITH (PARTITION_ID = 0);"
                : $"INSERT INTO output_c_{i} SELECT userkey FROM v{i} WITH (PARTITION_ID = 1);");
        return sql.ToString();
    }

    private sealed class Checkpoints : ICheckpointListener
    {
        private readonly ConcurrentDictionary<string, List<long>> times = new();
        public void Clear() => times.Clear();
        public void OnCheckpointComplete(StreamCheckpointNotification notification)
        {
            var list = times.GetOrAdd(notification.StreamName, _ => new());
            lock (list) list.Add(Stopwatch.GetTimestamp());
        }
        public Dictionary<string, double> CadenceMs() => times.ToDictionary(p => p.Key, p =>
        {
            lock (p.Value) return p.Value.Count < 2 ? 0 : Stopwatch.GetElapsedTime(p.Value[0], p.Value[^1]).TotalMilliseconds / (p.Value.Count - 1);
        });
    }

    private sealed class Store(int delayMs) : IPersistentStorage
    {
        private readonly ReservoirPersistentStorage inner = new(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        public bool SupportsDistributedCheckpoints => true;
        public long CurrentVersion => inner.CurrentVersion;
        public Task InitializeAsync(StorageInitializationMetadata metadata) => inner.InitializeAsync(metadata);
        public IPersistentStorageSession CreateSession() => inner.CreateSession();
        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            if (delayMs > 0) await Task.Delay(delayMs);
            await inner.CheckpointAsync(metadata, includeIndex);
        }
        public ValueTask CompactAsync(ulong changes, ulong pages) => inner.CompactAsync(changes, pages);
        public ValueTask ResetAsync() => inner.ResetAsync();
        public ValueTask RecoverAsync(long version) => inner.RecoverAsync(version);
        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value) => inner.TryGetValue(key, out value);
        public ValueTask Write(long key, byte[] value) => inner.Write(key, value);
        public void ClearForRestore() => inner.ClearForRestore();
        public void Dispose() => inner.Dispose();
    }

    private sealed class HandlerFactory(ISubstreamCommunicationHandlerFactory inner, ConcurrentDictionary<string, long> counts, int delayMs) : ISubstreamCommunicationHandlerFactory
    {
        public ISubstreamCommunicationHandler GetCommunicationHandler(string target, string self) =>
            new Handler(inner.GetCommunicationHandler(target, self), counts, self + "->" + target, delayMs);
    }

    private sealed class Handler(ISubstreamCommunicationHandler inner, ConcurrentDictionary<string, long> counts, string pair, int delayMs) : ISubstreamCommunicationHandler
    {
        private async Task Delay(string kind)
        {
            counts.AddOrUpdate(pair + ":" + kind, 1, (_, count) => count + 1);
            if (delayMs > 0) await Task.Delay(delayMs);
        }
        public void OnStreamFailure() => inner.OnStreamFailure();
        public async Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> ids, int count, CancellationToken token)
        {
            if (delayMs > 0) await Task.Delay(delayMs, token);
            return await inner.FetchData(ids, count, token);
        }
#if FINALITY
        public void Initialize(Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> fetch, Func<RecoveryWave, Task> fail, Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> init, Func<long, long, bool, Task> done) => inner.Initialize(fetch, fail, init, done);
        public async Task SendFailAndRecover(RecoveryWave wave) { await Delay("failure"); await inner.SendFailAndRecover(wave); }
        public async Task<SubstreamInitializeResponse> SendInitializeRequest(long version, long epoch, bool handoff, RecoveryWave wave, CancellationToken token)
        { await Delay("initialize"); return await inner.SendInitializeRequest(version, epoch, handoff, wave, token); }
        public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> receive) => inner.InitializeDurabilityClaims(receive);
        public async Task SendDurabilityClaim(long version, int radius, long init, RecoveryWave wave, long sender, long target, bool reply, CancellationToken token)
        { await Delay("claim"); await inner.SendDurabilityClaim(version, radius, init, wave, sender, target, reply, token); }
#else
        public void Initialize(Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> fetch, Func<long, Task> fail, Func<long, long, bool, Task<SubstreamInitializeResponse>> init, Func<long, long, bool, Task> done) => inner.Initialize(fetch, fail, init, done);
        public async Task SendFailAndRecover(long version) { await Delay("failure"); await inner.SendFailAndRecover(version); }
        public async Task<SubstreamInitializeResponse> SendInitializeRequest(long version, long epoch, bool handoff, CancellationToken token)
        { await Delay("initialize"); return await inner.SendInitializeRequest(version, epoch, handoff, token); }
#endif
        public async Task SendCheckpointDone(long version, long epoch, bool coversStop)
        { await Delay("checkpoint"); await inner.SendCheckpointDone(version, epoch, coversStop); }
    }
}
