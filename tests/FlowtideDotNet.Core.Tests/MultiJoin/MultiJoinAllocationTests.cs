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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;
using Xunit.Abstractions;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [CollectionDefinition(nameof(MultiJoinAllocationCollection), DisableParallelization = true)]
    public class MultiJoinAllocationCollection
    {
    }

    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinAllocationTests
    {
        private const int Keys = 4096;

        // Per input and round
        private const int MaxMessages = 1000;

        private readonly ITestOutputHelper _output;

        public MultiJoinAllocationTests(ITestOutputHelper output)
        {
            _output = output;
        }

        // Two operators run side by side, each needs storage of its own
        internal sealed class Host : OperatorTestBase, IDisposable
        {
            private readonly string _directory = $"./data/multiJoinAllocationTests/{Guid.NewGuid():N}";

            // Set for a host that should keep only this many pages in memory
            internal static int? CachePages;

            // Set to false for a host whose checkpoint has written its pages when it returns
            internal static bool? BackgroundCommit;

            // The operator the host was made for, it goes before the storage
            internal IAsyncDisposable? Operator;

            // What the cache has written out to make room
            internal long TemporaryBytes => Directory.Exists($"{_directory}/tmp") ? new DirectoryInfo($"{_directory}/tmp").EnumerateFiles("*", SearchOption.AllDirectories).Sum(x => x.Length) : 0;
            private FileCachePersistentStorage? _storage;

            // Every page read from the persistent storage waits this long, as a read from a blob store does
            internal static int ReadDelayMs;

            // Page reads that waited, the ones waiting now, and the read that fails (counted from one, zero for none)
            internal static long ReadsThatWaited;
            internal static int ReadsWaiting;
            internal static long FailingRead;

            private sealed class WaitingSession : IPersistentStorageSession
            {
                private readonly IPersistentStorageSession _inner;

                public WaitingSession(IPersistentStorageSession inner)
                {
                    _inner = inner;
                }

                public bool SupportsConcurrentReads => true;

                public Task Commit() => _inner.Commit();

                public Task Delete(long key) => _inner.Delete(key);

                public void Dispose() => _inner.Dispose();

                private static async ValueTask Wait()
                {
                    if (ReadDelayMs <= 0)
                    {
                        return;
                    }
                    long read = Interlocked.Increment(ref ReadsThatWaited);
                    Interlocked.Increment(ref ReadsWaiting);
                    try
                    {
                        await Task.Delay(ReadDelayMs);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref ReadsWaiting);
                    }
                    if (read == Interlocked.Read(ref FailingRead))
                    {
                        throw new IOException("Injected page read failure");
                    }
                }

                public async ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject
                {
                    await Wait();
                    return await _inner.Read(key, stateSerializer);
                }

                public async ValueTask<ReadOnlyMemory<byte>> Read(long key)
                {
                    await Wait();
                    return await _inner.Read(key);
                }

                public Task Write(long key, SerializableObject value) => _inner.Write(key, value);
            }

            private sealed class WaitingStorage : FileCachePersistentStorage
            {
                public WaitingStorage(FileCacheOptions options) : base(options)
                {
                }

                public override IPersistentStorageSession CreateSession() => new WaitingSession(base.CreateSession());
            }

            protected override StateManagerOptions CreateStateManagerOptions()
            {
                _storage = new WaitingStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/persistent" });
                return new StateManagerOptions()
                {
                    BackgroundCommit = BackgroundCommit ?? FlowtideDotNet.Storage.StateManager.Internal.Sync.BackgroundCommit.Enabled,
                    CachePageCount = CachePages ?? 10000,
                    MinCachePageCount = CachePages == null ? 1000 : CachePages.Value / 2,
                    PersistentStorage = _storage,
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/tmp" }
                };
            }

            void IDisposable.Dispose()
            {
                Dispose();
                _storage?.Dispose();
                try
                {
                    Directory.Delete(_directory, true);
                }
                catch (IOException)
                {
                    // A file may still be closing, the directory is under the build output and goes with it
                }
            }
        }

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                    Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(x => (SubstraitBaseType)new AnyType()).ToList() }
                }
            };
        }

        private static MergeJoinRelation Join(Relation left, Relation right, int leftKey, int rightKey)
        {
            return new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = left,
                Right = right,
                LeftKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = leftKey } } },
                RightKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = rightKey } } }
            };
        }

        // a(k, v), b(k, j, v), c(j, v)
        private static (MergeJoinRelation ab, MergeJoinRelation abc) Chain()
        {
            var ab = Join(Read("a", 2), Read("b", 3), 0, 2);
            return (ab, Join(ab, Read("c", 2), 3, 5));
        }

        private static readonly int[] ColumnCounts = new int[] { 2, 3, 2 };

        // Every other key carries a double as its value, so the value columns hold two types
        private static bool _mixedValues;

        private static IDataValue Value(int key, int generation)
        {
            if (_mixedValues && key % 2 == 1)
            {
                return new DoubleValue(generation);
            }
            return new Int64Value(generation);
        }

        // Generation 0 inserts the keys.
        internal static StreamMessage<StreamEventBatch> CreateMessage(int input, int firstKey, int keys, int generation, int columnCount = 0)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[columnCount == 0 ? ColumnCounts[input] : columnCount];
            int rowsPerKey = generation == 0 ? 1 : 2;
            for (int c = 0; c < columns.Length; c++)
            {
                var column = Column.Create(allocator);
                bool isValue = c == columns.Length - 1;
                for (int k = 0; k < keys; k++)
                {
                    if (isValue && _payloadWidth > 0)
                    {
                        column.Add(new StringValue((firstKey + k).ToString().PadLeft(_payloadWidth, 'x')));
                        continue;
                    }
                    if (generation > 0)
                    {
                        column.Add(isValue ? Value(firstKey + k, generation - 1) : new Int64Value(firstKey + k));
                    }
                    column.Add(isValue ? Value(firstKey + k, generation) : new Int64Value(firstKey + k));
                }
                columns[c] = column;
            }
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            for (int k = 0; k < keys; k++)
            {
                if (generation > 0)
                {
                    weights.Add(-1);
                    iterations.Add(0);
                }
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static long NativeAllocations()
        {
            long total = 0;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name == "flowtide.memory.allocation_count")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => total += value);
            listener.Start();
            listener.RecordObservableInstruments();
            return total;
        }

        internal static async Task<long> Drain(BufferBlock<IStreamEvent> output)
        {
            long rows = 0;
            while (true)
            {
                var received = await output.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (received is ICheckpointEvent)
                {
                    return rows;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    rows += message.Data.Data.Count;
                    message.Data.Data.Return();
                }
            }
        }

        // With several measured runs the one that allocated least counts: a round larger than any before makes scratch arrays grow once
        private async Task<(double managed, double native, long rows, double ms)> Measure(ITargetBlock<IStreamEvent>[] targets, BufferBlock<IStreamEvent> output, int keysPerMessage, int measuredRuns = 1)
        {
            long time = 1;
            async Task<long> Send(List<StreamMessage<StreamEventBatch>> messages)
            {
                for (int i = 0; i < messages.Count; i++)
                {
                    await targets[i % 3].SendAsync(messages[i]);
                }
                foreach (var target in targets)
                {
                    await target.SendAsync(new Checkpoint(time, time + 1, 1));
                }
                time++;
                return await Drain(output);
            }

            // Every key in every input, then the same kind of traffic that is measured so all scratch has its size
            var load = new List<StreamMessage<StreamEventBatch>>();
            for (int k = 0; k < Keys; k += 512)
            {
                for (int input = 0; input < 3; input++)
                {
                    load.Add(CreateMessage(input, k, 512, 0));
                }
            }
            await Send(load);

            int messagesPerInput = Math.Min(MaxMessages, Keys / keysPerMessage);
            List<StreamMessage<StreamEventBatch>> Traffic(int generation)
            {
                var traffic = new List<StreamMessage<StreamEventBatch>>(messagesPerInput * 3);
                for (int m = 0; m < messagesPerInput; m++)
                {
                    for (int input = 0; input < 3; input++)
                    {
                        traffic.Add(CreateMessage(input, m * keysPerMessage, keysPerMessage, generation));
                    }
                }
                return traffic;
            }
            await Send(Traffic(1));

            double inputRows = messagesPerInput * 3.0 * keysPerMessage * 2;
            (double managed, double native, long rows, double ms) least = (double.MaxValue, 0, 0, 0);
            for (int run = 0; run < measuredRuns; run++)
            {
                var measured = Traffic(2 + run);
                GC.Collect();
                long nativeBefore = NativeAllocations();
                long managedBefore = GC.GetTotalAllocatedBytes(true);
                var watch = System.Diagnostics.Stopwatch.StartNew();
                long rows = await Send(measured);
                watch.Stop();
                long managed = GC.GetTotalAllocatedBytes(true) - managedBefore;
                long native = NativeAllocations() - nativeBefore;
                if (managed / inputRows < least.managed)
                {
                    least = (managed / inputRows, native / inputRows, rows, watch.Elapsed.TotalMilliseconds);
                }
            }
            return least;
        }

        // The same traffic handed to the operator as rounds of one size, so every run does the same work as the runs that warmed it
        private static async Task<(double managed, double native, long rows, double ms)> MeasureRounds(MultiJoinOperator op, int keysPerMessage, int measuredRuns)
        {
            const int WarmRuns = 3;
            int messagesPerInput = Math.Min(MaxMessages, Keys / keysPerMessage);
            int perRound = Math.Clamp(MultiJoinOperator.RoundRowsPerInput / (keysPerMessage * 2), 1, MultiJoinOperator.RoundMessages / 3);
            double inputRows = messagesPerInput * 3.0 * keysPerMessage * 2;
            (double managed, double native, long rows, double ms) least = (double.MaxValue, 0, 0, 0);
            for (int run = 0; run < WarmRuns + measuredRuns; run++)
            {
                // The loading and one run of traffic went before, with generations 0 and 1
                var rounds = new List<List<RoundMessage<StreamEventBatch>>>();
                for (int m = 0; m < messagesPerInput; m += perRound)
                {
                    var round = new List<RoundMessage<StreamEventBatch>>(perRound * 3);
                    for (int i = m; i < Math.Min(m + perRound, messagesPerInput); i++)
                    {
                        for (int input = 0; input < 3; input++)
                        {
                            round.Add(new RoundMessage<StreamEventBatch>(input, CreateMessage(input, i * keysPerMessage, keysPerMessage, 2 + run).Data));
                        }
                    }
                    rounds.Add(round);
                }
                long sideBySideBefore = op.RoundsSideBySide;
                GC.Collect();
                long nativeBefore = NativeAllocations();
                long managedBefore = GC.GetTotalAllocatedBytes(true);
                var watch = System.Diagnostics.Stopwatch.StartNew();
                long rows = 0;
                foreach (var round in rounds)
                {
                    await foreach (var part in op.OnRecieveRound(round, 2 + run))
                    {
                        rows += part.Data.Count;
                        part.Rent(1);
                        part.Return();
                    }
                }
                watch.Stop();
                long managed = GC.GetTotalAllocatedBytes(true) - managedBefore;
                long native = NativeAllocations() - nativeBefore;
                foreach (var round in rounds)
                {
                    foreach (var message in round)
                    {
                        message.Message.Return();
                    }
                }
                Assert.Equal(rounds.Count, op.RoundsSideBySide - sideBySideBefore);
                if (run >= WarmRuns && managed / inputRows < least.managed)
                {
                    least = (managed / inputRows, native / inputRows, rows, watch.Elapsed.TotalMilliseconds);
                }
            }
            return least;
        }

        // Without rounds a message is handled on its own, what it allocates does not depend on what happened to be queued with it
        private async Task<(double managed, double native, long rows, double ms)> MeasureMulti(int keysPerMessage, JoinType type = JoinType.Inner, bool rounds = false, int measuredRuns = 1)
        {
            bool roundsBefore = MultiJoinOperator.Rounds;
            MultiJoinOperator.Rounds = rounds;
            try
            {
                return await MeasureMultiCore(keysPerMessage, type, rounds, measuredRuns);
            }
            finally
            {
                MultiJoinOperator.Rounds = roundsBefore;
            }
        }

        private async Task<(double managed, double native, long rows, double ms)> MeasureMultiCore(int keysPerMessage, JoinType type, bool rounds, int measuredRuns)
        {
            using var host = new Host();
            var (ab, abc) = Chain();
            ab.Type = type;
            abc.Type = type;
            var plan = MultiJoinPlanBuilder.Discover(abc, 9);
            Assert.NotNull(plan);
            var output = new BufferBlock<IStreamEvent>();
            var op = new MultiJoinOperator(plan, host.FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(output);
            await host.InitializeOperator(op);
            try
            {
                if (!rounds)
                {
                    return await Measure(op.Targets, output, keysPerMessage, measuredRuns);
                }
                await Measure(op.Targets, output, keysPerMessage, 0);
                return await MeasureRounds(op, keysPerMessage, measuredRuns);
            }
            finally
            {
                await op.DisposeAsync();
            }
        }

        private async Task<(double managed, double native, long rows, double ms)> MeasureChain(int keysPerMessage, int measuredRuns = 1)
        {
            using var firstHost = new Host();
            using var secondHost = new Host();
            var (ab, abc) = Chain();
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            var output = new BufferBlock<IStreamEvent>();
            var first = new ColumnBulkMergeJoin(ab, firstHost.FunctionsRegister, options);
            var second = new ColumnBulkMergeJoin(abc, secondHost.FunctionsRegister, options);
            second.LinkTo(output);
            await secondHost.InitializeOperator(second);
            // The targets of the second join exist once it is initialized
            first.LinkTo(second.Targets[0]);
            await firstHost.InitializeOperator(first);
            try
            {
                return await Measure(new ITargetBlock<IStreamEvent>[] { first.Targets[0], first.Targets[1], second.Targets[1] }, output, keysPerMessage, measuredRuns);
            }
            finally
            {
                await first.DisposeAsync();
                await second.DisposeAsync();
            }
        }

        // Rows per second through the joins alone: the clock stops when the last output row is out, before the checkpoint
        private async Task<(double wall, double cpu)> Throughput(ITargetBlock<IStreamEvent>[] targets, BufferBlock<IStreamEvent> output, int keysPerMessage, int generations)
        {
            async Task Checkpoint(long time)
            {
                foreach (var target in targets)
                {
                    await target.SendAsync(new Checkpoint(time, time + 1, 1));
                }
                await Drain(output);
            }
            int Columns(int input) => input == 0 || input == targets.Length - 1 ? 2 : 3;
            // FLOWTIDE_MULTIJOIN_PERF_KEYS: keys per input. FLOWTIDE_MULTIJOIN_PERF_PARTITION=1: every input changes keys of its own
            int keys = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_KEYS"), out var keyCount) ? keyCount : Keys;
            bool partition = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_PARTITION") == "1";
            for (int k = 0; k < keys; k += 512)
            {
                for (int input = 0; input < targets.Length; input++)
                {
                    await targets[input].SendAsync(CreateMessage(input, k, 512, 0, Columns(input)));
                }
            }
            await Checkpoint(1);

            int messagesPerInput = (partition ? keys / targets.Length : keys) / keysPerMessage;
            var messages = new List<StreamMessage<StreamEventBatch>>();
            for (int generation = 1; generation <= generations; generation++)
            {
                for (int m = 0; m < messagesPerInput; m++)
                {
                    for (int input = 0; input < targets.Length; input++)
                    {
                        messages.Add(CreateMessage(input, (partition ? input * (keys / targets.Length) : 0) + m * keysPerMessage, keysPerMessage, generation, Columns(input)));
                    }
                }
            }
            long inputRows = (long)messages.Count * keysPerMessage * 2;
            var process = System.Diagnostics.Process.GetCurrentProcess();
            var cpuBefore = process.TotalProcessorTime;
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var send = Task.Run(async () =>
            {
                for (int i = 0; i < messages.Count; i++)
                {
                    await targets[i % targets.Length].SendAsync(messages[i]);
                }
            });
            // A left join gives more rows than it takes, so the end is the checkpoint and the clock is read at the last row before it
            var end = Task.Run(async () =>
            {
                await send;
                foreach (var target in targets)
                {
                    await target.SendAsync(new Checkpoint(2, 3, 1));
                }
            });
            var lastRow = watch.Elapsed;
            var cpu = TimeSpan.Zero;
            while (true)
            {
                var received = await output.ReceiveAsync(TimeSpan.FromSeconds(60));
                if (received is ICheckpointEvent)
                {
                    break;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    message.Data.Data.Return();
                    lastRow = watch.Elapsed;
                    process.Refresh();
                    cpu = process.TotalProcessorTime - cpuBefore;
                }
            }
            await end;
            // By the clock, and by processor time: the chain is two operators that run side by side
            return (inputRows / lastRow.TotalSeconds, inputRows / cpu.TotalSeconds);
        }

        [Fact]
        public async Task RoundsAllocateLittleMoreThanMessagesOneByOne()
        {
            var oneByOne = await MeasureMulti(256, measuredRuns: 3);
            double inRounds = (await MeasureMulti(256, rounds: true, measuredRuns: 3)).managed;

            _output.WriteLine($"Managed bytes per input row: one by one {oneByOne.managed:F0}, in rounds {inRounds:F0}");
            // Other threads of the test host allocate a little too, a quarter megabyte of that is let through
            double slack = 262144.0 / (Keys / 256 * 3.0 * 256 * 2);
            // A round has its queues, tasks and channel, nothing per row
            Assert.True(inRounds <= oneByOne.managed + 8 + slack, $"Managed bytes per input row: one by one {oneByOne.managed:F0}, in rounds {inRounds:F0}");
        }

        /// <summary>
        /// Prints rows per second for the chain and the multi join, it asserts nothing about them.
        /// </summary>
        // The first input is (k, v), the last (k, v), the ones between (k, j, v): each joins the next on j = k
        internal static List<MergeJoinRelation> ChainOf(int inputs, bool? asStar = null)
        {
            // FLOWTIDE_MULTIJOIN_PERF_SHAPE=star joins every input to the first, FLOWTIDE_MULTIJOIN_PERF_JOIN=left makes them left joins
            bool star = asStar ?? Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_SHAPE") == "star";
            bool leftJoins = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_JOIN") == "left";
            var joins = new List<MergeJoinRelation>();
            Relation left = Read("i0", 2);
            int leftKey = 0;
            int width = 2;
            for (int i = 1; i < inputs; i++)
            {
                int columns = i == inputs - 1 ? 2 : 3;
                var join = Join(left, Read("i" + i, columns), leftKey, width);
                if (leftJoins)
                {
                    join.Type = JoinType.Left;
                }
                joins.Add(join);
                left = join;
                leftKey = star ? 0 : width + 1;
                width += columns;
            }
            return joins;
        }

        [Theory]
        [InlineData(3, 1, 4)]
        [InlineData(3, 16, 20)]
        [InlineData(3, 256, 40)]
        [InlineData(3, 2048, 40)]
        [InlineData(5, 16, 20)]
        [InlineData(5, 256, 40)]
        [InlineData(5, 2048, 40)]
        [InlineData(9, 16, 10)]
        [InlineData(9, 256, 20)]
        [InlineData(9, 2048, 20)]
        public async Task PrintThroughput(int inputs, int keysPerMessage, int generations)
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF") == null)
            {
                return;
            }
            // On one core the clock shows the work done, without what running side by side hides
            if (int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_CORES"), out var cores) && OperatingSystem.IsWindows())
            {
                System.Diagnostics.Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)((1L << cores) - 1);
            }
            var only = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_CASE");
            if (only != null && only != $"{inputs},{keysPerMessage}")
            {
                return;
            }
            int runs = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_RUNS"), out var r) ? r : 11;
            // The first runs are on code that is not optimized yet, the multi join takes longer to get there
            const int WarmUpRuns = 6;
            var chainRates = new List<double>();
            var multiRates = new List<double>();
            if (int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_MINROWS"), out var minRows))
            {
                MultiJoinOperator.ParallelInsertMinRows = minRows;
            }
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            for (int run = 0; run < runs; run++)
            {
                (double wall, double cpu) chain;
                var hosts = new List<Host>();
                try
                {
                    var joins = ChainOf(inputs);
                    var output = new BufferBlock<IStreamEvent>();
                    var operators = new ColumnBulkMergeJoin[joins.Count];
                    // The targets of a join exist once it is initialized, so the last one comes first
                    for (int j = joins.Count - 1; j >= 0; j--)
                    {
                        var host = new Host();
                        hosts.Add(host);
                        operators[j] = new ColumnBulkMergeJoin(joins[j], host.FunctionsRegister, options);
                        if (j == joins.Count - 1)
                        {
                            operators[j].LinkTo(output);
                        }
                        else
                        {
                            operators[j].LinkTo(operators[j + 1].Targets[0]);
                        }
                        await host.InitializeOperator(operators[j]);
                    }
                    var targets = new ITargetBlock<IStreamEvent>[inputs];
                    targets[0] = operators[0].Targets[0];
                    for (int i = 1; i < inputs; i++)
                    {
                        targets[i] = operators[i - 1].Targets[1];
                    }
                    chain = await Throughput(targets, output, keysPerMessage, generations);
                }
                finally
                {
                    foreach (IDisposable host in hosts)
                    {
                        host.Dispose();
                    }
                }
                async Task<(double wall, double cpu)> Multi()
                {
                    if (int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_CAP"), out var cap))
                    {
                        // Several multi joins in a row, each of at most this many inputs
                        var capped = new List<Host>();
                        try
                        {
                            var cappedOutput = new BufferBlock<IStreamEvent>();
                            var cappedTargets = new ITargetBlock<IStreamEvent>[inputs];
                            await BuildMulti(ChainOf(inputs)[inputs - 2], cappedOutput, cap, capped, cappedTargets, options);
                            return await Throughput(cappedTargets, cappedOutput, keysPerMessage, generations);
                        }
                        finally
                        {
                            await DisposeAll(capped);
                        }
                    }
                    using var host = new Host();
                    var plan = MultiJoinPlanBuilder.Discover(ChainOf(inputs)[inputs - 2], 9);
                    Assert.NotNull(plan);
                    var output = new BufferBlock<IStreamEvent>();
                    await using var op = new MultiJoinOperator(plan, host.FunctionsRegister, options);
                    op.LinkTo(output);
                    await host.InitializeOperator(op);
                    return await Throughput(op.Targets, output, keysPerMessage, generations);
                }
                // With a switch named, the multi join without it stands where the chain does
                var compare = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_SWITCH");
                if (compare == "same")
                {
                    // The same multi join twice: what the order of the two runs alone does to the numbers
                    chain = await Multi();
                }
                if (compare == "rounds")
                {
                    // The multi join that takes its messages one by one stands where the chain does
                    MultiJoinOperator.Rounds = false;
                    try
                    {
                        chain = await Multi();
                    }
                    finally
                    {
                        MultiJoinOperator.Rounds = true;
                    }
                }
                if (compare == "lookups")
                {
                    MultiJoinOperator.ParallelLookups = false;
                    try
                    {
                        chain = await Multi();
                    }
                    finally
                    {
                        MultiJoinOperator.ParallelLookups = true;
                    }
                }
                if (compare == "distinct")
                {
                    MultiJoinTreeLookup.DistinctKeySearch = false;
                    try
                    {
                        chain = await Multi();
                    }
                    finally
                    {
                        MultiJoinTreeLookup.DistinctKeySearch = true;
                    }
                }
                double lookupMs = 0;
                double insertMs = 0;
                using var listener = new MeterListener();
                listener.InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Name.EndsWith("lookup_ms") || instrument.Name.EndsWith("insert_ms") || instrument.Name.EndsWith("parallel_lookups"))
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                };
                listener.SetMeasurementEventCallback<float>((instrument, value, tags, state) =>
                {
                    if (instrument.Name.EndsWith("lookup_ms"))
                    {
                        lookupMs += value;
                    }
                    else
                    {
                        insertMs += value;
                    }
                });
                long lookupsBeside = 0;
                listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
                {
                    if (!instrument.IsObservable)
                    {
                        Interlocked.Add(ref lookupsBeside, value);
                    }
                });
                listener.Start();
                var multiWatch = System.Diagnostics.Stopwatch.StartNew();
                var multi = await Multi();
                multiWatch.Stop();
                listener.Dispose();
                _output.WriteLine($"RUN {run:D2} {inputs} inputs, {keysPerMessage * 2} rows per message: chain {chain.wall / 1000:F0}k rows/s, multi join {multi.wall / 1000:F0}k ({multi.wall / chain.wall:F2}), lookup {lookupMs:F0} ms, insert {insertMs:F0} ms of {multiWatch.ElapsedMilliseconds} ms, {Interlocked.Read(ref lookupsBeside)} lookups beside another");
                if (run >= WarmUpRuns)
                {
                    chainRates.Add(chain.wall);
                    multiRates.Add(multi.wall);
                }
            }
            if (chainRates.Count > 0)
            {
                chainRates.Sort();
                multiRates.Sort();
                double chainMedian = chainRates[chainRates.Count / 2];
                double multiMedian = multiRates[multiRates.Count / 2];
                _output.WriteLine($"PERF {inputs} inputs, {keysPerMessage * 2} rows per message, median of {chainRates.Count} warm runs: chain {chainMedian / 1000:F0}k rows/s, multi join {multiMedian / 1000:F0}k ({multiMedian / chainMedian:F2})");
            }
        }

        // The payload column as a string of this many characters, 0 for a number
        private static int _payloadWidth;

        private static long LiveNativeBytes()
        {
            long total = 0;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name == "flowtide.memory.allocated_bytes" || instrument.Name == "flowtide.memory.freed_bytes")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => total += instrument.Name.EndsWith("allocated_bytes") ? value : -value);
            listener.Start();
            listener.RecordObservableInstruments();
            return total;
        }

        // Multi joins of at most cap inputs each, an input that is a join gets an operator of its own
        internal static async Task<int> BuildMulti(MergeJoinRelation root, ITargetBlock<IStreamEvent> output, int cap, List<Host> hosts, ITargetBlock<IStreamEvent>[] targets, ExecutionDataflowBlockOptions options)
        {
            var host = new Host();
            hosts.Add(host);
            // A cap of two leaves every join an ordinary one
            var plan = cap < MultiJoinPlanBuilder.MinInputs ? null : MultiJoinPlanBuilder.Discover(root, cap);
            ISourceBlock<IStreamEvent> source;
            ITargetBlock<IStreamEvent>[] own;
            Relation[] inputs;
            if (plan == null)
            {
                // Fewer than three inputs are left: an ordinary join
                var join = new ColumnBulkMergeJoin(root, host.FunctionsRegister, options);
                source = join;
                await InitializeLinked(host, join, source, output);
                own = join.Targets;
                inputs = new[] { root.Left, root.Right };
            }
            else
            {
                var op = new MultiJoinOperator(plan, host.FunctionsRegister, options);
                source = op;
                await InitializeLinked(host, op, source, output);
                own = op.Targets;
                inputs = plan.Inputs.Select(x => x.Relation).ToArray();
            }
            int operators = 1;
            for (int i = 0; i < inputs.Length; i++)
            {
                if (inputs[i] is MergeJoinRelation child)
                {
                    operators += await BuildMulti(child, own[i], cap, hosts, targets, options);
                }
                else
                {
                    targets[int.Parse(((ReadRelation)inputs[i]).NamedTable.Names[0].Substring(1))] = own[i];
                }
            }
            return operators;
        }

        internal static async Task DisposeAll(List<Host> hosts)
        {
            foreach (var host in hosts)
            {
                try
                {
                    if (host.Operator != null)
                    {
                        await host.Operator.DisposeAsync();
                    }
                }
                finally
                {
                    // The storage goes also when its operator fails to let go
                    ((IDisposable)host).Dispose();
                }
            }
        }

        private static async Task InitializeLinked(Host host, IStreamVertex vertex, ISourceBlock<IStreamEvent> source, ITargetBlock<IStreamEvent> output)
        {
            host.Operator = vertex as IAsyncDisposable;
            source.LinkTo(output);
            await host.InitializeOperator(vertex);
        }

        private static async Task LoadChain(ITargetBlock<IStreamEvent>[] targets, BufferBlock<IStreamEvent> output)
        {
            int Columns(int input) => input == 0 || input == targets.Length - 1 ? 2 : 3;
            for (int k = 0; k < Keys; k += 256)
            {
                for (int input = 0; input < targets.Length; input++)
                {
                    await targets[input].SendAsync(CreateMessage(input, k, 256, 0, Columns(input)));
                }
            }
            foreach (var target in targets)
            {
                await target.SendAsync(new Checkpoint(1, 2, 1));
            }
            Assert.Equal(Keys, await Drain(output));
        }

        // One delete and one insert for each of the keys, for rows loaded with generation 0
        internal static StreamMessage<StreamEventBatch> CreateUpdate(int[] keys, int columnCount)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columns.Length; c++)
            {
                var column = Column.Create(allocator);
                bool isValue = c == columns.Length - 1;
                for (int k = 0; k < keys.Length; k++)
                {
                    column.Add(new Int64Value(isValue ? 0 : keys[k]));
                    column.Add(new Int64Value(isValue ? 1 : keys[k]));
                }
                columns[c] = column;
            }
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            for (int k = 0; k < keys.Length; k++)
            {
                weights.Add(-1);
                iterations.Add(0);
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        /// <summary>
        /// Prints the time of updates on a star whose state does not fit the cache and whose page reads wait, it asserts nothing about it.
        /// </summary>
        [Theory]
        [InlineData(5)]
        [InlineData(9)]
        public async Task PrintWaitingReads(int inputs)
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF") == null)
            {
                return;
            }
            int Setting(string name, int standard) => int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : standard;
            int keyCount = Setting("FLOWTIDE_MULTIJOIN_PERF_KEYS", 100_000);
            int pagesPerJoin = Setting("FLOWTIDE_MULTIJOIN_PERF_PAGES", 32);
            int delay = Setting("FLOWTIDE_MULTIJOIN_PERF_DELAY", 10);
            int messageCount = Setting("FLOWTIDE_MULTIJOIN_PERF_MESSAGES", 20);
            int keysPerMessage = Setting("FLOWTIDE_MULTIJOIN_PERF_BATCH", 16);
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            int Columns(int input) => input == 0 || input == inputs - 1 ? 2 : 3;
            bool parallelBefore = MultiJoinOperator.ParallelLookups;

            bool roundsBefore = MultiJoinOperator.Rounds;
            foreach (var mode in new[] { "chain", "multi join, lookups in turn", "multi join, message by message", "multi join" })
            {
                var hosts = new List<Host>();
                try
                {
                    Host.ReadDelayMs = 0;
                    MultiJoinOperator.ParallelLookups = mode != "multi join, lookups in turn";
                    MultiJoinOperator.Rounds = mode == "multi join";
                    // The same number of pages in total: the chain has one cache per join
                    Host.CachePages = mode == "chain" ? pagesPerJoin : pagesPerJoin * (inputs - 1);
                    var output = new BufferBlock<IStreamEvent>();
                    var targets = new ITargetBlock<IStreamEvent>[inputs];
                    await BuildMulti(ChainOf(inputs, true)[inputs - 2], output, mode == "chain" ? 2 : 16, hosts, targets, options);
                    for (int k = 0; k < keyCount; k += 512)
                    {
                        for (int input = 0; input < inputs; input++)
                        {
                            await targets[input].SendAsync(CreateMessage(input, k, Math.Min(512, keyCount - k), 0, Columns(input)));
                        }
                    }
                    foreach (var target in targets)
                    {
                        await target.SendAsync(new Checkpoint(1, 2, 1));
                    }
                    await Drain(output);

                    long time = 2;
                    // The same keys for every mode
                    var random = new Random(inputs);
                    var used = new HashSet<int>();
                    // -1: the messages go to the inputs in turn
                    foreach (int updated in new[] { 0, 1, -1 })
                    {
                        var messages = new List<StreamMessage<StreamEventBatch>>();
                        for (int m = 0; m < messageCount; m++)
                        {
                            var keys = new int[keysPerMessage];
                            for (int k = 0; k < keys.Length; k++)
                            {
                                int key;
                                do
                                {
                                    key = random.Next(keyCount);
                                }
                                while (!used.Add(key));
                                keys[k] = key;
                            }
                            messages.Add(CreateUpdate(keys, Columns(updated < 0 ? m % inputs : updated)));
                        }
                        Host.ReadDelayMs = delay;
                        long readsBefore = Interlocked.Read(ref Host.ReadsThatWaited);
                        var watch = System.Diagnostics.Stopwatch.StartNew();
                        long checkpointTime = time;
                        var send = Task.Run(async () =>
                        {
                            for (int i = 0; i < messages.Count; i++)
                            {
                                await targets[updated < 0 ? i % inputs : updated].SendAsync(messages[i]);
                            }
                            foreach (var target in targets)
                            {
                                await target.SendAsync(new Checkpoint(checkpointTime, checkpointTime + 1, 1));
                            }
                        });
                        var lastRow = watch.Elapsed;
                        long rows = 0;
                        while (true)
                        {
                            var received = await output.ReceiveAsync(TimeSpan.FromSeconds(600));
                            if (received is ICheckpointEvent)
                            {
                                break;
                            }
                            if (received is StreamMessage<StreamEventBatch> message)
                            {
                                rows += message.Data.Data.Count;
                                message.Data.Data.Return();
                                lastRow = watch.Elapsed;
                            }
                        }
                        await send;
                        Host.ReadDelayMs = 0;
                        time++;
                        long reads = Interlocked.Read(ref Host.ReadsThatWaited) - readsBefore;
                        _output.WriteLine($"PERF waiting reads, star of {inputs}, {mode}, updates on input {updated}: {lastRow.TotalMilliseconds:F0} ms for {messageCount} messages of {keysPerMessage * 2} rows, {rows} rows out, {reads} page reads that waited");
                    }
                }
                finally
                {
                    Host.ReadDelayMs = 0;
                    Host.CachePages = null;
                    MultiJoinOperator.ParallelLookups = parallelBefore;
                    MultiJoinOperator.Rounds = roundsBefore;
                    await DisposeAll(hosts);
                }
            }
        }

        /// <summary>
        /// Prints the memory the operators hold for their state after a load, it asserts nothing about it.
        /// </summary>
        [Theory]
        [InlineData(3, 0)]
        [InlineData(3, 100)]
        [InlineData(5, 0)]
        [InlineData(5, 100)]
        [InlineData(11, 0)]
        [InlineData(11, 100)]
        [InlineData(11, 400)]
        public async Task PrintStateSize(int inputs, int payloadWidth)
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF") == null)
            {
                return;
            }
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            _payloadWidth = payloadWidth;
            var hosts = new List<Host>();
            try
            {
                long before = LiveNativeBytes();
                var chainOutput = new BufferBlock<IStreamEvent>();
                var chainTargets = new ITargetBlock<IStreamEvent>[inputs];
                // A cap of two leaves every join an ordinary one
                await BuildMulti(ChainOf(inputs)[inputs - 2], chainOutput, 2, hosts, chainTargets, options);
                await LoadChain(chainTargets, chainOutput);
                long chainBytes = LiveNativeBytes() - before;
                var line = $"PERF state, {inputs} inputs, payload of {payloadWidth} characters: chain {chainBytes / 1024}k bytes";
                foreach (int cap in new[] { 5, 16 })
                {
                    if (cap == 5 && inputs <= 5)
                    {
                        continue;
                    }
                    before = LiveNativeBytes();
                    var output = new BufferBlock<IStreamEvent>();
                    var targets = new ITargetBlock<IStreamEvent>[inputs];
                    int operators = await BuildMulti(ChainOf(inputs)[inputs - 2], output, cap, hosts, targets, options);
                    await LoadChain(targets, output);
                    long multiBytes = LiveNativeBytes() - before;
                    line += $"; multi joins of at most {cap} inputs ({operators} operators) {multiBytes / 1024}k bytes ({(double)multiBytes / chainBytes:F2})";
                }
                _output.WriteLine(line);
            }
            finally
            {
                _payloadWidth = 0;
                await DisposeAll(hosts);
            }
        }

        [Theory]
        [InlineData(JoinType.Left)]
        [InlineData(JoinType.Outer)]
        public async Task JoinsThatKeepUnmatchedRowsAllocateNothingPerRow(JoinType type)
        {
            var small = await MeasureMulti(64, type);
            var large = await MeasureMulti(256, type);
            _output.WriteLine($"{type}: managed bytes per input row {small.managed:F1} at 128 rows per message, {large.managed:F1} at 512");
            // Two message sizes tell what is allocated per row from what is allocated per message, an object per row is 24 bytes
            double perRow = (4 * large.managed - small.managed) / 3;
            Assert.True(perRow <= 12, $"Managed bytes per input row whatever the message size: {perRow:F1} ({small.managed:F1} at 128 rows per message, {large.managed:F1} at 512)");
        }

        [Fact]
        public async Task LoadsThroughASmallPageCache()
        {
            // Five inputs with wide rows through a cache of a few pages: lookups and inserts run while pages go out and come back
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            var hosts = new List<Host>();
            _payloadWidth = 200;
            Host.CachePages = 48;
            try
            {
                var output = new BufferBlock<IStreamEvent>();
                var targets = new ITargetBlock<IStreamEvent>[5];
                Assert.Equal(1, await BuildMulti(ChainOf(5)[3], output, 9, hosts, targets, options));
                await LoadChain(targets, output);
                Assert.True(hosts[0].TemporaryBytes > 0, "No page left memory, the cache was not small enough to say anything");
            }
            finally
            {
                Host.CachePages = null;
                _payloadWidth = 0;
                await DisposeAll(hosts);
            }
        }

        [Fact]
        public async Task MixedTypeColumnsAreCopiedWithoutAnObjectPerValue()
        {
            var same = await MeasureMulti(256);
            (double managed, double native, long rows, double ms) mixed;
            _mixedValues = true;
            try
            {
                mixed = await MeasureMulti(256);
            }
            finally
            {
                _mixedValues = false;
            }
            _output.WriteLine($"Managed bytes per input row: one type {same.managed:F1}, two types {mixed.managed:F1}");
            Assert.Equal(same.rows, mixed.rows);
            // Storing such a column costs about 70 bytes a row in the trees, an object per copied value adds 48 to that
            Assert.True(mixed.managed <= same.managed + 96, $"Managed bytes per input row: one type {same.managed:F1}, two types {mixed.managed:F1}");
        }

        [Theory]
        [InlineData(1)]
        [InlineData(16)]
        [InlineData(256)]
        public async Task AllocatesNoMoreThanTheChain(int keysPerMessage)
        {
            // The least of three runs for both, a single run also has what other threads of the test host allocate
            var chain = await MeasureChain(keysPerMessage, measuredRuns: 3);
            var multi = await MeasureMulti(keysPerMessage, measuredRuns: 3);
            // In rounds too, on one operator so its scratch has the size of its rounds
            var inRounds = await MeasureMulti(keysPerMessage, rounds: true, measuredRuns: 3);
            _output.WriteLine($"{keysPerMessage * 2} rows per message, per input row: chain {chain.managed:F0} managed bytes and {chain.native:F2} native allocations ({chain.rows} output rows), multi join {multi.managed:F0} managed bytes and {multi.native:F2} native allocations ({multi.rows} output rows). Time: chain {chain.ms:F0} ms, multi join {multi.ms:F0} ms");
            Assert.Equal(chain.rows, multi.rows);
            Assert.Equal(chain.rows, inRounds.rows);
            // Other threads of the test host allocate a little too, a quarter megabyte of that is let through
            double slack = 262144.0 / (Math.Min(MaxMessages, Keys / keysPerMessage) * 3.0 * keysPerMessage * 2);
            // The chain is the bar: two ordinary joins doing the same work, measured the same way
            Assert.True(multi.managed <= chain.managed * 1.15 + slack, $"Managed bytes per input row: multi join {multi.managed:F0}, chain {chain.managed:F0}");
            Assert.True(multi.native <= chain.native * 1.15, $"Native allocations per input row: multi join {multi.native:F2}, chain {chain.native:F2}");
            Assert.True(inRounds.managed <= chain.managed * 1.15 + slack, $"Managed bytes per input row in rounds: multi join {inRounds.managed:F0}, chain {chain.managed:F0}");
            if (keysPerMessage == 256)
            {
                Assert.True(inRounds.managed <= 40, $"Managed bytes per input row in rounds: multi join {inRounds.managed:F0}");
                // About 19 bytes are measured and one object per row would add 24; the chain is no bar in a build that writes its rows out
                Assert.True(multi.managed <= 40, $"Managed bytes per input row: multi join {multi.managed:F0}");
            }
        }
    }
}
