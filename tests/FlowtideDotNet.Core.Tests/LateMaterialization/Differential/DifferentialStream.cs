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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    // Table of longs, strings and nulls, key first
    internal sealed class DifferentialTable
    {
        public DifferentialTable(string name, int columns)
        {
            Name = name;
            Columns = columns;
        }

        public string Name { get; }

        public int Columns { get; }

        public List<object?[]> Rows { get; } = new List<object?[]>();

        public ConcurrentQueue<(object?[] row, int weight)[]> Deltas { get; } = new ConcurrentQueue<(object?[] row, int weight)[]>();

        // Test hook, awaited before the source takes a change
        public Func<Task>? BeforeTake { get; set; }

        public Func<Task>? AfterDequeue { get; set; }

        // Changes given to the table, and changes its source has sent
        public int Enqueued;
        public int Sent;

        // Barriers the source has passed in this run
        public int Barriers;

        // Barriers passed when the last change was taken
        public int TakenAt = -1;

        public void Load(params object?[][] rows)
        {
            lock (Rows)
            {
                Rows.AddRange(rows);
            }
        }

        public void Upsert(params object?[][] rows)
        {
            lock (Rows)
            {
                foreach (var row in rows)
                {
                    Rows.RemoveAll(x => Equals(x[0], row[0]));
                    Rows.Add(row);
                }
            }
            Interlocked.Increment(ref Enqueued);
            Deltas.Enqueue(rows.Select(x => (x, 1)).ToArray());
        }

        public void Delete(params long[] keys)
        {
            var deleted = new List<(object?[] row, int weight)>();
            lock (Rows)
            {
                foreach (var key in keys)
                {
                    var row = Rows.Find(x => Equals(x[0], (object)key));
                    if (row != null)
                    {
                        Rows.Remove(row);
                        deleted.Add((row, -1));
                    }
                }
            }
            Interlocked.Increment(ref Enqueued);
            Deltas.Enqueue(deleted.ToArray());
        }
    }

    internal sealed class DifferentialSource : ColumnBatchReadBaseOperator
    {
        private readonly DifferentialTable _table;
        private long _watermark;

        public DifferentialSource(ReadRelation relation, DifferentialTable table, IFunctionsRegister functions)
            : base(relation, functions, new DataflowBlockOptions())
        {
            _table = table;
            // One row a page, so every release frees its page
            RowHeapOptions = new Core.Operators.LateMaterialization.RowHeapOptions() { PageTargetBytes = 1 };
        }

        public override string DisplayName => "differential source " + _table.Name;

        public override Task Compact() => Task.CompletedTask;

        protected override Task Checkpoint(long checkpointTime)
        {
            // Under the checkpoint lock, as a take is
            Interlocked.Increment(ref _table.Barriers);
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _table.Name });

        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int>() { 0 });

        private EventBatchWeighted Batch(IReadOnlyList<(object?[] row, int weight)> rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[_table.Columns];
            for (int c = 0; c < columns.Length; c++)
            {
                columns[c] = Column.Create(allocator);
            }
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var (row, weight) in rows)
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    switch (row[c])
                    {
                        case null:
                            columns[c].Add(NullValue.Instance);
                            break;
                        case long number:
                            columns[c].Add(new Int64Value(number));
                            break;
                        default:
                            columns[c].Add(new StringValue((string)row[c]!));
                            break;
                    }
                }
                weights.Add(weight);
                iterations.Add(0);
            }
            return new EventBatchWeighted(weights, iterations, new EventBatchData(columns));
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.Yield();
            (object?[] row, int weight)[] rows;
            lock (_table.Rows)
            {
                rows = _table.Rows.Select(x => (x, 1)).ToArray();
            }
            if (rows.Length > 0)
            {
                yield return new ColumnReadEvent(Batch(rows), ++_watermark);
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            while (!_table.Deltas.IsEmpty)
            {
                var beforeTake = _table.BeforeTake;
                if (beforeTake != null)
                {
                    await beforeTake();
                }
                await EnterCheckpointLock();
                try
                {
                    // Taken under the lock, no checkpoint can come between
                    if (_table.Deltas.TryDequeue(out var rows))
                    {
                        var afterDequeue = _table.AfterDequeue;
                        if (afterDequeue != null)
                        {
                            await afterDequeue();
                        }
                        if (rows.Length > 0)
                        {
                            Volatile.Write(ref _table.TakenAt, Volatile.Read(ref _table.Barriers));
                            yield return new DeltaReadEvent(Batch(rows), new Watermark(_table.Name, LongWatermarkValue.Create(++_watermark)));
                        }
                        // Counted last, a settle waits for it
                        Interlocked.Increment(ref _table.Sent);
                    }
                }
                finally
                {
                    ExitCheckpointLock();
                }
            }
        }
    }

    // Keeps the net weight of every row it was sent
    internal sealed class DifferentialSink() : EgressVertex<StreamEventBatch>(new ExecutionDataflowBlockOptions())
    {
        private readonly Dictionary<string, int> _rows = new Dictionary<string, int>();
        private int _checkpoints;

        public int Checkpoints => Volatile.Read(ref _checkpoints);

        public override string DisplayName => "differential sink";

        // Adds the net weights to what an earlier sink had
        public void AddTo(Dictionary<string, int> rows)
        {
            lock (_rows)
            {
                foreach (var row in _rows)
                {
                    rows.TryGetValue(row.Key, out var weight);
                    rows[row.Key] = weight + row.Value;
                }
            }
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            var data = msg.Data;
            var columns = data.EventBatchData.Columns;
            lock (_rows)
            {
                for (int i = 0; i < data.Count; i++)
                {
                    var parts = new string[columns.Count];
                    for (int c = 0; c < parts.Length; c++)
                    {
                        var value = columns[c].GetValueAt(i, default);
                        parts[c] = Text(value);
                    }
                    var row = string.Join("|", parts);
                    _rows.TryGetValue(row, out var weight);
                    weight += data.Weights.Get(i);
                    if (weight == 0)
                    {
                        _rows.Remove(row);
                    }
                    else
                    {
                        _rows[row] = weight;
                    }
                }
            }
            return Task.CompletedTask;
        }

        // A string that could be read as another value is quoted
        private static string Text<T>(T value) where T : IDataValue
        {
            switch (value.Type)
            {
                case ArrowTypeId.Null:
                    return "null";
                case ArrowTypeId.Int64:
                    return value.AsLong.ToString();
                case ArrowTypeId.Boolean:
                    return value.AsBool ? "true" : "false";
                case ArrowTypeId.String:
                    var text = value.AsString.ToString();
                    bool plain = text.Length > 0 && text != "null" && text != "true" && text != "false" && !long.TryParse(text, out _) && text.IndexOfAny(['|', '"']) < 0;
                    return plain ? text : System.Text.Json.JsonSerializer.Serialize(text);
                default:
                    throw new InvalidOperationException($"The sink got a value of type {value.Type}.");
            }
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            Interlocked.Increment(ref _checkpoints);
            return Task.CompletedTask;
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;

        public override Task Compact() => Task.CompletedTask;

        public override Task DeleteAsync() => Task.CompletedTask;
    }

    // One plan running over its own tables and storage
    internal sealed class DifferentialStream : IAsyncDisposable
    {
        private sealed class SourceFactory(DifferentialStream owner) : Core.Connectors.RegexConnectorSourceFactory("*")
        {
            public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            {
                return new DifferentialSource(readRelation, owner.Tables[readRelation.NamedTable.DotSeperated], functionsRegister);
            }

            public override Core.Lineage.TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            {
                return new Core.Lineage.TableLineageMetadata("test", readRelation.NamedTable.DotSeperated, default);
            }
        }

        private sealed class SinkFactory(DifferentialStream owner) : Core.Connectors.RegexConnectorSinkFactory("*")
        {
            public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            {
                return owner.Sink;
            }

            public override Core.Lineage.TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            {
                return new Core.Lineage.TableLineageMetadata("test", writeRelation.NamedObject.DotSeperated, default);
            }
        }

        private readonly string _name = "differential-" + Guid.NewGuid().ToString("N");
        private readonly ReservoirPersistentStorage _storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        private readonly Func<Plan> _plan;
        private readonly Action<FlowtideBuilder>? _configure;
        // Net weights of the sinks before a restart
        private readonly Dictionary<string, int> _restored = new Dictionary<string, int>();
        private Base.Engine.DataflowStream? _stream;

        public DifferentialStream(Func<Plan> plan, Action<FlowtideBuilder>? configure, params DifferentialTable[] tables)
        {
            _plan = plan;
            _configure = configure;
            foreach (var table in tables)
            {
                Tables.Add(table.Name, table);
            }
        }

        public Dictionary<string, DifferentialTable> Tables { get; } = new Dictionary<string, DifferentialTable>();

        // Every fault the stream reported, recovered or not
        public ConcurrentQueue<Exception?> Failures { get; } = new ConcurrentQueue<Exception?>();

        public DifferentialSink Sink { get; private set; } = new DifferentialSink();

        private sealed class CheckpointCounter(DifferentialStream owner) : ICheckpointListener
        {
            public void OnCheckpointComplete(StreamCheckpointNotification notification)
            {
                Interlocked.Increment(ref owner._completed);
                Interlocked.Increment(ref owner._runCompleted);
            }
        }

        private int _completed;
        private int _runCompleted;

        // Checkpoints that were saved, counted over restarts
        public int Completed => Volatile.Read(ref _completed);

        // Checkpoints saved in this run
        public int Saved => Volatile.Read(ref _runCompleted);

        // A checkpoint saved after every source took its last change
        public bool Covered
        {
            get
            {
                int saved = Volatile.Read(ref _runCompleted);
                return saved > 0 && Tables.Values.All(x => Volatile.Read(ref x.TakenAt) < saved);
            }
        }

        public Base.Engine.DataflowStream Stream => _stream!;

        public string Name => _name;

        public async Task StartAsync()
        {
            Volatile.Write(ref _runCompleted, 0);
            foreach (var table in Tables.Values)
            {
                Volatile.Write(ref table.Barriers, 0);
                Volatile.Write(ref table.TakenAt, -1);
            }
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new SourceFactory(this));
            connectorManager.AddSink(new SinkFactory(this));
            var builder = new FlowtideBuilder(_name)
                .AddPlan(_plan(), false)
                .AddConnectorManager(connectorManager)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = _storage })
                .WithFailureListener(e => Failures.Enqueue(e))
                .WithCheckpointListener(new CheckpointCounter(this));
            _configure?.Invoke(builder);
            _stream = builder.Build();
            await _stream.StartAsync();
        }

        // Stops the stream and starts it again from its storage
        public async Task RestartAsync()
        {
            await _stream!.StopAsync().WaitAsync(TimeSpan.FromSeconds(30));
            await _stream.DisposeAsync();
            Sink.AddTo(_restored);
            Sink = new DifferentialSink();
            await StartAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (_stream!.State != StreamStateValue.Running)
            {
                await Task.Delay(10, timeout.Token);
            }
        }

        public List<string> Result()
        {
            var rows = new Dictionary<string, int>(_restored);
            Sink.AddTo(rows);
            var result = new List<string>();
            foreach (var row in rows)
            {
                for (int i = 0; i < row.Value; i++)
                {
                    result.Add(row.Key);
                }
            }
            result.Sort(StringComparer.Ordinal);
            return result;
        }

        // Rows the sink holds below zero, kept apart from the result
        public List<string> Negative()
        {
            var rows = new Dictionary<string, int>(_restored);
            Sink.AddTo(rows);
            return rows.Where(x => x.Value < 0).Select(x => $"{x.Key} (weight {x.Value})").ToList();
        }

        public async Task TriggerAsync()
        {
            await _stream!.CallTrigger(ColumnBatchReadBaseOperator.DeltaLoadTriggerName, null);
        }

        public async ValueTask DisposeAsync()
        {
            if (_stream != null)
            {
                await _stream.DisposeAsync();
            }
            _storage.Dispose();
        }
    }

    // The same plan with and without row stores, on equal tables
    internal sealed class DifferentialPair : IAsyncDisposable
    {
        public DifferentialPair(Func<bool, Plan> plan, Func<DifferentialTable[]> tables, Action<FlowtideBuilder>? configure = null)
        {
            Hosted = new DifferentialStream(() => plan(true), configure, tables());
            Plain = new DifferentialStream(() => plan(false), configure, tables());
        }

        public DifferentialStream Hosted { get; }

        public DifferentialStream Plain { get; }

        public async Task StartAsync()
        {
            await Hosted.StartAsync();
            await Plain.StartAsync();
        }

        public void Change(Action<Dictionary<string, DifferentialTable>> change)
        {
            change(Hosted.Tables);
            change(Plain.Tables);
        }

        // How long a stream may take to settle
        public TimeSpan SettleTimeout { get; set; } = TimeSpan.FromSeconds(30);

        // Set once a check failed, dispose then stays quiet
        private bool _failed;

        public async Task RestartAsync()
        {
            try
            {
                await Hosted.RestartAsync();
                await Plain.RestartAsync();
                AssertNoFailures();
            }
            catch
            {
                _failed = true;
                throw;
            }
        }

        // Both streams must settle on these rows
        public async Task Expect(params string[] rows)
        {
            var expected = rows.ToList();
            expected.Sort(StringComparer.Ordinal);
            try
            {
                await Settle(Hosted, expected, "hosted");
                await Settle(Plain, expected, "plain");
                Assert.Equal(expected, Plain.Result());
                Assert.Equal(expected, Hosted.Result());
                Assert.Empty(Plain.Negative());
                Assert.Empty(Hosted.Negative());
                AssertNoFailures();
            }
            catch
            {
                _failed = true;
                throw;
            }
        }

        private static string Messages(Exception? exception)
        {
            if (exception is AggregateException aggregate)
            {
                return string.Join(" / ", aggregate.Flatten().InnerExceptions.Select(Messages));
            }
            return exception == null ? "no exception given" : exception.GetType().Name + ": " + exception.Message;
        }

        public void AssertNoFailures()
        {
            if (!Hosted.Failures.IsEmpty || !Plain.Failures.IsEmpty)
            {
                Assert.Fail("A stream failed. " + string.Join("; ", Hosted.Failures.Select(x => "hosted: " + Messages(x)).Concat(Plain.Failures.Select(x => "plain: " + Messages(x)))));
            }
        }

        // Waits for the rows and a checkpoint that covers them
        private async Task Settle(DifferentialStream stream, List<string> expected, string name)
        {
            var until = DateTime.UtcNow.Add(SettleTimeout);
            bool asked = false;
            while (true)
            {
                if (!stream.Failures.IsEmpty)
                {
                    Assert.Fail($"The {name} stream failed: {string.Join("; ", stream.Failures.Select(Messages))}");
                }
                if (stream.Tables.Values.Any(x => Volatile.Read(ref x.Sent) != Volatile.Read(ref x.Enqueued)))
                {
                    await stream.TriggerAsync();
                }
                else if (stream.Stream.State == StreamStateValue.Running && stream.Negative().Count == 0 && stream.Result().SequenceEqual(expected))
                {
                    if (stream.Covered)
                    {
                        return;
                    }
                    if (!asked && stream.Saved == 0)
                    {
                        // A restarted run with no change saves nothing by itself
                        asked = true;
                        await stream.Stream.TriggerCheckpoint();
                    }
                }
                if (DateTime.UtcNow > until)
                {
                    Assert.Fail($"The {name} stream did not settle: state {stream.Stream.State}, covered {stream.Covered}, rows [{string.Join(", ", stream.Result())}], below zero [{string.Join(", ", stream.Negative())}], expected [{string.Join(", ", expected)}]");
                }
                await Task.Delay(25);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Hosted.DisposeAsync();
            await Plain.DisposeAsync();
            if (!_failed)
            {
                AssertNoFailures();
            }
        }
    }
}
