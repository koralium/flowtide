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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// Shared between the run and its operators.
    /// </summary>
    internal sealed class LmControl
    {
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _requests = new ConcurrentDictionary<int, TaskCompletionSource>();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _sinkRounds = new ConcurrentDictionary<int, TaskCompletionSource>();
        private readonly ConcurrentDictionary<int, long> _sinkTicks = new ConcurrentDictionary<int, long>();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _asked = new ConcurrentDictionary<int, TaskCompletionSource>();
        private readonly IReadOnlyList<LmTable> _tables;
        private readonly object _sinkLock = new object();
        private long _upserts;
        private long _deletes;
        private long _loadRows;
        private long _churnRows;
        private long _generationTicks;
        private int _sinkRound = -1;

        public LmControl(long seed, bool shuffled, bool trackRows, IReadOnlyList<LmTable> tables)
        {
            Seed = seed;
            Shuffled = shuffled;
            _tables = tables;
            SinkRows = trackRows ? new Dictionary<string, string?[]>() : null;
            SinkWeights = trackRows ? new Dictionary<ulong, int>() : null;
        }

        public long Seed { get; }

        public bool Shuffled { get; }

        /// <summary>
        /// The sink's rows by key, kept for the self check only.
        /// </summary>
        public Dictionary<string, string?[]>? SinkRows { get; }

        /// <summary>
        /// The net weight of every row sent to the sink, by row hash.
        /// </summary>
        public Dictionary<ulong, int>? SinkWeights { get; }

        public IReadOnlyList<string> SinkColumns { get; set; } = [];

        public long Upserts => Interlocked.Read(ref _upserts);

        public long Deletes => Interlocked.Read(ref _deletes);

        public long LoadRows => Interlocked.Read(ref _loadRows);

        public long ChurnRows => Interlocked.Read(ref _churnRows);

        public double GenerationSeconds => Stopwatch.GetElapsedTime(0, Interlocked.Read(ref _generationTicks)).TotalSeconds;

        public void RequestRound(int round)
        {
            Signal(_requests, round).TrySetResult();
        }

        public Task WaitForRequest(int round, CancellationToken cancellationToken)
        {
            return Signal(_requests, round).Task.WaitAsync(cancellationToken);
        }

        public Task WaitForSink(int round)
        {
            return Signal(_sinkRounds, round).Task;
        }

        /// <summary>
        /// The run has asked for the round's checkpoint.
        /// </summary>
        public void CheckpointAsked(int round)
        {
            Signal(_asked, round).TrySetResult();
        }

        public Task WaitForCheckpointAsked(int round)
        {
            return Signal(_asked, round).Task;
        }

        public static ulong RowHash(string?[] values)
        {
            ulong hash = 0;
            for (int i = 0; i < values.Length; i++)
            {
                // Mixed once per column, so column borders count
                hash = LmData.Mix(hash ^ 0xFFFF);
                string? text = values[i];
                for (int c = 0; text != null && c < text.Length; c++)
                {
                    hash = LmData.Mix(hash ^ text[c]);
                }
            }
            return hash;
        }

        /// <summary>
        /// The timestamp at which the sink had the round.
        /// </summary>
        public long SinkTicks(int round)
        {
            return _sinkTicks[round];
        }

        /// <summary>
        /// A round has reached the sink when every table's watermark has.
        /// </summary>
        public void SinkWatermark(Watermark watermark)
        {
            long lowest = long.MaxValue;
            for (int i = 0; i < _tables.Count; i++)
            {
                if (!watermark.Watermarks.TryGetValue(_tables[i].Name, out var value) || value is not LongWatermarkValue number)
                {
                    return;
                }
                lowest = Math.Min(lowest, number.Value);
            }
            lock (_sinkLock)
            {
                // Watermark values are one above the round
                while (_sinkRound < lowest - 1)
                {
                    _sinkRound++;
                    _sinkTicks[_sinkRound] = Stopwatch.GetTimestamp();
                    Signal(_sinkRounds, _sinkRound).TrySetResult();
                }
            }
        }

        public void CountSourceRows(int round, int rows)
        {
            if (round == 0)
            {
                Interlocked.Add(ref _loadRows, rows);
            }
            else
            {
                Interlocked.Add(ref _churnRows, rows);
            }
        }

        public void CountGeneration(long ticks)
        {
            Interlocked.Add(ref _generationTicks, ticks);
        }

        public void CountSinkRow(bool deleted)
        {
            if (deleted)
            {
                Interlocked.Increment(ref _deletes);
            }
            else
            {
                Interlocked.Increment(ref _upserts);
            }
        }

        private static TaskCompletionSource Signal(ConcurrentDictionary<int, TaskCompletionSource> signals, int round)
        {
            return signals.GetOrAdd(round, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        }
    }

    internal sealed class LmSourceFactory : RegexConnectorSourceFactory
    {
        private readonly LmWorkload _workload;
        private readonly LmControl _control;

        public LmSourceFactory(LmWorkload workload, LmControl control) : base("*")
        {
            _workload = workload;
            _control = control;
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            var table = _workload.GetTable(readRelation.NamedTable.DotSeperated);
            return new LmSourceOperator(table, _control, readRelation, functionsRegister, dataflowBlockOptions);
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            if (readRelation.BaseSchema.Names.Contains(LmSourceOperator.KeyColumn))
            {
                return readRelation;
            }
            // The read emits the key too, a projection drops it again
            int columns = readRelation.OutputLength;
            int keyIndex = readRelation.BaseSchema.Names.Count;
            readRelation.BaseSchema.Names.Add(LmSourceOperator.KeyColumn);
            readRelation.BaseSchema.Struct?.Types.Add(new Int64Type());
            if (readRelation.EmitSet)
            {
                readRelation.Emit = new List<int>(readRelation.Emit!) { keyIndex };
            }
            var emit = new List<int>();
            for (int i = 0; i < columns; i++)
            {
                emit.Add(i);
            }
            return new ProjectRelation() { Input = readRelation, Expressions = new List<Expression>(), Emit = emit };
        }

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
        {
            return new TableLineageMetadata("lm", readRelation.NamedTable.DotSeperated, default);
        }

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
        {
            return [LmSourceOperator.KeyColumn];
        }
    }

    /// <summary>
    /// A keyed source that stores its rows like real connectors.
    /// </summary>
    internal sealed class LmSourceOperator : ColumnBatchReadBaseOperator
    {
        public const string KeyColumn = "id";

        private readonly LmTable _table;
        private readonly LmControl _control;
        private readonly int _batchRows;
        private readonly List<int> _tableColumns = new List<int>();
        private readonly int _keyIndex;
        private int _emittedRound;
        private int _roundLoopStarted;

        public LmSourceOperator(LmTable table, LmControl control, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options)
            : base(readRelation, functionsRegister, options)
        {
            _table = table;
            _control = control;
            // Batches of about two megabytes
            _batchRows = Math.Clamp(2 * 1024 * 1024 / Math.Max(1, table.RowBytes), 64, 8192);
            // The read lists the columns it wants in its own order
            var names = readRelation.BaseSchema.Names;
            for (int i = 0; i < names.Count; i++)
            {
                _tableColumns.Add(table.GetColumnIndex(names[i]));
            }
            _keyIndex = names.IndexOf(KeyColumn);
        }

        public override string DisplayName => $"{LmCensus.SourceDisplayName}({_table.Name})";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        protected override Task Checkpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _table.Name });
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult(new List<int>() { _keyIndex });
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await base.SendInitial(output);
            // Holds the first checkpoint until the sink has the load
            await _control.WaitForSink(0).WaitAsync(output.CancellationToken);
            // Rounds are asked for through the control, never by a trigger
            _ = RunTask(DeltaLoadTrigger);
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            var rows = new List<long>(_batchRows);
            long mark = Stopwatch.GetTimestamp();
            for (long index = 0; index < _table.Rows && !cancellationToken.IsCancellationRequested; index++)
            {
                rows.Add(LmData.RowAt(_control.Seed, _table, index, _control.Shuffled));
                if (rows.Count == _batchRows || index == _table.Rows - 1)
                {
                    var batch = CreateBatch(rows, 0, null);
                    _control.CountGeneration(Stopwatch.GetTimestamp() - mark);
                    yield return new ColumnReadEvent(batch, 1);
                    rows.Clear();
                    await Task.Yield();
                    mark = Stopwatch.GetTimestamp();
                }
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            // One loop serves every round, a second call has nothing to do
            if (Interlocked.Exchange(ref _roundLoopStarted, 1) == 1)
            {
                yield break;
            }
            while (true)
            {
                int round = _emittedRound + 1;
                bool cancelled = false;
                try
                {
                    await _control.WaitForRequest(round, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    cancelled = true;
                }
                if (cancelled)
                {
                    yield break;
                }
                await EnterCheckpointLock();
                try
                {
                    var rows = new List<long>(_batchRows);
                    var deleted = new List<bool>(_batchRows);
                    // The scan for changed rows is generator time too
                    long mark = Stopwatch.GetTimestamp();
                    for (long index = 0; index < _table.Rows && !cancellationToken.IsCancellationRequested; index++)
                    {
                        long row = LmData.RowAt(_control.Seed, _table, index, _control.Shuffled);
                        if (IsChanged(row, round, out bool delete))
                        {
                            rows.Add(row);
                            deleted.Add(delete);
                        }
                        if (rows.Count == _batchRows || (rows.Count > 0 && index == _table.Rows - 1))
                        {
                            var batch = CreateBatch(rows, round, deleted);
                            _control.CountGeneration(Stopwatch.GetTimestamp() - mark);
                            yield return new DeltaReadEvent(batch, null);
                            rows.Clear();
                            deleted.Clear();
                            mark = Stopwatch.GetTimestamp();
                        }
                    }
                    _control.CountGeneration(Stopwatch.GetTimestamp() - mark);
                    yield return new DeltaReadEvent(null, new Watermark(_table.Name, LongWatermarkValue.Create(round + 1)));
                }
                finally
                {
                    ExitCheckpointLock();
                }
                _emittedRound = round;
            }
        }

        private bool IsChanged(long row, int round, out bool delete)
        {
            delete = false;
            if (LmData.IsDeleted(_control.Seed, _table, row, round - 1))
            {
                return false;
            }
            delete = LmData.IsSelected(_control.Seed, _table, LmData.Delete, round, row);
            return delete ||
                LmData.IsSelected(_control.Seed, _table, LmData.PayloadUpdate, round, row) ||
                LmData.IsSelected(_control.Seed, _table, LmData.InlineUpdate, round, row);
        }

        private EventBatchWeighted CreateBatch(List<long> rows, int round, List<bool>? deleted)
        {
            var data = LmData.CreateBatch(_control.Seed, _table, _tableColumns, rows, round, MemoryAllocator);
            PrimitiveList<int>? weights = null;
            PrimitiveList<uint>? iterations = null;
            try
            {
                weights = new PrimitiveList<int>(MemoryAllocator);
                iterations = new PrimitiveList<uint>(MemoryAllocator);
                for (int i = 0; i < rows.Count; i++)
                {
                    weights.Add(deleted != null && deleted[i] ? -1 : 1);
                    iterations.Add(0);
                }
            }
            catch
            {
                data.Dispose();
                weights?.Dispose();
                iterations?.Dispose();
                throw;
            }
            _control.CountSourceRows(round, rows.Count);
            return new EventBatchWeighted(weights, iterations, data);
        }
    }

    internal sealed class LmSinkFactory : RegexConnectorSinkFactory
    {
        private readonly LmControl _control;
        private readonly int _keyColumns;

        public LmSinkFactory(LmControl control, int keyColumns) : base("*")
        {
            _control = control;
            _keyColumns = keyColumns;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            _control.SinkColumns = writeRelation.TableSchema.Names;
            return new LmSinkOperator(_control, _keyColumns, writeRelation, dataflowBlockOptions);
        }

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation)
        {
            return Enumerable.Range(0, _keyColumns).ToList();
        }

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
        {
            return new TableLineageMetadata("lm", writeRelation.NamedObject.DotSeperated, default);
        }
    }

    /// <summary>
    /// A keyed sink that stores its rows like real connectors.
    /// </summary>
    internal sealed class LmSinkOperator : ColumnGroupedWriteOperator
    {
        private readonly LmControl _control;
        private readonly int _keyColumns;

        public LmSinkOperator(LmControl control, int keyColumns, WriteRelation writeRelation, ExecutionDataflowBlockOptions options)
            : base(ExecutionMode.OnWatermark, writeRelation, options)
        {
            _control = control;
            _keyColumns = keyColumns;
        }

        public override string DisplayName => LmCensus.SinkDisplayName;

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            var keys = new List<int>();
            for (int i = 0; i < _keyColumns; i++)
            {
                keys.Add(i);
            }
            return ValueTask.FromResult<IReadOnlyList<int>>(keys);
        }

        protected override async Task OnWatermark(Watermark watermark)
        {
            // The upload for this watermark is done when the base returns
            await base.OnWatermark(watermark);
            _control.SinkWatermark(watermark);
        }

        // The weights it tracks read no column a fetch would bring
        protected override bool CanHostFetch => true;

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            if (_control.SinkWeights != null)
            {
                TrackWeights(msg.Data);
            }
            return base.OnRecieve(msg, time);
        }

        private void TrackWeights(EventBatchWeighted batch)
        {
            var columns = batch.EventBatchData.Columns;
            var values = new string?[columns.Count];
            lock (_control.SinkWeights!)
            {
                for (int row = 0; row < batch.Count; row++)
                {
                    for (int i = 0; i < values.Length; i++)
                    {
                        values[i] = LmData.Format(columns[i].GetValueAt(row, default));
                    }
                    ulong hash = LmControl.RowHash(values);
                    _control.SinkWeights.TryGetValue(hash, out var weight);
                    weight += batch.Weights[row];
                    if (weight == 0)
                    {
                        _control.SinkWeights.Remove(hash);
                    }
                    else
                    {
                        _control.SinkWeights[hash] = weight;
                    }
                }
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                _control.CountSinkRow(row.IsDeleted);
                if (_control.SinkRows != null)
                {
                    TrackRow(row);
                }
            }
        }

        private void TrackRow(ColumnWriteOperation row)
        {
            var key = new StringBuilder();
            for (int i = 0; i < _keyColumns; i++)
            {
                key.Append(LmData.Format(row.EventBatchData.Columns[i].GetValueAt(row.Index, default))).Append('|');
            }
            lock (_control.SinkRows!)
            {
                if (row.IsDeleted)
                {
                    _control.SinkRows.Remove(key.ToString());
                    return;
                }
                var values = new string?[row.EventBatchData.Columns.Count];
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = LmData.Format(row.EventBatchData.Columns[i].GetValueAt(row.Index, default));
                }
                _control.SinkRows[key.ToString()] = values;
            }
        }
    }
}
