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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// Joins several inputs and stores every input once, no join results are stored.
    /// </summary>
    internal sealed class MultiJoinOperator : MultipleInputRoundVertex<StreamEventBatch>
    {
        public const int DefaultRowBudget = 1024;

        /// <summary>
        /// Rows a lookup collects before it hands them on, lowered by tests.
        /// </summary>
        internal static int RowBudget = DefaultRowBudget;

        /// <summary>
        /// Store a batch while it is being joined, when nothing reads its own input before that is done.
        /// </summary>
        internal static bool ParallelInserts = true;

        // Below this a task costs more than it saves
        internal static int ParallelInsertMinRows = 256;

        /// <summary>
        /// When a lookup has to wait for a page, look up the later joins whose key rows are known beside it.
        /// </summary>
        internal static bool ParallelLookups = true;

        // Tests: every lookup counts as one that waits
        internal static bool ParallelLookupsAlways;

        // A lookup beside another keeps its rows, one that finds more than this is looked up in its turn instead
        internal static int ParallelLookupMaxRows = 65536;

        /// <summary>
        /// Take the batches that are queued together as a round: the inputs work side by side, the trees of an input serve them one at a time.
        /// </summary>
        internal static bool Rounds = true;

        // The most messages of a round, and the most rows it takes from one input
        internal static int RoundMessages = 256;
        internal static int RoundRowsPerInput = 4096;

        // A lookup of a round gives this many row budgets at a time: a batch in one part lets go of its trees for the next batch
        internal static int RoundPartBudgets = 8;

        private readonly MultiJoinPlan _plan;
        private readonly TaskScheduler _scheduler;
        private readonly IFunctionsRegister _functionsRegister;
        private readonly MultiJoinInputState[] _inputs;
        private readonly MultiJoinPadTable[] _tables;

        // Per input the cursor that carries its changes up
        private MultiJoinCursor[] _cursors = Array.Empty<MultiJoinCursor>();

        // One per input for the batches of a round, with lookups of their own. Empty when the plan has joins that keep unmatched rows
        private MultiJoinCursor[] _roundCursors = Array.Empty<MultiJoinCursor>();
        private ICounter<long>? _roundBatches;

        /// <summary>
        /// Holds every batch back until a flush, for tests.
        /// </summary>
        internal static bool DeferEveryBatch;

        /// <summary>
        /// Called when a batch taken on its own is about to be joined, for tests.
        /// </summary>
        internal static Action? JoinHookForTests;

        private ICounter<long>? _eventsCounter;
        /// <summary>
        /// Batches whose indexes were updated side by side, for tests.
        /// </summary>
        internal long ParallelIndexUpdates => _inputs.Sum(x => (long)x.ParallelIndexUpdates);

        private IHistogram<float>? _lookupMs;
        private IHistogram<float>? _insertMs;
        private IHistogram<float>? _padCorrectionMs;
        private ICounter<long>? _parallelLookups;
        private bool _deferredRowsGauge;
        private long _deferredRows;

        // The order the stored trees were written in, so rows are never read back in another one
        private IObjectState<string>? _storedLayout;
        private ICounter<long>? _eventsProcessed;

        public MultiJoinOperator(MultiJoinPlan plan, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
            : base(plan.Inputs.Count, executionDataflowBlockOptions)
        {
            _functionsRegister = functionsRegister;
            CompileChecks(plan.Root, functionsRegister);
            _plan = plan;
            _scheduler = executionDataflowBlockOptions.TaskScheduler;
            _inputs = new MultiJoinInputState[plan.Inputs.Count];
            for (int i = 0; i < _inputs.Length; i++)
            {
                _inputs[i] = new MultiJoinInputState(plan.Inputs[i], i);
            }
            _tables = new MultiJoinPadTable[plan.PadTables.Count];
            for (int i = 0; i < _tables.Length; i++)
            {
                _tables[i] = new MultiJoinPadTable(plan.PadTables[i].Join, plan.PadTables[i].KeptLeft, i);
            }
        }

        private static void CompileChecks(MultiJoinNode node, IFunctionsRegister functionsRegister)
        {
            if (node.IsLeaf)
            {
                return;
            }
            foreach (var check in node.Checks)
            {
                check.Compile(functionsRegister);
            }
            node.KeptOnlyCheck?.Compile(functionsRegister);
            CompileChecks(node.Left!, functionsRegister);
            CompileChecks(node.Right!, functionsRegister);
        }

        /// <summary>
        /// The pad tables, one per left join, for tests.
        /// </summary>
        internal IReadOnlyList<MultiJoinPadTable> PadTables => _tables;

        // Tests: step runs that compared before the copy, over every cursor
        internal long FilteredRunsForTests
        {
            get
            {
                long runs = 0;
                foreach (var cursor in _cursors)
                {
                    runs += cursor.FilteredRuns;
                }
                foreach (var cursor in _roundCursors)
                {
                    runs += cursor.FilteredRuns;
                }
                foreach (var table in _tables)
                {
                    runs += table.MatchTotals.FilteredRuns + table.PaddedRows.FilteredRuns;
                }
                return runs;
            }
        }

        /// <summary>
        /// Whether a batch is held back for a later flush, for tests.
        /// </summary>
        internal bool HasDeferredRows => _inputs.Any(x => x.Defer.HasPending);

        /// <summary>
        /// The most rows one lookup part has given in any cursor, for tests.
        /// </summary>
        internal int MaxPartRows
        {
            get
            {
                int max = 0;
                foreach (var cursor in _cursors)
                {
                    max = Math.Max(max, cursor.MaxPartRows);
                }
                foreach (var cursor in _roundCursors)
                {
                    max = Math.Max(max, cursor.MaxPartRows);
                }
                foreach (var table in _tables)
                {
                    max = Math.Max(max, Math.Max(table.MatchTotals.MaxPartRows, table.PaddedRows.MaxPartRows));
                }
                return max;
            }
        }

        public override string DisplayName => "Multi Join";

        public override Task Compact() => Task.CompletedTask;

        public override Task DeleteAsync() => Task.CompletedTask;

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            _eventsCounter ??= Metrics.CreateCounter<long>("events");
            _eventsProcessed ??= Metrics.CreateCounter<long>("events_processed");
            _lookupMs ??= Metrics.CreateHistogram<float>("lookup_ms");
            _insertMs ??= Metrics.CreateHistogram<float>("insert_ms");
            _padCorrectionMs ??= Metrics.CreateHistogram<float>("pad_correction_ms");
            _roundBatches ??= Metrics.CreateCounter<long>("round_batches");
            _parallelLookups ??= Metrics.CreateCounter<long>("parallel_lookups");
            if (!_deferredRowsGauge)
            {
                _deferredRowsGauge = true;
                Metrics.CreateObservableGauge<long>("deferred_rows", () => Volatile.Read(ref _deferredRows));
            }
            Volatile.Write(ref _deferredRows, 0);

            // A message cut short by the failure may have left lookups open
            AbandonLookups();
            _storedLayout = await stateManagerClient.GetOrCreateObjectStateAsync<string>("layout");
            string layout = DescribeLayout();
            if (_storedLayout.Value == null)
            {
                _storedLayout.Value = layout;
            }
            else if (_storedLayout.Value != layout)
            {
                throw new InvalidOperationException("The rows this multi join has stored are ordered for other lookups than its plan now makes. The stream has to be started under a new version.");
            }
            foreach (var input in _inputs)
            {
                // Nothing buffered before a rollback may be applied after it
                input.Defer.ReturnPending();
                input.Defer.ExitBuffering();
                await input.Initialize(stateManagerClient, MemoryAllocator);
            }
            DisposeCursors();
            _cursors = new MultiJoinCursor[_inputs.Length];
            for (int i = 0; i < _cursors.Length; i++)
            {
                _cursors[i] = new MultiJoinCursor(_plan, MultiJoinProgram.ForChange(_plan, i), _inputs, _tables, MemoryAllocator, RowBudget, _scheduler);
            }
            if (_tables.Length == 0)
            {
                int partRows = (int)Math.Min((long)RowBudget * RoundPartBudgets, 1 << 30);
                _roundCursors = new MultiJoinCursor[_inputs.Length];
                for (int i = 0; i < _roundCursors.Length; i++)
                {
                    _roundCursors[i] = new MultiJoinCursor(_plan, MultiJoinProgram.ForChange(_plan, i), _inputs, _tables, MemoryAllocator, partRows, _scheduler, true);
                    _roundCursors[i].CompileOwnConditions(_functionsRegister);
                }
            }
            foreach (var table in _tables)
            {
                await table.Initialize(stateManagerClient, MemoryAllocator);
                table.MatchTotals = new MultiJoinCursor(_plan, MultiJoinProgram.ForMatchTotals(_plan, table.Join, table.KeptLeft), _inputs, _tables, MemoryAllocator, RowBudget);
                table.PaddedRows = new MultiJoinCursor(_plan, MultiJoinProgram.ForPaddedRows(_plan, table.Join, table.KeptLeft), _inputs, _tables, MemoryAllocator, RowBudget);
            }
        }

        public override async Task OnCheckpoint()
        {
            foreach (var input in _inputs)
            {
                if (input.Defer.HasPending)
                {
                    throw new InvalidOperationException("Deferred join data pending at checkpoint, OnCheckpointFlush should have flushed it.");
                }
                input.ReleaseScratch();
                await input.Commit();
            }
            await _storedLayout!.Commit();
        }

        private string DescribeLayout()
        {
            var parts = new List<string>(_plan.Inputs.Count);
            foreach (var input in _plan.Inputs)
            {
                parts.Add($"{input.ColumnCount}:{string.Join(",", input.PrimaryOrder)}|{string.Join("|", input.SecondaryLayouts.Select(x => string.Join(",", x)))}");
            }
            return string.Join(";", parts);
        }

        protected override int MaxRoundMessages => Rounds && !DeferEveryBatch ? RoundMessages : 1;

        protected override long MaxRoundCostPerInput => RoundRowsPerInput;

        protected override long RoundCost(StreamEventBatch message) => message.Data.Weights.Count;

        public override IAsyncEnumerable<StreamEventBatch> OnRecieveRound(IReadOnlyList<RoundMessage<StreamEventBatch>> messages, long time)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                if (_inputs[messages[i].TargetId].Defer.Buffering)
                {
                    // An input that holds its batches back takes them one by one
                    return base.OnRecieveRound(messages, time);
                }
            }
            return ProcessRound(messages);
        }

        // The batches of one input as one batch, the rows in the order they arrived
        private StreamEventBatch Merge(List<StreamEventBatch> batches)
        {
            int rows = 0;
            for (int b = 0; b < batches.Count; b++)
            {
                rows += batches[b].Data.Weights.Count;
            }
            var columns = new IColumn[batches[0].Data.EventBatchData.Columns.Count];
            PrimitiveList<int>? weights = null;
            PrimitiveList<uint>? iterations = null;
            try
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    var column = Column.Create(MemoryAllocator);
                    columns[c] = column;
                    for (int b = 0; b < batches.Count; b++)
                    {
                        var data = batches[b].Data;
                        MultiJoinTreeLookup.AppendRows(column, data.EventBatchData.Columns[c], data.Weights.Count, ref _mergePositions);
                    }
                }
                weights = new PrimitiveList<int>(MemoryAllocator, rows);
                iterations = new PrimitiveList<uint>(MemoryAllocator, rows);
                for (int b = 0; b < batches.Count; b++)
                {
                    var data = batches[b].Data;
                    weights.AddRangeFrom(data.Weights, 0, data.Weights.Count);
                    iterations.AddRangeFrom(data.Iterations, 0, data.Weights.Count);
                }
            }
            catch
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c]?.Dispose();
                }
                weights?.Dispose();
                iterations?.Dispose();
                throw;
            }
            var merged = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            merged.Rent(1);
            return merged;
        }

        private int[] _mergePositions = Array.Empty<int>();

        /// <summary>
        /// Rounds whose batches worked side by side, for tests.
        /// </summary>
        internal long RoundsSideBySide { get; private set; }

        // Tests: called by the store and by the read of a batch in a round, with its input
        internal static Action<int>? RoundStoreHookForTests;
        internal static Action<int>? RoundReadHookForTests;

        private async IAsyncEnumerable<StreamEventBatch> ProcessRound(IReadOnlyList<RoundMessage<StreamEventBatch>> messages)
        {
            var batches = new StreamEventBatch?[_inputs.Length];
            List<StreamEventBatch>?[]? several = null;
            int participants = 0;
            int taken = 0;
            for (int i = 0; i < messages.Count; i++)
            {
                var message = messages[i].Message;
                int input = messages[i].TargetId;
                _eventsProcessed!.Add(message.Data.Weights.Count);
                if (message.Data.Weights.Count == 0)
                {
                    continue;
                }
                taken++;
                if (batches[input] == null)
                {
                    batches[input] = message;
                    participants++;
                    continue;
                }
                several ??= new List<StreamEventBatch>?[_inputs.Length];
                (several[input] ??= new List<StreamEventBatch>() { batches[input]! }).Add(message);
            }
            if (participants == 0)
            {
                yield break;
            }
            _roundBatches!.Add(taken);
            try
            {
                for (int i = 0; several != null && i < several.Length; i++)
                {
                    if (several[i] != null)
                    {
                        batches[i] = Merge(several[i]!);
                    }
                }
                if (participants == 1 || _roundCursors.Length == 0)
                {
                    // A join that keeps unmatched rows comes back to its trees to correct the rows it filled with nulls, its batches are taken in turn
                    for (int input = 0; input < batches.Length; input++)
                    {
                        if (batches[input] == null)
                        {
                            continue;
                        }
                        await foreach (var output in Process(input, batches[input]!))
                        {
                            yield return output;
                        }
                    }
                    yield break;
                }

                RoundsSideBySide++;
                var turns = new MultiJoinTreeTurn[_inputs.Length];
                for (int t = 0; t < turns.Length; t++)
                {
                    turns[t] = new MultiJoinTreeTurn(participants);
                }
                // A cursor that waits for a tree should not also wait for room here longer than it has to
                var parts = Channel.CreateBounded<StreamEventBatch>(new BoundedChannelOptions(participants * 2) { SingleReader = true });
                var work = new List<Task>(participants * 2);
                Exception? notStarted = null;
                try
                {
                    StartRound(batches, turns, parts.Writer, work);
                }
                catch (Exception e)
                {
                    // The tasks that did start read the batches of the round, they end before the failure is told
                    notStarted = e;
                }
                var ended = EndRound(work, parts.Writer);
                if (notStarted != null)
                {
                    await LetGoOfUnread(parts.Reader);
                    var alsoFailed = await ended;
                    if (alsoFailed != null)
                    {
                        // The cause first, a task that failed after it second
                        throw new AggregateException(notStarted, alsoFailed);
                    }
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(notStarted).Throw();
                }
                // True while a part is with whoever reads: leaving then is not a failure of the round
                bool handedOut = false;
                try
                {
                    await foreach (var part in parts.Reader.ReadAllAsync())
                    {
                        _eventsCounter!.Add(part.Data.Weights.Count);
                        handedOut = true;
                        try
                        {
                            yield return part;
                            handedOut = false;
                        }
                        finally
                        {
                            // Whoever took the part has rented it by now, also when nobody reads on
                            part.Return();
                        }
                    }
                }
                finally
                {
                    await LetGoOfUnread(parts.Reader);
                    var failure = await ended;
                    if (handedOut && failure != null)
                    {
                        // Nobody read on, so nobody was told that a batch of the round failed
                        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
                    }
                }
            }
            finally
            {
                for (int i = 0; several != null && i < several.Length; i++)
                {
                    if (several[i] != null && batches[i] != null && !ReferenceEquals(batches[i], several[i]![0]))
                    {
                        batches[i]!.Return();
                    }
                }
            }
        }

        private void StartRound(StreamEventBatch?[] batches, MultiJoinTreeTurn[] turns, ChannelWriter<StreamEventBatch> parts, List<Task> work)
        {
            int place = 0;
            for (int i = 0; i < batches.Length; i++)
            {
                var batch = batches[i];
                if (batch == null)
                {
                    continue;
                }
                var columns = batch.Data.EventBatchData.Columns;
                for (int c = 0; c < columns.Count; c++)
                {
                    // A view works out on first use whether it holds nulls and keeps the answer, two threads may not do that at once
                    if (columns[c] is ColumnWithOffset view && view.InnerColumn is Column)
                    {
                        view.GetColumnState();
                    }
                }
                int input = i;
                int own = place++;
                work.Add(Task.Factory.StartNew(() => ReadInRound(input, own, batch, turns, parts), CancellationToken.None, TaskCreationOptions.None, _scheduler).Unwrap());
                work.Add(Task.Factory.StartNew(() => StoreInRound(input, own, batch, turns), CancellationToken.None, TaskCreationOptions.None, _scheduler).Unwrap());
            }
        }

        // The parts end when every task of the round has: with its failure, also when a task ends canceled. Gives that failure
        private static async Task<Exception?> EndRound(List<Task> work, ChannelWriter<StreamEventBatch> parts)
        {
            try
            {
                await Task.WhenAll(work).ConfigureAwait(false);
                parts.TryComplete();
                return null;
            }
            catch (Exception e)
            {
                parts.TryComplete(e);
                return e;
            }
        }

        // Reached early when nobody reads on, or after a failure: the tasks of the round still end, what they give is let go of
        private static async ValueTask LetGoOfUnread(ChannelReader<StreamEventBatch> parts)
        {
            try
            {
                while (await parts.WaitToReadAsync().ConfigureAwait(false))
                {
                    while (parts.TryRead(out var unread))
                    {
                        unread.Return();
                    }
                }
            }
            catch
            {
                // The failure of the round was given to the one who read it, or is not the one being told now
            }
        }

        private async Task ReadInRound(int input, int place, StreamEventBatch batch, MultiJoinTreeTurn[] turns, ChannelWriter<StreamEventBatch> parts)
        {
            var cursor = _roundCursors[input];
            long lookupTicks = 0;
            cursor.EnterRound(turns, place);
            try
            {
                RoundReadHookForTests?.Invoke(input);
                if (cursor.Start(batch.Data))
                {
                    while (true)
                    {
                        // Only the time spent here is counted, not what the parts wait for downstream
                        long started = Stopwatch.GetTimestamp();
                        bool more = await cursor.MoveNext();
                        lookupTicks += Stopwatch.GetTimestamp() - started;
                        if (!more)
                        {
                            break;
                        }
                        var part = cursor.Current;
                        // The columns of a part go back when the cursor moves on
                        part.Rent(1);
                        try
                        {
                            await parts.WriteAsync(part);
                        }
                        catch
                        {
                            part.Return();
                            throw;
                        }
                    }
                }
            }
            catch
            {
                cursor.Reset();
                throw;
            }
            finally
            {
                cursor.LeaveRound();
                // Also the trees the batch never came to
                for (int t = 0; t < turns.Length; t++)
                {
                    if (t != input)
                    {
                        turns[t].Release(place);
                    }
                }
                long beside = cursor.TakeLookupsRunBeside();
                if (beside > 0)
                {
                    _parallelLookups!.Add(beside);
                }
            }
            _lookupMs!.Record((float)Stopwatch.GetElapsedTime(0, lookupTicks).TotalMilliseconds);
        }

        private async Task StoreInRound(int input, int place, StreamEventBatch batch, MultiJoinTreeTurn[] turns)
        {
            try
            {
                // The batches ahead read this input as it was, the ones behind find these rows stored
                await turns[input].Enter(place);
                RoundStoreHookForTests?.Invoke(input);
                await Insert(input, batch, batch.Data.Weights.Count, false);
            }
            finally
            {
                turns[input].Release(place);
            }
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            _eventsProcessed!.Add(msg.Data.Weights.Count);
            if (_inputs[targetId].Defer.Buffering || DeferEveryBatch)
            {
                return BufferBatch(targetId, msg);
            }
            return Process(targetId, msg);
        }

        private async IAsyncEnumerable<StreamEventBatch> BufferBatch(int targetId, StreamEventBatch msg)
        {
            if (msg.Data.Weights.Count == 0)
            {
                yield break;
            }
            Interlocked.Add(ref _deferredRows, msg.Data.Weights.Count);
            if (_inputs[targetId].Defer.Add(msg.Data))
            {
                await foreach (var batch in FlushPending(targetId))
                {
                    yield return batch;
                }
            }
        }

        private async IAsyncEnumerable<StreamEventBatch> FlushPending(int targetId)
        {
            var buffer = _inputs[targetId].Defer;
            if (!buffer.HasPending)
            {
                yield break;
            }
            var combined = buffer.TakePending(MemoryAllocator);
            Interlocked.Add(ref _deferredRows, -combined.Count);
            try
            {
                await foreach (var batch in Process(targetId, new StreamEventBatch(combined)))
                {
                    yield return batch;
                }
            }
            finally
            {
                combined.Return();
            }
        }

        private async IAsyncEnumerable<StreamEventBatch> FlushAll()
        {
            for (int i = 0; i < _inputs.Length; i++)
            {
                await foreach (var batch in FlushPending(i))
                {
                    yield return batch;
                }
            }
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            await foreach (var batch in FlushAll())
            {
                yield return batch;
            }
            foreach (var input in _inputs)
            {
                input.Defer.ExitBuffering();
            }
        }

        // Checkpoints arrive independently of watermarks
        protected override IAsyncEnumerable<StreamEventBatch> OnCheckpointFlush()
        {
            return FlushAll();
        }

        // Buffered rows must be visible to a loop before it can settle
        protected override IAsyncEnumerable<StreamEventBatch> OnLockingEventPrepare()
        {
            return FlushAll();
        }

        public override ValueTask DisposeAsync()
        {
            AbandonLookups();
            foreach (var input in _inputs)
            {
                input.Defer.ReturnPending();
                input.ReleaseScratch();
            }
            Volatile.Write(ref _deferredRows, 0);
            DisposeCursors();
            return base.DisposeAsync();
        }

        private Task StartInsert(int targetId, StreamEventBatch msg, int count)
        {
            var columns = msg.Data.EventBatchData.Columns;
            for (int i = 0; i < columns.Count; i++)
            {
                // A view works out on first use whether it holds nulls and keeps the answer, two threads may not do that at once
                if (columns[i] is ColumnWithOffset view && view.InnerColumn is Column)
                {
                    view.GetColumnState();
                }
            }
            return Task.Factory.StartNew(
                () => Insert(targetId, msg, count, true),
                CancellationToken.None,
                TaskCreationOptions.None,
                _scheduler).Unwrap();
        }

        private async Task Insert(int targetId, StreamEventBatch msg, int count, bool parallel)
        {
            long started = Stopwatch.GetTimestamp();
            // A large batch updates the indexes of its input side by side
            await _inputs[targetId].Insert(msg.Data.EventBatchData, msg.Data.Weights, count, MemoryAllocator, parallel || (ParallelInserts && count >= ParallelInsertMinRows) ? _scheduler : null);
            _insertMs!.Record((float)Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }

        private void AbandonLookups()
        {
            foreach (var cursor in _cursors)
            {
                cursor.Reset();
            }
            foreach (var cursor in _roundCursors)
            {
                cursor.Reset();
            }
            foreach (var table in _tables)
            {
                table.MatchTotals?.Reset();
                table.PaddedRows?.Reset();
                table.Abandon();
            }
            foreach (var input in _inputs)
            {
                input.AbandonLookups();
            }
        }

        private void DisposeCursors()
        {
            foreach (var cursor in _cursors)
            {
                cursor.Dispose();
            }
            _cursors = Array.Empty<MultiJoinCursor>();
            foreach (var cursor in _roundCursors)
            {
                cursor.Dispose();
            }
            _roundCursors = Array.Empty<MultiJoinCursor>();
            foreach (var table in _tables)
            {
                table.MatchTotals?.Dispose();
                table.PaddedRows?.Dispose();
            }
        }

        private async IAsyncEnumerable<StreamEventBatch> Process(int targetId, StreamEventBatch msg)
        {
            int count = msg.Data.Weights.Count;
            if (count == 0)
            {
                yield break;
            }
            var cursor = _cursors[targetId];
            bool joined = false;
            bool handedOut = false;
            // A pad table reads the rows of this input as they were, the batch may not be stored before it is drained
            Task? insert = null;
            if (ParallelInserts && count >= ParallelInsertMinRows && cursor.Program.TableJoins.Length == 0)
            {
                insert = StartInsert(targetId, msg, count);
            }
            long lookupTicks = 0;
            long padTicks = 0;
            try
            {
                JoinHookForTests?.Invoke();
                if (cursor.Start(msg.Data))
                {
                    // Only the time spent here is counted, not what the rows wait for downstream
                    while (true)
                    {
                        long started = Stopwatch.GetTimestamp();
                        bool more = await cursor.MoveNext();
                        lookupTicks += Stopwatch.GetTimestamp() - started;
                        if (!more)
                        {
                            break;
                        }
                        _eventsCounter!.Add(cursor.CurrentCount);
                        handedOut = true;
                        yield return cursor.Current;
                        handedOut = false;
                    }
                    // Lowest join first: the padded rows one left join adds or removes are a change for the ones above it
                    var tableJoins = cursor.Program.TableJoins;
                    for (int t = 0; t < tableJoins.Length; t++)
                    {
                        var table = _tables[tableJoins[t]];
                        if (!table.HasRows)
                        {
                            continue;
                        }
                        await using var drained = table.Drain().GetAsyncEnumerator();
                        while (true)
                        {
                            long started = Stopwatch.GetTimestamp();
                            bool more = await drained.MoveNextAsync();
                            padTicks += Stopwatch.GetTimestamp() - started;
                            if (!more)
                            {
                                break;
                            }
                            _eventsCounter!.Add(drained.Current.Data.Weights.Count);
                            handedOut = true;
                            yield return drained.Current;
                            handedOut = false;
                        }
                    }
                    _lookupMs!.Record((float)Stopwatch.GetElapsedTime(0, lookupTicks).TotalMilliseconds);
                    long beside = cursor.TakeLookupsRunBeside();
                    if (beside > 0)
                    {
                        _parallelLookups!.Add(beside);
                    }
                    if (tableJoins.Length > 0)
                    {
                        _padCorrectionMs!.Record((float)Stopwatch.GetElapsedTime(0, padTicks).TotalMilliseconds);
                    }
                }
                joined = true;
            }
            finally
            {
                // A catch cannot hold a yield, so the flag tells a run that was cut short
                if (!joined)
                {
                    AbandonLookups();
                    if (insert != null && handedOut)
                    {
                        // Nobody took the rest of the rows, there is no failure of the join to report before the one of the insert
                        await insert;
                    }
                    else if (insert != null)
                    {
                        // The batch is not let go of while it is being stored, the failure of the join is the one reported
                        await insert.ContinueWith(static x => _ = x.Exception, TaskScheduler.Default);
                    }
                }
            }
            if (insert != null)
            {
                await insert;
            }
            else
            {
                // The other inputs were read as they were before this batch, now it becomes part of the stored rows
                await Insert(targetId, msg, count, false);
            }
        }
    }
}
