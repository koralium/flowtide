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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockDataSourceState
    {
        public int LatestOffset { get; set; }
    }
    internal class MockDataSourceOperator : ReadBaseOperator
    {
#if DEBUG_WRITE
        private StreamWriter? allOutput;
#endif

        private readonly ReadRelation readRelation;
        private readonly MockDatabase mockDatabase;
        private HashSet<string> _watermarkNames;
        private MockTable _table;
        private IObjectState<MockDataSourceState>? _state;
        private BatchConverter _batchConverter;

        public static Dictionary<string, System.Threading.Tasks.TaskCompletionSource> TableInitialSignals { get; } = new Dictionary<string, System.Threading.Tasks.TaskCompletionSource>();
        public static Dictionary<string, string> TableWaitSignals { get; } = new Dictionary<string, string>();

        private readonly TimeSpan? _initialDataDelay;
        private readonly bool _failInitialize;

        public MockDataSourceOperator(ReadRelation readRelation, MockDatabase mockDatabase, DataflowBlockOptions options, TimeSpan? initialDataDelay = null, bool failInitialize = false) : base(options)
        {
            this.readRelation = readRelation;
            this.mockDatabase = mockDatabase;
            _initialDataDelay = initialDataDelay;
            _failInitialize = failInitialize;

            _table = mockDatabase.GetTable(readRelation.NamedTable.DotSeperated);

            _watermarkNames = new HashSet<string>() { readRelation.NamedTable.DotSeperated };
            _batchConverter = BatchConverter.GetBatchConverter(_table.Type, readRelation.BaseSchema.Names);
        }

        public override string DisplayName => "Mock data source";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        private async Task FetchChanges(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            // The checkpoint lock is entered before the database lock, and the database lock
            // is not held while sending. Both waits can park indefinitely: the checkpoint
            // lock while a checkpoint is in flight, a send on the pause gate while the
            // stream is paused. Holding a database slot across either wait starves the data
            // generator (and, with the synchronous wait that used to sit here, a thread pool
            // thread per fired trigger), deadlocking a test that pauses a stream and then
            // generates data.
            await output.EnterCheckpointLock();
            await mockDatabase.RwLock.WaitAsync();

            List<StreamEventBatch> pendingBatches = new List<StreamEventBatch>();
            int fetchedOffset;
            try
            {
                var (operations, offset) = _table.GetOperations(_state.Value.LatestOffset);
                fetchedOffset = offset;
                Logger.LogDebug("Mock source {table} fetch changes from offset {offset} to offset {fetchedOffset}", readRelation.NamedTable.DotSeperated, _state.Value.LatestOffset, fetchedOffset);

                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                Column[] columns = new Column[readRelation.OutputLength];

                for (int i = 0; i < readRelation.OutputLength; i++)
                {
                    columns[i] = new Column(MemoryAllocator);
                }

                foreach (var operation in operations)
                {
                    _batchConverter.AppendToColumns(operation.Object, columns);

                    iterations.Add(1);
                    if (operation.IsDelete)
                    {
                        weights.Add(-1);
                    }
                    else
                    {
                        weights.Add(1);
                    }

                    if (weights.Count > 100)
                    {
                        pendingBatches.Add(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));

                        columns = new Column[readRelation.OutputLength];
                        for (int i = 0; i < readRelation.OutputLength; i++)
                        {
                            columns[i] = new Column(MemoryAllocator);
                        }
                        weights = new PrimitiveList<int>(MemoryAllocator);
                        iterations = new PrimitiveList<uint>(MemoryAllocator);
                    }
                }

                if (weights.Count > 0)
                {
                    pendingBatches.Add(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                }
                else
                {
                    weights.Dispose();
                    iterations.Dispose();
                    foreach (var column in columns)
                    {
                        column.Dispose();
                    }
                }
            }
            finally
            {
                mockDatabase.RwLock.Release();
            }

            foreach (var outputBatch in pendingBatches)
            {
#if DEBUG_WRITE
                foreach (var o in outputBatch.Events)
                {
                    allOutput!.WriteLine($"{o.Weight} {o.ToJson()}");
                }
                await allOutput!.FlushAsync();
#endif
                await output.SendAsync(outputBatch);
            }

            _state.Value.LatestOffset = fetchedOffset;

            if (pendingBatches.Count > 0)
            {
                await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(fetchedOffset)));
                this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(200));
#if DEBUG_WRITE
                allOutput!.WriteLine("Delta done");
                await allOutput!.FlushAsync();
#endif
            }

            output.ExitCheckpointLock();
        }

        private async Task SendEmptyBatch(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            // Same lock discipline as FetchChanges: checkpoint lock first, no database slot
            // held across a send that can park on the pause gate.
            await output.EnterCheckpointLock();
            await mockDatabase.RwLock.WaitAsync();
            int fetchedOffset;
            try
            {
                fetchedOffset = _state.Value.LatestOffset;
            }
            finally
            {
                mockDatabase.RwLock.Release();
            }

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            Column[] columns = new Column[readRelation.OutputLength];

            for (int i = 0; i < readRelation.OutputLength; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            await output.SendAsync(outputBatch);

            await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(fetchedOffset)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));

            output.ExitCheckpointLock();
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "changes")
            {
                RunTask(FetchChanges);
            }
            else if (triggerName == "send_empty_batch")
            {
                if (state == null || (state is string tableName && tableName.Equals(readRelation.NamedTable.DotSeperated, StringComparison.OrdinalIgnoreCase)))
                {
                    RunTask(SendEmptyBatch);
                }
            }
            else if (triggerName == "crash")
            {
                RunTask((output, state) => throw new CrashException("crash"));
            }
            else if (triggerName == "ingress_no_autocomplete_dependencies")
            {
                AutoCompleteDependencies = false;
            }
            else if (triggerName == "ingress_fail_and_rollback")
            {
                if (state is long restoreVersion)
                {
                    FailAndRollback(new CrashException("crash"), restoreVersion: restoreVersion);
                }
                else
                {
                    FailAndRollback(new CrashException("crash"));
                }
            }
            else if (triggerName == "ingress_dependencies_done")
            {
                SetDependenciesDone();
            }
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarkNames);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            if (_failInitialize)
            {
                throw new InvalidOperationException($"Mock source {readRelation.NamedTable.DotSeperated} is configured to fail initialization.");
            }
#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
            {
                Directory.CreateDirectory("debugwrite");
            }
            if (allOutput == null)
            {
                allOutput = File.CreateText($"debugwrite/{StreamName}_{Name}_mock.alloutput.txt");
            }
            else
            {
                allOutput.WriteLine("Restart");
                await allOutput.FlushAsync();
            }
#endif

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<MockDataSourceState>("mock_data_source_state");
            if (_state.Value == null)
            {
                _state.Value = new MockDataSourceState();
            }
            Logger.LogDebug("Mock source {table} initialized with restored offset {offset}, restore time {restoreTime}", readRelation.NamedTable.DotSeperated, _state.Value.LatestOffset, restoreTime);
            await RegisterTrigger("crash");
            await RegisterTrigger("ingress_no_autocomplete_dependencies");
            await RegisterTrigger("ingress_fail_and_rollback");
            await RegisterTrigger("ingress_dependencies_done");
            await RegisterTrigger("send_empty_batch");
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state?.Value != null);
            await _state.Commit();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            var tableName = readRelation.NamedTable.DotSeperated;
            if (TableWaitSignals.TryGetValue(tableName, out var waitTableName) &&
                TableInitialSignals.TryGetValue(waitTableName, out var waitTcs))
            {
                await waitTcs.Task;
            }

            if (_initialDataDelay.HasValue)
            {
                // Keeps this stream in its starting phase, simulating a substream whose
                // startup is much slower than its peers.
                await Task.Delay(_initialDataDelay.Value);
            }

            Debug.Assert(_state?.Value != null);
            await output.EnterCheckpointLock();
            var (operations, fetchedOffset) = _table.GetOperations(_state.Value.LatestOffset);
            Logger.LogDebug("Mock source {table} sending initial from offset {offset} to offset {fetchedOffset}", tableName, _state.Value.LatestOffset, fetchedOffset);

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            Column[] columns = new Column[readRelation.OutputLength];

            for (int i = 0; i < readRelation.OutputLength; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            foreach (var operation in operations)
            {
                _batchConverter.AppendToColumns(operation.Object, columns);

                iterations.Add(1);
                if (operation.IsDelete)
                {
                    weights.Add(-1);
                }
                else
                {
                    weights.Add(1);
                }

                if (weights.Count > 1000)
                {
                    var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
#if DEBUG_WRITE
                    foreach (var o in outputBatch.Events)
                    {
                        allOutput!.WriteLine($"{o.Weight} {o.ToJson()}");
                    }
                    await allOutput!.FlushAsync();
#endif
                    await output.SendAsync(outputBatch);

                    columns = new Column[readRelation.OutputLength];
                    for (int i = 0; i < readRelation.OutputLength; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }

            if (weights.Count > 0)
            {
                var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
#if DEBUG_WRITE
                foreach (var o in outputBatch.Events)
                {
                    allOutput!.WriteLine($"{o.Weight} {o.ToJson()}");
                }
                await allOutput!.FlushAsync();
#endif
                await output.SendAsync(outputBatch);
                await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(fetchedOffset)));
                this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
            }
            _state.Value.LatestOffset = fetchedOffset;
            
            output.ExitCheckpointLock();
            await this.RegisterTrigger("changes", TimeSpan.FromMilliseconds(50));
#if DEBUG_WRITE
            allOutput!.WriteLine("Initial done");
            await allOutput!.FlushAsync();
#endif

            if (TableInitialSignals.TryGetValue(tableName, out var myTcs))
            {
                myTcs.TrySetResult();
            }
        }
    }
}
