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
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinInsertFailureTests : IAsyncLifetime
    {
        private readonly string _directory = $"./data/multiJoinInsertFailureTests/{Guid.NewGuid():N}";
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        private readonly List<IAsyncDisposable> _asyncDisposables = new List<IAsyncDisposable>();

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var item in _asyncDisposables)
            {
                await item.DisposeAsync();
            }
            foreach (var item in _disposables)
            {
                item.Dispose();
            }
            try
            {
                if (Directory.Exists(_directory))
                {
                    Directory.Delete(_directory, true);
                }
            }
            catch (IOException)
            {
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
                    Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(_ => (SubstraitBaseType)new AnyType()).ToList() }
                }
            };
        }

        private static FieldReference Field(int index)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };
        }

        private static MergeJoinRelation Join(JoinType type, Relation left, Relation right, params (int left, int right)[] keys)
        {
            return new MergeJoinRelation()
            {
                Type = type,
                Left = left,
                Right = right,
                LeftKeys = keys.Select(x => Field(x.left)).ToList(),
                RightKeys = keys.Select(x => Field(x.right)).ToList()
            };
        }

        private static StreamEventBatch Batch(long[][] rows)
        {
            int columnCount = rows[0].Length;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                columns[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int r = 0; r < rows.Length; r++)
            {
                for (int c = 0; c < columnCount; c++)
                {
                    columns[c].Add(new Int64Value(rows[r][c]));
                }
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return batch;
        }

        private async Task<(StateManagerSync<StreamState> stateManager, StreamMemoryManager streamMemory)> CreateStateManager(string folder, string streamName)
        {
            var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/persistent" });
            _disposables.Add(storage);
            var meter = new Meter("statemanager");
            _disposables.Add(meter);
            var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/tmp" }
                },
                NullLoggerFactory.Instance,
                meter,
                streamName,
                GlobalMemoryManager.Instance);
            _disposables.Add(stateManager);
            await stateManager.InitializeAsync();
            var streamMemory = new StreamMemoryManager(streamName);
            _disposables.Add(streamMemory);
            return (stateManager, streamMemory);
        }

        private MultiJoinOperator CreateOperator(MultiJoinPlan plan, string streamName, TaskScheduler? scheduler = null)
        {
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var options = new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 100,
                MaxDegreeOfParallelism = 1
            };
            if (scheduler != null)
            {
                options.TaskScheduler = scheduler;
            }
            var op = new MultiJoinOperator(plan, functions, options);
            _asyncDisposables.Add(op);
            op.Setup(streamName, "1");
            op.CreateBlock();
            op.Link();
            return op;
        }

        private StreamMetrics InitMetrics(string streamName)
        {
            var metrics = new StreamMetrics(streamName);
            _disposables.Add(metrics);
            return metrics;
        }

        private static Task InitOperator(MultiJoinOperator op, StateManagerSync<StreamState> stateManager, StreamMemoryManager streamMemory, StreamMetrics streamMetrics, string streamName)
        {
            var handler = new VertexHandler(
                streamName,
                "1",
                (_, _) => { },
                (_, _, _) => Task.CompletedTask,
                streamMetrics.GetOrCreateVertexMeter("1", () => ""),
                stateManager.GetOrCreateClient("1"),
                NullLoggerFactory.Instance,
                streamMemory.CreateOperatorMemoryManager("1"),
                (_, _) => Task.CompletedTask);
            return op.Initialize("1", 0, 0, handler, null);
        }

        private static async Task SendBatch(MultiJoinOperator op, int targetId, StreamEventBatch batch)
        {
            try
            {
                await foreach (var output in op.OnRecieve(targetId, batch, 0))
                {
                    output.Data.Rent(1);
                    output.Data.Return();
                }
            }
            finally
            {
                batch.Data.Return();
            }
        }

        private sealed class FaultingInserter : IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer>
        {
            public int LeafHitCount => 0;
            public int[] SortAndGetIndices(ColumnRowReference[] keys, int keyLength) => throw new NotSupportedException();
            public ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight> => throw new NotSupportedException();

            public ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, int[] sortedIndices, int[] duplicateTags, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight>
            {
                return ValueTask.FromException(new IOException("Simulated parallel primary insert failure."));
            }
        }

        [Fact]
        public async Task AbandoningOutputEnumerationDoesNotSwallowParallelInsertFailure()
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            bool parallelBefore = MultiJoinOperator.ParallelInserts;
            MultiJoinOperator.RowBudget = 1;
            MultiJoinOperator.ParallelInsertMinRows = 1;
            MultiJoinOperator.ParallelInserts = true;
            try
            {
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                string streamName = $"stream_{Guid.NewGuid():N}";
                var (stateManager, streamMemory) = await CreateStateManager("early_dispose_insert_fault", streamName);
                var metrics = InitMetrics(streamName);
                var op = CreateOperator(plan, streamName);
                await InitOperator(op, stateManager, streamMemory, metrics, streamName);

                await SendBatch(op, 1, Batch(new[] { new[] { 1L, 10L }, new[] { 1L, 20L } }));
                await SendBatch(op, 2, Batch(new[] { new[] { 1L, 30L } }));

                var inputs = (MultiJoinInputState[])typeof(MultiJoinOperator)
                    .GetField("_inputs", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(op)!;
                var primaryWriter = typeof(MultiJoinInputState)
                    .GetField("_primary", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(inputs[0])!;
                primaryWriter.GetType()
                    .GetProperty("Inserter", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                    .SetValue(primaryWriter, new FaultingInserter());

                var batchA = Batch(new[] { new[] { 1L, 100L } });
                try
                {
                    var enumerator = op.OnRecieve(0, batchA, 0).GetAsyncEnumerator();
                    Assert.True(await enumerator.MoveNextAsync());
                    enumerator.Current.Data.Rent(1);
                    enumerator.Current.Data.Return();

                    // Disposing at yield return without a foreground lookup fault must surface the failed parallel write
                    await Assert.ThrowsAsync<IOException>(async () => await enumerator.DisposeAsync());
                }
                finally
                {
                    batchA.Data.Return();
                }
            }
            finally
            {
                MultiJoinOperator.ParallelInserts = parallelBefore;
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
                MultiJoinOperator.RowBudget = budgetBefore;
            }
        }

        [Fact]
        public async Task DisposingOperatorWithBufferedRowsResetsDeferredRowsGauge()
        {
            bool deferBefore = MultiJoinOperator.DeferEveryBatch;
            MultiJoinOperator.DeferEveryBatch = true;
            try
            {
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                string streamName = $"stream_{Guid.NewGuid():N}";
                var (stateManager, streamMemory) = await CreateStateManager("dispose_deferred_gauge", streamName);
                var metrics = InitMetrics(streamName);

                long observedDeferredRows = -1;
                using var listener = new MeterListener();
                listener.InstrumentPublished = (instrument, meterListener) =>
                {
                    if (instrument.Name == "flowtide_deferred_rows" && instrument.Meter.Name.Contains(streamName, StringComparison.Ordinal))
                    {
                        meterListener.EnableMeasurementEvents(instrument);
                    }
                };
                listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
                {
                    if (instrument.Name == "flowtide_deferred_rows")
                    {
                        observedDeferredRows = measurement;
                    }
                });
                listener.Start();

                var op = CreateOperator(plan, streamName);
                await InitOperator(op, stateManager, streamMemory, metrics, streamName);

                await SendBatch(op, 0, Batch(new[] { new[] { 1L, 10L }, new[] { 2L, 20L } }));
                listener.RecordObservableInstruments();
                Assert.Equal(2L, observedDeferredRows);

                _asyncDisposables.Remove(op);
                await op.DisposeAsync();

                listener.RecordObservableInstruments();
                Assert.Equal(0L, observedDeferredRows);
            }
            finally
            {
                MultiJoinOperator.DeferEveryBatch = deferBefore;
            }
        }

        [Fact]
        public async Task AMessageRecordsItsLookupAndInsertTime()
        {
            var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            string streamName = $"stream_{Guid.NewGuid():N}";
            var (stateManager, streamMemory) = await CreateStateManager("time_metrics", streamName);
            var metrics = InitMetrics(streamName);

            var recorded = new Dictionary<string, int>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Name.EndsWith("_ms", StringComparison.Ordinal) && instrument.Meter.Name.Contains(streamName, StringComparison.Ordinal))
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<float>((instrument, measurement, _, _) =>
            {
                Assert.True(measurement >= 0);
                lock (recorded)
                {
                    recorded[instrument.Name] = recorded.GetValueOrDefault(instrument.Name) + 1;
                }
            });
            listener.Start();

            var op = CreateOperator(plan, streamName);
            await InitOperator(op, stateManager, streamMemory, metrics, streamName);
            for (int input = 0; input < 3; input++)
            {
                await SendBatch(op, input, Batch(new[] { new[] { 1L, 10L } }));
            }

            // Every message stores its rows and looks the other inputs up, none of these joins has a pad table
            Assert.Equal(3, recorded.Single(x => x.Key.EndsWith("insert_ms", StringComparison.Ordinal)).Value);
            Assert.Equal(3, recorded.Single(x => x.Key.EndsWith("lookup_ms", StringComparison.Ordinal)).Value);
            Assert.DoesNotContain(recorded, x => x.Key.EndsWith("pad_correction_ms", StringComparison.Ordinal));
        }

        // Counts the samples of the histograms and sums the counters of one stream
        private static MeterListener Listen(string streamName, Dictionary<string, long> recorded)
        {
            var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name.Contains(streamName, StringComparison.Ordinal))
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<float>((instrument, measurement, _, _) =>
            {
                lock (recorded)
                {
                    recorded[instrument.Name] = recorded.GetValueOrDefault(instrument.Name) + 1;
                }
            });
            listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
            {
                lock (recorded)
                {
                    recorded[instrument.Name] = recorded.GetValueOrDefault(instrument.Name) + measurement;
                }
            });
            listener.Start();
            return listener;
        }

        private static long Recorded(Dictionary<string, long> recorded, string name)
        {
            lock (recorded)
            {
                return recorded.Where(x => x.Key.EndsWith("_" + name, StringComparison.Ordinal)).Sum(x => x.Value);
            }
        }

        [Fact]
        public async Task ALeftJoinRecordsItsPadCorrectionTimeAndTheRowsItTakesAndGives()
        {
            var root = Join(JoinType.Left, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            string streamName = $"stream_{Guid.NewGuid():N}";
            var (stateManager, streamMemory) = await CreateStateManager("pad_metrics", streamName);
            var metrics = InitMetrics(streamName);
            var recorded = new Dictionary<string, long>();
            using var listener = Listen(streamName, recorded);

            var op = CreateOperator(plan, streamName);
            await InitOperator(op, stateManager, streamMemory, metrics, streamName);
            // The row of a and b goes out filled with nulls, the row of c takes that row back and gives the joined one
            for (int input = 0; input < 3; input++)
            {
                await SendBatch(op, input, Batch(new[] { new[] { 1L, 10L } }));
            }

            // The counters are read when asked for
            listener.RecordObservableInstruments();
            Assert.True(Recorded(recorded, "pad_correction_ms") >= 1);
            Assert.Equal(3, Recorded(recorded, "events_processed"));
            // The row with nulls, its removal and the joined row
            Assert.Equal(3, Recorded(recorded, "events"));
        }

        [Fact]
        public async Task RowsThatAreHeldBackRecordTheirTimeWhenTheyAreStored()
        {
            bool deferBefore = MultiJoinOperator.DeferEveryBatch;
            MultiJoinOperator.DeferEveryBatch = true;
            try
            {
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                string streamName = $"stream_{Guid.NewGuid():N}";
                var (stateManager, streamMemory) = await CreateStateManager("deferred_metrics", streamName);
                var metrics = InitMetrics(streamName);
                var recorded = new Dictionary<string, long>();
                using var listener = Listen(streamName, recorded);

                var op = CreateOperator(plan, streamName);
                await InitOperator(op, stateManager, streamMemory, metrics, streamName);
                await SendBatch(op, 0, Batch(new[] { new[] { 1L, 10L } }));
                await SendBatch(op, 0, Batch(new[] { new[] { 2L, 20L } }));
                Assert.Equal(0, Recorded(recorded, "insert_ms"));
                Assert.Equal(0, Recorded(recorded, "lookup_ms"));

                var flush = (IAsyncEnumerable<StreamEventBatch>)typeof(MultiJoinOperator)
                    .GetMethod("OnCheckpointFlush", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.Invoke(op, null)!;
                await foreach (var output in flush)
                {
                    output.Data.Rent(1);
                    output.Data.Return();
                }
                // The two messages are stored and looked up as one
                Assert.Equal(1, Recorded(recorded, "insert_ms"));
                Assert.Equal(1, Recorded(recorded, "lookup_ms"));
            }
            finally
            {
                MultiJoinOperator.DeferEveryBatch = deferBefore;
            }
        }
    }
}
