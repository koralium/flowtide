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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Static store shared between the tests and the operators running inside the test silo,
    /// the test cluster runs in the same process. Tables and sinks are identified by name, the
    /// tests use unique names per stream so streams do not share data.
    /// </summary>
    internal static class TestTableStore
    {
        private static readonly ConcurrentDictionary<string, List<(long Value, int Weight)>> _tables = new ConcurrentDictionary<string, List<(long Value, int Weight)>>(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, List<long>> _results = new ConcurrentDictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase);

        public static void AddRows(string table, IEnumerable<long> rows)
        {
            var list = _tables.GetOrAdd(table, _ => new List<(long, int)>());
            lock (list)
            {
                list.AddRange(rows.Select(x => (x, 1)));
            }
        }

        /// <summary>
        /// Removes previously added rows, the source emits them with a negative weight so the
        /// removal retracts through the stream.
        /// </summary>
        public static void RemoveRows(string table, IEnumerable<long> rows)
        {
            var list = _tables.GetOrAdd(table, _ => new List<(long, int)>());
            lock (list)
            {
                list.AddRange(rows.Select(x => (x, -1)));
            }
        }

        public static IReadOnlyList<(long Value, int Weight)> GetRows(string table)
        {
            if (!_tables.TryGetValue(table, out var list))
            {
                return Array.Empty<(long, int)>();
            }
            lock (list)
            {
                return list.ToList();
            }
        }

        public static void PublishResult(string sink, List<long> rows)
        {
            _results[sink] = rows;
        }

        public static List<long>? GetResult(string sink)
        {
            return _results.TryGetValue(sink, out var rows) ? rows : null;
        }
    }

    internal class TestDataSourceFactory : RegexConnectorSourceFactory
    {
        public TestDataSourceFactory(string regexPattern) : base(regexPattern)
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new TestDataSourceOperator(readRelation.NamedTable.DotSeperated, dataflowBlockOptions);
        }

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
        {
            return new TableLineageMetadata("test", readRelation.NamedTable.DotSeperated, default);
        }
    }

    /// <summary>
    /// Emits the rows of a table in <see cref="TestTableStore"/> and polls for rows added
    /// after the start. The emitted position is not persisted, after a restore everything is
    /// emitted again, the sink resets on restore so the end result stays correct.
    /// </summary>
    internal class TestDataSourceOperator : ReadBaseOperator
    {
        private readonly string _tableName;
        private int _emittedCount;

        public TestDataSourceOperator(string tableName, DataflowBlockOptions options) : base(options)
        {
            _tableName = tableName;
        }

        public override string DisplayName => $"Test source {_tableName}";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "changes")
            {
                RunTask(EmitNewRows);
            }
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _tableName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            // Tables whose name starts with poison fail initialization, used to test that
            // background start failures surface through the status API.
            if (_tableName.StartsWith("poison", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Poisoned table {_tableName} fails initialization.");
            }
            _emittedCount = 0;
            await RegisterTrigger("changes", TimeSpan.FromMilliseconds(100));
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await EmitNewRows(output, null);
        }

        private async Task EmitNewRows(IngressOutput<StreamEventBatch> output, object? state)
        {
            var rows = TestTableStore.GetRows(_tableName);
            if (rows.Count <= _emittedCount)
            {
                return;
            }

            await output.EnterCheckpointLock();

            var memoryManager = MemoryAllocator;
            var weights = new PrimitiveList<int>(memoryManager);
            var iterations = new PrimitiveList<uint>(memoryManager);
            var column = Column.Create(memoryManager);

            for (int i = _emittedCount; i < rows.Count; i++)
            {
                weights.Add(rows[i].Weight);
                iterations.Add(0);
                column.Add(new Int64Value(rows[i].Value));
            }
            _emittedCount = rows.Count;

            await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(new IColumn[] { column }))));
            output.ExitCheckpointLock();

            ScheduleCheckpoint(TimeSpan.FromMilliseconds(10));
        }
    }

    internal class TestDataSinkFactory : RegexConnectorSinkFactory
    {
        public TestDataSinkFactory(string regexPattern) : base(regexPattern)
        {
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new TestDataSinkOperator(writeRelation.NamedObject.DotSeperated, dataflowBlockOptions);
        }

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
        {
            return new TableLineageMetadata("test", writeRelation.NamedObject.DotSeperated, default);
        }
    }

    /// <summary>
    /// Collects weighted values in memory and publishes a snapshot of the current values to
    /// <see cref="TestTableStore"/> on every checkpoint. The state is not persisted, it resets
    /// on restore and the sources re emit everything.
    /// </summary>
    internal class TestDataSinkOperator : WriteBaseOperator
    {
        private readonly string _sinkName;
        private readonly Dictionary<long, int> _values = new Dictionary<long, int>();

        public TestDataSinkOperator(string sinkName, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _sinkName = sinkName;
        }

        public override string DisplayName => $"Test sink {_sinkName}";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            lock (_values)
            {
                _values.Clear();
            }
            return Task.CompletedTask;
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            List<long> snapshot;
            lock (_values)
            {
                snapshot = _values.Where(kv => kv.Value > 0).Select(kv => kv.Key).OrderBy(x => x).ToList();
            }
            TestTableStore.PublishResult(_sinkName, snapshot);
            return Task.CompletedTask;
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            var dataValueContainer = new DataValueContainer();
            lock (_values)
            {
                for (int i = 0; i < msg.Data.Weights.Count; i++)
                {
                    msg.Data.EventBatchData.Columns[0].GetValueAt(i, dataValueContainer, default);
                    var value = dataValueContainer.AsLong;
                    _values.TryGetValue(value, out var weight);
                    _values[value] = weight + msg.Data.Weights[i];
                }
            }
            return Task.CompletedTask;
        }
    }
}
