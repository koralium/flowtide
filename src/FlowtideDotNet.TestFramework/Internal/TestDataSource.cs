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
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.TestFramework.Internal
{
    internal class TestDataSource : ReadBaseOperator
    {
        private const string DeltaLoadName = "delta_load";
        private readonly TestDataTable _table;
        private readonly ReadRelation _readRelation;
        private int _batchIndex = 0;
        private string _tableName;

        // Table column index for each output column
        private readonly int[] _columnMapping;

        public TestDataSource(TestDataTable table, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this._table = table;
            _tableName = readRelation.NamedTable.DotSeperated;
            this._readRelation = readRelation;
            _columnMapping = CreateColumnMapping(table, readRelation);
        }

        private static int[] CreateColumnMapping(TestDataTable table, ReadRelation readRelation)
        {
            var tableNames = table.GetSchema().Names;
            var baseColumns = new int[readRelation.BaseSchema.Names.Count];
            for (int i = 0; i < baseColumns.Length; i++)
            {
                var name = readRelation.BaseSchema.Names[i];
                var index = tableNames.IndexOf(name);
                if (index < 0)
                {
                    index = tableNames.FindIndex(x => x.Equals(name, StringComparison.OrdinalIgnoreCase));
                }
                if (index < 0)
                {
                    throw new InvalidOperationException($"Column '{name}' does not exist in test data table '{readRelation.NamedTable.DotSeperated}'.");
                }
                baseColumns[i] = index;
            }

            if (!readRelation.EmitSet)
            {
                return baseColumns;
            }
            var mapping = new int[readRelation.Emit.Count];
            for (int i = 0; i < mapping.Length; i++)
            {
                mapping[i] = baseColumns[readRelation.Emit[i]];
            }
            return mapping;
        }

        /// <summary>
        /// Copies mapped columns, stored batches are shared between reads.
        /// </summary>
        private EventBatchWeighted Project(EventBatchWeighted batch)
        {
            var columns = new IColumn[_columnMapping.Length];
            for (int i = 0; i < _columnMapping.Length; i++)
            {
                columns[i] = batch.EventBatchData.Columns[_columnMapping[i]].Copy(MemoryAllocator);
            }
            return new EventBatchWeighted(batch.Weights.Copy(MemoryAllocator), batch.Iterations.Copy(MemoryAllocator), new EventBatchData(columns));
        }

        public override string DisplayName => _tableName;

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == DeltaLoadName)
            {
                RunTask(DeltaLoad);
            }
            return Task.CompletedTask;
        }

        private async Task DeltaLoad(IngressOutput<StreamEventBatch> output, object? state)
        {
            await output.EnterCheckpointLock();
            bool sentData = false;
            while (_table.TryGetNextBatch(_batchIndex, out var batch))
            {
                sentData = true;
                _batchIndex++;
                await output.SendAsync(new StreamEventBatch(Project(batch)));
            }
            if (sentData)
            {
                await output.SendWatermark(new Base.Watermark(_tableName, LongWatermarkValue.Create(_batchIndex)));
                this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            
            output.ExitCheckpointLock();
            
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _tableName });
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await output.EnterCheckpointLock();
            while(_table.TryGetNextBatch(_batchIndex, out var batch))
            {
                _batchIndex++;
                await output.SendAsync(new StreamEventBatch(Project(batch)));
            }
            await output.SendWatermark(new Base.Watermark(_tableName, LongWatermarkValue.Create(_batchIndex)));
            output.ExitCheckpointLock();
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            await RegisterTrigger(DeltaLoadName, TimeSpan.FromMilliseconds(100));
        }
    }
}
