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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class ImmutableGenericReadOperator<T> : ReadBaseOperator
        where T: class
    {
        public const string DeltaTriggerName = "delta_load";
        private readonly ImmutableGenericDataSourceAsync<T> _source;
        private readonly ReadRelation _readRelation;
        private readonly string _watermarkName;
        private readonly BatchConverter _batchConverter;
        private IObjectState<long>? _lastWatermark;
        private readonly object _taskLock = new object();
        private Task? _deltaLoadTask;

        public ImmutableGenericReadOperator(ImmutableGenericDataSourceAsync<T> source, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this._source = source;
            this._readRelation = readRelation;
            _watermarkName = readRelation.NamedTable.DotSeperated;
            _batchConverter = BatchConverter.GetBatchConverter(typeof(T), readRelation.BaseSchema.Names);
        }

        public override string DisplayName => "ImmutableGeneric";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName.Equals(DeltaTriggerName, StringComparison.OrdinalIgnoreCase))
            {
                lock (_taskLock)
                {
                    if (_deltaLoadTask == null)
                    {
                        _deltaLoadTask = RunTask(HandleDelta)
                            .ContinueWith(x =>
                            {
                                lock (_taskLock)
                                {
                                    _deltaLoadTask = default;
                                }
                            });
                    }
                }
                
            }
            return Task.CompletedTask;
        }

        private async Task HandleDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_lastWatermark != null);
            await output.EnterCheckpointLock();

            IColumn[] columns = new Column[_readRelation.BaseSchema.Names.Count];

            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            bool dataSent = false;
            await foreach(var obj in _source.DeltaLoadAsync(_lastWatermark.Value))
            {
                _batchConverter.AppendToColumns(obj.Value, columns);
                weights.Add(1);
                iterations.Add(0);

                _lastWatermark.Value = obj.Watermark;

                if (weights.Count >= 100)
                {
                    dataSent = true;
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                    columns = new Column[_readRelation.BaseSchema.Names.Count];
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }

            if (weights.Count > 0)
            {
                dataSent = true;
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Dispose();
                }
            }
            if (dataSent)
            {
                await output.SendWatermark(new Base.Watermark(_watermarkName, _lastWatermark.Value));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            output.ExitCheckpointLock();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _lastWatermark = await stateManagerClient.GetOrCreateObjectStateAsync<long>("lastwatermark");
            await _source.Initialize(_readRelation, stateManagerClient.GetChildManager("impl"));
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_lastWatermark != null);
            await _lastWatermark.Commit();
            await _source.Checkpoint();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_lastWatermark != null);
            
            await output.EnterCheckpointLock();
            IColumn[] columns = new Column[_readRelation.BaseSchema.Names.Count];

            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            await foreach(var obj in _source.FullLoadAsync())
            {
                _batchConverter.AppendToColumns(obj.Value, columns);
                weights.Add(1);
                iterations.Add(0);
                _lastWatermark.Value = obj.Watermark;

                if (weights.Count >= 100)
                {
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                    columns = new Column[_readRelation.BaseSchema.Names.Count + 1];
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }
            if (weights.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Dispose();
                }
            }

            await output.SendWatermark(new Base.Watermark(_watermarkName, _lastWatermark.Value));

            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            output.ExitCheckpointLock();

            await RegisterTrigger(DeltaTriggerName, _source.DeltaLoadInterval);
        }
    }
}
