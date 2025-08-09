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
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Configuration.Internal
{
    internal class OptionsMonitorSource<TOptions> : ReadBaseOperator
    {
        private readonly IOptionsMonitor<TOptions> _optionsMonitor;
        private readonly ReadRelation _readRelation;
        private readonly BatchConverter _batchConverter;
        private readonly string _watermarkName;
        private readonly string _displayName;
        private IObjectState<OptionsMonitorState<TOptions>>? _state;
        private IDisposable? _monitorDisposable;
        private SemaphoreSlim _updateSemaphore;
        private Task? _deltaTask;

        public OptionsMonitorSource(IOptionsMonitor<TOptions> optionsMonitor, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this._optionsMonitor = optionsMonitor;
            this._readRelation = readRelation;
            _watermarkName = readRelation.NamedTable.DotSeperated;
            _displayName = $"Configuration({readRelation.NamedTable.DotSeperated})";

            _updateSemaphore = new SemaphoreSlim(0);
            _batchConverter = BatchConverter.GetBatchConverter(typeof(TOptions), readRelation.BaseSchema.Names);
        }

        public override string DisplayName => _displayName;

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "delta_load" && (_deltaTask == null || _deltaTask.IsCompleted))
            {
                _deltaTask = RunTask(HandleDelta);
            }
            
            return Task.CompletedTask;
        }

        private async Task HandleDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            while (true)
            {
                output.CancellationToken.ThrowIfCancellationRequested();

                await _updateSemaphore.WaitAsync();

                await output.EnterCheckpointLock();
                await SendDataOptions(output);
                output.ExitCheckpointLock();
            }
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<OptionsMonitorState<TOptions>>("state");
            
            if (_state.Value == null)
            {
                _state.Value = new OptionsMonitorState<TOptions>()
                {
                    Version = 0
                };
            }
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        private async Task SendDataOptions(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            var jsonFormattedOptions = JsonSerializer.Serialize(_optionsMonitor.CurrentValue);
            if (_state.Value.Options == null)
            {
                var batch = _batchConverter.ConvertToEventBatch<TOptions>([_optionsMonitor.CurrentValue], MemoryAllocator);
                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                weights.Add(1);
                iterations.Add(0);

                await output.SendAsync(new StreamEventBatch(new ColumnStore.EventBatchWeighted(weights, iterations, batch)));

                _state.Value.Options = _optionsMonitor.CurrentValue;
                _state.Value.JsonFormatted = jsonFormattedOptions;
                _state.Value.Version++;

                await output.SendWatermark(new Watermark(_watermarkName, LongWatermarkValue.Create(_state.Value.Version)));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            else if (!_state.Value.JsonFormatted.Equals(jsonFormattedOptions))
            {
                Column[] columns = new Column[_batchConverter.Properties.Count];

                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = Column.Create(MemoryAllocator);
                }
                _batchConverter.AppendToColumns(_optionsMonitor.CurrentValue!, columns);
                _batchConverter.AppendToColumns(_state.Value.Options, columns);

                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                weights.Add(1);
                weights.Add(-1);
                iterations.Add(0);
                iterations.Add(0);

                _state.Value.Options = _optionsMonitor.CurrentValue;
                _state.Value.JsonFormatted = jsonFormattedOptions;
                _state.Value.Version++;

                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                await output.SendWatermark(new Watermark(_watermarkName, LongWatermarkValue.Create(_state.Value.Version)));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            if (_monitorDisposable != null)
            {
                _monitorDisposable.Dispose();
            }
            _monitorDisposable = _optionsMonitor.OnChange((newOptions, str) =>
            {
                _updateSemaphore.Release();
            });

            await output.EnterCheckpointLock();
            await SendDataOptions(output);
            output.ExitCheckpointLock();

            await RegisterTrigger("delta_load", TimeSpan.FromMinutes(1));
            await OnTrigger("delta_load", default);
        }

        public override ValueTask DisposeAsync()
        {
            _monitorDisposable?.Dispose();
            return base.DisposeAsync();
        }
    }
}
