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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TimestampProvider
{
    internal class TimestampProviderState
    {
        public DateTimeOffset? LastSentTimestamp { get; set; }
    }

    internal class TimestampProviderOperator : ReadBaseOperator
    {
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _interval;
        private IReadOnlySet<string> _watermarks;
        private IObjectState<TimestampProviderState>? _state;
        private ICounter<long>? _eventsProcessed;
        private ICounter<long>? _eventsCounter;

        public TimestampProviderOperator(TimeProvider timeProvider, TimeSpan interval, DataflowBlockOptions options) : base(options)
        {
            _timeProvider = timeProvider;
            _interval = interval;
            _watermarks = new HashSet<string>()
            {
                "__timestamp"
            };
        }

        public override string DisplayName => "Timestamp provider";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            RunTask(UpdateTimestamp);
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult(_watermarks);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<TimestampProviderState>("timestamp_state");
            if (_state.Value == null)
            {
                _state.Value = new TimestampProviderState();
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            await RegisterTrigger("update", _interval);
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        private long DateTimeToTicks(DateTimeOffset date)
        {
            return date.Subtract(DateTime.UnixEpoch).Ticks;
        }

        private async Task UpdateTimestamp(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);

            await output.EnterCheckpointLock();

            IColumn[] columns = [Column.Create(MemoryAllocator)];
            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            var currentTimestamp = _timeProvider.GetUtcNow();//.Subtract(DateTime.UnixEpoch).Ticks;
            if (!_state.Value.LastSentTimestamp.HasValue)
            {
                weights.Add(1);
                iterations.Add(0);
                columns[0].Add(new TimestampTzValue(currentTimestamp));

                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                _eventsProcessed.Add(1);
                _eventsCounter.Add(1);
            }
            else
            {
                weights.Add(1);
                iterations.Add(0);
                columns[0].Add(new TimestampTzValue(currentTimestamp));

                weights.Add(-1);
                iterations.Add(0);
                columns[0].Add(new TimestampTzValue(_state.Value.LastSentTimestamp.Value));

                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                _eventsProcessed.Add(2);
                _eventsCounter.Add(2);
            }

            _state.Value.LastSentTimestamp = currentTimestamp;

            // Set the watermark to the current timetamp
            await output.SendWatermark(new Base.Watermark("__timestamp", LongWatermarkValue.Create(DateTimeToTicks(currentTimestamp))));

            output.ExitCheckpointLock();
            // Schedule a checkpoint since data has changed
            ScheduleCheckpoint(TimeSpan.FromSeconds(1));
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await UpdateTimestamp(output, default);
        }
    }
}
