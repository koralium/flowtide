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
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TimestampProvider
{
    internal class TimestampProviderState
    {
        public long? LastSentTimestamp { get; set; } 
    }
    internal class TimestampProviderOperator : ReadBaseOperator<TimestampProviderState>
    {
        private readonly TimeSpan interval;
        private IReadOnlySet<string> _watermarks;
        private TimestampProviderState? _state;

        public TimestampProviderOperator(TimeSpan interval, DataflowBlockOptions options) : base(options)
        {
            _watermarks = new HashSet<string>()
            {
                "__timestamp"
            };
            this.interval = interval;
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

        protected override async Task InitializeOrRestore(long restoreTime, TimestampProviderState? state, IStateManagerClient stateManagerClient)
        {
            if (state != null)
            {
                _state = state;
            }
            else
            {
                _state = new TimestampProviderState();
            }
            await RegisterTrigger("update", interval);
        }

        protected override Task<TimestampProviderState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            return Task.FromResult(_state);
        }

        private async Task UpdateTimestamp(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state != null);

            await output.EnterCheckpointLock();

            var currentTimestamp = DateTime.UtcNow.Subtract(DateTime.UnixEpoch).Ticks;
            if (!_state.LastSentTimestamp.HasValue)
            {
                await output.SendAsync(new StreamEventBatch(null, new List<StreamEvent>()
                {
                    StreamEvent.Create(1, 0, b =>
                    {
                        b.Add(currentTimestamp);
                    })
                }));
            }
            else
            {
                await output.SendAsync(new StreamEventBatch(null, new List<StreamEvent>()
                {
                    StreamEvent.Create(1, 0, b =>
                    {
                        b.Add(currentTimestamp);
                    })
                }));
                // Remove the previous row
                await output.SendAsync(new StreamEventBatch(null, new List<StreamEvent>()
                {
                    StreamEvent.Create(-1, 0, b =>
                    {
                        b.Add(_state.LastSentTimestamp.Value);
                    })
                }));
            }

            _state.LastSentTimestamp = currentTimestamp;

            // Set the watermark to the current timetamp
            await output.SendWatermark(new Base.Watermark("__timestamp", currentTimestamp));

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
