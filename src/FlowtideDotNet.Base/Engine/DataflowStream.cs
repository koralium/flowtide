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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Metrics;
using System.Text.Json;

namespace FlowtideDotNet.Base.Engine
{
    public class DataflowStream : IAsyncDisposable
    {
        private readonly StreamContext streamContext;

        internal DataflowStream(StreamContext streamContext)
        {
            this.streamContext = streamContext;
        }

        public StreamStateValue State => streamContext.currentState;

        public StreamStatus Status => streamContext.GetStatus();

        public IStreamScheduler Scheduler => streamContext._streamScheduler;

        public Task StartAsync()
        {
            return streamContext.StartAsync();
        }

        public void Complete()
        {
            throw new NotImplementedException();
        }

        public async Task RunAsync()
        {
            if (streamContext._streamScheduler is DefaultStreamScheduler streamScheduler)
            {
#if DEBUG_WRITE
                var diagnosticsWriter = File.CreateText("diagnostics.txt");
#endif
                await StartAsync();
                PeriodicTimer periodicTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
                int count = 0;
                while (await periodicTimer.WaitForNextTickAsync())
                {
                    await streamScheduler.Tick();
                    count++;

                    if (count % 1000 == 0)
                    {
#if DEBUG_WRITE
                        var graph = GetDiagnosticsGraph();
                        var str = JsonSerializer.Serialize(graph, new JsonSerializerOptions()
                        {
                            WriteIndented = true
                        });
                        diagnosticsWriter.WriteLine(str);
                        await diagnosticsWriter.FlushAsync();
#endif
                        // Run garbage collection once every 10 seconds
                        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true, true);
                        count = 1;
                    }
                }
            }
        }

        /// <summary>
        /// Trigger a manual checkpoint on the stream
        /// </summary>
        public Task TriggerCheckpoint()
        {
            return streamContext.TriggerCheckpoint();
        }

        public void TryScheduleCheckpoint(TimeSpan timeSpan)
        {
            streamContext.TryScheduleCheckpointIn(timeSpan);
        }

        public StreamState? GetLatestState()
        {
            return streamContext._lastState;
        }

        public Task CallTrigger(string name, object? state)
        {
            return streamContext.CallTrigger(name, state);
        }

        public Task DeleteAsync()
        {
            return streamContext.DeleteAsync();
        }

        public ValueTask DisposeAsync()
        {
            return streamContext.DisposeAsync();
        }

        public StreamGraph GetDiagnosticsGraph()
        {
            return streamContext.GetGraph();
        }

        public Task StopAsync()
        {
            return streamContext.StopAsync();
        }
    }
}
