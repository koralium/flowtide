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

namespace FlowtideDotNet.Base.Engine
{
    /// <summary>
    /// The primary public-facing handle for a Flowtide dataflow stream, providing lifecycle management,
    /// monitoring, and operational control over the stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="DataflowStream"/> wraps an internal <c>StreamContext</c> state machine and exposes
    /// an API for starting, stopping, pausing, checkpointing, and inspecting the stream.
    /// Instances are created via <see cref="DataflowStreamBuilder.Build"/> and are not intended to be
    /// constructed directly.
    /// </para>
    /// <para>
    /// The stream can be driven in two ways depending on the configured <see cref="IStreamScheduler"/>:
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       Call <see cref="RunAsync"/> to start the stream and block the caller within a scheduler
    ///       tick loop. This is the typical usage when using the <see cref="DefaultStreamScheduler"/>,
    ///       and is how the hosted service <c>StreamWorkerService</c> operates it.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       Call <see cref="StartAsync"/> to initiate the startup sequence only, leaving scheduler
    ///       tick management to the caller. This is appropriate for custom <see cref="IStreamScheduler"/>
    ///       implementations.
    ///     </description>
    ///   </item>
    /// </list>
    /// </para>
    /// <para>
    /// The stream progresses through <see cref="StreamStateValue"/> states (e.g. <c>NotStarted</c>,
    /// <c>Starting</c>, <c>Running</c>, <c>Failure</c>) and <see cref="StreamStatus"/> operational
    /// statuses (e.g. <c>Running</c>, <c>Paused</c>, <c>Failing</c>). The combined <see cref="Health"/>
    /// property derives a <see cref="FlowtideHealth"/> value suitable for health checks and monitoring.
    /// </para>
    /// </remarks>
    public class DataflowStream : IAsyncDisposable
    {
        private readonly StreamContext streamContext;

        internal DataflowStream(StreamContext streamContext)
        {
            this.streamContext = streamContext;
        }

        /// <summary>
        /// Gets the current high-level state machine value of the stream.
        /// </summary>
        /// <remarks>
        /// Reflects the current position within the stream lifecycle, such as
        /// <see cref="StreamStateValue.NotStarted"/>, <see cref="StreamStateValue.Running"/>, 
        /// <see cref="StreamStateValue.Failure"/>, or <see cref="StreamStateValue.Stopping"/>.
        /// </remarks>
        public StreamStateValue State => streamContext.currentState;

        /// <summary>
        /// Gets the desired target state that the stream is transitioning towards.
        /// </summary>
        /// <remarks>
        /// During a state transition the current <see cref="State"/> may differ from
        /// <see cref="WantedState"/> until the transition completes.
        /// </remarks>
        public StreamStateValue WantedState => streamContext._wantedState;

        /// <summary>
        /// Gets the current operational status of the stream.
        /// </summary>
        /// <remarks>
        /// Provides finer-grained operational information than <see cref="State"/>, including
        /// values such as <see cref="StreamStatus.Running"/>, <see cref="StreamStatus.Paused"/>, 
        /// <see cref="StreamStatus.Degraded"/>, and <see cref="StreamStatus.Failing"/>.
        /// </remarks>
        public StreamStatus Status => streamContext.Status;

        /// <summary>
        /// Gets the <see cref="IStreamScheduler"/> instance responsible for scheduling and firing
        /// registered trigger events within this stream.
        /// </summary>
        public IStreamScheduler Scheduler => streamContext._streamScheduler;

        /// <summary>
        /// Gets the overall health of the stream, derived from the combination of <see cref="State"/>
        /// and <see cref="Status"/>.
        /// </summary>
        /// <remarks>
        /// Returns <see cref="FlowtideHealth.Healthy"/> when the stream is running normally,
        /// <see cref="FlowtideHealth.Degraded"/> during start-up or when paused, and
        /// <see cref="FlowtideHealth.Unhealthy"/> when the stream has faulted or stopped.
        /// Suitable for use with health check integrations.
        /// </remarks>
        public FlowtideHealth Health => streamContext.Health;

        /// <summary>
        /// Initiates the stream startup sequence, setting up all blocks, linking them, and restoring
        /// state from the last checkpoint if available.
        /// </summary>
        /// <returns>A task representing the asynchronous startup operation.</returns>
        public Task StartAsync()
        {
            return streamContext.StartAsync();
        }

        /// <summary>
        /// Starts the stream and drives the scheduler tick loop until the stream transitions to
        /// <see cref="StreamStateValue.NotStarted"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is only active when the configured <see cref="IStreamScheduler"/> is a
        /// <see cref="DefaultStreamScheduler"/>. It calls <see cref="StartAsync"/> internally,
        /// then enters a 10 ms periodic timer loop that calls <c>Tick</c> on the scheduler to
        /// dispatch any due trigger events.
        /// </para>
        /// <para>
        /// A full garbage collection is performed once every 10 seconds (every 1000 ticks)
        /// to manage native memory pressure from large stream workloads.
        /// </para>
        /// <para>
        /// When using a custom <see cref="IStreamScheduler"/>, use <see cref="StartAsync"/>
        /// directly instead and manage the scheduler externally.
        /// </para>
        /// </remarks>
        /// <returns>A task that completes when the stream stops running.</returns>
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
                while (await periodicTimer.WaitForNextTickAsync() && State != StreamStateValue.NotStarted)
                {
                    await streamScheduler.Tick();
                    count++;
                    
                    if (count % 1000 == 0)
                    {
#if DEBUG_WRITE
                        var graph = GetDiagnosticsGraph();
                        var str = System.Text.Json.JsonSerializer.Serialize(graph, new System.Text.Json.JsonSerializerOptions()
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
        /// Triggers a manual checkpoint on the stream, requesting all vertices to persist their
        /// current state.
        /// </summary>
        /// <returns>A task representing the asynchronous checkpoint trigger operation.</returns>
        public Task TriggerCheckpoint()
        {
            return streamContext.TriggerCheckpoint();
        }

        /// <summary>
        /// Schedules a checkpoint to be triggered after the specified delay, respecting any
        /// configured minimum time between checkpoints.
        /// </summary>
        /// <param name="timeSpan">The delay after which the checkpoint should be triggered.</param>
        public void TryScheduleCheckpoint(TimeSpan timeSpan)
        {
            streamContext.TryScheduleCheckpointIn(timeSpan);
        }

        /// <summary>
        /// Returns the latest persisted <see cref="StreamState"/>, or <see langword="null"/> if no
        /// state has been saved yet.
        /// </summary>
        /// <returns>
        /// The most recent <see cref="StreamState"/> loaded or saved by the stream, or
        /// <see langword="null"/> if the stream has not yet completed its first checkpoint.
        /// </returns>
        public StreamState? GetLatestState()
        {
            return streamContext._lastState;
        }

        /// <summary>
        /// Manually invokes a named trigger on all operators registered to handle it.
        /// </summary>
        /// <param name="name">The name of the trigger to invoke.</param>
        /// <param name="state">Optional state object passed to the trigger handler.</param>
        /// <returns>A task that completes when all trigger handlers have been queued.</returns>
        public Task CallTrigger(string name, object? state)
        {
            return streamContext.CallTrigger(name, state);
        }

        /// <summary>
        /// Transitions the stream to the deleting state, permanently removing all persistent state
        /// associated with every vertex in the stream.
        /// </summary>
        /// <returns>A task representing the asynchronous delete operation.</returns>
        public Task DeleteAsync()
        {
            return streamContext.DeleteAsync();
        }

        /// <summary>
        /// Releases all resources held by the stream asynchronously, completing all dataflow blocks
        /// and disposing each vertex.
        /// </summary>
        /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
        public ValueTask DisposeAsync()
        {
            return streamContext.DisposeAsync();
        }

        /// <summary>
        /// Returns a <see cref="StreamGraph"/> describing the current operator topology of the stream,
        /// including per-vertex metric snapshots and the directed edges between operators.
        /// </summary>
        /// <returns>
        /// A <see cref="StreamGraph"/> containing all operator nodes with their current metric counters
        /// and gauges, the edges between them, and the current <see cref="State"/>.
        /// </returns>
        public StreamGraph GetDiagnosticsGraph()
        {
            return streamContext.GetGraph();
        }

        /// <summary>
        /// Gracefully stops the stream, waiting for the stop sequence to complete before returning.
        /// </summary>
        /// <returns>A task that completes when the stream has fully stopped.</returns>
        public Task StopAsync()
        {
            return streamContext.StopAsync();
        }

        /// <summary>
        /// Suspends message processing throughout the stream. Data will continue to queue but will
        /// not be processed until <see cref="Resume"/> is called.
        /// </summary>
        /// <remarks>
        /// Sets the operational <see cref="Status"/> to <see cref="StreamStatus.Paused"/> for the
        /// duration of the pause. Has no effect if the stream is already paused.
        /// </remarks>
        public void Pause()
        {
            streamContext.Pause();
        }

        /// <summary>
        /// Resumes message processing after a prior call to <see cref="Pause"/>, restoring the
        /// <see cref="Status"/> to its value before the pause was applied.
        /// </summary>
        public void Resume()
        {
            streamContext.Resume();
        }
    }
}
