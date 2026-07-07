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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Engine.Distributed
{
    /// <summary>
    /// A distributed stream where all substreams run in the same process.
    /// Built with <see cref="DistributedStreamBuilder"/>.
    /// </summary>
    public sealed class DistributedFlowtideStream : IAsyncDisposable
    {
        private readonly IReadOnlyDictionary<string, Base.Engine.DataflowStream> _substreams;
        private readonly ILogger _logger;
        private readonly object _tickLock = new object();
        private CancellationTokenSource? _tickCancellation;
        private Task? _tickLoop;

        internal DistributedFlowtideStream(string streamName, IReadOnlyDictionary<string, Base.Engine.DataflowStream> substreams, ILogger? logger)
        {
            StreamName = streamName;
            _substreams = substreams;
            _logger = logger ?? NullLogger.Instance;
        }

        public string StreamName { get; }

        /// <summary>
        /// The substreams by substream name.
        /// </summary>
        public IReadOnlyDictionary<string, Base.Engine.DataflowStream> Substreams => _substreams;

        /// <summary>
        /// Test seam: whether the scheduler tick loop is armed. It runs while the stream is started
        /// and is torn down on stop, so a stopped stream does not keep ticking or running a blocking GC.
        /// </summary>
        internal bool IsTickLoopRunning
        {
            get
            {
                lock (_tickLock)
                {
                    return _tickCancellation != null;
                }
            }
        }

        /// <summary>
        /// The worst health across all substreams. The stream only processes data correctly
        /// when every substream is healthy, per substream details are available through
        /// <see cref="Substreams"/>.
        /// </summary>
        public Base.FlowtideHealth Health => _substreams.Values.Max(x => x.Health);

        /// <summary>
        /// Starts all substreams and drives their schedulers (no need to call RunAsync). A substream
        /// failure does not fail this call, it is retried in the background and surfaces through the
        /// failure listeners and <see cref="Health"/>. If the start itself throws, the substreams
        /// that did start are stopped before rethrowing.
        /// </summary>
        public async Task StartAsync()
        {
            EnsureTickLoop();
            try
            {
                await Task.WhenAll(_substreams.Values.Select(x => x.StartAsync()));
            }
            catch
            {
                // The stop is bounded by the drain timeout, and its own failures must not
                // hide the start failure.
                try
                {
                    await Task.WhenAll(_substreams.Values.Select(x => x.StopAsync()));
                }
                catch
                {
                }
                throw;
            }
        }

        /// <summary>
        /// Pauses all substreams, they stop emitting data to their sinks until
        /// <see cref="Resume"/> is called. Data exchanged between the substreams pauses with
        /// them since it is produced by the paused operators.
        /// </summary>
        public void Pause()
        {
            foreach (var substream in _substreams.Values)
            {
                substream.Pause();
            }
        }

        /// <summary>
        /// Resumes all substreams after <see cref="Pause"/>.
        /// </summary>
        public void Resume()
        {
            foreach (var substream in _substreams.Values)
            {
                substream.Resume();
            }
        }

        /// <summary>
        /// Drives the substream schedulers so recurring triggers fire; StartAsync does not tick them.
        /// One loop runs for the lifetime of this instance, surviving stop and restart.
        /// </summary>
        private void EnsureTickLoop()
        {
            lock (_tickLock)
            {
                if (_tickCancellation != null)
                {
                    return;
                }
                _tickCancellation = new CancellationTokenSource();
                var token = _tickCancellation.Token;
                _tickLoop = Task.Run(async () =>
                {
                    using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
                    // Dispatches are not awaited inline: one into a paused or backpressured substream
                    // can park, and it must not stall dispatch for the others. Tracked per substream
                    // to bound it to one in flight each; a substream still dispatching is skipped.
                    var inflightTicks = new Dictionary<string, Task>();
                    long tickCount = 0;
                    try
                    {
                        while (await timer.WaitForNextTickAsync(token))
                        {
                            foreach (var substream in _substreams)
                            {
                                // Custom schedulers are managed externally by whoever configured them
                                if (substream.Value.Scheduler is not Base.Engine.DefaultStreamScheduler scheduler)
                                {
                                    continue;
                                }
                                if (inflightTicks.TryGetValue(substream.Key, out var previous) && !previous.IsCompleted)
                                {
                                    continue;
                                }
                                inflightTicks[substream.Key] = TickScheduler(scheduler);
                            }
                            tickCount++;
                            if (tickCount % 1000 == 0)
                            {
                                // Run garbage collection once every 10 seconds, the same
                                // native memory pressure mitigation DataflowStream.RunAsync
                                // performs for a single stream.
                                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true, true);
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                });
            }
        }

        private async Task TickScheduler(Base.Engine.DefaultStreamScheduler scheduler)
        {
            try
            {
                await scheduler.Tick();
            }
            catch (Exception e)
            {
                // A trigger dispatch can fail while a substream is failing over, the loop
                // must keep ticking for the others.
                _logger.LogDebug(e, "Trigger tick failed in stream {stream}, retrying at the next tick.", StreamName);
            }
        }

        /// <summary>
        /// Stops all substreams.
        /// The substreams are stopped in parallel since a final checkpoint requires
        /// communication between the substreams.
        /// </summary>
        public async Task StopAsync()
        {
            await Task.WhenAll(_substreams.Values.Select(x => x.StopAsync()));
            // Stop driving the schedulers now the substreams are idle. A stopped-but-not-
            // disposed stream must not keep ticking its NotStarted substreams (each overdue
            // trigger throws) and running a blocking full GC every 10 seconds. StartAsync's
            // EnsureTickLoop restarts the loop on a restart.
            await CancelTickLoopAsync();
        }

        /// <summary>
        /// Deletes the state of all substreams.
        /// </summary>
        public async Task DeleteAsync()
        {
            await Task.WhenAll(_substreams.Values.Select(x => x.DeleteAsync()));
            await CancelTickLoopAsync();
        }

        /// <summary>
        /// Cancels and awaits the scheduler tick loop, bounded so a stuck trigger dispatch
        /// cannot hang stop or dispose. A restart re-arms it through <see cref="StartAsync"/>.
        /// </summary>
        private async Task CancelTickLoopAsync()
        {
            Task? tickLoop;
            lock (_tickLock)
            {
                _tickCancellation?.Cancel();
                _tickCancellation?.Dispose();
                _tickCancellation = null;
                tickLoop = _tickLoop;
                _tickLoop = null;
            }
            if (tickLoop != null)
            {
                var finished = await Task.WhenAny(tickLoop, Task.Delay(TimeSpan.FromSeconds(5)));
                if (finished != tickLoop)
                {
                    _logger.LogWarning("The trigger tick loop of stream {stream} did not stop within the timeout, a trigger dispatch is stuck and the loop is abandoned.", StreamName);
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            // Stop all substreams first, disposing them one by one would have live
            // substreams exchanging data with an already disposed one. The stop is bounded
            // by the drain timeout and dispose must always continue to the disposal.
            try
            {
                await Task.WhenAll(_substreams.Values.Select(x => x.StopAsync()));
            }
            catch
            {
            }
            // Stop the tick loop BEFORE disposing the substreams, so it cannot drive a
            // scheduler tick into a substream whose blocks are being completed and disposed.
            await CancelTickLoopAsync();
            // Disposed in parallel like start, stop and delete, the substreams can wait on
            // each other while shutting down.
            await Task.WhenAll(_substreams.Values.Select(x => x.DisposeAsync().AsTask()));
        }
    }
}
