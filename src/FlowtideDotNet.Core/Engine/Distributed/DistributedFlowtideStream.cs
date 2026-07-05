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
        /// The worst health across all substreams. The stream only processes data correctly
        /// when every substream is healthy, per substream details are available through
        /// <see cref="Substreams"/>.
        /// </summary>
        public Base.FlowtideHealth Health => _substreams.Values.Max(x => x.Health);

        /// <summary>
        /// Starts all substreams and begins driving their schedulers.
        /// Recurring connector triggers, for example sources polling for changes, are
        /// dispatched by the stream itself, there is no need to call RunAsync on the
        /// substreams.
        ///
        /// When any substream fails to start, the substreams that did start are stopped
        /// before the failure is rethrown, so a failed start never leaves part of the
        /// topology running.
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
        /// Drives the default schedulers of all substreams. DataflowStream.StartAsync does
        /// not tick the scheduler, only RunAsync does, so without this loop no recurring
        /// operator trigger would ever fire and sources that poll for changes through
        /// triggers would never emit new data. One loop runs for the lifetime of this
        /// instance, it survives stop and restart and ends on dispose.
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
                    try
                    {
                        while (await timer.WaitForNextTickAsync(token))
                        {
                            foreach (var substream in _substreams.Values)
                            {
                                // Substreams with a custom scheduler are managed externally
                                // by whoever configured them.
                                if (substream.Scheduler is Base.Engine.DefaultStreamScheduler scheduler)
                                {
                                    try
                                    {
                                        await scheduler.Tick();
                                    }
                                    catch (Exception e)
                                    {
                                        // A trigger dispatch can fail while a substream is
                                        // failing over, the loop must keep ticking for the
                                        // other substreams and the restarted stream.
                                        _logger.LogDebug(e, "Trigger tick failed in stream {stream}, retrying at the next tick.", StreamName);
                                    }
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                });
            }
        }

        /// <summary>
        /// Stops all substreams.
        /// The substreams are stopped in parallel since a final checkpoint requires
        /// communication between the substreams.
        /// </summary>
        public Task StopAsync()
        {
            return Task.WhenAll(_substreams.Values.Select(x => x.StopAsync()));
        }

        /// <summary>
        /// Deletes the state of all substreams.
        /// </summary>
        public Task DeleteAsync()
        {
            return Task.WhenAll(_substreams.Values.Select(x => x.DeleteAsync()));
        }

        public async ValueTask DisposeAsync()
        {
            // The substreams are stopped together first: disposing them one by one while
            // the others keep running would have live substreams exchanging data with an
            // already disposed peer. The stop is bounded internally by the drain timeout
            // and the stop watchdogs, and failures are swallowed since dispose must always
            // continue to the actual disposal.
            try
            {
                await Task.WhenAll(_substreams.Values.Select(x => x.StopAsync()));
            }
            catch
            {
            }
            // Disposed in parallel like start, stop and delete, the substreams can wait on
            // each other while shutting down.
            await Task.WhenAll(_substreams.Values.Select(x => x.DisposeAsync().AsTask()));

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
                // A tick dispatches triggers into the substreams, a dispatch into a stream
                // that failed while being disposed can hang, and dispose must not hang with
                // it. The loop observes the cancellation at the next timer tick, when the
                // in-flight dispatch never returns the loop is abandoned.
                var finished = await Task.WhenAny(tickLoop, Task.Delay(TimeSpan.FromSeconds(5)));
                if (finished != tickLoop)
                {
                    _logger.LogWarning("The trigger tick loop of stream {stream} did not stop within the timeout during dispose, a trigger dispatch is stuck and the loop is abandoned.", StreamName);
                }
            }
        }
    }
}
