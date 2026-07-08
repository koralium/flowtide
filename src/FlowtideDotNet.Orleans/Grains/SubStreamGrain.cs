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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using FlowtideDotNet.DependencyInjection.Internal;
using Microsoft.Extensions.DependencyInjection;
using FlowtideDotNet.Storage;
using Orleans.Concurrency;
using Orleans.Serialization.Buffers;
using System.Buffers;

namespace FlowtideDotNet.Orleans.Grains
{
    // Reentrant so the grain can serve fetches from the other substreams while it runs a
    // long operation itself, two stopping grains would otherwise deadlock waiting for each
    // other to serve the stop drain fetches. The handlers only read fields set at startup
    // and delegate to the stream which does its own synchronization.
    [Reentrant]
    [KeepAlive]
    internal class SubStreamGrain : Grain, ISubStreamGrain, IRemindable
    {
        // Keeps the substream running across silo failures. KeepAlive only protects a live
        // activation, if all substream grains were on a lost silo no call ever reactivates
        // them, the reminder fires on a surviving silo and resumes the stream.
        private const string KeepAliveReminderName = "flowtide_keepalive";

        private readonly IPersistentState<SubStreamGrainStorage> _state;
        private readonly ConnectorManagerFactory _connectorManagerFactory;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<SubStreamGrain> _logger;
        private readonly IGrainFactory _grainFactory;
        private readonly Action<string, string, IFlowtideStorageBuilder> _storageBuilder;
        private readonly FlowtideOrleansOptions _options;
        private Base.Engine.DataflowStream? _stream;
        private OrleansCommunicationFactory? _orleansCommunicationFactory;
        private readonly SubstreamEventWireSerializer _wireSerializer = new SubstreamEventWireSerializer();
        private CancellationTokenSource? _tickCancellation;

        public SubStreamGrain(
            [PersistentState("substream", "stream_metadata")] IPersistentState<SubStreamGrainStorage> state,
            ConnectorManagerFactory connectorManagerFactory,
            ILoggerFactory loggerFactory,
            IGrainFactory grainFactory,
            Action<string, string, IFlowtideStorageBuilder> storageBuilder,
            FlowtideOrleansOptions options)
        {
            this._state = state;
            this._connectorManagerFactory = connectorManagerFactory;
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory.CreateLogger<SubStreamGrain>();
            this._grainFactory = grainFactory;
            this._storageBuilder = storageBuilder;
            this._options = options;
        }

        public async Task CheckpointDone(CheckpointDoneRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                // The stream has not started yet, nothing waits for this notification
                return;
            }
            await handler.TargetCheckpointDone(request.CheckpointVersion, request.CheckpointEpoch);
        }

        public Task FailAndRecoverAsync(FailAndRecoverRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                // The stream has not started yet, there is nothing to recover.
                return Task.CompletedTask;
            }
            // Acknowledge immediately, awaiting the recovery would time the caller out
            _ = Task.Run(async () =>
            {
                try
                {
                    await handler.FailAndRecover(request.RecoveryPoint);
                }
                catch (Exception e)
                {
                    _logger.LogWarning(e, "Fail and recover of substream {substream} failed, the initialize handshake reconciles the versions at the next start.", this.GetPrimaryKeyString());
                }
            });
            return Task.CompletedTask;
        }

        public async Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                // No stream runs on this activation (e.g. a stop cleared the grain state while a peer
                // still fetches). Marked unknown so the fetcher fails and recovers instead of reading
                // an empty response as a healthy poll and starving forever.
                return new FetchDataResponse(default) { RequestorUnknown = true };
            }
            if (!_peerFetchEpochs.TryGetValue(request.Requestor, out var announcedEpoch))
            {
                // This activation has never seen an epoch announcement from the requestor,
                // for example after a reactivation on another silo. The requestor is told so
                // it can fail and recover, the handshake then re-announces the epoch.
                _logger.LogDebug("Refusing fetch from {requestor} with fetch epoch {requestEpoch}, no epoch has been announced to this activation.", request.Requestor, request.FetchEpoch);
                return new FetchDataResponse(default) { RequestorUnknown = true };
            }
            if (announcedEpoch != request.FetchEpoch)
            {
                // Fetch from a stale epoch (an abandoned instance, or from before a rollback); serving
                // it would hand events to a consumer that discards them. Marked unknown so a live
                // fetcher that landed here after an overwrite fails and recovers instead of starving.
                _logger.LogDebug("Refusing fetch from {requestor} with fetch epoch {requestEpoch}, announced epoch is {announcedEpoch}.", request.Requestor, request.FetchEpoch, announcedEpoch);
                return new FetchDataResponse(default) { RequestorUnknown = true };
            }
            var data = await handler.GetData(request.TargetIds, request.NumberOfEvents, default);
            // Serialized into pooled segments, no byte arrays are allocated. PooledBuffer is
            // a mutable struct so all writes go through one boxed IBufferWriter reference.
            // The response consumer owns the buffer, see FetchDataResponse.Events.
            var buffer = new PooledBuffer();
            IBufferWriter<byte> bufferWriter = buffer;
            try
            {
                _wireSerializer.Serialize(data, bufferWriter);
            }
            catch
            {
                ((PooledBuffer)bufferWriter).Dispose();
                throw;
            }
            finally
            {
                SubstreamEventWireSerializer.ReturnEvents(data);
            }
            return new FetchDataResponse((PooledBuffer)bufferWriter);
        }

        public async Task<GetEventsResponse> GetEventsAsync(GetEventsRequest request)
        {
            if (_stream == null)
            {
                return new GetEventsResponse(0, new List<IStreamEvent>(), true);
            }
            var msg = new ExchangeFetchDataMessage()
            {
                FromEventId = request.FromEventId
            };
            await _stream.CallTrigger($"exchange_{request.ExchangeTargetId}", msg);
            if (msg.OutEvents == null)
            {
                // The trigger has not been registered yet, treat the stream as not started
                return new GetEventsResponse(0, new List<IStreamEvent>(), true);
            }
            return new GetEventsResponse(msg.LastEventId, msg.OutEvents, false);
        }

        // The teardown in progress, stop and delete share it. The grain is reentrant, so a
        // retried or interleaved call must await the same teardown instead of starting a
        // second one, and while it is set the stream must not be rebuilt, a keep alive
        // reminder or start could otherwise run a new stream over the same storage that the
        // teardown still works on.
        private Task? _teardownTask;

        public async Task StopStreamAsync()
        {
            if (_teardownTask == null)
            {
                _teardownTask = StopStreamCore();
            }
            await AwaitTeardown();
        }

        private async Task StopStreamCore()
        {
            if (_stream != null)
            {
                var stream = _stream;
                _stream = null;
                await stream.StopAsync();
            }
            // Clear the communication factory and announced epochs now the stream is gone, so
            // a peer fetch that races in before deactivation takes FetchDataAsync's 'no stream
            // runs' RequestorUnknown fast-path instead of being routed into the torn-down
            // exchange point. A restart rebuilds the factory and re-runs the handshake.
            _orleansCommunicationFactory = null;
            _peerFetchEpochs.Clear();
            _tickCancellation?.Cancel();
            _tickCancellation = null;
            var reminder = await this.GetReminder(KeepAliveReminderName);
            if (reminder != null)
            {
                await this.UnregisterReminder(reminder);
            }
            // Clear the grain state so a reactivation does not start the stream again, the
            // stream state itself stays in its storage and a new start resumes from it.
            await _state.ClearStateAsync();
            DeactivateOnIdle();
        }

        public async Task DeleteStreamAsync()
        {
            if (_teardownTask == null)
            {
                // A reactivated grain may not have built the stream yet, the delete needs
                // the instance to reach its state storage. Rebuilt before the teardown task
                // is set, StartStream refuses while a teardown is in progress.
                StartStream();
                _teardownTask = DeleteStreamCore();
            }
            await AwaitTeardown();
        }

        private async Task DeleteStreamCore()
        {
            if (_stream != null)
            {
                var stream = _stream;
                _stream = null;
                // The engine handles delete in any state and completes when the state is
                // deleted. The dispose runs even when the delete throws, an undisposed
                // instance keeps storage handles that make every retry fail too.
                try
                {
                    await stream.DeleteAsync();
                }
                finally
                {
                    await stream.DisposeAsync();
                }
            }
            // See StopStreamCore: drop the communication factory and epochs so a racing peer
            // fetch hits the RequestorUnknown fast-path rather than the deleted exchange point.
            _orleansCommunicationFactory = null;
            _peerFetchEpochs.Clear();
            _tickCancellation?.Cancel();
            _tickCancellation = null;
            var reminder = await this.GetReminder(KeepAliveReminderName);
            if (reminder != null)
            {
                await this.UnregisterReminder(reminder);
            }
            await _state.ClearStateAsync();
            DeactivateOnIdle();
        }

        private async Task AwaitTeardown()
        {
            var teardown = _teardownTask!;
            try
            {
                await teardown;
            }
            catch
            {
                // A failed teardown may be retried, the next call starts a fresh attempt.
                // Only the observed task is cleared, a retry may already have replaced it.
                if (ReferenceEquals(_teardownTask, teardown))
                {
                    _teardownTask = null;
                }
                throw;
            }
            // Cleared on success too: the grain state is already cleared, and a stale
            // completed teardown must not refuse a fresh start on this activation.
            if (ReferenceEquals(_teardownTask, teardown))
            {
                _teardownTask = null;
            }
        }

        // The most recent stream failure on this activation, surfaced through GetStatusAsync.
        // Failures do not propagate out of StartAsync, the stream retries them in the
        // background, so they are captured through the failure listener.
        private volatile string? _lastFailure;

        public Task<SubstreamStatus> GetStatusAsync()
        {
            var stream = _stream;
            return Task.FromResult(new SubstreamStatus
            {
                SubstreamName = _state.State.SubstreamName,
                IsStarted = _state.RecordExists,
                State = stream?.State,
                Health = stream?.Health,
                LastFailure = _lastFailure
            });
        }

        // Stream state observed by the previous keep alive reminder tick, used to detect a
        // stream that is stuck in the same non running state for a whole reminder period.
        private Base.Engine.StreamStateValue? _reminderObservedState;

        // Fetch epoch per requestor substream, announced through the initialize handshake.
        // Fetches from any other epoch are refused, see FetchDataRequest.FetchEpoch. Only
        // accessed from grain turns.
        private readonly Dictionary<string, long> _peerFetchEpochs = new Dictionary<string, long>();

        // Test seam: the fetch epoch currently announced for a requestor, so the epoch
        // fencing decision can be asserted without wiring up a live communication factory.
        internal bool TryGetAnnouncedFetchEpoch(string requestor, out long epoch) => _peerFetchEpochs.TryGetValue(requestor, out epoch);

        public Task ReceiveReminder(string reminderName, TickStatus status)
        {
            // Delivering the reminder activates the grain, which resumes the stream when
            // none is running. A stream stuck in the same non running state for a whole
            // reminder period, for example when a recovery hangs on a lost silo, is
            // recreated by deactivating the grain.
            var stream = _stream;
            if (stream == null)
            {
                StartStream();
                return Task.CompletedTask;
            }
            var state = stream.State;
            if (state != Base.Engine.StreamStateValue.Running &&
                _reminderObservedState == state)
            {
                _logger.LogWarning(
                    "Substream {substream} has been stuck in state {state} for a whole reminder period, recreating it.",
                    this.GetPrimaryKeyString(), state);
                _reminderObservedState = null;
                DeactivateOnIdle();
                return Task.CompletedTask;
            }
            _reminderObservedState = state;
            return Task.CompletedTask;
        }

        public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        {
            if (_stream != null)
            {
                var stream = _stream;
                _stream = null;
                // The grain is deactivating, tear down locally instead of running the
                // coordinated stop, the stop drain needs fetches from other substreams which
                // a shutting down silo blocks. The next activation resumes from the last
                // checkpoint and the initialize handshake reconciles the versions.
                // Bounded wait, the dispose can hang on in flight calls to lost silos and
                // must not stall the deactivation.
                var disposeTask = stream.DisposeAsync().AsTask();
                await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(5), cancellationToken));
            }
            _tickCancellation?.Cancel();
            _tickCancellation = null;
            await base.OnDeactivateAsync(reason, cancellationToken);
        }

        public async Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request)
        {
            // Fetch epochs come from a monotonically increasing per-process seed, so within one
            // process a newer stream instance always announces a higher epoch than an abandoned
            // one. A handshake carrying an older epoch than the one already recorded is treated
            // as a stale instance, for example one still running on a silo the requestors grain
            // has moved off of. Installing it would overwrite the live instances announcement
            // and make every fetch from the live instance mismatch the recorded epoch, fencing
            // the healthy consumer out of its own data. The announcement is kept and the
            // stale instance is answered as an already reconciled success: refusing or
            // reporting a version mismatch would drive the live serving stream into a needless
            // fail over to the abandoned instances restore point. The seeds are clock-based per
            // process though, so after a silo failover a LIVE requestor can also land here: its
            // grain reactivated on a process whose seed started earlier than the dead instances,
            // and each failure only draws +1 from that seed, which never bridges a clock-scale
            // gap. The response therefore carries the recorded epoch, so a live requestor can
            // raise its seed above it and re-run the handshake; a genuinely stale instance dies
            // with its bounded startup retry loop and cannot keep reclaiming the record, while
            // the live instance re-announces on every recovery and wins terminally.
            if (_peerFetchEpochs.TryGetValue(request.Requestor, out var recordedEpoch) &&
                request.FetchEpoch < recordedEpoch)
            {
                _logger.LogWarning(
                    "Refusing stale initialize handshake from {requestor} with fetch epoch {requestEpoch}, a newer epoch {recordedEpoch} is already announced to this activation.",
                    request.Requestor, request.FetchEpoch, recordedEpoch);
                return new InitSubstreamResponse(false, true, request.RestorePoint, recordedFetchEpoch: recordedEpoch);
            }

            // Only fetches from the announced epoch are served, recorded even when the
            // stream has not started so the epoch is known as soon as it does.
            _peerFetchEpochs[request.Requestor] = request.FetchEpoch;
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                return new InitSubstreamResponse(true, false, request.RestorePoint, recordedFetchEpoch: request.FetchEpoch);
            }
            var response = await handler.TargetInitializeRequest(request.RestorePoint, request.CheckpointEpoch);
            return new InitSubstreamResponse(false, response.Success, response.RestoreVersion, response.CheckpointEpoch, recordedFetchEpoch: request.FetchEpoch);
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            // Format parsing alone misclassifies old keys whose stream name is a small
            // integer, when state exists the key must match the persisted names exactly.
            var key = this.GetPrimaryKeyString();
            bool legacyKey = !SubStreamGrainKey.TryParse(key, out _, out _);
            if (!legacyKey && _state.RecordExists &&
                _state.State.StreamName != null && _state.State.SubstreamName != null)
            {
                legacyKey = !SubStreamGrainKey.MatchesState(key, _state.State.StreamName, _state.State.SubstreamName);
            }
            if (legacyKey)
            {
                // The activation was reached through a key in the old format, typically by a
                // reminder persisted before the key format changed. It must not run a
                // stream, stop and delete route through the new key and could never reach
                // it. The reminder and grain state are removed so the identity dies out, the
                // streams own state storage is untouched and a start under the new key
                // resumes from it.
                _logger.LogWarning(
                    "Substream grain key '{key}' uses an old format, cleaning up its reminder and state. Start the stream again to run it under the current key format.",
                    this.GetPrimaryKeyString());
                var reminder = await this.GetReminder(KeepAliveReminderName);
                if (reminder != null)
                {
                    await this.UnregisterReminder(reminder);
                }
                await _state.ClearStateAsync();
                DeactivateOnIdle();
                await base.OnActivateAsync(cancellationToken);
                return;
            }
            StartStream();
            await base.OnActivateAsync(cancellationToken);
        }

        public async Task StartStreamAsync(StartStreamMessage startStreamMessage)
        {
            if (_teardownTask is { IsCompleted: false })
            {
                // The grain is reentrant, a start interleaving a running stop or delete at
                // their await points would re-persist state and re-register the reminder
                // the teardown is removing, resurrecting the stream it tears down.
                throw new InvalidOperationException(
                    $"Substream '{this.GetPrimaryKeyString()}' is stopping or being deleted, start it again when the teardown has completed.");
            }
            if (_state.RecordExists)
            {
                // The stream grain already rejects a changed plan, but a direct start with
                // different SQL must not be silently ignored either.
                if (!string.Equals(_state.State.SqlText, startStreamMessage.SqlText, StringComparison.Ordinal) ||
                    _state.State.SubstreamCount != startStreamMessage.SubstreamCount)
                {
                    throw new InvalidOperationException(
                        $"Substream '{this.GetPrimaryKeyString()}' is already started with a different SQL text or substream count. Stop the stream before starting it with a new plan.");
                }
                // Same plan, make sure the stream is running
                StartStream();
                return;
            }

            _state.State.SqlText = startStreamMessage.SqlText;
            _state.State.StreamName = startStreamMessage.StreamName;
            _state.State.SubstreamName = startStreamMessage.SubstreamName;
            _state.State.SubstreamCount = startStreamMessage.SubstreamCount;
            await _state.WriteStateAsync();

            // One minute is the smallest period Orleans reminders allow
            await this.RegisterOrUpdateReminder(KeepAliveReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            StartStream();
        }

        private void StartStream()
        {
            if (_stream != null || _teardownTask != null ||
                _state.State.StreamName == null || _state.State.SqlText == null || _state.State.SubstreamName == null)
            {
                return;
            }
            ServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddKeyedSingleton(_state.State.StreamName, new FileCacheOptions()
            {
                // Unique path per activation, after a silo failure a new activation can start
                // while the old activations files are not released yet.
                DirectoryPath = $"./temp/{this.GetPrimaryKeyString()}/{Guid.NewGuid():N}"
            });
            var storageBuild = new FlowtideStorageBuilder(_state.State.StreamName, serviceCollection);
            _storageBuilder(_state.State.StreamName, _state.State.SubstreamName, storageBuild);

            var stateManagerOptions = storageBuild.Build(serviceCollection.BuildServiceProvider());

            // The plan builder is deterministic so all substream grains compute an identical plan
            var connectorManager = _connectorManagerFactory.Create(_state.State.StreamName);
            var plan = OrleansStreamPlanBuilder.BuildPlan(connectorManager, _state.State.SqlText, _state.State.SubstreamCount);

            FlowtideBuilder flowtideBuilder = new FlowtideBuilder(this.GetPrimaryKeyString());
            flowtideBuilder.AddPlan(plan, false);
            flowtideBuilder.AddConnectorManager(connectorManager);
            flowtideBuilder.WithStateOptions(stateManagerOptions);

            _orleansCommunicationFactory = new OrleansCommunicationFactory(_state.State.StreamName, _grainFactory);
            // Grain timers cannot be used, the stream re-registers triggers during failure
            // recovery outside any grain turn which fails with an activation access
            // violation. The default scheduler is ticked by a loop below instead.
            var scheduler = new Base.Engine.DefaultStreamScheduler();
            flowtideBuilder.WithScheduler(scheduler);
            flowtideBuilder.WithLoggerFactory(_loggerFactory);
            // Failures inside the stream are retried in the background and never propagate
            // to a grain call, the listener captures them so GetStatusAsync can report why
            // a stream does not become healthy.
            flowtideBuilder.WithFailureListener(e => _lastFailure = e.ToString());
            if (_state.State.SubstreamName != null)
            {
                flowtideBuilder.SetDistributedOptions(new DistributedOptions(
                    _state.State.SubstreamName,
                    new PullExchangeReadFactory(_state.State.StreamName, _grainFactory),
                    _orleansCommunicationFactory));
            }

            // Applied last so user configuration can override the defaults set above.
            _options.ConfigureBuilder?.Invoke(_state.State.StreamName, _state.State.SubstreamName!, flowtideBuilder);

            _stream = flowtideBuilder.Build();
            var stream = _stream;
            _tickCancellation = new CancellationTokenSource();
            var tickToken = _tickCancellation.Token;
            // Task.Run so the stream runs on the thread pool, Task.Factory.StartNew from a
            // grain turn would capture the activation scheduler and a single synchronous
            // wait in the stream would then block the whole activation.
            _lastFailure = null;
            _ = Task.Run(async () =>
            {
                try
                {
                    await stream.StartAsync();
                }
                catch (Exception e)
                {
                    // The task is fire and forget, without logging here the exception is
                    // never observed. The failure is also kept so GetStatusAsync can report
                    // why the stream did not start.
                    _lastFailure = e.ToString();
                    _logger.LogError(e, "Starting the stream for substream {substream} failed.", this.GetPrimaryKeyString());
                    return;
                }
                // Drive the schedulers trigger dispatch, StartAsync does not tick it and no
                // recurring trigger would ever fire without the loop.
                try
                {
                    using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
                    while (await timer.WaitForNextTickAsync(tickToken))
                    {
                        try
                        {
                            await scheduler.Tick();
                        }
                        catch (Exception e)
                        {
                            // A trigger dispatch can fail while the stream is failing over,
                            // the tick loop must keep running for the restarted stream.
                            _logger.LogDebug(e, "Trigger tick failed, retrying at the next tick.");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
        }
    }
}
