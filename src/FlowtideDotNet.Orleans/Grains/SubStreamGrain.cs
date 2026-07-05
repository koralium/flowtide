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
    // Reentrant so the grain can serve fetch, checkpoint done and initialize calls from the
    // other substreams while it is itself running a long operation such as the coordinated
    // stop, which drains data by fetching the other substreams stop barrier. A non reentrant
    // grain would deadlock, the stopping grain would wait for the other substream to serve a
    // fetch while it is busy waiting for this grain to serve one. The message handlers only
    // read fields set once at startup and delegate to the stream and handlers, which do their
    // own synchronization, so interleaving is safe.
    [Reentrant]
    [KeepAlive]
    public class SubStreamGrain : Grain, ISubStreamGrain, IRemindable
    {
        // Reminder that keeps the substream running across silo failures. KeepAlive only
        // protects a live activation, when the hosting silo dies nothing reactivates the
        // grain unless something calls it, another substreams fetch loop does when parts of
        // the stream survive, but if all substream grains were on the lost silo no call ever
        // comes. The reminder fires on a surviving silo and its delivery activates the grain,
        // which resumes the stream from its stored state.
        private const string KeepAliveReminderName = "flowtide_keepalive";

        private readonly IPersistentState<SubStreamGrainStorage> _state;
        private readonly IConnectorManager _connectorManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IGrainFactory _grainFactory;
        private readonly Action<string, string, IFlowtideStorageBuilder> _storageBuilder;
        private readonly FlowtideOrleansOptions _options;
        private Base.Engine.DataflowStream? _stream;
        private OrleansCommunicationFactory? _orleansCommunicationFactory;
        // Only used to serialize outgoing events, serializing never allocates batch memory.
        private readonly SubstreamEventWireSerializer _wireSerializer = new SubstreamEventWireSerializer();
        // Ends the trigger tick loop when the stream is stopped or the grain deactivates.
        private CancellationTokenSource? _tickCancellation;

        public SubStreamGrain(
            [PersistentState("substream", "stream_metadata")] IPersistentState<SubStreamGrainStorage> state,
            IConnectorManager connectorManager,
            ILoggerFactory loggerFactory,
            IGrainFactory grainFactory,
            Action<string, string, IFlowtideStorageBuilder> storageBuilder,
            FlowtideOrleansOptions options)
        {
            this._state = state;
            this._connectorManager = connectorManager;
            this._loggerFactory = loggerFactory;
            this._grainFactory = grainFactory;
            this._storageBuilder = storageBuilder;
            this._options = options;
        }

        public async Task CheckpointDone(CheckpointDoneRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                // The stream has not started yet, there is no pending checkpoint that waits
                // for this notification so it can be ignored.
                return;
            }
            await handler.TargetCheckpointDone(request.CheckpointVersion);
        }

        public Task FailAndRecoverAsync(FailAndRecoverRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                // The stream has not started yet, there is nothing to recover.
                return Task.CompletedTask;
            }
            // Acknowledge immediately and recover in the background. The recovery stops and
            // restarts the whole stream, which includes calls to other substreams that can
            // take longer than the callers response timeout, awaiting it here would time the
            // caller out and trigger another recovery, cascading across the substreams.
            _ = Task.Run(async () =>
            {
                try
                {
                    await handler.FailAndRecover(request.RecoveryPoint);
                }
                catch (Exception e)
                {
                    _loggerFactory.CreateLogger<SubStreamGrain>().LogWarning(e, "Fail and recover of substream {substream} failed, the initialize handshake reconciles the versions at the next start.", this.GetPrimaryKeyString());
                }
            });
            return Task.CompletedTask;
        }

        public async Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                return new FetchDataResponse(default);
            }
            if (!_peerFetchEpochs.TryGetValue(request.Requestor, out var announcedEpoch))
            {
                // This activation has never seen an announcement from the requestor, which
                // happens when this grain was reactivated, for example on another silo, and
                // lost the per activation epoch table. The requestor is told so it can fail
                // and recover, its initialize handshake then re-announces the epoch. Serving
                // the fetch blind would risk handing events to an abandoned instance.
                _loggerFactory.CreateLogger<SubStreamGrain>().LogDebug("Refusing fetch from {requestor} with fetch epoch {requestEpoch}, no epoch has been announced to this activation.", request.Requestor, request.FetchEpoch);
                return new FetchDataResponse(default) { RequestorUnknown = true };
            }
            if (announcedEpoch != request.FetchEpoch)
            {
                // The fetch does not come from the requestors current epoch: it can be from
                // an abandoned stream instance whose grain moved to another silo, or a fetch
                // started before the requestor rolled back. Fetching removes events from the
                // queues, serving it would hand events, including checkpoint barriers, to a
                // consumer that throws them away, freezing the real consumer.
                _loggerFactory.CreateLogger<SubStreamGrain>().LogDebug("Refusing fetch from {requestor} with fetch epoch {requestEpoch}, announced epoch is {announcedEpoch}.", request.Requestor, request.FetchEpoch, announcedEpoch);
                return new FetchDataResponse(default);
            }
            var data = await handler.GetData(request.TargetIds, request.NumberOfEvents, default);
            // The events are serialized into pooled segments so they can cross silo
            // boundaries without allocating byte arrays, the local copies end their journey
            // here and their receiver claims are released, the other substream works with
            // the deserialized copies. PooledBuffer is a mutable struct, writing through a
            // single boxed IBufferWriter reference keeps one authoritative instance which is
            // read back out when the response is created. The response consumer owns the
            // buffer from here on, see FetchDataResponse.Events.
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

        public async Task StopStreamAsync()
        {
            if (_stream != null)
            {
                var stream = _stream;
                _stream = null;
                await stream.StopAsync();
            }
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

        // Failure of the last background start attempt on this activation, surfaced through
        // GetStatusAsync. Written from the start task on the thread pool, read from grain
        // turns.
        private volatile string? _lastStartFailure;

        public Task<SubstreamStatus> GetStatusAsync()
        {
            var stream = _stream;
            return Task.FromResult(new SubstreamStatus
            {
                SubstreamName = _state.State.SubstreamName,
                IsStarted = _state.RecordExists,
                State = stream?.State,
                Health = stream?.Health,
                StartFailure = _lastStartFailure
            });
        }

        // Stream state observed by the previous keep alive reminder tick, used to detect a
        // stream that is stuck in the same non running state for a whole reminder period.
        private Base.Engine.StreamStateValue? _reminderObservedState;

        // Fetch epoch per requestor substream, announced through the initialize handshake.
        // Fetches from any other epoch are refused, see FetchDataRequest.FetchEpoch. Only
        // accessed from grain turns.
        private readonly Dictionary<string, long> _peerFetchEpochs = new Dictionary<string, long>();

        public Task ReceiveReminder(string reminderName, TickStatus status)
        {
            // The reminder is the watchdog that keeps the substream running across failures.
            // Delivering it activates the grain, which resumes the stream from its stored
            // state when no stream is running. A stream can also die without the grain
            // noticing, for example when a recovery hangs on a call to a lost silo, so a
            // stream that sits in the same non running state for a whole reminder period is
            // recreated by deactivating the grain, disposing the stream, and letting the next
            // call or reminder tick activate it again.
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
                _loggerFactory.CreateLogger<SubStreamGrain>().LogWarning(
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
                // The grain is deactivating, for example because its silo shuts down, the
                // stream itself is not stopped. Tear down locally instead of running the
                // coordinated stop: the stop drain needs the other substreams to fetch this
                // streams stop barrier, which cannot happen while a shutting down silo blocks
                // application messages, and a checkpoint in flight defers the stop until acks
                // arrive, so a stop here can hang until the deactivation timeout. The state
                // stays at the last checkpoint, the next activation resumes from it and the
                // initialize handshake reconciles the versions with the other substreams.
                // The dispose gets a bounded wait, it can hang on in flight calls to lost
                // silos and must not stall the deactivation, a new activation does not share
                // any files with the abandoned instance.
                var disposeTask = stream.DisposeAsync().AsTask();
                await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(5), cancellationToken));
            }
            _tickCancellation?.Cancel();
            _tickCancellation = null;
            await base.OnDeactivateAsync(reason, cancellationToken);
        }

        public async Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request)
        {
            // The handshake announces the requestors current fetch epoch, only fetches from
            // this epoch are served from now on. Recorded even when the stream here has not
            // started so the epoch is known as soon as it starts.
            _peerFetchEpochs[request.Requestor] = request.FetchEpoch;
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                return new InitSubstreamResponse(true, false, request.RestorePoint);
            }
            var response = await handler.TargetInitializeRequest(request.RestorePoint);
            return new InitSubstreamResponse(false, response.Success, response.RestoreVersion);
        }

        public override Task OnActivateAsync(CancellationToken cancellationToken)
        {
            StartStream();
            return base.OnActivateAsync(cancellationToken);
        }

        public async Task StartStreamAsync(StartStreamMessage startStreamMessage)
        {
            if (_state.RecordExists)
            {
                // Defense in depth, the stream grain already rejects a changed plan. A
                // direct start with different SQL must not be silently ignored, this grain
                // would keep running the old plan next to substreams running the new one.
                if (!string.Equals(_state.State.SqlText, startStreamMessage.SqlText, StringComparison.Ordinal) ||
                    _state.State.SubstreamCount != startStreamMessage.SubstreamCount)
                {
                    throw new InvalidOperationException(
                        $"Substream '{this.GetPrimaryKeyString()}' is already started with a different SQL text or substream count. Stop the stream before starting it with a new plan.");
                }
                // Same plan, a repeated start is the natural retry after a failed start,
                // make sure the stream is running.
                StartStream();
                return;
            }

            _state.State.SqlText = startStreamMessage.SqlText;
            _state.State.StreamName = startStreamMessage.StreamName;
            _state.State.SubstreamName = startStreamMessage.SubstreamName;
            _state.State.SubstreamCount = startStreamMessage.SubstreamCount;
            await _state.WriteStateAsync();

            // Keeps the substream alive across silo failures, see KeepAliveReminderName.
            // One minute is the smallest period Orleans reminders allow.
            await this.RegisterOrUpdateReminder(KeepAliveReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            StartStream();
        }

        private void StartStream()
        {
            if (_stream != null || _state.State.StreamName == null || _state.State.SqlText == null || _state.State.SubstreamName == null)
            {
                return;
            }
            ServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddKeyedSingleton(_state.State.StreamName, new FileCacheOptions()
            {
                // The file cache is transient scratch space for this stream instance. The
                // path must be unique per activation: after a silo failure a new activation
                // can start while the old activations files are not released yet, sharing a
                // directory would collide on the files.
                DirectoryPath = $"./temp/{this.GetPrimaryKeyString()}/{Guid.NewGuid():N}"
            });
            var storageBuild = new FlowtideStorageBuilder(_state.State.StreamName, serviceCollection);
            _storageBuilder(_state.State.StreamName, _state.State.SubstreamName, storageBuild);

            var stateManagerOptions = storageBuild.Build(serviceCollection.BuildServiceProvider());

            // Rebuild the plan from the SQL text, the plan builder is deterministic so all
            // substream grains compute an identical plan.
            var plan = OrleansStreamPlanBuilder.BuildPlan(_connectorManager, _state.State.SqlText, _state.State.SubstreamCount);

            FlowtideBuilder flowtideBuilder = new FlowtideBuilder(this.GetPrimaryKeyString());
            flowtideBuilder.AddPlan(plan, false);
            flowtideBuilder.AddConnectorManager(_connectorManager);
            flowtideBuilder.WithStateOptions(stateManagerOptions);

            _orleansCommunicationFactory = new OrleansCommunicationFactory(_state.State.StreamName, _grainFactory);
            // The stream uses the default scheduler instead of grain timers. Grain timers
            // require the grain activation context, but the stream registers triggers again
            // when it restarts after a failure, which happens on stream threads outside any
            // grain turn and would fail with an activation access violation. The default
            // scheduler must be ticked externally, the grain runs a tick loop next to the
            // stream, see below.
            var scheduler = new Base.Engine.DefaultStreamScheduler();
            flowtideBuilder.WithScheduler(scheduler);
            flowtideBuilder.WithLoggerFactory(_loggerFactory);
            if (_state.State.SubstreamName != null)
            {
                flowtideBuilder.SetDistributedOptions(new DistributedOptions(
                    _state.State.SubstreamName,
                    new PullExchangeReadFactory(_state.State.StreamName, _grainFactory),
                    _orleansCommunicationFactory));
            }

            // Applied last so user configuration can override the defaults set above.
            _options.ConfigureBuilder?.Invoke(flowtideBuilder);

            _stream = flowtideBuilder.Build();
            var stream = _stream;
            _tickCancellation = new CancellationTokenSource();
            var tickToken = _tickCancellation.Token;
            var logger = _loggerFactory.CreateLogger<SubStreamGrain>();
            // Task.Run so the stream runs on the thread pool. StartStream is called from a
            // grain turn, Task.Factory.StartNew would capture the grain activation scheduler
            // and run the streams state machine as activation work items, a single
            // synchronous wait in that machinery then blocks the whole activation, no grain
            // call, not even from other substreams fetching data, is served anymore.
            _lastStartFailure = null;
            _ = Task.Run(async () =>
            {
                try
                {
                    await stream.StartAsync();
                }
                catch (Exception e)
                {
                    // The failure must be logged here, the task is fire and forget so the
                    // exception is otherwise never observed anywhere. The keep alive reminder
                    // sees the stream stuck in a non running state and recreates the grain,
                    // but without this log the root cause would be undiagnosable. The failure
                    // is also kept so GetStatusAsync can surface it to the caller, the start
                    // call itself already returned success.
                    _lastStartFailure = e.ToString();
                    logger.LogError(e, "Starting the stream for substream {substream} failed.", this.GetPrimaryKeyString());
                    return;
                }
                // Drive the schedulers trigger dispatch, StartAsync does not tick it. Without
                // the ticks no recurring operator trigger ever fires, sources that poll for
                // changes through triggers would never emit new data. The loop ends when the
                // stream is stopped or the grain deactivates.
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
                            logger.LogDebug(e, "Trigger tick failed, retrying at the next tick.");
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
