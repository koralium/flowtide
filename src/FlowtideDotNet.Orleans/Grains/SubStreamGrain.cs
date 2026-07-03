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
    public class SubStreamGrain : Grain, ISubStreamGrain
    {
        private readonly IPersistentState<SubStreamGrainStorage> _state;
        private readonly IConnectorManager _connectorManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IGrainFactory _grainFactory;
        private readonly Action<string, string, IFlowtideStorageBuilder> _storageBuilder;
        private Base.Engine.DataflowStream? _stream;
        private OrleansCommunicationFactory? _orleansCommunicationFactory;
        // Only used to serialize outgoing events, serializing never allocates batch memory.
        private readonly SubstreamEventWireSerializer _wireSerializer = new SubstreamEventWireSerializer();

        public SubStreamGrain(
            [PersistentState("substream", "stream_metadata")] IPersistentState<SubStreamGrainStorage> state,
            IConnectorManager connectorManager, 
            ILoggerFactory loggerFactory, 
            IGrainFactory grainFactory,
            Action<string, string, IFlowtideStorageBuilder> storageBuilder)
        {
            this._state = state;
            this._connectorManager = connectorManager;
            this._loggerFactory = loggerFactory;
            this._grainFactory = grainFactory;
            this._storageBuilder = storageBuilder;
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

        public async Task FailAndRecoverAsync(FailAndRecoverRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
                // The stream has not started yet, there is nothing to recover.
                return;
            }
            await handler.FailAndRecover(request.RecoveryPoint);
        }

        public async Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request)
        {
            if (_orleansCommunicationFactory == null ||
                !_orleansCommunicationFactory.handlers.TryGetValue(request.Requestor, out var handler))
            {
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
            // Clear the grain state so a reactivation does not start the stream again, the
            // stream state itself stays in its storage and a new start resumes from it.
            await _state.ClearStateAsync();
            DeactivateOnIdle();
        }

        public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        {
            if (_stream != null)
            {
                // Stop the stream if this is being deactivated
                await _stream.StopAsync();
            }
            await base.OnDeactivateAsync(reason, cancellationToken);
        }

        public async Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request)
        {
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
                return;
            }

            _state.State.SqlText = startStreamMessage.SqlText;
            _state.State.StreamName = startStreamMessage.StreamName;
            _state.State.SubstreamName = startStreamMessage.SubstreamName;
            _state.State.SubstreamCount = startStreamMessage.SubstreamCount;
            await _state.WriteStateAsync();

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
                DirectoryPath = "./temp/" + this.GetPrimaryKeyString()
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
            flowtideBuilder.WithScheduler(new OrleansStreamScheduler(this.RegisterTimer));
            flowtideBuilder.WithLoggerFactory(_loggerFactory);
            if (_state.State.SubstreamName != null)
            {
                flowtideBuilder.SetDistributedOptions(new DistributedOptions(
                    _state.State.SubstreamName,
                    new PullExchangeReadFactory(_state.State.StreamName, _grainFactory),
                    _orleansCommunicationFactory));
            }

            _stream = flowtideBuilder.Build();
            _ = Task.Factory.StartNew(async () =>
            {
                await _stream.StartAsync();
            })
                .Unwrap();

        }
    }
}
