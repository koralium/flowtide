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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.DependencyInjection.Internal;
using Microsoft.Extensions.DependencyInjection;
using FlowtideDotNet.Storage;

namespace FlowtideDotNet.Orleans.Grains
{
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
            var handler = _orleansCommunicationFactory.handlers[request.Requestor];
            await handler.TargetCheckpointDone(request.CheckpointVersion);
        }

        public async Task FailAndRecoverAsync(FailAndRecoverRequest request)
        {
            var handler = _orleansCommunicationFactory.handlers[request.Requestor];
            await handler.FailAndRecover(request.RecoveryPoint);
        }

        public async Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request)
        {
            var handler = _orleansCommunicationFactory.handlers[request.Requestor];
            var data = await handler.GetData(request.TargetIds, request.NumberOfEvents, default);
            return new FetchDataResponse(data);
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
            return new GetEventsResponse(msg.LastEventId, msg.OutEvents, false);
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
            if (_orleansCommunicationFactory == null)
            {
                return new InitSubstreamResponse(true, false, request.RestorePoint);
            }
            var handler = _orleansCommunicationFactory.handlers[request.Requestor];
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

            _state.State.Plan = startStreamMessage.Plan;
            _state.State.StreamName = startStreamMessage.StreamName;
            _state.State.SubstreamName = startStreamMessage.SubstreamName;
            await _state.WriteStateAsync();

            StartStream();
        }

        private void StartStream()
        {
            if (_stream != null || _state.State.StreamName == null || _state.State.Plan == null || _state.State.SubstreamName == null)
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

            FlowtideBuilder flowtideBuilder = new FlowtideBuilder(this.GetPrimaryKeyString());
            flowtideBuilder.AddPlan(_state.State.Plan, false);
            flowtideBuilder.AddConnectorManager(_connectorManager);
            flowtideBuilder.WithStateOptions(stateManagerOptions);

            _orleansCommunicationFactory = new OrleansCommunicationFactory(_state.State.StreamName, _grainFactory);
            flowtideBuilder.WithScheduler(new OrleansStreamScheduler(this.RegisterTimer));
            flowtideBuilder.WithLoggerFactory(_loggerFactory);
            if (_state.State.SubstreamName != null)
            {
                flowtideBuilder.SetDistributedOptions(new DistributedOptions(
                    _state.State.SubstreamName,
                    new PullExchangeReadFactory(_grainFactory),
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
