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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using Microsoft.Extensions.Logging;
using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Grains
{
    public class StreamGrain : Grain, IStreamGrain
    {
        private readonly IConnectorManager _connectorManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IGrainFactory _grainFactory;
        private Base.Engine.DataflowStream? _stream;
        private OrleansCommunicationFactory? _orleansCommunicationFactory;

        public StreamGrain(IConnectorManager connectorManager, ILoggerFactory loggerFactory, IGrainFactory grainFactory)
        {
            this._connectorManager = connectorManager;
            this._loggerFactory = loggerFactory;
            this._grainFactory = grainFactory;
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

        public async Task StartStreamAsync(StartStreamMessage startStreamMessage)
        {
            FlowtideBuilder flowtideBuilder = new FlowtideBuilder(startStreamMessage.StreamName);
            flowtideBuilder.AddPlan(startStreamMessage.Plan, false);
            flowtideBuilder.AddConnectorManager(_connectorManager);
            flowtideBuilder.WithStateOptions(new Storage.StateManager.StateManagerOptions()
            {
                MinCachePageCount = 0,
                CachePageCount = 300_000,
                //MaxProcessMemory = 5 * 1024 * 1024 * 1024L,
                //PersistentStorage = new FasterKvPersistentStorage(new FASTER.core.FasterKVSettings<long, FASTER.core.SpanByte>("./data/" + startStreamMessage.SubstreamName, true)
                //{
                //    PageSize = 16 * 1024 * 1024,
                //    MemorySize = 32 * 1024 * 1024
                //})
                PersistentStorage = new FileCachePersistentStorage(new Storage.FileCacheOptions()
                {
                    DirectoryPath = "./data" + startStreamMessage.SubstreamName
                }),
                TemporaryStorageOptions = new Storage.FileCacheOptions()
                {
                    DirectoryPath = "./temp" + startStreamMessage.SubstreamName
                }
            });

            _orleansCommunicationFactory = new OrleansCommunicationFactory(_grainFactory);
            flowtideBuilder.WithScheduler(new OrleansStreamScheduler(this.RegisterTimer));
            flowtideBuilder.WithLoggerFactory(_loggerFactory);
            if (startStreamMessage.SubstreamName != null)
            {
                flowtideBuilder.SetDistributedOptions(new DistributedOptions(
                    startStreamMessage.SubstreamName, 
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
