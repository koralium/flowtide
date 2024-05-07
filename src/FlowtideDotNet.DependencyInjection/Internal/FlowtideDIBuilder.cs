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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.DependencyInjection.Internal
{
    internal class FlowtideDIBuilder : IFlowtideDIBuilder
    {
        private readonly string streamName;
        private readonly IServiceCollection services; 

        public FlowtideDIBuilder(string streamName, IServiceCollection services)
        {
            this.streamName = streamName;
            this.services = services;
        }

        public IServiceCollection Services => services;

        public string StreamName => streamName;

        public IFlowtideDIBuilder AddConnectors(Action<IDependencyInjectionConnectorManager> registerFunc)
        {   
            services.AddKeyedSingleton<IConnectorManager>(streamName, (provider, key) =>
            {
                var manager = new DependencyInjectionConnectorManager(provider);
                registerFunc(manager);
                return manager;
            });
            return this;
        }

        public IFlowtideDIBuilder AddStorage(Action<IFlowtideStorageBuilder> storageOptions)
        {
            var storageBuilder = new FlowtideStorageBuilder(streamName, services);
            storageOptions?.Invoke(storageBuilder);
            services.AddKeyedSingleton(streamName, (provider, key) =>
            {
                return storageBuilder.Build(provider);
            });
            return this;
        }

        public IFlowtideDIBuilder SetPlanProvider(IFlowtidePlanProvider planProvider)
        {
            services.AddKeyedSingleton(streamName, planProvider);
            return this;
        }

        IFlowtideDIBuilder IFlowtideDIBuilder.SetPlanProvider<TPlanProvider>()
        {
            services.AddKeyedSingleton<IFlowtidePlanProvider, TPlanProvider>(streamName);
            return this;
        }

        internal Base.Engine.DataflowStream Build(IServiceProvider serviceProvider)
        {
            var connectorManager = serviceProvider.GetRequiredKeyedService<IConnectorManager>(streamName);
            var planProvider = serviceProvider.GetRequiredKeyedService<IFlowtidePlanProvider>(streamName);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var stateManager = serviceProvider.GetRequiredKeyedService<StateManagerOptions>(streamName);

            var plan = planProvider.GetPlan();

            var streamBuilder = new FlowtideBuilder(streamName)
                .AddConnectorManager(connectorManager)
                .AddPlan(plan)
                .WithStateOptions(stateManager)
                .WithLoggerFactory(loggerFactory);

            var stream = streamBuilder.Build();

            return stream;
        }
    }
}
