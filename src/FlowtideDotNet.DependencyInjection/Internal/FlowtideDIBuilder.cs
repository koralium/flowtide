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
using FlowtideDotNet.DependencyInjection.Exceptions;
using FlowtideDotNet.Engine.FailureStrategies;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.DependencyInjection.Internal
{
    internal class FlowtideDIBuilder : IFlowtideDIBuilder
    {
        private readonly string streamName;
        private readonly IServiceCollection services;
        private readonly List<Action<IServiceProvider, FlowtideBuilder>> _customOptions;

        public FlowtideDIBuilder(string streamName, IServiceCollection services)
        {
            this.streamName = streamName;
            this.services = services;
            _customOptions = new List<Action<IServiceProvider, FlowtideBuilder>>();
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
            var connectorManager = serviceProvider.GetKeyedService<IConnectorManager>(streamName);
            var planProvider = serviceProvider.GetKeyedService<IFlowtidePlanProvider>(streamName);
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var stateManager = serviceProvider.GetKeyedService<StateManagerOptions>(streamName);

            var pauseMonitor = serviceProvider.GetService<IOptionsMonitor<FlowtidePauseOptions>>();
            
            if (connectorManager == null)
            {
                throw new FlowtideMissingConnectorManagerException("IConnectorManager must be registered in the service collection, please do so manually or use the \"AddConnectors\" method.");
            }

            if (planProvider == null)
            {
                throw new FlowtideMissingPlanProviderException("IFlowtidePlanProvider must be registered in the service collection, please do so manually or use the \"AddPlan\" method.");
            }

            if (stateManager == null)
            {
                throw new FlowtideMissingStateManagerException("StateManagerOptions must be registered in the service collection, please do so manually or use the \"AddStorage\" method.");
            }

            var plan = planProvider.GetPlan();

            var streamBuilder = new FlowtideBuilder(streamName)
                .AddConnectorManager(connectorManager)
                .AddPlan(plan)
                .WithStateOptions(stateManager);

            if (pauseMonitor != null)
            {
                streamBuilder.WithPauseMonitor(pauseMonitor);
            }

            if (loggerFactory != null)
            {
                streamBuilder.WithLoggerFactory(loggerFactory);
            }

            foreach (var customOption in _customOptions)
            {
                customOption(serviceProvider, streamBuilder);
            }

            var stream = streamBuilder.Build();

            return stream;
        }

        public IFlowtideDIBuilder AddCustomOptions(Action<IServiceProvider, FlowtideBuilder> options)
        {
            _customOptions.Add(options);
            return this;
        }
    }

}
