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
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using FlowtideDotNet.Substrait;
using System.Security.Cryptography;
using System.Text;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using System.Diagnostics;
using FlowtideDotNet.Core.Connectors;

namespace FlowtideDotNet.Core.Engine
{
    public class FlowtideBuilder
    {
        DataflowStreamBuilder dataflowStreamBuilder;
        private Plan? _plan;
        private IConnectorManager? _connectorManager;
        private IReadWriteFactory? _readWriteFactory;
        private IStateHandler? _stateHandler;
        private StateManagerOptions? _stateManagerOptions;
        private int _queueSize = 100;
        private FunctionsRegister _functionsRegister;
        private int _parallelism = 1;
        private TimeSpan _getTimestampInterval = TimeSpan.FromHours(1);

        public FlowtideBuilder(string streamName)
        {
            dataflowStreamBuilder = new DataflowStreamBuilder(streamName);
            _functionsRegister = new FunctionsRegister();
            // Register default functions directly
            BuiltinFunctions.RegisterFunctions(_functionsRegister);
        }

        public IFunctionsRegister FunctionsRegister => _functionsRegister;

        public FlowtideBuilder AddPlan(Plan plan, bool optimize = true)
        {
            if (optimize)
            {
                plan = Optimizer.PlanOptimizer.Optimize(plan);
            }
            
            _plan = plan;
            return this;
        }

        public FlowtideBuilder AddConnectorManager(IConnectorManager connectorManager)
        {
            _connectorManager = connectorManager;
            return this;
        }

        [Obsolete("Use ConnectorManager instead")]
        public FlowtideBuilder AddReadWriteFactory(IReadWriteFactory readWriteFactory)
        {
            _readWriteFactory = readWriteFactory;
            return this;
        }

        public FlowtideBuilder WithStateOptions(StateManagerOptions stateManagerOptions)
        {
            dataflowStreamBuilder.WithStateOptions(stateManagerOptions);
            return this;
        }

        public FlowtideBuilder AddStateHandler(IStateHandler stateHandler)
        {
            dataflowStreamBuilder.WithStateHandler(stateHandler);
            _stateHandler = stateHandler;
            return this;
        }

        public FlowtideBuilder WithScheduler(IStreamScheduler streamScheduler)
        {
            dataflowStreamBuilder.WithStreamScheduler(streamScheduler);
            return this;
        }

        public FlowtideBuilder WithNotificationReciever(IStreamNotificationReciever notificationReciever)
        {
            dataflowStreamBuilder.WithNotificationReciever(notificationReciever);
            return this;
        }

        public FlowtideBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            dataflowStreamBuilder.WithLoggerFactory(loggerFactory);
            return this;
        }

        public FlowtideBuilder SetMessageQueueSize(int queueSize)
        {
            _queueSize = queueSize;
            return this;
        }

        public FlowtideBuilder SetParallelism(int parallelism)
        {
            _parallelism = parallelism;
            return this;
        }

        public FlowtideBuilder SetGetTimestampUpdateInterval(TimeSpan interval)
        {
            _getTimestampInterval = interval;
            return this;
        }

        public FlowtideBuilder SetMinimumTimeBetweenCheckpoint(TimeSpan timeSpan)
        {
            dataflowStreamBuilder.SetMinimumTimeBetweenCheckpoint(timeSpan);
            return this;
        }

        private string ComputePlanHash()
        {
            Debug.Assert(_plan != null, "Plan should not be null.");
            using (SHA256 sha256 = SHA256.Create())
            {
                string json = "";
                try
                {
                    json = SubstraitSerializer.SerializeToJson(_plan);
                }
                catch
                {
                    Console.Error.WriteLine("Failed to serialize plan for hash check.");
                }
                var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(json));
                return Convert.ToBase64String(hashBytes);
            }
        }

        public FlowtideDotNet.Base.Engine.DataflowStream Build()
        {
            if (_plan == null)
            {
                throw new InvalidOperationException("No plan has been added.");
            }
            if (_connectorManager == null && _readWriteFactory == null)
            {
                throw new InvalidOperationException("No connector manager or ReadWriteFactory has been added.");
            }
            var hash = ComputePlanHash();
            dataflowStreamBuilder.SetVersionInformation(1, hash);

            // Modify plan
            if (_connectorManager != null)
            {
                var planModifier = new ConnectorPlanModifyVisitor(_connectorManager);
                planModifier.VisitPlan(_plan);
            }

            SubstraitVisitor visitor = new SubstraitVisitor(
                _plan, 
                dataflowStreamBuilder, 
                _connectorManager, 
                _readWriteFactory,
                _queueSize, 
                _functionsRegister, 
                _parallelism, 
                _getTimestampInterval);

            visitor.BuildPlan();

            return dataflowStreamBuilder.Build();
        }
    }
}
