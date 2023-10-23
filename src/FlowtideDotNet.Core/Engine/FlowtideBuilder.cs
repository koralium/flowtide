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
using System.Text.Json;
using System.Text;

namespace FlowtideDotNet.Core.Engine
{
    public class FlowtideBuilder
    {
        DataflowStreamBuilder dataflowStreamBuilder;
        private Plan? _plan;
        private IReadWriteFactory? _readWriteFactory;
        private IStateHandler? _stateHandler;
        private StateManagerOptions? _stateManagerOptions;
        private int _queueSize = 100;
        public FlowtideBuilder(string streamName)
        {
            dataflowStreamBuilder = new DataflowStreamBuilder(streamName);
        }

        public FlowtideBuilder AddPlan(Plan plan, bool optimize = true)
        {
            if (optimize)
            {
                plan = Optimizer.PlanOptimizer.Optimize(plan);
            }
            
            _plan = plan;
            return this;
        }

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

        private string ComputePlanHash()
        {
            using (SHA256 sha256 = SHA256.Create())
            {
                MemoryStream memoryStream = new MemoryStream();
                JsonSerializer.Serialize(memoryStream, _plan);
                var hashBytes = sha256.ComputeHash(memoryStream);
                return Convert.ToBase64String(hashBytes);
            }
        }

        public FlowtideDotNet.Base.Engine.DataflowStream Build()
        {
            if (_plan == null)
            {
                throw new InvalidOperationException("No plan has been added.");
            }
            var hash = ComputePlanHash();
            dataflowStreamBuilder.SetVersionInformation(1, hash);

            SubstraitVisitor visitor = new SubstraitVisitor(_plan, dataflowStreamBuilder, _readWriteFactory, _queueSize);
            visitor.BuildPlan();

            return dataflowStreamBuilder.Build();
        }
    }
}
