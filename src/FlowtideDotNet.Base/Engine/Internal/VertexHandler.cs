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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Engine.Internal
{
    internal class VertexHandler : IVertexHandler
    {
        private readonly string operatorName;
        private readonly Action<TimeSpan> checkpointFunc;
        private readonly Func<string, string, TimeSpan?, Task> registerTrigger;

        public VertexHandler(
            string streamName, 
            string operatorName, 
            Action<TimeSpan> checkpointFunc, 
            Func<string, string, TimeSpan?, Task> registerTrigger, 
            IMeter metrics, 
            IStateManagerClient stateClient,
            ILoggerFactory loggerFactory)
        {
            StreamName = streamName;
            this.operatorName = operatorName;
            this.checkpointFunc = checkpointFunc;
            this.registerTrigger = registerTrigger;
            Metrics = metrics;
            StateClient = stateClient;
            LoggerFactory = loggerFactory;
        }

        public string StreamName { get; }

        public IMeter Metrics { get; }

        public IStateManagerClient StateClient { get; }

        public ILoggerFactory LoggerFactory { get; }

        public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null)
        {
            return registerTrigger(operatorName, name, scheduledInterval);
        }

        public void ScheduleCheckpoint(TimeSpan time)
        {
            checkpointFunc(time);
        }
    }
}
