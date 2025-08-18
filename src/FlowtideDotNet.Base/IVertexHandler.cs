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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Base
{
    public interface IVertexHandler
    {
        string OperatorId { get; }

        string StreamName { get; }

        IMeter Metrics { get; }

        IStateManagerClient StateClient { get; }

        ILoggerFactory LoggerFactory { get; }

        IOperatorMemoryManager MemoryManager { get; }

        /// <summary>
        /// Schedules a checkpoint
        /// </summary>
        /// <param name="time"></param>
        void ScheduleCheckpoint(TimeSpan time);

        /// <summary>
        /// Register a trigger that can be called outside from the stream
        /// it can also be registered with a schedule where the trigger will be called in a certain interval.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="schedule"></param>
        Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null);

        Task FailAndRollback(Exception? exception, long? restoreVersion = default);
    }
}
