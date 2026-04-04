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
    /// <summary>
    /// Provides context and operational hooks for a stream vertex (operator) allowing it to interact 
    /// with the underlying stream engine, manage state, log information, and report metrics.
    /// </summary>
    public interface IVertexHandler
    {
        /// <summary>
        /// Gets the unique identifier or name of the operator within the stream.
        /// </summary>
        string OperatorId { get; }

        /// <summary>
        /// Gets the name of the stream this operator belongs to.
        /// </summary>
        string StreamName { get; }

        /// <summary>
        /// Gets the metric manager used to record operational metrics for this vertex.
        /// </summary>
        IMeter Metrics { get; }

        /// <summary>
        /// Gets the state manager client used by this vertex to read, write, and persist distributed state.
        /// </summary>
        IStateManagerClient StateClient { get; }

        /// <summary>
        /// Gets the logger factory used to create loggers for tracing and debugging within the vertex.
        /// </summary>
        ILoggerFactory LoggerFactory { get; }

        /// <summary>
        /// Gets the memory manager configured for this specific operator, used to track and allocate memory safely.
        /// </summary>
        IOperatorMemoryManager MemoryManager { get; }

        /// <summary>
        /// Schedules a checkpoint to be taken by the stream engine.
        /// </summary>
        /// <param name="time">The interval after which the checkpoint should be scheduled.</param>
        void ScheduleCheckpoint(TimeSpan time);

        /// <summary>
        /// Registers a trigger that can be called externally from the stream.
        /// It can also be registered with a schedule where the trigger will be called in a specific interval.
        /// </summary>
        /// <param name="name">The unique name of the trigger.</param>
        /// <param name="scheduledInterval">An optional interval indicating how often the trigger should fire automatically.</param>
        Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null);

        /// <summary>
        /// Signals the stream engine that a critical failure has occurred and requests a rollback to a previous checkpoint.
        /// </summary>
        /// <param name="exception">The exception that caused the failure, if any.</param>
        /// <param name="restoreVersion">An optional specific checkpoint version to roll back to. If not provided, the latest successful checkpoint is used.</param>
        Task FailAndRollback(Exception? exception, long? restoreVersion = default);
    }
}
