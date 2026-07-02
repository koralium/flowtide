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

namespace FlowtideDotNet.Base.Engine
{
    /// <summary>
    /// Defines the contract for scheduling and dispatching recurring trigger events to stream operators.
    /// </summary>
    /// <remarks>
    /// <para>
    /// An <see cref="IStreamScheduler"/> is configured on a <see cref="DataflowStreamBuilder"/> via
    /// <see cref="DataflowStreamBuilder.WithStreamScheduler"/>. If no scheduler is provided,
    /// <see cref="DefaultStreamScheduler"/> is used automatically. The stream engine calls
    /// <see cref="Initialize"/> once during <c>StreamContext</c> construction, supplying itself as
    /// the <see cref="IStreamTriggerCaller"/> through which trigger events are dispatched to operators.
    /// </para>
    /// <para>
    /// <see cref="Schedule"/> is called each time a vertex registers a recurring trigger (for example
    /// via <c>RegisterTrigger</c> inside a vertex's <c>Initialize</c> override). All registered
    /// schedules are cleared between stream restarts by calls to <see cref="RemoveSchedule"/> from
    /// <c>StreamContext.ClearTriggers</c>, ensuring that vertices re-register their triggers with the
    /// correct interval on every startup.
    /// </para>
    /// <para>
    /// Implementations are free to use any timing mechanism — for example a background
    /// <see cref="System.Threading.Timer"/>, a hosted service, or Quartz.NET — as long as they
    /// ultimately call <see cref="IStreamTriggerCaller.CallTrigger"/> when a trigger is due.
    /// <see cref="DefaultStreamScheduler"/> uses a priority-queue approach driven externally by
    /// <see cref="DataflowStream.RunAsync"/>'s 10 ms periodic tick loop.
    /// </para>
    /// </remarks>
    public interface IStreamScheduler
    {
        /// <summary>
        /// Connects this scheduler to the stream engine and prepares it to begin dispatching triggers.
        /// </summary>
        /// <remarks>
        /// Called once by <c>StreamContext</c> immediately after the scheduler is assigned, before any
        /// calls to <see cref="Schedule"/>. The supplied <paramref name="streamTriggerCaller"/> must be
        /// stored and used by subsequent scheduling operations to dispatch trigger events to the
        /// correct operator vertices.
        /// </remarks>
        /// <param name="streamTriggerCaller">
        /// The <see cref="IStreamTriggerCaller"/> provided by the stream engine that routes a named
        /// trigger event to all operator vertices that registered it.
        /// </param>
        void Initialize(IStreamTriggerCaller streamTriggerCaller);

        /// <summary>
        /// Registers a recurring trigger that the scheduler should dispatch at the specified interval.
        /// </summary>
        /// <remarks>
        /// Called by the stream engine each time a vertex registers a periodic trigger during its
        /// <c>Initialize</c> phase. The combination of <paramref name="operatorName"/> and
        /// <paramref name="triggerName"/> uniquely identifies the schedule entry. Implementations
        /// must ensure that <see cref="IStreamTriggerCaller.CallTrigger"/> is invoked with the same
        /// <paramref name="operatorName"/> and <paramref name="triggerName"/> each time the interval elapses.
        /// </remarks>
        /// <param name="triggerName">The name identifying the trigger within the operator.</param>
        /// <param name="operatorName">The name of the operator that owns the trigger.</param>
        /// <param name="interval">The recurring interval at which the trigger should fire.</param>
        /// <returns>A task representing the asynchronous registration operation.</returns>
        Task Schedule(string triggerName, string operatorName, TimeSpan interval);

        /// <summary>
        /// Removes a previously registered trigger schedule so that it is no longer dispatched.
        /// </summary>
        /// <remarks>
        /// Called by <c>StreamContext.ClearTriggers</c> before each stream restart to reset all
        /// active schedules. Vertices re-register their triggers on every startup, so implementations
        /// must honour this removal and stop dispatching the given trigger after this call returns.
        /// </remarks>
        /// <param name="triggerName">The name of the trigger to remove.</param>
        /// <param name="operatorName">The name of the operator that owns the trigger.</param>
        /// <returns>A task representing the asynchronous removal operation.</returns>
        Task RemoveSchedule(string triggerName, string operatorName);
    }
}
