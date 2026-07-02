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
    /// Defines the contract for dispatching trigger events to one or all operators within a stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="IStreamTriggerCaller"/> is implemented by the stream engine's <c>StreamContext</c>
    /// and is passed to an <see cref="IStreamScheduler"/> via <see cref="IStreamScheduler.Initialize"/>
    /// so that the scheduler can dispatch trigger events when a scheduled interval elapses.
    /// It is also surfaced publicly on <see cref="DataflowStream.CallTrigger"/> for imperative,
    /// on-demand trigger invocations from outside the stream.
    /// </para>
    /// <para>
    /// Two dispatch modes are available. The broadcast overload
    /// <see cref="CallTrigger(string, object?)"/> routes the trigger to every operator that has
    /// registered a handler for the given <c>triggerName</c>, making it suitable for stream-wide
    /// signals such as a scheduled flush. The targeted overload
    /// <see cref="CallTrigger(string, string, object?)"/> routes the trigger to a single named
    /// operator, making it suitable for precise, per-operator control.
    /// In both cases, each matching operator receives the trigger via its <c>OnTrigger</c> override
    /// by queuing a <see cref="TriggerEvent"/> onto the operator's internal dataflow block.
    /// </para>
    /// </remarks>
    public interface IStreamTriggerCaller
    {
        /// <summary>
        /// Dispatches a named trigger to every operator in the stream that has registered a handler
        /// for <paramref name="triggerName"/>.
        /// </summary>
        /// <remarks>
        /// The engine looks up all operators registered under <paramref name="triggerName"/> and
        /// queues a <see cref="TriggerEvent"/> on each one. If no operator has registered the given
        /// trigger name the call is a no-op. The returned task completes once the event has been
        /// enqueued on all matching operators, not once their <c>OnTrigger</c> handlers have finished.
        /// </remarks>
        /// <param name="triggerName">
        /// The name identifying the trigger, matching the name used when the operator registered
        /// the trigger via <c>RegisterTrigger</c>.
        /// </param>
        /// <param name="state">
        /// An optional context object forwarded as-is to each operator's <c>OnTrigger</c> override.
        /// Pass <see langword="null"/> when no additional context is needed.
        /// </param>
        /// <returns>
        /// A task that completes once the trigger event has been queued on all matching operators.
        /// </returns>
        Task CallTrigger(string triggerName, object? state);

        /// <summary>
        /// Dispatches a named trigger to a single specific operator within the stream.
        /// </summary>
        /// <remarks>
        /// The engine looks up the operator identified by <paramref name="operatorName"/> and, if it
        /// has registered a handler for <paramref name="triggerName"/>, queues a <see cref="TriggerEvent"/>
        /// on it. If the operator does not exist or has not registered the given trigger name the call
        /// is a no-op. The returned task completes once the event has been enqueued, not once the
        /// operator's <c>OnTrigger</c> handler has finished.
        /// </remarks>
        /// <param name="operatorName">
        /// The unique name of the target operator within the stream, as set during
        /// <see cref="IStreamVertex.Setup"/>.
        /// </param>
        /// <param name="triggerName">
        /// The name identifying the trigger, matching the name used when the operator registered
        /// the trigger via <c>RegisterTrigger</c>.
        /// </param>
        /// <param name="state">
        /// An optional context object forwarded as-is to the operator's <c>OnTrigger</c> override.
        /// Pass <see langword="null"/> when no additional context is needed.
        /// </param>
        /// <returns>
        /// A task that completes once the trigger event has been queued on the target operator,
        /// or immediately if no matching operator or trigger registration was found.
        /// </returns>
        Task CallTrigger(string operatorName, string triggerName, object? state);
    }
}
