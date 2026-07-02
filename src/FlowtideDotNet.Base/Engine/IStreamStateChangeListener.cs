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
    /// Defines a receiver for state change notifications raised each time the stream transitions
    /// to a new <see cref="StreamStateValue"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementations are registered on a <see cref="DataflowStreamBuilder"/> via
    /// <see cref="DataflowStreamBuilder.AddStateChangeListener"/> and are invoked each time the stream
    /// enters a new lifecycle state. Multiple listeners can be registered; each is called in
    /// registration order.
    /// </para>
    /// <para>
    /// <see cref="OnStreamStateChange"/> is called synchronously within <c>StreamContext.TransitionTo</c>
    /// before the state machine transition begins. This means the notification arrives at the moment
    /// the new <see cref="StreamStateValue"/> is recorded — for example
    /// <see cref="StreamStateValue.Starting"/>, <see cref="StreamStateValue.Running"/>,
    /// <see cref="StreamStateValue.Failure"/>, or <see cref="StreamStateValue.Stopping"/> — before any
    /// of the associated transition work, such as stopping blocks or re-initializing the stream, is
    /// performed. Typical uses include updating an external health dashboard, logging state transitions,
    /// or signalling a dependent service.
    /// </para>
    /// <para>
    /// Exceptions thrown by an implementation are swallowed by the stream engine to ensure
    /// that a misbehaving listener cannot block a state transition or disrupt stream processing.
    /// </para>
    /// </remarks>
    public interface IStreamStateChangeListener
    {
        /// <summary>
        /// Called when the stream transitions to a new <see cref="StreamStateValue"/>.
        /// </summary>
        /// <remarks>
        /// This method is invoked synchronously before the transition work begins; implementations
        /// should avoid long-running or blocking operations to prevent delaying the stream's lifecycle
        /// progression. Access <see cref="StreamStateChangeNotification.StreamName"/> to identify
        /// the affected stream and <see cref="StreamStateChangeNotification.State"/> for the
        /// <see cref="StreamStateValue"/> the stream is transitioning into.
        /// </remarks>
        /// <param name="notification">
        /// A <see cref="StreamStateChangeNotification"/> carrying the name of the stream and the
        /// new <see cref="StreamStateValue"/> that the stream is entering.
        /// </param>
        void OnStreamStateChange(StreamStateChangeNotification notification);
    }
}
