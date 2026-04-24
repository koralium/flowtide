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
    /// Defines a receiver for stream failure notifications raised when an unhandled exception
    /// causes the stream to transition to a failure state.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementations are registered on a <see cref="DataflowStreamBuilder"/> via
    /// <see cref="DataflowStreamBuilder.AddFailureListener"/> and are invoked each time the stream
    /// encounters an unhandled exception that triggers a failure state transition. Multiple listeners
    /// can be registered; each is called in registration order.
    /// </para>
    /// <para>
    /// <see cref="OnFailure"/> is called before the stream begins its automatic recovery sequence.
    /// After dispatching to all listeners, the stream stops and disposes its running blocks, then
    /// automatically restarts from the last successfully committed checkpoint. Listeners can use this
    /// notification to implement custom recovery strategies — for example <c>ExitProcessStrategy</c>
    /// calls <see cref="Environment.Exit"/> to let a container orchestrator handle the restart, and
    /// <c>KillProcessStrategy</c> forcibly kills the process via
    /// <see cref="System.Diagnostics.Process.Kill()"/>. Use <c>FlowtideBuilder.WithFailureListener</c>
    /// to register a custom <see cref="Action{T}"/> callback without implementing the full interface.
    /// </para>
    /// <para>
    /// Exceptions thrown by an implementation are swallowed by the stream engine to ensure
    /// that a misbehaving listener cannot prevent the failure state from being entered or the
    /// automatic recovery from proceeding.
    /// </para>
    /// </remarks>
    public interface IFailureListener
    {
        /// <summary>
        /// Called when the stream encounters an unhandled exception and is about to enter
        /// a failure state and initiate its automatic recovery sequence.
        /// </summary>
        /// <remarks>
        /// This method is invoked synchronously within the failure transition. Implementations
        /// should avoid long-running or blocking operations, as this call occurs before the stream
        /// has stopped its running blocks. Access <see cref="StreamFailureNotification.StreamName"/>
        /// to identify the affected stream, and <see cref="StreamFailureNotification.Exception"/>
        /// for the cause of the failure, which may be <see langword="null"/> in cases where
        /// no specific exception was captured.
        /// </remarks>
        /// <param name="notification">
        /// A <see cref="StreamFailureNotification"/> carrying the name of the stream that failed
        /// and the optional exception that caused the failure.
        /// </param>
        void OnFailure(StreamFailureNotification notification);
    }
}
