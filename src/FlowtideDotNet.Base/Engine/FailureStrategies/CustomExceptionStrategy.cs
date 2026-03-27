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

namespace FlowtideDotNet.Engine.FailureStrategies
{
    /// <summary>
    /// An <see cref="IFailureListener"/> that delegates stream failure notifications to a
    /// caller-supplied <see cref="Action{T}"/> callback.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Register this strategy via <see cref="DataflowStreamBuilder.AddFailureListener"/> or use
    /// <c>FlowtideBuilder.WithFailureListener(Action&lt;Exception?&gt;)</c>, which constructs a
    /// <see cref="CustomExceptionStrategy"/> internally, to react to stream failures without
    /// implementing the full <see cref="IFailureListener"/> interface.
    /// </para>
    /// <para>
    /// Unlike <see cref="ExitProcessStrategy"/> and <see cref="KillProcessStrategy"/>, this strategy
    /// does not terminate the process. After the callback returns, the stream's built-in automatic
    /// recovery sequence continues normally, restarting the stream from the last committed checkpoint.
    /// Typical uses for the callback include custom logging, sending an alert, incrementing a failure
    /// metric, or signalling an external monitoring system.
    /// </para>
    /// </remarks>
    public class CustomExceptionStrategy : IFailureListener
    {
        private readonly Action<Exception?> _handler;

        /// <summary>
        /// Initializes a new <see cref="CustomExceptionStrategy"/> with the specified failure callback.
        /// </summary>
        /// <param name="handler">
        /// The <see cref="Action{T}"/> to invoke when a stream failure occurs.
        /// Receives the <see cref="Exception"/> that caused the failure, or <see langword="null"/>
        /// if no specific exception was captured at the point of failure.
        /// </param>
        public CustomExceptionStrategy(Action<Exception?> handler)
        {
            _handler = handler;
        }

        /// <summary>
        /// Invokes the callback supplied at construction time, passing the exception that caused
        /// the stream failure.
        /// </summary>
        /// <remarks>
        /// The callback receives <see cref="StreamFailureNotification.Exception"/>, which may be
        /// <see langword="null"/> if no specific exception was captured. After the callback returns,
        /// the stream's built-in automatic recovery proceeds normally; the callback must not block
        /// indefinitely, as this method is invoked synchronously within the failure transition.
        /// </remarks>
        /// <param name="notification">
        /// The <see cref="StreamFailureNotification"/> describing the stream failure. Only
        /// <see cref="StreamFailureNotification.Exception"/> is forwarded to the callback;
        /// <see cref="StreamFailureNotification.StreamName"/> is available if a custom
        /// <see cref="IFailureListener"/> implementation is preferred over this strategy.
        /// </param>
        public void OnFailure(StreamFailureNotification notification)
        {
            _handler(notification.Exception);
        }
    }
}
