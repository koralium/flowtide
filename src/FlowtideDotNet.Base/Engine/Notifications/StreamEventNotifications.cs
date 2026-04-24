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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;

namespace FlowtideDotNet.Base.Engine
{
    /// <summary>
    /// A stack-allocated notification carrying stream state change information
    /// delivered to <see cref="IStreamStateChangeListener"/> implementations.
    /// </summary>
    /// <remarks>
    /// This type is a <see langword="ref struct"/> to avoid heap allocation on each
    /// state transition and to allow its fields to be stored as managed references.
    /// It must not be stored beyond the duration of the
    /// <see cref="IStreamStateChangeListener.OnStreamStateChange"/> call in which it is received.
    /// Instances are created internally by <c>StreamNotificationReceiver</c> each time
    /// <c>StreamContext</c> transitions to a new <see cref="StreamStateValue"/>.
    /// </remarks>
    public ref struct StreamStateChangeNotification
    {
        /// <summary>
        /// A read-only managed reference to the name of the stream that changed state.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// A read-only managed reference to the new <see cref="StreamStateValue"/> the stream is
        /// transitioning into.
        /// </summary>
        public readonly ref StreamStateValue State;

        /// <summary>
        /// Initializes a new <see cref="StreamStateChangeNotification"/> with references to the
        /// stream name and the new state value.
        /// </summary>
        /// <param name="streamName">
        /// A <see langword="ref"/> to the stream name string held by <c>StreamNotificationReceiver</c>.
        /// </param>
        /// <param name="state">
        /// A <see langword="ref"/> to the new <see cref="StreamStateValue"/> that the stream is entering.
        /// </param>
        public StreamStateChangeNotification(ref string streamName, ref StreamStateValue state)
        {
            StreamName = ref streamName;
            State = ref state;
        }
    }

    /// <summary>
    /// A stack-allocated notification carrying stream checkpoint completion information
    /// delivered to <see cref="ICheckpointListener"/> implementations.
    /// </summary>
    /// <remarks>
    /// This type is a <see langword="ref struct"/> to avoid heap allocation on each checkpoint
    /// boundary and to allow its fields to be stored as managed references.
    /// It must not be stored beyond the duration of the
    /// <see cref="ICheckpointListener.OnCheckpointComplete"/> call in which it is received.
    /// Instances are created internally by <c>StreamNotificationReceiver</c> after the
    /// state manager has durably written all operator state for the completed checkpoint.
    /// </remarks>
    public ref struct StreamCheckpointNotification
    {
        /// <summary>
        /// A read-only managed reference to the name of the stream that completed the checkpoint.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// Initializes a new <see cref="StreamCheckpointNotification"/> with a reference to the
        /// stream name.
        /// </summary>
        /// <param name="streamName">
        /// A <see langword="ref"/> to the stream name string held by <c>StreamNotificationReceiver</c>.
        /// </param>
        public StreamCheckpointNotification(ref string streamName)
        {
            StreamName = ref streamName;
        }
    }

    /// <summary>
    /// A stack-allocated notification carrying stream failure information
    /// delivered to <see cref="IFailureListener"/> implementations.
    /// </summary>
    /// <remarks>
    /// This type is a <see langword="ref struct"/> to avoid heap allocation on each failure event
    /// and to allow its fields to be stored as managed references.
    /// It must not be stored beyond the duration of the
    /// <see cref="IFailureListener.OnFailure"/> call in which it is received.
    /// Instances are created internally by <c>StreamNotificationReceiver</c> when the stream
    /// engine calls <c>StreamContext.OnFailure</c> in response to an unhandled exception.
    /// </remarks>
    public ref struct StreamFailureNotification
    {
        /// <summary>
        /// A read-only managed reference to the name of the stream that failed.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// The exception that caused the failure, or <see langword="null"/> if no specific
        /// exception was captured at the point of failure.
        /// </summary>
        public readonly Exception? Exception;

        /// <summary>
        /// Initializes a new <see cref="StreamFailureNotification"/> with a reference to the
        /// stream name and the optional failure exception.
        /// </summary>
        /// <param name="streamName">
        /// A <see langword="ref"/> to the stream name string held by <c>StreamNotificationReceiver</c>.
        /// </param>
        /// <param name="exception">
        /// The exception that caused the failure, or <see langword="null"/> if no specific
        /// exception was captured.
        /// </param>
        public StreamFailureNotification(ref string streamName, Exception? exception)
        {
            StreamName = ref streamName;
            Exception = exception;
        }
    }

    /// <summary>
    /// A stack-allocated notification carrying data quality check failure information
    /// delivered to <see cref="ICheckFailureListener"/> implementations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type is a <see langword="ref struct"/> to avoid heap allocation on each check failure
    /// and to allow its <see cref="Tags"/> field to hold a <see cref="ReadOnlySpan{T}"/>, which
    /// itself cannot appear as a field in a non-ref-struct type.
    /// It must not be stored or accessed beyond the duration of the
    /// <see cref="ICheckFailureListener.OnCheckFailure"/> call in which it is received; copy any
    /// required fields to local variables if they must outlive the call.
    /// </para>
    /// <para>
    /// Instances are created internally by <c>StreamNotificationReceiver</c> each time a compiled
    /// SQL <c>CHECK</c> expression calls <see cref="ICheckNotificationReceiver.OnCheckFailure"/>
    /// with a failing message and diagnostic tags.
    /// </para>
    /// </remarks>
    public ref struct CheckFailureNotification
    {
        /// <summary>
        /// A read-only managed reference to the name of the stream in which the check failed.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// The human-readable failure message declared in the SQL <c>CHECK</c> expression.
        /// </summary>
        public readonly string Message;

        /// <summary>
        /// A stack-allocated span of named diagnostic key-value pairs declared as tag arguments
        /// in the SQL <c>CHECK</c> expression.
        /// Must not be stored or accessed after the enclosing
        /// <see cref="ICheckFailureListener.OnCheckFailure"/> call returns.
        /// </summary>
        public readonly ReadOnlySpan<KeyValuePair<string, object?>> Tags;

        /// <summary>
        /// Initializes a new <see cref="CheckFailureNotification"/> with a reference to the
        /// stream name, the failure message, and the associated diagnostic tags.
        /// </summary>
        /// <param name="streamName">
        /// A <see langword="ref"/> to the stream name string held by <c>StreamNotificationReceiver</c>.
        /// </param>
        /// <param name="message">
        /// The human-readable failure message from the SQL <c>CHECK</c> expression.
        /// </param>
        /// <param name="tags">
        /// A stack-allocated span of key-value diagnostic tag pairs from the <c>CHECK</c> expression.
        /// Must not be stored beyond the scope of the constructor call.
        /// </param>
        public CheckFailureNotification(ref string streamName, string message, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            StreamName = ref streamName;
            Message = message;
            Tags = tags;
        }
    }
}
