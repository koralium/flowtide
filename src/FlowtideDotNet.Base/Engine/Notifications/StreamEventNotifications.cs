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
    /// A check issue delivered to <see cref="ICheckFailureListener"/>, valid only during the call.
    /// </summary>
    public ref struct CheckFailureNotification
    {
        /// <summary>
        /// The name of the stream that runs the check.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// Identifies the check within the stream.
        /// </summary>
        public readonly string CheckId;

        /// <summary>
        /// The check's message template, its {tag} placeholders are not rendered.
        /// </summary>
        public readonly string CheckName;

        /// <summary>
        /// The issue tags, must not be kept after the call returns.
        /// </summary>
        public readonly ReadOnlySpan<KeyValuePair<string, object?>> Tags;

        /// <summary>
        /// Creates a notification for one issue of a check.
        /// </summary>
        public CheckFailureNotification(ref string streamName, string checkId, string checkName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            StreamName = ref streamName;
            CheckId = checkId;
            CheckName = checkName;
            Tags = tags;
        }
    }

    /// <summary>
    /// Tells <see cref="ICheckFailureListener"/> to forget every issue of a check.
    /// </summary>
    public ref struct CheckResetNotification
    {
        /// <summary>
        /// The name of the stream that runs the check.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// Identifies the check within the stream.
        /// </summary>
        public readonly string CheckId;

        /// <summary>
        /// The check's message template.
        /// </summary>
        public readonly string CheckName;

        /// <summary>
        /// Creates a reset notification for a check.
        /// </summary>
        public CheckResetNotification(ref string streamName, string checkId, string checkName)
        {
            StreamName = ref streamName;
            CheckId = checkId;
            CheckName = checkName;
        }
    }

    /// <summary>
    /// The status of a check delivered to <see cref="ICheckStatusListener"/>, valid only during the call.
    /// </summary>
    public ref struct CheckStatusNotification
    {
        /// <summary>
        /// The name of the stream that runs the check.
        /// </summary>
        public readonly ref string StreamName;

        /// <summary>
        /// Identifies the check within the stream.
        /// </summary>
        public readonly string CheckId;

        /// <summary>
        /// The check's message template.
        /// </summary>
        public readonly string CheckName;

        /// <summary>
        /// The number of distinct active issues.
        /// </summary>
        public readonly long ActiveIssues;

        /// <summary>
        /// The number of rows that fail the check.
        /// </summary>
        public readonly long FailingRows;

        /// <summary>
        /// True when the check has no active issue.
        /// </summary>
        public readonly bool Passed => ActiveIssues == 0;

        /// <summary>
        /// Creates a status notification for a check.
        /// </summary>
        public CheckStatusNotification(ref string streamName, string checkId, string checkName, long activeIssues, long failingRows)
        {
            StreamName = ref streamName;
            CheckId = checkId;
            CheckName = checkName;
            ActiveIssues = activeIssues;
            FailingRows = failingRows;
        }
    }
}
