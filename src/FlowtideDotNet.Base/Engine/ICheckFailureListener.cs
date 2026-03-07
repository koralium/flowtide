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
    /// Defines a receiver for data quality check failure notifications raised by the stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementations are registered on a <see cref="DataflowStreamBuilder"/> via
    /// <see cref="DataflowStreamBuilder.AddCheckFailureListener"/> and are invoked each time a
    /// SQL-level <c>CHECK</c> assertion evaluated within the stream produces a failing result.
    /// Multiple listeners can be registered; each is called in registration order.
    /// </para>
    /// <para>
    /// The notification is passed as a <c>ref readonly</c> <see cref="CheckFailureNotification"/>,
    /// which is a <see langword="ref struct"/>. This avoids a heap allocation and allows the
    /// notification to carry a <see cref="System.ReadOnlySpan{T}"/> of diagnostic tags.
    /// Implementations must therefore not store the notification beyond the duration of
    /// <see cref="OnCheckFailure"/> — copy any required fields to local variables instead.
    /// </para>
    /// <para>
    /// Exceptions thrown by an implementation are swallowed by the stream engine to ensure
    /// that a misbehaving listener cannot disrupt stream processing.
    /// The built-in implementations are <c>LoggerCheckFailureListener</c>, which forwards the
    /// failure message and tags to <see cref="Microsoft.Extensions.Logging.ILogger"/>, and
    /// <c>ActivityCheckFailureListener</c>, which emits an OpenTelemetry
    /// <see cref="System.Diagnostics.Activity"/> under the source <c>FlowtideDotNet.CheckFailures</c>.
    /// </para>
    /// </remarks>
    public interface ICheckFailureListener
    {
        /// <summary>
        /// Called when a data quality check within the stream produces a failing result.
        /// </summary>
        /// <remarks>
        /// The <paramref name="notification"/> is a <c>ref readonly</c> <see langword="ref struct"/>;
        /// it must not be stored or captured beyond the scope of this method call.
        /// Access <see cref="CheckFailureNotification.StreamName"/> to identify the originating stream,
        /// <see cref="CheckFailureNotification.Message"/> for the human-readable failure description,
        /// and <see cref="CheckFailureNotification.Tags"/> for the associated diagnostic key-value metadata.
        /// </remarks>
        /// <param name="notification">
        /// A <c>ref readonly</c> <see cref="CheckFailureNotification"/> carrying the stream name,
        /// failure message, and a <see cref="System.ReadOnlySpan{T}"/> of diagnostic tags
        /// that describe the context in which the check failed.
        /// </param>
        void OnCheckFailure(ref readonly CheckFailureNotification notification);
    }
}
