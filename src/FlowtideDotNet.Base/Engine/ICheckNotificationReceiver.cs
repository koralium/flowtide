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
    /// Defines the internal compute-layer sink for data quality check failure events raised
    /// directly from compiled SQL <c>CHECK</c> expressions.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This interface is the low-level counterpart to <see cref="ICheckFailureListener"/>.
    /// Where <see cref="ICheckFailureListener"/> is the user-facing observer registered on
    /// <see cref="DataflowStreamBuilder"/>, <see cref="ICheckNotificationReceiver"/> is the
    /// compute-layer target whose reference is baked into the compiled expression tree of every
    /// SQL <c>CHECK</c> function at stream build time. When a <c>CHECK</c> condition evaluates
    /// to <see langword="false"/> for a row, the generated expression calls
    /// <see cref="OnCheckFailure"/> directly via a constant reference captured in the expression,
    /// making this a hot-path call that may occur for every failing row during stream processing.
    /// </para>
    /// <para>
    /// The sole built-in implementation is <c>StreamNotificationReceiver</c>, which bridges this
    /// compute-layer interface to the set of registered <see cref="ICheckFailureListener"/> instances
    /// by wrapping the arguments in a stack-allocated <see cref="CheckFailureNotification"/>
    /// <see langword="ref struct"/> and dispatching it to each listener in turn.
    /// </para>
    /// <para>
    /// The receiver is wired up during stream initialization through
    /// <c>FunctionsRegister.SetCheckNotificationReceiver</c>, which stores it in
    /// <c>FunctionServices</c> so that the expression compiler can retrieve it when building
    /// <c>CHECK</c> function expressions.
    /// </para>
    /// </remarks>
    public interface ICheckNotificationReceiver
    {
        /// <summary>
        /// Called from within a compiled SQL <c>CHECK</c> expression when its condition
        /// evaluates to <see langword="false"/> for a row being processed.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is invoked on the hot data path and may be called for every row in a
        /// stream batch where the check fails. Implementations should be allocation-free and
        /// return as quickly as possible to avoid stalling stream throughput.
        /// </para>
        /// <para>
        /// The <paramref name="message"/> parameter uses the <see langword="in"/> modifier so
        /// the string reference is passed by read-only reference without copying the pointer.
        /// The <paramref name="tags"/> span is stack-allocated by the compiled expression and
        /// must not be stored or accessed after this method returns.
        /// </para>
        /// </remarks>
        /// <param name="message">
        /// The human-readable failure message declared in the SQL <c>CHECK</c> expression,
        /// passed by read-only reference to avoid an additional pointer copy on the call stack.
        /// </param>
        /// <param name="tags">
        /// A stack-allocated <see cref="ReadOnlySpan{T}"/> of <see cref="KeyValuePair{TKey,TValue}"/>
        /// entries providing named diagnostic context values declared as tag arguments in the
        /// SQL <c>CHECK</c> expression. Must not be stored or accessed after this method returns.
        /// </param>
        void OnCheckFailure(in string message, ReadOnlySpan<KeyValuePair<string, object?>> tags);
    }
}
