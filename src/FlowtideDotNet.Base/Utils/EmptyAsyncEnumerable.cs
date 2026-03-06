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

namespace FlowtideDotNet.Base.Utils
{
    /// <summary>
    /// A zero-allocation async enumerable that always yields zero elements of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The element type declared by this enumerable, though no elements are ever produced.</typeparam>
    /// <remarks>
    /// <para>
    /// Both the enumerable and its inner enumerator are singletons. <see cref="Instance"/> is a shared
    /// static instance of the enumerable, and <see cref="GetAsyncEnumerator"/> always returns the same
    /// static <c>Enumerator</c> instance. This means the type produces no heap allocations at all,
    /// whether accessed via <see cref="Instance"/> or by calling <see cref="GetAsyncEnumerator"/>.
    /// Because the enumerator is shared, this type is <b>not safe for concurrent enumeration</b>.
    /// </para>
    /// <para>
    /// It is used throughout the dataflow vertex pipeline wherever a method returning
    /// <see cref="IAsyncEnumerable{T}"/> must produce no output, for example when a checkpoint or
    /// watermark handler chooses to suppress output without starting an iterator state machine.
    /// For the single-element counterpart see <see cref="SingleAsyncEnumerable{T}"/>.
    /// </para>
    /// </remarks>
    public class EmptyAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        /// <summary>
        /// Gets the shared singleton instance of <see cref="EmptyAsyncEnumerable{T}"/> for the given type argument.
        /// </summary>
        /// <remarks>
        /// Prefer using this field over constructing a new instance to avoid any heap allocation.
        /// </remarks>
        public static readonly EmptyAsyncEnumerable<T> Instance = new EmptyAsyncEnumerable<T>();

        private static readonly IAsyncEnumerator<T> EnumeratorInstance = new Enumerator();

        private sealed class Enumerator : IAsyncEnumerator<T>
        {
#pragma warning disable CS8603 // Possible null reference return.
            public T Current => default(T);
#pragma warning restore CS8603 // Possible null reference return.

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                return ValueTask.FromResult(false);
            }
        }

        /// <summary>
        /// Returns the enumerator for this empty sequence.
        /// </summary>
        /// <remarks>
        /// Always returns the shared static <c>Enumerator</c> instance, producing no heap allocation.
        /// The returned enumerator's <c>MoveNextAsync</c> immediately returns <see langword="false"/>,
        /// and its <c>Current</c> property always yields <see langword="default"/>(<typeparamref name="T"/>).
        /// </remarks>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. Not observed by this implementation.</param>
        /// <returns>The shared static <see cref="IAsyncEnumerator{T}"/> that immediately signals end-of-sequence.</returns>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return EnumeratorInstance;
        }
    }
}
