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
    /// A single-allocation async enumerable that yields exactly one element of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The type of the single element produced by this enumerable.</typeparam>
    /// <remarks>
    /// <para>
    /// This class implements both <see cref="IAsyncEnumerable{T}"/> and <see cref="IAsyncEnumerator{T}"/>
    /// on the same instance to avoid a secondary heap allocation for the enumerator. As a result,
    /// <see cref="GetAsyncEnumerator"/> returns <c>this</c> and the class is <b>not safe for concurrent enumeration</b>
    /// and must <b>not be enumerated more than once</b>.
    /// </para>
    /// <para>
    /// It is used throughout the dataflow vertex pipeline wherever a method returning
    /// <see cref="IAsyncEnumerable{T}"/> must produce exactly one output item, for example when
    /// a checkpoint handler or a single-result <c>OnRecieve</c> override needs to emit a single
    /// <see cref="IStreamEvent"/> without the overhead of a full iterator state machine.
    /// For the zero-element counterpart see <see cref="EmptyAsyncEnumerable{T}"/>.
    /// </para>
    /// </remarks>
    public class SingleAsyncEnumerable<T> : IAsyncEnumerable<T>, IAsyncEnumerator<T>
    {
        private readonly T value;
        private bool hasMovedNext;

        /// <summary>
        /// Initializes a new instance of <see cref="SingleAsyncEnumerable{T}"/> that will yield <paramref name="value"/> exactly once.
        /// </summary>
        /// <param name="value">The single element to be produced during enumeration.</param>
        public SingleAsyncEnumerable(T value)
        {
            hasMovedNext = false;
            this.value = value;
        }

        /// <summary>
        /// Gets the current element in the enumeration.
        /// </summary>
        /// <remarks>
        /// This property always returns the value supplied at construction time, regardless of the current
        /// iteration state. It should only be accessed after a successful call to <see cref="MoveNextAsync"/>.
        /// </remarks>
        public T Current => value;

        /// <summary>
        /// Performs any cleanup required when the enumerator is no longer needed.
        /// This implementation is a no-op and completes synchronously.
        /// </summary>
        /// <returns>A completed <see cref="ValueTask"/>.</returns>
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Returns the enumerator for this sequence.
        /// </summary>
        /// <remarks>
        /// Because this class implements <see cref="IAsyncEnumerator{T}"/> directly, this method returns
        /// <c>this</c> instance. Calling this method more than once or enumerating concurrently produces
        /// undefined behaviour.
        /// </remarks>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. Not observed by this implementation.</param>
        /// <returns>This instance as <see cref="IAsyncEnumerator{T}"/>.</returns>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return this;
        }

        /// <summary>
        /// Advances the enumerator to the single element of the sequence.
        /// </summary>
        /// <remarks>
        /// Returns <see langword="true"/> on the first call, making <see cref="Current"/> available.
        /// Returns <see langword="false"/> on every subsequent call, signalling that enumeration is complete.
        /// </remarks>
        /// <returns>
        /// A <see cref="ValueTask{TResult}"/> that resolves to <see langword="true"/> the first time it is called,
        /// and <see langword="false"/> on all subsequent calls.
        /// </returns>
        public ValueTask<bool> MoveNextAsync()
        {
            if (hasMovedNext)
            {
                return ValueTask.FromResult(false);
            }
            hasMovedNext = true;
            return ValueTask.FromResult(true);
        }
    }
}
