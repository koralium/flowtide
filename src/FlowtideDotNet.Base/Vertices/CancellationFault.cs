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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Storage.Exceptions;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Base.Vertices
{
    // Fault on cancellations the stream did not ask for
    internal static class CancellationFault
    {
        public static Func<TInput, Task> Wrap<TInput>(IStreamVertex vertex, CancellationToken cancellationToken, Func<TInput, Task> func)
        {
            return (input) =>
            {
                Task task;
                try
                {
                    task = func(input);
                }
                catch (OperationCanceledException e) when (IsStray(e, cancellationToken))
                {
                    throw Create(vertex, e);
                }
                if (task.IsCompletedSuccessfully)
                {
                    return task;
                }
                return Wait_Slow(vertex, cancellationToken, task);
            };
        }

        public static Func<TInput, IAsyncEnumerable<TOutput>> Wrap<TInput, TOutput>(IStreamVertex vertex, CancellationToken cancellationToken, Func<TInput, IAsyncEnumerable<TOutput>> func)
        {
            return (input) =>
            {
                IAsyncEnumerable<TOutput> output;
                try
                {
                    output = func(input);
                }
                catch (OperationCanceledException e) when (IsStray(e, cancellationToken))
                {
                    throw Create(vertex, e);
                }
                // The shared empty result never throws
                if (ReferenceEquals(output, EmptyAsyncEnumerable<TOutput>.Instance))
                {
                    return output;
                }
                return new FaultingEnumerable<TOutput>(vertex, cancellationToken, output);
            };
        }

        // A commit given up by a stop is the engine's cancel
        private static bool IsStray(OperationCanceledException exception, CancellationToken cancellationToken)
        {
            return !cancellationToken.IsCancellationRequested && exception is not FlowtideCommitStoppedException;
        }

        private static async Task Wait_Slow(IStreamVertex vertex, CancellationToken cancellationToken, Task task)
        {
            try
            {
                await task;
            }
            catch (OperationCanceledException e) when (IsStray(e, cancellationToken))
            {
                throw Create(vertex, e);
            }
        }

        private static InvalidOperationException Create(IStreamVertex vertex, OperationCanceledException exception)
        {
            return new InvalidOperationException($"Operator {vertex.Name} ({vertex.DisplayName}) was cancelled, but the stream did not request it", exception);
        }

        private sealed class FaultingEnumerable<T> : IAsyncEnumerable<T>, IAsyncEnumerator<T>
        {
            private readonly IStreamVertex _vertex;
            private readonly CancellationToken _cancellationToken;
            private readonly IAsyncEnumerable<T> _source;
            private IAsyncEnumerator<T>? _enumerator;

            public FaultingEnumerable(IStreamVertex vertex, CancellationToken cancellationToken, IAsyncEnumerable<T> source)
            {
                _vertex = vertex;
                _cancellationToken = cancellationToken;
                _source = source;
            }

            public T Current
            {
                get
                {
                    Debug.Assert(_enumerator != null);
                    try
                    {
                        return _enumerator.Current;
                    }
                    catch (OperationCanceledException e) when (IsStray(e, _cancellationToken))
                    {
                        throw Create(_vertex, e);
                    }
                }
            }

            public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            {
                try
                {
                    _enumerator = _source.GetAsyncEnumerator(cancellationToken);
                }
                catch (OperationCanceledException e) when (IsStray(e, _cancellationToken))
                {
                    throw Create(_vertex, e);
                }
                return this;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                Debug.Assert(_enumerator != null);
                ValueTask<bool> valueTask;
                try
                {
                    valueTask = _enumerator.MoveNextAsync();
                }
                catch (OperationCanceledException e) when (IsStray(e, _cancellationToken))
                {
                    throw Create(_vertex, e);
                }
                if (valueTask.IsCompletedSuccessfully)
                {
                    return valueTask;
                }
                return MoveNextAsync_Slow(valueTask);
            }

            [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
            private async ValueTask<bool> MoveNextAsync_Slow(ValueTask<bool> valueTask)
            {
                try
                {
                    return await valueTask;
                }
                catch (OperationCanceledException e) when (IsStray(e, _cancellationToken))
                {
                    throw Create(_vertex, e);
                }
            }

            public ValueTask DisposeAsync()
            {
                if (_enumerator == null)
                {
                    return ValueTask.CompletedTask;
                }
                ValueTask valueTask;
                try
                {
                    valueTask = _enumerator.DisposeAsync();
                }
                catch (OperationCanceledException e) when (IsStray(e, _cancellationToken))
                {
                    throw Create(_vertex, e);
                }
                if (valueTask.IsCompletedSuccessfully)
                {
                    return valueTask;
                }
                return DisposeAsync_Slow(valueTask);
            }

            [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
            private async ValueTask DisposeAsync_Slow(ValueTask valueTask)
            {
                try
                {
                    await valueTask;
                }
                catch (OperationCanceledException e) when (IsStray(e, _cancellationToken))
                {
                    throw Create(_vertex, e);
                }
            }
        }
    }
}
