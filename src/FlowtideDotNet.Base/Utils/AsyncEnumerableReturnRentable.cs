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
    internal class AsyncEnumerableReturnRentable<TSource, TDest> : IAsyncEnumerable<TDest>
    {
        private readonly IRentable rentable;
        private readonly IAsyncEnumerable<TSource> source;
        private readonly Func<TSource, TDest> func;

        public AsyncEnumerableReturnRentable(IRentable rentable, IAsyncEnumerable<TSource> source, Func<TSource, TDest> func)
        {
            this.rentable = rentable;
            this.source = source;
            this.func = func;
        }

        internal class Enumerator : IAsyncEnumerator<TDest>
        {
            private readonly IRentable rentable;
            private readonly IAsyncEnumerator<TSource> enumerator;
            private readonly Func<TSource, TDest> func;

            public Enumerator(IRentable rentable, IAsyncEnumerator<TSource> enumerator, Func<TSource, TDest> func)
            {
                this.rentable = rentable;
                this.enumerator = enumerator;
                this.func = func;
            }

            public TDest Current => func(enumerator.Current);

            public ValueTask DisposeAsync()
            {

                return enumerator.DisposeAsync();
            }

            public ValueTask<bool> MoveNextAsync()
            {
                var valuetask = enumerator.MoveNextAsync();
                if (valuetask.IsCompletedSuccessfully)
                {
                    if (!valuetask.Result)
                    {
                        rentable.Return();
                        return ValueTask.FromResult(false);
                    }
                    return ValueTask.FromResult(true);
                }
                return MoveNextAsync_Slow(valuetask);
            }

            private async ValueTask<bool> MoveNextAsync_Slow(ValueTask<bool> task)
            {
                var result = await task;
                if (!result)
                {
                    rentable.Return();
                }
                return result;
            }
        }

        public IAsyncEnumerator<TDest> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new Enumerator(rentable, source.GetAsyncEnumerator(cancellationToken), func);
        }
    }
}
