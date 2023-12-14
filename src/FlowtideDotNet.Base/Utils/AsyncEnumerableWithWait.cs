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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base.Utils
{
    internal class AsyncEnumerableWithWait<TSource, TDest> : IAsyncEnumerable<TDest>
    {
        private readonly IAsyncEnumerable<TSource> source;
        private readonly Func<TSource, TDest> func;
        private readonly Func<bool> shouldWaitFunc;

        public AsyncEnumerableWithWait(IAsyncEnumerable<TSource> source, Func<TSource, TDest> func, Func<bool> shouldWaitFunc)
        {
            this.source = source;
            this.func = func;
            this.shouldWaitFunc = shouldWaitFunc;
        }

        internal class Enumerator : IAsyncEnumerator<TDest>
        {
            private readonly IAsyncEnumerator<TSource> enumerator;
            private readonly Func<TSource, TDest> func;
            private readonly Func<bool> shouldWaitFunc;

            public Enumerator(IAsyncEnumerator<TSource> enumerator, Func<TSource, TDest> func, Func<bool> shouldWaitFunc)
            {
                this.enumerator = enumerator;
                this.func = func;
                this.shouldWaitFunc = shouldWaitFunc;
            }

            public TDest Current => func(enumerator.Current);

            public ValueTask DisposeAsync()
            {

                return enumerator.DisposeAsync();
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (shouldWaitFunc())
                {
                    return MoveNextSlow();
                }
                
                return enumerator.MoveNextAsync();
            }

            private async ValueTask<bool> MoveNextSlow()
            {
                while (shouldWaitFunc())
                {
                    await Task.Delay(10);
                }
                return await enumerator.MoveNextAsync();
            }
        }

        public IAsyncEnumerator<TDest> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new Enumerator(source.GetAsyncEnumerator(cancellationToken), func, shouldWaitFunc);
        }
    }
}
