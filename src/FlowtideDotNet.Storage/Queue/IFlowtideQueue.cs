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

using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Queue
{
    public interface IFlowtideQueue<V, TValueContainer>
        where TValueContainer : IValueContainer<V>
    {
        ValueTask Enqueue(in V value);

        ValueTask<V> Dequeue();

        ValueTask<V> Pop();

        /// <summary>
        /// Peeks the latest enqueued value, returns an action which if set disposes the value after its been used.
        /// This is required if the value is in another page.
        /// </summary>
        /// <returns></returns>
        ValueTask<(V value, Action? returnFunc)> PeekPop();

        /// <summary>
        /// Peeks the first value in the queue, returns an action which if set disposes the value after its been used.
        /// This is required if the value is in another page.
        /// </summary>
        /// <returns></returns>
        ValueTask<(V value, Action? returnFunc)> Peek();

        ValueTask Commit();

        ValueTask Clear();

        long Count { get; }
    }
}
