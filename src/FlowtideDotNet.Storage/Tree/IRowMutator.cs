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

namespace FlowtideDotNet.Storage.Tree
{
    /// <summary>
    /// Decides, per row, how a bulk insert applies an incoming row to a B+ tree. The bulk inserter calls
    /// <see cref="Process"/> for each row to merge the incoming value with any existing value and choose the operation
    /// to perform, and <see cref="GetSizePrefixSum"/> to estimate the serialized size of a run of rows for split planning.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="V">The value type.</typeparam>
    public interface IRowMutator<K, V>
    {
        /// <summary>
        /// Accumulates, into <paramref name="sizes"/>, the running prefix sum of the serialized byte size of the rows at
        /// the given indices. The bulk inserter uses this to decide where a page should be split.
        /// </summary>
        /// <param name="keys">The keys of the batch.</param>
        /// <param name="indices">The indices of the rows to size, in the order they will be inserted.</param>
        /// <param name="sizes">The running per-row byte size totals to add to.</param>
        void GetSizePrefixSum(K[] keys, ReadOnlySpan<int> indices, Span<int> sizes);

        /// <summary>
        /// Processes a single incoming row and returns the operation to apply for it. When the key already exists the
        /// mutator can merge <paramref name="existingData"/> into <paramref name="incomingData"/> before the result is stored.
        /// </summary>
        /// <param name="key">The key of the incoming row.</param>
        /// <param name="exists">True when a value already exists for the key.</param>
        /// <param name="existingData">The current stored value, when <paramref name="exists"/> is true.</param>
        /// <param name="incomingData">The incoming value, which may be modified in place, for example to merge it with the existing value.</param>
        /// <returns>The operation to apply for this row.</returns>
        GenericWriteOperation Process(K key, bool exists, in V existingData, ref V incomingData, int sortedIndex);
    }
}
