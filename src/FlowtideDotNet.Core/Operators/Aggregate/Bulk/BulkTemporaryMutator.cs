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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Aggregate.Bulk
{
    internal struct BulkTemporaryMutator : IRowMutator<ColumnRowReference, int>
    {
        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            var idx = indices[0];
            var batch = keys[idx].referenceBatch;
            for (int i = 0; i < batch.Columns.Count; i++)
            {
                batch.Columns[i].GetPrefixSumByteSizes(indices, sizes);
            }
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in int existingData, ref int incomingData, int sortedIndex)
        {
            if (exists)
            {
                if (incomingData == -1)
                {
                    return GenericWriteOperation.Delete;
                }
                return GenericWriteOperation.None;
            }
            if (incomingData == -1)
            {
                return GenericWriteOperation.None;
            }
            return GenericWriteOperation.Upsert;
        }
    }
}
