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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class PartitionIterator : IAsyncEnumerable<KeyValuePair<ColumnRowReference, WindowStateReference>>
    {
        private readonly ColumnRowReference partitionRow;
        private readonly IBPlusTreeIterator<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer> iterator;
        private readonly WindowPartitionStartSearchComparer searchComparer;

        public PartitionIterator(
            ColumnRowReference partitionRow,
            IBPlusTreeIterator<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer> iterator,
            WindowPartitionStartSearchComparer searchComparer)
        {
            this.partitionRow = partitionRow;
            this.iterator = iterator;
            this.searchComparer = searchComparer;
        }

        public IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return GetRows().GetAsyncEnumerator();
        }

        public async IAsyncEnumerable<KeyValuePair<ColumnRowReference, WindowStateReference>> GetRows()
        {
            bool firstPage = true;
            await foreach(var page in iterator)
            {
                if (!firstPage)
                {
                    // Locate indices again
                    var index = searchComparer.FindIndex(in partitionRow, page.Keys!);
                    if (searchComparer.noMatch)
                    {
                        break;
                    }
                }
                firstPage = false;
                for (int k = searchComparer.start; k <= searchComparer.end; k++)
                {
                    var pageVal = page.Values.Get(k);
                    for (int w = 0; w < pageVal.weight; w++)
                    {
                        yield return new KeyValuePair<ColumnRowReference, WindowStateReference>(new ColumnRowReference()
                        {
                            referenceBatch = page.Keys.Data,
                            RowIndex = k
                        }, new WindowStateReference()
                        {
                            referenceBatch = pageVal.referenceBatch,
                            rowIndex = pageVal.RowIndex,
                            weightIndex = w
                        });
                    }
                    
                }
            }
        }
    }
}
