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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class PartitionIterator : IAsyncEnumerable<KeyValuePair<ColumnRowReference, WindowStateReference>>
    {
        private ColumnRowReference partitionRow;
        private readonly IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer> iterator;
        private readonly WindowPartitionStartSearchComparer searchComparer;
        private readonly WindowStateReference _windowStateReference;
        private readonly IWindowAddOutputRow? _addOutputRow;

        public PartitionIterator(IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer> iterator, List<int> partitionColumns, IWindowAddOutputRow? addOutputRow = default)
        {
            this.iterator = iterator;
            _addOutputRow = addOutputRow;
            _windowStateReference = new WindowStateReference(addOutputRow);
            searchComparer = new WindowPartitionStartSearchComparer(partitionColumns);
        }

        /// <summary>
        /// Reset with new partition values
        /// </summary>
        /// <param name="partitionValue"></param>
        /// <returns></returns>
        public ValueTask Reset(ColumnRowReference partitionValue)
        {
            this.partitionRow = partitionValue;
            return iterator.Seek(partitionRow, searchComparer);
        }

        /// <summary>
        /// Reset with new partition values copied from another partition iterator
        /// </summary>
        /// <param name="other"></param>
        public void ResetCopyFrom(PartitionIterator other)
        {
            this.partitionRow = other.partitionRow;
            this.searchComparer.start = other.searchComparer!.start;
            this.searchComparer.end = other.searchComparer.end;
            this.searchComparer.noMatch = other.searchComparer.noMatch;
            other.iterator.CloneSeekResultTo(iterator);
        }

        public IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return GetRows().GetAsyncEnumerator();
        }

        public async IAsyncEnumerable<KeyValuePair<ColumnRowReference, WindowStateReference>> GetRows()
        {
            Debug.Assert(iterator != null, "Run reset first before getting rows");
            Debug.Assert(searchComparer != null, "Run reset first before getting rows");

            bool firstPage = true;
            await foreach(var page in iterator)
            {
                _windowStateReference.ResetPage();
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
                    var oldOutputCount = pageVal.valueContainer._functionStates[0].GetListLength(k);

                    var columnRowReference = new ColumnRowReference()
                    {
                        referenceBatch = page.Keys.Data,
                        RowIndex = k
                    };

                    for (int w = 0; w < pageVal.weight; w++)
                    {
                        _windowStateReference.ResetRow(page.Keys.Get(k), w, pageVal);
                        yield return new KeyValuePair<ColumnRowReference, WindowStateReference>(columnRowReference, _windowStateReference);
                    }
                    // Check if there has been more output before than the current weight
                    // If that is the case, those weights need to be outputted as negative
                    if (pageVal.weight < oldOutputCount && _addOutputRow != null)
                    {
                        for (int w = oldOutputCount - 1; w >= pageVal.weight; w--)
                        {
                            var oldValue = pageVal.valueContainer._functionStates[0].GetListElementValue(k, w);
                            _addOutputRow.AddOutputRow(columnRowReference, oldValue, -1);
                        }
                        _windowStateReference.Updated = true;
                    }
                    if (_windowStateReference.Updated)
                    {
                        // Save page if it was updated
                        await page.SavePage(false);
                    }
                }
            }
        }
    }
}
