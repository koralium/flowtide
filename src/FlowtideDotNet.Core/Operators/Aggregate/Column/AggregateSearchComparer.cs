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
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class AggregateSearchComparer : IBplusTreeComparer<ColumnRowReference, AggregateKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly int columnCount;

        public int start;
        public int end;
        public bool noMatch = false;

        public bool SeekNextPageForValue => true;

        public AggregateSearchComparer(int columnCount)
        {
            dataValueContainer = new DataValueContainer();
            this.columnCount = columnCount;
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            return x.referenceBatch.CompareRows(y.referenceBatch, x.RowIndex, y.RowIndex);
        }

        public int CompareTo(in ColumnRowReference key, in AggregateKeyStorageContainer keyContainer, in int index)
        {
            return keyContainer._data.CompareRows(key.referenceBatch, index, key.RowIndex);
        }

        public int FindIndex(in ColumnRowReference key, in AggregateKeyStorageContainer keyContainer)
        {
            int index = -1;
            start = 0;
            noMatch = false;
            end = keyContainer.Count - 1;
            for (int i = 0; i < columnCount; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default);

                if (low < 0)
                {
                    start = low;
                    noMatch = true;
                    return low;
                }
                else
                {
                    index = low;
                    start = low;
                    end = high;
                }
            }
            if (columnCount == 0)
            {
                if (keyContainer.Count > 0)
                {
                    return 0;
                }
                else
                {
                    noMatch = true;
                }
            }
            return index;
        }
    }
}
