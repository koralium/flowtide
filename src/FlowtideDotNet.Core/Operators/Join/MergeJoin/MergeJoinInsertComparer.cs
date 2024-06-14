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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinInsertComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private List<int> columnOrder;
        public MergeJoinInsertComparer(List<int> comparisonColumns, int columnCount)
        {
            dataValueContainer = new DataValueContainer();
            columnOrder = new List<int>();
            // Add the comparison columns first
            for (int i = 0; i < comparisonColumns.Count; i++)
            {
                columnOrder.Add(comparisonColumns[i]);
            }
            // Add the missing columns in the order they appear in the data
            for (int i = 0; i < columnCount; i++)
            {
                if (!columnOrder.Contains(i))
                {
                    columnOrder.Add(i);
                }
            }
        }
        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count;
            for (int i = 0; i < columnOrder.Count; i++)
            {
                var column = columnOrder[i];
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[column].GetValueAt(key.RowIndex, dataValueContainer);
                var (low, high) = keyContainer._data.Columns[column].SearchBoundries(dataValueContainer, start, end);

                if (low < 0)
                {
                    return low;
                }
                else
                {
                    index = low;
                    start = low;
                    end = high;
                }
            }
            return index;
        }
    }
}
