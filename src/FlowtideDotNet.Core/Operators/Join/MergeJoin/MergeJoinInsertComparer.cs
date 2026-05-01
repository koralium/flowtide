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

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinInsertComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        public List<int> ColumnOrder { get; }
        public MergeJoinInsertComparer(List<int> comparisonColumns, int columnCount)
        {
            dataValueContainer = new DataValueContainer();
            ColumnOrder = new List<int>();
            // Add the comparison columns first
            for (int i = 0; i < comparisonColumns.Count; i++)
            {
                ColumnOrder.Add(comparisonColumns[i]);
            }

            // Add the missing columns in the order they appear in the data
            for (int i = 0; i < columnCount; i++)
            {
                if (!ColumnOrder.Contains(i))
                {
                    ColumnOrder.Add(i);
                }
            }
        }

        public bool SeekNextPageForValue => false;

        private readonly DataValueContainer _yDataValueContainer = new DataValueContainer();

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < ColumnOrder.Count; i++)
            {
                var col = ColumnOrder[i];
                x.referenceBatch.Columns[col].GetValueAt(x.RowIndex, dataValueContainer, default);
                y.referenceBatch.Columns[col].GetValueAt(y.RowIndex, _yDataValueContainer, default);
                int cmp = FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer.CompareTo(dataValueContainer, _yDataValueContainer);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            for (int i = 0; i < ColumnOrder.Count; i++)
            {
                var col = ColumnOrder[i];
                key.referenceBatch.Columns[col].GetValueAt(key.RowIndex, dataValueContainer, default);
                keyContainer._data.Columns[col].GetValueAt(index, _yDataValueContainer, default);
                int cmp = FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer.CompareTo(dataValueContainer, _yDataValueContainer);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public FindBoundriesResult FindBoundries(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            for (int i = 0; i < ColumnOrder.Count; i++)
            {
                var column = ColumnOrder[i];
                key.referenceBatch.Columns[column].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[column].SearchBoundries(dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                else
                {
                    start = low;
                    end = high;
                }
            }
            return new FindBoundriesResult(start, end);
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count - 1;
            for (int i = 0; i < ColumnOrder.Count; i++)
            {
                var column = ColumnOrder[i];
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[column].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[column].SearchBoundries(dataValueContainer, start, end, default);

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
