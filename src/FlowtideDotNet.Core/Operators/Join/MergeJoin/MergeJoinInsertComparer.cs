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
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinInsertComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private List<KeyValuePair<int, ReferenceSegment?>> columnOrder;
        public MergeJoinInsertComparer(List<KeyValuePair<int, ReferenceSegment?>> comparisonColumns, int columnCount)
        {
            dataValueContainer = new DataValueContainer();
            columnOrder = new List<KeyValuePair<int, ReferenceSegment?>>();
            // Add the comparison columns first
            for (int i = 0; i < comparisonColumns.Count; i++)
            {
                columnOrder.Add(comparisonColumns[i]);
            }

            // Add the missing columns in the order they appear in the data
            for (int i = 0; i < columnCount; i++)
            {
                var exists = columnOrder.Exists(x => x.Key == i);
                var existingColumn = columnOrder.Find(x => x.Key == i);
                if (!exists || existingColumn.Value != null)
                {
                    columnOrder.Add(new KeyValuePair<int, ReferenceSegment?>(i, default));
                }
            }
        }

        public bool SeekNextPageForValue => false;

        private readonly DataValueContainer _yDataValueContainer = new DataValueContainer();

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < columnOrder.Count; i++)
            {
                var col = columnOrder[i];
                x.referenceBatch.Columns[col.Key].GetValueAt(x.RowIndex, dataValueContainer, col.Value);
                y.referenceBatch.Columns[col.Key].GetValueAt(y.RowIndex, _yDataValueContainer, col.Value);
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
            for (int i = 0; i < columnOrder.Count; i++)
            {
                var col = columnOrder[i];
                key.referenceBatch.Columns[col.Key].GetValueAt(key.RowIndex, dataValueContainer, col.Value);
                keyContainer._data.Columns[col.Key].GetValueAt(index, _yDataValueContainer, col.Value);
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
            for (int i = 0; i < columnOrder.Count; i++)
            {
                var column = columnOrder[i];
                key.referenceBatch.Columns[column.Key].GetValueAt(key.RowIndex, dataValueContainer, column.Value);
                var (low, high) = keyContainer._data.Columns[column.Key].SearchBoundries(dataValueContainer, start, end, column.Value);

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
            for (int i = 0; i < columnOrder.Count; i++)
            {
                var column = columnOrder[i];
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[column.Key].GetValueAt(key.RowIndex, dataValueContainer, column.Value);
                var (low, high) = keyContainer._data.Columns[column.Key].SearchBoundries(dataValueContainer, start, end, column.Value);

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
