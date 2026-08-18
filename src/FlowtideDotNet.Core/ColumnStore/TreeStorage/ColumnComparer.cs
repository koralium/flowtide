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

using FlowtideDotNet.Core.ColumnStore.BoundarySearching;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    public class ColumnComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly int columnCount;
        private readonly ColumnBoundarySearch _columnBoundarySearch;

        public bool SeekNextPageForValue => false;

        public ColumnComparer(int columnCount)
        {
            dataValueContainer = new DataValueContainer();
            this.columnCount = columnCount;
            var columnOrder = new int[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                columnOrder[i] = i;
            }
            _columnBoundarySearch = new ColumnBoundarySearch(columnOrder, columnOrder);
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            return x.referenceBatch.CompareRows(y.referenceBatch, x.RowIndex, y.RowIndex);
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            return keyContainer._data.CompareRows(key.referenceBatch, index, key.RowIndex);
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count - 1;
            if (columnCount == 0)
            {
                if (keyContainer.Count == 0)
                {
                    return -1;
                }
                return 0;
            }
            for (int i = 0; i < columnCount; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default);

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

        public FindBoundriesResult FindBoundries(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;

            if (columnCount == 0)
            {
                // No columns, so the whole range matches
                if (start > end)
                {
                    return new FindBoundriesResult(~start, ~start);
                }
                return new FindBoundriesResult(start, end);
            }

            for (int i = 0; i < columnCount; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                start = low;
                end = high;
            }
            return new FindBoundriesResult(start, end);
        }

        void IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>.FindBoundriesBulk(
            ReadOnlySpan<ColumnRowReference> keys,
            ReadOnlySpan<int> sortedLookup,
            in ColumnKeyStorageContainer keyContainer,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            Span<int> lookupBuffer)
        {
            if (columnCount == 0)
            {
                // No columns, all keys match the first row
                var bounds = keyContainer.Count > 0 ? 0 : ~0;
                lowerBounds.Fill(bounds);
                upperBounds.Fill(keyContainer.Count > 0 ? keyContainer.Count - 1 : ~0);
                return;
            }
            // All keys come from the same batch
            var incomingColumns = keys[0].referenceBatch.Columns;
            _columnBoundarySearch.SearchBoundries(keyContainer._data.Columns, incomingColumns, sortedLookup, lowerBounds, upperBounds, 0, keyContainer.Count - 1, false, lookupBuffer);
        }
    }
}
