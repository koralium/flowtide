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
using FlowtideDotNet.Core.ColumnStore.BoundarySearching;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.TestFramework.Internal
{
    internal class PrimaryKeyColumnComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly DataValueContainer dataValueContainer;
        private readonly DataValueContainer _yDataValueContainer;
        private readonly ColumnBoundarySearch _columnBoundarySearch;
        private readonly IReadOnlyList<int> _keyColumns;

        public bool SeekNextPageForValue => false;

        public PrimaryKeyColumnComparer(IReadOnlyList<int> keyColumns)
        {
            dataValueContainer = new DataValueContainer();
            _yDataValueContainer = new DataValueContainer();
            _keyColumns = keyColumns;
            _columnBoundarySearch = new ColumnBoundarySearch(keyColumns, keyColumns);
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                var col = _keyColumns[i];
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
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                var col = _keyColumns[i];
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
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                var column = _keyColumns[i];
                key.referenceBatch.Columns[column].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[column].SearchBoundries(dataValueContainer, start, end, default);

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
            var incomingBatch = keys[0].referenceBatch.Columns;
            _columnBoundarySearch.SearchBoundries(keyContainer._data.Columns, incomingBatch, sortedLookup, lowerBounds, upperBounds, 0, keyContainer.Count - 1, false, lookupBuffer);
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count - 1;
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                var column = _keyColumns[i];
                key.referenceBatch.Columns[column].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[column].SearchBoundries(dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return low;
                }
                index = low;
                start = low;
                end = high;
            }
            return index;
        }
    }
}
