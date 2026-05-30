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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Storage.Tree;
using System.Collections.Generic;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    internal class MinInsertComparer : IBplusTreeComparer<ListAggColumnRowReference, ListAggKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private DataValueContainer dataValueContainer2;
        private readonly int _groupingKeyLength;
        private readonly ColumnBoundarySearch _columnBoundarySearch;

        public MinInsertComparer(int groupingKeyLength)
        {
            _groupingKeyLength = groupingKeyLength;
            dataValueContainer = new DataValueContainer();
            dataValueContainer2 = new DataValueContainer();
            var columnOrder = new List<int>();
            for (int i = 0; i <= groupingKeyLength; i++)
            {
                columnOrder.Add(i);
            }
            _columnBoundarySearch = new ColumnBoundarySearch(columnOrder, columnOrder);
        }

        public bool SeekNextPageForValue => false;

        public int CompareTo(in ListAggColumnRowReference x, in ListAggColumnRowReference y)
        {
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                x.batch.Columns[i].GetValueAt(x.index, dataValueContainer, default);
                y.batch.Columns[i].GetValueAt(y.index, dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(dataValueContainer, dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            if (x.insertValue == null && y.insertValue == null)
            {
                return 0;
            }
            if (x.insertValue == null)
            {
                return -1;
            }
            if (y.insertValue == null)
            {
                return 1;
            }
            return DataValueComparer.CompareTo(x.insertValue, y.insertValue);
        }

        public int CompareTo(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer, in int index)
        {
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, dataValueContainer, default);
                keyContainer._data.Columns[i].GetValueAt(index, dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(dataValueContainer, dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            keyContainer._data.Columns[_groupingKeyLength].GetValueAt(index, dataValueContainer2, default);
            return DataValueComparer.CompareTo(key.insertValue, dataValueContainer2);
        }

        public int FindIndex(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count - 1;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                // Get value by container to skip boxing for each value
                key.batch.Columns[i].GetValueAt(key.index, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return low;
                }
                else
                {
                    start = low;
                    end = high;
                }
            }

            (index, _) = keyContainer._data.Columns[_groupingKeyLength].SearchBoundries(key.insertValue, start, end, default);

            return index;
        }

        public FindBoundriesResult FindBoundries(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default);

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

            var (index, endIndexResult) = keyContainer._data.Columns[_groupingKeyLength].SearchBoundries(key.insertValue, start, end, default);
            if (index < 0)
            {
                return new FindBoundriesResult(index, index);
            }
            return new FindBoundriesResult(index, endIndexResult);
        }

        public void FindBoundriesBulk(
            ReadOnlySpan<ListAggColumnRowReference> keys,
            ReadOnlySpan<int> sortedLookup,
            in ListAggKeyStorageContainer keyContainer,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            Span<int> lookupBuffer)
        {
            var firstIndex = sortedLookup[0];
            var incomingBatch = keys[firstIndex].batch.Columns;

            Span<int> mappedIndices = stackalloc int[sortedLookup.Length];
            for (int i = 0; i < sortedLookup.Length; i++)
            {
                mappedIndices[i] = keys[sortedLookup[i]].index;
            }

            _columnBoundarySearch.SearchBoundries(
                keyContainer._data.Columns,
                incomingBatch,
                mappedIndices,
                lowerBounds,
                upperBounds,
                0,
                keyContainer.Count - 1,
                false,
                lookupBuffer);
        }
    }
}
