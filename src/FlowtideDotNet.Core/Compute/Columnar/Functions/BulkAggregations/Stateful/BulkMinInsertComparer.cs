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
using System;
using System.Buffers;
using System.Collections.Generic;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    public class BulkMinInsertComparer : IBplusTreeComparer<BulkGroupValueRowReference, BulkGroupValueKeyContainer>
    {
        private DataValueContainer dataValueContainer;
        private DataValueContainer dataValueContainer2;
        private readonly int _groupingKeyLength;
        private readonly ColumnBoundarySearch _columnBoundarySearch;

        public BulkMinInsertComparer(int groupingKeyLength)
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

        public int CompareTo(in BulkGroupValueRowReference x, in BulkGroupValueRowReference y)
        {
            for (int i = 0; i <= _groupingKeyLength; i++)
            {
                x.batch.Columns[i].GetValueAt(x.index, dataValueContainer, default);
                y.batch.Columns[i].GetValueAt(y.index, dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(dataValueContainer, dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public int CompareTo(in BulkGroupValueRowReference key, in BulkGroupValueKeyContainer keyContainer, in int index)
        {
            for (int i = 0; i <= _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, dataValueContainer, default);
                keyContainer._data.Columns[i].GetValueAt(index, dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(dataValueContainer, dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public int FindIndex(in BulkGroupValueRowReference key, in BulkGroupValueKeyContainer keyContainer)
        {
            int start = 0;
            int end = keyContainer.Count - 1;
            for (int i = 0; i <= _groupingKeyLength; i++)
            {
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
            return start;
        }

        public FindBoundriesResult FindBoundries(in BulkGroupValueRowReference key, in BulkGroupValueKeyContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            for (int i = 0; i <= _groupingKeyLength; i++)
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
            return new FindBoundriesResult(start, end);
        }

        public void FindBoundriesBulk(
            ReadOnlySpan<BulkGroupValueRowReference> keys,
            ReadOnlySpan<int> sortedLookup,
            in BulkGroupValueKeyContainer keyContainer,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            Span<int> lookupBuffer)
        {
            if (sortedLookup.Length == 0)
            {
                return;
            }
            var firstIndex = sortedLookup[0];
            var incomingBatch = keys[firstIndex].batch.Columns;

            int[]? rented = null;
            Span<int> mappedIndices = sortedLookup.Length <= 256 ? 
                stackalloc int[sortedLookup.Length] : 
                (rented = ArrayPool<int>.Shared.Rent(sortedLookup.Length));

            try
            {
                for (int i = 0; i < sortedLookup.Length; i++)
                {
                    mappedIndices[i] = keys[sortedLookup[i]].index;
                }

                _columnBoundarySearch.SearchBoundries(
                    keyContainer._data.Columns,
                    incomingBatch,
                    mappedIndices.Slice(0, sortedLookup.Length),
                    lowerBounds,
                    upperBounds,
                    0,
                    keyContainer.Count - 1,
                    false,
                    lookupBuffer);
            }
            finally
            {
                if (rented != null)
                {
                    ArrayPool<int>.Shared.Return(rented);
                }
            }
        }
    }
}
