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
using System.Collections.Generic;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    public class BulkMinSearchComparer : IBplusTreeComparer<BulkGroupValueRowReference, BulkGroupValueKeyContainer>
    {
        private readonly int _groupingKeyLength;
        private readonly DataValueContainer _dataValueContainer;
        private readonly DataValueContainer _dataValueContainer2;
        private readonly ColumnBoundarySearch _columnBoundarySearch;
        private readonly bool _seekNextPageForValue;

        public BulkMinSearchComparer(int groupingKeyLength, bool seekNextPageForValue = false)
        {
            _groupingKeyLength = groupingKeyLength;
            _seekNextPageForValue = seekNextPageForValue;
            _dataValueContainer = new DataValueContainer();
            _dataValueContainer2 = new DataValueContainer();

            var columnOrder = new List<int>();
            for (int i = 0; i < groupingKeyLength; i++)
            {
                columnOrder.Add(i);
            }
            _columnBoundarySearch = new ColumnBoundarySearch(columnOrder, columnOrder);
        }

        public bool SeekNextPageForValue => _seekNextPageForValue;

        public int Start { get; set; }
        public int End { get; set; }

        public int CompareTo(in BulkGroupValueRowReference x, in BulkGroupValueRowReference y)
        {
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                x.batch.Columns[i].GetValueAt(x.index, _dataValueContainer, default);
                y.batch.Columns[i].GetValueAt(y.index, _dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(_dataValueContainer, _dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public int CompareTo(in BulkGroupValueRowReference key, in BulkGroupValueKeyContainer keyContainer, in int index)
        {
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                keyContainer._data.Columns[i].GetValueAt(index, _dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(_dataValueContainer, _dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public FindBoundriesResult FindBoundries(in BulkGroupValueRowReference key, in BulkGroupValueKeyContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_dataValueContainer, start, end, default);

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

        public int FindIndex(in BulkGroupValueRowReference key, in BulkGroupValueKeyContainer keyContainer)
        {
            int index = -1;
            Start = 0;
            End = keyContainer.Count - 1;

            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_dataValueContainer, Start, End, default);

                if (low < 0)
                {
                    return low;
                }
                else
                {
                    Start = low;
                    End = high;
                }
            }

            index = Start;
            return index;
        }

        public void FindBoundriesBulk(
            ReadOnlySpan<BulkGroupValueRowReference> keys,
            ReadOnlySpan<int> sortedLookup,
            in BulkGroupValueKeyContainer keyContainer,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            Span<int> lookupBuffer)
        {
            if (_groupingKeyLength == 0)
            {
                if (keyContainer.Count > 0)
                {
                    for (int i = 0; i < lowerBounds.Length; i++)
                    {
                        lowerBounds[i] = 0;
                        upperBounds[i] = 0;
                    }
                }
                else
                {
                    for (int i = 0; i < lowerBounds.Length; i++)
                    {
                        lowerBounds[i] = -1;
                        upperBounds[i] = -1;
                    }
                }
                return;
            }

            var firstIndex = sortedLookup[0];
            var incomingBatch = keys[firstIndex].batch.Columns;

            _columnBoundarySearch.SearchBoundries(
                keyContainer._data.Columns,
                incomingBatch,
                sortedLookup,
                lowerBounds,
                upperBounds,
                0,
                keyContainer.Count - 1,
                false,
                lookupBuffer);
        }
    }
}
