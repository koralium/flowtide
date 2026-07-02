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
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinSearchComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>, IRouteToLeftmost
    {
        private DataValueContainer dataValueContainer;
        private readonly List<int> selfColumns;
        private readonly List<int> referenceColumns;
        private readonly List<JoinComparisonType>? comparisonTypes;
        private readonly ColumnBoundarySearch _columnBoundarySearch;

        public int start;
        public int end;
        public bool noMatch = false;

        public bool SeekNextPageForValue => true;

        public bool RouteToLeftmost => comparisonTypes != null && comparisonTypes.Count > 0 &&
                                       (comparisonTypes[0] == JoinComparisonType.LessThan || comparisonTypes[0] == JoinComparisonType.LessThanOrEqual);

        public MergeJoinSearchComparer(List<int> selfColumns, List<int> referenceColumns, List<JoinComparisonType>? comparisonTypes = null)
        {
            dataValueContainer = new DataValueContainer();
            this.selfColumns = selfColumns;
            this.referenceColumns = referenceColumns;
            this.comparisonTypes = comparisonTypes;
            _columnBoundarySearch = new ColumnBoundarySearch(selfColumns, referenceColumns, comparisonTypes);
        }
        private readonly DataValueContainer _yDataValueContainer = new DataValueContainer();

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            // Usually not called for SearchComparer, but implementing similarly if needed
            for (int i = 0; i < selfColumns.Count; i++)
            {
                var col = selfColumns[i];
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
            for (int i = 0; i < selfColumns.Count; i++)
            {
                var col = selfColumns[i];
                key.referenceBatch.Columns[referenceColumns[i]].GetValueAt(key.RowIndex, dataValueContainer, default);
                keyContainer._data.Columns[col].GetValueAt(index, _yDataValueContainer, default);
                int cmp = FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer.CompareTo(dataValueContainer, _yDataValueContainer);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            start = 0;
            end = keyContainer.Count - 1;
            noMatch = false;
            for (int i = 0; i < selfColumns.Count; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[referenceColumns[i]].GetValueAt(key.RowIndex, dataValueContainer, default);

                if (dataValueContainer._type == ArrowTypeId.Null)
                {
                    noMatch = true;
                    return start;
                }
                var (low, high) = keyContainer._data.Columns[selfColumns[i]].SearchBoundries(dataValueContainer, start, end, default);

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
            return index;
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
            _columnBoundarySearch.SearchBoundries(keyContainer._data.Columns, incomingBatch, sortedLookup, lowerBounds, upperBounds, 0, keyContainer.Count - 1, true, lookupBuffer);
        }

        public FindBoundriesResult FindBoundries(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            int currentStart = startIndex;
            int currentEnd = endIndex;
            for (int i = 0; i < selfColumns.Count; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[referenceColumns[i]].GetValueAt(key.RowIndex, dataValueContainer, default);

                if (dataValueContainer._type == ArrowTypeId.Null)
                {
                    return new FindBoundriesResult(~currentStart, ~currentStart);
                }
                var (low, high) = keyContainer._data.Columns[selfColumns[i]].SearchBoundries(dataValueContainer, currentStart, currentEnd, default);

                var op = comparisonTypes != null && i < comparisonTypes.Count ? comparisonTypes[i] : JoinComparisonType.Equal;
                int matchStart;
                int matchEnd;

                if (op == JoinComparisonType.Equal)
                {
                    matchStart = low;
                    matchEnd = high;
                }
                else if (op == JoinComparisonType.LessThan)
                {
                    int firstGte = low >= 0 ? low : ~low;
                    matchStart = currentStart;
                    matchEnd = firstGte - 1;
                    if (firstGte <= currentStart)
                    {
                        matchStart = ~currentStart;
                        matchEnd = ~currentStart;
                    }
                }
                else if (op == JoinComparisonType.LessThanOrEqual)
                {
                    int firstGt = low >= 0 ? high + 1 : ~low;
                    matchStart = currentStart;
                    matchEnd = firstGt - 1;
                    if (firstGt <= currentStart)
                    {
                        matchStart = ~currentStart;
                        matchEnd = ~currentStart;
                    }
                }
                else if (op == JoinComparisonType.GreaterThan)
                {
                    int firstGt = low >= 0 ? high + 1 : ~low;
                    matchStart = firstGt;
                    matchEnd = currentEnd;
                    if (firstGt > currentEnd)
                    {
                        matchStart = ~firstGt;
                        matchEnd = ~firstGt;
                    }
                }
                else // GreaterThanOrEqual
                {
                    int firstGte = low >= 0 ? low : ~low;
                    matchStart = firstGte;
                    matchEnd = currentEnd;
                    if (firstGte > currentEnd)
                    {
                        matchStart = ~firstGte;
                        matchEnd = ~firstGte;
                    }
                }

                if (matchStart < 0)
                {
                    return new FindBoundriesResult(matchStart, matchStart);
                }
                else
                {
                    currentStart = matchStart;
                    currentEnd = matchEnd;
                }
            }
            return new FindBoundriesResult(currentStart, currentEnd);
        }
    }
}
