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
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Orders full rows for the bulk window operator's trees: first by the partition columns, then by the
    /// window order by expressions and finally by the remaining columns so that equal rows compare equal.
    /// Unlike <see cref="WindowInsertComparer"/> this implements the full comparer surface needed by the
    /// bulk inserter (row to row comparison and boundary searches). When the order by keys are plain
    /// columns the bulk boundary search runs vectorized over all probes at once; computed order by
    /// expressions keep the per key search through the compiled order comparer.
    /// </summary>
    internal class BulkWindowInsertComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly IColumnComparer<ColumnRowReference>? _orderComparer;
        private readonly Func<EventBatchData, int, EventBatchData, int, int>? _orderCompareFunction;
        private readonly IReadOnlyList<int> _partitionColumns;
        private readonly IReadOnlyList<int> _otherColumns;
        private readonly DataValueContainer _dataValueContainer = new DataValueContainer();
        private readonly ColumnBoundarySearch? _bulkSearch;

        public BulkWindowInsertComparer(
            Func<EventBatchData, int, EventBatchData, int, int>? orderCompareFunction,
            IReadOnlyList<int> partitionColumns,
            IReadOnlyList<int> otherColumns)
            : this(orderCompareFunction, partitionColumns, otherColumns, null, null)
        {
        }

        public BulkWindowInsertComparer(
            Func<EventBatchData, int, EventBatchData, int, int>? orderCompareFunction,
            IReadOnlyList<int> partitionColumns,
            IReadOnlyList<int> otherColumns,
            IReadOnlyList<int>? sortLayoutColumns,
            IReadOnlyList<SortColumnDirection>? sortLayoutDirections)
        {
            _orderCompareFunction = orderCompareFunction;
            if (orderCompareFunction != null)
            {
                _orderComparer = new SortFieldColumnRowComparer(orderCompareFunction);
            }
            _partitionColumns = partitionColumns;
            _otherColumns = otherColumns;
            if (sortLayoutColumns != null)
            {
                _bulkSearch = new ColumnBoundarySearch(sortLayoutColumns, sortLayoutColumns, sortLayoutDirections);
            }
        }

        void IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>.FindBoundriesBulk(
            ReadOnlySpan<ColumnRowReference> keys,
            ReadOnlySpan<int> sortedLookup,
            in ColumnKeyStorageContainer keyContainer,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            Span<int> lookupBuffer)
        {
            if (_bulkSearch != null && keys.Length > 0)
            {
                var incomingBatch = keys[0].referenceBatch.Columns;
                _bulkSearch.SearchBoundries(keyContainer._data.Columns, incomingBatch, sortedLookup, lowerBounds, upperBounds, 0, keyContainer.Count - 1, false, lookupBuffer);
                return;
            }

            // Same shape as the interface default: per key searches narrowed by the previous result.
            int currentStart = 0;
            int maxEnd = keyContainer.Count - 1;
            for (int i = 0; i < sortedLookup.Length; i++)
            {
                var keyIndex = sortedLookup[i];
                var bounds = FindBoundries(in keys[keyIndex], in keyContainer, currentStart, maxEnd);
                lowerBounds[i] = bounds.lowerBounds;
                upperBounds[i] = bounds.upperBounds;
                currentStart = bounds.lowerBounds < 0 ? ~bounds.lowerBounds : bounds.lowerBounds;
            }
        }

        private sealed class SortFieldColumnRowComparer : IColumnComparer<ColumnRowReference>
        {
            private readonly Func<EventBatchData, int, EventBatchData, int, int> _func;

            public SortFieldColumnRowComparer(Func<EventBatchData, int, EventBatchData, int, int> func)
            {
                _func = func;
            }

            public int Compare(in ColumnRowReference x, in ColumnRowReference y)
            {
                return _func(x.referenceBatch, x.RowIndex, y.referenceBatch, y.RowIndex);
            }
        }

        public bool SeekNextPageForValue => false;

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < _partitionColumns.Count; i++)
            {
                var column = _partitionColumns[i];
                var compareResult = x.referenceBatch.Columns[column].CompareTo(y.referenceBatch.Columns[column], x.RowIndex, y.RowIndex);
                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            if (_orderCompareFunction != null)
            {
                var compareResult = _orderCompareFunction(x.referenceBatch, x.RowIndex, y.referenceBatch, y.RowIndex);
                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            for (int i = 0; i < _otherColumns.Count; i++)
            {
                var column = _otherColumns[i];
                var compareResult = x.referenceBatch.Columns[column].CompareTo(y.referenceBatch.Columns[column], x.RowIndex, y.RowIndex);
                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            return 0;
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            var containerRow = new ColumnRowReference()
            {
                referenceBatch = keyContainer._data,
                RowIndex = index
            };
            return CompareTo(in containerRow, in key);
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            var result = FindBoundries(in key, in keyContainer, 0, keyContainer.Count - 1);
            return result.lowerBounds;
        }

        public FindBoundriesResult FindBoundries(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            bool anyColumn = false;
            for (int i = 0; i < _partitionColumns.Count; i++)
            {
                anyColumn = true;
                key.referenceBatch.Columns[_partitionColumns[i]].GetValueAt(key.RowIndex, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[_partitionColumns[i]].SearchBoundries(_dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                start = low;
                end = high;
            }
            if (_orderComparer != null)
            {
                anyColumn = true;
                var (low, high) = keyContainer.Data.FindBoundries(key, start, end, _orderComparer);
                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                start = low;
                end = high;
            }
            for (int i = 0; i < _otherColumns.Count; i++)
            {
                anyColumn = true;
                key.referenceBatch.Columns[_otherColumns[i]].GetValueAt(key.RowIndex, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[_otherColumns[i]].SearchBoundries(_dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                start = low;
                end = high;
            }
            if (!anyColumn)
            {
                if (keyContainer.Count > 0)
                {
                    return new FindBoundriesResult(0, 0);
                }
                return new FindBoundriesResult(-1, -1);
            }
            return new FindBoundriesResult(start, end);
        }
    }
}
