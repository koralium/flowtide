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
    internal class MergeJoinSearchComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly List<int> selfColumns;
        private readonly List<int> referenceColumns;

        public int start;
        public int end;
        public bool noMatch = false;

        public bool SeekNextPageForValue => true;

        public MergeJoinSearchComparer(List<int> selfColumns, List<int> referenceColumns)
        {
            dataValueContainer = new DataValueContainer();
            this.selfColumns = selfColumns;
            this.referenceColumns = referenceColumns;
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

                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                else
                {
                    currentStart = low;
                    currentEnd = high;
                }
            }
            return new FindBoundriesResult(currentStart, currentEnd);
        }
    }
}
