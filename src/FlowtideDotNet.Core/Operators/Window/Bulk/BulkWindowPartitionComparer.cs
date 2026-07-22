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

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Finds a partition's row range in a page, comparing only the partition columns.
    /// The matched range is exposed through <see cref="start"/> and <see cref="end"/>.
    /// </summary>
    internal class BulkWindowPartitionComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly DataValueContainer _dataValueContainer;
        private readonly IReadOnlyList<int> _partitionColumns;

        public int start;
        public int end;
        public bool noMatch;

        public BulkWindowPartitionComparer(IReadOnlyList<int> partitionColumns)
        {
            _dataValueContainer = new DataValueContainer();
            _partitionColumns = partitionColumns;
        }

        public bool SeekNextPageForValue => true;

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
            return 0;
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            start = 0;
            noMatch = false;
            end = keyContainer.Count - 1;
            for (int i = 0; i < _partitionColumns.Count; i++)
            {
                key.referenceBatch.Columns[_partitionColumns[i]].GetValueAt(key.RowIndex, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[_partitionColumns[i]].SearchBoundries(_dataValueContainer, start, end, default);

                if (low < 0)
                {
                    start = low;
                    noMatch = true;
                    return low;
                }
                index = low;
                start = low;
                end = high;
            }

            if (_partitionColumns.Count == 0)
            {
                if (keyContainer.Count > 0)
                {
                    return 0;
                }
                noMatch = true;
            }
            return index;
        }

        public FindBoundriesResult FindBoundries(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            throw new NotImplementedException();
        }
    }
}
