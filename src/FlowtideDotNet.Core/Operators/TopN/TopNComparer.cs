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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.TopN
{
    internal class TopNComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly IColumnComparer<ColumnRowReference> _sortComparer;
        private DataValueContainer _dataValueContainer;

        public TopNComparer(IColumnComparer<ColumnRowReference> sortComparer)
        {
            _sortComparer = sortComparer;
            _dataValueContainer = new DataValueContainer();
        }

        public bool SeekNextPageForValue => false;

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count - 1;

            (start, end) = keyContainer._data.FindBoundries(key, start, end, _sortComparer);

            if (start < 0)
            {
                return start;
            }

            if (start == end)
            {
                return start;
            }

            for (int i = 0; i < keyContainer._data.Columns.Count; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_dataValueContainer, start, end, default);

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
