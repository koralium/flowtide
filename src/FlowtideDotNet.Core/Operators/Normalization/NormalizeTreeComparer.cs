﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class NormalizeTreeComparer : IBplusTreeComparer<ColumnRowReference, NormalizeKeyStorage>
    {
        private DataValueContainer dataValueContainer;
        private readonly List<int> _keyColumns;

        public bool SeekNextPageForValue => false;

        public NormalizeTreeComparer(List<int> keyColumns)
        {
            dataValueContainer = new DataValueContainer();
            this._keyColumns = keyColumns;
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in ColumnRowReference key, in NormalizeKeyStorage keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in ColumnRowReference key, in NormalizeKeyStorage keyContainer)
        {
            int index = -1;
            var start = 0;
            var end = keyContainer.Count - 1;
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                var col = _keyColumns[i];
                key.referenceBatch.Columns[col].GetValueAt(key.RowIndex, dataValueContainer, default);
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
    }
}
