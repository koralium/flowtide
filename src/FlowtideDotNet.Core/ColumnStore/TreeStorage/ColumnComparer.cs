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

using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class ColumnComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly int columnCount;

        public bool SeekNextPageForValue => false;

        public ColumnComparer(int columnCount)
        {
            dataValueContainer = new DataValueContainer();
            this.columnCount = columnCount;
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            return x.referenceBatch.CompareRows(y.referenceBatch, x.RowIndex, y.RowIndex);
        }

        public int CompareTo(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer, in int index)
        {
            return keyContainer._data.CompareRows(key.referenceBatch, index, key.RowIndex);
        }

        public int FindIndex(in ColumnRowReference key, in ColumnKeyStorageContainer keyContainer)
        {
            int index = -1;
            int start = 0;
            int end = keyContainer.Count - 1;
            for (int i = 0; i < columnCount; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default);

                if (low != 0)
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
