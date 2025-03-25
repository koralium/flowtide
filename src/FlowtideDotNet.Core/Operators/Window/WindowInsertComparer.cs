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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class WindowInsertComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly IColumnComparer<ColumnRowReference>? orderComparer;
        private readonly List<int> partitionColumns;
        private readonly List<int> otherColumns;
        private readonly DataValueContainer dataValueContainer = new DataValueContainer();

        public WindowInsertComparer(
            IColumnComparer<ColumnRowReference>? orderComparer, 
            List<int> partitionColumns,
            List<int> otherColumns)
        {
            this.orderComparer = orderComparer;
            this.partitionColumns = partitionColumns;
            this.otherColumns = otherColumns;
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
            // First search the partition columns
            for (int i = 0; i < partitionColumns.Count; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[partitionColumns[i]].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[partitionColumns[i]].SearchBoundries(dataValueContainer, start, end, default);

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
            if (partitionColumns.Count == 0 && orderComparer == null)
            {
                if (keyContainer.Count > 0)
                {
                    return 0;
                }
            }
            if (orderComparer != null)
            {
                // Search using the order by functions
                var (low, high) = keyContainer.Data.FindBoundries(key, start, end, orderComparer);
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
            // Finally search using the remainder of the columns
            for (int i = 0; i < otherColumns.Count; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[otherColumns[i]].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[otherColumns[i]].SearchBoundries(dataValueContainer, start, end, default);

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
