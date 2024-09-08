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
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations
{
    internal class ListAggSearchComparer : IBplusTreeComparer<ListAggColumnRowReference, ListAggKeyStorageContainer>
    {
        private readonly int _groupingKeyLength;
        private readonly DataValueContainer _dataValueContainer;
        public int start;
        public int end;
        public bool noMatch = false;

        public ListAggSearchComparer(int groupingKeyLength)
        {
            this._groupingKeyLength = groupingKeyLength;
            _dataValueContainer = new DataValueContainer();
        }

        public bool SeekNextPageForValue => true;

        public int CompareTo(in ListAggColumnRowReference x, in ListAggColumnRowReference y)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer)
        {
            int index = -1;
            start = 0;
            end = keyContainer.Count - 1;
            noMatch = false;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                // Get value by container to skip boxing for each value
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_dataValueContainer, start, end, default);

                if (low < 0)
                {
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
    }
}
