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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    internal class MaxInsertComparer : IBplusTreeComparer<ListAggColumnRowReference, ListAggKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly int _groupingKeyLength;

        public MaxInsertComparer(int groupingKeyLength)
        {
            _groupingKeyLength = groupingKeyLength;
            dataValueContainer = new DataValueContainer();
        }

        public bool SeekNextPageForValue => false;

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
            int start = 0;
            int end = keyContainer.Count - 1;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                // Get value by container to skip boxing for each value
                key.batch.Columns[i].GetValueAt(key.index, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(dataValueContainer, start, end, default, true);

                if (low < 0)
                {
                    return low;
                }
                else
                {
                    start = low;
                    end = high;
                }
            }

            (index, _) = keyContainer._data.Columns[_groupingKeyLength].SearchBoundries(key.insertValue, start, end, default, true);

            return index;
        }
    }
}
