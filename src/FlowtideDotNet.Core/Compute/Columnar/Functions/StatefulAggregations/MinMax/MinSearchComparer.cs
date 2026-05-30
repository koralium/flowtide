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
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    internal class MinSearchComparer : IMinMaxSearchComparer
    {
        private readonly int _groupingKeyLength;
        private readonly DataValueContainer _dataValueContainer;
        private readonly DataValueContainer _dataValueContainer2;

        public MinSearchComparer(int groupingKeyLength)
        {
            this._groupingKeyLength = groupingKeyLength;
            _dataValueContainer = new DataValueContainer();
            _dataValueContainer2 = new DataValueContainer();
        }

        public bool SeekNextPageForValue => true;

        public bool NoMatch { get; set; }
        public int Start { get; set; }
        public int End { get; set; }

        public int CompareTo(in ListAggColumnRowReference x, in ListAggColumnRowReference y)
        {
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                x.batch.Columns[i].GetValueAt(x.index, _dataValueContainer, default);
                y.batch.Columns[i].GetValueAt(y.index, _dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(_dataValueContainer, _dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public int CompareTo(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer, in int index)
        {
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                keyContainer._data.Columns[i].GetValueAt(index, _dataValueContainer2, default);
                var cmp = DataValueComparer.CompareTo(_dataValueContainer, _dataValueContainer2);
                if (cmp != 0)
                {
                    return cmp;
                }
            }
            return 0;
        }

        public FindBoundriesResult FindBoundries(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_dataValueContainer, start, end, default);

                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                else
                {
                    start = low;
                    end = high;
                }
            }
            return new FindBoundriesResult(start, end);
        }

        public int FindIndex(in ListAggColumnRowReference key, in ListAggKeyStorageContainer keyContainer)
        {
            int index = -1;
            Start = 0;
            End = keyContainer.Count - 1;
            NoMatch = false;
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                // Get value by container to skip boxing for each value
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_dataValueContainer, Start, End, default);

                if (low < 0)
                {
                    NoMatch = true;
                    return low;
                }
                else
                {
                    index = low;
                    Start = low;
                    End = high;
                }
            }
            return index;
        }
    }
}
