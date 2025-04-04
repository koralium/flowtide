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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static SqlParser.Ast.MatchRecognizeSymbol;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class WindowPartitionStartSearchComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly List<int> _partitionColumnIndices;

        public int start;
        public int end;
        public bool noMatch = false;

        public WindowPartitionStartSearchComparer(List<int> partitionColumnIndices)
        {
            dataValueContainer = new DataValueContainer();
            _partitionColumnIndices = partitionColumnIndices;
        }

        public bool SeekNextPageForValue => throw new NotImplementedException();

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
            start = 0;
            noMatch = false;
            end = keyContainer.Count - 1;
            for (int i = 0; i < _partitionColumnIndices.Count; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, dataValueContainer, default);
                var (low, high) = keyContainer._data.Columns[_partitionColumnIndices[i]].SearchBoundries(dataValueContainer, start, end, default);

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

            if (_partitionColumnIndices.Count == 0)
            {
                if (keyContainer.Count > 0)
                {
                    return 0;
                }
                else
                {
                    noMatch = true;
                }
            }
            return index;
        }
    }
}
