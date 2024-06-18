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
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinSearchComparer : IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private DataValueContainer dataValueContainer;
        private readonly List<KeyValuePair<int, ReferenceSegment?>> selfColumns;
        private readonly List<KeyValuePair<int, ReferenceSegment?>> referenceColumns;

        public int start;
        public int end;
        public bool noMatch = false;

        public MergeJoinSearchComparer(List<KeyValuePair<int, ReferenceSegment?>> selfColumns, List<KeyValuePair<int, ReferenceSegment?>> referenceColumns)
        {
            dataValueContainer = new DataValueContainer();
            this.selfColumns = selfColumns;
            this.referenceColumns = referenceColumns;
        }
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
            end = keyContainer.Count - 1;
            noMatch = false;
            for (int i = 0; i < selfColumns.Count; i++)
            {
                // Get value by container to skip boxing for each value
                key.referenceBatch.Columns[referenceColumns[i].Key].GetValueAt(key.RowIndex, dataValueContainer, referenceColumns[i].Value);
                if (dataValueContainer._type == ArrowTypeId.Null)
                {
                    noMatch = true;
                    return start;
                }
                var (low, high) = keyContainer._data.Columns[selfColumns[i].Key].SearchBoundries(dataValueContainer, start, end, selfColumns[i].Value);

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
    }
}
