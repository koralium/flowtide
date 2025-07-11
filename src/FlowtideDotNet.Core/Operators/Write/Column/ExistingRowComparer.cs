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
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Operators.Write.Column
{
    internal class ExistingRowComparer
    {
        private readonly DataValueContainer _leftContainer;
        private readonly DataValueContainer _rightContainer;
        private readonly List<KeyValuePair<int, ReferenceSegment?>> leftColumns;
        private readonly List<KeyValuePair<int, ReferenceSegment?>> rightColumns;

        public ExistingRowComparer(List<KeyValuePair<int, ReferenceSegment?>> leftColumns, List<KeyValuePair<int, ReferenceSegment?>> rightColumns)
        {
            this.leftColumns = leftColumns;
            this.rightColumns = rightColumns;
            _leftContainer = new DataValueContainer();
            _rightContainer = new DataValueContainer();

            if (leftColumns.Count != rightColumns.Count)
            {
                throw new ArgumentException("The number of columns in the left and right columns must be the same.");
            }
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < leftColumns.Count; i++)
            {
                x.referenceBatch.Columns[leftColumns[i].Key].GetValueAt(x.RowIndex, _leftContainer, leftColumns[i].Value);
                y.referenceBatch.Columns[rightColumns[i].Key].GetValueAt(y.RowIndex, _rightContainer, rightColumns[i].Value);
                int compareVal = DataValueComparer.CompareTo(_leftContainer, _rightContainer);

                if (compareVal != 0)
                {
                    return compareVal;
                }
            }
            return 0;
        }
    }
}
