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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore;

namespace FlowtideDotNet.Core.Operators.Write.Column
{
    internal class ModifiedKeyExistingComparer
    {
        private readonly DataValueContainer _leftContainer;
        private readonly DataValueContainer _rightContainer;
        private readonly IReadOnlyList<int> primaryKeys;

        public ModifiedKeyExistingComparer(IReadOnlyList<int> primaryKeys)
        {
            _leftContainer = new DataValueContainer();
            _rightContainer = new DataValueContainer();
            this.primaryKeys = primaryKeys;
        }

        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                var pk = primaryKeys[i];
                x.referenceBatch.Columns[i].GetValueAt(x.RowIndex, _leftContainer, default);
                y.referenceBatch.Columns[pk].GetValueAt(y.RowIndex, _rightContainer, default);
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
