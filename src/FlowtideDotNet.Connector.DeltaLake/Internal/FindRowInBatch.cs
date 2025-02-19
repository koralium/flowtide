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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal static class FindRowInBatch
    {
        /// <summary>
        /// Scans a batch linearly to find a row, scanning one column at the time.
        /// </summary>
        /// <param name="rowToFind"></param>
        /// <param name="batch"></param>
        /// <returns></returns>
        public static int FindRow(ColumnRowReference rowToFind, EventBatchData batch, IDeleteVector deleteVector)
        {
            return FindRowInColumn(rowToFind, batch, 0, deleteVector);
        }

        private static int FindRowInColumn(ColumnRowReference rowToFind, EventBatchData batch, int columnIndex, IDeleteVector deleteVector)
        {
            var column = batch.Columns[columnIndex];
            var dataToFind = rowToFind.referenceBatch.Columns[columnIndex].GetValueAt(rowToFind.RowIndex, default);
            for (int i = 0; i < column.Count; i++)
            {
                if (deleteVector.Contains(i))
                {
                    continue;
                }
                if (DataValueComparer.Instance.Compare(dataToFind, column.GetValueAt(i, default)) == 0)
                {
                    if (batch.Columns.Count > columnIndex + 1)
                    {
                        var index = FindRowInColumn(rowToFind, batch, columnIndex + 1, deleteVector);
                        if (index >= 0)
                        {
                            return index;
                        }
                    }
                    else
                    {
                        return i;
                    }
                }
            }
            return -1;
        }
    }
}
