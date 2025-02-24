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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
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
        public static int FindRow(ColumnRowReference rowToFind, EventBatchData batch, IDeleteVector deleteVector, List<StructField> fields)
        {
            return FindRowInColumn(rowToFind, batch, 0, deleteVector, fields);
        }

        /// <summary>
        /// Searches an initial column index, if it is found in that column it checks the other columns.
        /// This can be useful if one column is known to have a high cardinality.
        /// </summary>
        /// <param name="rowToFind"></param>
        /// <param name="batch"></param>
        /// <param name="columnIndex"></param>
        /// <param name="deleteVector"></param>
        /// <returns></returns>
        private static int FindRowInColumn(ColumnRowReference rowToFind, EventBatchData batch, int columnIndex, IDeleteVector deleteVector, List<StructField> fields)
        {
            var column = batch.Columns[columnIndex];
            var dataToFind = rowToFind.referenceBatch.Columns[columnIndex].GetValueAt(rowToFind.RowIndex, default);

            if (fields[columnIndex].Type is FloatType)
            {
                // Special case since flowtide treats float and double as the same type (double)
                // But we need to cast this to float to fix rounding errors
                dataToFind = new DoubleValue((float)dataToFind.AsDouble);
            }
            else if (fields[columnIndex].Type is DecimalType decType)
            {
                // Same for decimal, must round it to the scale of the decimal type
                dataToFind = new DecimalValue(Math.Round(dataToFind.AsDecimal, decType.Scale));
            }

            for (int i = 0; i < column.Count; i++)
            {
                if (deleteVector.Contains(i))
                {
                    continue;
                }
                if (DataValueComparer.Instance.Compare(dataToFind, column.GetValueAt(i, default)) == 0)
                {
                    bool found = true;
                    for (int c = 0; c < batch.Columns.Count; c++)
                    {
                        if (c == columnIndex)
                        {
                            continue;
                        }
                        var data = rowToFind.referenceBatch.Columns[c].GetValueAt(rowToFind.RowIndex, default);

                        if (fields[c].Type is FloatType)
                        {
                            // Special case since flowtide treats float and double as the same type (double)
                            // But we need to cast this to float to fix rounding errors
                            data = new DoubleValue((float)data.AsDouble);
                        }
                        else if (fields[c].Type is DecimalType decType)
                        {
                            // Same for decimal, must round it to the scale of the decimal type
                            data = new DecimalValue(Math.Round(dataToFind.AsDecimal, decType.Scale));
                        }

                        if (DataValueComparer.Instance.Compare(data, batch.Columns[c].GetValueAt(i, default)) != 0)
                        {
                            found = false;
                            break;
                        }
                    }
                    if (found)
                    {
                        return i;
                    }
                }
            }
            return -1;
        }
    }
}
