﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations
{
    /// <summary>
    /// Special column row reference that also contains a value that will be treated as the last column.
    /// This is used so the value does not need to be copied into a column before insertion.
    /// 
    /// During insert, batch will be the length of grouping keys, but when fetching data it will be grouping keys + 1 to contain the values.
    /// </summary>
    internal struct ListAggColumnRowReference
    {
        public EventBatchData batch;
        public int index;
        public IDataValue insertValue;

        public override string ToString()
        {
            List<string> vals = new List<string>();
            for (int i = 0; i < batch.Columns.Count; i++)
            {
                vals.Add(batch.Columns[i].GetValueAt(index, default).ToString()!);
            }
            return $"{{{string.Join(",", vals)}}}";
        }
    }
}
