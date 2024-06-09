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

using FlowtideDotNet.Core.Compute;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class EventBatchData
    {
        private readonly List<Column> columns;

        public EventBatchData(List<Column> columns)
        {
            this.columns = columns;
        }

        public IReadOnlyList<Column> Columns => columns;

        /// <summary>
        /// Compares two rows from different batches.
        /// </summary>
        /// <param name="otherBatch"></param>
        /// <param name="thisIndex"></param>
        /// <param name="otherIndex"></param>
        /// <returns></returns>
        public int CompareRows(EventBatchData otherBatch, in int thisIndex, in int otherIndex)
        {
            for (int i = 0; i < columns.Count; i++)
            {
                int compareResult = columns[i].CompareTo(otherBatch.columns[i], thisIndex, otherIndex);
                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            return 0;
        }
    }
}
