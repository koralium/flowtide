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

using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    /// <summary>
    /// Handles the conversion of an EventBatchData to an Arrow RecordBatch.
    /// This is useful if one wants to use existing apache arrow libraries to work with the data.
    /// </summary>
    internal static class EventBatchToArrow
    {
        public static RecordBatch BatchToArrow(EventBatchData eventBatchData)
        {
            var schemaBuilder = new Apache.Arrow.Schema.Builder();
            List<IArrowArray> arrays = new List<IArrowArray>();
            int length = 0;
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                length = eventBatchData.Columns[i].Count;
                var (array, type) = eventBatchData.Columns[i].ToArrowArray();
                schemaBuilder.Field(new Apache.Arrow.Field($"{i}", type, true));
                arrays.Add(array);
            }
            return new Apache.Arrow.RecordBatch(schemaBuilder.Build(), arrays, length);
        }
    }
}
