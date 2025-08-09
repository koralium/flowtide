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

using FlexBuffers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Flexbuffer;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal class ColumnRowData : IRowData
    {
        private readonly EventBatchData eventBatchData;
        private readonly int index;

        public ColumnRowData(EventBatchData eventBatchData, int index)
        {
            this.eventBatchData = eventBatchData;
            this.index = index;
        }
        public int Length => eventBatchData.Columns.Count;

        public FlxValue GetColumn(int index)
        {
            var dataValue = eventBatchData.GetColumn(index).GetValueAt(this.index, default);
            return RowEventToEventBatchData.DataValueToFlxValue(dataValue);
        }

        public FlxValueRef GetColumnRef(scoped in int index)
        {
            throw new NotImplementedException();
        }
    }
}
