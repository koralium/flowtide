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

namespace FlowtideDotNet.Core.Operators.Write.Column
{
    public struct ColumnWriteOperation
    {
        /// <summary>
        /// Contains the batch where the data resides.
        /// If it is a delete operation, only the primary key columns will be present in order they were provided.
        /// </summary>
        public EventBatchData EventBatchData { get; set; }

        public int Index { get; set; }

        public bool IsDeleted { get; set; }
    }
}
