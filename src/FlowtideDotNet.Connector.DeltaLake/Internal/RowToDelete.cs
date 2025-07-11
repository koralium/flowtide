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

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    /// <summary>
    /// Represents a row that should be deleted from the table.
    /// Files are scanned and the weight of this will increase until it reaches 0.
    /// This allows deletion of duplicate rows, or a single row if one should remain.
    /// The lock is used when updating the weight. Since multiple files can be looked at in parallel, this is necessary.
    /// </summary>
    internal class RowToDelete
    {
        /// <summary>
        /// Location in the delete record batch
        /// </summary>
        public int DeleteIndex { get; set; }

        public int Weight { get; set; }

        public object Lock = new object();
    }
}
