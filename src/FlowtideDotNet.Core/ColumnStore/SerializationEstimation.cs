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

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct SerializationEstimation
    {
        public int fieldNodeCount;
        public int bufferCount;
        public int bodyLength;

        /// <summary>
        /// Number of variadic-width columns (e.g. StringView/BinaryView) that require
        /// a variadic_buffer_counts entry in the Arrow IPC RecordBatch message.
        /// </summary>
        public int variadicColumnCount;

        public SerializationEstimation(int fieldNodeCount, int bufferCount, int bodyLength, int variadicColumnCount = 0)
        {
            this.fieldNodeCount = fieldNodeCount;
            this.bufferCount = bufferCount;
            this.bodyLength = bodyLength;
            this.variadicColumnCount = variadicColumnCount;
        }
    }
}
