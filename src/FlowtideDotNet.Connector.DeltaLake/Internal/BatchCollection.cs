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
using Apache.Arrow.Memory;
using FlowtideDotNet.Connector.DeltaLake.Internal.ArrowEncoding;
using FlowtideDotNet.Core.Operators.Write.Column;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class BatchCollection : IReadOnlyCollection<RecordBatch>
    {
        private readonly ColumnWriteOperation columnWriteOperation;
        private readonly IAsyncEnumerator<ColumnWriteOperation> enumerable;
        private readonly RecordBatchEncoder recordBatchEncoder;
        private readonly MemoryAllocator memoryAllocator;
        private readonly bool includeDeletedColumn;

        public BatchCollection(
            ColumnWriteOperation columnWriteOperation, 
            IAsyncEnumerator<ColumnWriteOperation> enumerable,
            RecordBatchEncoder recordBatchEncoder,
            MemoryAllocator memoryAllocator, 
            bool includeDeletedColumn)
        {
            this.columnWriteOperation = columnWriteOperation;
            this.enumerable = enumerable;
            this.recordBatchEncoder = recordBatchEncoder;
            this.memoryAllocator = memoryAllocator;
            this.includeDeletedColumn = includeDeletedColumn;
        }
        public int Count => 1;

        public IEnumerator<RecordBatch> GetEnumerator()
        {
            return new BatchEnumerator(columnWriteOperation, enumerable, recordBatchEncoder, memoryAllocator, includeDeletedColumn);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new BatchEnumerator(columnWriteOperation, enumerable, recordBatchEncoder, memoryAllocator, includeDeletedColumn);
        }
    }
}
