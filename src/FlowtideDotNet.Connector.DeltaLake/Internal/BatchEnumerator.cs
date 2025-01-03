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
    internal class BatchEnumerator : IEnumerator<RecordBatch>
    {
        private ColumnWriteOperation? firstOperation;
        private readonly IAsyncEnumerator<ColumnWriteOperation> enumerator;
        private readonly RecordBatchEncoder encoder;
        private readonly MemoryAllocator memoryAllocator;
        private RecordBatch? recordBatch;

        public RecordBatch Current => recordBatch ?? throw new InvalidOperationException();

        object IEnumerator.Current => Current;

        public BatchEnumerator(
            ColumnWriteOperation firstOperation, 
            IAsyncEnumerator<ColumnWriteOperation> enumerator,
            RecordBatchEncoder encoder,
            MemoryAllocator memoryAllocator)
        {
            this.firstOperation = firstOperation;
            this.enumerator = enumerator;
            this.encoder = encoder;
            this.memoryAllocator = memoryAllocator;
        }

        public void Dispose()
        {
            if (recordBatch != null)
            {
                recordBatch.Dispose();
                recordBatch = null;
            }
        }

        public bool MoveNext()
        {
            int count = 0;

            while (count < 1000)
            {
                if (firstOperation != null)
                {
                    // TODO: Implement logic to create a RecordBatch from ColumnWriteOperation
                    encoder.AddRow(firstOperation.Value);
                    firstOperation = default;
                    count++;
                    continue;
                }
                if (!enumerator.MoveNextAsync().GetAwaiter().GetResult())
                {
                    break;
                }
                count++;
                // TODO: Implement logic to create a RecordBatch from ColumnWriteOperation
                encoder.AddRow(enumerator.Current);
            }
            if (recordBatch != null)
            {
                recordBatch.Dispose();
                recordBatch = null;
            }
            if (count > 0)
            {
                recordBatch = encoder.RecordBatch(memoryAllocator);
                encoder.NewBatch();
                return true;
            }
            encoder.NewBatch();
            return false;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }
    }
}
