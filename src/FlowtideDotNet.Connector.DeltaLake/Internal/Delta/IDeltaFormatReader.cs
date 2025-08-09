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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal struct BatchResult
    {
        public EventBatchData data;
        public long count;
    }

    internal struct BatchResultWithWeights
    {
        public EventBatchData data;
        public PrimitiveList<int> weights;
        public long count;
    }

    internal struct CdcBatchResult
    {
        public EventBatchData data;
        public PrimitiveList<int> weights;
        public long count;
    }

    internal interface IDeltaFormatReader
    {
        /// <summary>
        /// Initialize the reader with the table.
        /// This allows the reader to access the schema and other table metadata.
        /// </summary>
        /// <param name="table"></param>
        void Initialize(DeltaTable table, IReadOnlyList<string> columnNames);

        IAsyncEnumerable<BatchResult> ReadDataFile(
            IFileStorage storage,
            IOPath table,
            string path,
            IDeleteVector deleteVector,
            Dictionary<string, string>? partitionValues,
            IMemoryAllocator memoryAllocator);

        IAsyncEnumerable<BatchResultWithWeights> ReadAddRemovedDataFile(
            IFileStorage storage,
            IOPath table,
            string path,
            IEnumerable<(long id, int weight)> changedIndices,
            Dictionary<string, string>? partitionValues,
            IMemoryAllocator memoryAllocator);

        IAsyncEnumerable<CdcBatchResult> ReadCdcFile(IFileStorage storage, IOPath table, string path, Dictionary<string, string>? partitionValues, IMemoryAllocator memoryAllocator);
    }
}
