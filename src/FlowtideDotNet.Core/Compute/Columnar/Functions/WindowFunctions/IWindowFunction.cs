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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal interface IWindowFunction
    {
        bool RequirePartitionCompute { get; }

        Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree,
            List<int> partitionColumns,
            IMemoryAllocator memoryAllocator,
            IStateManagerClient stateManagerClient,
            IWindowAddOutputRow addOutputRow);

        IAsyncEnumerable<EventBatchWeighted> ComputePartition(
            ColumnRowReference partitionValues);

        IAsyncEnumerable<EventBatchWeighted> OnReceive(
            ColumnRowReference partitionValues,
            ColumnRowReference inputRow,
            int weight);

        ValueTask Commit();
    }
}
