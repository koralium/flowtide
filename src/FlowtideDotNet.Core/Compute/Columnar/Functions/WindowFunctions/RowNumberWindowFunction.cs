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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal class RowNumberWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            return new RowNumberWindowFunction();
        }
    }
    internal class RowNumberWindowFunction : IWindowFunction
    {
        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            return ValueTask.FromResult<IDataValue>(new Int64Value(partitionRowIndex + 1));
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            return ValueTask.CompletedTask;
        }

        public Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, 
            List<int> partitionColumns, 
            IMemoryAllocator memoryAllocator, 
            IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
