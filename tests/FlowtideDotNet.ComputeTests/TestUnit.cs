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

using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;

namespace FlowtideDotNet.ComputeTests
{
    public class TestUnit
    {
        [Fact]
        public async Task TestAgg()
        {
            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);

            StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions(), NullLogger.Instance, new System.Diagnostics.Metrics.Meter(""), "");

            AggregateFunction aggregateFunction = new AggregateFunction()
            {
                ExtensionName = "count",
                ExtensionUri = "/functions_aggregate_generic.yaml",
                Arguments = new List<Expression>()
                {
                    new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment()
                        {
                            Field = 0
                        }
                    }
                }
            };

            var stateClient = stateManager.GetOrCreateClient("a");
            var res = await ColumnMeasureCompiler.CompileMeasure(0, stateClient, aggregateFunction, register, GlobalMemoryManager.Instance);

            IColumn[] columns = [Column.Create(GlobalMemoryManager.Instance)];
            var batch = new EventBatchData(columns);
            new Column(GlobalMemoryManager.Instance) { new Int64Value(1) };
            columns[0].Add(new Int64Value(1));
            columns[0].Add(new Int64Value(2));
            columns[0].Add(new Int64Value(3));
                
            IColumn[] groupingBatchColumns = new IColumn[0];
            var groupBatch = new EventBatchData(groupingBatchColumns);

            Column stateColumn = Column.Create(GlobalMemoryManager.Instance);
            stateColumn.Add(NullValue.Instance);
            var stateColumnRef = new ColumnReference(stateColumn, 0, default);

            var groupingKey = new Core.ColumnStore.TreeStorage.ColumnRowReference() { referenceBatch = groupBatch, RowIndex = 0 };
            for (int i = 0; i < batch.Count; i++)
            {
                await res.Compute(groupingKey, batch, i, stateColumnRef, 1);
            }

            Column outputColumn = Column.Create(GlobalMemoryManager.Instance);

            await res.GetValue(groupingKey, stateColumnRef, outputColumn);

            var actual = outputColumn.GetValueAt(0, default);
            
        }
    }
}
