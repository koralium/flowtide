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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateless
{
    internal class AvgAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new AvgAggregation(compiledValue);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Average, new AvgAggregationDefinition());
        }
    }

    internal class AvgAggregation : IColumnBulkAggregation
    {
        private static readonly StructHeader AvgStateHeader = StructHeader.Create("sum", "count");

        private readonly Func<EventBatchData, int, IDataValue> projectionFunction;
        private readonly DataValueContainer _dataValueContainer;
        private readonly DataValueContainer _sumContainer;

        public AvgAggregation(Func<EventBatchData, int, IDataValue> projectionFunction)
        {
            this.projectionFunction = projectionFunction;
            _dataValueContainer = new DataValueContainer();
            _sumContainer = new DataValueContainer();
        }

        public Task CommitAsync()
        {
            return Task.CompletedTask;
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            groupState.GetValue(_dataValueContainer);

            long currentCount = 0;

            if (_dataValueContainer.Type == ArrowTypeId.Struct)
            {
                var structValue = _dataValueContainer.AsStruct;
                structValue.GetAt(0, _sumContainer);
                currentCount = structValue.GetAt(1).AsLong;
            }
            else
            {
                _sumContainer._type = ArrowTypeId.Null;
            }

            for (int i = 0; i < indices.Length; i++)
            {
                var value = projectionFunction(data, indices[i]);
                if (value.Type != ArrowTypeId.Null)
                {
                    var weight = weights[indices[i]];
                    SumAggregation.DoSum(value, _sumContainer, weight);
                    currentCount += weight;
                }
            }

            if (currentCount <= 0 || _sumContainer.Type == ArrowTypeId.Null)
            {
                _dataValueContainer._type = ArrowTypeId.Null;
            }
            else
            {
                var countVal = new Int64Value(currentCount);
                var structVal = new StructValue(AvgStateHeader, _sumContainer, countVal);
                _dataValueContainer._structValue = structVal;
                _dataValueContainer._type = ArrowTypeId.Struct;
            }

            groupState.Update(_dataValueContainer);
            return true;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = startIndex; i < startIndex + length; i++)
            {
                groupStates[i].GetValue(_dataValueContainer);
                if (_dataValueContainer.Type == ArrowTypeId.Struct)
                {
                    var structValue = _dataValueContainer.AsStruct;
                    var sum = structValue.GetAt(0);
                    var count = structValue.GetAt(1).AsLong;
                    if (count > 0 && sum.Type != ArrowTypeId.Null)
                    {
                        var avg = AverageWindowUtils.DivideWithCount(sum, count);
                        outputColumn.Add(avg);
                    }
                    else
                    {
                        outputColumn.Add(NullValue.Instance);
                    }
                }
                else
                {
                    outputColumn.Add(NullValue.Instance);
                }
            }
            return ValueTask.CompletedTask;
        }

        public Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return Task.CompletedTask;
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
        }

        public ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, EventBatchData incoming, ReadOnlySpan<int> sortedByGroupIndices)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }
    }
}
