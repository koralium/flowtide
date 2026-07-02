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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateless
{
    internal class Sum0AggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new Sum0Aggregation(compiledValue, aggregateFunction.OutputType);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum0, new Sum0AggregationDefinition());
        }
    }

    internal class Sum0Aggregation : IColumnBulkAggregation
    {
        private readonly Func<EventBatchData, int, IDataValue> projectionFunction;
        private readonly DataValueContainer _dataValueContainer;
        // The zero emitted for an empty/all-null group, typed to the declared return type so it matches the
        // type of valued groups (a never-valued group has no value to infer a type from). When the type is
        // unknown (AnyType / not resolved) this falls back to Double 0.0 - the only case where a mixed type
        // can still occur, and one where there is no concrete type contract to honour anyway.
        private readonly IDataValue _zero;

        public Sum0Aggregation(Func<EventBatchData, int, IDataValue> projectionFunction, SubstraitBaseType? outputType)
        {
            this.projectionFunction = projectionFunction;
            _dataValueContainer = new DataValueContainer();
            _zero = ZeroForType(outputType);
        }

        private static IDataValue ZeroForType(SubstraitBaseType? outputType)
        {
            return outputType?.Type switch
            {
                SubstraitType.Int64 or SubstraitType.Int32 => new Int64Value(0),
                SubstraitType.Fp64 or SubstraitType.Fp32 => new DoubleValue(0.0),
                SubstraitType.Decimal => new DecimalValue(0m),
                _ => new DoubleValue(0.0)
            };
        }

        public Task CommitAsync()
        {
            return Task.CompletedTask;
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            groupState.GetValue(_dataValueContainer);
            for (int i = 0; i < indices.Length; i++)
            {
                var value = projectionFunction(data, indices[i]);
                SumAggregation.DoSum(value, _dataValueContainer, weights[indices[i]]);
            }
            groupState.Update(_dataValueContainer);
            return true;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = startIndex; i < startIndex + length; i++)
            {
                groupStates[i].GetValue(_dataValueContainer);
                if (_dataValueContainer.Type == ArrowTypeId.Null)
                {
                    outputColumn.Add(_zero);
                }
                else
                {
                    outputColumn.Add(_dataValueContainer);
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
