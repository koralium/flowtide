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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Substrait.Protobuf.Expression.Types;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateless
{
    internal class SumAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new SumAggregation(compiledValue);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum, new SumAggregationDefinition());
        }
    }

    internal class SumAggregation : IColumnBulkAggregation
    {
        // sum keeps a non-null contributor count alongside the running sum so it can revert to
        // null once every non-null contributor has been retracted. A plain numeric state cannot
        // distinguish "no contributors" from "contributors that happen to sum to 0", which made the
        // all-null result history-dependent (null if never valued, 0 if set then retracted). SQL SUM
        // is null for an all-null group; sum0 is the always-0 variant.
        private static readonly StructHeader SumStateHeader = StructHeader.Create("sum", "count");

        private readonly Func<EventBatchData, int, IDataValue> projectionFunction;
        private readonly DataValueContainer _dataValueContainer;
        private readonly DataValueContainer _sumContainer;

        public SumAggregation(Func<EventBatchData, int, IDataValue> projectionFunction)
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
                    DoSum(value, _sumContainer, weight);
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
                var structVal = new StructValue(SumStateHeader, _sumContainer, countVal);
                _dataValueContainer._structValue = structVal;
                _dataValueContainer._type = ArrowTypeId.Struct;
            }

            groupState.Update(_dataValueContainer);
            return true;
        }

        internal static void DoSum<T>(T value, DataValueContainer currentState, long weight)
            where T : IDataValue
        {
            if (currentState.Type == ArrowTypeId.Int64)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsLong + (value.AsLong * weight);
                    currentState._type = ArrowTypeId.Int64;
                    currentState._int64Value = new Int64Value(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var floatCount = currentState.AsLong + (value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(floatCount);
                }
                else if (value.Type == ArrowTypeId.Decimal128)
                {
                    var count = currentState.AsLong + (value.AsDecimal * weight);
                    currentState._type = ArrowTypeId.Decimal128;
                    currentState._decimalValue = new DecimalValue(count);
                }
            }
            else if (currentState.Type == ArrowTypeId.Double)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsDouble + ((double)value.AsLong * (double)weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = currentState.AsDouble + (value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(count);
                }
                else if (value.Type == ArrowTypeId.Decimal128)
                {
                    var count = (decimal)currentState.AsDouble + (value.AsDecimal * weight);
                    currentState._type = ArrowTypeId.Decimal128;
                    currentState._decimalValue = new DecimalValue(count);
                }
            }
            else if (currentState.Type == ArrowTypeId.Decimal128)
            {
                if (value.Type == ArrowTypeId.Decimal128)
                {
                    var count = currentState.AsDecimal + (value.AsDecimal * weight);
                    currentState._type = ArrowTypeId.Decimal128;
                    currentState._decimalValue = new DecimalValue(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = currentState.AsDecimal + (decimal)(value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Decimal128;
                    currentState._decimalValue = new DecimalValue(count);
                }
                else if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsDecimal + ((decimal)value.AsLong * weight);
                    currentState._type = ArrowTypeId.Decimal128;
                    currentState._decimalValue = new DecimalValue(count);
                }
            }
            else if (currentState.Type == ArrowTypeId.Null)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = (value.AsLong * weight);
                    currentState._type = ArrowTypeId.Int64;
                    currentState._int64Value = new Int64Value(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = (value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(count);
                }
                else if (value.Type == ArrowTypeId.Decimal128)
                {
                    var count = (value.AsDecimal * weight);
                    currentState._type = ArrowTypeId.Decimal128;
                    currentState._decimalValue = new DecimalValue(count);
                }
            }
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = startIndex; i < startIndex + length; i++)
            {
                groupStates[i].GetValue(_dataValueContainer);
                if (_dataValueContainer.Type == ArrowTypeId.Struct)
                {
                    var structValue = _dataValueContainer.AsStruct;
                    structValue.GetAt(0, _sumContainer);
                    var count = structValue.GetAt(1).AsLong;
                    if (count > 0 && _sumContainer.Type != ArrowTypeId.Null)
                    {
                        outputColumn.Add(_sumContainer);
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
