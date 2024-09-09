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

using FlexBuffers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StreamingAggregations
{
    internal static class BuiltInGenericFunctions
    {
        public static void AddBuiltInAggregateGenericFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStreamingColumnAggregateFunction(
                FunctionsAggregateGeneric.Uri,
                FunctionsAggregateGeneric.Count,
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments != null && aggregateFunction.Arguments.Count != 0)
                    {
                        throw new ArgumentException("Count function does not take any arguments");
                    }
                    var expr = GetCountBody();
                    var body = expr.Body;
                    var replacer = new ParameterReplacerVisitor(expr.Parameters[0], parametersInfo.BatchParameters[0]);
                    Expression e = replacer.Visit(body);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[1], parametersInfo.IndexParameters[0]);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[2], stateParameter);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[3], weightParameter);
                    e = replacer.Visit(e);
                    return e;
                },
                GetCountValue
                );
        }

        private static Expression<Action<EventBatchData, int, ColumnReference, long>> GetCountBody()
        {
            return (batch, index, bytes, weight) => DoCount(batch, index, bytes, weight);
        }

        private static void DoCount(EventBatchData data, int index, ColumnReference state, long weight)
        {
            var currentState = state.column.GetValueAt(state.index, default);
            long count = 0;
            if (currentState.Type == ArrowTypeId.Int64)
            {
                count = currentState.AsLong;
            }
            if (weight < 0)
            {

            }
            var newCount = count + weight;
            state.column.UpdateAt(state.index, new Int64Value(newCount));
        }

        private static void GetCountValue(ColumnReference state, Column outputColumn)
        {
            var stateValue = state.column.GetValueAt(state.index, default);
            if (stateValue.Type == ArrowTypeId.Null)
            {
                outputColumn.Add(new Int64Value(0));
                return;
            }
            outputColumn.Add(stateValue);
        }
    }
}
