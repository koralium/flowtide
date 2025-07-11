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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using System.Linq.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.SurrogateKey
{
    internal class SurrogateKeyInt64AggregationSingleton
    {
        public SurrogateKeyInt64AggregationSingleton(IObjectState<long> state)
        {
            State = state;
        }

        public IObjectState<long> State { get; }
    }

    internal class SurrogateKeyInt64Aggregation
    {
        private static async Task<SurrogateKeyInt64AggregationSingleton> Initialize(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            var counter = await stateManagerClient.GetOrCreateObjectStateAsync<long>("counter");
            return new SurrogateKeyInt64AggregationSingleton(counter);
        }

        private static Expression SurrogateKeyMapFunction(
           Substrait.Expressions.AggregateFunction function,
           ColumnParameterInfo parametersInfo,
           ColumnarExpressionVisitor visitor,
           ParameterExpression stateParameter,
           ParameterExpression weightParameter,
           ParameterExpression singletonAccess,
           ParameterExpression groupingKeyParameter)
        {
            if (function.Arguments.Count != 0)
            {
                throw new InvalidOperationException("surrogate_key_int64 must have zero arguments.");
            }

            var expr = GetSurrogateKeyBody();
            var body = expr.Body;
            var e = body;
            var replacer = new ParameterReplacerVisitor(expr.Parameters[0], stateParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[1], weightParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[2], singletonAccess);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[3], groupingKeyParameter);
            e = replacer.Visit(e);
            return e;
        }

        private static LambdaExpression GetSurrogateKeyBody()
        {
            var methodInfo = typeof(SurrogateKeyInt64Aggregation).GetMethod(nameof(DoSurrogateKeyInt64), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

            var state = Expression.Parameter(typeof(ColumnReference), "state");
            var weight = Expression.Parameter(typeof(long), "weight");
            var singleton = Expression.Parameter(typeof(SurrogateKeyInt64AggregationSingleton), "singleton");
            var groupingKey = Expression.Parameter(typeof(ColumnRowReference), "groupingKey");

            var call = Expression.Call(methodInfo, state, weight, singleton, groupingKey);
            return Expression.Lambda(call, state, weight, singleton, groupingKey);
        }

        private static ValueTask DoSurrogateKeyInt64(ColumnReference currentState, long weight, SurrogateKeyInt64AggregationSingleton singleton, ColumnRowReference groupingKey)
        {
            var stateValue = currentState.GetValue();

            if (stateValue.IsNull)
            {
                var nextId = singleton.State.Value++;
                currentState.Update(new Int64Value(nextId));
            }
            return ValueTask.CompletedTask;
        }

        private static ValueTask SurrogateKeyGetValue(ColumnReference state, ColumnRowReference groupingKey, SurrogateKeyInt64AggregationSingleton singleton, Column outputColumn)
        {
            outputColumn.Add(state.GetValue());
            return ValueTask.CompletedTask;
        }

        private static async Task Commit(SurrogateKeyInt64AggregationSingleton singleton)
        {
            await singleton.State.Commit();
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction(
                FunctionsAggregateGeneric.Uri,
                FunctionsAggregateGeneric.SurrogateKeyInt64,
                Initialize,
                (singleton) => { },
                Commit,
                SurrogateKeyMapFunction,
                SurrogateKeyGetValue
                );
        }
    }
}
