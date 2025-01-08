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

using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal static class MeasureCompiler
    {
        public static Task<IAggregateContainer> CompileMeasure(int groupingLength, IStateManagerClient stateManagerClient, AggregateFunction aggregateFunction, FunctionsRegister functionsRegister, IMemoryAllocator memoryAllocator)
        {
            if (functionsRegister.TryGetAggregateFunction(aggregateFunction.ExtensionUri, aggregateFunction.ExtensionName, out var definition))
            {
                var param = System.Linq.Expressions.Expression.Parameter(typeof(RowEvent));
                var stateParam = System.Linq.Expressions.Expression.Parameter(typeof(byte[]));
                var weightParam = System.Linq.Expressions.Expression.Parameter(typeof(long));
                var groupingKeyParameter = System.Linq.Expressions.Expression.Parameter(typeof(RowEvent));
                var parametersInfo = new ParametersInfo(new List<ParameterExpression>() { param }, new List<int> { 0 });
                var expressionVisitor = new FlowtideExpressionVisitor(functionsRegister, typeof(RowEvent));
                var container =  definition.CreateContainer(
                    groupingLength,
                    stateManagerClient,
                    memoryAllocator,
                    aggregateFunction,
                    parametersInfo,
                    expressionVisitor,
                    param,
                    stateParam,
                    weightParam,
                    groupingKeyParameter);

                return container;
            }
            else
            {
                throw new InvalidOperationException($"The function {aggregateFunction.ExtensionUri}:{aggregateFunction.ExtensionName} is not defined.");
            }
        }
    }
}
