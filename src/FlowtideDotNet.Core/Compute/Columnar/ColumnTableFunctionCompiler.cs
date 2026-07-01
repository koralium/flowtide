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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    /// <summary>
    /// A compiled table function. For input row <c>index</c> in <c>batch</c> it appends the
    /// produced rows directly into <c>output</c> instead of allocating a batch per row.
    /// </summary>
    public delegate void TableFunctionEmit(EventBatchData batch, int index, ITableFunctionOutput output);

    public record TableFunctionCompileResult(TableFunctionEmit Emit);
    internal static class ColumnTableFunctionCompiler
    {
        public static TableFunctionCompileResult CompileWithArg(
            TableFunction tableFunction,
            IFunctionsRegister functionsRegister,
            IMemoryAllocator memoryAllocator)
        {
            if (!functionsRegister.TryGetColumnTableFunction(tableFunction.ExtensionUri, tableFunction.ExtensionName, out var tableFunctionFactory))
            {
                throw new InvalidOperationException($"Table function {tableFunction.ExtensionUri}.{tableFunction.ExtensionName} not found.");
            }
            var param = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            var indexParam = System.Linq.Expressions.Expression.Parameter(typeof(int));
            var outputParam = System.Linq.Expressions.Expression.Parameter(typeof(ITableFunctionOutput));
            var resultContainer = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
            var parameterInfo = new ColumnParameterInfo(new List<ParameterExpression>() { param }, new List<ParameterExpression>() { indexParam }, new List<int> { 0 }, resultContainer);
            var visitor = new ColumnarExpressionVisitor(functionsRegister);

            var tableFunctionResult = tableFunctionFactory.MapFunc(tableFunction, parameterInfo, visitor, memoryAllocator, outputParam);
            var lambda = System.Linq.Expressions.Expression.Lambda<TableFunctionEmit>(tableFunctionResult.Expression, param, indexParam, outputParam);
            return new TableFunctionCompileResult(lambda.Compile());
        }
    }
}
