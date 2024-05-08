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

using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class TableFunctionCompiler
    {
        public static Func<RowEvent, IEnumerable<RowEvent>> CompileWithArg(TableFunction tableFunction, IFunctionsRegister functionsRegister)
        {
            if (!functionsRegister.TryGetTableFunction(tableFunction.ExtensionUri, tableFunction.ExtensionName, out var tableFunctionFactory))
            {
                throw new InvalidOperationException($"Table function {tableFunction.ExtensionUri}.{tableFunction.ExtensionName} not found.");
            }
            var param = System.Linq.Expressions.Expression.Parameter(typeof(RowEvent));
            var parameterInfo = new ParametersInfo(new List<ParameterExpression>() { param }, new List<int> { 0 });

            var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(RowEvent));

            var expr = tableFunctionFactory.MapFunc(tableFunction, parameterInfo, visitor);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<RowEvent, IEnumerable<RowEvent>>>(expr, param);
            return lambda.Compile();
        }
    }
}
