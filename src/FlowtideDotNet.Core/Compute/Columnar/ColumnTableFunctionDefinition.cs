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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    public class TableFunctionResult
    {
        /// <summary>
        /// A void-returning expression that appends the produced rows for a single input
        /// row into the <see cref="ITableFunctionOutput"/> passed to the compiled function.
        /// </summary>
        public System.Linq.Expressions.Expression Expression { get; }
        public TableFunctionResult(System.Linq.Expressions.Expression expression)
        {
            Expression = expression;
        }
    }


    public class ColumnTableFunctionDefinition
    {
        public ColumnTableFunctionDefinition(
            string uri,
            string name,
            Func<TableFunction, ColumnParameterInfo, ColumnarExpressionVisitor, IMemoryAllocator, ParameterExpression, TableFunctionResult> mapFunc)
        {
            Uri = uri;
            Name = name;
            MapFunc = mapFunc;
        }

        public string Uri { get; }

        public string Name { get; }

        /// <summary>
        /// Builds the append expression for the function. The last parameter is the
        /// <see cref="ITableFunctionOutput"/> parameter expression that the produced rows
        /// should be written into.
        /// </summary>
        public Func<TableFunction, ColumnParameterInfo, ColumnarExpressionVisitor, IMemoryAllocator, ParameterExpression, TableFunctionResult> MapFunc { get; }
    }
}
