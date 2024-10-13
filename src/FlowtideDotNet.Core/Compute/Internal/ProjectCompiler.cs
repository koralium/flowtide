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
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class ProjectCompiler
    {
        public static Func<RowEvent, FlxValue> Compile(Expression expression, FunctionsRegister functionsRegister)
        {
            var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(RowEvent)); //new ProjectVisitor();

            var param = System.Linq.Expressions.Expression.Parameter(typeof(RowEvent));
            var expr = visitor.Visit(expression, new ParametersInfo(new List<System.Linq.Expressions.ParameterExpression> { param }, new List<int>() { 0 }));
            if (expr == null)
            {
                throw new FlowtideException("Expression visitor did not return a result expression");
            }
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<RowEvent, FlxValue>>(expr, param);
            return lambda.Compile();
        }
    }
}
