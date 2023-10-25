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
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class ProjectCompiler
    {
        public static Func<StreamEvent, FlxValue> Compile(Expression expression, FunctionsRegister functionsRegister)
        {
            var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(StreamEvent)); //new ProjectVisitor();

            var param = System.Linq.Expressions.Expression.Parameter(typeof(StreamEvent));
            var expr = visitor.Visit(expression, new ParametersInfo(new List<System.Linq.Expressions.ParameterExpression> { param }, new List<int>() { 0 }));
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<StreamEvent, FlxValue>>(expr, param);
            return lambda.Compile();
        }
    }
}
