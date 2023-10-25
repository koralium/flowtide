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

using FlowtideDotNet.Substrait.FunctionExtensions;
using SqlParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInBooleanFunctions
    {
        public static void AddBooleanFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.And,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    var expr = visitor.Visit(scalarFunction.Arguments.First(), parametersInfo);

                    for (int i = 1; i < scalarFunction.Arguments.Count; i++)
                    {
                        var resolved = visitor.Visit(scalarFunction.Arguments[i], parametersInfo);
                        if (resolved == null)
                        {
                            throw new Exception();
                        }
                        expr = System.Linq.Expressions.Expression.AndAlso(expr!, resolved!);
                    }

                    return expr;
                });
            functionsRegister.RegisterScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.Or,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    var expr = visitor.Visit(scalarFunction.Arguments.First(), parametersInfo);

                    for (int i = 1; i < scalarFunction.Arguments.Count; i++)
                    {
                        var resolved = visitor.Visit(scalarFunction.Arguments[i], parametersInfo);
                        if (resolved == null)
                        {
                            throw new Exception();
                        }
                        expr = System.Linq.Expressions.Expression.OrElse(expr!, resolved!);
                    }

                    return expr;
                });
        }
    }
}
