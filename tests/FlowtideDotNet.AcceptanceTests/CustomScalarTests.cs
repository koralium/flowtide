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
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests
{
    public class CustomScalarTests : FlowtideAcceptanceBase
    {
        [Fact]
        public async Task CustomStaticScalar()
        {
            SqlFunctionRegister.RegisterScalarFunction("static", 
                (func, visitor, emitData) =>
                {
                    List<Substrait.Expressions.Expression> args = new List<Substrait.Expressions.Expression>();
                    return new ScalarFunction()
                    {
                        ExtensionUri = "/custom.yaml",
                        ExtensionName = "static",
                        Arguments = args
                    };
                });

            FunctionsRegister.RegisterScalarFunction("/custom.yaml", "static",
                (func, parameterInfo, visitor) =>
                {
                    return System.Linq.Expressions.Expression.Constant(FlxValue.FromMemory(FlexBuffer.SingleValue("staticvalue")));
                });

            GenerateData();
            await StartStream("INSERT INTO output SELECT static() as v FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { v = "staticvalue" }));
        }

        [Fact]
        public async Task CustomOneParameterWithExpression()
        {
            SqlFunctionRegister.RegisterScalarFunction("addnumbers",
                (func, visitor, emitData) =>
                {
                    if (func.Args?.Count != 1)
                    {
                        throw new ArgumentException("Addnumbers must have 1 parameter");
                    }
                    List<Substrait.Expressions.Expression> args = new List<Substrait.Expressions.Expression>();
                    if (func.Args[0] is FunctionArg.Unnamed unnamedArg)
                    {
                        if (unnamedArg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            args.Add(visitor.Visit(funcExpr.Expression, emitData).Expr);
                        }
                        else
                        {
                            throw new ArgumentException("Wildcard is not supported in addnumbers");
                        }
                    }
                    else
                    {
                        throw new ArgumentException("Named arguments is not supported for addnumbers");
                    }
                    
                    return new ScalarFunction()
                    {
                        ExtensionUri = "/custom.yaml",
                        ExtensionName = "addnumbers",
                        Arguments = args
                    };
                });

            FunctionsRegister.RegisterScalarFunctionWithExpression("/custom.yaml", "addnumbers",
                (p1) => FlxValue.FromMemory(FlexBuffer.SingleValue(p1.AsString + "123"))
            );

            GenerateData();
            await StartStream("INSERT INTO output SELECT addnumbers(firstName) as v FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { v = x.FirstName + "123" }));
        }
    }
}
