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
using FlowtideDotNet.Substrait.Exceptions;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Type;
using SqlParser.Ast;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class CustomScalarTests : FlowtideAcceptanceBase
    {
        public CustomScalarTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task CustomStaticScalar()
        {
            SqlFunctionRegister.RegisterScalarFunction("static", 
                (func, options, visitor, emitData) =>
                {
                    List<Substrait.Expressions.Expression> args = new List<Substrait.Expressions.Expression>();
                    return new Substrait.Sql.ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = "/custom.yaml",
                            ExtensionName = "static",
                            Arguments = args
                        },
                        new AnyType()
                        );
                });

            FunctionsRegister.RegisterScalarFunction("/custom.yaml", "static",
                (func, arameterInfo, visitor) =>
                {
                    return System.Linq.Expressions.Expression.Constant(FlxValue.FromMemory(FlexBuffer.SingleValue("staticvalue")));
                });

            GenerateData();
            await StartStream("INSERT INTO output SELECT static() as v FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { v = "staticvalue" }));
        }

        internal static FunctionArgumentList GetFunctionArguments(FunctionArguments arguments)
        {
            if (arguments is FunctionArguments.List listArguments)
            {
                return listArguments.ArgumentList;
            }
            if (arguments is FunctionArguments.None noneArguments)
            {
                return new FunctionArgumentList(new SqlParser.Sequence<FunctionArg>());
            }
            if (arguments is FunctionArguments.Subquery subQueryArgument)
            {
                throw new SubstraitParseException("Subquery is not supported as an argument");
            }
            else
            {
                throw new SubstraitParseException("Unknown function argument type");
            }
        }

        [Fact]
        public async Task CustomOneParameterWithExpression()
        {
            SqlFunctionRegister.RegisterScalarFunction("addnumbers",
                (func, options, visitor, emitData) =>
                {
                    var argList = GetFunctionArguments(func.Args);
                    if (argList.Args?.Count != 1)
                    {
                        throw new ArgumentException("Addnumbers must have 1 parameter");
                    }
                    List<Substrait.Expressions.Expression> args = new List<Substrait.Expressions.Expression>();
                    if (argList.Args[0] is FunctionArg.Unnamed unnamedArg)
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

                    return new Substrait.Sql.ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = "/custom.yaml",
                            ExtensionName = "addnumbers",
                            Arguments = args
                        },
                        new AnyType()
                        );
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
