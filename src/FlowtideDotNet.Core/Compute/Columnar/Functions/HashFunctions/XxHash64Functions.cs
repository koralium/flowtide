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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.HashFunctions
{
    internal static class XxHash64Functions
    {
        public static void AddXxHash64Functions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsHash.Uri, FunctionsHash.XxHash64,
                (func, paramInfo, visitor, functionServices) =>
                {
                    if (func.Arguments.Count < 1)
                    {
                        throw new ArgumentException($"Function {FunctionsHash.XxHash64} expects at least one argument.");
                    }

                    var hashInstanceExpr = Expression.Constant(new XxHash64());

                    List<Expression> hashExpressions = new List<Expression>();
                    for (int i = 0; i < func.Arguments.Count; i++)
                    {
                        var arg = func.Arguments[i];
                        var argExpr = visitor.Visit(arg, paramInfo);

                        if (argExpr == null)
                        {
                            throw new ArgumentException($"Argument {arg} cannot be null.");
                        }

                        var addToHashMethod = argExpr.Type.GetMethod("AddToHash", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                        if (addToHashMethod == null)
                        {
                            throw new InvalidOperationException($"Method AddToHash not found on type {argExpr.Type}.");
                        }

                        hashExpressions.Add(Expression.Call(argExpr, addToHashMethod, hashInstanceExpr));
                    }


                    var hashMethod = typeof(XxHash64).GetMethod("GetCurrentHashAsUInt64", []);

                    if (hashMethod == null)
                    {
                        throw new InvalidOperationException("Method GetCurrentHashAsUInt64 not found on XxHash64.");
                    }

                    var longVariable = Expression.Variable(typeof(long), "result");

                    // Call GetHashAndReset
                    var callGetHash = Expression.Call(hashInstanceExpr, hashMethod);
                    var castToLong = Expression.Convert(callGetHash, typeof(long));
                    var assignToResult = Expression.Assign(longVariable, castToLong);
                    hashExpressions.Add(assignToResult);

                    var resetMethod = typeof(XxHash64).GetMethod("Reset", []);

                    if (resetMethod == null)
                    {
                        throw new InvalidOperationException("Method Reset not found on XxHash64.");
                    }

                    // Call Reset
                    var callReset = Expression.Call(hashInstanceExpr, resetMethod);
                    hashExpressions.Add(callReset);


                    var newInt64Value = Expression.New(typeof(Int64Value).GetConstructor([typeof(long)])!, longVariable);

                    var convertToIDataValue = Expression.Convert(newInt64Value, typeof(IDataValue));
                    hashExpressions.Add(convertToIDataValue);
                    var blockExpr = Expression.Block(typeof(IDataValue), new List<ParameterExpression>() { longVariable }, hashExpressions);

                    return blockExpr;
                });
        }
    }
}
