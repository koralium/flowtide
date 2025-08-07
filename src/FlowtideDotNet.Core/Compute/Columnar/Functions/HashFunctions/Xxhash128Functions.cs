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
using System.IO.Hashing;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.HashFunctions
{
    internal static class XxHash128Functions
    {
        public static void AddXxHash128Functions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsHash.Uri, FunctionsHash.XxHash128GuidString,
                (func, paramInfo, visitor, functionServices) =>
                {
                    if (func.Arguments.Count < 1)
                    {
                        throw new ArgumentException($"Function {FunctionsHash.XxHash128GuidString} expects atleast one argument.");
                    }

                    var hashInstanceExpr = Expression.Constant(new XxHash128());
                    var destinationExpr = Expression.Constant(new byte[16]);
                    var utf8DestinationExpr = Expression.Constant(new byte[36]);
                    var outputExpr = Expression.Constant(new DataValueContainer());

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

                    var spanVariable = Expression.Variable(typeof(Span<byte>), "span");
                    var destAsSpan = Expression.New(typeof(Span<byte>).GetConstructor([typeof(byte[])])!, destinationExpr);
                    var assignToSpanExpr = Expression.Assign(spanVariable, destAsSpan);

                    hashExpressions.Add(assignToSpanExpr);

                    var hashMethod = typeof(XxHash128).GetMethod("GetHashAndReset", [typeof(Span<byte>)]);

                    if (hashMethod == null)
                    {
                        throw new InvalidOperationException("Method GetHashAndReset not found on XxHash128.");
                    }

                    // Call GetHashAndReset
                    var callGetHash = Expression.Call(hashInstanceExpr, hashMethod, spanVariable);
                    hashExpressions.Add(callGetHash);

                    var guidCtor = Expression.New(typeof(Guid).GetConstructor([typeof(byte[])])!, destinationExpr);

                    // Get the try format method for guid to take it to a string
                    var tryFormatMethod = guidCtor.Type.GetMethod("TryFormat", [typeof(Span<byte>), typeof(int).MakeByRefType(), typeof(ReadOnlySpan<char>)]);

                    if (tryFormatMethod == null)
                    {
                        throw new InvalidOperationException("Method TryFormat not found on Guid.");
                    }

                    var outBytesWrittenExpr = Expression.Variable(typeof(int), "bytesWritten");

                    var readonlySpanChar = Expression.Default(typeof(ReadOnlySpan<char>));

                    // Call TryFormat to convert the Guid to a string
                    var tryFormatCall = Expression.Call(
                        guidCtor,
                        tryFormatMethod,
                        Expression.New(typeof(Span<byte>).GetConstructor([typeof(byte[])])!, utf8DestinationExpr),
                        outBytesWrittenExpr,
                        readonlySpanChar
                        ); 

                    var newStringValue = Expression.New(typeof(StringValue).GetConstructor([typeof(byte[])])!, utf8DestinationExpr);

                    hashExpressions.Add(tryFormatCall);
                    var convertToIDataValue = Expression.Convert(newStringValue, typeof(IDataValue));
                    hashExpressions.Add(convertToIDataValue);
                    var blockExpr = Expression.Block(typeof(IDataValue), new List<ParameterExpression>() { spanVariable, outBytesWrittenExpr }, hashExpressions);

                    return blockExpr;
                });
        }
    }
}
