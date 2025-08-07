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
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.HashFunctions
{
    internal static class XxHash128Functions
    {
        public static void AddXxHash128Functions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsHash.Uri, FunctionsHash.XxHash128GuidString,
                (func, paramInfo, visitor, functionServices) =>
                {
                    if (func.Arguments.Count != 1)
                    {
                        throw new ArgumentException($"Function {FunctionsHash.XxHash128GuidString} expects exactly one argument.");
                    }
                    var arg = func.Arguments[0];
                    var argExpr = visitor.Visit(arg, paramInfo);

                    if (argExpr == null)
                    {
                        throw new ArgumentException($"Argument {arg} cannot be null.");
                    }

                    var methodInfo = typeof(XxHash128Functions).GetMethod(nameof(XxHash128GuidStringMethod), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
                    if (methodInfo == null)
                    {
                        throw new InvalidOperationException(
                            $"Method {nameof(XxHash128GuidStringMethod)} not found in {nameof(XxHash128Functions)}.");
                    }
                    var shakeInstanceExpr = System.Linq.Expressions.Expression.Constant(new XxHash128());
                    var destinationExpr = System.Linq.Expressions.Expression.Constant(new byte[16]);
                    var utf8DestinationExpr = System.Linq.Expressions.Expression.Constant(new byte[36]);
                    var outputExpr = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
                    var genericMethod = methodInfo.MakeGenericMethod(argExpr.Type);
                    return System.Linq.Expressions.Expression.Call(
                        genericMethod,
                        argExpr,
                        shakeInstanceExpr,
                        destinationExpr,
                        utf8DestinationExpr,
                        outputExpr);
                });
        }

        private static IDataValue XxHash128GuidStringMethod<T>(T value, XxHash128 hashInstance, byte[] hashDestination, byte[] utf8Destination, DataValueContainer output)
            where T : IDataValue
        {
            value.AddToHash(hashInstance);
            hashInstance.GetHashAndReset(hashDestination);

            new Guid(hashDestination).TryFormat(utf8Destination, out int bytesWritten);

            output._type = ArrowTypeId.String;
            output._stringValue = new StringValue(utf8Destination);
            return output;
        }
    }
}
