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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class FunctionRegisterExtensions
    {
        private static void RegisterMethod(string extensionUri, string extensionName, IFunctionsRegister functionRegister, MethodInfo method)
        {
            var methodParameters = method.GetParameters();
            var genericArguments = method.GetGenericArguments().ToList();

            functionRegister.RegisterColumnScalarFunction(extensionUri, extensionName, (func, paramInfo, visitor) =>
            {
                if (genericArguments.Count != func.Arguments.Count)
                {
                    throw new InvalidOperationException("Generic argument count does not match function argument count");
                }
                System.Type[] genericTypes = new System.Type[genericArguments.Count];
                System.Linq.Expressions.Expression[] parameters = new System.Linq.Expressions.Expression[methodParameters.Length];
                for (int i = 0; i < func.Arguments.Count; i++)
                {
                    var arg = func.Arguments[i];
                    var expr = visitor.Visit(arg, paramInfo);
                    parameters[i] = expr!;
                    var paramType = methodParameters[i].ParameterType;
                    if (paramType.ContainsGenericParameters)
                    {
                        var eleType = paramType.GetElementType();

                        if (eleType == null)
                        {
                            throw new InvalidOperationException("Element type is null");
                        }
                        if (eleType.IsGenericParameter)
                        {
                            var genericArgumentIndex = genericArguments.IndexOf(eleType);
                            genericTypes[genericArgumentIndex] = expr.Type;
                        }
                    }
                }

                if (parameters.Length - 1 == func.Arguments.Count)
                {
                    parameters[parameters.Length - 1] = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
                }

                var genericMethod = method.MakeGenericMethod(genericTypes);
                System.Linq.Expressions.Expression call = System.Linq.Expressions.Expression.Call(genericMethod, parameters);

                return call;
            });
        }

        public static void RegisterScalarMethod(this IFunctionsRegister functionsRegister, string extensionUri, string extensionName, System.Type classType, string methodName)
        {
            var method = classType.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
            RegisterMethod(extensionUri, extensionName, functionsRegister, method);
        }
    }
}
