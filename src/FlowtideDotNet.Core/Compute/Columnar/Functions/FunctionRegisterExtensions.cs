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
using System.Diagnostics;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class FunctionRegisterExtensions
    {
        private record MethodDetails(MethodInfo methodInfo, ParameterInfo[] parameters, List<Type> genericArguments, string? option, string? optionValue);
        private static void RegisterMethod(string extensionUri, string extensionName, IFunctionsRegister functionRegister, string methodName, MethodInfo[] methods)
        {
            Dictionary<int, List<MethodDetails>> methodDetails = new Dictionary<int, List<MethodDetails>>();

            for (int i = 0; i < methods.Length; i++)
            {
                string? optionName = null;
                string? optionValue = null;
                if (methods[i].Name != methodName)
                {
                    var option = methods[i].Name.Substring(methodName.Length);
                    if (!option.StartsWith("_"))
                    {
                        continue;
                    }
                    option = option.Substring(1);
                    var split = option.Split("__");
                    optionName = split[0];
                    optionValue = split.Length > 1 ? split[1] : null;
                }
                var methodParameters = methods[i].GetParameters();
                var genericArguments = methods[i].GetGenericArguments().ToList();
                if (!methodDetails.TryGetValue(genericArguments.Count, out var list))
                {
                    list = new List<MethodDetails>();
                    methodDetails.Add(genericArguments.Count, list);
                }
                list.Add(new MethodDetails(methods[i], methodParameters, genericArguments, optionName, optionValue));
            }


            functionRegister.RegisterColumnScalarFunction(extensionUri, extensionName, (func, paramInfo, visitor, functionServices) =>
            {
                if (!methodDetails.TryGetValue(func.Arguments.Count, out var methodsInformation))
                {
                    throw new InvalidOperationException("Generic argument count does not match function argument count");
                }

                MethodDetails? methodInformation = null;
                if (func.Options != null && func.Options.Count > 0)
                {
                    if (func.Options.Count > 1)
                    {
                        throw new NotSupportedException("Only one option is supported at this time.");
                    }

                    methodInformation = methodsInformation.FirstOrDefault(x => x.option == func.Options.Keys[0] && x.optionValue == func.Options.Values[0]);
                }
                if (methodInformation == null)
                {
                    // Default take the first one
                    methodInformation = methodsInformation.FirstOrDefault();
                    if (methodInformation == null)
                    {
                        throw new InvalidOperationException("Method not found");
                    }
                }
                var genericArguments = methodInformation.genericArguments;
                var methodParameters = methodInformation.parameters;
                var method = methodInformation.methodInfo;
                System.Type[] genericTypes = new System.Type[genericArguments.Count];
                System.Linq.Expressions.Expression[] parameters = new System.Linq.Expressions.Expression[methodParameters.Length];
                for (int i = 0; i < func.Arguments.Count; i++)
                {
                    var arg = func.Arguments[i];
                    var expr = visitor.Visit(arg, paramInfo);
                    parameters[i] = expr!;
                    var paramType = methodParameters[i].ParameterType;

                    var argIndex = genericArguments.IndexOf(paramType);
                    if (argIndex >= 0)
                    {
                        genericTypes[argIndex] = expr!.Type;
                    }
                    else if (paramType.ContainsGenericParameters)
                    {
                        var eleType = paramType.GetElementType();

                        if (eleType == null)
                        {
                            throw new InvalidOperationException("Element type is null");
                        }
                        if (eleType.IsGenericParameter)
                        {
                            var genericArgumentIndex = genericArguments.IndexOf(eleType);
                            genericTypes[genericArgumentIndex] = expr!.Type;
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
            var methods = classType.GetMethods(BindingFlags.NonPublic | BindingFlags.Static).Where(x => x.Name.StartsWith(methodName)).ToArray();
            Debug.Assert(methods != null, "Method not found");
            RegisterMethod(extensionUri, extensionName, functionsRegister, methodName, methods);
        }
    }
}
