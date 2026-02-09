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
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInLogarithmicFunctions
    {
        private enum OnLogZero
        {
            NAN,
            ERROR,
            MINUS_INFINITY
        }

        private enum OnDomainError
        {
            NONE,
            NAN,
            NULL,
            ERROR
        }

        private enum Rounding
        {
            NONE,
            TIE_TO_EVEN,
            TIE_AWAY_FROM_ZERO,
            TRUNCATE,
            CEILING,
            FLOOR
        }

        public static void AddBuiltInLogarithmicFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsLogarithmic.Uri, FunctionsLogarithmic.Log10, (func, paramInfo, visitor, functionServices) =>
            {
                var method = typeof(BuiltInLogarithmicFunctions).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).Where(x => x.Name.StartsWith(nameof(Log10Implementation))).First();

                var methodParameters = method.GetParameters();
                var genericArguments = method.GetGenericArguments().ToList();

                OnLogZero onLogZero = OnLogZero.MINUS_INFINITY;
                OnDomainError onDomainError = OnDomainError.NONE;
                Rounding rounding = Rounding.NONE;
                if (func.Options != null)
                {
                    if (func.Options.TryGetValue("on_log_zero", out var logZero))
                    {
                        if (!Enum.TryParse<OnLogZero>(logZero, out onLogZero))
                        {
                            throw new InvalidOperationException($"Invalid on_log_zero option value: {logZero}");
                        }
                    }
                    if (func.Options.TryGetValue("on_domain_error", out var domainError))
                    {
                        if (!Enum.TryParse<OnDomainError>(domainError, out onDomainError))
                        {
                            throw new InvalidOperationException($"Invalid on_domain_error option value: {domainError}");
                        }
                    }
                }

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


                parameters[1] = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
                parameters[2] = System.Linq.Expressions.Expression.Constant(rounding);
                parameters[3] = System.Linq.Expressions.Expression.Constant(onLogZero);
                parameters[4] = System.Linq.Expressions.Expression.Constant(onDomainError);

                var genericMethod = method.MakeGenericMethod(genericTypes);
                System.Linq.Expressions.Expression call = System.Linq.Expressions.Expression.Call(genericMethod, parameters);

                return call;
            });
        }

        private static IDataValue Log10Implementation<T1>(T1 x, DataValueContainer result, Rounding rounding, OnLogZero onLogZero, OnDomainError onDomainError)
            where T1 : IDataValue
        {
            if (x.Type == ArrowTypeId.Double)
            {
                result._type = ArrowTypeId.Double;

                var val = x.AsDouble;

                if (val == 0.0)
                {
                    if (onLogZero == OnLogZero.ERROR)
                    {
                        throw new InvalidOperationException("Log10 error: input value is zero");
                    }
                    else if (onLogZero == OnLogZero.NAN)
                    {
                        result._doubleValue = new DoubleValue(double.NaN);
                        return result;
                    }
                }

                var logResult = Math.Log10(val);

                if (double.IsNaN(logResult))
                {
                    if (onDomainError == OnDomainError.ERROR)
                    {
                        throw new InvalidOperationException("Log10 domain error: input value is negative");
                    }
                    else if (onDomainError == OnDomainError.NONE)
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                }
                else if (onLogZero == OnLogZero.MINUS_INFINITY && logResult == double.NegativeInfinity)
                {
                    // Handle log(0) case if needed
                }

                switch (rounding)
                {
                    case Rounding.CEILING:
                        logResult = Math.Ceiling(logResult);
                        break;
                    case Rounding.TRUNCATE:
                        logResult = Math.Truncate(logResult);
                        break;
                    case Rounding.FLOOR:
                        logResult = Math.Floor(logResult);
                        break;
                    case Rounding.TIE_TO_EVEN:
                        logResult = Math.Round(logResult, MidpointRounding.ToEven);
                        break;
                    case Rounding.TIE_AWAY_FROM_ZERO:
                        logResult = Math.Round(logResult, MidpointRounding.AwayFromZero);
                        break;
                }
                
                result._doubleValue = new DoubleValue(logResult);
                return result;
            }
            else if (x.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.Double;
                var val = x.AsLong;

                if (val == 0)
                {
                    if (onLogZero == OnLogZero.ERROR)
                    {
                        throw new InvalidOperationException("Log10 error: input value is zero");
                    }
                    else if (onLogZero == OnLogZero.NAN)
                    {
                        result._doubleValue = new DoubleValue(double.NaN);
                        return result;
                    }
                }

                var logResult = Math.Log10(val);

                if (double.IsNaN(logResult))
                {
                    if (onDomainError == OnDomainError.ERROR)
                    {
                        throw new InvalidOperationException("Log10 domain error: input value is negative");
                    }
                    else if (onDomainError == OnDomainError.NONE)
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                }

                switch (rounding)
                {
                    case Rounding.CEILING:
                        logResult = Math.Ceiling(logResult);
                        break;
                    case Rounding.TRUNCATE:
                        logResult = Math.Truncate(logResult);
                        break;
                    case Rounding.FLOOR:
                        logResult = Math.Floor(logResult);
                        break;
                    case Rounding.TIE_TO_EVEN:
                        logResult = Math.Round(logResult, MidpointRounding.ToEven);
                        break;
                    case Rounding.TIE_AWAY_FROM_ZERO:
                        logResult = Math.Round(logResult, MidpointRounding.AwayFromZero);
                        break;
                }

                result._doubleValue = new DoubleValue(logResult);
                return result;
            }
            else if (x.Type == ArrowTypeId.Decimal128)
            {
                result._type = ArrowTypeId.Decimal128;
                var val = x.AsDecimal;

                if (val == 0m)
                {
                    if (onLogZero == OnLogZero.ERROR)
                    {
                        throw new InvalidOperationException("Log10 error: input value is zero");
                    }
                    else if (onLogZero == OnLogZero.NAN)
                    {
                        result._type = ArrowTypeId.Double;
                        result._doubleValue = new DoubleValue(double.NaN); // NaN representation
                        return result;
                    }
                }

                var logResult = Math.Log10((double)val);

                if (double.IsNaN(logResult))
                {
                    if (onDomainError == OnDomainError.ERROR)
                    {
                        throw new InvalidOperationException("Log10 domain error: input value is negative");
                    }
                    else if (onDomainError == OnDomainError.NONE)
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                }

                switch (rounding)
                {
                    case Rounding.CEILING:
                        logResult = Math.Ceiling(logResult);
                        break;
                    case Rounding.TRUNCATE:
                        logResult = Math.Truncate(logResult);
                        break;
                    case Rounding.FLOOR:
                        logResult = Math.Floor(logResult);
                        break;
                    case Rounding.TIE_TO_EVEN:
                        logResult = Math.Round(logResult, MidpointRounding.ToEven);
                        break;
                    case Rounding.TIE_AWAY_FROM_ZERO:
                        logResult = Math.Round(logResult, MidpointRounding.AwayFromZero);
                        break;
                }

                // TODO: Fix potential precision loss
                result._decimalValue = new DecimalValue((decimal)logResult);
                return result;
            }
            result._type = ArrowTypeId.Null;
            return result;
        }
    }
}
