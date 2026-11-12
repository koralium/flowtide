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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using LinqExpression = System.Linq.Expressions.Expression;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInArithmeticFunctions
    {
        public static void AddBuiltInArithmeticFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Urn, FunctionsArithmetic.Add, typeof(BuiltInArithmeticFunctions), nameof(AddImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Urn, FunctionsArithmetic.Subtract, typeof(BuiltInArithmeticFunctions), nameof(SubtractImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Urn, FunctionsArithmetic.Negate, typeof(BuiltInArithmeticFunctions), nameof(NegateImplementation));
            foreach (var urn in new[] { FunctionsArithmetic.Urn, FunctionsArithmetic.DecimalUrn, FunctionsArithmetic.FlowtideUrn })
            {
                functionsRegister.RegisterColumnScalarFunction(urn, FunctionsArithmetic.Modulus, (function, parameters, visitor, _) =>
                {
                    ModulusOptions.Validate(function);
                    var left = visitor.Visit(function.Arguments[0], parameters.UpdateResultDataValue(LinqExpression.Constant(new DataValueContainer())))
                        ?? throw new InvalidOperationException("The first modulus argument could not be compiled.");
                    var right = visitor.Visit(function.Arguments[1], parameters.UpdateResultDataValue(LinqExpression.Constant(new DataValueContainer())))
                        ?? throw new InvalidOperationException("The second modulus argument could not be compiled.");
                    var method = typeof(BuiltInArithmeticFunctions).GetMethod(nameof(ModulusImplementation), BindingFlags.NonPublic | BindingFlags.Static)!;
                    return LinqExpression.Call(method.MakeGenericMethod(left.Type, right.Type), left, right,
                        LinqExpression.Constant(new DataValueContainer()));
                });
            }
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Urn, FunctionsArithmetic.Divide, typeof(BuiltInArithmeticFunctions), nameof(DivideImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Urn, FunctionsArithmetic.Multiply, typeof(BuiltInArithmeticFunctions), nameof(MultiplyImplementation));
        }

        private static IDataValue AddImplementation<T1, T2>(T1 x, T2 y, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x.Type == ArrowTypeId.Double)
            {
                // Float add float always return a float
                if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble + y.AsDouble);
                    return result;
                }
                // Float add int, will always return a float
                else if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble + y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)x.AsDouble + y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Int64)
            {
                // Int add int, will always return an int
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value(x.AsLong + y.AsLong);
                    return result;
                }
                // Int add float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsLong + y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsLong + y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Decimal128)
            {
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal + y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal + (decimal)y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal + y.AsDecimal);
                    return result;
                }
            }
            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue SubtractImplementation<T1, T2>(T1 x, T2 y, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x.Type == ArrowTypeId.Double)
            {
                // Float subtract float always return a float
                if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble - y.AsDouble);
                    return result;
                }
                // Float subtract int, will always return a float
                else if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble - y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)x.AsDouble - y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Int64)
            {
                // Int subtract int, will always return an int
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value(x.AsLong - y.AsLong);
                    return result;
                }
                // Int subtract float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsLong - y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsLong - y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Decimal128)
            {
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal - y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal - (decimal)y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal - y.AsDecimal);
                    return result;
                }
            }
            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue NegateImplementation<T>(T x, DataValueContainer result)
            where T : IDataValue
        {
            if (x.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.Int64;
                result._int64Value = new Int64Value(-x.AsLong);
                return result;
            }
            if (x.Type == ArrowTypeId.Double)
            {
                result._type = ArrowTypeId.Double;
                result._doubleValue = new DoubleValue(-x.AsDouble);
                return result;
            }
            if (x.Type == ArrowTypeId.Decimal128)
            {
                result._type = ArrowTypeId.Decimal128;
                result._decimalValue = new DecimalValue(-x.AsDecimal);
                return result;
            }
            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue ModulusImplementation<T1, T2>(T1 x, T2 y, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x.Type == ArrowTypeId.Double)
            {
                // Float add float always return a float
                if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble % y.AsDouble);
                    return result;
                }
                // Float add int, will always return a float
                else if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble % y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)x.AsDouble % y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Int64)
            {
                // Int add int, will always return an int
                if (y.Type == ArrowTypeId.Int64)
                {
                    if (y.AsLong == 0)
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value(x.AsLong % y.AsLong);
                    return result;
                }
                // Int add float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsLong % y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsLong % y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Decimal128)
            {
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal % y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal % (decimal)y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal % y.AsDecimal);
                    return result;
                }
            }
            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue DivideImplementation<T1, T2>(T1 x, T2 y, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x.Type == ArrowTypeId.Double)
            {
                // Float add float always return a float
                if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble / y.AsDouble);
                    return result;
                }
                // Float add int, will always return a float
                else if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble / y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)x.AsDouble / y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Int64)
            {
                // Int add int, will always return a double when dividing
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsLong / (double)y.AsLong);
                    return result;
                }
                // Int add float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsLong / y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsLong / y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Decimal128)
            {
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal / y.AsLong);
                    return result;
                }
                // Int add float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal / (decimal)y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal / y.AsDecimal);
                    return result;
                }
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue MultiplyImplementation<T1, T2>(T1 x, T2 y, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x.Type == ArrowTypeId.Double)
            {
                // Float add float always return a float
                if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble * y.AsDouble);
                    return result;
                }
                // Float add int, will always return a float
                else if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsDouble * y.AsLong);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)x.AsDouble * y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type== ArrowTypeId.Int64)
            {
                // Int add int, will always return an int
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value(x.AsLong * y.AsLong);
                    return result;
                }
                // Int add float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(x.AsLong * y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsLong * y.AsDecimal);
                    return result;
                }
            }
            else if (x.Type == ArrowTypeId.Decimal128)
            {
                if (y.Type == ArrowTypeId.Int64)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal * y.AsLong);
                    return result;
                }
                // Int add float, will always return a float
                else if (y.Type == ArrowTypeId.Double)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal * (decimal)y.AsDouble);
                    return result;
                }
                else if (y.Type == ArrowTypeId.Decimal128)
                {
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(x.AsDecimal * y.AsDecimal);
                    return result;
                }
            }
            result._type = ArrowTypeId.Null;
            return result;
        }
    }
}
