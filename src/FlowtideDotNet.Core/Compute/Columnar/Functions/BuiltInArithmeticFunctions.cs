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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInArithmeticFunctions
    {
        public static void AddBuiltInArithmeticFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Uri, FunctionsArithmetic.Modulo, typeof(BuiltInArithmeticFunctions), nameof(ModuloImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsArithmetic.Uri, FunctionsArithmetic.Divide, typeof(BuiltInArithmeticFunctions), nameof(DivideImplementation));
        }

        private static IDataValue ModuloImplementation<T1, T2>(T1 x, T2 y, DataValueContainer result)
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
                    try
                    {
                        result._type = ArrowTypeId.Int64;
                        result._int64Value = new Int64Value(x.AsLong % y.AsLong);
                        return result;
                    }
                    catch (DivideByZeroException)
                    {
                        // Handle divide by zero
                        result._type = ArrowTypeId.Double;
                        result._doubleValue = new DoubleValue(double.NaN);
                        return result;
                    }

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
    }
}
