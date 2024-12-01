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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInRoundingFunctions
    {
        public static void AddRoundingFunctions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsRounding.Uri, FunctionsRounding.Ceil, typeof(BuiltInRoundingFunctions), nameof(CeilImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsRounding.Uri, FunctionsRounding.Floor, typeof(BuiltInRoundingFunctions), nameof(FloorImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsRounding.Uri, FunctionsRounding.Round, typeof(BuiltInRoundingFunctions), nameof(RoundImplementation));
        }

        private static IDataValue CeilImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.Int64;
                result._int64Value = new Int64Value(value.AsLong);
                return result;
            }
            else if (value.Type == ArrowTypeId.Double)
            {
                result._type = ArrowTypeId.Double;
                result._doubleValue = new DoubleValue((long)Math.Ceiling(value.AsDouble));
                return result;
            }
            else if (value.Type == ArrowTypeId.Decimal128)
            {
                result._type = ArrowTypeId.Decimal128;
                result._decimalValue = new DecimalValue(Math.Ceiling(value.AsDecimal));
                return result;
            }
            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue FloorImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.Int64;
                result._int64Value = new Int64Value(value.AsLong);
                return result;
            }
            else if (value.Type == ArrowTypeId.Double)
            {
                result._type = ArrowTypeId.Double;
                result._doubleValue = new DoubleValue((long)Math.Floor(value.AsDouble));
                return result;
            }
            else if (value.Type == ArrowTypeId.Decimal128)
            {
                result._type = ArrowTypeId.Decimal128;
                result._decimalValue = new DecimalValue(Math.Floor(value.AsDecimal));
                return result;
            }
            result._type = ArrowTypeId.Null;
            return result;
        }

        private static IDataValue RoundImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.Int64;
                result._int64Value = new Int64Value(value.AsLong);
                return result;
            }
            else if (value.Type == ArrowTypeId.Double)
            {
                result._type = ArrowTypeId.Double;
                result._doubleValue = new DoubleValue((long)Math.Round(value.AsDouble));
                return result;
            }
            else if (value.Type == ArrowTypeId.Decimal128)
            {
                result._type = ArrowTypeId.Decimal128;
                result._decimalValue = new DecimalValue(Math.Round(value.AsDecimal));
                return result;
            }
            result._type = ArrowTypeId.Null;
            return result;
        }
    }
}
