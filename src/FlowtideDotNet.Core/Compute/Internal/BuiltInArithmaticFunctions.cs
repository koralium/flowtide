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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInArithmaticFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        public static void AddBuiltInArithmaticFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Add, (x, y) => AddImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Subtract, (x, y) => SubtractImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Multiply, (x, y) => MultiplyImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Divide, (x, y) => DivideImplementation(x, y));
        }

        private static FlxValue AddImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble + y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble + y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong + y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong + y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue SubtractImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble - y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble - y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong - y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong - y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue MultiplyImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble * y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble * y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong * y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong * y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue DivideImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble / y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble / y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return a double when dividing
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong / (double)y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong / y.AsDouble));
                }
            }
            return NullValue;
        }
    }
}
