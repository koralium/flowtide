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
    internal static class BuiltInRoundingFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());

        public static void AddBuiltInRoundingFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsRounding.Uri, FunctionsRounding.Ceil, (x) => CeilingImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsRounding.Uri, FunctionsRounding.Floor, (x) => FloorImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsRounding.Uri, FunctionsRounding.Round, (x) => RoundImplementation(x));
        }
        
        private static FlxValue CeilingImplementation(FlxValue x)
        {
            // Integers are already rounded
            if (x.ValueType == FlexBuffers.Type.Int)
            {
                return x;
            }
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue((long)Math.Ceiling(x.AsDouble)));
            }
            return NullValue;
        }

        private static FlxValue FloorImplementation(FlxValue x)
        {
            // Integers are already rounded
            if (x.ValueType == FlexBuffers.Type.Int)
            {
                return x;
            }
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue((long)Math.Floor(x.AsDouble)));
            }
            return NullValue;
        }

        private static FlxValue RoundImplementation(FlxValue x)
        {
            // Integers are already rounded
            if (x.ValueType == FlexBuffers.Type.Int)
            {
                return x;
            }
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue((long)Math.Round(x.AsDouble)));
            }
            return NullValue;
        }
    }
}
