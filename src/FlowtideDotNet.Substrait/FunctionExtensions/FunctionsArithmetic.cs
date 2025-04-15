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

namespace FlowtideDotNet.Substrait.FunctionExtensions
{
    public static class FunctionsArithmetic
    {
        public const string Uri = "/functions_arithmetic.yaml";
        public const string Add = "add";
        public const string Subtract = "subtract";
        public const string Multiply = "multiply";
        public const string Divide = "divide";
        public const string Negate = "negate";
        public const string Modulo = "modulo";
        public const string Power = "power";
        public const string Sqrt = "sqrt";
        public const string Exp = "exp";
        public const string Cos = "cos";
        public const string Sin = "sin";
        public const string Tan = "tan";
        public const string Cosh = "cosh";
        public const string Sinh = "sinh";
        public const string Tanh = "tanh";
        public const string Acos = "acos";
        public const string Asin = "asin";
        public const string Atan = "atan";
        public const string Acosh = "acosh";
        public const string Asinh = "asinh";
        public const string Atanh = "atanh";
        public const string Atan2 = "atan2";
        public const string Radians = "radians";
        public const string Degrees = "degrees";
        public const string Abs = "abs";
        public const string Sign = "sign";

        //Aggregate
        public const string Sum = "sum";
        public const string Sum0 = "sum0";
        public const string Min = "min";
        public const string Max = "max";

        // Window
        public const string RowNumber = "row_number";
        public const string Lead = "lead";
        public const string Lag = "lag";
        public const string LastValue = "last_value";
    }
}
