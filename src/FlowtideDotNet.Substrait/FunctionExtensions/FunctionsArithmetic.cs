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

        //Aggregate
        public const string Sum = "sum";
        public const string Sum0 = "sum0";
        public const string Min = "min";
        public const string Max = "max";
    }
}
