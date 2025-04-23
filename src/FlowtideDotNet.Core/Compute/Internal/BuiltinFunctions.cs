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

using FlowtideDotNet.Core.Compute.Columnar.Functions.CheckFunctions;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StreamingAggregations;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltinFunctions
    {
        public static void RegisterFunctions(FunctionsRegister functionsRegister)
        {
            // Column functions
            Columnar.Functions.BuiltInComparisonFunctions.AddComparisonFunctions(functionsRegister);
            BuiltInGenericFunctions.AddBuiltInAggregateGenericFunctions(functionsRegister);
            ArithmaticStreamingFunctions.AddBuiltInArithmaticFunctions(functionsRegister);
            Columnar.Functions.BuiltInStringFunctions.RegisterFunctions(functionsRegister);
            Columnar.Functions.BuiltInBooleanFunctions.AddBooleanFunctions(functionsRegister);
            Columnar.Functions.BuiltInDatetimeFunctions.AddBuiltInDatetimeFunctions(functionsRegister);
            Columnar.Functions.BuiltInRoundingFunctions.AddRoundingFunctions(functionsRegister);
            BuiltInCheckFunctions.RegisterCheckFunctions(functionsRegister);
            Columnar.Functions.BuiltInStructFunctions.AddBuiltInStructFunctions(functionsRegister);
            Columnar.Functions.BuiltInListFunctions.AddBuiltInListFunctions(functionsRegister);
            Columnar.Functions.BuiltInArithmeticFunctions.AddBuiltInArithmeticFunctions(functionsRegister);

            BuiltInComparisonFunctions.AddComparisonFunctions(functionsRegister);
            BuiltInBooleanFunctions.AddBooleanFunctions(functionsRegister);
            BuiltInStringFunctions.AddStringFunctions(functionsRegister);
            BuiltInAggregateGenericFunctions.AddBuiltInAggregateGenericFunctions(functionsRegister);
            BuiltInArithmaticFunctions.AddBuiltInArithmaticFunctions(functionsRegister);
            BuiltInRoundingFunctions.AddBuiltInRoundingFunctions(functionsRegister);
            BuiltInDatetimeFunctions.AddBuiltInDatetimeFunctions(functionsRegister);
            BuiltInListFunctions.AddListFunctions(functionsRegister);
            BuiltInGuidFunctions.AddBuiltInGuidFunctions(functionsRegister);

            BuiltInWindowFunctions.AddBuiltInWindowFunctions(functionsRegister);
        }
    }
}
