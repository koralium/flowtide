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

using FlowtideDotNet.Substrait.FunctionExtensions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.CheckFunctions
{
    internal static class BuiltInCheckFunctions
    {
        public static void RegisterCheckFunctions(IFunctionsRegister functionsRegister)
        {
            // Registered so a missed extraction fails clearly instead of "function not found".
            functionsRegister.RegisterColumnScalarFunction(FunctionsCheck.Uri, FunctionsCheck.CheckValue,
                (func, parameterInfo, visitor, functionServices) => throw NotExtracted(FunctionsCheck.CheckValue));

            functionsRegister.RegisterColumnScalarFunction(FunctionsCheck.Uri, FunctionsCheck.CheckTrue,
                (func, parameterInfo, visitor, functionServices) => throw NotExtracted(FunctionsCheck.CheckTrue));
        }

        private static InvalidOperationException NotExtracted(string functionName)
        {
            return new InvalidOperationException($"The function '{functionName}' should have been extracted into a check relation by the plan optimizer, it cannot be compiled as an expression.");
        }
    }
}
