﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.MinMax;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.SurrogateKey;
using FlowtideDotNet.Substrait.FunctionExtensions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal static class BuiltInWindowFunctions
    {
        public static void AddBuiltInWindowFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum, new SumWindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.RowNumber, new RowNumberWindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Lead, new LeadWindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsAggregateGeneric.Uri, FunctionsAggregateGeneric.SurrogateKeyInt64, new SurrogateKeyInt64WindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Lag, new LagWindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.LastValue, new LastValueWindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.MinBy, new MinByWindowFunctionDefinition());
            functionsRegister.RegisterWindowFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.MaxBy, new MaxByWindowFunctionDefinition());
        }
    }
}
