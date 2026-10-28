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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal static class BuiltInBulkWindowFunctions
    {
        public static void AddBuiltInBulkWindowFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.Urn, FunctionsArithmetic.RowNumber, new BulkRowNumberWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.Urn, FunctionsArithmetic.Sum, new BulkSumWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.FlowtideUrn, FunctionsArithmetic.MinBy, new BulkMinByWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.FlowtideUrn, FunctionsArithmetic.MaxBy, new BulkMaxByWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.FlowtideUrn, FunctionsArithmetic.Average, new BulkAverageWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.Urn, FunctionsArithmetic.Lag, new BulkLagWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.Urn, FunctionsArithmetic.Lead, new BulkLeadWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsArithmetic.Urn, FunctionsArithmetic.LastValue, new BulkLastValueWindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsAggregateGeneric.FlowtideUrn, FunctionsAggregateGeneric.SurrogateKeyInt64, new BulkSurrogateKeyInt64WindowFunctionDefinition());
            functionsRegister.RegisterBulkWindowFunction(FunctionsDatetime.Urn, FunctionsDatetime.SessionWindow, new BulkSessionWindowFunctionDefinition());
        }
    }
}
