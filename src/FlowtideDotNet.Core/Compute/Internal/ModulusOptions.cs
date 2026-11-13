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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;

namespace FlowtideDotNet.Core.Compute.Internal;

internal static class ModulusOptions
{
    public static void Validate(ScalarFunction function)
    {
        if (function.Arguments.Count != 2)
        {
            throw new ArgumentException("modulus requires exactly two arguments.", nameof(function));
        }
        if (function.Options == null)
        {
            return;
        }
        if (function.ExtensionUrn == FunctionsArithmetic.FlowtideUrn && function.Options.Count > 0)
        {
            throw new NotSupportedException("The Flowtide modulus profile does not define function options.");
        }
        var decimalModulus = function.ExtensionUrn == FunctionsArithmetic.DecimalUrn;
        foreach (var option in function.Options)
        {
            var expected = option.Key switch
            {
                "division_type" when !decimalModulus => "TRUNCATE",
                "overflow" => "ERROR",
                "on_domain_error" when !decimalModulus => "NULL",
                _ => null
            };
            if (expected == null || option.Value != expected)
            {
                throw new NotSupportedException($"Unsupported modulus option: {option.Key}={option.Value}.");
            }
        }
    }
}
