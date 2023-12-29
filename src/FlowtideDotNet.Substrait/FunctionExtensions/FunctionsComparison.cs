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
    public static class FunctionsComparison
    {
        public const string Uri = "/functions_comparison.yaml";
        public const string Equal = "equal";
        public const string NotEqual = "not_equal";
        public const string GreaterThan = "gt";
        public const string GreaterThanOrEqual = "gte";
        public const string LessThan = "lt";
        public const string LessThanOrEqual = "lte";
        public const string IsNotNull = "is_not_null";
        public const string Coalesce = "coalesce";
        public const string isInfinite = "is_infinite";
        public const string IsFinite = "is_finite";
        public const string Between = "between";
        public const string IsNull = "is_null";
        public const string IsNan = "is_nan";
        public const string NullIf = "nullif";
    }
}
