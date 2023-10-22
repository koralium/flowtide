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

namespace FlowtideDotNet.Substrait.Expressions.ScalarFunctions
{
    public class BooleanComparison : ScalarFunction
    {
        public Expression Left { get; set; }

        public Expression Right { get; set; }

        public BooleanComparisonType Type { get; set; }

        public override string ExtensionUri => "/functions_comparison.yaml";

        public override string ExtensionName
        {
            get
            {
                switch (Type)
                {
                    case BooleanComparisonType.Equals:
                        return "equal:any_any";
                    case BooleanComparisonType.NotEqualTo:
                        return "not_equal:any_any";
                    case BooleanComparisonType.GreaterThanOrEqualTo:
                        return "gte:any_any";
                    case BooleanComparisonType.GreaterThan:
                        return "gt:any_any";
                    default:
                        throw new NotImplementedException();
                }
            }
        }
        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitBooleanComparison(this, state);
        }
    }
}
