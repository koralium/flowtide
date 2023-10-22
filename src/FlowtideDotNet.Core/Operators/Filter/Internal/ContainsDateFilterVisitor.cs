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
using FlowtideDotNet.Substrait.Expressions.Functions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;

namespace FlowtideDotNet.Core.Operators.Filter.Internal
{
    internal class ContainsDateFilterVisitor : ExpressionVisitor<bool, object?>
    {
        public override bool VisitAndFunction(AndFunction andFunction, object state)
        {
            bool containsDate = false;
            foreach (var arg in andFunction.Arguments)
            {
                containsDate |= arg.Accept(this, state);
            }
            return containsDate;
        }

        public override bool VisitEqualsFunction(EqualsFunction equalsFunction, object state)
        {
            bool containsDate = false;
            containsDate |= equalsFunction.Left.Accept(this, state);
            containsDate |= equalsFunction.Right.Accept(this, state);
            return containsDate;
        }

        public override bool VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
        {
            return false;
        }

        public override bool VisitOrFunction(OrFunction orFunction, object state)
        {
            bool containsDate = false;
            foreach (var arg in orFunction.Arguments)
            {
                containsDate |= arg.Accept(this, state);
            }
            return containsDate;
        }

        public override bool VisitBooleanComparison(BooleanComparison booleanComparison, object state)
        {
            bool containsDate = false;
            containsDate |= booleanComparison.Left.Accept(this, state);
            containsDate |= booleanComparison.Right.Accept(this, state);
            return containsDate;
        }

        public override bool VisitStringLiteral(StringLiteral stringLiteral, object state)
        {
            return false;
        }

        public override bool VisitGetDateTimeFunction(GetDateTimeFunction getDateTimeFunction, object state)
        {
            return true;
        }
    }
}
