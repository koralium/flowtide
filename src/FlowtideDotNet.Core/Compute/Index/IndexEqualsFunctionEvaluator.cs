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

namespace FlowtideDotNet.Core.Compute.Index
{
    internal class IndexEqualsFunctionEvaluator : ExpressionVisitor<bool, object>
    {
        private readonly int colStart;
        private readonly int colEnd;

        // Should check each side of the equals function

        public IndexEqualsFunctionEvaluator(int colStart, int colEnd)
        {
            this.colStart = colStart;
            this.colEnd = colEnd;
        }

        public override bool VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment segment)
            {
                if (segment.Field >= colStart && segment.Field < colEnd)
                {
                    return true;
                }
            }
            return false;
        }

        public override bool VisitEqualsFunction(EqualsFunction equalsFunction, object state)
        {
            return equalsFunction.Left.Accept(this, state) && equalsFunction.Right.Accept(this, state);
        }
    }
}
