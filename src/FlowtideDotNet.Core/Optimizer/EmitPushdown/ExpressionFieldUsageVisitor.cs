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

namespace FlowtideDotNet.Core.Optimizer.EmitPushdown
{
    internal class ExpressionFieldUsageVisitor : ExpressionVisitor<object, object?>
    {
        private readonly int leftSize;

        public List<int> UsedFieldsLeft { get; } = new List<int>();
        public List<int> UsedFieldsRight { get; } = new List<int>();
        public bool CanOptimize { get; private set; } = true;

        public ExpressionFieldUsageVisitor(int leftSize)
        {
            this.leftSize = leftSize;
        }

        public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                if (structReferenceSegment.Field < leftSize)
                {
                    UsedFieldsLeft.Add(structReferenceSegment.Field);
                }
                else
                {
                    UsedFieldsRight.Add(structReferenceSegment.Field);
                }
            }
            else
            {
                CanOptimize = false;
            }
            return base.VisitDirectFieldReference(directFieldReference, state);
        }
    }
}
