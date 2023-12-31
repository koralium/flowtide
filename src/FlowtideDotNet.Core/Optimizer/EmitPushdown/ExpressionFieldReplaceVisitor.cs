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

using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Optimizer.EmitPushdown
{
    internal class ExpressionFieldReplaceVisitor : ExpressionVisitor<object, object?>
    {
        private readonly Dictionary<int, int> oldToNew;

        public ExpressionFieldReplaceVisitor(Dictionary<int, int> oldToNew)
        {
            this.oldToNew = oldToNew;
        }

        public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                if (oldToNew.TryGetValue(structReferenceSegment.Field, out var newField))
                {
                    structReferenceSegment.Field = newField;
                }
                else
                {
                    throw new NotImplementedException("Only struct reference segments is supported at this time for emit optimizer");
                }
            }
            return base.VisitDirectFieldReference(directFieldReference, state);
        }
    }
}
