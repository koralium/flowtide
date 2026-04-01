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

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageExpressionVisitor : ExpressionVisitor<object?, object?>
    {
        private List<DirectFieldReference> _usedFields = new List<DirectFieldReference>(); 

        public IReadOnlyList<DirectFieldReference> UsedFields => _usedFields;

        public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
        {
            _usedFields.Add(directFieldReference);
            return base.VisitDirectFieldReference(directFieldReference, state);
        }

        public static IReadOnlyList<DirectFieldReference> GetFieldReferences(Expression expression)
        {
            var visitor  = new LineageExpressionVisitor();
            visitor.Visit(expression, default);
            return visitor.UsedFields;
        }
    }
}
