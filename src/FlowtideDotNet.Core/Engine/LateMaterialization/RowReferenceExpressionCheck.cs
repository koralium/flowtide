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

using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    // Refuses an expression that cannot run next to references
    internal sealed class RowReferenceExpressionCheck : ExpressionVisitor<object, object?>
    {
        private readonly Relation _relation;
        private readonly string _part;
        private readonly string?[] _columns;
        private readonly IFunctionsRegister _functions;

        private RowReferenceExpressionCheck(Relation relation, string part, string?[] columns, IFunctionsRegister functions)
        {
            _relation = relation;
            _part = part;
            _columns = columns;
            _functions = functions;
        }

        public static void Check(Relation relation, string part, Expression expression, string?[] columns, IFunctionsRegister functions)
        {
            new RowReferenceExpressionCheck(relation, part, columns, functions).Visit(expression, null);
        }

        private string Name()
        {
            var name = _relation.GetType().Name;
            return name.EndsWith("Relation") ? name.Substring(0, name.Length - "Relation".Length) + " relation" : name;
        }

        public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment segment &&
                segment.Field >= 0 && segment.Field < _columns.Length &&
                _columns[segment.Field] != null)
            {
                throw new InvalidOperationException(
                    $"A {Name()} reads column {segment.Field} in its {_part}, that column holds row references of row store '{_columns[segment.Field]}' and a reference is not the value. Fetch the stored columns before it.");
            }
            return base.VisitDirectFieldReference(directFieldReference, state);
        }

        public override object? VisitScalarFunction(ScalarFunction scalarFunction, object? state)
        {
            // Without a columnar function the whole row becomes a row event
            if (!_functions.TryGetColumnScalarFunction(scalarFunction.ExtensionUrn, scalarFunction.ExtensionName, out _))
            {
                throw new InvalidOperationException(
                    $"A {Name()} uses function '{scalarFunction.ExtensionName}' in its {_part} next to row references and the function has no columnar form, it would work on whole rows and a row reference cannot be turned into one. Fetch every stored column before it.");
            }
            return base.VisitScalarFunction(scalarFunction, state);
        }

        public override object? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object? state)
        {
            throw new InvalidOperationException(
                $"A {Name()} has a subquery in its {_part} next to row references. Fetch the stored columns before it.");
        }
    }
}
