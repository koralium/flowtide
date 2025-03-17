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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class OpenFgaFilterVisitor : ExpressionVisitor<string?, object?>
    {
        private readonly ReadRelation readRelation;

        public OpenFgaFilterVisitor(ReadRelation readRelation)
        {
            this.readRelation = readRelation;
        }
        public override string? VisitScalarFunction(ScalarFunction scalarFunction, object? state)
        {
            if (scalarFunction.ExtensionUri == FunctionsComparison.Uri)
            {
                switch (scalarFunction.ExtensionName)
                {
                    case FunctionsComparison.Equal:
                        return VisitEqualComparison(scalarFunction);
                }
            }
            if (scalarFunction.ExtensionUri == FunctionsBoolean.Uri)
            {
                if (scalarFunction.ExtensionName == FunctionsBoolean.And)
                {
                    return VisitAndFunction(scalarFunction, state);
                }
                if (scalarFunction.ExtensionName == FunctionsBoolean.Or)
                {
                    return null;
                }
            }
            return null;
        }

        private bool TryGetObjectTypeString(Expression arg1, Expression arg2, [NotNullWhen(true)] out string? objectType)
        {
            if (arg1 is DirectFieldReference directFieldReference &&
                directFieldReference.ReferenceSegment is StructReferenceSegment structReference)
            {
                var columnName = readRelation.BaseSchema.Names[structReference.Field];

                if (!columnName.Equals("object_type", StringComparison.OrdinalIgnoreCase))
                {
                    objectType = null;
                    return false;
                }
                if (arg2 is StringLiteral stringLiteral)
                {
                    objectType = stringLiteral.Value;
                    return true;
                }
            }
            objectType = null;
            return false;
        }

        private string? VisitEqualComparison(ScalarFunction scalarFunction)
        {
            Debug.Assert(scalarFunction.Arguments.Count == 2);

            if (TryGetObjectTypeString(scalarFunction.Arguments[0], scalarFunction.Arguments[1], out var objectType))
            {
                return objectType;
            }
            if (TryGetObjectTypeString(scalarFunction.Arguments[1], scalarFunction.Arguments[0], out var objectType2))
            {
                return objectType2;
            }

            return null;
        }

        private string? VisitAndFunction(ScalarFunction andFunction, object? state)
        {
            foreach (var expr in andFunction.Arguments)
            {
                var result = Visit(expr, state);
                if (result != null)
                {
                    return result;
                }
            }

            return null;
        }
    }
}
