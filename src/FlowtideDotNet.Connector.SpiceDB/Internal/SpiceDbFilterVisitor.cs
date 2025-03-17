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

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class SpiceDbFilterVisitor : ExpressionVisitor<bool, object?>
    {
        private readonly ReadRelation readRelation;

        private string? _resourceTypeFilter = default;
        private string? _relationFilter = default;
        private string? _subjectTypeFilter = default;

        public string? ResourceType => _resourceTypeFilter;
        public string? Relation => _relationFilter;
        public string? SubjectType => _subjectTypeFilter;

        public SpiceDbFilterVisitor(ReadRelation readRelation)
        {
            this.readRelation = readRelation;
        }

        public override bool VisitScalarFunction(ScalarFunction scalarFunction, object? state)
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
                    return false;
                }
            }
            return false;
        }

        private bool TryGetColumn(string column, Expression arg1, Expression arg2, [NotNullWhen(true)] out string? output)
        {
            if (arg1 is DirectFieldReference directFieldReference &&
                directFieldReference.ReferenceSegment is StructReferenceSegment structReference)
            {
                var columnName = readRelation.BaseSchema.Names[structReference.Field];

                if (!columnName.Equals(column, StringComparison.OrdinalIgnoreCase))
                {
                    output = null;
                    return false;
                }
                if (arg2 is StringLiteral stringLiteral)
                {
                    output = stringLiteral.Value;
                    return true;
                }
            }
            output = null;
            return false;
        }

        private bool VisitEqualComparison(ScalarFunction scalarFunction)
        {
            Debug.Assert(scalarFunction.Arguments.Count == 2);

            if (TryGetColumn("resource_type", scalarFunction.Arguments[0], scalarFunction.Arguments[1], out var objectType))
            {
                _resourceTypeFilter = objectType;
                return true;
            }
            if (TryGetColumn("resource_type", scalarFunction.Arguments[1], scalarFunction.Arguments[0], out var objectType2))
            {
                _resourceTypeFilter = objectType2;
                return true;
            }
            if (TryGetColumn("relation", scalarFunction.Arguments[0], scalarFunction.Arguments[1], out var relation))
            {
                _relationFilter = relation;
                return true;
            }
            if (TryGetColumn("relation", scalarFunction.Arguments[1], scalarFunction.Arguments[0], out var relation2))
            {
                _relationFilter = relation2;
                return true;
            }
            if (TryGetColumn("subject_type", scalarFunction.Arguments[0], scalarFunction.Arguments[1], out var subjctType))
            {
                _subjectTypeFilter = subjctType;
                return true;
            }
            if (TryGetColumn("subject_type", scalarFunction.Arguments[1], scalarFunction.Arguments[0], out var subjctType2))
            {
                _subjectTypeFilter = subjctType2;
                return true;
            }

            return false;
        }

        private bool VisitAndFunction(ScalarFunction andFunction, object? state)
        {
            bool containsFilter = false;
            foreach (var expr in andFunction.Arguments)
            {
                var result = Visit(expr, state);
                containsFilter |= result;
            }

            return containsFilter;
        }
    }
}
