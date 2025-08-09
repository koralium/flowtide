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
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text;

namespace FlowtideDotNet.SqlServer.SqlServer
{
    public class FilterResult
    {
        public FilterResult(string content, bool isBoolean)
        {
            Content = content;
            IsBoolean = isBoolean;
        }

        public string Content { get; }

        public bool IsBoolean { get; }
    }

    internal class SqlServerFilterVisitor : ExpressionVisitor<FilterResult, object?>
    {
        private readonly ReadRelation readRelation;



        public SqlServerFilterVisitor(ReadRelation readRelation)
        {
            this.readRelation = readRelation;
        }

        public override FilterResult? VisitScalarFunction(ScalarFunction scalarFunction, object? state)
        {
            if (scalarFunction.ExtensionUri == FunctionsComparison.Uri)
            {
                switch (scalarFunction.ExtensionName)
                {
                    case FunctionsComparison.Equal:
                        return VisitBooleanComparison(scalarFunction, "=", state);
                    case FunctionsComparison.GreaterThan:
                        return VisitBooleanComparison(scalarFunction, ">", state);
                    case FunctionsComparison.GreaterThanOrEqual:
                        return VisitBooleanComparison(scalarFunction, ">=", state);
                    case FunctionsComparison.LessThan:
                        return VisitBooleanComparison(scalarFunction, "<", state);
                    case FunctionsComparison.LessThanOrEqual:
                        return VisitBooleanComparison(scalarFunction, "<=", state);
                    case FunctionsComparison.NotEqual:
                        return VisitBooleanComparison(scalarFunction, "!=", state);
                    case FunctionsComparison.IsNotNull:
                        return VisitIsNotNull(scalarFunction, state);
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
                    return VisitOrFunction(scalarFunction, state);
                }
            }
            if (scalarFunction.ExtensionUri == FunctionsString.Uri)
            {
                if (scalarFunction.ExtensionName == FunctionsString.Concat)
                {
                    return VisitConcatFunction(scalarFunction, state);
                }
                else if (scalarFunction.ExtensionName == FunctionsString.Lower)
                {
                    return VisitLowerFunction(scalarFunction, state);
                }
            }
            return base.VisitScalarFunction(scalarFunction, state);
        }

        public override FilterResult? VisitSingularOrList(SingularOrListExpression singularOrList, object? state)
        {
            var columnExpr = Visit(singularOrList.Value, state);
            if (columnExpr == null)
            {
                return null;
            }
            List<string> values = new List<string>();
            for (int i = 0; i < singularOrList.Options.Count; i++)
            {
                var v = Visit(singularOrList.Options[i], state);
                if (v == null)
                {
                    return null;
                }
                values.Add(v.Content);
            }
            return new FilterResult($"{columnExpr.Content} IN ({string.Join(", ", values)})", true);
        }

        private FilterResult? VisitLowerFunction(ScalarFunction scalarFunction, object? state)
        {
            var input = Visit(scalarFunction.Arguments[0], state);

            if (input == null)
            {
                return null;
            }

            return new FilterResult($"LOWER({input.Content})", false);
        }

        private FilterResult? VisitBooleanComparison(ScalarFunction scalarFunction, string op, object? state)
        {
            Debug.Assert(scalarFunction.Arguments.Count == 2);
            var left = Visit(scalarFunction.Arguments[0], state);
            var right = Visit(scalarFunction.Arguments[1], state);

            if (left == null || right == null)
            {
                return null;
            }
            return new FilterResult($"{left.Content} {op} {right.Content}", true);
        }

        private FilterResult? VisitAndFunction(ScalarFunction andFunction, object? state)
        {
            List<string> resolved = new List<string>();
            foreach (var expr in andFunction.Arguments)
            {
                var result = Visit(expr, state);
                if (result == null)
                {
                    return null;
                }
                if (!result.IsBoolean)
                {
                    resolved.Add($"{result.Content} = 1");
                }
                else
                {
                    resolved.Add(result.Content);
                }

            }
            return new(string.Join(" AND ", resolved), true);
        }

        public override FilterResult? VisitBoolLiteral(BoolLiteral boolLiteral, object? state)
        {
            if (boolLiteral.Value)
            {
                return new FilterResult("1", false);
            }
            return new FilterResult("0", false);
        }

        public override FilterResult? VisitStringLiteral(StringLiteral stringLiteral, object? state)
        {
            return new FilterResult($"'{stringLiteral.Value}'", false);
        }

        private FilterResult? VisitConcatFunction(ScalarFunction concatFunction, object? state)
        {
            List<string> resolved = new List<string>();
            foreach (var expr in concatFunction.Arguments)
            {
                var result = Visit(expr, state);
                if (result == null)
                {
                    return null;
                }
                resolved.Add(result.Content);
            }
            return new FilterResult($"concat({string.Join(", ", resolved)})", false);
        }

        public override FilterResult? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                return new FilterResult($"[{readRelation.BaseSchema.Names[structReferenceSegment.Field]}]", false);
            }
            return null;
        }

        public override FilterResult? VisitNumericLiteral(NumericLiteral numericLiteral, object? state)
        {
            return new(numericLiteral.Value.ToString(), false);
        }

        private FilterResult? VisitOrFunction(ScalarFunction orFunction, object? state)
        {
            List<string> resolved = new List<string>();
            foreach (var expr in orFunction.Arguments)
            {
                var result = Visit(expr, state);
                if (result == null)
                {
                    return null;
                }
                if (result.IsBoolean)
                {
                    resolved.Add(result.Content);
                }
                else
                {
                    resolved.Add($"{result.Content} = 1");
                }

            }
            return new(string.Join(" OR ", resolved), true);
        }

        public override FilterResult? VisitNullLiteral(NullLiteral nullLiteral, object? state)
        {
            return new FilterResult("null", false);
        }

        public override FilterResult? VisitIfThen(IfThenExpression ifThenExpression, object? state)
        {
            List<string> resolved = new List<string>();
            foreach (var ifThenStatement in ifThenExpression.Ifs)
            {
                var ifstatement = Visit(ifThenStatement.If, state);
                var thenstatement = Visit(ifThenStatement.Then, state);

                if (ifstatement == null || thenstatement == null)
                {
                    return null;
                }
                if (ifstatement.IsBoolean)
                {
                    resolved.Add($"WHEN {ifstatement.Content} THEN {thenstatement.Content}");
                }
                else
                {
                    resolved.Add($"WHEN {ifstatement.Content} = 1 THEN {thenstatement.Content}");
                }

            }

            string? elseString = default;

            if (ifThenExpression.Else != null)
            {
                var result = Visit(ifThenExpression.Else, state);
                if (result == null)
                {
                    return null;
                }
                elseString = result.Content;
            }

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("CASE");
            foreach (var line in resolved)
            {
                stringBuilder.AppendLine(line);
            }
            if (elseString != null)
            {
                stringBuilder.AppendLine($"ELSE {elseString}");
            }
            stringBuilder.AppendLine("END");

            return new FilterResult(stringBuilder.ToString(), false);
        }

        private FilterResult? VisitIsNotNull(ScalarFunction isNotNullFunction, object? state)
        {
            Debug.Assert(isNotNullFunction.Arguments.Count == 1);
            var result = Visit(isNotNullFunction.Arguments[0], state);
            if (result == null)
            {
                return null;
            }

            return new FilterResult($"{result.Content} IS NOT NULL", true);
        }
    }
}
