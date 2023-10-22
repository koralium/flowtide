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
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
using FlowtideDotNet.Substrait.Relations;
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

    internal class SqlServerFilterVisitor : ExpressionVisitor<FilterResult, object>
    {
        private readonly ReadRelation readRelation;

        

        public SqlServerFilterVisitor(ReadRelation readRelation)
        {
            this.readRelation = readRelation;
        }

        public override FilterResult? VisitBooleanComparison(BooleanComparison booleanComparison, object state)
        {
            var left = Visit(booleanComparison.Left, state);
            var right = Visit(booleanComparison.Right, state);

            string? op = default;
            switch (booleanComparison.Type)
            {
                case BooleanComparisonType.Equals:
                    op = "=";
                    break;
                case BooleanComparisonType.GreaterThanOrEqualTo:
                    op = ">=";
                    break;
                case BooleanComparisonType.GreaterThan:
                    op = ">";
                    break;
                case BooleanComparisonType.NotEqualTo:
                    op = "!=";
                    break;
            }

            return new FilterResult($"{left.Content} {op} {right.Content}", true);
        }

        public override FilterResult? VisitAndFunction(AndFunction andFunction, object state)
        {
            List<string> resolved = new List<string>();
            foreach(var expr in andFunction.Arguments)
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
            return new (string.Join(" AND ", resolved), true);
        }

        public override FilterResult? VisitBoolLiteral(BoolLiteral boolLiteral, object state)
        {
            if (boolLiteral.Value)
            {
                return new FilterResult("1", false);
            }
            return new FilterResult("0", false);
        }

        public override FilterResult? VisitStringLiteral(StringLiteral stringLiteral, object state)
        {
            return new FilterResult($"'{stringLiteral.Value}'", false);
        }

        public override FilterResult? VisitConcatFunction(ConcatFunction concatFunction, object state)
        {
            List<string> resolved = new List<string>();
            foreach (var expr in concatFunction.Expressions)
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

        public override FilterResult? VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                return new FilterResult($"[{readRelation.BaseSchema.Names[structReferenceSegment.Field]}]", false);
            }
            return null;
        }

        public override FilterResult? VisitNumericLiteral(NumericLiteral numericLiteral, object state)
        {
            return new(numericLiteral.Value.ToString(), false);
        }

        public override FilterResult? VisitOrFunction(OrFunction orFunction, object state)
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
            return new (string.Join(" OR ", resolved), true);
        }

        public override FilterResult? VisitNullLiteral(NullLiteral nullLiteral, object state)
        {
            return new FilterResult("null", false);
        }

        public override FilterResult? VisitIfThen(IfThenExpression ifThenExpression, object state)
        {
            List<string> resolved = new List<string>();
            foreach(var ifThenStatement in ifThenExpression.Ifs)
            {
                var ifstatement = Visit(ifThenStatement.If, state);
                var thenstatement = Visit(ifThenExpression.Else, state);

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
            foreach(var line in resolved)
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

        public override FilterResult? VisitIsNotNull(IsNotNullFunction isNotNullFunction, object state)
        {
            var result = Visit(isNotNullFunction.Expression, state);
            if (result == null)
            {
                return null;
            }

            return new FilterResult($"{result.Content} IS NOT NULL", true);
        }
    }
}
