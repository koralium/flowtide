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

using FlexBuffers;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Functions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Filter
{
    internal class FilterCompilerVisitor<T> : ExpressionVisitor<System.Linq.Expressions.Expression, ParameterExpression>
    {
        public ParameterExpression? _dateParam;

        public override System.Linq.Expressions.Expression VisitAndFunction(AndFunction andFunction, ParameterExpression state)
        {
            var expr = Visit(andFunction.Arguments.First(), state);

            for (int i = 1; i < andFunction.Arguments.Count; i++)
            {
                var resolved = Visit(andFunction.Arguments[i], state);
                if (resolved == null)
                {
                    throw new Exception();
                }
                 expr = System.Linq.Expressions.Expression.AndAlso(expr!, resolved!);
            }

            return expr;
        }

        private static System.Linq.Expressions.Expression AccessRootVector(ParameterExpression p)
        {
            var props = typeof(T).GetProperties().FirstOrDefault(x => x.Name == "Vector");
            var getMethod = props.GetMethod;
            return System.Linq.Expressions.Expression.Property(p, getMethod);
            //return System.Linq.Expressions.Expression.MakeMemberAccess(p, getMethod);
        }

        public override System.Linq.Expressions.Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, ParameterExpression state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var method = typeof(FlxVector).GetMethod("Get");
                return System.Linq.Expressions.Expression.Call(AccessRootVector(state), method, System.Linq.Expressions.Expression.Constant(structReferenceSegment.Field));
            }
            return base.VisitDirectFieldReference(directFieldReference, state);
        }

        private static System.Linq.Expressions.MethodCallExpression Compare(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        public override System.Linq.Expressions.Expression? VisitEqualsFunction(EqualsFunction equalsFunction, ParameterExpression state)
        {
            var left = Visit(equalsFunction.Left, state);
            var right = Visit(equalsFunction.Right, state);
            return System.Linq.Expressions.Expression.Condition(
                System.Linq.Expressions.Expression.Equal(Compare(left, right), System.Linq.Expressions.Expression.Constant(0)),
                System.Linq.Expressions.Expression.Constant(true),
                System.Linq.Expressions.Expression.Constant(false)
                );
            //return Compare(left, right);// System.Linq.Expressions.Expression.Equal(left, right);
        }

        public override System.Linq.Expressions.Expression? VisitBooleanComparison(BooleanComparison booleanComparison, ParameterExpression state)
        {
            var left = Visit(booleanComparison.Left, state);
            var right = Visit(booleanComparison.Right, state);

            System.Linq.Expressions.Expression compareExpr = null;
            if (booleanComparison.Type == FlowtideDotNet.Substrait.Expressions.ScalarFunctions.BooleanComparisonType.Equals)
            {
                compareExpr = System.Linq.Expressions.Expression.Equal(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
            }
            else if (booleanComparison.Type == FlowtideDotNet.Substrait.Expressions.ScalarFunctions.BooleanComparisonType.GreaterThan)
            {
                compareExpr = System.Linq.Expressions.Expression.GreaterThan(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
            }
            else if (booleanComparison.Type == FlowtideDotNet.Substrait.Expressions.ScalarFunctions.BooleanComparisonType.GreaterThanOrEqualTo)
            {
                compareExpr = System.Linq.Expressions.Expression.GreaterThanOrEqual(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
            }
            else if (booleanComparison.Type == FlowtideDotNet.Substrait.Expressions.ScalarFunctions.BooleanComparisonType.NotEqualTo)
            {
                compareExpr = System.Linq.Expressions.Expression.NotEqual(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
            }
            else
            {
                throw new NotImplementedException();
            }

            return System.Linq.Expressions.Expression.Condition(
                compareExpr,
                System.Linq.Expressions.Expression.Constant(true),
                System.Linq.Expressions.Expression.Constant(false)
                );
        }

        public override System.Linq.Expressions.Expression? VisitNullLiteral(NullLiteral nullLiteral, ParameterExpression state)
        {
            return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.Null()));
        }

        public override System.Linq.Expressions.Expression? VisitNumericLiteral(NumericLiteral numericLiteral, ParameterExpression state)
        {
            if (numericLiteral.Value % 1 == 0)
            {
                var v = Decimal.ToInt32(numericLiteral.Value);
                return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(v)));
            }
            else
            {
                var v = Decimal.ToDouble(numericLiteral.Value);
                return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(v)));
            }
        }

        public override System.Linq.Expressions.Expression? VisitStringLiteral(StringLiteral stringLiteral, ParameterExpression state)
        {
            return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(stringLiteral.Value)));
        }

        public override System.Linq.Expressions.Expression? VisitOrFunction(OrFunction orFunction, ParameterExpression state)
        {
            var expr = Visit(orFunction.Arguments.First(), state);

            for (int i = 1; i < orFunction.Arguments.Count; i++)
            {
                expr = System.Linq.Expressions.Expression.OrElse(expr!, Visit(orFunction.Arguments[i], state)!);
            }

            return expr;
        }

        public override System.Linq.Expressions.Expression? VisitGetDateTimeFunction(GetDateTimeFunction getDateTimeFunction, ParameterExpression state)
        {
            if (_dateParam == null)
            {
                throw new NotSupportedException("Date parameter not set");
            }
            return _dateParam;
        }

        private static System.Linq.Expressions.Expression AccessIsNullProperty(System.Linq.Expressions.Expression p)
        {
            var props = typeof(FlxValue).GetProperties().FirstOrDefault(x => x.Name == "IsNull");
            var getMethod = props.GetMethod;
            return System.Linq.Expressions.Expression.Property(p, getMethod);
        }

        public override System.Linq.Expressions.Expression? VisitIsNotNull(IsNotNullFunction isNotNullFunction, ParameterExpression state)
        {
            return System.Linq.Expressions.Expression.Not(AccessIsNullProperty(Visit(isNotNullFunction.Expression, state)));
        }

        public override System.Linq.Expressions.Expression? VisitBoolLiteral(BoolLiteral boolLiteral, ParameterExpression state)
        {
            return base.VisitBoolLiteral(boolLiteral, state);
        }
    }
}
