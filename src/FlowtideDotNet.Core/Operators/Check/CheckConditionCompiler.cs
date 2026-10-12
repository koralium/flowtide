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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Substrait.Relations;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Operators.Check
{
    /// <summary>
    /// Compiles the row tests of a check definition.
    /// </summary>
    internal static class CheckConditionCompiler
    {
        private static readonly MethodInfo s_isBooleanFalse = GetHelper(nameof(IsBooleanFalse));
        private static readonly MethodInfo s_isNullValue = GetHelper(nameof(IsNullValue));
        private static readonly MethodInfo s_toBool = typeof(DataValueBoolFunctions).GetMethod(nameof(DataValueBoolFunctions.ToBool), BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException("ToBool method not found");

        /// <summary>
        /// True when every guard holds and the condition is Boolean false.
        /// </summary>
        public static Func<EventBatchData, int, bool> CompileFailing(CheckDefinition check, IFunctionsRegister functionsRegister)
        {
            var batchParam = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            var indexParam = System.Linq.Expressions.Expression.Parameter(typeof(int));
            var visitor = new ColumnarExpressionVisitor(functionsRegister);

            var condition = Visit(visitor, check.Condition, batchParam, indexParam);
            System.Linq.Expressions.Expression body = condition.Type == typeof(bool) ?
                System.Linq.Expressions.Expression.Not(condition) :
                System.Linq.Expressions.Expression.Call(s_isBooleanFalse.MakeGenericMethod(condition.Type), condition);

            // Outermost guard is tested first
            for (int i = check.Guards.Count - 1; i >= 0; i--)
            {
                var guard = check.Guards[i];
                var guardExpression = Visit(visitor, guard.Expression, batchParam, indexParam);
                body = System.Linq.Expressions.Expression.AndAlso(CompileGuard(guardExpression, guard.Kind), body);
            }

            return System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, bool>>(body, batchParam, indexParam).Compile();
        }

        private static System.Linq.Expressions.Expression CompileGuard(System.Linq.Expressions.Expression expression, CheckGuardKind kind)
        {
            switch (kind)
            {
                case CheckGuardKind.IsTrue:
                    return ToBool(expression);
                case CheckGuardKind.IsNotTrue:
                    return System.Linq.Expressions.Expression.Not(ToBool(expression));
                case CheckGuardKind.IsNull:
                    if (expression.Type == typeof(bool))
                    {
                        return System.Linq.Expressions.Expression.Block(expression, System.Linq.Expressions.Expression.Constant(false));
                    }
                    return System.Linq.Expressions.Expression.Call(s_isNullValue.MakeGenericMethod(expression.Type), expression);
                default:
                    throw new NotSupportedException($"Check guard kind {kind} is not supported.");
            }
        }

        private static System.Linq.Expressions.Expression ToBool(System.Linq.Expressions.Expression expression)
        {
            if (expression.Type == typeof(bool))
            {
                return expression;
            }
            return System.Linq.Expressions.Expression.Call(s_toBool.MakeGenericMethod(expression.Type), expression);
        }

        private static System.Linq.Expressions.Expression Visit(
            ColumnarExpressionVisitor visitor,
            Substrait.Expressions.Expression expression,
            ParameterExpression batchParam,
            ParameterExpression indexParam)
        {
            // Own result container per expression
            var parameterInfo = new ColumnParameterInfo(
                new List<ParameterExpression>() { batchParam },
                new List<ParameterExpression>() { indexParam },
                new List<int>() { 0 },
                System.Linq.Expressions.Expression.Constant(new DataValueContainer()));
            var result = visitor.Visit(expression, parameterInfo);
            if (result == null)
            {
                throw new InvalidOperationException("Could not compile a check expression.");
            }
            return result;
        }

        private static MethodInfo GetHelper(string name)
        {
            return typeof(CheckConditionCompiler).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException($"{name} method not found");
        }

        private static bool IsBooleanFalse<T>(T value)
            where T : IDataValue
        {
            return value.Type == ArrowTypeId.Boolean && !value.AsBool;
        }

        private static bool IsNullValue<T>(T value)
            where T : IDataValue
        {
            return value.IsNull;
        }
    }
}
