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

using FlowtideDotNet.Core.Compute.Columnar.Functions.StringFunctions;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.CheckExtraction
{
    /// <summary>
    /// Replaces check functions in an expression and collects them as check definitions.
    /// </summary>
    internal sealed class CheckExpressionRewriter : ExpressionVisitor<Expression, CheckGuardNode?>
    {
        public List<CheckDefinition> Checks { get; } = new List<CheckDefinition>();

        /// <summary>
        /// Returns the expression without check functions, rewriting in place.
        /// </summary>
        public Expression Rewrite(Expression expression, CheckGuardNode? guards = null)
        {
            if (!CheckFunctionMatcher.ContainsCheck(expression))
            {
                return expression;
            }
            return Visit(expression, guards) ?? expression;
        }

        /// <summary>
        /// Boolean true when the expression is Boolean true, otherwise Boolean false.
        /// </summary>
        public static Expression IsTrue(Expression expression)
        {
            return new ScalarFunction()
            {
                ExtensionUri = FunctionsComparison.Uri,
                ExtensionName = FunctionsComparison.IsNotDistinctFrom,
                Arguments = new List<Expression>()
                {
                    expression,
                    new BoolLiteral() { Value = true }
                }
            };
        }

        public override Expression? VisitScalarFunction(ScalarFunction scalarFunction, CheckGuardNode? state)
        {
            if (CheckFunctionMatcher.IsFunction(scalarFunction, FunctionsCheck.Uri, FunctionsCheck.CheckValue))
            {
                return RewriteCheck(scalarFunction, FunctionsCheck.CheckValue, false, state);
            }
            if (CheckFunctionMatcher.IsFunction(scalarFunction, FunctionsCheck.Uri, FunctionsCheck.CheckTrue))
            {
                return RewriteCheck(scalarFunction, FunctionsCheck.CheckTrue, true, state);
            }

            var arguments = scalarFunction.Arguments;
            if (CheckFunctionMatcher.IsFunction(scalarFunction, FunctionsComparison.Uri, FunctionsComparison.Coalesce))
            {
                // Argument i runs only after earlier nulls
                var guards = state;
                for (int i = 0; i < arguments.Count; i++)
                {
                    arguments[i] = Visit(arguments[i], guards)!;
                    guards = new CheckGuardNode(arguments[i], CheckGuardKind.IsNull, guards);
                }
                return scalarFunction;
            }

            var firstGuardedArgument = GetFirstNullShortCircuitArgument(scalarFunction);
            if (firstGuardedArgument >= 0)
            {
                // Evaluation stops at the first null argument
                var guards = state;
                for (int i = 0; i < arguments.Count; i++)
                {
                    arguments[i] = Visit(arguments[i], i >= firstGuardedArgument ? guards : state)!;
                    guards = new CheckGuardNode(IsNotNull(arguments[i]), CheckGuardKind.IsTrue, guards);
                }
                return scalarFunction;
            }

            for (int i = 0; i < arguments.Count; i++)
            {
                arguments[i] = Visit(arguments[i], state)!;
            }
            return scalarFunction;
        }

        /// <summary>
        /// First argument skipped after an earlier null, -1 when eager.
        /// </summary>
        private static int GetFirstNullShortCircuitArgument(ScalarFunction scalarFunction)
        {
            if (CheckFunctionMatcher.IsFunction(scalarFunction, FunctionsComparison.Uri, FunctionsComparison.Greatest))
            {
                // The first comparison runs after the second argument
                return 2;
            }
            if (CheckFunctionMatcher.IsFunction(scalarFunction, FunctionsString.Uri, FunctionsString.Concat))
            {
                if (scalarFunction.Options != null &&
                    scalarFunction.Options.TryGetValue(BuiltInStringFunctions.NullHandling, out var nullHandling) &&
                    nullHandling == BuiltInStringFunctions.IgnoreNulls)
                {
                    return -1;
                }
                return 1;
            }
            return -1;
        }

        private static Expression IsNotNull(Expression expression)
        {
            return new ScalarFunction()
            {
                ExtensionUri = FunctionsComparison.Uri,
                ExtensionName = FunctionsComparison.IsNotNull,
                Arguments = new List<Expression>() { expression }
            };
        }

        private Expression RewriteCheck(ScalarFunction function, string name, bool isCheckTrue, CheckGuardNode? guards)
        {
            var fixedArguments = isCheckTrue ? 2 : 3;
            var arguments = function.Arguments;
            if (arguments.Count < fixedArguments)
            {
                throw new InvalidOperationException($"The function '{name}' requires at least {fixedArguments} arguments.");
            }
            if ((arguments.Count - fixedArguments) % 2 != 0)
            {
                throw new InvalidOperationException($"The function '{name}' has invalid tag arguments, they must come in pairs with key and then value.");
            }

            var conditionIndex = isCheckTrue ? 0 : 1;
            if (arguments[conditionIndex + 1] is not StringLiteral message)
            {
                throw new NotSupportedException($"The message argument of '{name}' must be a string literal, it is the name of the check. Put row values in tags and reference them in the message as {{tag}}, for example the message 'User {{userkey}} is invalid' with the tag userkey.");
            }

            var tags = new List<CheckTag>();
            for (int i = fixedArguments; i < arguments.Count; i += 2)
            {
                if (arguments[i] is not StringLiteral key)
                {
                    throw new InvalidOperationException($"The function '{name}' requires string literal tag keys.");
                }
                var value = arguments[i + 1];
                if (CheckFunctionMatcher.ContainsCheck(value))
                {
                    throw NestedInTag(name);
                }
                tags.Add(new CheckTag()
                {
                    Key = key.Value,
                    Value = value.Clone()
                });
            }

            // Checks nested in the condition run before this one
            var condition = Visit(arguments[conditionIndex], guards)!;
            Checks.Add(new CheckDefinition()
            {
                Condition = condition.Clone(),
                Message = message.Value,
                Tags = tags,
                Guards = CheckGuardNode.ToGuards(guards)
            });

            if (isCheckTrue)
            {
                return IsTrue(condition);
            }
            return Visit(arguments[0], guards)!;
        }

        private static NotSupportedException NestedInTag(string name)
        {
            return new NotSupportedException($"A check function is not supported inside the tag arguments of '{name}', those arguments are only evaluated when the check fails. Move the inner check into the SELECT list or WHERE clause.");
        }

        public override Expression? VisitIfThen(IfThenExpression ifThenExpression, CheckGuardNode? state)
        {
            // Clause i runs only when earlier conditions failed
            var notTaken = state;
            for (int i = 0; i < ifThenExpression.Ifs.Count; i++)
            {
                var clause = ifThenExpression.Ifs[i];
                clause.If = Visit(clause.If, notTaken)!;
                clause.Then = Visit(clause.Then, new CheckGuardNode(clause.If, CheckGuardKind.IsTrue, notTaken))!;
                notTaken = new CheckGuardNode(clause.If, CheckGuardKind.IsNotTrue, notTaken);
            }
            if (ifThenExpression.Else != null)
            {
                ifThenExpression.Else = Visit(ifThenExpression.Else, notTaken);
            }
            return ifThenExpression;
        }

        public override Expression? VisitSingularOrList(SingularOrListExpression singularOrList, CheckGuardNode? state)
        {
            singularOrList.Value = Visit(singularOrList.Value, state)!;
            for (int i = 0; i < singularOrList.Options.Count; i++)
            {
                singularOrList.Options[i] = Visit(singularOrList.Options[i], state)!;
            }
            return singularOrList;
        }

        public override Expression? VisitMultiOrList(MultiOrListExpression multiOrList, CheckGuardNode? state)
        {
            for (int i = 0; i < multiOrList.Value.Count; i++)
            {
                multiOrList.Value[i] = Visit(multiOrList.Value[i], state)!;
            }
            foreach (var option in multiOrList.Options)
            {
                for (int i = 0; i < option.Fields.Count; i++)
                {
                    option.Fields[i] = Visit(option.Fields[i], state)!;
                }
            }
            return multiOrList;
        }

        public override Expression? VisitCastExpression(CastExpression castExpression, CheckGuardNode? state)
        {
            castExpression.Expression = Visit(castExpression.Expression, state)!;
            return castExpression;
        }

        public override Expression? VisitStructExpression(StructExpression structExpression, CheckGuardNode? state)
        {
            for (int i = 0; i < structExpression.Fields.Count; i++)
            {
                structExpression.Fields[i] = Visit(structExpression.Fields[i], state)!;
            }
            return structExpression;
        }

        public override Expression? VisitListNestedExpression(ListNestedExpression listNestedExpression, CheckGuardNode? state)
        {
            for (int i = 0; i < listNestedExpression.Values.Count; i++)
            {
                listNestedExpression.Values[i] = Visit(listNestedExpression.Values[i], state)!;
            }
            return listNestedExpression;
        }

        public override Expression? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, CheckGuardNode? state)
        {
            for (int i = 0; i < mapNestedExpression.KeyValues.Count; i++)
            {
                var pair = mapNestedExpression.KeyValues[i];
                var key = Visit(pair.Key, state)!;
                var value = Visit(pair.Value, state)!;
                mapNestedExpression.KeyValues[i] = new KeyValuePair<Expression, Expression>(key, value);
            }
            return mapNestedExpression;
        }

        public override Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, CheckGuardNode? state)
        {
            for (int i = 0; i < arrayLiteral.Expressions.Count; i++)
            {
                arrayLiteral.Expressions[i] = Visit(arrayLiteral.Expressions[i], state)!;
            }
            return arrayLiteral;
        }

        public override Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, CheckGuardNode? state)
        {
            return directFieldReference;
        }

        public override Expression? VisitStringLiteral(StringLiteral stringLiteral, CheckGuardNode? state)
        {
            return stringLiteral;
        }

        public override Expression? VisitNumericLiteral(NumericLiteral numericLiteral, CheckGuardNode? state)
        {
            return numericLiteral;
        }

        public override Expression? VisitNullLiteral(NullLiteral nullLiteral, CheckGuardNode? state)
        {
            return nullLiteral;
        }

        public override Expression? VisitBoolLiteral(BoolLiteral boolLiteral, CheckGuardNode? state)
        {
            return boolLiteral;
        }

        public override Expression? VisitBinaryLiteral(BinaryLiteral binaryLiteral, CheckGuardNode? state)
        {
            return binaryLiteral;
        }

        public override Expression? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, CheckGuardNode? state)
        {
            return setPredicateExpression;
        }
    }
}
