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
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.CheckExtraction
{
    internal static class CheckFunctionMatcher
    {
        /// <summary>
        /// Matches a function the same way the functions register looks it up.
        /// </summary>
        public static bool IsFunction(ScalarFunction function, string uri, string name)
        {
            return string.Equals(function.ExtensionUri, uri, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(function.ExtensionName, name, StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsCheckFunction(ScalarFunction function)
        {
            return IsFunction(function, FunctionsCheck.Uri, FunctionsCheck.CheckValue) ||
                IsFunction(function, FunctionsCheck.Uri, FunctionsCheck.CheckTrue);
        }

        public static bool ContainsCheck(Expression? expression)
        {
            if (expression == null)
            {
                return false;
            }
            var finder = new CheckFinder();
            finder.Visit(expression, null);
            return finder.Found;
        }

        public static bool ContainsCheck(CheckDefinition check)
        {
            foreach (var expression in GetExpressions(check))
            {
                if (ContainsCheck(expression))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// The condition, tag values and guard expressions of a check.
        /// </summary>
        public static IEnumerable<Expression> GetExpressions(CheckDefinition check)
        {
            yield return check.Condition;
            foreach (var tag in check.Tags)
            {
                yield return tag.Value;
            }
            foreach (var guard in check.Guards)
            {
                yield return guard.Expression;
            }
        }

        private sealed class CheckFinder : ExpressionVisitor<object?, object?>
        {
            public bool Found { get; private set; }

            public override object? VisitScalarFunction(ScalarFunction scalarFunction, object? state)
            {
                if (IsCheckFunction(scalarFunction))
                {
                    Found = true;
                    return null;
                }
                return base.VisitScalarFunction(scalarFunction, state);
            }
        }
    }
}
