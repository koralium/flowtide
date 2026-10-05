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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.CheckExtraction
{
    /// <summary>
    /// Immutable guard stack entry, the parent holds the outer guards.
    /// </summary>
    internal sealed class CheckGuardNode
    {
        public CheckGuardNode(Expression expression, CheckGuardKind kind, CheckGuardNode? parent)
        {
            Expression = expression;
            Kind = kind;
            Parent = parent;
        }

        public Expression Expression { get; }

        public CheckGuardKind Kind { get; }

        public CheckGuardNode? Parent { get; }

        /// <summary>
        /// Guards outermost first, with cloned expressions.
        /// </summary>
        public static List<CheckGuard> ToGuards(CheckGuardNode? node)
        {
            var guards = new List<CheckGuard>();
            for (var current = node; current != null; current = current.Parent)
            {
                guards.Add(new CheckGuard()
                {
                    Expression = current.Expression.Clone(),
                    Kind = current.Kind
                });
            }
            guards.Reverse();
            return guards;
        }
    }
}
