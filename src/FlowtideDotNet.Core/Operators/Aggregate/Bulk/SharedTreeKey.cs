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

namespace FlowtideDotNet.Core.Operators.Aggregate.Bulk
{
    internal class SharedTreeKey : IEquatable<SharedTreeKey>
    {
        public SharedTreeKey(Expression valueExpression, Expression? filterExpression, bool ignoreNulls)
        {
            ValueExpression = valueExpression;
            FilterExpression = filterExpression;
            IgnoreNulls = ignoreNulls;
        }

        public Expression ValueExpression { get; }

        public Expression? FilterExpression { get; }

        public bool IgnoreNulls { get; }

        public override bool Equals(object? obj)
        {
            return Equals(obj as SharedTreeKey);
        }

        public bool Equals(SharedTreeKey? other)
        {
            return other != null &&
                   EqualityComparer<Expression>.Default.Equals(ValueExpression, other.ValueExpression) &&
                   EqualityComparer<Expression?>.Default.Equals(FilterExpression, other.FilterExpression) &&
                   IgnoreNulls == other.IgnoreNulls;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(ValueExpression, FilterExpression, IgnoreNulls);
        }
    }
}
