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

namespace FlowtideDotNet.Substrait.Expressions
{
    public enum SortDirection
    {
        SortDirectionUnspecified = 0,
        SortDirectionAscNullsFirst = 1,
        SortDirectionAscNullsLast = 2,
        SortDirectionDescNullsFirst = 3,
        SortDirectionDescNullsLast = 4,
        SortDirectionClustered = 5
    }

    public sealed class SortField : IEquatable<SortField>
    {
        public required Expression Expression { get; set; }

        public SortDirection SortDirection { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is SortField field &&
                   Equals(field);
        }

        public bool Equals(SortField? other)
        {
            return other != null &&
                   Equals(Expression, other.Expression) &&
                   SortDirection == other.SortDirection;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Expression, SortDirection);
        }

        public static bool operator ==(SortField? left, SortField? right)
        {
            return EqualityComparer<SortField>.Default.Equals(left, right);
        }

        public static bool operator !=(SortField? left, SortField? right)
        {
            return !(left == right);
        }
    }
}
