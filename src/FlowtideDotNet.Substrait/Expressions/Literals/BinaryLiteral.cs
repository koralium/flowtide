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

namespace FlowtideDotNet.Substrait.Expressions.Literals
{
    public sealed class BinaryLiteral : Literal, IEquatable<BinaryLiteral>
    {
        public required byte[] Value { get; set; }
        public override LiteralType Type => LiteralType.Binary;

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitBinaryLiteral(this, state)!;
        }

        public override bool Equals(object? obj)
        {
            return obj is BinaryLiteral literal &&
                   Equals(literal);
        }

        public bool Equals(BinaryLiteral? other)
        {
            if (other == null)
            {
                return false;
            }
            if (Value == null && other.Value == null)
            {
                return true;
            }
            if (Value == null || other.Value == null)
            {
                return false;
            }
            return Value.SequenceEqual(other.Value);
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            if (Value == null)
            {
                return 0;
            }

            for (int i = 0; i < Value.Length; i++)
            {
                hash.Add(Value[i]);
            }
            return hash.ToHashCode();
        }

        public static bool operator ==(BinaryLiteral? left, BinaryLiteral? right)
        {
            return EqualityComparer<BinaryLiteral>.Default.Equals(left, right);
        }

        public static bool operator !=(BinaryLiteral? left, BinaryLiteral? right)
        {
            return !(left == right);
        }
    }
}
