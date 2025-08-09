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

namespace FlowtideDotNet.Base
{
    public abstract class AbstractWatermarkValue<T> : AbstractWatermarkValue
    {
        public override int Compare(AbstractWatermarkValue? other)
        {
            if (other is T otherValue)
            {
                return Compare(otherValue);
            }
            else if (other is null)
            {
                return 1; // This instance is greater than null
            }
            else
            {
                throw new ArgumentException($"Cannot compare {GetType().Name} with {other.GetType().Name}", nameof(other));
            }
        }

        public abstract int Compare(T? other);
    }

    public abstract class AbstractWatermarkValue : IComparable<AbstractWatermarkValue>
    {
        public abstract int TypeId { get; }

        public long BatchID { get; set; }

        public static AbstractWatermarkValue? Min(AbstractWatermarkValue? a, AbstractWatermarkValue? b)
        {
            if (a == null || b == null)
            {
                return null;
            }
            return a.CompareTo(b) < 0 ? a : b;
        }

        public abstract int Compare(AbstractWatermarkValue? other);

        public int CompareTo(AbstractWatermarkValue? other)
        {
            if (ReferenceEquals(other, null))
            {
                return 1; // This instance is greater than null
            }
            if (this.TypeId != other.TypeId)
            {
                throw new ArgumentException("Cannot compare different types of AbstractWatermarkValue");
            }
            var result = Compare(other);
            if (result != 0)
            {
                return result;
            }
            return BatchID.CompareTo(other.BatchID);
        }

        public override bool Equals(object? obj)
        {
            if (obj is AbstractWatermarkValue other)
            {
                if (this.TypeId != other.TypeId)
                {
                    return false; // Different types cannot be equal
                }
                return CompareTo(other) == 0;
            }
            return false;
        }

        public static bool operator ==(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            if (ReferenceEquals(left, right))
            {
                return true; // Both are the same instance or both are null
            }
            if (ReferenceEquals(left, null) || ReferenceEquals(right, null))
            {
                return false; // One is null, the other is not
            }
            // If both are not null, compare them
            return left.CompareTo(right) == 0;
        }

        public override int GetHashCode()
        {
            // Typeid and batchId for hashcode, this will result in many collisions, but will give correct hash results
            // Implementors of the abstraction need to provide its own hashcode for less collisions
            return HashCode.Combine(TypeId, BatchID);
        }

        public static bool operator !=(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return !(left == right);
        }

        public static bool operator <(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return ReferenceEquals(left, null) ? !ReferenceEquals(right, null) : left.CompareTo(right) < 0;
        }

        public static bool operator <=(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return ReferenceEquals(left, null) || left.CompareTo(right) <= 0;
        }

        public static bool operator >(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return !ReferenceEquals(left, null) && left.CompareTo(right) > 0;
        }

        public static bool operator >=(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return ReferenceEquals(left, null) ? ReferenceEquals(right, null) : left.CompareTo(right) >= 0;
        }
    }
}
