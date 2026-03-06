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
    /// <summary>
    /// Typed base class for specific watermark values in the dataflow stream.
    /// </summary>
    /// <typeparam name="T">The specific type of the watermark value.</typeparam>
    /// <remarks>
    /// Inheriting from this class simplifies the implementation of comparison logic for custom watermark types 
    /// by ensuring that comparisons only occur between watermarks of the exact same type.
    /// </remarks>
    public abstract class AbstractWatermarkValue<T> : AbstractWatermarkValue
    {
        /// <summary>
        /// Compares the current watermark value with another watermark value.
        /// </summary>
        /// <param name="other">The other watermark value to compare against.</param>
        /// <returns>A value indicating the relative order of the objects being compared.</returns>
        /// <remarks>
        /// This method performs type-checking to ensure both values are of the same type <typeparamref name="T"/> 
        /// before delegating to the strongly-typed <see cref="Compare(T?)"/> method.
        /// </remarks>
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

        /// <summary>
        /// Compares the current strongly-typed watermark value with another of the same type.
        /// </summary>
        /// <param name="other">The other strongly-typed watermark value to compare with.</param>
        /// <returns>An integer that indicates whether the current instance precedes, follows, or occurs in the same position in the sort order as the other object.</returns>
        public abstract int Compare(T? other);
    }

    /// <summary>
    /// Base abstract class for watermark values used in the dataflow stream to track data progression.
    /// </summary>
    /// <remarks>
    /// This class provides a common structure for all watermark values, 
    /// including a BatchID that is used to split up a watermark with the same value into multiple batches.
    /// </remarks>
    public abstract class AbstractWatermarkValue : IComparable<AbstractWatermarkValue>
    {
        /// <summary>
        /// Gets the unique identifier for the specific type of watermark.
        /// </summary>
        /// <remarks>
        /// Used internally to differentiate between various implementations of watermarks during comparisons. 
        /// Ensures that operations such as equating and sorting only occur between watermarks of the exact same type.
        /// </remarks>
        public abstract int TypeId { get; }

        /// <summary>
        /// Gets or sets the batch identifier associated with this watermark.
        /// </summary>
        /// <remarks>
        /// The <see cref="BatchID"/> is utilized when a single watermark duration or metric needs to be processed 
        /// in chunks. By incrementing this ID, the engine splits a primary watermark value into multiple, ordered sub-batches.
        /// </remarks>
        public long BatchID { get; set; }

        /// <summary>
        /// Returns the smaller of two watermark values.
        /// </summary>
        /// <param name="a">The first watermark value.</param>
        /// <param name="b">The second watermark value.</param>
        /// <returns>The smaller of the two watermarks, or <c>null</c> if either is <c>null</c>.</returns>
        public static AbstractWatermarkValue? Min(AbstractWatermarkValue? a, AbstractWatermarkValue? b)
        {
            if (a == null || b == null)
            {
                return null;
            }
            return a.CompareTo(b) < 0 ? a : b;
        }

        /// <summary>
        /// Compares the internal value of the watermark with another abstract watermark value.
        /// </summary>
        /// <param name="other">The other watermark to compare.</param>
        /// <returns>An integer indicating sort order.</returns>
        /// <remarks>
        /// This base implementation ignores <see cref="BatchID"/>. The full comparable sequence is evaluated in <see cref="CompareTo(AbstractWatermarkValue?)"/>.
        /// </remarks>
        public abstract int Compare(AbstractWatermarkValue? other);

        /// <summary>
        /// Compares the current instance with another watermark and returns an integer that indicates their relative position in the sort order.
        /// </summary>
        /// <param name="other">An abstract watermark to compare with this instance.</param>
        /// <returns>A value that indicates the relative order of the objects being compared.</returns>
        /// <remarks>
        /// The comparison first checks the core watermark value using <see cref="Compare(AbstractWatermarkValue?)"/>. 
        /// If the values are equal, the tie is broken by comparing the <see cref="BatchID"/> properties.
        /// </remarks>
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

        /// <summary>
        /// Determines whether the specified object is equal to the current watermark value.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns><c>true</c> if the specified object is equal to the current object; otherwise, <c>false</c>.</returns>
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

        /// <summary>
        /// Determines whether two specified watermark values have the same value.
        /// </summary>
        /// <param name="left">The first watermark to compare.</param>
        /// <param name="right">The second watermark to compare.</param>
        /// <returns><c>true</c> if the value of left is the same as the value of right; otherwise, <c>false</c>.</returns>
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

        /// <summary>
        /// Base hash function of a watermark value, only looks at typeId and batchId.
        /// </summary>
        /// <returns>A hash code for the current object.</returns>
        /// <remarks>
        /// The base implementation combines <see cref="TypeId"/> and <see cref="BatchID"/>, which may result in collisions 
        /// if there are many unique values. Implementors of the abstraction should provide their own hash code logic for fewer collisions.
        /// </remarks>
        public override int GetHashCode()
        {
            // Typeid and batchId for hashcode, this will result in many collisions, but will give correct hash results
            // Implementors of the abstraction need to provide its own hashcode for less collisions
            return HashCode.Combine(TypeId, BatchID);
        }

        /// <summary>
        /// Determines whether two specified watermark values have different values.
        /// </summary>
        /// <param name="left">The first watermark to compare.</param>
        /// <param name="right">The second watermark to compare.</param>
        /// <returns><c>true</c> if the value of left is different from the value of right; otherwise, <c>false</c>.</returns>
        public static bool operator !=(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return !(left == right);
        }

        /// <summary>
        /// Determines whether one specified watermark value is less than another specified watermark value.
        /// </summary>
        /// <param name="left">The first watermark to compare.</param>
        /// <param name="right">The second watermark to compare.</param>
        /// <returns><c>true</c> if left is strictly less than right; otherwise, <c>false</c>.</returns>
        public static bool operator <(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return ReferenceEquals(left, null) ? !ReferenceEquals(right, null) : left.CompareTo(right) < 0;
        }

        /// <summary>
        /// Determines whether one specified watermark value is less than or equal to another specified watermark value.
        /// </summary>
        /// <param name="left">The first watermark to compare.</param>
        /// <param name="right">The second watermark to compare.</param>
        /// <returns><c>true</c> if left is less than or equal to right; otherwise, <c>false</c>.</returns>
        public static bool operator <=(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return ReferenceEquals(left, null) || left.CompareTo(right) <= 0;
        }

        /// <summary>
        /// Determines whether one specified watermark value is greater than another specified watermark value.
        /// </summary>
        /// <param name="left">The first watermark to compare.</param>
        /// <param name="right">The second watermark to compare.</param>
        /// <returns><c>true</c> if left is strictly greater than right; otherwise, <c>false</c>.</returns>
        public static bool operator >(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return !ReferenceEquals(left, null) && left.CompareTo(right) > 0;
        }

        /// <summary>
        /// Determines whether one specified watermark value is greater than or equal to another specified watermark value.
        /// </summary>
        /// <param name="left">The first watermark to compare.</param>
        /// <param name="right">The second watermark to compare.</param>
        /// <returns><c>true</c> if left is greater than or equal to right; otherwise, <c>false</c>.</returns>
        public static bool operator >=(AbstractWatermarkValue? left, AbstractWatermarkValue? right)
        {
            return ReferenceEquals(left, null) ? ReferenceEquals(right, null) : left.CompareTo(right) >= 0;
        }
    }
}
