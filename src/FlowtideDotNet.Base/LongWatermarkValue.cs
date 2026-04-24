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

using System;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Watermark value backed by a 64-bit integer (<see cref="long"/>).
    /// </summary>
    /// <remarks>
    /// This is commonly used for time-based or sequence-based watermarks in the stream processing engine.
    /// It inherits from <see cref="AbstractWatermarkValue{T}"/> to enforce type-safety during comparisons.
    /// </remarks>
    public class LongWatermarkValue : AbstractWatermarkValue<LongWatermarkValue>
    {
        /// <summary>
        /// Gets the integer type identifier used for type-checking comparisons and serialization.
        /// </summary>
        public override int TypeId => 1;

        /// <summary>
        /// Gets the underlying long integer value of this watermark.
        /// </summary>
        public long Value { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="LongWatermarkValue"/> class.
        /// </summary>
        /// <param name="value">The underlying 64-bit integer value.</param>
        public LongWatermarkValue(long value)
        {
            Value = value;
        }

        /// <summary>
        /// Compares the current LongWatermarkValue with another instance of the same type.
        /// </summary>
        /// <param name="other">The other <see cref="LongWatermarkValue"/> to compare against.</param>
        /// <returns>An integer that indicates the relative order of the values being compared.</returns>
        public override int Compare(LongWatermarkValue? other)
        {
            if (other is LongWatermarkValue otherLong)
            {
                return Value.CompareTo(otherLong.Value);
            }
            else if (other is null)
            {
                return 1; // This instance is greater than null
            }
            else
            {
                throw new ArgumentException("Cannot compare LongWatermarkValue with " + other.GetType().Name, nameof(other));
            }
        }

        /// <summary>
        /// Hash function that combines the hash code of the base class with the hash code of the <see cref="Value"/> property.
        /// </summary>
        /// <returns>A hash code based on the base class's hash code and the <see cref="Value"/> property.</returns>
        public override int GetHashCode()
        {
            return HashCode.Combine(Value, base.GetHashCode());
        }

        /// <summary>
        /// Creates a new <see cref="LongWatermarkValue"/> instance.
        /// </summary>
        /// <param name="value">The integer value to encapsulate.</param>
        /// <returns>A newly created <see cref="LongWatermarkValue"/>.</returns>
        public static LongWatermarkValue Create(long value)
        {
            return new LongWatermarkValue(value);
        }
    }

    /// <summary>
    /// Implements serialization and deserialization for <see cref="LongWatermarkValue"/> instances.
    /// </summary>
    internal class LongWatermarkValueSerializer : IWatermarkSerializer
    {
        /// <summary>
        /// Deserializes a <see cref="LongWatermarkValue"/> from a sequence of bytes.
        /// </summary>
        /// <param name="reader">The byte sequence reader containing the serialized long value.</param>
        /// <returns>The deserialized <see cref="AbstractWatermarkValue"/>.</returns>
        public AbstractWatermarkValue Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out long val))
            {
                throw new InvalidOperationException("Failed to read long value from the buffer.");
            }
            return new LongWatermarkValue(val);
        }

        /// <summary>
        /// Serializes an <see cref="AbstractWatermarkValue"/> into the specified buffer writer.
        /// </summary>
        /// <param name="value">The <see cref="AbstractWatermarkValue"/> (expected to be <see cref="LongWatermarkValue"/>) to serialize.</param>
        /// <param name="writer">The buffer writer to output the serialized data into.</param>
        public void Serialize(AbstractWatermarkValue value, IBufferWriter<byte> writer)
        {
            if (value is LongWatermarkValue longWatermarkValue)
            {
                var span = writer.GetSpan(8);
                BinaryPrimitives.WriteInt64LittleEndian(span, longWatermarkValue.Value);
                writer.Advance(8);
            }
            else
            {
                throw new ArgumentException("Value must be of type LongWatermarkValue", nameof(value));
            }
        }
    }
}
